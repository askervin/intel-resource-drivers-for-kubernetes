/*
 * Copyright (c) 2026, Intel Corporation.  All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	coreclientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	cdiapi "tags.cncf.io/container-device-interface/pkg/cdi"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
)

// The lease broker serves the lease sockets of token claims. A request
// from a container creates, in the container's namespace, a ResourceClaim
// and a companion pause pod "cxl-lease-<id>" pinned to this node. The
// scheduler allocates a local memdev (cxl.generic) or a pool device to the
// claim, the pool controller attaches a pool device, and kubelet prepares
// the claim through the driver of the device, which lends the memory to
// the requesting container (leaseHooks, lending.go). Deleting the
// companion releases it again. See doc/cxl/POOL.md and doc/cxl/LOCAL.md.

const (
	// Labels of the companion claim and pod.
	leaseLabel        = "cxl-pool.generic/lease"
	requesterUIDLabel = "cxl-pool.generic/requester-uid"
	leaseNodeLabel    = "cxl-pool.generic/node"
	// requesterAnnotation is <namespace>/<pod>/<container> for humans.
	requesterAnnotation = "cxl-pool.generic/requester"

	companionClaimRef  = "lease" // pod.spec.resourceClaims[0].name and request name
	companionContainer = "pause"
	poolMemoryClass    = "cxl-pool-memory"
	poolSharedClass    = "cxl-shared-memory"
	localMemoryClass   = "cxl-local-memory"
	localDaxClass      = "cxl-local-dax"
	// leaseCloseTimeout bounds how long the Unprepare of a token claim
	// waits for its leases to be revoked and their companions to be
	// deleted (not gone: a companion of a local lease is unprepared by
	// cxl.generic, whose calls the kubelet plugin helper serializes).
	leaseCloseTimeout   = 5 * time.Second
	leaseReconcileEvery = 60 * time.Second
	// leaseCreatingGrace is how long a lease may stay Creating before the
	// reconcile treats it as left over by a crash.
	leaseCreatingGrace = 2 * time.Minute
	// apiTimeout bounds one Kubernetes API call.
	apiTimeout = 15 * time.Second
	// maxRequestLine bounds a request line.
	maxRequestLine = 64 * 1024
)

// poolHostsCapacity is the capacity of a shared pool device that limits
// the number of nodes attaching it at a time; an exclusive lease requests
// all of it.
const poolHostsCapacity resourceapi.QualifiedName = "hosts"

var (
	errLendingUnavailable = errors.New("lending unavailable: NRI not connected")
	errNotOurPod          = errors.New("not a container of the claim's pod")
	errPoolDisabled       = errors.New("pool helper disabled (--pool-driver-name)")
)

// leaseRequest is one request line on a lease socket.
type leaseRequest struct {
	Op   string `json:"op"`             // lease | release | list | fd
	Kind string `json:"kind,omitempty"` // memory | shared
	Size string `json:"size,omitempty"` // memory: quantity
	// Serial is the device of a shared lease, and optionally selects
	// the device of a memory lease.
	Serial string `json:"serial,omitempty"`
	// Source of a memory lease: pool ("", pool devices), local (memdevs
	// of this node) or any (local if one fits, else pool). Shared leases
	// are pool leases.
	Source    string `json:"source,omitempty"`
	Wait      string `json:"wait,omitempty"` // Go duration, "" = broker default
	RequestID string `json:"requestId,omitempty"`
	Lease     string `json:"lease,omitempty"`
	All       bool   `json:"all,omitempty"`
	// Mode of a memory lease: numa ("", lent as a NUMA node) or dax
	// (passed as a devdax fd). Shared leases are always dax.
	Mode string `json:"mode,omitempty"`
	// Exclusive: a shared lease that takes all of the device's "hosts"
	// capacity, so that no other claim can use the device meanwhile.
	Exclusive bool `json:"exclusive,omitempty"`
	// ReadOnly: the dax fd is opened read-only (dax leases only).
	ReadOnly bool `json:"readOnly,omitempty"`
}

// leaseResponse is one response line. A response of a Ready dax lease to
// "lease" and "fd" carries the open dax fd (SCM_RIGHTS).
type leaseResponse struct {
	Lease     string           `json:"lease,omitempty"`
	State     string           `json:"state,omitempty"`
	Kind      string           `json:"kind,omitempty"`
	Source    string           `json:"source,omitempty"` // requested, or allocated once Ready
	Mode      string           `json:"mode,omitempty"`
	Exclusive bool             `json:"exclusive,omitempty"`
	ReadOnly  bool             `json:"readOnly,omitempty"`
	Node      *int             `json:"node,omitempty"`
	Size      uint64           `json:"size,omitempty"`
	Serial    string           `json:"serial,omitempty"`
	Dax       string           `json:"dax,omitempty"`
	Error     string           `json:"error,omitempty"`
	Leases    *[]leaseResponse `json:"leases,omitempty"` // list, release all
}

// daxFd names the dax device whose fd goes with a response: none if path
// is "".
type daxFd struct {
	path     string
	readOnly bool
}

// lendConfig is the opaque configuration (kind LendConfig) of a
// companion claim, for the driver that prepares it (apiVersion
// <driver>/v1alpha1).
type lendConfig struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Lease      string `json:"lease"`
	Pod        podRef `json:"pod"`
	Container  string `json:"container"`
}

type brokerConfig struct {
	Client      coreclientset.Interface
	NodeName    string
	DataDir     string        // cxl.generic plugin data directory
	CDI         *cdiapi.Cache // nil: no CDI spec is written
	RequestBin  string        // host path of cxl-request
	Image       string        // companion image
	WaitDefault time.Duration
	PoolDriver  string // cxl-pool.generic, "" when the pool helper is disabled
}

type broker struct {
	cfg       brokerConfig
	mu        sync.Mutex
	store     *leaseStore
	lender    Lender
	listeners map[string]*net.UnixListener // token claim uid -> socket
	releasing map[string]bool              // lease id -> release goroutine runs
	deleting  map[string]bool              // lease id -> revoked, companion deletion requested
	changed   chan struct{}                // closed and replaced on every lease change
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	closeTimeout    time.Duration
	pollInterval    time.Duration
	reconcileEvery  time.Duration
	creatingGrace   time.Duration
	syncWaitTimeout time.Duration
	openDax         func(path string, readOnly bool) (*os.File, error)
	newID           func() string
}

// newBroker loads the lease store, rewrites the CDI spec and re-creates
// the sockets of the stored token claims. Requests are answered with
// errLendingUnavailable until start sets the lender.
func newBroker(cfg brokerConfig) (*broker, error) {
	if cfg.WaitDefault <= 0 {
		cfg.WaitDefault = defaultLeaseWait
	}
	if cfg.Image == "" {
		cfg.Image = defaultCompanionImage
	}
	if err := os.MkdirAll(filepath.Join(cfg.DataDir, leaseDirName), 0755); err != nil {
		return nil, fmt.Errorf("lease broker: %w", err)
	}
	store, err := loadLeaseStore(filepath.Join(cfg.DataDir, leasesFileName))
	if err != nil {
		return nil, fmt.Errorf("lease broker: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &broker{
		cfg:             cfg,
		store:           store,
		listeners:       map[string]*net.UnixListener{},
		releasing:       map[string]bool{},
		deleting:        map[string]bool{},
		changed:         make(chan struct{}),
		ctx:             ctx,
		cancel:          cancel,
		closeTimeout:    leaseCloseTimeout,
		pollInterval:    500 * time.Millisecond,
		reconcileEvery:  leaseReconcileEvery,
		creatingGrace:   leaseCreatingGrace,
		syncWaitTimeout: 30 * time.Second,
		openDax:         openDaxDevice,
		newID:           randomLeaseID,
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.writeCDISpec(); err != nil {
		cancel()
		return nil, fmt.Errorf("lease broker: %w", err)
	}
	for uid := range store.Claims {
		if err := b.listen(uid); err != nil {
			klog.Errorf("lease broker: token claim %s: %v", uid, err)
		}
	}
	klog.Infof("lease broker: loaded %d token claims and %d leases from %s", len(store.Claims), len(store.Leases), store.path)
	return b, nil
}

func randomLeaseID() string {
	var buf [4]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}

// start sets the lender (nil: NRI is not connected, lease requests fail),
// registers the container callbacks and starts the reconcile loop.
func (b *broker) start(lender Lender) {
	b.mu.Lock()
	b.lender = lender
	b.mu.Unlock()
	if lender != nil {
		lender.OnContainerGone(b.containerGone)
		lender.OnContainerStarted(b.containerStarted)
	}
	b.wg.Add(1)
	go b.reconcileLoop(lender)
}

// stop closes the sockets and stops the background work. Leases are
// kept: a restarted plugin continues with them.
func (b *broker) stop() {
	if b == nil {
		return
	}
	b.cancel()
	b.mu.Lock()
	for uid, l := range b.listeners {
		_ = l.Close()
		delete(b.listeners, uid)
	}
	b.mu.Unlock()
	b.wg.Wait()
}

// notify wakes up the waiters of lease changes. Called with b.mu held.
func (b *broker) notify() {
	close(b.changed)
	b.changed = make(chan struct{})
}

// waitFor polls cond on every lease change and every poll interval
// until it is true or ctx is done.
func (b *broker) waitFor(ctx context.Context, cond func() bool) error {
	for {
		b.mu.Lock()
		ch := b.changed
		b.mu.Unlock()
		if cond() {
			return nil
		}
		t := time.NewTimer(b.pollInterval)
		select {
		case <-ch:
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			if cond() {
				return nil
			}
			return ctx.Err()
		}
		t.Stop()
	}
}

// saveLocked persists the store and wakes up waiters. Called with b.mu
// held; a write error is logged (the in-memory state stays authoritative).
func (b *broker) saveLocked() {
	if err := b.store.save(); err != nil {
		klog.Errorf("lease broker: %v", err)
	}
	b.notify()
}

// accept serves a token claim's socket.
func (b *broker) accept(claimUID string, l *net.UnixListener) {
	defer b.wg.Done()
	for {
		c, err := l.AcceptUnix()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				klog.Errorf("lease broker: claim %s: accept: %v", claimUID, err)
			}
			return
		}
		go b.serveConn(claimUID, c)
	}
}

// peerPid returns the pid of the process that connected.
func peerPid(c *net.UnixConn) (int, error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return 0, err
	}
	var cred *syscall.Ucred
	var credErr error
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return 0, err
	}
	if credErr != nil {
		return 0, credErr
	}
	return int(cred.Pid), nil
}

// serveConn answers the request lines of one connection.
func (b *broker) serveConn(claimUID string, c *net.UnixConn) {
	defer c.Close()
	ref, authErr := b.authenticate(claimUID, c)
	if authErr != nil {
		klog.Warningf("lease broker: claim %s: rejected connection: %v", claimUID, authErr)
	}
	r := bufio.NewReaderSize(c, 4096)
	for {
		line, err := readLine(r)
		if err != nil {
			return
		}
		var req leaseRequest
		var resp *leaseResponse
		var fd daxFd
		switch {
		case json.Unmarshal(line, &req) != nil:
			resp = &leaseResponse{Error: "invalid request: not a JSON object"}
		case authErr != nil:
			resp = &leaseResponse{Error: authErr.Error()}
		default:
			resp, fd = b.handle(claimUID, ref, &req)
		}
		if err := b.respond(c, resp, fd); err != nil {
			klog.Warningf("lease broker: claim %s: response to %s/%s: %v", claimUID, ref.PodName, ref.Name, err)
			return
		}
	}
}

// readLine reads one request line of at most maxRequestLine bytes.
func readLine(r *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		chunk, isPrefix, err := r.ReadLine()
		if err != nil {
			return nil, err
		}
		line = append(line, chunk...)
		if len(line) > maxRequestLine {
			return nil, errors.New("request line too long")
		}
		if !isPrefix {
			return line, nil
		}
	}
}

// respond writes a response line, with the open dax device of fd as
// SCM_RIGHTS if it names one.
func (b *broker) respond(c *net.UnixConn, resp *leaseResponse, fd daxFd) error {
	if fd.path != "" {
		f, err := b.openDax(fd.path, fd.readOnly)
		if err != nil {
			resp = &leaseResponse{Lease: resp.Lease, State: resp.State, Kind: resp.Kind, Error: fmt.Sprintf("opening %s: %v", fd.path, err)}
		} else {
			defer f.Close()
			data, err := json.Marshal(resp)
			if err != nil {
				return err
			}
			data = append(data, '\n')
			n, oobn, err := c.WriteMsgUnix(data, syscall.UnixRights(int(f.Fd())), nil)
			if err != nil {
				return err
			}
			if oobn == 0 {
				return errors.New("fd not sent")
			}
			if n < len(data) {
				_, err = c.Write(data[n:])
			}
			return err
		}
	}
	data, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	_, err = c.Write(append(data, '\n'))
	return err
}

// authenticate maps the peer to a container of the token claim's pod.
func (b *broker) authenticate(claimUID string, c *net.UnixConn) (containerRef, error) {
	b.mu.Lock()
	lender := b.lender
	tc := b.store.Claims[claimUID]
	b.mu.Unlock()
	if lender == nil {
		return containerRef{}, errLendingUnavailable
	}
	if tc == nil {
		return containerRef{}, errNotOurPod
	}
	pid, err := peerPid(c)
	if err != nil {
		return containerRef{}, fmt.Errorf("peer credentials: %w", err)
	}
	ref, ok := lender.ContainerByPid(pid)
	if !ok {
		return containerRef{}, fmt.Errorf("%w (pid %d is in no known container)", errNotOurPod, pid)
	}
	if !tc.hasPod(ref.PodUID) && !b.refreshClaimPods(tc, ref.PodUID) {
		return containerRef{}, fmt.Errorf("%w (pid %d is in %s/%s/%s)", errNotOurPod, pid, ref.PodNamespace, ref.PodName, ref.Name)
	}
	return *ref, nil
}

// refreshClaimPods re-reads the reservedFor of a token claim (a claim
// shared by several pods is prepared only once) and reports whether the
// pod is in it now.
func (b *broker) refreshClaimPods(tc *tokenClaim, podUID string) bool {
	if b.cfg.Client == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(b.ctx, apiTimeout)
	defer cancel()
	claim, err := b.cfg.Client.ResourceV1().ResourceClaims(tc.Namespace).Get(ctx, tc.Name, metav1.GetOptions{})
	if err != nil || string(claim.UID) != tc.UID {
		return false
	}
	pods := reservedPods(claim)
	b.mu.Lock()
	defer b.mu.Unlock()
	if cur := b.store.Claims[tc.UID]; cur != nil {
		cur.Pods = pods
		b.saveLocked()
		return cur.hasPod(podUID)
	}
	return false
}

// handle runs one authenticated request. It returns the response and the
// dax device whose fd goes with it.
func (b *broker) handle(claimUID string, ref containerRef, req *leaseRequest) (*leaseResponse, daxFd) {
	switch req.Op {
	case "lease":
		return b.requestLease(claimUID, ref, req)
	case "release":
		if req.All {
			return b.releaseAll(ref, req), daxFd{}
		}
		return b.releaseOne(ref, req), daxFd{}
	case "list":
		return b.list(ref), daxFd{}
	case "fd":
		return b.resendFd(ref, req)
	default:
		return &leaseResponse{Error: fmt.Sprintf("unknown op %q (lease, release, list, fd)", req.Op)}, daxFd{}
	}
}

func (b *broker) parseWait(s string) (time.Duration, error) {
	if s == "" {
		return b.cfg.WaitDefault, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("invalid wait %q", s)
	}
	return d, nil
}

// leaseOptions checks the kind and options of a request and sets them on
// the lease: a memory lease comes from a source and is numa (lent as a
// NUMA node) or dax, a shared lease is a dax pool lease and may be
// exclusive, and read-only is for dax leases.
func (b *broker) leaseOptions(l *lease, req *leaseRequest) error {
	var err error
	switch req.Kind {
	case leaseKindMemory:
		err = b.memoryLeaseOptions(l, req)
	case leaseKindShared:
		err = b.sharedLeaseOptions(l, req)
	default:
		err = fmt.Errorf("invalid kind %q (memory or shared)", req.Kind)
	}
	if err != nil {
		return err
	}
	if req.ReadOnly && l.Mode != device.ModeDax {
		return fmt.Errorf("readOnly needs a dax lease (shared, or memory with mode %s)", device.ModeDax)
	}
	l.ReadOnly = req.ReadOnly
	return nil
}

// memoryLeaseOptions sets the source, size, mode and the optional serial
// of a memory lease.
func (b *broker) memoryLeaseOptions(l *lease, req *leaseRequest) error {
	switch req.Source {
	case "", device.SourcePool:
		if b.cfg.PoolDriver == "" {
			return errPoolDisabled
		}
		l.Source = device.SourcePool
	case device.SourceLocal, leaseSourceAny:
		l.Source = req.Source
	default:
		return fmt.Errorf("invalid source %q (%s, %s or %s)", req.Source, device.SourceLocal, device.SourcePool, leaseSourceAny)
	}
	q, err := resource.ParseQuantity(req.Size)
	if err != nil || q.Sign() <= 0 {
		return fmt.Errorf("invalid size %q", req.Size)
	}
	l.Size = q.String()
	switch req.Mode {
	case "", device.ModeNUMA:
		l.Mode = device.ModeNUMA
	case device.ModeDax:
		l.Mode = device.ModeDax
	default:
		return fmt.Errorf("invalid mode %q (%s or %s)", req.Mode, device.ModeNUMA, device.ModeDax)
	}
	if req.Exclusive {
		return errors.New("exclusive is for shared leases (a memory lease is exclusive)")
	}
	if req.Serial != "" {
		if l.Serial, err = normalizeSerial(req.Serial); err != nil {
			return err
		}
	}
	return nil
}

// sharedLeaseOptions sets the serial of a shared lease, and the hosts
// capacity to request if it is exclusive.
func (b *broker) sharedLeaseOptions(l *lease, req *leaseRequest) error {
	if req.Source != "" && req.Source != device.SourcePool {
		return fmt.Errorf("source %s: a shared lease is a %s lease (a local slice is shared through a ResourceClaim)", req.Source, device.SourcePool)
	}
	if b.cfg.PoolDriver == "" {
		return errPoolDisabled
	}
	l.Source = device.SourcePool
	serial, err := normalizeSerial(req.Serial)
	if err != nil {
		return err
	}
	l.Serial = serial
	if req.Mode != "" && req.Mode != device.ModeDax {
		return fmt.Errorf("mode %s: a shared lease is %s only", req.Mode, device.ModeDax)
	}
	l.Mode = device.ModeDax
	if req.Exclusive {
		hosts, err := b.sharedHosts(serial)
		if err != nil {
			return err
		}
		l.Exclusive, l.Hosts = true, hosts.String()
	}
	return nil
}

// sharedHosts returns the "hosts" capacity of the pool device with the
// serial, from the pool controller's ResourceSlices: an exclusive lease of
// a shared device requests all of it.
func (b *broker) sharedHosts(serial string) (resource.Quantity, error) {
	if b.cfg.Client == nil {
		return resource.Quantity{}, errors.New("no Kubernetes client")
	}
	ctx, cancel := context.WithTimeout(b.ctx, apiTimeout)
	defer cancel()
	list, err := b.cfg.Client.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{})
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("listing the ResourceSlices of %s: %w", b.cfg.PoolDriver, err)
	}
	for i := range list.Items {
		rs := &list.Items[i]
		if rs.Spec.Driver != b.cfg.PoolDriver {
			continue
		}
		for j := range rs.Spec.Devices {
			d := &rs.Spec.Devices[j]
			if deviceSerial(d, b.cfg.PoolDriver) != serial {
				continue
			}
			c, ok := d.Capacity[poolHostsCapacity]
			if !ok {
				return resource.Quantity{}, fmt.Errorf("pool device %s (serial %s) has no %q capacity: not a shared device", d.Name, serial, poolHostsCapacity)
			}
			return c.Value, nil
		}
	}
	return resource.Quantity{}, fmt.Errorf("no pool device with serial %s in the ResourceSlices of %s", serial, b.cfg.PoolDriver)
}

// deviceSerial returns the normalized serial attribute of a pool device,
// "" if it has none.
func deviceSerial(d *resourceapi.Device, driver string) string {
	for _, k := range []resourceapi.QualifiedName{"serial", resourceapi.QualifiedName(driver + "/serial")} {
		if a, ok := d.Attributes[k]; ok && a.StringValue != nil {
			if s, err := normalizeSerial(*a.StringValue); err == nil {
				return s
			}
		}
	}
	return ""
}

// normalizeSerial returns the serial as 0x<lower-case hex>, the format of
// the pool slice's serial attribute.
func normalizeSerial(s string) (string, error) {
	v, err := strconv.ParseUint(s, 0, 64)
	if err != nil {
		return "", fmt.Errorf("invalid serial %q", s)
	}
	return fmt.Sprintf("0x%x", v), nil
}

// leaseInfo is the response of a lease. Called with b.mu held.
func leaseInfo(l *lease) *leaseResponse {
	r := &leaseResponse{Lease: l.ID, State: l.State, Kind: l.Kind, Source: l.source(), Mode: l.Mode, Exclusive: l.Exclusive, ReadOnly: l.ReadOnly,
		Serial: l.Serial, Dax: l.Dax, Error: l.Error, Size: l.Bytes}
	if l.Node >= 0 {
		n := l.Node
		r.Node = &n
	}
	if r.Size == 0 && l.Size != "" {
		if q, err := resource.ParseQuantity(l.Size); err == nil {
			r.Size = uint64(q.Value())
		}
	}
	return r
}

// fdOf returns the dax device whose fd goes with the response of a lease:
// that of a Ready dax lease.
func fdOf(l *lease) daxFd {
	if l.State == leaseReady && l.Dax != "" {
		return daxFd{path: l.Dax, readOnly: l.ReadOnly}
	}
	return daxFd{}
}

func (b *broker) requestLease(claimUID string, ref containerRef, req *leaseRequest) (*leaseResponse, daxFd) {
	wait, err := b.parseWait(req.Wait)
	if err != nil {
		return &leaseResponse{Error: err.Error()}, daxFd{}
	}
	l := &lease{
		TokenClaim: claimUID,
		RequestID:  req.RequestID,
		Kind:       req.Kind,
		Pod:        podRef{Namespace: ref.PodNamespace, Name: ref.PodName, UID: ref.PodUID},
		Container:  ref.Name,
		State:      leaseCreating,
		Node:       -1,
		Created:    time.Now().UTC(),
	}
	if err := b.leaseOptions(l, req); err != nil {
		return &leaseResponse{Error: err.Error()}, daxFd{}
	}

	b.mu.Lock()
	if req.RequestID != "" {
		for _, old := range b.store.Leases {
			if old.RequestID == req.RequestID && old.Pod.UID == ref.PodUID && old.Container == ref.Name {
				id := old.ID
				b.mu.Unlock()
				return b.waitReady(id, wait)
			}
		}
	}
	for l.ID == "" || b.store.Leases[l.ID] != nil {
		l.ID = b.newID()
	}
	b.store.Leases[l.ID] = l
	if err := b.store.save(); err != nil {
		delete(b.store.Leases, l.ID)
		b.mu.Unlock()
		return &leaseResponse{Error: err.Error()}, daxFd{}
	}
	b.notify()
	b.mu.Unlock()
	klog.Infof("lease broker: lease %s: %s/%s/%s requests %s %s%s (source %s, mode %s, exclusive %v, read-only %v)",
		l.ID, l.Pod.Namespace, l.Pod.Name, l.Container, l.Kind, l.Size, l.Serial, l.Source, l.Mode, l.Exclusive, l.ReadOnly)

	if err := b.createCompanion(l); err != nil {
		klog.Errorf("lease broker: lease %s: %v", l.ID, err)
		b.mu.Lock()
		if cur := b.store.Leases[l.ID]; cur != nil {
			cur.State, cur.Error = leaseFailed, err.Error()
		}
		b.mu.Unlock()
		resp := b.startRelease(l.ID, err.Error())
		resp.State, resp.Error = leaseFailed, err.Error()
		return resp, daxFd{}
	}
	b.mu.Lock()
	if cur := b.store.Leases[l.ID]; cur != nil && cur.State == leaseCreating {
		cur.State = leasePending
		b.saveLocked()
	}
	b.mu.Unlock()
	return b.waitReady(l.ID, wait)
}

// waitReady waits until the lease is Ready or Failed. At timeout the
// lease is released and reported Failed. wait 0 returns the state at
// once.
func (b *broker) waitReady(id string, wait time.Duration) (*leaseResponse, daxFd) {
	ctx, cancel := context.WithTimeout(b.ctx, wait)
	defer cancel()
	var resp *leaseResponse
	var fd daxFd
	done := func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		l := b.store.Leases[id]
		switch {
		case l == nil:
			resp = &leaseResponse{Lease: id, State: leaseReleased, Error: "lease was released"}
			return true
		case l.State == leaseReady || l.State == leaseFailed || l.State == leaseReleasing:
			resp, fd = leaseInfo(l), fdOf(l)
			return true
		}
		resp = leaseInfo(l)
		return wait == 0
	}
	if err := b.waitFor(ctx, done); err != nil {
		msg := fmt.Sprintf("timeout: not Ready after %v", wait)
		klog.Warningf("lease broker: lease %s: %s, releasing", id, msg)
		resp = b.startRelease(id, msg)
		resp.State, resp.Error = leaseFailed, msg
		return resp, daxFd{}
	}
	return resp, fd
}

// ownLease returns the lease if it belongs to the pod of ref. Called
// with b.mu held.
func (b *broker) ownLease(ref containerRef, id string) *lease {
	l := b.store.Leases[id]
	if l == nil || l.Pod.UID != ref.PodUID {
		return nil
	}
	return l
}

func (b *broker) releaseOne(ref containerRef, req *leaseRequest) *leaseResponse {
	wait, err := b.parseWait(req.Wait)
	if err != nil {
		return &leaseResponse{Error: err.Error()}
	}
	b.mu.Lock()
	l := b.ownLease(ref, req.Lease)
	b.mu.Unlock()
	if l == nil {
		return &leaseResponse{Lease: req.Lease, Error: "no such lease"}
	}
	b.startRelease(req.Lease, "released by "+ref.Name)
	return b.waitReleased([]string{req.Lease}, wait)[0]
}

func (b *broker) releaseAll(ref containerRef, req *leaseRequest) *leaseResponse {
	wait, err := b.parseWait(req.Wait)
	if err != nil {
		return &leaseResponse{Error: err.Error()}
	}
	var ids []string
	b.mu.Lock()
	for id, l := range b.store.Leases {
		if l.Pod.UID == ref.PodUID {
			ids = append(ids, id)
		}
	}
	b.mu.Unlock()
	sort.Strings(ids)
	for _, id := range ids {
		b.startRelease(id, "released by "+ref.Name)
	}
	out := []leaseResponse{}
	for _, r := range b.waitReleased(ids, wait) {
		out = append(out, *r)
	}
	return &leaseResponse{Leases: &out}
}

// waitReleased waits until the leases are gone, at most wait, and
// returns their states (Released, or the current state).
func (b *broker) waitReleased(ids []string, wait time.Duration) []*leaseResponse {
	ctx, cancel := context.WithTimeout(b.ctx, wait)
	defer cancel()
	gone := func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		for _, id := range ids {
			if b.store.Leases[id] != nil {
				return false
			}
		}
		return true
	}
	_ = b.waitFor(ctx, gone)
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]*leaseResponse, 0, len(ids))
	for _, id := range ids {
		if l := b.store.Leases[id]; l != nil {
			out = append(out, leaseInfo(l))
		} else {
			out = append(out, &leaseResponse{Lease: id, State: leaseReleased})
		}
	}
	return out
}

func (b *broker) list(ref containerRef) *leaseResponse {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := []leaseResponse{}
	for _, l := range b.store.Leases {
		if l.Pod.UID == ref.PodUID {
			out = append(out, *leaseInfo(l))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Lease < out[j].Lease })
	return &leaseResponse{Leases: &out}
}

func (b *broker) resendFd(ref containerRef, req *leaseRequest) (*leaseResponse, daxFd) {
	b.mu.Lock()
	defer b.mu.Unlock()
	l := b.ownLease(ref, req.Lease)
	if l == nil || l.Container != ref.Name {
		return &leaseResponse{Lease: req.Lease, Error: "no such lease of this container"}, daxFd{}
	}
	resp := leaseInfo(l)
	fd := fdOf(l)
	if fd.path == "" {
		resp.Error = "not a Ready dax lease"
	}
	return resp, fd
}

// companionObjects returns the companion claim and pod of a lease.
func (b *broker) companionObjects(l *lease) (*resourceapi.ResourceClaim, *corev1.Pod, error) {
	name := companionName(l.ID)
	labels := map[string]string{
		leaseLabel:        l.ID,
		requesterUIDLabel: l.Pod.UID,
		leaseNodeLabel:    b.cfg.NodeName,
	}
	annotations := map[string]string{
		requesterAnnotation: l.Pod.Namespace + "/" + l.Pod.Name + "/" + l.Container,
	}
	owner := metav1.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: l.Pod.Name, UID: types.UID(l.Pod.UID)}
	claimOwner := owner
	claimOwner.Controller = ptr(true)

	req, configs, err := b.companionRequest(l)
	if err != nil {
		return nil, nil, err
	}
	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       l.Pod.Namespace,
			Labels:          labels,
			Annotations:     annotations,
			OwnerReferences: []metav1.OwnerReference{claimOwner},
		},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Requests: []resourceapi.DeviceRequest{*req},
				Config:   configs,
			},
		},
	}
	// Guaranteed QoS keeps the companion from being evicted first. The
	// CPU limit is 100m, not 1m: with a 1m limit a pause pod needs ~12 s to
	// reach Running on the e2e VMs (CFS throttling of the container start),
	// which delays the release of a lease whose companion has not started
	// yet; with 100m it takes ~2 s (plan-3-cxl-live-attach, W2 finding 3).
	// The memory limit leaves room for runc init, which needs about 9 MiB
	// in the container's cgroup.
	res := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("100m"),
		corev1.ResourceMemory: resource.MustParse("16Mi"),
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       l.Pod.Namespace,
			Labels:          labels,
			Annotations:     annotations,
			OwnerReferences: []metav1.OwnerReference{owner},
		},
		Spec: corev1.PodSpec{
			Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchFields: []corev1.NodeSelectorRequirement{{
							Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{b.cfg.NodeName},
						}},
					}},
				},
			}},
			Tolerations:                   []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
			RestartPolicy:                 corev1.RestartPolicyAlways,
			AutomountServiceAccountToken:  ptr(false),
			EnableServiceLinks:            ptr(false),
			TerminationGracePeriodSeconds: ptr(int64(1)),
			ResourceClaims:                []corev1.PodResourceClaim{{Name: companionClaimRef, ResourceClaimName: ptr(name)}},
			Containers: []corev1.Container{{
				Name:      companionContainer,
				Image:     b.cfg.Image,
				Resources: corev1.ResourceRequirements{Requests: res, Limits: res.DeepCopy()},
			}},
		},
	}
	return claim, pod, nil
}

// companionSources returns the sources of the devices that the companion
// claim of a lease accepts, the preferred one first.
func (b *broker) companionSources(l *lease) []string {
	switch l.Source {
	case device.SourceLocal:
		return []string{device.SourceLocal}
	case leaseSourceAny:
		if b.cfg.PoolDriver == "" {
			return []string{device.SourceLocal}
		}
		return []string{device.SourceLocal, device.SourcePool}
	}
	return []string{device.SourcePool}
}

// companionRequest returns the device request and the opaque configs of
// the companion claim of a lease. A lease with one source gets an exact
// request; with more, a request with one subrequest per source in order
// of preference (firstAvailable), and configs that apply to the
// subrequest of their driver only.
func (b *broker) companionRequest(l *lease) (*resourceapi.DeviceRequest, []resourceapi.DeviceClaimConfiguration, error) {
	sources := b.companionSources(l)
	req := &resourceapi.DeviceRequest{Name: companionClaimRef}
	var configs []resourceapi.DeviceClaimConfiguration
	for _, source := range sources {
		exact, driver, objs, err := b.companionDevices(l, source)
		if err != nil {
			return nil, nil, err
		}
		var requests []string
		if len(sources) == 1 {
			req.Exactly = exact
		} else {
			req.FirstAvailable = append(req.FirstAvailable, resourceapi.DeviceSubRequest{
				Name:            source,
				DeviceClassName: exact.DeviceClassName,
				Selectors:       exact.Selectors,
				Capacity:        exact.Capacity,
			})
			requests = []string{companionClaimRef + "/" + source}
		}
		for _, obj := range objs {
			params, err := json.Marshal(obj)
			if err != nil {
				return nil, nil, err
			}
			configs = append(configs, resourceapi.DeviceClaimConfiguration{
				Requests: requests,
				DeviceConfiguration: resourceapi.DeviceConfiguration{
					Opaque: &resourceapi.OpaqueDeviceConfiguration{
						Driver:     driver,
						Parameters: runtime.RawExtension{Raw: params},
					},
				},
			})
		}
	}
	return req, configs, nil
}

// companionDevices returns what the companion claim of a lease requests
// from one source: the device class, selectors and capacity, the driver
// of the devices, and its opaque configs: the LendConfig, and for a dax
// lease the device config with the mode and read-only setting.
func (b *broker) companionDevices(l *lease, source string) (*resourceapi.ExactDeviceRequest, string, []any, error) {
	driver, configKind := b.cfg.PoolDriver, device.PoolDeviceConfigKind
	exact := &resourceapi.ExactDeviceRequest{}
	switch {
	case source == device.SourceLocal:
		driver, configKind = device.DriverName, device.LocalDeviceConfigKind
		exact.DeviceClassName = localMemoryClass
		if l.Mode == device.ModeDax {
			exact.DeviceClassName = localDaxClass
		}
	case b.cfg.PoolDriver == "":
		return nil, "", nil, errPoolDisabled
	case l.Kind == leaseKindShared:
		exact.DeviceClassName = poolSharedClass
	default:
		exact.DeviceClassName = poolMemoryClass
	}
	switch l.Kind {
	case leaseKindMemory:
		q, err := resource.ParseQuantity(l.Size)
		if err != nil {
			return nil, "", nil, err
		}
		exact.Capacity = &resourceapi.CapacityRequirements{
			Requests: map[resourceapi.QualifiedName]resource.Quantity{device.CapacityMemory: q},
		}
	case leaseKindShared:
		if l.Exclusive {
			hosts, err := resource.ParseQuantity(l.Hosts)
			if err != nil {
				return nil, "", nil, fmt.Errorf("hosts %q: %w", l.Hosts, err)
			}
			exact.Capacity = &resourceapi.CapacityRequirements{
				Requests: map[resourceapi.QualifiedName]resource.Quantity{poolHostsCapacity: hosts},
			}
		}
	}
	if l.Serial != "" {
		exact.Selectors = []resourceapi.DeviceSelector{{
			CEL: &resourceapi.CELDeviceSelector{
				Expression: fmt.Sprintf(`device.attributes[%q].serial == %q`, driver, l.Serial),
			},
		}}
	}
	objs := []any{&lendConfig{
		APIVersion: driver + "/v1alpha1",
		Kind:       device.LendConfigKind,
		Lease:      l.ID,
		Pod:        l.Pod,
		Container:  l.Container,
	}}
	if l.Mode == device.ModeDax {
		objs = append(objs, &deviceConfig{
			APIVersion: driver + "/v1alpha1",
			Kind:       configKind,
			Mode:       device.ModeDax,
			ReadOnly:   l.ReadOnly,
		})
	}
	return exact, driver, objs, nil
}

// createCompanion creates the claim, then the pod of a lease.
func (b *broker) createCompanion(l *lease) error {
	if b.cfg.Client == nil {
		return errors.New("no Kubernetes client")
	}
	claim, pod, err := b.companionObjects(l)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(b.ctx, apiTimeout)
	defer cancel()
	if _, err := b.cfg.Client.ResourceV1().ResourceClaims(claim.Namespace).Create(ctx, claim, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("creating companion claim %s/%s: %w", claim.Namespace, claim.Name, err)
	}
	if _, err := b.cfg.Client.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("creating companion pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	klog.Infof("lease broker: lease %s: created companion claim and pod %s/%s", l.ID, pod.Namespace, pod.Name)
	return nil
}

// startRelease starts releasing a lease (once) and returns its state.
func (b *broker) startRelease(id, reason string) *leaseResponse {
	b.mu.Lock()
	defer b.mu.Unlock()
	l := b.store.Leases[id]
	if l == nil {
		return &leaseResponse{Lease: id, State: leaseReleased}
	}
	if !b.releasing[id] {
		b.releasing[id] = true
		klog.Infof("lease broker: lease %s: releasing (%s, was %s)", id, reason, l.State)
		if l.State != leaseFailed { // a failed lease shows why until it is gone
			l.State = leaseReleasing
		}
		b.saveLocked()
		cp := *l
		b.wg.Add(1)
		go b.release(&cp)
	}
	return leaseInfo(l)
}

// release revokes the lend, deletes the companion pod and claim, waits
// until both are gone and drops the lease. Kubelet unprepares the
// companion's claim in between (its driver releases the device). The
// lease is marked deleting once the deletion of both is requested.
func (b *broker) release(l *lease) {
	defer b.wg.Done()
	b.mu.Lock()
	lender := b.lender
	b.mu.Unlock()
	if l.Kind == leaseKindMemory && l.Node >= 0 && lender != nil {
		ctx, cancel := context.WithTimeout(b.ctx, apiTimeout)
		if err := lender.RevokeNode(ctx, l.Pod.UID, l.Container, l.Node, l.ID); err != nil {
			// The driver of the device revokes again before it
			// releases it, and fails (kubelet retries) while it cannot.
			klog.Errorf("lease broker: lease %s: revoking node %d: %v", l.ID, l.Node, err)
		}
		cancel()
	}
	name := companionName(l.ID)
	ns := l.Pod.Namespace
	delay := b.pollInterval
	for {
		gone, err := b.deleteCompanion(ns, name)
		if err != nil {
			klog.Warningf("lease broker: lease %s: %v", l.ID, err)
		} else {
			b.mu.Lock()
			if !b.deleting[l.ID] {
				b.deleting[l.ID] = true
				b.notify()
			}
			b.mu.Unlock()
		}
		if gone {
			break
		}
		select {
		case <-b.ctx.Done():
			return // continued by the next plugin instance
		case <-time.After(delay):
		}
		if delay < 5*time.Second {
			delay *= 2
		}
	}
	b.mu.Lock()
	delete(b.store.Leases, l.ID)
	delete(b.releasing, l.ID)
	delete(b.deleting, l.ID)
	b.saveLocked()
	b.mu.Unlock()
	klog.Infof("lease broker: lease %s: released", l.ID)
}

// deleteCompanion deletes the companion pod and claim and reports
// whether both are gone.
func (b *broker) deleteCompanion(ns, name string) (bool, error) {
	if b.cfg.Client == nil {
		return true, nil
	}
	ctx, cancel := context.WithTimeout(b.ctx, apiTimeout)
	defer cancel()
	fg := metav1.DeletePropagationForeground
	opts := metav1.DeleteOptions{PropagationPolicy: &fg}
	var errs []error
	gone := true
	pod, err := b.cfg.Client.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		gone = false
		errs = append(errs, err)
	default:
		gone = false
		if pod.DeletionTimestamp == nil {
			if err := b.cfg.Client.CoreV1().Pods(ns).Delete(ctx, name, opts); err != nil && !apierrors.IsNotFound(err) {
				errs = append(errs, fmt.Errorf("deleting companion pod %s/%s: %w", ns, name, err))
			}
		}
	}
	claim, err := b.cfg.Client.ResourceV1().ResourceClaims(ns).Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		gone = false
		errs = append(errs, err)
	default:
		gone = false
		if claim.DeletionTimestamp == nil {
			if err := b.cfg.Client.ResourceV1().ResourceClaims(ns).Delete(ctx, name, opts); err != nil && !apierrors.IsNotFound(err) {
				errs = append(errs, fmt.Errorf("deleting companion claim %s/%s: %w", ns, name, err))
			}
		}
	}
	return gone, errors.Join(errs...)
}

// requesterDone reports whether the leases of a container that is not
// running can be released: the pod is gone, being deleted or finished,
// or does not restart its containers. A container that restarts keeps
// its leases (exclusive lends are re-applied when it starts).
func (b *broker) requesterDone(pod podRef) bool {
	if b.cfg.Client == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(b.ctx, apiTimeout)
	defer cancel()
	p, err := b.cfg.Client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return true
	case err != nil:
		return false
	}
	return string(p.UID) != pod.UID || p.DeletionTimestamp != nil ||
		p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed ||
		p.Spec.RestartPolicy == corev1.RestartPolicyNever
}

// containerGone is the OnContainerGone callback: the leases of a
// container that will not come back are released.
func (b *broker) containerGone(ref containerRef) {
	b.mu.Lock()
	var ids []string
	var pod podRef
	for id, l := range b.store.Leases {
		if l.Pod.UID == ref.PodUID && l.Container == ref.Name {
			ids = append(ids, id)
			pod = l.Pod
		}
	}
	lender := b.lender
	b.mu.Unlock()
	if len(ids) == 0 {
		return
	}
	if lender != nil {
		if _, ok := lender.Container(ref.PodUID, ref.Name); ok {
			return // a new instance is already there
		}
	}
	if !b.requesterDone(pod) {
		klog.Infof("lease broker: container %s/%s/%s stopped, keeping its %d leases for its restart", ref.PodNamespace, ref.PodName, ref.Name, len(ids))
		return
	}
	for _, id := range ids {
		b.startRelease(id, "requester container gone")
	}
}

// containerStarted is the OnContainerStarted callback: the exclusive
// lends of Ready leases are re-applied to a restarted container.
func (b *broker) containerStarted(ref containerRef) {
	b.relendContainer(ref.PodUID, ref.Name)
}

// relendContainer applies LendNode for the Ready memory leases of a
// container (idempotent).
func (b *broker) relendContainer(podUID, name string) {
	b.mu.Lock()
	lender := b.lender
	var nodes []int
	var ids []string
	for id, l := range b.store.Leases {
		if l.Pod.UID == podUID && l.Container == name && l.Kind == leaseKindMemory && l.State == leaseReady && l.Node >= 0 {
			nodes = append(nodes, l.Node)
			ids = append(ids, id)
		}
	}
	b.mu.Unlock()
	if lender == nil {
		return
	}
	for i, node := range nodes {
		ctx, cancel := context.WithTimeout(b.ctx, apiTimeout)
		if err := lender.LendNode(ctx, podUID, name, node, ids[i]); err != nil {
			klog.Errorf("lease broker: lease %s: re-applying node %d: %v", ids[i], node, err)
		}
		cancel()
	}
}

// reconcileLoop reconciles at start (once NRI is synchronized) and
// periodically.
func (b *broker) reconcileLoop(lender Lender) {
	defer b.wg.Done()
	if s, ok := lender.(interface{ WaitSynchronized(context.Context) error }); ok && lender != nil {
		ctx, cancel := context.WithTimeout(b.ctx, b.syncWaitTimeout)
		if err := s.WaitSynchronized(ctx); err != nil {
			klog.Warningf("lease broker: %v; reconciling anyway", err)
		}
		cancel()
	}
	for {
		b.reconcile()
		select {
		case <-b.ctx.Done():
			return
		case <-time.After(b.reconcileEvery):
		}
	}
}

// reconcile releases leases whose requester container or companion pod
// is gone, restarts interrupted releases, re-applies exclusive lends and
// deletes companions of this node that have no lease.
func (b *broker) reconcile() {
	b.mu.Lock()
	lender := b.lender
	leases := make([]lease, 0, len(b.store.Leases))
	for _, l := range b.store.Leases {
		leases = append(leases, *l)
	}
	b.mu.Unlock()

	for _, l := range leases {
		switch l.State {
		case leaseReleasing:
			b.startRelease(l.ID, "release interrupted")
			continue
		case leaseCreating:
			if time.Since(l.Created) > b.creatingGrace {
				b.startRelease(l.ID, "left over in Creating")
			}
			continue
		}
		if gone, reason := b.companionGone(l.Pod.Namespace, companionName(l.ID)); gone {
			b.startRelease(l.ID, reason)
			continue
		}
		if lender != nil {
			if _, ok := lender.Container(l.Pod.UID, l.Container); !ok {
				if b.requesterDone(l.Pod) {
					b.startRelease(l.ID, "requester container gone")
				}
				continue
			}
		}
		if l.State == leaseReady && l.Kind == leaseKindMemory && l.Node >= 0 {
			b.relendContainer(l.Pod.UID, l.Container)
		}
	}
	b.deleteOrphans()
}

// companionGone reports whether a companion pod no longer holds its
// claim: deleted, being deleted or finished (evicted).
func (b *broker) companionGone(ns, name string) (bool, string) {
	if b.cfg.Client == nil {
		return false, ""
	}
	ctx, cancel := context.WithTimeout(b.ctx, apiTimeout)
	defer cancel()
	p, err := b.cfg.Client.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return true, "companion pod gone"
	case err != nil:
		return false, ""
	case p.DeletionTimestamp != nil:
		return true, "companion pod being deleted"
	case p.Status.Phase == corev1.PodFailed || p.Status.Phase == corev1.PodSucceeded:
		return true, "companion pod " + string(p.Status.Phase)
	}
	return false, ""
}

// deleteOrphans deletes companion pods and claims of this node that
// have no lease.
func (b *broker) deleteOrphans() {
	if b.cfg.Client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(b.ctx, apiTimeout)
	defer cancel()
	sel := metav1.ListOptions{LabelSelector: leaseNodeLabel + "=" + b.cfg.NodeName}
	known := func(id string) bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.store.Leases[id] != nil
	}
	if pods, err := b.cfg.Client.CoreV1().Pods("").List(ctx, sel); err != nil {
		klog.Warningf("lease broker: listing companion pods: %v", err)
	} else {
		for _, p := range pods.Items {
			if id := p.Labels[leaseLabel]; !known(id) && p.DeletionTimestamp == nil {
				klog.Infof("lease broker: deleting companion pod %s/%s without a lease", p.Namespace, p.Name)
				if err := b.cfg.Client.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
					klog.Warningf("lease broker: %v", err)
				}
			}
		}
	}
	if claims, err := b.cfg.Client.ResourceV1().ResourceClaims("").List(ctx, sel); err != nil {
		klog.Warningf("lease broker: listing companion claims: %v", err)
	} else {
		for _, c := range claims.Items {
			if id := c.Labels[leaseLabel]; !known(id) && c.DeletionTimestamp == nil {
				klog.Infof("lease broker: deleting companion claim %s/%s without a lease", c.Namespace, c.Name)
				if err := b.cfg.Client.ResourceV1().ResourceClaims(c.Namespace).Delete(ctx, c.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
					klog.Warningf("lease broker: %v", err)
				}
			}
		}
	}
}

// Lease hooks (leaseHooks), called by the drivers that prepare companion
// claims with their locks held: they only touch the lease table.

var _ leaseHooks = (*broker)(nil)

// OnLending checks that a LendConfig names a lease of this broker for the
// same container, and marks it Lending.
func (b *broker) OnLending(cfg *lendConfig) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	l := b.store.Leases[cfg.Lease]
	if l == nil {
		return fmt.Errorf("unknown lease %q (not requested through this node's lease broker)", cfg.Lease)
	}
	if l.Pod != cfg.Pod || l.Container != cfg.Container {
		return fmt.Errorf("lease %s belongs to %s/%s/%s, not to %s/%s/%s", l.ID,
			l.Pod.Namespace, l.Pod.Name, l.Container, cfg.Pod.Namespace, cfg.Pod.Name, cfg.Container)
	}
	switch l.State {
	case leaseCreating, leasePending:
		l.State = leaseLending
		b.saveLocked()
	case leaseLending, leaseReady:
	default:
		return fmt.Errorf("lease %s is %s", l.ID, l.State)
	}
	return nil
}

// OnPrepared marks a lease Ready with the prepared device.
func (b *broker) OnPrepared(cfg *lendConfig, devs []lentDevice) {
	b.mu.Lock()
	defer b.mu.Unlock()
	l := b.store.Leases[cfg.Lease]
	if l == nil || l.State == leaseReleasing || len(devs) == 0 {
		return
	}
	d := devs[0]
	l.Serial = "0x" + d.SerialHex
	l.Bytes = d.Size
	if d.Source != "" {
		l.Source = d.Source
	}
	if d.Mode == device.ModeDax {
		l.Node, l.Dax = -1, d.DaxDevice
	} else {
		l.Node, l.Dax = d.Node, ""
	}
	l.State, l.Error = leaseReady, ""
	b.saveLocked()
	klog.Infof("lease broker: lease %s: Ready (source %s, serial %s, node %d, dax %q, %d bytes)", l.ID, l.Source, l.Serial, l.Node, l.Dax, l.Bytes)
}

// OnLendFailed fails the lease with the lend error (for example a
// container that is memory-steered, not started or gone) and releases it:
// the waiting request returns Failed with the message.
func (b *broker) OnLendFailed(cfg *lendConfig, err error) {
	b.mu.Lock()
	l := b.store.Leases[cfg.Lease]
	if l == nil || l.State == leaseReleasing || l.State == leaseFailed {
		b.mu.Unlock()
		return
	}
	l.State, l.Error = leaseFailed, err.Error()
	b.saveLocked()
	b.mu.Unlock()
	b.startRelease(cfg.Lease, "lend failed")
}

// OnReleasing is called when the companion claim of a lease is
// unprepared. Unless the broker is releasing the lease, the companion was
// deleted or evicted behind its back: the lease fails (the requester has
// lost the memory) and the reconcile drops it.
func (b *broker) OnReleasing(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if l := b.store.Leases[id]; l != nil && l.State != leaseReleasing {
		l.State = leaseFailed
		l.Error = "companion claim unprepared (companion pod deleted or evicted)"
		b.saveLocked()
		klog.Warningf("lease broker: lease %s: %s", id, l.Error)
	}
}

// openDaxDevice opens /dev/daxX.Y for passing to a container, read-only
// (the container can map it for reading only) or read-write. The device
// node is created by udev after the device_dax driver bound the dax device,
// and a node of an earlier dax device with the same name may still carry the
// old minor number, so right after a region was set up the open can fail
// with ENOENT or ENXIO (seen once in five lend cycles). Wait up to 5 s for a
// node whose major:minor matches /sys/bus/dax/devices/<name>/dev; if udev
// is still behind after that, open a private node with the numbers from
// sysfs instead and remove it again.
func openDaxDevice(path string, readOnly bool) (*os.File, error) {
	flag := os.O_RDWR
	if readOnly {
		flag = os.O_RDONLY
	}
	name := filepath.Base(path)
	major, minor, numErr := readDaxDevNumbers(name)
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for {
		if numErr == nil {
			var st syscall.Stat_t
			if err := syscall.Stat(path, &st); err == nil {
				rdev := uint64(st.Rdev)
				if st.Mode&syscall.S_IFMT != syscall.S_IFCHR ||
					int(unixMajor(rdev)) != major || int(unixMinor(rdev)) != minor {
					lastErr = fmt.Errorf("%s is not the char device %d:%d yet", path, major, minor)
					goto wait
				}
			}
		}
		if f, err := os.OpenFile(path, flag, 0); err == nil {
			return f, nil
		} else {
			lastErr = err
			var errno syscall.Errno
			if !(errors.As(err, &errno) && (errno == syscall.ENXIO || errno == syscall.ENOENT)) && !os.IsNotExist(err) {
				return nil, err
			}
		}
	wait:
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if numErr != nil {
		return nil, fmt.Errorf("%w (and %v)", lastErr, numErr)
	}
	tmp := filepath.Join(os.TempDir(), fmt.Sprintf("cxl-lease-%s-%d", name, os.Getpid()))
	_ = os.Remove(tmp)
	dev := unixMkdev(uint32(major), uint32(minor))
	if err := syscall.Mknod(tmp, syscall.S_IFCHR|0600, int(dev)); err != nil {
		return nil, fmt.Errorf("%w; mknod %s %d:%d: %v", lastErr, tmp, major, minor, err)
	}
	defer os.Remove(tmp)
	f, err := os.OpenFile(tmp, flag, 0)
	if err != nil {
		return nil, fmt.Errorf("%w; opening private node %d:%d: %v", lastErr, major, minor, err)
	}
	klog.Warningf("lease broker: %s was not usable within 5 s (%v), opened %d:%d through a private node", path, lastErr, major, minor)
	return f, nil
}

// readDaxDevNumbers reads "major:minor" of a dax device from sysfs.
func readDaxDevNumbers(name string) (int, int, error) {
	b, err := os.ReadFile(filepath.Join("/sys/bus/dax/devices", name, "dev"))
	if err != nil {
		return 0, 0, err
	}
	var major, minor int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(b)), "%d:%d", &major, &minor); err != nil {
		return 0, 0, fmt.Errorf("parsing %s/dev %q: %w", name, strings.TrimSpace(string(b)), err)
	}
	return major, minor, nil
}

// Linux dev_t encoding (same as the kernel's MKDEV/MAJOR/MINOR for glibc).
func unixMajor(dev uint64) uint32 {
	return uint32((dev>>8)&0xfff) | uint32((dev>>32)&^0xfff)
}

func unixMinor(dev uint64) uint32 {
	return uint32(dev&0xff) | uint32((dev>>12)&^0xff)
}

func unixMkdev(major, minor uint32) uint64 {
	return uint64(major&0xfff)<<8 | uint64(minor&0xff) | uint64(major&^0xfff)<<32 | uint64(minor&^0xff)<<12
}
