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
	"bytes"
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
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
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

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/api"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"

	"github.com/containers/nri-plugins/pkg/cxl/memctl"
)

// The lease broker serves the lease sockets of lease claims. A request
// from a container creates, in the container's namespace, a ResourceClaim
// and a companion pause pod "cxl-lease-<id>" pinned to this node. The
// scheduler allocates a local memdev (cxl.generic) or a pool device to the
// claim, the pool controller attaches a pool device, and kubelet prepares
// the claim through the driver of the device, which lends the memory to
// the requesting container (leaseHooks, lending.go). Deleting the
// companion releases it again. See doc/cxl/LEASES.md.

const (
	companionClaimRef  = "lease" // pod.spec.resourceClaims[0].name and request name
	companionContainer = "pause"
	// leaseCloseTimeout bounds how long the Unprepare of a lease claim
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

var (
	errLendingUnavailable = errors.New("lending unavailable: NRI not connected")
	errNotOurPod          = errors.New("not a container of the claim's pod")
	errPoolDisabled       = errors.New("pool driver disabled (--pool-driver-name)")
)

// daxFd names the dax device whose fd goes with a response: none if path
// is "".
type daxFd struct {
	path     string
	readOnly bool
}

type brokerConfig struct {
	Client      coreclientset.Interface
	NodeName    string
	DataDir     string        // cxl.generic plugin data directory
	CDI         *cdiapi.Cache // nil: no CDI spec is written
	CLI         string        // host path of cxl-request
	Image       string        // companion image
	WaitDefault time.Duration
	PoolDriver  string // cxl-pool.generic, "" when the pool driver is disabled
}

type broker struct {
	cfg       brokerConfig
	mu        sync.Mutex
	store     *leaseStore
	lender    nodeLender
	listeners map[string]*net.UnixListener // lease claim uid -> socket
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
// the sockets of the stored lease claims. Requests are answered with
// errLendingUnavailable until start sets the lender.
func newBroker(cfg brokerConfig) (*broker, error) {
	if cfg.WaitDefault <= 0 {
		cfg.WaitDefault = defaultLeaseWait
	}
	if cfg.Image == "" {
		cfg.Image = defaultLeaseCompanionImage
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
			klog.Errorf("lease broker: lease claim %s: %v", uid, err)
		}
	}
	klog.Infof("lease broker: loaded %d lease claims and %d leases from %s", len(store.Claims), len(store.Leases), store.path)
	return b, nil
}

func randomLeaseID() string {
	var buf [4]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}

// start sets the lender (nil: NRI is not connected, lease requests fail),
// registers the container callbacks and starts the reconcile loop.
func (b *broker) start(lender nodeLender) {
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

// accept serves a lease claim's socket.
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
		var resp *api.Response
		var fd daxFd
		req, err := decodeRequest(line)
		switch {
		case err != nil:
			resp = &api.Response{Error: err.Error()}
		case authErr != nil:
			resp = &api.Response{Error: authErr.Error()}
		default:
			resp, fd = b.handle(claimUID, ref, req)
		}
		if err := b.respond(c, resp, fd); err != nil {
			klog.Warningf("lease broker: claim %s: response to %s/%s: %v", claimUID, ref.PodName, ref.Name, err)
			return
		}
	}
}

// decodeRequest returns the request of a line, decoded strictly.
func decodeRequest(line []byte) (*api.Request, error) {
	req := &api.Request{}
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.DisallowUnknownFields()
	if err := dec.Decode(req); err != nil {
		return nil, fmt.Errorf("invalid request: %v", err)
	}
	return req, nil
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
func (b *broker) respond(c *net.UnixConn, resp *api.Response, fd daxFd) error {
	if fd.path != "" {
		f, err := b.openDax(fd.path, fd.readOnly)
		if err != nil {
			resp = &api.Response{Lease: resp.Lease, State: resp.State, Error: fmt.Sprintf("opening %s: %v", fd.path, err)}
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

// authenticate maps the peer to a container of the lease claim's pod.
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

// refreshClaimPods re-reads the reservedFor of a lease claim (a claim
// shared by several pods is prepared only once) and reports whether the
// pod is in it now.
func (b *broker) refreshClaimPods(tc *leaseClaim, podUID string) bool {
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
func (b *broker) handle(claimUID string, ref containerRef, req *api.Request) (*api.Response, daxFd) {
	if err := req.Validate(); err != nil {
		return &api.Response{Lease: req.Lease, Error: err.Error()}, daxFd{}
	}
	wait := b.parseWait(req.Wait)
	switch req.Op {
	case api.OpRequest:
		return b.requestLease(claimUID, ref, req, wait)
	case api.OpRelease:
		if req.All {
			return b.releaseAll(ref, wait), daxFd{}
		}
		return b.releaseOne(ref, req.Lease, wait), daxFd{}
	case api.OpList:
		return b.list(ref), daxFd{}
	case api.OpFd:
		return b.waitLease(ref, req.Lease, wait, true)
	default: // api.OpEnv
		return b.waitLease(ref, req.Lease, wait, false)
	}
}

// parseWait returns the duration of a validated wait, the default for "".
func (b *broker) parseWait(s string) time.Duration {
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}
	return b.cfg.WaitDefault
}

// requestOptions sets the source, mode, sharing, size and device of a
// validated request on the lease.
func (b *broker) requestOptions(l *lease, req *api.Request) error {
	if b.cfg.PoolDriver == "" && (req.Source == api.SourcePool || req.Shared) {
		return errPoolDisabled
	}
	l.Source, l.Shared, l.ReadOnly, l.Mode = req.Source, req.Shared, req.ReadOnly, req.EffectiveMode()
	if req.Size != "" {
		q, err := resource.ParseQuantity(req.Size)
		if err != nil || q.Sign() <= 0 {
			return fmt.Errorf("invalid size %q", req.Size)
		}
		l.Size = q.String()
	}
	if req.Serial == "" {
		return nil
	}
	return b.serialOptions(l, req)
}

// serialOptions sets the serial of a validated request on the lease and,
// when it names a shared device, mode dax and the hosts to request when
// the lease takes the device exclusively.
func (b *broker) serialOptions(l *lease, req *api.Request) error {
	serial, _ := api.ParseSerial(req.Serial)
	l.Serial = api.FormatSerial(serial)
	dev, err := b.deviceBySerial(serial)
	switch {
	case err != nil:
		return err
	case dev == nil:
		return nil
	case !dev.shared && l.Shared:
		return fmt.Errorf("device %s: not shareable", l.Serial)
	case !dev.shared:
		return nil
	case req.Source == api.SourceLocal:
		return fmt.Errorf("device %s: not a local device", l.Serial)
	case req.Mode == api.ModeNUMA:
		return errors.New("shared memory is never onlined as RAM: use mode dax")
	case !l.Shared && dev.hosts == nil:
		return fmt.Errorf("device %s (serial %s) has no %q capacity", dev.name, l.Serial, device.CapacityHosts)
	case !l.Shared:
		l.Hosts = dev.hosts.String()
	}
	l.Mode = api.ModeDax
	return nil
}

// sliceDevice is a device with a serial in a ResourceSlice.
type sliceDevice struct {
	name   string
	shared bool
	hosts  *resource.Quantity // capacity of a shared device, nil if none
}

// deviceBySerial returns the device with the serial in the ResourceSlices
// of cxl.generic and the pool driver, nil if there is none or no
// Kubernetes client.
func (b *broker) deviceBySerial(serial uint64) (*sliceDevice, error) {
	if b.cfg.Client == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(b.ctx, apiTimeout)
	defer cancel()
	list, err := b.cfg.Client.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing ResourceSlices: %w", err)
	}
	for i := range list.Items {
		rs := &list.Items[i]
		if rs.Spec.Driver != device.DriverName && (b.cfg.PoolDriver == "" || rs.Spec.Driver != b.cfg.PoolDriver) {
			continue
		}
		for j := range rs.Spec.Devices {
			d := &rs.Spec.Devices[j]
			if s, ok := deviceSerial(d, rs.Spec.Driver); !ok || s != serial {
				continue
			}
			dev := &sliceDevice{name: d.Name}
			if a, ok := deviceAttribute(d, rs.Spec.Driver, device.AttrShared); ok && a.BoolValue != nil {
				dev.shared = *a.BoolValue
			}
			if c, ok := d.Capacity[device.CapacityHosts]; ok {
				dev.hosts = &c.Value
			}
			return dev, nil
		}
	}
	return nil, nil
}

// deviceAttribute returns the attribute of a device of driver, unqualified
// or qualified with the driver name.
func deviceAttribute(d *resourceapi.Device, driver string, name resourceapi.QualifiedName) (resourceapi.DeviceAttribute, bool) {
	for _, k := range []resourceapi.QualifiedName{name, resourceapi.QualifiedName(driver + "/" + string(name))} {
		if a, ok := d.Attributes[k]; ok {
			return a, true
		}
	}
	return resourceapi.DeviceAttribute{}, false
}

// deviceSerial returns the serial attribute of a device of driver, and
// whether it has a valid one.
func deviceSerial(d *resourceapi.Device, driver string) (uint64, bool) {
	if a, ok := deviceAttribute(d, driver, device.AttrSerial); ok && a.StringValue != nil {
		if v, err := api.ParseSerial(*a.StringValue); err == nil {
			return v, true
		}
	}
	return 0, false
}

// leaseInfo is the response of a lease. Called with b.mu held.
func leaseInfo(l *lease) *api.Response {
	r := &api.Response{Lease: l.ID, State: l.State, Source: l.Source, Mode: l.Mode, Shared: l.Shared, ReadOnly: l.ReadOnly,
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
	if m, ok := l.memory(); ok {
		r.Env = api.Env(m)
	}
	return r
}

// fdOf returns the dax device whose fd goes with the response of a lease:
// that of a Ready dax lease.
func fdOf(l *lease) daxFd {
	if l.State == api.StateReady && l.Dax != "" {
		return daxFd{path: l.Dax, readOnly: l.ReadOnly}
	}
	return daxFd{}
}

// requestLease returns the response to a request of a container of ref on
// the socket of a lease claim, and the dax device whose fd goes with it:
// it stores the lease, creates its companion and waits up to wait for it.
func (b *broker) requestLease(claimUID string, ref containerRef, req *api.Request, wait time.Duration) (*api.Response, daxFd) {
	l := &lease{
		Claim:     claimUID,
		RequestID: req.RequestID,
		Pod:       podRef{Namespace: ref.PodNamespace, Name: ref.PodName, UID: ref.PodUID},
		Container: ref.Name,
		State:     api.StateCreating,
		Node:      -1,
		Created:   time.Now().UTC(),
	}
	b.mu.Lock()
	if req.RequestID != "" {
		for _, old := range b.store.Leases {
			if old.RequestID == req.RequestID && old.Pod.UID == ref.PodUID && old.Container == ref.Name {
				id := old.ID
				b.mu.Unlock()
				return b.waitReady(id, wait, true)
			}
		}
	}
	b.mu.Unlock()
	if err := b.requestOptions(l, req); err != nil {
		return &api.Response{Error: err.Error()}, daxFd{}
	}

	b.mu.Lock()
	for l.ID == "" || b.store.Leases[l.ID] != nil {
		l.ID = b.newID()
	}
	b.store.Leases[l.ID] = l
	if err := b.store.save(); err != nil {
		delete(b.store.Leases, l.ID)
		b.mu.Unlock()
		return &api.Response{Error: err.Error()}, daxFd{}
	}
	b.notify()
	b.mu.Unlock()
	klog.Infof("lease broker: lease %s: %s/%s/%s requests size %q serial %q (source %q, mode %s, shared %v, hosts %q, read-only %v)",
		l.ID, l.Pod.Namespace, l.Pod.Name, l.Container, l.Size, l.Serial, l.Source, l.Mode, l.Shared, l.Hosts, l.ReadOnly)

	if err := b.createCompanion(l); err != nil {
		klog.Errorf("lease broker: lease %s: %v", l.ID, err)
		b.mu.Lock()
		if cur := b.store.Leases[l.ID]; cur != nil {
			cur.State, cur.Error = api.StateFailed, err.Error()
		}
		b.mu.Unlock()
		resp := b.startRelease(l.ID, err.Error())
		resp.State, resp.Error = api.StateFailed, err.Error()
		return resp, daxFd{}
	}
	b.mu.Lock()
	if cur := b.store.Leases[l.ID]; cur != nil && cur.State == api.StateCreating {
		cur.State = api.StatePending
		b.saveLocked()
	}
	b.mu.Unlock()
	return b.waitReady(l.ID, wait, true)
}

// waitReady waits until the lease is Ready or Failed, at most wait. At
// timeout a lease is released and reported Failed if release is set, else
// reported in its state. wait 0 returns the state at once.
func (b *broker) waitReady(id string, wait time.Duration, release bool) (*api.Response, daxFd) {
	ctx, cancel := context.WithTimeout(b.ctx, wait)
	defer cancel()
	var resp *api.Response
	var fd daxFd
	done := func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		l := b.store.Leases[id]
		switch {
		case l == nil:
			resp = &api.Response{Lease: id, State: api.StateReleased, Error: "lease was released"}
			return true
		case l.State == api.StateReady || l.State == api.StateFailed || l.State == api.StateReleasing:
			resp, fd = leaseInfo(l), fdOf(l)
			return true
		}
		resp = leaseInfo(l)
		return wait == 0
	}
	if err := b.waitFor(ctx, done); err != nil && release {
		msg := fmt.Sprintf("timeout: not Ready after %v", wait)
		klog.Warningf("lease broker: lease %s: %s, releasing", id, msg)
		resp = b.startRelease(id, msg)
		resp.State, resp.Error = api.StateFailed, msg
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

// releaseOne releases a lease of the pod of ref and returns its state
// after waiting up to wait for it to be gone.
func (b *broker) releaseOne(ref containerRef, id string, wait time.Duration) *api.Response {
	b.mu.Lock()
	l := b.ownLease(ref, id)
	b.mu.Unlock()
	if l == nil {
		return &api.Response{Lease: id, Error: "no such lease"}
	}
	b.startRelease(id, "released by "+ref.Name)
	return b.waitReleased([]string{id}, wait)[0]
}

// releaseAll releases every lease of the pod of ref and returns their
// states after waiting up to wait for them to be gone.
func (b *broker) releaseAll(ref containerRef, wait time.Duration) *api.Response {
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
	out := []api.Response{}
	for _, r := range b.waitReleased(ids, wait) {
		out = append(out, *r)
	}
	return &api.Response{Leases: &out}
}

// waitReleased waits until the leases are gone, at most wait, and
// returns their states (Released, or the current state).
func (b *broker) waitReleased(ids []string, wait time.Duration) []*api.Response {
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
	out := make([]*api.Response, 0, len(ids))
	for _, id := range ids {
		if l := b.store.Leases[id]; l != nil {
			out = append(out, leaseInfo(l))
		} else {
			out = append(out, &api.Response{Lease: id, State: api.StateReleased})
		}
	}
	return out
}

// list returns the leases of the pod of ref.
func (b *broker) list(ref containerRef) *api.Response {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := []api.Response{}
	for _, l := range b.store.Leases {
		if l.Pod.UID == ref.PodUID {
			out = append(out, *leaseInfo(l))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Lease < out[j].Lease })
	return &api.Response{Leases: &out}
}

// waitLease waits until a lease of the pod of ref is Ready, at most wait,
// and returns its info, and with withFd the fd of a dax lease of the
// container of ref.
func (b *broker) waitLease(ref containerRef, id string, wait time.Duration, withFd bool) (*api.Response, daxFd) {
	b.mu.Lock()
	l := b.ownLease(ref, id)
	switch {
	case l == nil || (withFd && l.Container != ref.Name):
		b.mu.Unlock()
		return &api.Response{Lease: id, Error: "no such lease of this container"}, daxFd{}
	case withFd && l.Mode != api.ModeDax:
		resp := leaseInfo(l)
		b.mu.Unlock()
		resp.Error = fmt.Sprintf("lease %s is not a %s lease; use %s", id, api.ModeDax, api.OpEnv)
		return resp, daxFd{}
	}
	b.mu.Unlock()
	resp, fd := b.waitReady(id, wait, false)
	if !withFd {
		fd = daxFd{}
	}
	return resp, fd
}

// companionObjects returns the companion claim and pod of a lease.
func (b *broker) companionObjects(l *lease) (*resourceapi.ResourceClaim, *corev1.Pod, error) {
	name := companionName(l.ID)
	labels := map[string]string{
		device.LeaseLabel:        l.ID,
		device.RequesterUIDLabel: l.Pod.UID,
		device.NodeLabel:         b.cfg.NodeName,
	}
	annotations := map[string]string{
		device.RequesterAnnotation: l.Pod.Namespace + "/" + l.Pod.Name + "/" + l.Container,
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
	// yet; with 100m it takes ~2 s. The memory limit leaves room for runc
	// init, which needs about 9 MiB in the container's cgroup.
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
// claim of a lease accepts, the preferred one first: the requested one, or
// local, then pool. Shared devices are pool devices only.
func (b *broker) companionSources(l *lease) ([]string, error) {
	var sources []string
	for _, s := range []string{api.SourceLocal, api.SourcePool} {
		switch {
		case l.Source != "" && l.Source != s:
		case s == api.SourceLocal && l.deviceShared():
		case s == api.SourcePool && b.cfg.PoolDriver == "":
		default:
			sources = append(sources, s)
		}
	}
	if len(sources) == 0 {
		return nil, errPoolDisabled
	}
	return sources, nil
}

// companionRequest returns the device request and the opaque configs of
// the companion claim of a lease. A lease with one source gets an exact
// request; with more, a request with one subrequest per source in order
// of preference (firstAvailable), and configs that apply to the
// subrequest of their driver only.
func (b *broker) companionRequest(l *lease) (*resourceapi.DeviceRequest, []resourceapi.DeviceClaimConfiguration, error) {
	sources, err := b.companionSources(l)
	if err != nil {
		return nil, nil, err
	}
	req := &resourceapi.DeviceRequest{Name: companionClaimRef}
	var configs []resourceapi.DeviceClaimConfiguration
	for _, source := range sources {
		exact, driver, err := b.companionDevices(l, source)
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
		params, err := json.Marshal(&deviceConfig{
			APIVersion: driver + "/v1alpha1",
			Kind:       device.DeviceConfigKind,
			Mode:       l.Mode,
			ReadOnly:   l.ReadOnly,
		})
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
	return req, configs, nil
}

// companionDevices returns what the companion claim of a lease requests
// from one source, and the driver of its devices: the DeviceClass, the
// selectors of the serial and, on shared devices, of the size, and the
// capacity: memory on exclusive devices, all hosts when a shared device is
// taken exclusively.
func (b *broker) companionDevices(l *lease, source string) (*resourceapi.ExactDeviceRequest, string, error) {
	driver := device.DriverName
	exact := &resourceapi.ExactDeviceRequest{DeviceClassName: device.ClassLocal}
	if source == api.SourcePool {
		driver, exact.DeviceClassName = b.cfg.PoolDriver, device.ClassPool
		if l.deviceShared() {
			exact.DeviceClassName = device.ClassPoolShared
		}
	}
	var cel []string
	if l.Serial != "" {
		cel = append(cel, fmt.Sprintf(`device.attributes[%q].%s == %q`, driver, device.AttrSerial, l.Serial))
	}
	var size resource.Quantity
	if l.Size != "" {
		q, err := resource.ParseQuantity(l.Size)
		if err != nil {
			return nil, "", fmt.Errorf("size %q: %w", l.Size, err)
		}
		size = q
	}
	switch {
	case exact.DeviceClassName != device.ClassPoolShared:
		if l.Size != "" {
			exact.Capacity = &resourceapi.CapacityRequirements{
				Requests: map[resourceapi.QualifiedName]resource.Quantity{device.CapacityMemory: size},
			}
		}
	default:
		if l.Size != "" {
			cel = append(cel, fmt.Sprintf(`device.attributes[%q].%s >= %d`, driver, device.AttrSize, size.Value()))
		}
		if l.Hosts != "" {
			hosts, err := resource.ParseQuantity(l.Hosts)
			if err != nil {
				return nil, "", fmt.Errorf("hosts %q: %w", l.Hosts, err)
			}
			exact.Capacity = &resourceapi.CapacityRequirements{
				Requests: map[resourceapi.QualifiedName]resource.Quantity{device.CapacityHosts: hosts},
			}
		}
	}
	for _, e := range cel {
		exact.Selectors = append(exact.Selectors, resourceapi.DeviceSelector{CEL: &resourceapi.CELDeviceSelector{Expression: e}})
	}
	return exact, driver, nil
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
func (b *broker) startRelease(id, reason string) *api.Response {
	b.mu.Lock()
	defer b.mu.Unlock()
	l := b.store.Leases[id]
	if l == nil {
		return &api.Response{Lease: id, State: api.StateReleased}
	}
	if !b.releasing[id] {
		b.releasing[id] = true
		klog.Infof("lease broker: lease %s: releasing (%s, was %s)", id, reason, l.State)
		if l.State != api.StateFailed { // a failed lease shows why until it is gone
			l.State = api.StateReleasing
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
	if l.Mode == api.ModeNUMA && l.Node >= 0 && lender != nil {
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
// its leases (the nodes of numa leases are lent again when it starts).
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

// containerStarted is the OnContainerStarted callback: the nodes of Ready
// numa leases are lent again to a restarted container.
func (b *broker) containerStarted(ref containerRef) {
	b.relendContainer(ref.PodUID, ref.Name)
}

// relendContainer applies LendNode for the Ready numa leases of a
// container (idempotent).
func (b *broker) relendContainer(podUID, name string) {
	b.mu.Lock()
	lender := b.lender
	var nodes []int
	var ids []string
	for id, l := range b.store.Leases {
		if l.Pod.UID == podUID && l.Container == name && l.Mode == api.ModeNUMA && l.State == api.StateReady && l.Node >= 0 {
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
func (b *broker) reconcileLoop(lender nodeLender) {
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
// is gone, restarts interrupted releases, lends the nodes of numa leases
// again and deletes companions of this node that have no lease.
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
		case api.StateReleasing:
			b.startRelease(l.ID, "release interrupted")
			continue
		case api.StateCreating:
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
		if l.State == api.StateReady && l.Mode == api.ModeNUMA && l.Node >= 0 {
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

// deleteOrphans deletes the companion pods and claims of this node that
// have no lease: the objects with the node and lease labels whose name is
// cxl-lease-<lease label>.
func (b *broker) deleteOrphans() {
	if b.cfg.Client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(b.ctx, apiTimeout)
	defer cancel()
	sel := metav1.ListOptions{LabelSelector: device.NodeLabel + "=" + b.cfg.NodeName + "," + device.LeaseLabel}
	// orphan tells whether an object is a companion without a lease.
	orphan := func(o metav1.ObjectMeta) bool {
		id := o.Labels[device.LeaseLabel]
		if o.Name != companionName(id) || o.DeletionTimestamp != nil {
			return false
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.store.Leases[id] == nil
	}
	if pods, err := b.cfg.Client.CoreV1().Pods("").List(ctx, sel); err != nil {
		klog.Warningf("lease broker: listing companion pods: %v", err)
	} else {
		for _, p := range pods.Items {
			if orphan(p.ObjectMeta) {
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
			if orphan(c.ObjectMeta) {
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

// OnLending checks that a claim is the companion of a lease of this
// broker that is being created or lent: its name is cxl-lease-<id>, its
// namespace and owner pod those of the requester. It marks the lease
// Lending and returns the lend of the requester's container.
func (b *broker) OnLending(id string, claim *resourceapi.ResourceClaim) (*lendRecord, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	l := b.store.Leases[id]
	notCompanion := fmt.Errorf("claim %s/%s: not the companion of lease %s", claim.Namespace, claim.Name, id)
	if l == nil || claim.Name != companionName(id) || claim.Namespace != l.Pod.Namespace || !ownedBy(claim, l.Pod.UID) {
		return nil, notCompanion
	}
	switch l.State {
	case api.StateCreating, api.StatePending:
		l.State = api.StateLending
		b.saveLocked()
	case api.StateLending, api.StateReady:
	default:
		return nil, fmt.Errorf("lease %s is %s", l.ID, l.State)
	}
	return &lendRecord{Lease: id, Pod: l.Pod, Container: l.Container}, nil
}

// ownedBy tells whether an object has an owner reference with the uid.
func ownedBy(claim *resourceapi.ResourceClaim, uid string) bool {
	for _, o := range claim.OwnerReferences {
		if string(o.UID) == uid {
			return true
		}
	}
	return false
}

// OnPrepared marks a lease Ready with the prepared device.
func (b *broker) OnPrepared(id string, devs []lentDevice) {
	b.mu.Lock()
	defer b.mu.Unlock()
	l := b.store.Leases[id]
	if l == nil || l.State == api.StateReleasing || len(devs) == 0 {
		return
	}
	d := devs[0]
	l.Serial = api.FormatSerial(d.Serial)
	l.Bytes = d.Size
	l.Source = d.Source
	if d.Mode == api.ModeDax {
		l.Node, l.Dax = -1, d.DaxDevice
	} else {
		l.Node, l.Dax = d.Node, ""
	}
	l.State, l.Error = api.StateReady, ""
	b.saveLocked()
	klog.Infof("lease broker: lease %s: Ready (source %s, serial %s, node %d, dax %q, %d bytes)", l.ID, l.Source, l.Serial, l.Node, l.Dax, l.Bytes)
}

// OnLendFailed fails the lease with the lend error (for example a
// container that is memory-steered, not started or gone) and releases it:
// the waiting request returns Failed with the message.
func (b *broker) OnLendFailed(id string, err error) {
	b.mu.Lock()
	l := b.store.Leases[id]
	if l == nil || l.State == api.StateReleasing || l.State == api.StateFailed {
		b.mu.Unlock()
		return
	}
	l.State, l.Error = api.StateFailed, err.Error()
	b.saveLocked()
	b.mu.Unlock()
	b.startRelease(id, "lend failed")
}

// OnReleasing is called when the companion claim of a lease is
// unprepared. Unless the broker is releasing the lease, the companion was
// deleted or evicted behind its back: the lease fails (the requester has
// lost the memory) and the reconcile drops it.
func (b *broker) OnReleasing(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if l := b.store.Leases[id]; l != nil && l.State != api.StateReleasing {
		l.State = api.StateFailed
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
	major, minor, numErr := memctl.New().DaxDevNumbers(name)
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for {
		if numErr == nil {
			var st syscall.Stat_t
			if err := syscall.Stat(path, &st); err == nil {
				rdev := uint64(st.Rdev)
				if st.Mode&syscall.S_IFMT != syscall.S_IFCHR ||
					int(unix.Major(rdev)) != major || int(unix.Minor(rdev)) != minor {
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
	dev := unix.Mkdev(uint32(major), uint32(minor))
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
