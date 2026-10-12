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
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	cdiapi "tags.cncf.io/container-device-interface/pkg/cdi"
	"tags.cncf.io/container-device-interface/specs-go"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/helpers"

	nricxl "github.com/containers/nri-plugins/pkg/cxl"
)

// fakeLender is a Lender with a fixed container table.
type fakeLender struct {
	mu      sync.Mutex
	byPid   *containerRef            // returned for any pid, nil: unknown
	ctrs    map[string]*containerRef // podUID/name
	lent    map[string][]int         // podUID/name -> nodes
	calls   []string
	lendErr error
	gone    []func(containerRef)
	started []func(containerRef)
}

func newFakeLender(refs ...containerRef) *fakeLender {
	f := &fakeLender{ctrs: map[string]*containerRef{}, lent: map[string][]int{}}
	for i := range refs {
		r := refs[i]
		f.ctrs[r.PodUID+"/"+r.Name] = &r
	}
	if len(refs) > 0 {
		r := refs[0]
		f.byPid = &r
	}
	return f
}

func (f *fakeLender) ContainerByPid(pid int) (*containerRef, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.byPid == nil {
		return nil, false
	}
	r := *f.byPid
	return &r, true
}

func (f *fakeLender) Container(podUID, name string) (*containerRef, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.ctrs[podUID+"/"+name]
	if !ok {
		return nil, false
	}
	c := *r
	return &c, true
}

func (f *fakeLender) LendNode(ctx context.Context, podUID, name string, node int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fmt.Sprintf("lend %s/%s %d", podUID, name, node))
	if f.lendErr != nil {
		return f.lendErr
	}
	k := podUID + "/" + name
	if !slices.Contains(f.lent[k], node) {
		f.lent[k] = append(f.lent[k], node)
	}
	return nil
}

func (f *fakeLender) RevokeNode(ctx context.Context, podUID, name string, node int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fmt.Sprintf("revoke %s/%s %d", podUID, name, node))
	k := podUID + "/" + name
	f.lent[k] = slices.DeleteFunc(f.lent[k], func(n int) bool { return n == node })
	return nil
}

func (f *fakeLender) LentNodes(podUID, name string) []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.lent[podUID+"/"+name])
}

func (f *fakeLender) OnContainerGone(fn func(ref containerRef)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gone = append(f.gone, fn)
}

func (f *fakeLender) OnContainerStarted(fn func(ref containerRef)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started = append(f.started, fn)
}

func (f *fakeLender) callLog() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.calls, "\n")
}

func (f *fakeLender) setPeer(r *containerRef) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byPid = r
}

func (f *fakeLender) remove(podUID, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.ctrs, podUID+"/"+name)
}

var _ Lender = (*fakeLender)(nil)

const (
	testNode     = "node1"
	testNS       = "demo"
	testPod      = "borrower"
	testPodUID   = "pod-uid-1"
	testCtr      = "app"
	testTokenUID = "tok1"
)

var testRef = containerRef{PodUID: testPodUID, PodNamespace: testNS, PodName: testPod, Name: testCtr, ID: "c1", Pid: 42}

type brokerEnv struct {
	t      *testing.T
	dir    string
	cdiDir string
	bin    string
	client *fake.Clientset
	lender *fakeLender
	b      *broker
}

func requesterPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: testPod, Namespace: testNS, UID: types.UID(testPodUID)},
		Spec:       corev1.PodSpec{RestartPolicy: corev1.RestartPolicyAlways, NodeName: testNode},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func tokenClaimObj() *resourceapi.ResourceClaim {
	return &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "dyn", Namespace: testNS, UID: types.UID(testTokenUID)},
		Status: resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{Devices: resourceapi.DeviceAllocationResult{
				Results: []resourceapi.DeviceRequestAllocationResult{{Request: "dyn", Driver: device.DriverName, Pool: testNode, Device: device.DynamicDeviceName}},
			}},
			ReservedFor: []resourceapi.ResourceClaimConsumerReference{{Resource: "pods", Name: testPod, UID: types.UID(testPodUID)}},
		},
	}
}

func newBrokerEnv(t *testing.T) *brokerEnv {
	t.Helper()
	dir, err := os.MkdirTemp("", "w1a")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	e := &brokerEnv{
		t:      t,
		dir:    filepath.Join(dir, "cxl.generic"),
		cdiDir: filepath.Join(dir, "cdi"),
		bin:    filepath.Join(dir, "cxl-request"),
		client: fake.NewClientset(requesterPod(), tokenClaimObj()),
		lender: newFakeLender(testRef),
	}
	noErr(t, os.MkdirAll(e.cdiDir, 0o755))
	noErr(t, os.WriteFile(e.bin, []byte("#!/bin/true\n"), 0o755))
	e.restart()
	return e
}

// restart creates a new broker on the same data directory, as after a
// plugin restart.
func (e *brokerEnv) restart() {
	e.t.Helper()
	if e.b != nil {
		e.b.stop()
	}
	cache, err := cdiapi.NewCache(cdiapi.WithSpecDirs(e.cdiDir), cdiapi.WithAutoRefresh(false))
	noErr(e.t, err)
	b, err := newBroker(brokerConfig{
		Client:      e.client,
		NodeName:    testNode,
		DataDir:     e.dir,
		CDI:         cache,
		RequestBin:  e.bin,
		WaitDefault: 5 * time.Second,
		PoolDriver:  device.PoolDriverName,
	})
	noErr(e.t, err)
	b.pollInterval = 5 * time.Millisecond
	b.reconcileEvery = time.Hour // the tests call reconcile()
	b.closeTimeout = 2 * time.Second
	n := 0
	b.newID = func() string { n++; return fmt.Sprintf("%08x", 0xa0+n) }
	e.b = b
	e.t.Cleanup(b.stop)
}

func (e *brokerEnv) start() {
	e.b.start(e.lender)
}

func (e *brokerEnv) sock() string {
	return filepath.Join(e.dir, leaseDirName, testTokenUID, leaseSocketName)
}

// call sends one request and returns the response and the received fd
// (-1 for none).
func (e *brokerEnv) call(req leaseRequest) (leaseResponse, int) {
	e.t.Helper()
	c, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: e.sock(), Net: "unix"})
	noErr(e.t, err)
	defer c.Close()
	data, _ := json.Marshal(req)
	_, err = c.Write(append(data, '\n'))
	noErr(e.t, err)
	buf := make([]byte, 64*1024)
	oob := make([]byte, syscall.CmsgSpace(4))
	n, oobn, _, _, err := c.ReadMsgUnix(buf, oob)
	noErr(e.t, err)
	fd := -1
	if oobn > 0 {
		msgs, err := syscall.ParseSocketControlMessage(oob[:oobn])
		noErr(e.t, err)
		fds, err := syscall.ParseUnixRights(&msgs[0])
		noErr(e.t, err)
		fd = fds[0]
	}
	line := buf[:n]
	if !strings.Contains(string(line), "\n") {
		rest, err := bufio.NewReader(c).ReadBytes('\n')
		noErr(e.t, err)
		line = append(line, rest...)
	}
	var resp leaseResponse
	noErr(e.t, json.Unmarshal(line, &resp))
	return resp, fd
}

// async runs call in the background.
func (e *brokerEnv) async(req leaseRequest) chan leaseResponse {
	ch := make(chan leaseResponse, 1)
	go func() {
		c, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: e.sock(), Net: "unix"})
		if err != nil {
			ch <- leaseResponse{Error: err.Error()}
			return
		}
		defer c.Close()
		data, _ := json.Marshal(req)
		_, _ = c.Write(append(data, '\n'))
		line, err := bufio.NewReader(c).ReadBytes('\n')
		var resp leaseResponse
		if err == nil {
			err = json.Unmarshal(line, &resp)
		}
		if err != nil {
			resp.Error = err.Error()
		}
		ch <- resp
	}()
	return ch
}

func (e *brokerEnv) waitClaim(name string) *resourceapi.ResourceClaim {
	e.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := e.client.ResourceV1().ResourceClaims(testNS).Get(context.Background(), name, metav1.GetOptions{})
		if err == nil {
			// the pod is created right after the claim
			if _, err := e.client.CoreV1().Pods(testNS).Get(context.Background(), name, metav1.GetOptions{}); err == nil {
				return c
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	e.t.Fatalf("claim %s not created", name)
	return nil
}

func (e *brokerEnv) wantGone(name string) {
	e.t.Helper()
	_, err := e.client.CoreV1().Pods(testNS).Get(context.Background(), name, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		e.t.Fatalf("companion pod %s still exists (%v)", name, err)
	}
	_, err = e.client.ResourceV1().ResourceClaims(testNS).Get(context.Background(), name, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		e.t.Fatalf("companion claim %s still exists (%v)", name, err)
	}
}

func (e *brokerEnv) lease(id string) *lease {
	e.b.mu.Lock()
	defer e.b.mu.Unlock()
	if l := e.b.store.Leases[id]; l != nil {
		c := *l
		return &c
	}
	return nil
}

func (e *brokerEnv) waitState(id, state string) {
	e.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		l := e.lease(id)
		if (l == nil && state == leaseReleased) || (l != nil && l.State == state) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	e.t.Fatalf("lease %s not %s: %+v", id, state, e.lease(id))
}

// allocated returns the companion claim as the scheduler and the pool
// controller leave it: allocated a pool device, config copied from the
// spec, device data in the status.
func allocated(c *resourceapi.ResourceClaim, dev, data string) *resourceapi.ResourceClaim {
	c = c.DeepCopy()
	c.UID = types.UID("uid-" + c.Name)
	var cfgs []resourceapi.DeviceAllocationConfiguration
	for _, sc := range c.Spec.Devices.Config {
		cfgs = append(cfgs, resourceapi.DeviceAllocationConfiguration{Source: resourceapi.AllocationConfigSourceClaim, Requests: sc.Requests, DeviceConfiguration: sc.DeviceConfiguration})
	}
	c.Status.Allocation = &resourceapi.AllocationResult{Devices: resourceapi.DeviceAllocationResult{
		Results: []resourceapi.DeviceRequestAllocationResult{{Request: companionClaimRef, Driver: device.PoolDriverName, Pool: "fake-cxl-pool", Device: dev}},
		Config:  cfgs,
	}}
	c.Status.Devices = []resourceapi.AllocatedDeviceStatus{{Driver: device.PoolDriverName, Pool: "fake-cxl-pool", Device: dev, Data: &runtime.RawExtension{Raw: []byte(data)}}}
	return c
}

func TestBrokerTokenClaimSocketAndCDI(t *testing.T) {
	e := newBrokerEnv(t)
	noErr(t, e.b.OpenClaim(tokenClaimObj()))
	noErr(t, e.b.OpenClaim(tokenClaimObj())) // idempotent

	st, err := os.Stat(e.sock())
	noErr(t, err)
	if st.Mode()&os.ModeSocket == 0 || st.Mode().Perm() != 0o666 {
		t.Fatalf("socket mode %v", st.Mode())
	}
	if st, _ := os.Stat(filepath.Dir(e.sock())); st.Mode().Perm() != 0o755 {
		t.Fatalf("directory mode %v", st.Mode())
	}

	// the CDI device of the claim
	m, _ := filepath.Glob(filepath.Join(e.cdiDir, device.LeaseCDISpecName+".*"))
	if len(m) != 1 {
		t.Fatalf("no CDI spec in %s", e.cdiDir)
	}
	cache, err := cdiapi.NewCache(cdiapi.WithSpecDirs(e.cdiDir), cdiapi.WithAutoRefresh(false))
	noErr(t, err)
	dev := cache.GetDevice(leaseCDIDeviceID(testTokenUID))
	if dev == nil {
		t.Fatalf("CDI device %s missing: %v", leaseCDIDeviceID(testTokenUID), cache.ListDevices())
	}
	if leaseCDIDeviceID(testTokenUID) != "generic/cxl=lease-tok1" {
		t.Fatalf("CDI id %s", leaseCDIDeviceID(testTokenUID))
	}
	wantMounts := []*specs.Mount{
		{HostPath: filepath.Dir(e.sock()), ContainerPath: "/run/cxl", Type: "bind", Options: []string{"rbind", "rw"}},
		{HostPath: e.bin, ContainerPath: "/usr/local/bin/cxl-request", Type: "bind", Options: []string{"bind", "ro"}},
		{HostPath: e.bin, ContainerPath: "/usr/local/bin/cxl-release", Type: "bind", Options: []string{"bind", "ro"}},
	}
	if !reflect.DeepEqual(dev.ContainerEdits.Mounts, wantMounts) {
		t.Fatalf("mounts %+v", dev.ContainerEdits.Mounts)
	}
	if !slices.Equal(dev.ContainerEdits.Env, []string{"CXL_LEASE_SOCKET=/run/cxl/cxl.sock"}) {
		t.Fatalf("env %v", dev.ContainerEdits.Env)
	}

	// without a lender: lending unavailable
	resp, _ := e.call(leaseRequest{Op: "list"})
	wantErr(t, errors.New(resp.Error), "lending unavailable: NRI not connected")

	// restart: the socket is re-created
	e.restart()
	e.start()
	resp, _ = e.call(leaseRequest{Op: "list"})
	if resp.Error != "" || resp.Leases == nil || len(*resp.Leases) != 0 {
		t.Fatalf("list after restart: %+v", resp)
	}

	// close: socket, directory and CDI device gone
	noErr(t, e.b.CloseClaim(context.Background(), testTokenUID))
	if _, err := os.Stat(filepath.Dir(e.sock())); !os.IsNotExist(err) {
		t.Fatalf("lease directory not removed: %v", err)
	}
	if m, _ := filepath.Glob(filepath.Join(e.cdiDir, device.LeaseCDISpecName+".*")); len(m) != 0 {
		t.Fatalf("CDI spec not removed: %v", m)
	}
	noErr(t, e.b.CloseClaim(context.Background(), testTokenUID)) // unknown: fine
}

func TestBrokerAuthRejection(t *testing.T) {
	e := newBrokerEnv(t)
	noErr(t, e.b.OpenClaim(tokenClaimObj()))
	e.lender.setPeer(&containerRef{PodUID: "other-pod", PodNamespace: testNS, PodName: "intruder", Name: "x"})
	e.start()
	resp, _ := e.call(leaseRequest{Op: "lease", Kind: "memory", Size: "512Mi"})
	wantErr(t, errors.New(resp.Error), "not a container of the claim's pod")
	e.lender.setPeer(nil)
	resp, _ = e.call(leaseRequest{Op: "list"})
	wantErr(t, errors.New(resp.Error), "not a container of the claim's pod")
	if l, _ := e.client.ResourceV1().ResourceClaims(testNS).List(context.Background(), metav1.ListOptions{}); len(l.Items) != 1 {
		t.Fatalf("objects created for a rejected request: %d claims", len(l.Items))
	}

	// a pod added to a shared token claim later is found by re-reading
	// the claim's reservedFor
	tc := tokenClaimObj()
	tc.Status.ReservedFor = append(tc.Status.ReservedFor, resourceapi.ResourceClaimConsumerReference{Resource: "pods", Name: "second", UID: "pod-uid-2"})
	_, err := e.client.ResourceV1().ResourceClaims(testNS).Update(context.Background(), tc, metav1.UpdateOptions{})
	noErr(t, err)
	e.lender.setPeer(&containerRef{PodUID: "pod-uid-2", PodNamespace: testNS, PodName: "second", Name: "x"})
	resp, _ = e.call(leaseRequest{Op: "list"})
	if resp.Error != "" {
		t.Fatalf("second pod rejected: %+v", resp)
	}

	// bad requests
	e.lender.setPeer(&testRef)
	for _, c := range []struct {
		req leaseRequest
		err string
	}{
		{leaseRequest{Op: "lease", Kind: "memory", Size: "lots"}, "invalid size"},
		{leaseRequest{Op: "lease", Kind: "memory", Size: "0"}, "invalid size"},
		{leaseRequest{Op: "lease", Kind: "shared", Serial: "xyz"}, "invalid serial"},
		{leaseRequest{Op: "lease", Kind: "nvme"}, "invalid kind"},
		{leaseRequest{Op: "lease", Kind: "memory", Size: "1Gi", Wait: "-1s"}, "invalid wait"},
		{leaseRequest{Op: "frobnicate"}, "unknown op"},
		{leaseRequest{Op: "release", Lease: "nope"}, "no such lease"},
		{leaseRequest{Op: "fd", Lease: "nope"}, "no such lease"},
	} {
		resp, _ := e.call(c.req)
		wantErr(t, errors.New(resp.Error), c.err)
	}
}

// TestBrokerMemoryLease runs a memory lease through the real pool helper
// (fake sysfs) and the fake lender: request -> companion objects ->
// prepare (lend) -> Ready -> release (revoke, objects deleted) ->
// unprepare.
func TestBrokerMemoryLease(t *testing.T) {
	pe := newPoolTestEnv(t)
	pe.f.plug("0xc1ee0001")
	p := pe.driver(t)
	e := newBrokerEnv(t)
	noErr(t, e.b.OpenClaim(tokenClaimObj()))
	e.start()
	p.setLending(e.lender, e.b)

	ch := e.async(leaseRequest{Op: "lease", Kind: "memory", Size: "256Mi", Wait: "5s", RequestID: "r1"})
	name := "cxl-lease-000000a1"
	claim := e.waitClaim(name)
	e.waitState("000000a1", leasePending)

	// the companion claim, exactly
	params := `{"apiVersion":"cxl-pool.generic/v1alpha1","kind":"LendConfig","lease":"000000a1","pod":{"namespace":"demo","name":"borrower","uid":"pod-uid-1"},"container":"app"}`
	wantSpec := resourceapi.ResourceClaimSpec{Devices: resourceapi.DeviceClaim{
		Requests: []resourceapi.DeviceRequest{{Name: "lease", Exactly: &resourceapi.ExactDeviceRequest{
			DeviceClassName: "cxl-pool-memory",
			Capacity:        &resourceapi.CapacityRequirements{Requests: map[resourceapi.QualifiedName]resource.Quantity{"memory": resource.MustParse("256Mi")}},
		}}},
		Config: []resourceapi.DeviceClaimConfiguration{{DeviceConfiguration: resourceapi.DeviceConfiguration{Opaque: &resourceapi.OpaqueDeviceConfiguration{
			Driver: "cxl-pool.generic", Parameters: runtime.RawExtension{Raw: []byte(params)},
		}}}},
	}}
	if !reflect.DeepEqual(claim.Spec, wantSpec) {
		got, _ := json.Marshal(claim.Spec)
		t.Fatalf("claim spec:\n%s", got)
	}
	wantOwner := []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: testPod, UID: testPodUID, Controller: ptr(true)}}
	if !reflect.DeepEqual(claim.OwnerReferences, wantOwner) {
		t.Fatalf("claim owners %+v", claim.OwnerReferences)
	}
	wantLabels := map[string]string{"cxl-pool.generic/lease": "000000a1", "cxl-pool.generic/requester-uid": testPodUID, "cxl-pool.generic/node": testNode}
	if !reflect.DeepEqual(claim.Labels, wantLabels) {
		t.Fatalf("claim labels %v", claim.Labels)
	}

	// the companion pod, exactly
	pod, err := e.client.CoreV1().Pods(testNS).Get(context.Background(), name, metav1.GetOptions{})
	noErr(t, err)
	if !reflect.DeepEqual(pod.Labels, wantLabels) {
		t.Fatalf("pod labels %v", pod.Labels)
	}
	if !reflect.DeepEqual(pod.OwnerReferences, []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: testPod, UID: testPodUID}}) {
		t.Fatalf("pod owners %+v", pod.OwnerReferences)
	}
	res := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("8Mi")}
	wantPodSpec := corev1.PodSpec{
		Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{testNode}}}}},
		}}},
		Tolerations:                   []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
		RestartPolicy:                 corev1.RestartPolicyAlways,
		AutomountServiceAccountToken:  ptr(false),
		EnableServiceLinks:            ptr(false),
		TerminationGracePeriodSeconds: ptr(int64(1)),
		ResourceClaims:                []corev1.PodResourceClaim{{Name: "lease", ResourceClaimName: ptr(name)}},
		Containers: []corev1.Container{{
			Name: "pause", Image: "registry.k8s.io/pause:3.10",
			Resources: corev1.ResourceRequirements{Requests: res, Limits: res},
		}},
	}
	if !reflect.DeepEqual(pod.Spec, wantPodSpec) {
		got, _ := json.Marshal(pod.Spec)
		t.Fatalf("pod spec:\n%s", got)
	}

	// kubelet prepares the companion claim: region, online, lend
	alloc := allocated(claim, "pooled0", exclusiveData)
	r := prepare(t, p, alloc)
	noErr(t, r.Err)
	if got := e.lender.LentNodes(testPodUID, testCtr); !slices.Equal(got, []int{2}) {
		t.Fatalf("lent nodes %v", got)
	}
	resp := <-ch
	if resp.Error != "" || resp.State != leaseReady || resp.Lease != "000000a1" || resp.Node == nil || *resp.Node != 2 ||
		resp.Size != 268435456 || resp.Serial != "0xc1ee0001" || resp.Kind != "memory" {
		t.Fatalf("lease response %+v", resp)
	}
	data, _ := os.ReadFile(pe.stateFile)
	if !strings.Contains(string(data), `"lease": "000000a1"`) || !strings.Contains(string(data), `"nodes": [`) {
		t.Fatalf("lend not persisted in the pool state:\n%s", data)
	}

	// the same requestId returns the same lease
	again, _ := e.call(leaseRequest{Op: "lease", Kind: "memory", Size: "256Mi", RequestID: "r1"})
	if again.Lease != "000000a1" || again.State != leaseReady {
		t.Fatalf("retry with requestId: %+v", again)
	}
	list, _ := e.call(leaseRequest{Op: "list"})
	if list.Leases == nil || len(*list.Leases) != 1 || (*list.Leases)[0].Lease != "000000a1" {
		t.Fatalf("list %+v", list)
	}

	// a restarted container gets the node again
	e.lender.mu.Lock()
	e.lender.lent = map[string][]int{}
	started := e.lender.started[0]
	e.lender.mu.Unlock()
	started(testRef)
	if got := e.lender.LentNodes(testPodUID, testCtr); !slices.Equal(got, []int{2}) {
		t.Fatalf("not re-lent after restart: %v", got)
	}

	// release: revoke, companion objects deleted, lease dropped
	rel, _ := e.call(leaseRequest{Op: "release", Lease: "000000a1"})
	if rel.State != leaseReleased {
		t.Fatalf("release %+v", rel)
	}
	e.wantGone(name)
	if !strings.Contains(e.lender.callLog(), "revoke pod-uid-1/app 2") {
		t.Fatalf("not revoked:\n%s", e.lender.callLog())
	}
	// kubelet unprepares the companion claim: revoke again, release
	noErr(t, unprepare(t, p, string(alloc.UID)))
	pe.f.wantReleased(t)
	if l := e.lender.LentNodes(testPodUID, testCtr); len(l) != 0 {
		t.Fatalf("still lent %v", l)
	}
}

func TestBrokerSharedLeaseFd(t *testing.T) {
	e := newLeaseFdEnv(t, "0xc1ae0001")
	claim, resp, fd := e.prepareLease(leaseRequest{Op: "lease", Kind: "shared", Serial: "0xC1AE0001"}, "shared0", sharedData)
	want := resourceapi.ExactDeviceRequest{
		DeviceClassName: "cxl-shared-memory",
		Selectors:       []resourceapi.DeviceSelector{{CEL: &resourceapi.CELDeviceSelector{Expression: `device.attributes["cxl-pool.generic"].serial == "0xc1ae0001"`}}},
	}
	if !reflect.DeepEqual(*claim.Spec.Devices.Requests[0].Exactly, want) {
		t.Fatalf("shared request %+v", claim.Spec.Devices.Requests[0].Exactly)
	}
	e.wantDaxFd(resp, fd)
	if resp.Exclusive || resp.ReadOnly {
		t.Fatalf("shared response %+v", resp)
	}
	if calls := e.lender.callLog(); strings.Contains(calls, "lend") {
		t.Fatalf("shared device lent as a node:\n%s", calls)
	}

	// re-send the fd
	resp, fd = e.call(leaseRequest{Op: "fd", Lease: "000000a1"})
	e.wantDaxFd(resp, fd)
	if got := e.opened(); !slices.Equal(got, []bool{false, false}) {
		t.Fatalf("dax opened read-only: %v", got)
	}
	e.wantReleaseAll(1)
	e.wantGone("cxl-lease-000000a1")
}

func TestBrokerTimeoutReleases(t *testing.T) {
	e := newBrokerEnv(t)
	noErr(t, e.b.OpenClaim(tokenClaimObj()))
	e.start()
	resp, _ := e.call(leaseRequest{Op: "lease", Kind: "memory", Size: "1Gi", Wait: "50ms"})
	if resp.State != leaseFailed || !strings.Contains(resp.Error, "timeout") {
		t.Fatalf("timeout response %+v", resp)
	}
	e.waitState(resp.Lease, leaseReleased)
	e.wantGone(companionName(resp.Lease))

	// wait 0 returns Pending at once and keeps the lease
	resp, _ = e.call(leaseRequest{Op: "lease", Kind: "memory", Size: "1Gi", Wait: "0s"})
	if resp.State != leasePending || resp.Error != "" || resp.Size != 1<<30 {
		t.Fatalf("wait 0 response %+v", resp)
	}
}

func TestBrokerLendFailure(t *testing.T) {
	pe := newPoolTestEnv(t)
	pe.f.plug("0xc1ee0001")
	p := pe.driver(t)
	e := newBrokerEnv(t)
	noErr(t, e.b.OpenClaim(tokenClaimObj()))
	e.start()
	p.setLending(e.lender, e.b)
	e.lender.mu.Lock()
	e.lender.lendErr = errSteered
	e.lender.mu.Unlock()

	ch := e.async(leaseRequest{Op: "lease", Kind: "memory", Size: "256Mi"})
	claim := e.waitClaim("cxl-lease-000000a1")
	r := prepare(t, p, allocated(claim, "pooled0", exclusiveData))
	wantErr(t, r.Err, "memory-steered")
	pe.f.wantReleased(t) // rolled back
	resp := <-ch
	if resp.State != leaseFailed || !strings.Contains(resp.Error, "memory-steered") {
		t.Fatalf("lend failure response %+v", resp)
	}
	e.waitState("000000a1", leaseReleased)
	e.wantGone("cxl-lease-000000a1")
}

func TestBrokerRestartAndReconcile(t *testing.T) {
	e := newBrokerEnv(t)
	noErr(t, e.b.OpenClaim(tokenClaimObj()))
	e.start()
	resp, _ := e.call(leaseRequest{Op: "lease", Kind: "memory", Size: "256Mi", Wait: "0s"})
	id := resp.Lease
	cfg := &lendConfig{Lease: id, Pod: podRef{Namespace: testNS, Name: testPod, UID: testPodUID}, Container: testCtr}
	noErr(t, e.b.OnLending(cfg))
	e.b.OnPrepared(cfg, []preparedPoolDevice{{SerialHex: "c1ee0001", Size: 1 << 28, Node: 2}})
	e.waitState(id, leaseReady)

	// a lease from an unknown LendConfig is refused
	wantErr(t, e.b.OnLending(&lendConfig{Lease: "ffffffff"}), "unknown lease")
	wantErr(t, e.b.OnLending(&lendConfig{Lease: id, Pod: cfg.Pod, Container: "other"}), "belongs to")

	// restart: lease and socket are back, the lend is re-applied
	e.restart()
	e.lender.mu.Lock()
	e.lender.lent = map[string][]int{}
	e.lender.mu.Unlock()
	e.start()
	if l := e.lease(id); l == nil || l.State != leaseReady || l.Node != 2 {
		t.Fatalf("lease after restart %+v", l)
	}
	list, _ := e.call(leaseRequest{Op: "list"})
	if list.Leases == nil || len(*list.Leases) != 1 {
		t.Fatalf("list after restart %+v", list)
	}
	e.b.reconcile()
	if got := e.lender.LentNodes(testPodUID, testCtr); !slices.Equal(got, []int{2}) {
		t.Fatalf("lend not re-applied by reconcile: %v", got)
	}

	// an orphaned companion (no lease) is deleted
	orphan := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "cxl-lease-0badf00d", Namespace: testNS,
		Labels: map[string]string{leaseLabel: "0badf00d", leaseNodeLabel: testNode}}}
	_, err := e.client.CoreV1().Pods(testNS).Create(context.Background(), orphan, metav1.CreateOptions{})
	noErr(t, err)
	orphanClaim := &resourceapi.ResourceClaim{ObjectMeta: orphan.ObjectMeta}
	_, err = e.client.ResourceV1().ResourceClaims(testNS).Create(context.Background(), orphanClaim, metav1.CreateOptions{})
	noErr(t, err)
	e.b.reconcile()
	e.wantGone("cxl-lease-0badf00d")
	if e.lease(id) == nil {
		t.Fatal("live lease dropped by reconcile")
	}

	// the requester container stopped but the pod restarts it: kept
	e.lender.remove(testPodUID, testCtr)
	e.b.reconcile()
	if e.lease(id) == nil {
		t.Fatal("lease of a restarting container dropped")
	}
	// the requester pod is deleted: the lease is released
	noErr(t, e.client.CoreV1().Pods(testNS).Delete(context.Background(), testPod, metav1.DeleteOptions{}))
	e.b.reconcile()
	e.waitState(id, leaseReleased)
	e.wantGone(companionName(id))
	if !strings.Contains(e.lender.callLog(), "revoke pod-uid-1/app 2") {
		t.Fatalf("not revoked:\n%s", e.lender.callLog())
	}
}

func TestBrokerCompanionGoneAndContainerGone(t *testing.T) {
	e := newBrokerEnv(t)
	noErr(t, e.b.OpenClaim(tokenClaimObj()))
	e.start()
	r1, _ := e.call(leaseRequest{Op: "lease", Kind: "memory", Size: "256Mi", Wait: "0s"})
	r2, _ := e.call(leaseRequest{Op: "lease", Kind: "shared", Serial: "0xc1ae0001", Wait: "0s"})

	// companion deleted behind the broker's back: unprepare -> Failed,
	// reconcile drops it
	e.b.OnReleasing(r1.Lease)
	if l := e.lease(r1.Lease); l.State != leaseFailed {
		t.Fatalf("lease %+v", l)
	}
	noErr(t, e.client.CoreV1().Pods(testNS).Delete(context.Background(), companionName(r1.Lease), metav1.DeleteOptions{}))
	e.b.reconcile()
	e.waitState(r1.Lease, leaseReleased)

	// the requester pod has restartPolicy Never: its stopped container
	// releases its leases
	pod := requesterPod()
	pod.Spec.RestartPolicy = corev1.RestartPolicyNever
	_, err := e.client.CoreV1().Pods(testNS).Update(context.Background(), pod, metav1.UpdateOptions{})
	noErr(t, err)
	e.lender.remove(testPodUID, testCtr)
	e.b.containerGone(testRef)
	e.waitState(r2.Lease, leaseReleased)
	e.wantGone(companionName(r2.Lease))
}

func TestBrokerCloseClaimReleasesLeases(t *testing.T) {
	e := newBrokerEnv(t)
	noErr(t, e.b.OpenClaim(tokenClaimObj()))
	e.start()
	r, _ := e.call(leaseRequest{Op: "lease", Kind: "memory", Size: "256Mi", Wait: "0s"})
	noErr(t, e.b.CloseClaim(context.Background(), testTokenUID))
	if e.lease(r.Lease) != nil {
		t.Fatal("lease survived the token claim")
	}
	e.wantGone(companionName(r.Lease))

	// a companion that does not go away: "still releasing", kubelet retries
	noErr(t, e.b.OpenClaim(tokenClaimObj()))
	r, _ = e.call(leaseRequest{Op: "lease", Kind: "memory", Size: "256Mi", Wait: "0s"})
	e.client.PrependReactor("delete", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil // accepted, but the pod stays (Terminating)
	})
	e.b.closeTimeout = 50 * time.Millisecond
	wantErr(t, e.b.CloseClaim(context.Background(), testTokenUID), "still releasing 1 leases")
	if _, err := os.Stat(e.sock()); err != nil {
		t.Fatalf("socket closed too early: %v", err)
	}
}

func TestParseLendConfig(t *testing.T) {
	mk := func(src resourceapi.AllocationConfigSource, params string) *resourceapi.ResourceClaim {
		c := &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "c"}}
		c.Status.Allocation = &resourceapi.AllocationResult{Devices: resourceapi.DeviceAllocationResult{
			Config: []resourceapi.DeviceAllocationConfiguration{
				{Source: src, DeviceConfiguration: resourceapi.DeviceConfiguration{Opaque: &resourceapi.OpaqueDeviceConfiguration{
					Driver: device.PoolDriverName, Parameters: runtime.RawExtension{Raw: []byte(params)}}}},
				{Source: src, DeviceConfiguration: resourceapi.DeviceConfiguration{Opaque: &resourceapi.OpaqueDeviceConfiguration{
					Driver: device.DriverName, Parameters: runtime.RawExtension{Raw: []byte(`{"kind":"LendConfig"}`)}}}},
			},
		}}
		return c
	}
	good := `{"apiVersion":"cxl-pool.generic/v1alpha1","kind":"LendConfig","lease":"abc","pod":{"namespace":"demo","name":"p","uid":"u"},"container":"app"}`
	cfg, err := parseLendConfig(mk(resourceapi.AllocationConfigSourceClaim, good), device.PoolDriverName)
	noErr(t, err)
	if cfg == nil || cfg.Lease != "abc" || cfg.Container != "app" || cfg.Pod != (podRef{Namespace: testNS, Name: "p", UID: "u"}) {
		t.Fatalf("parsed %+v", cfg)
	}
	cfg, err = parseLendConfig(mk(resourceapi.AllocationConfigSourceClaim, `{"kind":"Other"}`), device.PoolDriverName)
	if cfg != nil || err != nil {
		t.Fatalf("other kind: %+v %v", cfg, err)
	}
	for _, c := range []struct {
		src    resourceapi.AllocationConfigSource
		params string
		err    string
	}{
		{resourceapi.AllocationConfigSourceClass, good, "only from the claim"},
		{resourceapi.AllocationConfigSourceClaim, strings.Replace(good, `"namespace":"demo"`, `"namespace":"other"`, 1), "not the claim's namespace"},
		{resourceapi.AllocationConfigSourceClaim, strings.Replace(good, `"container":"app"`, `"container":""`, 1), "required"},
		{resourceapi.AllocationConfigSourceClaim, strings.Replace(good, "v1alpha1", "v9", 1), "apiVersion"},
		{resourceapi.AllocationConfigSourceClaim, `{"kind":`, "invalid opaque config"},
	} {
		_, err := parseLendConfig(mk(c.src, c.params), device.PoolDriverName)
		wantErr(t, err, c.err)
	}

	// the pool helper refuses a LendConfig without a broker or a lender
	pe := newPoolTestEnv(t)
	pe.f.plug("0xc1ee0001")
	p := pe.driver(t)
	claim := poolClaim("u9", "c9", "pooled0", exclusiveData)
	claim.Namespace = testNS
	claim.Status.Allocation.Devices.Config = mk(resourceapi.AllocationConfigSourceClaim, good).Status.Allocation.Devices.Config
	wantErr(t, prepare(t, p, claim).Err, "lending to running containers is disabled")
	p.setLending(nil, &broker{store: &leaseStore{Leases: map[string]*lease{}}, changed: make(chan struct{})})
	wantErr(t, prepare(t, p, claim).Err, "lending unavailable: NRI not connected")
	if strings.Contains(pe.f.commands(), "create-region") {
		t.Fatalf("device touched for a refused lend:\n%s", pe.f.commands())
	}
}

// TestTokenDevice publishes the token device and prepares a claim on it
// through nodeState: claim marker + lease CDI device, socket opened.
func TestTokenDevice(t *testing.T) {
	devs, err := buildDevInfos(&DriverConfig{IgnoreDRAMNodes: true, PublishDynamicToken: true}, &nricxl.Devices{}, nil)
	noErr(t, err)
	if d := devs["dynamic"]; d == nil || d.Name != "dynamic" {
		t.Fatalf("no token device in %v", devs)
	} else if _, ok := d.Dev.(*device.DynamicToken); !ok {
		t.Fatalf("token Dev %T", d.Dev)
	}
	none, _ := buildDevInfos(&DriverConfig{IgnoreDRAMNodes: true}, &nricxl.Devices{}, nil)
	if len(none) != 0 {
		t.Fatalf("token published without --pool-lend: %v", none)
	}

	e := newBrokerEnv(t)
	cache, err := cdiapi.NewCache(cdiapi.WithSpecDirs(e.cdiDir), cdiapi.WithAutoRefresh(false))
	noErr(t, err)
	s := &nodeState{
		NodeState: &helpers.NodeState{
			CdiCache:               cache,
			Allocatable:            devs,
			Prepared:               helpers.ClaimPreparations{},
			PreparedClaimsFilePath: filepath.Join(e.dir, "preparedClaims.json"),
			NodeName:               testNode,
		},
		config:         &DriverConfig{},
		preparedClaims: PreparedClaimsInfo{},
		claimsFilePath: filepath.Join(e.dir, "preparedClaimsInfo.json"),
		podClaims:      map[string]map[string]bool{},
	}

	r := s.GetResources()
	got := r.Pools[testNode].Slices[0].Devices
	want := []resourceapi.Device{{
		Name: "dynamic",
		Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			"type": {StringValue: ptr("dynamic")},
			"name": {StringValue: ptr("dynamic")},
		},
		AllowMultipleAllocations: ptr(true),
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("published %+v", got)
	}

	// lending disabled: Prepare fails
	wantErr(t, s.Prepare(context.Background(), tokenClaimObj()), "lending to running containers is disabled")

	s.dynamic = e.b
	noErr(t, s.Prepare(context.Background(), tokenClaimObj()))
	res := s.Prepared[testTokenUID]
	if len(res.Devices) != 1 || !slices.Equal(res.Devices[0].CDIDeviceIDs, []string{"generic/cxl=claim-tok1", "generic/cxl=lease-tok1"}) {
		t.Fatalf("prepared %+v", res.Devices)
	}
	if info := s.preparedClaims[testTokenUID]; info == nil || info.Devices[0].DeviceType != "dynamic" {
		t.Fatalf("claim info %+v", info)
	}
	if _, err := os.Stat(e.sock()); err != nil {
		t.Fatalf("no lease socket: %v", err)
	}
}

// poolSlice is the pool controller's ResourceSlice with an exclusive
// device pooled0 and a shared device shared0 (hosts 4).
func poolSlice() *resourceapi.ResourceSlice {
	str := func(s string) resourceapi.DeviceAttribute { return resourceapi.DeviceAttribute{StringValue: &s} }
	return &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "fake-cxl-pool"},
		Spec: resourceapi.ResourceSliceSpec{
			Driver:   device.PoolDriverName,
			Pool:     resourceapi.ResourcePool{Name: "fake-cxl-pool", ResourceSliceCount: 1},
			AllNodes: ptr(true),
			Devices: []resourceapi.Device{
				{
					Name:       "pooled0",
					Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"serial": str("0xc1ee0001")},
					Capacity:   map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{"memory": {Value: resource.MustParse("512Mi")}},
				},
				{
					Name:       "shared0",
					Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"serial": str("0xC1AE0001")},
					Capacity:   map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{"hosts": {Value: resource.MustParse("4")}},
				},
			},
		},
	}
}

func TestBrokerLeaseOptions(t *testing.T) {
	e := newBrokerEnv(t)
	_, err := e.client.ResourceV1().ResourceSlices().Create(context.Background(), poolSlice(), metav1.CreateOptions{})
	noErr(t, err)
	for _, tc := range []struct {
		req  leaseRequest
		want lease
		err  string
	}{
		{req: leaseRequest{Kind: "memory", Size: "1Gi"}, want: lease{Size: "1Gi", Mode: "numa"}},
		{req: leaseRequest{Kind: "memory", Size: "1Gi", Mode: "dax", ReadOnly: true}, want: lease{Size: "1Gi", Mode: "dax", ReadOnly: true}},
		{req: leaseRequest{Kind: "shared", Serial: "0xC1AE0001", ReadOnly: true}, want: lease{Serial: "0xc1ae0001", Mode: "dax", ReadOnly: true}},
		{req: leaseRequest{Kind: "shared", Serial: "0xc1ae0001", Exclusive: true}, want: lease{Serial: "0xc1ae0001", Mode: "dax", Exclusive: true, Hosts: "4"}},
		{req: leaseRequest{Kind: "memory", Size: "1Gi", ReadOnly: true}, err: "readOnly needs a dax lease"},
		{req: leaseRequest{Kind: "memory", Size: "1Gi", Exclusive: true}, err: "exclusive is for shared leases"},
		{req: leaseRequest{Kind: "memory", Size: "1Gi", Mode: "ram"}, err: `invalid mode "ram"`},
		{req: leaseRequest{Kind: "shared", Serial: "0xc1ae0001", Mode: "numa"}, err: "a shared lease is dax only"},
		{req: leaseRequest{Kind: "shared", Serial: "0xc1ae0002", Exclusive: true}, err: "no pool device with serial 0xc1ae0002"},
		{req: leaseRequest{Kind: "shared", Serial: "0xc1ee0001", Exclusive: true}, err: `has no "hosts" capacity`},
	} {
		var l lease
		err := e.b.leaseOptions(&l, &tc.req)
		switch {
		case tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)):
			t.Errorf("%+v: expected error %q, got %v", tc.req, tc.err, err)
		case tc.err == "" && (err != nil || !reflect.DeepEqual(l, tc.want)):
			t.Errorf("%+v: got %+v %v, expected %+v", tc.req, l, err, tc.want)
		}
	}
}

// poolDeviceConfigOf returns the PoolDeviceConfig of a companion claim,
// nil if it has none.
func poolDeviceConfigOf(t *testing.T, c *resourceapi.ResourceClaim) *poolDeviceConfig {
	t.Helper()
	for _, cfg := range c.Spec.Devices.Config {
		var pc poolDeviceConfig
		noErr(t, json.Unmarshal(cfg.Opaque.Parameters.Raw, &pc))
		if pc.Kind == device.PoolDeviceConfigKind {
			return &pc
		}
	}
	return nil
}

// leaseFdEnv is a broker and a pool helper with a fake dax device that
// records how it was opened.
type leaseFdEnv struct {
	*brokerEnv
	pe       *poolTestEnv
	p        *poolDriver
	mu       sync.Mutex
	readOnly []bool
}

func newLeaseFdEnv(t *testing.T, serial string) *leaseFdEnv {
	pe := newPoolTestEnv(t)
	pe.f.plug(serial)
	e := &leaseFdEnv{brokerEnv: newBrokerEnv(t), pe: pe, p: pe.driver(t)}
	_, err := e.client.ResourceV1().ResourceSlices().Create(context.Background(), poolSlice(), metav1.CreateOptions{})
	noErr(t, err)
	noErr(t, e.b.OpenClaim(tokenClaimObj()))
	e.start()
	e.p.setLending(e.lender, e.b)
	daxFile := filepath.Join(e.dir, "fake-dax")
	noErr(t, os.WriteFile(daxFile, []byte("TAG"), 0o600))
	e.b.openDax = func(path string, readOnly bool) (*os.File, error) {
		if path != "/dev/dax0.0" {
			return nil, fmt.Errorf("unexpected dax %s", path)
		}
		e.mu.Lock()
		e.readOnly = append(e.readOnly, readOnly)
		e.mu.Unlock()
		return os.Open(daxFile)
	}
	return e
}

func (e *leaseFdEnv) opened() []bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.readOnly)
}

// prepareLease requests a lease in the background, waits for its companion
// claim, prepares it as the scheduler, the pool controller and kubelet
// would, and returns the claim and the response with its fd.
func (e *leaseFdEnv) prepareLease(req leaseRequest, dev, data string) (*resourceapi.ResourceClaim, leaseResponse, int) {
	e.t.Helper()
	type result struct {
		resp leaseResponse
		fd   int
	}
	ch := make(chan result, 1)
	go func() {
		r, fd := e.call(req)
		ch <- result{r, fd}
	}()
	claim := e.waitClaim("cxl-lease-000000a1")
	noErr(e.t, prepare(e.t, e.p, allocated(claim, dev, data)).Err)
	r := <-ch
	return claim, r.resp, r.fd
}

func TestBrokerExclusiveReadOnlySharedLease(t *testing.T) {
	e := newLeaseFdEnv(t, "0xc1ae0001")
	claim, resp, fd := e.prepareLease(leaseRequest{Op: "lease", Kind: "shared", Serial: "0xc1ae0001", Exclusive: true, ReadOnly: true}, "shared0", sharedData)
	// all of the device's hosts capacity, so that nothing else gets it
	got := claim.Spec.Devices.Requests[0].Exactly.Capacity
	if got == nil {
		t.Fatal("exclusive lease without a capacity request")
	}
	if hosts := got.Requests[poolHostsCapacity]; hosts.Cmp(resource.MustParse("4")) != 0 {
		t.Fatalf("exclusive capacity request %+v", got)
	}
	if pc := poolDeviceConfigOf(t, claim); pc == nil || pc.Mode != poolModeDax || !pc.ReadOnly {
		t.Fatalf("companion PoolDeviceConfig %+v", pc)
	}
	e.wantDaxFd(resp, fd)
	if !resp.Exclusive || !resp.ReadOnly {
		t.Fatalf("response %+v", resp)
	}
	// the fd is opened read-only, also when it is sent again
	resp, fd = e.call(leaseRequest{Op: "fd", Lease: resp.Lease})
	e.wantDaxFd(resp, fd)
	if got := e.opened(); !slices.Equal(got, []bool{true, true}) {
		t.Fatalf("dax opened read-only: %v", got)
	}
	e.wantReleaseAll(1)
}

func TestBrokerMemoryDaxLease(t *testing.T) {
	e := newLeaseFdEnv(t, "0xc1ee0001")
	claim, resp, fd := e.prepareLease(leaseRequest{Op: "lease", Kind: "memory", Size: "256Mi", Mode: "dax"}, "pooled0", exclusiveData)
	ex := claim.Spec.Devices.Requests[0].Exactly
	if size := ex.Capacity.Requests["memory"]; ex.DeviceClassName != poolMemoryClass || size.String() != "256Mi" {
		t.Fatalf("memory request %+v", ex)
	}
	if pc := poolDeviceConfigOf(t, claim); pc == nil || pc.Mode != poolModeDax || pc.ReadOnly {
		t.Fatalf("companion PoolDeviceConfig %+v", pc)
	}
	e.pe.f.wantCommand(t, "daxctl reconfigure-device --mode=devdax --force dax0.0")
	e.pe.f.wantBlocks(t, "offline")
	e.wantDaxFd(resp, fd)
	if got := e.opened(); !slices.Equal(got, []bool{false}) {
		t.Fatalf("dax opened read-only: %v", got)
	}
	if calls := e.lender.callLog(); strings.Contains(calls, "lend") {
		t.Fatalf("dax memory lent as a node:\n%s", calls)
	}
	// a plain memory lease has no PoolDeviceConfig
	l := &lease{ID: "x", Kind: leaseKindMemory, Size: "1Gi", Mode: poolModeNUMA, Pod: podRef{Namespace: testNS, Name: testPod, UID: testPodUID}}
	plain, _, err := e.b.companionObjects(l)
	noErr(t, err)
	if pc := poolDeviceConfigOf(t, plain); pc != nil || len(plain.Spec.Devices.Config) != 1 {
		t.Fatalf("numa companion configs %+v", plain.Spec.Devices.Config)
	}
	e.wantReleaseAll(1)
}

// wantDaxFd checks a Ready dax lease response that came with the fd of
// the fake dax device, and closes the fd.
func (e *leaseFdEnv) wantDaxFd(resp leaseResponse, fd int) {
	e.t.Helper()
	if resp.State != leaseReady || resp.Mode != poolModeDax || resp.Dax != "/dev/dax0.0" || resp.Node != nil || resp.Size != 268435456 || fd < 0 {
		e.t.Fatalf("response %+v fd %d", resp, fd)
	}
	f := os.NewFile(uintptr(fd), "dax")
	defer f.Close()
	buf := make([]byte, 3)
	if _, err := f.ReadAt(buf, 0); err != nil || string(buf) != "TAG" {
		e.t.Fatalf("read %q through the fd: %v", buf, err)
	}
}

// wantReleaseAll releases all leases of the pod and checks that the n
// leases are gone.
func (e *brokerEnv) wantReleaseAll(n int) {
	e.t.Helper()
	rel, _ := e.call(leaseRequest{Op: "release", All: true})
	if rel.Leases == nil || len(*rel.Leases) != n {
		e.t.Fatalf("release all %+v", rel)
	}
	for _, l := range *rel.Leases {
		if l.State != leaseReleased {
			e.t.Fatalf("release all: %+v", l)
		}
	}
}
