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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	cdiSpecs "tags.cncf.io/container-device-interface/specs-go"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
)

// fakeHooks records the leaseHooks calls.
type fakeHooks struct {
	mu         sync.Mutex
	calls      []string
	lendingErr error
	devs       []lentDevice
}

func (h *fakeHooks) OnLending(cfg *lendConfig) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, "lending "+cfg.Lease)
	return h.lendingErr
}

func (h *fakeHooks) OnPrepared(cfg *lendConfig, devs []lentDevice) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, "prepared "+cfg.Lease)
	h.devs = devs
}

func (h *fakeHooks) OnLendFailed(cfg *lendConfig, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, "failed "+cfg.Lease+": "+err.Error())
}

func (h *fakeHooks) OnReleasing(lease string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, "releasing "+lease)
}

func (h *fakeHooks) log() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return strings.Join(h.calls, "\n")
}

var _ leaseHooks = (*fakeHooks)(nil)

// revokeFailer is a Lender whose RevokeNode fails.
type revokeFailer struct {
	*fakeLender
}

func (r revokeFailer) RevokeNode(ctx context.Context, podUID, name string, node int, holder string) error {
	_ = r.fakeLender.RevokeNode(ctx, podUID, name, node, holder)
	return fmt.Errorf("node %d busy", node)
}

func testLendConfig() *lendConfig {
	return &lendConfig{Kind: device.LendConfigKind, Lease: "l1", Pod: podRef{Namespace: testNS, Name: testPod, UID: testPodUID}, Container: testCtr}
}

func TestClaimLenderDisabled(t *testing.T) {
	cfg := testLendConfig()
	c := newClaimLender(device.DriverName)
	if c.enabled() {
		t.Fatal("enabled without hooks")
	}
	wantErr(t, c.check(cfg), "lease l1: lending to running containers is disabled")
	hooks := &fakeHooks{}
	c.set(nil, hooks)
	if !c.enabled() {
		t.Fatal("not enabled with hooks")
	}
	err := c.check(cfg)
	if !errors.Is(err, errLendingUnavailable) {
		t.Fatalf("check without NRI: %v", err)
	}
	// numa devices cannot be lent without NRI, dax devices need no lend
	rec, err := c.lend(context.Background(), cfg, []lentDevice{{Mode: device.ModeNUMA, Node: 3}})
	if !errors.Is(err, errLendingUnavailable) || len(rec.Nodes) != 0 {
		t.Fatalf("lend without NRI: %+v %v", rec, err)
	}
	rec, err = c.lend(context.Background(), cfg, []lentDevice{{Mode: device.ModeDax, Node: -1, DaxDevice: "/dev/dax0.1"}})
	noErr(t, err)
	if rec.Lease != "l1" || rec.Container != testCtr || len(rec.Nodes) != 0 {
		t.Fatalf("dax lend record %+v", rec)
	}
	// revoking without NRI only warns
	noErr(t, c.revoke(context.Background(), &lendRecord{Lease: "l1", Nodes: []int{3}}, "ns/c"))
	if log := hooks.log(); log != "" {
		t.Fatalf("hooks called: %s", log)
	}

	// a nil claimLender is disabled and safe to use
	var n *claimLender
	n.set(newFakeLender(), hooks)
	n.prepared(cfg, nil)
	n.failed(cfg, errors.New("x"))
	n.releasing("l1")
	n.reprepared(&lendRecord{Lease: "l1"}, nil)
	if n.enabled() || hooks.log() != "" {
		t.Fatalf("nil claimLender: enabled %v, hooks %s", n.enabled(), hooks.log())
	}
	wantErr(t, n.check(cfg), "disabled")
	if _, err := n.parse(&resourceapi.ResourceClaim{}); err == nil {
		t.Fatal("nil claimLender parsed a claim")
	}
}

func TestClaimLenderLendRevoke(t *testing.T) {
	ctx := context.Background()
	cfg := testLendConfig()
	nodes := newFakeLender(testRef)
	hooks := &fakeHooks{}
	c := newClaimLender(device.PoolDriverName)
	c.set(nodes, hooks)

	noErr(t, c.check(cfg))
	hooks.lendingErr = errors.New("lease l1 is Failed")
	wantErr(t, c.check(cfg), "lease l1 is Failed")
	hooks.lendingErr = nil

	devs := []lentDevice{
		{Mode: device.ModeNUMA, Node: 2},
		{Mode: device.ModeDax, Node: -1, DaxDevice: "/dev/dax0.0"},
		{Mode: device.ModeNUMA, Node: -1},
		{Mode: device.ModeNUMA, Node: 3},
	}
	rec, err := c.lend(ctx, cfg, devs)
	noErr(t, err)
	want := &lendRecord{Lease: "l1", Pod: cfg.Pod, Container: testCtr, Nodes: []int{2, 3}}
	if !reflect.DeepEqual(rec, want) {
		t.Fatalf("lend record %+v", rec)
	}
	if got := nodes.LentNodes(testPodUID, testCtr); !reflect.DeepEqual(got, []int{2, 3}) {
		t.Fatalf("lent nodes %v", got)
	}
	noErr(t, c.revoke(ctx, rec, "ns/c"))
	if got := nodes.LentNodes(testPodUID, testCtr); len(got) != 0 {
		t.Fatalf("still lent %v", got)
	}
	noErr(t, c.revoke(ctx, nil, "ns/c"))

	// a failing LendNode returns the nodes lent so far, including the
	// failed one, so that a revoke cleans up
	nodes.mu.Lock()
	nodes.lendErr = errSteered
	nodes.mu.Unlock()
	rec, err = c.lend(ctx, cfg, devs)
	if !errors.Is(err, errSteered) || !reflect.DeepEqual(rec.Nodes, []int{2}) {
		t.Fatalf("failed lend: %+v %v", rec, err)
	}

	// revoke errors are joined
	c.set(revokeFailer{nodes}, hooks)
	err = c.revoke(ctx, &lendRecord{Lease: "l1", Pod: cfg.Pod, Container: testCtr, Nodes: []int{2, 3}}, "ns/c")
	wantErr(t, err, "revoking node 2")
	wantErr(t, err, "node 3 busy")

	// hook forwarding
	hooks.calls = nil
	c.prepared(cfg, devs[:1])
	c.failed(cfg, errors.New("boom"))
	c.releasing("l1")
	c.reprepared(&lendRecord{Lease: "l2", Pod: cfg.Pod, Container: testCtr}, devs[1:2])
	hooks.lendingErr = errors.New("unknown lease")
	c.reprepared(&lendRecord{Lease: "l3"}, devs)
	if got, want := hooks.log(), "prepared l1\nfailed l1: boom\nreleasing l1\nlending l2\nprepared l2\nlending l3"; got != want {
		t.Fatalf("hook calls:\n%s\nexpected:\n%s", got, want)
	}
	if !reflect.DeepEqual(hooks.devs, devs[1:2]) {
		t.Fatalf("prepared devices %+v", hooks.devs)
	}
}

// TestLendRecordJSON checks that lend records of state files written by
// earlier versions of the pool helper load.
func TestLendRecordJSON(t *testing.T) {
	old := `{"lease":"000000a1","pod":{"namespace":"demo","name":"borrower","uid":"pod-uid-1"},"container":"app","nodes":[2]}`
	var rec lendRecord
	noErr(t, json.Unmarshal([]byte(old), &rec))
	want := lendRecord{Lease: "000000a1", Pod: podRef{Namespace: testNS, Name: testPod, UID: testPodUID}, Container: testCtr, Nodes: []int{2}}
	if !reflect.DeepEqual(rec, want) {
		t.Fatalf("loaded %+v", rec)
	}
	data, err := json.Marshal(&rec)
	noErr(t, err)
	if string(data) != old {
		t.Fatalf("encoded %s", data)
	}
	if cfg := rec.config(); cfg.Kind != device.LendConfigKind || cfg.Lease != rec.Lease || cfg.Pod != rec.Pod || cfg.Container != rec.Container {
		t.Fatalf("config %+v", cfg)
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
	p.lender.set(nil, &broker{store: &leaseStore{Leases: map[string]*lease{}}, changed: make(chan struct{})})
	wantErr(t, prepare(t, p, claim).Err, "lending unavailable: NRI not connected")
	if strings.Contains(pe.f.commands(), "create-region") {
		t.Fatalf("device touched for a refused lend:\n%s", pe.f.commands())
	}
}

// withDriverConfig appends an opaque config of driver to a claim's
// allocation.
func withDriverConfig(c *resourceapi.ResourceClaim, driver string, source resourceapi.AllocationConfigSource, requests []string, params string) *resourceapi.ResourceClaim {
	c.Status.Allocation.Devices.Config = append(c.Status.Allocation.Devices.Config, resourceapi.DeviceAllocationConfiguration{
		Source:   source,
		Requests: requests,
		DeviceConfiguration: resourceapi.DeviceConfiguration{Opaque: &resourceapi.OpaqueDeviceConfiguration{
			Driver: driver, Parameters: runtime.RawExtension{Raw: []byte(params)},
		}},
	})
	return c
}

func TestParseDeviceConfig(t *testing.T) {
	const (
		class = resourceapi.AllocationConfigSourceClass
		claim = resourceapi.AllocationConfigSourceClaim
		pool  = device.PoolDriverName
		local = device.DriverName
	)
	lend := `{"apiVersion":"cxl-pool.generic/v1alpha1","kind":"LendConfig","lease":"l1"}`
	localDax := `{"apiVersion":"cxl.generic/v1alpha1","kind":"LocalDeviceConfig","mode":"dax"}`
	localRO := `{"apiVersion":"cxl.generic/v1alpha1","kind":"LocalDeviceConfig","mode":"dax","readOnly":true}`
	poolDax := deviceConfig{APIVersion: "cxl-pool.generic/v1alpha1", Kind: "PoolDeviceConfig", Mode: "dax"}
	for _, tc := range []struct {
		name, driver, kind, request string
		configs                     func(c *resourceapi.ResourceClaim)
		want                        deviceConfig
		err                         string
	}{
		{name: "none", driver: pool, kind: device.PoolDeviceConfigKind, request: "mem", configs: func(c *resourceapi.ResourceClaim) {}},
		{name: "claim wins over class", driver: pool, kind: device.PoolDeviceConfigKind, request: "mem", configs: func(c *resourceapi.ResourceClaim) {
			withConfig(c, claim, nil, daxConfig)
			withConfig(c, class, nil, roConfig)
		}, want: poolDax},
		{name: "class only", driver: pool, kind: device.PoolDeviceConfigKind, request: "mem", configs: func(c *resourceapi.ResourceClaim) {
			withConfig(c, class, nil, roConfig)
		}, want: deviceConfig{APIVersion: "cxl-pool.generic/v1alpha1", Kind: "PoolDeviceConfig", ReadOnly: true}},
		{name: "another request", driver: pool, kind: device.PoolDeviceConfigKind, request: "mem", configs: func(c *resourceapi.ResourceClaim) {
			withConfig(c, claim, []string{"other"}, daxConfig)
		}},
		{name: "subrequest", driver: pool, kind: device.PoolDeviceConfigKind, request: "mem/a", configs: func(c *resourceapi.ResourceClaim) {
			withConfig(c, claim, []string{"mem"}, daxConfig)
		}, want: poolDax},
		{name: "other kinds and drivers", driver: pool, kind: device.PoolDeviceConfigKind, request: "mem", configs: func(c *resourceapi.ResourceClaim) {
			withConfig(c, claim, nil, lend)
			withDriverConfig(c, local, claim, nil, daxConfig)
		}},
		{name: "local class and claim", driver: local, kind: device.LocalDeviceConfigKind, request: "mem", configs: func(c *resourceapi.ResourceClaim) {
			withDriverConfig(c, local, claim, nil, localRO)
			withDriverConfig(c, local, class, nil, localDax)
		}, want: deviceConfig{APIVersion: "cxl.generic/v1alpha1", Kind: "LocalDeviceConfig", Mode: "dax", ReadOnly: true}},
		{name: "local ignores pool kind", driver: local, kind: device.LocalDeviceConfigKind, request: "mem", configs: func(c *resourceapi.ResourceClaim) {
			withDriverConfig(c, local, claim, nil, `{"apiVersion":"cxl.generic/v1alpha1","kind":"PoolDeviceConfig","mode":"dax"}`)
		}},
		// firstAvailable of a companion claim: each config lists the
		// subrequest of its driver
		{name: "subrequest of this driver", driver: local, kind: device.LocalDeviceConfigKind, request: "lease/local", configs: func(c *resourceapi.ResourceClaim) {
			withDriverConfig(c, local, claim, []string{"lease/local"}, localDax)
			withDriverConfig(c, local, claim, []string{"lease/pool"}, localRO)
		}, want: deviceConfig{APIVersion: "cxl.generic/v1alpha1", Kind: "LocalDeviceConfig", Mode: "dax"}},
		{name: "subrequest of another", driver: pool, kind: device.PoolDeviceConfigKind, request: "lease/pool", configs: func(c *resourceapi.ResourceClaim) {
			withConfig(c, claim, []string{"lease/local"}, daxConfig)
		}},
		{name: "unknown field", driver: local, kind: device.LocalDeviceConfigKind, request: "mem", configs: func(c *resourceapi.ResourceClaim) {
			withDriverConfig(c, local, claim, nil, `{"apiVersion":"cxl.generic/v1alpha1","kind":"LocalDeviceConfig","slice":"1"}`)
		}, err: `invalid LocalDeviceConfig from FromClaim config: json: unknown field "slice"`},
		{name: "wrong apiVersion", driver: local, kind: device.LocalDeviceConfigKind, request: "mem", configs: func(c *resourceapi.ResourceClaim) {
			withDriverConfig(c, local, class, nil, `{"apiVersion":"cxl-pool.generic/v1alpha1","kind":"LocalDeviceConfig"}`)
		}, err: `LocalDeviceConfig: apiVersion "cxl-pool.generic/v1alpha1", expected "cxl.generic/v1alpha1"`},
		{name: "not json", driver: local, kind: device.LocalDeviceConfigKind, request: "mem", configs: func(c *resourceapi.ResourceClaim) {
			withDriverConfig(c, local, class, nil, `{"kind":`)
		}, err: "invalid opaque config for cxl.generic"},
	} {
		c := poolClaim("u1", "c1", "shared0", sharedData)
		tc.configs(c)
		got, err := parseDeviceConfig(c, tc.driver, tc.kind, tc.request)
		switch {
		case tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)):
			t.Errorf("%s: expected error %q, got %v", tc.name, tc.err, err)
		case tc.err == "" && (err != nil || *got != tc.want):
			t.Errorf("%s: got %+v %v, expected %+v", tc.name, got, err, tc.want)
		}
	}
}

func TestDeviceConfigModeFor(t *testing.T) {
	for _, tc := range []struct {
		cfg    deviceConfig
		shared bool
		want   string
		err    string
	}{
		{shared: true, want: device.ModeDax},
		{want: device.ModeNUMA},
		{cfg: deviceConfig{Mode: "dax", ReadOnly: true}, want: device.ModeDax},
		{cfg: deviceConfig{Mode: "dax", ReadOnly: true}, shared: true, want: device.ModeDax},
		{cfg: deviceConfig{Kind: "PoolDeviceConfig", Mode: "numa"}, shared: true, err: "PoolDeviceConfig: mode numa: a shared device is used as dax only"},
		{cfg: deviceConfig{Kind: "LocalDeviceConfig", ReadOnly: true}, err: "LocalDeviceConfig: readOnly needs mode dax"},
		{cfg: deviceConfig{Kind: "LocalDeviceConfig", Mode: "kmem"}, err: `LocalDeviceConfig: invalid mode "kmem" (numa or dax)`},
	} {
		got, err := tc.cfg.modeFor(tc.shared)
		if (tc.err == "" && (err != nil || got != tc.want)) || (tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err))) {
			t.Errorf("%+v shared %v: got %q %v, expected %q %q", tc.cfg, tc.shared, got, err, tc.want, tc.err)
		}
	}
}

func TestDaxCDIDevice(t *testing.T) {
	got := daxCDIDevice("local-u1", "CXL_LOCAL_", "c100e2e1", "/dev/dax1.2", 252, 2, 64<<20, false)
	want := cdiSpecs.Device{Name: "local-u1", ContainerEdits: cdiSpecs.ContainerEdits{
		Env:         []string{"CXL_LOCAL_DAX_C100E2E1=/dev/dax1.2", "CXL_LOCAL_SIZE_C100E2E1=67108864", "CXL_LOCAL_SERIAL_C100E2E1=0xc100e2e1"},
		DeviceNodes: []*cdiSpecs.DeviceNode{{Path: "/dev/dax1.2", Type: "c", Major: 252, Minor: 2}},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v", got)
	}
	got = daxCDIDevice("pool-c1ae0001-ro", "CXL_SHARED_", "c1ae0001", "/dev/dax0.0", 251, 3, 1<<28, true)
	want = cdiSpecs.Device{Name: "pool-c1ae0001-ro", ContainerEdits: cdiSpecs.ContainerEdits{
		Env: []string{"CXL_SHARED_DAX_C1AE0001=/dev/dax0.0", "CXL_SHARED_SIZE_C1AE0001=268435456", "CXL_SHARED_SERIAL_C1AE0001=0xc1ae0001",
			"CXL_SHARED_READONLY_C1AE0001=1"},
		DeviceNodes: []*cdiSpecs.DeviceNode{{Path: "/dev/dax0.0", Type: "c", Major: 251, Minor: 3, Permissions: "r"}},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("read-only: got %+v", got)
	}
}
