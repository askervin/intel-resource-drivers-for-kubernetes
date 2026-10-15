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
	"k8s.io/apimachinery/pkg/types"
	cdiSpecs "tags.cncf.io/container-device-interface/specs-go"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/api"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/memorypolicy"
)

// fakeHooks records the leaseHooks calls.
type fakeHooks struct {
	mu         sync.Mutex
	calls      []string
	lendingErr error
	devs       []lentDevice
}

func (h *fakeHooks) OnLending(id string, claim *resourceapi.ResourceClaim) (*lendRecord, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, "lending "+id+" "+claim.Name)
	if h.lendingErr != nil {
		return nil, h.lendingErr
	}
	return &lendRecord{Lease: id, Pod: podRef{Namespace: testNS, Name: testPod, UID: testPodUID}, Container: testCtr}, nil
}

func (h *fakeHooks) OnPrepared(id string, devs []lentDevice) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, "prepared "+id)
	h.devs = devs
}

func (h *fakeHooks) OnLendFailed(id string, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, "failed "+id+": "+err.Error())
}

func (h *fakeHooks) OnReleasing(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, "releasing "+id)
}

func (h *fakeHooks) log() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return strings.Join(h.calls, "\n")
}

var _ leaseHooks = (*fakeHooks)(nil)

// revokeFailer is a nodeLender whose RevokeNode fails.
type revokeFailer struct {
	*fakeLender
}

func (r revokeFailer) RevokeNode(ctx context.Context, podUID, name string, node int, holder string) error {
	_ = r.fakeLender.RevokeNode(ctx, podUID, name, node, holder)
	return fmt.Errorf("node %d busy", node)
}

// companionClaim returns the companion claim of a lease as the broker
// creates it: name, namespace, label and owner.
func companionClaim(id string) *resourceapi.ResourceClaim {
	return &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{
		Name:            companionName(id),
		Namespace:       testNS,
		UID:             types.UID("uid-" + companionName(id)),
		Labels:          map[string]string{device.LeaseLabel: id},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: testPod, UID: testPodUID, Controller: ptr(true)}},
	}}
}

func testLendRecord() *lendRecord {
	return &lendRecord{Lease: "l1", Pod: podRef{Namespace: testNS, Name: testPod, UID: testPodUID}, Container: testCtr}
}

func TestLeaseOf(t *testing.T) {
	if id := leaseOf(companionClaim("1a2b3c4d")); id != "1a2b3c4d" {
		t.Fatalf("lease of a companion claim %q", id)
	}
	if id := leaseOf(&resourceapi.ResourceClaim{}); id != "" {
		t.Fatalf("lease of another claim %q", id)
	}
}

func TestCompanionLenderDisabled(t *testing.T) {
	claim := companionClaim("l1")
	c := newCompanionLender(device.DriverName)
	_, err := c.check("l1", claim)
	wantErr(t, err, "lease l1: lending to running containers is disabled (--leases=false)")
	hooks := &fakeHooks{}
	c.set(nil, hooks)
	if _, err := c.check("l1", claim); !errors.Is(err, errLendingUnavailable) {
		t.Fatalf("check without NRI: %v", err)
	}
	// numa devices cannot be lent without NRI, dax devices need no lend
	rec := testLendRecord()
	if err := c.lend(context.Background(), rec, []lentDevice{{Mode: api.ModeNUMA, Node: 3}}); !errors.Is(err, errLendingUnavailable) || len(rec.Nodes) != 0 {
		t.Fatalf("lend without NRI: %+v %v", rec, err)
	}
	noErr(t, c.lend(context.Background(), rec, []lentDevice{{Mode: api.ModeDax, Node: -1, DaxDevice: "/dev/dax0.1"}}))
	if rec.Lease != "l1" || rec.Container != testCtr || len(rec.Nodes) != 0 {
		t.Fatalf("dax lend record %+v", rec)
	}
	// revoking without NRI only warns
	noErr(t, c.revoke(context.Background(), &lendRecord{Lease: "l1", Nodes: []int{3}}, "ns/c"))
	if log := hooks.log(); log != "" {
		t.Fatalf("hooks called: %s", log)
	}

	// a nil companionLender is disabled and safe to use
	var n *companionLender
	n.set(newFakeLender(), hooks)
	n.prepared("l1", nil)
	n.failed("l1", errors.New("x"))
	n.releasing("l1")
	n.reprepared(&lendRecord{Lease: "l1"}, claim, nil)
	if hooks.log() != "" {
		t.Fatalf("nil companionLender: hooks %s", hooks.log())
	}
	_, err = n.check("l1", claim)
	wantErr(t, err, "disabled")
}

func TestCompanionLenderLendRevoke(t *testing.T) {
	ctx := context.Background()
	claim := companionClaim("l1")
	nodes := newFakeLender(testRef)
	hooks := &fakeHooks{}
	c := newCompanionLender(device.PoolDriverName)
	c.set(nodes, hooks)

	rec, err := c.check("l1", claim)
	noErr(t, err)
	if !reflect.DeepEqual(rec, testLendRecord()) {
		t.Fatalf("lend of the lease %+v", rec)
	}
	hooks.lendingErr = errors.New("lease l1 is Failed")
	_, err = c.check("l1", claim)
	wantErr(t, err, "lease l1 is Failed")
	hooks.lendingErr = nil

	devs := []lentDevice{
		{Mode: api.ModeNUMA, Node: 2},
		{Mode: api.ModeDax, Node: -1, DaxDevice: "/dev/dax0.0"},
		{Mode: api.ModeNUMA, Node: -1},
		{Mode: api.ModeNUMA, Node: 3},
	}
	noErr(t, c.lend(ctx, rec, devs))
	want := &lendRecord{Lease: "l1", Pod: rec.Pod, Container: testCtr, Nodes: []int{2, 3}}
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

	// a failing LendNode leaves the nodes lent so far, including the
	// failed one, in the record, so that a revoke cleans up
	nodes.mu.Lock()
	nodes.lendErr = errSteered
	nodes.mu.Unlock()
	rec = testLendRecord()
	if err := c.lend(ctx, rec, devs); !errors.Is(err, errSteered) || !reflect.DeepEqual(rec.Nodes, []int{2}) {
		t.Fatalf("failed lend: %+v %v", rec, err)
	}

	// revoke errors are joined
	c.set(revokeFailer{nodes}, hooks)
	err = c.revoke(ctx, &lendRecord{Lease: "l1", Pod: rec.Pod, Container: testCtr, Nodes: []int{2, 3}}, "ns/c")
	wantErr(t, err, "revoking node 2")
	wantErr(t, err, "node 3 busy")

	// hook forwarding
	hooks.calls = nil
	c.prepared("l1", devs[:1])
	c.failed("l1", errors.New("boom"))
	c.releasing("l1")
	c.reprepared(&lendRecord{Lease: "l2", Pod: rec.Pod, Container: testCtr}, companionClaim("l2"), devs[1:2])
	hooks.lendingErr = errors.New("unknown lease")
	c.reprepared(&lendRecord{Lease: "l3"}, companionClaim("l3"), devs)
	if got, want := hooks.log(), "prepared l1\nfailed l1: boom\nreleasing l1\nlending l2 cxl-lease-l2\nprepared l2\nlending l3 cxl-lease-l3"; got != want {
		t.Fatalf("hook calls:\n%s\nexpected:\n%s", got, want)
	}
	if !reflect.DeepEqual(hooks.devs, devs[1:2]) {
		t.Fatalf("prepared devices %+v", hooks.devs)
	}
}

func TestLendRecordJSON(t *testing.T) {
	enc := `{"lease":"000000a1","pod":{"namespace":"demo","name":"borrower","uid":"pod-uid-1"},"container":"app","nodes":[2]}`
	var rec lendRecord
	noErr(t, json.Unmarshal([]byte(enc), &rec))
	want := lendRecord{Lease: "000000a1", Pod: podRef{Namespace: testNS, Name: testPod, UID: testPodUID}, Container: testCtr, Nodes: []int{2}}
	if !reflect.DeepEqual(rec, want) {
		t.Fatalf("loaded %+v", rec)
	}
	data, err := json.Marshal(&rec)
	noErr(t, err)
	if string(data) != enc {
		t.Fatalf("encoded %s", data)
	}
}

// TestPoolCompanionRefused prepares a companion claim through the pool
// driver without a broker or a lender: nothing is touched.
func TestPoolCompanionRefused(t *testing.T) {
	pe := newPoolTestEnv(t)
	pe.f.plug("0xc1ee0001")
	p := pe.driver(t)
	claim := poolClaim("u9", "c9", "pooled0", exclusiveData)
	claim.Namespace = testNS
	claim.Labels = map[string]string{device.LeaseLabel: "abc"}
	wantErr(t, prepare(t, p, claim).Err, "lending to running containers is disabled (--leases=false)")
	p.lender.set(nil, &broker{store: &leaseStore{Leases: map[string]*lease{}}, changed: make(chan struct{})})
	wantErr(t, prepare(t, p, claim).Err, "lending unavailable: NRI not connected")
	p.lender.set(newFakeLender(testRef), &broker{store: &leaseStore{Leases: map[string]*lease{}}, changed: make(chan struct{})})
	wantErr(t, prepare(t, p, claim).Err, "claim demo/c9: not the companion of lease abc")
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

func TestParseConfigDeviceConfig(t *testing.T) {
	const (
		class = resourceapi.AllocationConfigSourceClass
		claim = resourceapi.AllocationConfigSourceClaim
		pool  = device.PoolDriverName
		local = device.DriverName
	)
	policy := `{"apiVersion":"cxl.generic/v1alpha1","kind":"MemoryPolicyConfig","memoryUseOrder":"first-dram"}`
	localDax := `{"apiVersion":"cxl.generic/v1alpha1","kind":"DeviceConfig","mode":"dax"}`
	localRO := `{"apiVersion":"cxl.generic/v1alpha1","kind":"DeviceConfig","mode":"dax","readOnly":true}`
	poolDax := deviceConfig{APIVersion: "cxl-pool.generic/v1alpha1", Kind: "DeviceConfig", Mode: "dax"}
	for _, tc := range []struct {
		name, driver, request string
		configs               func(c *resourceapi.ResourceClaim)
		want                  deviceConfig
		err                   string
	}{
		{name: "none", driver: pool, request: "mem", configs: func(c *resourceapi.ResourceClaim) {}},
		{name: "claim replaces class", driver: pool, request: "mem", configs: func(c *resourceapi.ResourceClaim) {
			withConfig(c, claim, nil, daxConfig)
			withConfig(c, class, nil, roConfig)
		}, want: poolDax},
		{name: "class only", driver: pool, request: "mem", configs: func(c *resourceapi.ResourceClaim) {
			withConfig(c, class, nil, roConfig)
		}, want: deviceConfig{APIVersion: "cxl-pool.generic/v1alpha1", Kind: "DeviceConfig", ReadOnly: true}},
		{name: "another request", driver: pool, request: "mem", configs: func(c *resourceapi.ResourceClaim) {
			withConfig(c, claim, []string{"other"}, daxConfig)
		}},
		{name: "subrequest", driver: pool, request: "mem/a", configs: func(c *resourceapi.ResourceClaim) {
			withConfig(c, claim, []string{"mem"}, daxConfig)
		}, want: poolDax},
		{name: "another kind and another driver", driver: local, request: "mem", configs: func(c *resourceapi.ResourceClaim) {
			withDriverConfig(c, local, claim, nil, policy)
			withConfig(c, claim, nil, daxConfig)
		}},
		{name: "local class and claim", driver: local, request: "mem", configs: func(c *resourceapi.ResourceClaim) {
			withDriverConfig(c, local, claim, nil, localRO)
			withDriverConfig(c, local, class, nil, localDax)
			withDriverConfig(c, local, claim, nil, policy)
		}, want: deviceConfig{APIVersion: "cxl.generic/v1alpha1", Kind: "DeviceConfig", Mode: "dax", ReadOnly: true}},
		// firstAvailable of a companion claim: each config lists the
		// subrequest of its driver
		{name: "subrequest of this driver", driver: local, request: "lease/local", configs: func(c *resourceapi.ResourceClaim) {
			withDriverConfig(c, local, claim, []string{"lease/local"}, localDax)
			withDriverConfig(c, local, claim, []string{"lease/pool"}, localRO)
		}, want: deviceConfig{APIVersion: "cxl.generic/v1alpha1", Kind: "DeviceConfig", Mode: "dax"}},
		{name: "subrequest of another", driver: pool, request: "lease/pool", configs: func(c *resourceapi.ResourceClaim) {
			withConfig(c, claim, []string{"lease/local"}, daxConfig)
		}},
		{name: "unknown field", driver: local, request: "mem", configs: func(c *resourceapi.ResourceClaim) {
			withDriverConfig(c, local, claim, nil, `{"apiVersion":"cxl.generic/v1alpha1","kind":"DeviceConfig","slice":"1"}`)
		}, err: `DeviceConfig: invalid FromClaim config: json: unknown field "slice"`},
		{name: "wrong apiVersion", driver: local, request: "mem", configs: func(c *resourceapi.ResourceClaim) {
			withDriverConfig(c, local, class, nil, `{"apiVersion":"cxl-pool.generic/v1alpha1","kind":"DeviceConfig"}`)
		}, err: `DeviceConfig: apiVersion "cxl-pool.generic/v1alpha1", expected "cxl.generic/v1alpha1"`},
		{name: "not json", driver: local, request: "mem", configs: func(c *resourceapi.ResourceClaim) {
			withDriverConfig(c, local, class, nil, `{"kind":`)
		}, err: "invalid opaque FromClass config for cxl.generic"},
	} {
		c := poolClaim("u1", "c1", "shared0", sharedData)
		tc.configs(c)
		got := &deviceConfig{}
		_, err := parseConfig(c, tc.driver, device.DeviceConfigKind, tc.request, got)
		switch {
		case tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)):
			t.Errorf("%s: expected error %q, got %v", tc.name, tc.err, err)
		case tc.err == "" && (err != nil || *got != tc.want):
			t.Errorf("%s: got %+v %v, expected %+v", tc.name, got, err, tc.want)
		}
	}
}

// TestParseConfigWithoutRequest decodes a MemoryPolicyConfig, which is per
// claim: every config of the kind applies, whatever requests it lists.
func TestParseConfigWithoutRequest(t *testing.T) {
	c := poolClaim("u1", "c1", "shared0", sharedData)
	withDriverConfig(c, device.DriverName, resourceapi.AllocationConfigSourceClaim, []string{"cxl"},
		`{"apiVersion":"cxl.generic/v1alpha1","kind":"MemoryPolicyConfig","memoryUseOrder":"first-dram"}`)
	var cfg memorypolicy.MemoryPolicyConfig
	found, err := parseConfig(c, device.DriverName, memorypolicy.Kind, "", &cfg)
	if err != nil || !found || cfg.MemoryUseOrder != "first-dram" {
		t.Fatalf("policy %+v found %v %v", cfg, found, err)
	}
	found, err = parseConfig(c, device.DriverName, memorypolicy.Kind, "dram", &cfg)
	if err != nil || found {
		t.Fatalf("policy of another request found %v %v", found, err)
	}
	// a later config replaces an earlier one as a whole
	withDriverConfig(c, device.DriverName, resourceapi.AllocationConfigSourceClaim, nil,
		`{"apiVersion":"cxl.generic/v1alpha1","kind":"MemoryPolicyConfig","memoryUseOrder":"first-cxl","maxStep":"8Mi"}`)
	withDriverConfig(c, device.DriverName, resourceapi.AllocationConfigSourceClaim, nil,
		`{"apiVersion":"cxl.generic/v1alpha1","kind":"MemoryPolicyConfig","memoryUseOrder":"interleave"}`)
	found, err = parseConfig(c, device.DriverName, memorypolicy.Kind, "", &cfg)
	if err != nil || !found || cfg.MemoryUseOrder != "interleave" || cfg.MaxStep != "" {
		t.Fatalf("replaced policy %+v found %v %v", cfg, found, err)
	}
}

func TestDeviceConfigModeFor(t *testing.T) {
	for _, tc := range []struct {
		cfg    deviceConfig
		shared bool
		want   string
		err    string
	}{
		{shared: true, want: api.ModeDax},
		{want: api.ModeNUMA},
		{cfg: deviceConfig{Mode: "dax", ReadOnly: true}, want: api.ModeDax},
		{cfg: deviceConfig{Mode: "dax", ReadOnly: true}, shared: true, want: api.ModeDax},
		{cfg: deviceConfig{Mode: "numa"}, shared: true, err: "DeviceConfig: mode numa: a shared device is used as dax only"},
		{cfg: deviceConfig{ReadOnly: true}, err: "DeviceConfig: readOnly needs mode dax"},
		{cfg: deviceConfig{Mode: "kmem"}, err: `DeviceConfig: invalid mode "kmem" (numa or dax)`},
	} {
		got, err := tc.cfg.modeFor(tc.shared)
		if (tc.err == "" && (err != nil || got != tc.want)) || (tc.err != "" && (err == nil || err.Error() != tc.err)) {
			t.Errorf("%+v shared %v: got %q %v, expected %q %q", tc.cfg, tc.shared, got, err, tc.want, tc.err)
		}
	}
}

func TestDaxDeviceNode(t *testing.T) {
	if got, want := daxDeviceNode("/dev/dax1.2", 252, 2, false), (&cdiSpecs.DeviceNode{Path: "/dev/dax1.2", Type: "c", Major: 252, Minor: 2}); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v", got)
	}
	if got, want := daxDeviceNode("/dev/dax0.0", 251, 3, true), (&cdiSpecs.DeviceNode{Path: "/dev/dax0.0", Type: "c", Major: 251, Minor: 3, Permissions: "r"}); !reflect.DeepEqual(got, want) {
		t.Fatalf("read-only: got %+v", got)
	}
}

func TestEnvKeys(t *testing.T) {
	keys := envKeys{}
	var got []string
	for _, base := range []string{"C100E2E1", "REGION0", "C100E2E1", "DRAM", "C100E2E1"} {
		got = append(got, keys.next(base))
	}
	if want := []string{"C100E2E1", "REGION0", "C100E2E1_2", "DRAM", "C100E2E1_3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("keys %v", got)
	}
}

func TestCheckConfigKinds(t *testing.T) {
	const (
		local = device.DriverName
		pool  = device.PoolDriverName
	)
	kinds := map[string][]string{local: {device.DeviceConfigKind, memorypolicy.Kind}, pool: {device.DeviceConfigKind}}
	for _, tc := range []struct {
		name, driver, params, err string
	}{
		{"DeviceConfig", local, `{"apiVersion":"cxl.generic/v1alpha1","kind":"DeviceConfig","mode":"dax"}`, ""},
		{"MemoryPolicyConfig", local, `{"apiVersion":"cxl.generic/v1alpha1","kind":"MemoryPolicyConfig"}`, ""},
		{"pool DeviceConfig", pool, daxConfig, ""},
		{"LocalDeviceConfig", local, `{"apiVersion":"cxl.generic/v1alpha1","kind":"LocalDeviceConfig","mode":"dax"}`,
			`unknown kind "LocalDeviceConfig" (DeviceConfig or MemoryPolicyConfig)`},
		{"PoolDeviceConfig", pool, `{"apiVersion":"cxl-pool.generic/v1alpha1","kind":"PoolDeviceConfig","readOnly":true}`,
			`unknown kind "PoolDeviceConfig" (DeviceConfig)`},
		{"pool MemoryPolicyConfig", pool, `{"apiVersion":"cxl-pool.generic/v1alpha1","kind":"MemoryPolicyConfig"}`,
			`unknown kind "MemoryPolicyConfig" (DeviceConfig)`},
		{"no kind", local, `{"apiVersion":"cxl.generic/v1alpha1","mode":"dax"}`, `unknown kind "" (DeviceConfig or MemoryPolicyConfig)`},
		{"not json", pool, `{"kind":`, "invalid opaque FromClass config for cxl-pool.generic"},
	} {
		for _, driver := range []string{local, pool} {
			c := poolClaim("u1", "c1", "shared0", sharedData)
			withDriverConfig(c, tc.driver, resourceapi.AllocationConfigSourceClass, nil, tc.params)
			err := checkConfigKinds(c, driver, kinds[driver]...)
			switch {
			case driver != tc.driver && err != nil:
				t.Errorf("%s: the config of %s fails %s: %v", tc.name, tc.driver, driver, err)
			case driver == tc.driver && tc.err == "" && err != nil:
				t.Errorf("%s: %v", tc.name, err)
			case driver == tc.driver && tc.err != "" && (err == nil || !strings.HasPrefix(err.Error(), tc.err)):
				t.Errorf("%s: expected error %q, got %v", tc.name, tc.err, err)
			}
		}
	}
}

func TestDeviceConfigNUMAOnly(t *testing.T) {
	for _, tc := range []struct {
		cfg  deviceConfig
		what string
		err  string
	}{
		{deviceConfig{}, "region", ""},
		{deviceConfig{Mode: "numa"}, "dram", ""},
		{deviceConfig{Mode: "dax"}, "region", "DeviceConfig: mode dax: a region is used as numa only"},
		{deviceConfig{Mode: "dax", ReadOnly: true}, "dram", "DeviceConfig: mode dax: a dram is used as numa only"},
		{deviceConfig{ReadOnly: true}, "region", "DeviceConfig: readOnly needs mode dax"},
		{deviceConfig{Mode: "kmem"}, "dram", `DeviceConfig: invalid mode "kmem" (numa or dax)`},
	} {
		err := tc.cfg.numaOnly(tc.what)
		if (tc.err == "" && err != nil) || (tc.err != "" && (err == nil || err.Error() != tc.err)) {
			t.Errorf("%+v %s: %v, expected %q", tc.cfg, tc.what, err, tc.err)
		}
	}
}
