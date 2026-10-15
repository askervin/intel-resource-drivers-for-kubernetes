/*
 * Copyright (c) 2025, Intel Corporation.  All Rights Reserved.
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
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"sigs.k8s.io/yaml"
	cdiapi "tags.cncf.io/container-device-interface/pkg/cdi"
	cdiSpecs "tags.cncf.io/container-device-interface/specs-go"

	"github.com/containers/nri-plugins/pkg/cgmpolmgr"
	nricxl "github.com/containers/nri-plugins/pkg/cxl"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/api"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/memorypolicy"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/helpers"
)

func makeOpaqueConfig(driver string, source resourcev1.AllocationConfigSource, config interface{}) resourcev1.DeviceAllocationConfiguration {
	raw, _ := json.Marshal(config)
	return resourcev1.DeviceAllocationConfiguration{
		Source: source,
		DeviceConfiguration: resourcev1.DeviceConfiguration{
			Opaque: &resourcev1.OpaqueDeviceConfiguration{
				Driver: driver,
				Parameters: runtime.RawExtension{
					Raw: raw,
				},
			},
		},
	}
}

func makeClaim(uid string, configs ...resourcev1.DeviceAllocationConfiguration) *resourcev1.ResourceClaim {
	return &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-claim",
			Namespace: "test-ns",
			UID:       types.UID(uid),
		},
		Status: resourcev1.ResourceClaimStatus{
			Allocation: &resourcev1.AllocationResult{
				Devices: resourcev1.DeviceAllocationResult{
					Config: configs,
				},
			},
		},
	}
}

func TestParseMemoryPolicyFromClaim_NoConfig(t *testing.T) {
	claim := makeClaim("uid-1")
	policy, err := parseMemoryPolicy(claim)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if policy != nil {
		t.Fatalf("expected nil policy, got %+v", policy)
	}
}

func TestParseMemoryPolicyFromClaim_NoAllocation(t *testing.T) {
	claim := &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
	}
	policy, err := parseMemoryPolicy(claim)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if policy != nil {
		t.Fatalf("expected nil policy, got %+v", policy)
	}
}

func TestParseMemoryPolicyFromClaim_ClaimConfig(t *testing.T) {
	cfg := memorypolicy.MemoryPolicyConfig{
		APIVersion: memorypolicy.APIVersion,
		Kind:       memorypolicy.Kind,
		Policy: cgmpolmgr.Policy{
			MemoryUseOrder: "first-dram",
			MinStep:        "128M",
			MaxStep:        "1G",
		},
	}
	claim := makeClaim("uid-2",
		makeOpaqueConfig(device.DriverName, resourcev1.AllocationConfigSourceClaim, cfg),
	)

	policy, err := parseMemoryPolicy(claim)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if policy == nil {
		t.Fatal("expected non-nil policy")
	}
	if policy.MemoryUseOrder != "first-dram" {
		t.Errorf("expected order first-dram, got %s", policy.MemoryUseOrder)
	}
	if policy.MinStep != "128M" {
		t.Errorf("expected minStep 128M, got %s", policy.MinStep)
	}
	if policy.MaxStep != "1G" {
		t.Errorf("expected maxStep 1G, got %s", policy.MaxStep)
	}
}

func TestParseMemoryPolicyFromClaim_ClassConfig(t *testing.T) {
	cfg := memorypolicy.MemoryPolicyConfig{
		APIVersion: memorypolicy.APIVersion,
		Kind:       memorypolicy.Kind,
		Policy: cgmpolmgr.Policy{
			MemoryUseOrder: "first-cxl",
		},
	}
	claim := makeClaim("uid-3",
		makeOpaqueConfig(device.DriverName, resourcev1.AllocationConfigSourceClass, cfg),
	)

	policy, err := parseMemoryPolicy(claim)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if policy == nil {
		t.Fatal("expected non-nil policy")
	}
	if policy.MemoryUseOrder != "first-cxl" {
		t.Errorf("expected order first-cxl, got %s", policy.MemoryUseOrder)
	}
}

func TestParseMemoryPolicyFromClaim_ClaimOverridesClass(t *testing.T) {
	classCfg := memorypolicy.MemoryPolicyConfig{
		APIVersion: memorypolicy.APIVersion,
		Kind:       memorypolicy.Kind,
		Policy: cgmpolmgr.Policy{
			MemoryUseOrder: "first-cxl",
		},
	}
	claimCfg := memorypolicy.MemoryPolicyConfig{
		APIVersion: memorypolicy.APIVersion,
		Kind:       memorypolicy.Kind,
		Policy: cgmpolmgr.Policy{
			MemoryUseOrder: "first-dram",
			MinStep:        "64M",
		},
	}
	claim := makeClaim("uid-4",
		makeOpaqueConfig(device.DriverName, resourcev1.AllocationConfigSourceClass, classCfg),
		makeOpaqueConfig(device.DriverName, resourcev1.AllocationConfigSourceClaim, claimCfg),
	)

	policy, err := parseMemoryPolicy(claim)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if policy == nil {
		t.Fatal("expected non-nil policy")
	}
	if policy.MemoryUseOrder != "first-dram" {
		t.Errorf("expected claim-level order first-dram, got %s", policy.MemoryUseOrder)
	}
	if policy.MinStep != "64M" {
		t.Errorf("expected claim-level minStep 64M, got %s", policy.MinStep)
	}
}

func TestParseMemoryPolicyFromClaim_WrongDriver(t *testing.T) {
	cfg := memorypolicy.MemoryPolicyConfig{
		APIVersion: memorypolicy.APIVersion,
		Kind:       memorypolicy.Kind,
		Policy: cgmpolmgr.Policy{
			MemoryUseOrder: "first-dram",
		},
	}
	claim := makeClaim("uid-5",
		makeOpaqueConfig("other.driver", resourcev1.AllocationConfigSourceClaim, cfg),
	)

	policy, err := parseMemoryPolicy(claim)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if policy != nil {
		t.Fatalf("expected nil policy for wrong driver, got %+v", policy)
	}
}

// A DeviceConfig is not a policy; a config of an unknown kind fails the
// Prepare (checkConfigKinds).
func TestParseMemoryPolicyFromClaim_OtherKinds(t *testing.T) {
	claim := makeClaim("uid-6",
		makeOpaqueConfig(device.DriverName, resourcev1.AllocationConfigSourceClaim, json.RawMessage(localDaxConfig)),
	)
	policy, err := parseMemoryPolicy(claim)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if policy != nil {
		t.Fatalf("expected nil policy for a DeviceConfig, got %+v", policy)
	}
	other := map[string]string{
		"apiVersion": memorypolicy.APIVersion,
		"kind":       "SomeOtherConfig",
		"foo":        "bar",
	}
	claim = makeClaim("uid-6", makeOpaqueConfig(device.DriverName, resourcev1.AllocationConfigSourceClaim, other))
	err = checkConfigKinds(claim, device.DriverName, device.DeviceConfigKind, memorypolicy.Kind)
	if err == nil || err.Error() != `unknown kind "SomeOtherConfig" (DeviceConfig or MemoryPolicyConfig)` {
		t.Fatalf("unknown kind: %v", err)
	}
}

func TestParseMemoryPolicyFromClaim_InvalidConfig(t *testing.T) {
	cfg := memorypolicy.MemoryPolicyConfig{
		APIVersion: memorypolicy.APIVersion,
		Kind:       memorypolicy.Kind,
		Policy: cgmpolmgr.Policy{
			MemoryUseOrder: "invalid-order",
		},
	}
	claim := makeClaim("uid-7",
		makeOpaqueConfig(device.DriverName, resourcev1.AllocationConfigSourceClaim, cfg),
	)

	_, err := parseMemoryPolicy(claim)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
}

func TestParseMemoryPolicyStrict(t *testing.T) {
	claim := makeClaim("uid-8", makeOpaqueConfig(device.DriverName, resourcev1.AllocationConfigSourceClaim,
		json.RawMessage(`{"apiVersion":"cxl.generic/v1alpha1","kind":"MemoryPolicyConfig","memoryUseOrdr":"first-dram"}`)))
	_, err := parseMemoryPolicy(claim)
	wantErr(t, err, `MemoryPolicyConfig: invalid FromClaim config: json: unknown field "memoryUseOrdr"`)
	claim = makeClaim("uid-9", makeOpaqueConfig(device.DriverName, resourcev1.AllocationConfigSourceClaim,
		json.RawMessage(`{"apiVersion":"cxl-pool.generic/v1alpha1","kind":"MemoryPolicyConfig","memoryUseOrder":"first-dram"}`)))
	_, err = parseMemoryPolicy(claim)
	wantErr(t, err, `MemoryPolicyConfig: apiVersion "cxl-pool.generic/v1alpha1", expected "cxl.generic/v1alpha1"`)
}

func TestPreparedClaimsInfo_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "preparedClaimsInfo.json")

	// Write claims info.
	claims := PreparedClaimsInfo{
		"uid-1": {
			ClaimName: "test-ns/test-claim",
			Policy: &memorypolicy.MemoryPolicyConfig{
				APIVersion: memorypolicy.APIVersion,
				Kind:       memorypolicy.Kind,
				Policy: cgmpolmgr.Policy{
					MemoryUseOrder: "first-dram",
					MinStep:        "128M",
					MaxStep:        "1G",
				},
			},
			Devices: []PreparedDeviceInfo{
				{
					RequestName:    "req1",
					DeviceName:     "region0-node2",
					DeviceType:     device.DeviceTypeRegion,
					Mode:           api.ModeNUMA,
					Region:         "region0",
					SysfsPath:      "/sys/bus/cxl/devices/region0",
					NUMANodes:      []int{2},
					NodeAffinities: []int{0},
					TotalBytes:     4294967296,
					ConsumedBytes:  1073741824,
				},
			},
		},
	}
	if err := writePreparedClaimsInfoToFile(filePath, claims); err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	// Read them back.
	loaded, err := getOrCreatePreparedClaimsInfo(filePath)
	if err != nil {
		t.Fatalf("failed to load: %v", err)
	}

	if len(loaded) != 1 {
		t.Fatalf("expected 1 claim, got %d", len(loaded))
	}
	info := loaded["uid-1"]
	if info == nil {
		t.Fatal("expected non-nil claim info for uid-1")
	}
	if info.ClaimName != "test-ns/test-claim" {
		t.Errorf("expected test-ns/test-claim, got %s", info.ClaimName)
	}
	if info.Policy == nil {
		t.Fatal("expected non-nil policy")
	}
	if info.Policy.MemoryUseOrder != "first-dram" {
		t.Errorf("expected first-dram, got %s", info.Policy.MemoryUseOrder)
	}
	if info.Policy.MinStep != "128M" {
		t.Errorf("expected 128M, got %s", info.Policy.MinStep)
	}
	if len(info.Devices) != 1 {
		t.Fatalf("expected 1 device, got %d", len(info.Devices))
	}
	dev := info.Devices[0]
	if got := [3]string{dev.DeviceType, dev.Mode, dev.Region}; got != [3]string{device.DeviceTypeRegion, api.ModeNUMA, "region0"} {
		t.Errorf("expected a numa region region0, got %+v", dev)
	}
	if dev.SysfsPath != "/sys/bus/cxl/devices/region0" {
		t.Errorf("expected sysfs path, got %s", dev.SysfsPath)
	}
	if dev.TotalBytes != 4294967296 {
		t.Errorf("expected 4GiB total, got %d", dev.TotalBytes)
	}
	if dev.ConsumedBytes != 1073741824 {
		t.Errorf("expected 1GiB consumed, got %d", dev.ConsumedBytes)
	}
	if len(dev.NUMANodes) != 1 || dev.NUMANodes[0] != 2 {
		t.Errorf("expected NUMA [2], got %v", dev.NUMANodes)
	}
}

func TestPreparedClaimsInfo_CreateEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "preparedClaimsInfo.json")

	claims, err := getOrCreatePreparedClaimsInfo(filePath)
	if err != nil {
		t.Fatalf("failed: %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("expected empty claims, got %d", len(claims))
	}

	// Verify the file exists.
	if _, err := os.Stat(filePath); err != nil {
		t.Fatalf("file should exist: %v", err)
	}
}

func TestPreparedClaimsInfo_NilPolicy(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "preparedClaimsInfo.json")

	claims := PreparedClaimsInfo{
		"uid-no-policy": {
			ClaimName: "ns/claim-no-policy",
			Devices: []PreparedDeviceInfo{
				{
					RequestName: "req1",
					DeviceName:  "dram-0",
					DeviceType:  device.DeviceTypeDRAM,
					NUMANodes:   []int{0, 1},
					TotalBytes:  8589934592,
				},
			},
		},
	}
	if err := writePreparedClaimsInfoToFile(filePath, claims); err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	loaded, err := getOrCreatePreparedClaimsInfo(filePath)
	if err != nil {
		t.Fatalf("failed to load: %v", err)
	}

	info := loaded["uid-no-policy"]
	if info == nil {
		t.Fatal("expected claim info")
	}
	if info.Policy != nil {
		t.Errorf("expected nil policy, got %+v", info.Policy)
	}
	if info.Devices[0].DeviceType != device.DeviceTypeDRAM {
		t.Errorf("expected %s, got %s", device.DeviceTypeDRAM, info.Devices[0].DeviceType)
	}
}

const localDeviceName = "memdev-c100e2e1"

// localMemdev returns the detected local memdev of a fakeLocalSys.
func localMemdev() *nricxl.MemoryDevice {
	return &nricxl.MemoryDevice{
		Name: "mem0", DevName: "cxl/mem0", SysfsPath: "/sys/bus/cxl/devices/mem0", RamSize: localRegionSize,
		Serial: localSerial, NodeAffinity: 0, Driver: "cxl_mem", Major: 251, Minor: 0, Enabled: true,
	}
}

func TestBuildDevInfosLocalMemdevs(t *testing.T) {
	memdev := func(name string, serial uint64) *nricxl.MemoryDevice {
		return &nricxl.MemoryDevice{Name: name, DevName: "cxl/" + name, RamSize: 1 << 30, Serial: serial, NodeAffinity: -1, Enabled: true}
	}
	free, foreign, pooled, ignored, used := memdev("mem1", 0xa1), memdev("mem2", 0xa2), memdev("mem3", 0xa3), memdev("mem4", 0xa4), memdev("mem5", 0xa5)
	free.NodeAffinity = 1
	disabled, noRAM := memdev("mem6", 0xa6), memdev("mem7", 0xa7)
	disabled.Enabled = false
	noRAM.RamSize = 0
	region := func(name string, node int, mem *nricxl.MemoryDevice) *nricxl.RegionDevice {
		return &nricxl.RegionDevice{Name: name, Size: 1 << 30, Mode: "ram", Node: node, Enabled: true, Memories: []*nricxl.MemoryDevice{mem}}
	}
	devices := &nricxl.Devices{
		MemoryDevices: []*nricxl.MemoryDevice{free, foreign, pooled, ignored, used, disabled, noRAM},
		RegionDevices: []*nricxl.RegionDevice{region("region0", 2, foreign), region("region1", 3, used)},
		MemoryNodes:   []*nricxl.MemoryNode{{ID: 0, Size: 1 << 30}, {ID: 2, Size: 1 << 30}, {ID: 3, Size: 1 << 30}},
	}
	cfg := &DriverConfig{IgnoreDevices: []string{"0xa4"}}
	local := &localSnapshot{Serials: map[uint64]bool{0xa5: true}, BlockSize: localBlockSize}
	devInfos, err := buildDevInfos(cfg, devices, map[uint64]bool{0xa3: true}, local)
	noErr(t, err)
	got := slices.Sorted(maps.Keys(devInfos))
	want := []string{"dram", "memdev-a1", "memdev-a5", "region0-node2"}
	if !slices.Equal(got, want) {
		t.Fatalf("published %v, expected %v", got, want)
	}
	for name, d := range devInfos {
		if d.Name != name {
			t.Fatalf("device %s named %s", name, d.Name)
		}
	}
	if m, ok := devInfos["memdev-a1"].Dev.(*device.Memdev); !ok || *m != (device.Memdev{Name: "mem1", Serial: 0xa1, Size: 1 << 30, NodeAffinity: 1, BlockSize: localBlockSize}) {
		t.Fatalf("free memdev %+v", devInfos["memdev-a1"].Dev)
	}
	if _, ok := devInfos["memdev-a5"].Dev.(*device.Memdev); !ok {
		t.Fatalf("used memdev %+v", devInfos["memdev-a5"].Dev)
	}
	// memdevs smaller than a block, or an unknown block size: none
	free.RamSize = localBlockSize - 2*mib
	devInfos, err = buildDevInfos(cfg, devices, map[uint64]bool{0xa3: true}, local)
	noErr(t, err)
	if _, ok := devInfos["memdev-a1"]; ok {
		t.Fatal("a memdev smaller than a memory block published")
	}
	free.RamSize = localBlockSize
	// a block of 512 MiB is not sure to fit 512 MiB
	devInfos, err = buildDevInfos(cfg, devices, map[uint64]bool{0xa3: true}, &localSnapshot{Serials: local.Serials, BlockSize: localBlockSize * 4})
	noErr(t, err)
	if _, ok := devInfos["memdev-a1"]; ok {
		t.Fatal("a memdev of one large block published")
	}
	m, ok := devInfos["memdev-a5"].Dev.(*device.Memdev)
	if !ok {
		t.Fatalf("used memdev %+v", devInfos["memdev-a5"])
	}
	if c := localMemdevDevice("x", m).Capacity["memory"]; c.Value.Value() != 512*mib {
		t.Fatalf("capacity %v of a 1 GiB memdev with 512 MiB blocks", c.Value)
	}
	devInfos, err = buildDevInfos(cfg, devices, map[uint64]bool{0xa3: true}, &localSnapshot{Serials: local.Serials})
	noErr(t, err)
	if got := slices.Sorted(maps.Keys(devInfos)); !slices.Equal(got, []string{"dram", "region0-node2"}) {
		t.Fatalf("published %v without a block size", got)
	}
	// the node of the plugin's region is not DRAM
	if dram, ok := devInfos["dram"].Dev.(*device.DRAM); !ok || !slices.Equal(dram.Nodes, []int{0}) {
		t.Fatalf("DRAM %+v", devInfos["dram"].Dev)
	}

	// manageMemdevs: false publishes regions only
	cfg.ManageMemdevs = ptr(false)
	devInfos, err = buildDevInfos(cfg, devices, map[uint64]bool{0xa3: true}, nil)
	noErr(t, err)
	if got := slices.Sorted(maps.Keys(devInfos)); !slices.Equal(got, []string{"dram", "region0-node2", "region1-node3"}) {
		t.Fatalf("published %v without managed memdevs", got)
	}
}

func TestGetResourcesLocalMemdev(t *testing.T) {
	devs, err := buildDevInfos(&DriverConfig{IgnoreDRAMNodes: true}, &nricxl.Devices{MemoryDevices: []*nricxl.MemoryDevice{localMemdev()}}, nil, &localSnapshot{BlockSize: localBlockSize})
	noErr(t, err)
	s := &nodeState{NodeState: &helpers.NodeState{Allocatable: devs, NodeName: testNode}}
	got := s.GetResources().Pools[testNode].Slices[0].Devices
	q := func(v int64) *resource.Quantity { return resource.NewQuantity(v, resource.BinarySI) }
	want := []resourcev1.Device{{
		Name: localDeviceName,
		Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
			"type":            {StringValue: ptr("memdev")},
			"source":          {StringValue: ptr("local")},
			"serial":          {StringValue: ptr("0xc100e2e1")},
			"shared":          {BoolValue: ptr(false)},
			"size":            {IntValue: ptr(int64(localRegionSize))},
			"nodeAffinity":    {IntValue: ptr(int64(0))},
			"memoryBlockSize": {IntValue: ptr(int64(134217728))},
		},
		Capacity: map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{
			"memory": {
				Value: *q(localRegionSize),
				RequestPolicy: &resourcev1.CapacityRequestPolicy{
					Default:    q(localRegionSize),
					ValidRange: &resourcev1.CapacityRequestPolicyRange{Min: q(134217728), Step: q(134217728)},
				},
			},
		},
		NodeAllocatableResources: memoryNodeAllocatableResources(),
		AllowMultipleAllocations: ptr(true),
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("published\n%+v\nexpected\n%+v", got, want)
	}
	if p := got[0].Capacity["memory"].RequestPolicy; p.Default.String() != "1Gi" || p.ValidRange.Min.String() != "128Mi" || p.ValidRange.Step.String() != "128Mi" {
		t.Fatalf("request policy %v %v %v", p.Default, p.ValidRange.Min, p.ValidRange.Step)
	}
	// no nodeAffinity when unknown
	d := localMemdevDevice("x", &device.Memdev{Size: localRegionSize, NodeAffinity: -1, BlockSize: localBlockSize})
	if _, ok := d.Attributes["nodeAffinity"]; ok {
		t.Fatalf("memdev %+v", d)
	}
}

// The devices of regions, DRAM and the lease device: every memory device
// has type, source and size.
func TestGetResourcesRegionDRAMLease(t *testing.T) {
	mem := func(affinity int) *nricxl.MemoryDevice {
		return &nricxl.MemoryDevice{Serial: 1, NodeAffinity: affinity, Enabled: true}
	}
	devices := &nricxl.Devices{
		RegionDevices: []*nricxl.RegionDevice{
			{Name: "region0", Size: 2 << 30, Mode: "ram", Node: 2, Enabled: true, Memories: []*nricxl.MemoryDevice{mem(0), mem(0)}},
			{Name: "region1", Size: 1 << 30, Mode: "ram", Node: 3, Enabled: true, Memories: []*nricxl.MemoryDevice{mem(0), mem(1)}},
		},
		MemoryNodes: []*nricxl.MemoryNode{{ID: 0, Size: 1 << 30}, {ID: 1, Size: 1 << 30}, {ID: 2, Size: 2 << 30}, {ID: 3, Size: 1 << 30}},
	}
	devs, err := buildDevInfos(&DriverConfig{PublishLeaseDevice: true, ManageMemdevs: ptr(false)}, devices, nil, nil)
	noErr(t, err)
	s := &nodeState{NodeState: &helpers.NodeState{Allocatable: devs, NodeName: testNode}}
	got := map[string]resourcev1.Device{}
	for _, d := range s.GetResources().Pools[testNode].Slices[0].Devices {
		got[d.Name] = d
	}
	if names := slices.Sorted(maps.Keys(got)); !slices.Equal(names, []string{"dram", "lease", "region0-node2", "region1-node3"}) {
		t.Fatalf("published %v", names)
	}
	wantAttrs := map[string]map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
		"region0-node2": {"type": {StringValue: ptr("region")}, "source": {StringValue: ptr("local")}, "node": {IntValue: ptr(int64(2))},
			"size": {IntValue: ptr(int64(2 << 30))}, "nodeAffinity": {IntValue: ptr(int64(0))}},
		// the memdevs of the region do not agree: no nodeAffinity
		"region1-node3": {"type": {StringValue: ptr("region")}, "source": {StringValue: ptr("local")}, "node": {IntValue: ptr(int64(3))},
			"size": {IntValue: ptr(int64(1 << 30))}},
		"dram": {"type": {StringValue: ptr("dram")}, "source": {StringValue: ptr("local")}, "nodes": {StringValue: ptr("0-1")},
			"size": {IntValue: ptr(int64(2 << 30))}},
		"lease": {"type": {StringValue: ptr("lease")}},
	}
	for name, want := range wantAttrs {
		if !reflect.DeepEqual(got[name].Attributes, want) {
			t.Errorf("%s: attributes %+v, expected %+v", name, got[name].Attributes, want)
		}
	}
	for _, name := range []string{"region0-node2", "region1-node3", "dram"} {
		d := got[name]
		c := d.Capacity["memory"]
		if c.Value.Value() != *d.Attributes["size"].IntValue || d.NodeAllocatableResources == nil || !*d.AllowMultipleAllocations {
			t.Errorf("%s: capacity %+v", name, d)
		}
	}
	if l := got["lease"]; l.Capacity != nil || !*l.AllowMultipleAllocations {
		t.Errorf("lease device %+v", l)
	}
	if _, ok := regionNodeAffinity(&nricxl.RegionDevice{Memories: []*nricxl.MemoryDevice{mem(-1)}}); ok {
		t.Error("an unknown affinity is published")
	}
}

// The request policy of a local memdev: steps of the memory block size,
// the whole device (its blocks) by default.
func TestLocalRequestPolicy(t *testing.T) {
	q := func(v int64) *resource.Quantity { return resource.NewQuantity(v, resource.BinarySI) }
	for _, tc := range []struct {
		size, bs uint64
		want     *resourcev1.CapacityRequestPolicy
	}{
		{localRegionSize, localBlockSize, &resourcev1.CapacityRequestPolicy{Default: q(localRegionSize),
			ValidRange: &resourcev1.CapacityRequestPolicyRange{Min: q(localBlockSize), Step: q(localBlockSize)}}},
		// rounded down to whole blocks
		{localRegionSize + 64*mib, localBlockSize, &resourcev1.CapacityRequestPolicy{Default: q(localRegionSize),
			ValidRange: &resourcev1.CapacityRequestPolicyRange{Min: q(localBlockSize), Step: q(localBlockSize)}}},
		{2 * localBlockSize, localBlockSize, &resourcev1.CapacityRequestPolicy{Default: q(2 * localBlockSize),
			ValidRange: &resourcev1.CapacityRequestPolicyRange{Min: q(localBlockSize), Step: q(localBlockSize)}}},
		// one block: a range needs room for min and one step
		{localBlockSize + 64*mib, localBlockSize, &resourcev1.CapacityRequestPolicy{Default: q(localBlockSize),
			ValidValues: []resource.Quantity{*q(localBlockSize)}}},
		{localRegionSize, 2 << 30, nil},
		{localRegionSize, 0, nil},
		// blocks larger than the region alignment: the head that a
		// region can have (block size - 256 MiB) is not counted
		{2 << 30, 512 * mib, &resourcev1.CapacityRequestPolicy{Default: q(1536 * mib),
			ValidRange: &resourcev1.CapacityRequestPolicyRange{Min: q(512 * mib), Step: q(512 * mib)}}},
		{768 * mib, 512 * mib, &resourcev1.CapacityRequestPolicy{Default: q(512 * mib),
			ValidValues: []resource.Quantity{*q(512 * mib)}}},
		{512 * mib, 512 * mib, nil},
		{4 << 30, 2 << 30, &resourcev1.CapacityRequestPolicy{Default: q(2 << 30),
			ValidValues: []resource.Quantity{*q(2 << 30)}}},
	} {
		if got := localRequestPolicy(tc.size, tc.bs); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("size %d, block size %d: policy %+v, expected %+v", tc.size, tc.bs, got, tc.want)
		}
	}
}

// localTestState is a nodeState of node testNode with the local memdev of
// a fakeLocalSys published, as newDriver sets it up.
type localTestState struct {
	*nodeState
	f      *fakeLocalSys
	cdiDir string
}

func newLocalTestState(t *testing.T) *localTestState {
	f := newFakeLocalSys(t)
	dir := t.TempDir()
	cdiDir := filepath.Join(dir, "cdi")
	noErr(t, os.MkdirAll(cdiDir, 0o755))
	cache, err := cdiapi.NewCache(cdiapi.WithSpecDirs(cdiDir), cdiapi.WithAutoRefresh(false))
	noErr(t, err)
	e := &localTestState{f: f, cdiDir: cdiDir}
	e.nodeState = &nodeState{
		NodeState: &helpers.NodeState{
			CdiCache:               cache,
			Prepared:               helpers.ClaimPreparations{},
			PreparedClaimsFilePath: filepath.Join(dir, "preparedClaims.json"),
			NodeName:               testNode,
		},
		config:         &DriverConfig{IgnoreDRAMNodes: true},
		preparedClaims: PreparedClaimsInfo{},
		claimsFilePath: filepath.Join(dir, preparedClaimsInfoFileName),
		podClaims:      map[string]map[string]bool{},
		local:          f.allocator(t, dir),
		lending:        newCompanionLender(device.DriverName),
		locks:          &nodeLocks{},
	}
	e.rescan()
	return e
}

// rescan publishes the local memdev as rescanAndPublish does.
func (e *localTestState) rescan() {
	devs, _ := buildDevInfos(e.config, &nricxl.Devices{MemoryDevices: []*nricxl.MemoryDevice{localMemdev()}}, nil, e.local.Snapshot())
	e.Lock()
	e.Allocatable = devs
	e.Unlock()
}

// inUse tells whether the local allocator uses the local memdev.
func (e *localTestState) inUse() bool {
	return e.local.Snapshot().uses(localSerial)
}

// cdiSpec returns the CDI spec of the claims on disk, nil if there is
// none.
func (e *localTestState) cdiSpec(t *testing.T) *cdiSpecs.Spec {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(e.cdiDir, device.ClaimsCDISpecName+".*"))
	if len(m) == 0 {
		return nil
	}
	data, err := os.ReadFile(m[0])
	noErr(t, err)
	spec := &cdiSpecs.Spec{}
	noErr(t, yaml.Unmarshal(data, spec))
	return spec
}

// localClaim returns a claim allocated the local memdev for request "mem",
// consuming bytes of it (0: no consumed capacity), with opaque cxl.generic
// claim configs.
func localClaim(uid string, consumed int64, configs ...string) *resourcev1.ResourceClaim {
	r := resourcev1.DeviceRequestAllocationResult{Request: "mem", Driver: device.DriverName, Pool: testNode, Device: localDeviceName}
	if consumed > 0 {
		r.ConsumedCapacity = map[resourcev1.QualifiedName]resource.Quantity{device.CapacityMemory: *resource.NewQuantity(consumed, resource.BinarySI)}
	}
	c := &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), Name: "claim-" + uid, Namespace: testNS},
		Status: resourcev1.ResourceClaimStatus{
			Allocation: &resourcev1.AllocationResult{
				Devices: resourcev1.DeviceAllocationResult{Results: []resourcev1.DeviceRequestAllocationResult{r}},
			},
		},
	}
	for _, params := range configs {
		c.Status.Allocation.Devices.Config = append(c.Status.Allocation.Devices.Config, makeOpaqueConfig(device.DriverName, resourcev1.AllocationConfigSourceClaim, json.RawMessage(params)))
	}
	return c
}

// localCompanion returns localClaim as the companion claim of lease l1.
func localCompanion(uid string, consumed int64, configs ...string) *resourcev1.ResourceClaim {
	c := localClaim(uid, consumed, configs...)
	c.ObjectMeta = companionClaim("l1").ObjectMeta
	c.UID = types.UID(uid)
	return c
}

const (
	localDaxConfig = `{"apiVersion":"cxl.generic/v1alpha1","kind":"DeviceConfig","mode":"dax"}`
	localROConfig  = `{"apiVersion":"cxl.generic/v1alpha1","kind":"DeviceConfig","mode":"dax","readOnly":true}`
)

func TestPrepareLocalNUMA(t *testing.T) {
	e := newLocalTestState(t)
	ctx := context.Background()
	// 64Mi is rounded up to a memory block
	noErr(t, e.Prepare(ctx, localClaim("u1", 64*mib)))
	if !e.inUse() {
		t.Fatal("memdev not in use")
	}
	res := e.Prepared["u1"]
	if len(res.Devices) != 1 || !slices.Equal(res.Devices[0].CDIDeviceIDs, []string{"generic/cxl=claim-u1-0"}) {
		t.Fatalf("prepared %+v", res.Devices)
	}
	e.wantDeviceInfo(t, "u1", PreparedDeviceInfo{
		RequestName: "mem", DeviceName: localDeviceName, DeviceType: device.DeviceTypeMemdev, Mode: api.ModeNUMA,
		SysfsPath: e.f.p("bus/cxl/devices/region0"), NUMANodes: []int{2}, NodeAffinities: []int{0},
		TotalBytes: localBlockSize, ConsumedBytes: localBlockSize, Serial: "0xc100e2e1",
	})
	e.f.wantMemoryBlocks(t, "online_movable", 74)
	e.wantCDIDevice(t, "claim-u1-0", []string{"CXL_C100E2E1_MODE=numa", "CXL_C100E2E1_NODE=2", "CXL_C100E2E1_SERIAL=0xc100e2e1",
		"CXL_C100E2E1_SHARED=0", "CXL_C100E2E1_SIZE=134217728", "CXL_CLAIM_u1=1"}, nil)
	// a dax claim on the same device, and another numa claim on the node
	noErr(t, e.Prepare(ctx, localClaim("u2", localBlockSize, localDaxConfig)))
	noErr(t, e.Prepare(ctx, localClaim("u3", 2*localBlockSize)))
	if got := e.preparedClaims["u3"].Devices[0]; !slices.Equal(got.NUMANodes, []int{2}) || got.TotalBytes != 2*localBlockSize || got.ConsumedBytes != 2*localBlockSize {
		t.Fatalf("device info %+v", got)
	}
	e.f.wantMemoryBlocks(t, "online_movable", 74, 76, 77)

	noErr(t, e.Unprepare(ctx, "u1"))
	e.f.wantMemoryBlocks(t, "online_movable", 76, 77)
	noErr(t, e.Unprepare(ctx, "u3"))
	e.f.wantMemoryBlocks(t, "")
	noErr(t, e.Unprepare(ctx, "u2"))
	e.f.wantRegionReleased(t, "region0")
	if e.inUse() || len(e.Prepared) != 0 || len(e.preparedClaims) != 0 {
		t.Fatalf("after unprepare: in use %v, prepared %v", e.inUse(), e.Prepared)
	}
	// the whole memdev without consumed capacity
	noErr(t, e.Prepare(ctx, localClaim("u4", 0)))
	if got := e.preparedClaims["u4"].Devices[0].ConsumedBytes; got != localRegionSize {
		t.Fatalf("consumed %d", got)
	}
	e.f.wantMemoryBlocks(t, "online_movable", 74, 75, 76, 77, 78, 79, 80, 81)
	noErr(t, e.Unprepare(ctx, "u4"))
	e.f.wantRegionReleased(t, "region0")
	// an unknown claim
	noErr(t, e.Unprepare(ctx, "u1"))
}

// wantCDIDevices checks the CDI devices of the first device of a prepared
// claim.
func (e *localTestState) wantCDIDevices(t *testing.T, uid string, ids ...string) {
	t.Helper()
	if res := e.Prepared[uid]; len(res.Devices) == 0 || !slices.Equal(res.Devices[0].CDIDeviceIDs, ids) {
		t.Fatalf("claim %s: prepared %+v, expected CDI devices %v", uid, res.Devices, ids)
	}
}

// wantDeviceInfo checks the info of the first device of a prepared claim.
func (e *localTestState) wantDeviceInfo(t *testing.T, uid string, want PreparedDeviceInfo) {
	t.Helper()
	if got := e.preparedClaims[uid].Devices[0]; !reflect.DeepEqual(got, want) {
		t.Fatalf("claim %s: device info\n%+v\nexpected\n%+v", uid, got, want)
	}
}

// wantCDIDevice checks the env and the device nodes of a CDI device of the
// claims spec.
func (e *localTestState) wantCDIDevice(t *testing.T, name string, env []string, nodes []*cdiSpecs.DeviceNode) {
	t.Helper()
	d := cdiDeviceByName(e.cdiSpec(t), name)
	if d == nil || !slices.Equal(d.ContainerEdits.Env, env) || !reflect.DeepEqual(d.ContainerEdits.DeviceNodes, nodes) {
		t.Fatalf("CDI device %s: %+v, expected env %v nodes %+v", name, d, env, nodes)
	}
}

func TestPrepareLocalDax(t *testing.T) {
	e := newLocalTestState(t)
	ctx := context.Background()
	noErr(t, e.Prepare(ctx, localClaim("u1", localBlockSize, localDaxConfig)))
	// 32Mi is rounded up to a memory block
	noErr(t, e.Prepare(ctx, localClaim("u2", 32*mib, localROConfig)))
	e.wantCDIDevices(t, "u2", "generic/cxl=claim-u2-0")
	e.wantDeviceInfo(t, "u2", PreparedDeviceInfo{
		RequestName: "mem", DeviceName: localDeviceName, DeviceType: device.DeviceTypeMemdev, Mode: api.ModeDax,
		SysfsPath: e.f.p("bus/cxl/devices/region0"), NodeAffinities: []int{0},
		TotalBytes: localBlockSize, ConsumedBytes: localBlockSize, Serial: "0xc100e2e1",
		DaxDevice: "/dev/dax0.1", DaxMajor: 252, DaxMinor: 1, ReadOnly: true,
	})
	e.wantCDIDevice(t, "claim-u2-0",
		[]string{"CXL_C100E2E1_DAX=/dev/dax0.1", "CXL_C100E2E1_MODE=dax", "CXL_C100E2E1_READONLY=1", "CXL_C100E2E1_SERIAL=0xc100e2e1",
			"CXL_C100E2E1_SHARED=0", "CXL_C100E2E1_SIZE=134217728", "CXL_CLAIM_u2=1"},
		[]*cdiSpecs.DeviceNode{{Path: "/dev/dax0.1", Type: "c", Major: 252, Minor: 1, Permissions: "r"}})
	e.wantCDIDevice(t, "claim-u1-0",
		[]string{"CXL_C100E2E1_DAX=/dev/dax0.0", "CXL_C100E2E1_MODE=dax", "CXL_C100E2E1_READONLY=0", "CXL_C100E2E1_SERIAL=0xc100e2e1",
			"CXL_C100E2E1_SHARED=0", "CXL_C100E2E1_SIZE=134217728", "CXL_CLAIM_u1=1"},
		[]*cdiSpecs.DeviceNode{{Path: "/dev/dax0.0", Type: "c", Major: 252, Minor: 0}})

	// the spec is rebuilt from the prepared claims after a restart,
	// without the slices that are gone
	_ = os.RemoveAll(e.cdiDir)
	noErr(t, os.MkdirAll(e.cdiDir, 0o755))
	e.Prepared["stale"] = kubeletplugin.PrepareResult{}
	e.preparedClaims["stale"] = &PreparedClaimInfo{Devices: []PreparedDeviceInfo{{DeviceType: device.DeviceTypeMemdev, Mode: api.ModeDax,
		Serial: "0xc100e2e1", TotalBytes: localBlockSize, DaxDevice: "/dev/dax0.7", DaxMajor: 252, DaxMinor: 7}}}
	e.forgetGoneSlices()
	noErr(t, e.persist())
	if spec := e.cdiSpec(t); len(spec.Devices) != 3 {
		t.Fatalf("rebuilt CDI spec %+v", spec)
	}
	e.wantCDIDevice(t, "claim-stale-0", []string{"CXL_C100E2E1_MODE=dax", "CXL_C100E2E1_READONLY=0", "CXL_C100E2E1_SERIAL=0xc100e2e1",
		"CXL_C100E2E1_SHARED=0", "CXL_C100E2E1_SIZE=134217728", "CXL_CLAIM_stale=1"}, nil)
	delete(e.Prepared, "stale")
	delete(e.preparedClaims, "stale")
	// a numa claim on the same device gets the node
	noErr(t, e.Prepare(ctx, localClaim("u3", localBlockSize)))
	if got := e.preparedClaims["u3"].Devices[0]; got.DeviceType != device.DeviceTypeMemdev || got.Mode != api.ModeNUMA || !slices.Equal(got.NUMANodes, []int{2}) {
		t.Fatalf("numa claim %+v", got)
	}
	noErr(t, e.Unprepare(ctx, "u3"))

	noErr(t, e.Unprepare(ctx, "u1"))
	if cdiDeviceByName(e.cdiSpec(t), "claim-u1-0") != nil {
		t.Fatal("CDI device of u1 not removed")
	}
	e.f.wantDax(t, "dax0.0", 0, "")
	noErr(t, e.Unprepare(ctx, "u2"))
	if e.cdiSpec(t) != nil || e.inUse() {
		t.Fatalf("after the last unprepare: in use %v", e.inUse())
	}
	e.f.wantRegionReleased(t, "region0")
}

// A claim with two requests, the second one read-only: each allocation
// result has a CDI device of its own with only its slice, so that a
// container that references one request gets only that slice.
func TestPrepareLocalTwoSlices(t *testing.T) {
	e := newLocalTestState(t)
	claim := localClaim("u1", 8*mib, localDaxConfig)
	r := claim.Status.Allocation.Devices.Results[0]
	r.Request = "mem2"
	claim.Status.Allocation.Devices.Results = append(claim.Status.Allocation.Devices.Results, r)
	ro := makeOpaqueConfig(device.DriverName, resourcev1.AllocationConfigSourceClaim, json.RawMessage(localROConfig))
	ro.Requests = []string{"mem2"}
	claim.Status.Allocation.Devices.Config = append(claim.Status.Allocation.Devices.Config, ro)
	noErr(t, e.Prepare(context.Background(), claim))
	devs := e.Prepared["u1"].Devices
	if len(devs) != 2 || !slices.Equal(devs[0].CDIDeviceIDs, []string{"generic/cxl=claim-u1-0"}) ||
		!slices.Equal(devs[1].CDIDeviceIDs, []string{"generic/cxl=claim-u1-1"}) {
		t.Fatalf("prepared %+v", devs)
	}
	e.wantCDIDevice(t, "claim-u1-0", []string{
		"CXL_C100E2E1_DAX=/dev/dax0.0", "CXL_C100E2E1_MODE=dax", "CXL_C100E2E1_READONLY=0", "CXL_C100E2E1_SERIAL=0xc100e2e1",
		"CXL_C100E2E1_SHARED=0", "CXL_C100E2E1_SIZE=134217728", "CXL_CLAIM_u1=1"},
		[]*cdiSpecs.DeviceNode{{Path: "/dev/dax0.0", Type: "c", Major: 252, Minor: 0}})
	// the second slice of the memdev in the claim: <HEX>_2
	e.wantCDIDevice(t, "claim-u1-1", []string{
		"CXL_C100E2E1_2_DAX=/dev/dax0.1", "CXL_C100E2E1_2_MODE=dax", "CXL_C100E2E1_2_READONLY=1", "CXL_C100E2E1_2_SERIAL=0xc100e2e1",
		"CXL_C100E2E1_2_SHARED=0", "CXL_C100E2E1_2_SIZE=134217728", "CXL_CLAIM_u1=1"},
		[]*cdiSpecs.DeviceNode{{Path: "/dev/dax0.1", Type: "c", Major: 252, Minor: 1, Permissions: "r"}})
	if spec := e.cdiSpec(t); len(spec.Devices) != 2 {
		t.Fatalf("CDI devices %+v", spec.Devices)
	}
	noErr(t, e.Unprepare(context.Background(), "u1"))
	e.f.wantRegionReleased(t, "region0")
}

func TestPrepareLocalErrors(t *testing.T) {
	e := newLocalTestState(t)
	ctx := context.Background()
	for _, tc := range []struct{ params, err string }{
		{`{"apiVersion":"cxl.generic/v1alpha1","kind":"DeviceConfig","readOnly":true}`, "DeviceConfig: readOnly needs mode dax"},
		{`{"apiVersion":"cxl.generic/v1alpha1","kind":"DeviceConfig","mode":"ram"}`, `DeviceConfig: invalid mode "ram" (numa or dax)`},
		{`{"apiVersion":"cxl.generic/v1alpha1","kind":"DeviceConfig","size":1}`, `unknown field "size"`},
		{`{"apiVersion":"cxl.generic/v1alpha1","kind":"LocalDeviceConfig","mode":"dax"}`, `claim demo/claim-u1: unknown kind "LocalDeviceConfig" (DeviceConfig or MemoryPolicyConfig)`},
		{`{"apiVersion":"cxl.generic/v1alpha1","kind":"LendConfig","lease":"l1"}`, `unknown kind "LendConfig" (DeviceConfig or MemoryPolicyConfig)`},
		{`{"apiVersion":"cxl.generic/v1alpha1","mode":"dax"}`, `unknown kind "" (DeviceConfig or MemoryPolicyConfig)`},
	} {
		wantErr(t, e.Prepare(ctx, localClaim("u1", 2*mib, tc.params)), tc.err)
	}
	if strings.Contains(e.f.commands(), "create-region") || len(e.Prepared) != 0 {
		t.Fatalf("a refused config touched the memdev:\n%s", e.f.commands())
	}
	// a failure after the memory was acquired rolls it back
	claim := localClaim("u1", 2*mib, localDaxConfig)
	claim.Status.Allocation.Devices.Results = append(claim.Status.Allocation.Devices.Results,
		resourcev1.DeviceRequestAllocationResult{Request: "other", Driver: device.DriverName, Pool: testNode, Device: "missing"})
	wantErr(t, e.Prepare(ctx, claim), "could not find allocatable device missing")
	e.f.wantRegionReleased(t, "region0")
	if e.inUse() || len(e.Prepared) != 0 {
		t.Fatal("not rolled back")
	}
	// unmanaged memdevs
	e.local = nil
	wantErr(t, e.Prepare(ctx, localClaim("u1", 2*mib)), "manageMemdevs: false")
}

// newLendTestState returns a localTestState with lending enabled.
func newLendTestState(t *testing.T) (*localTestState, *fakeLender, *fakeHooks) {
	e := newLocalTestState(t)
	lender, hooks := newFakeLender(), &fakeHooks{}
	e.lending.set(lender, hooks)
	return e, lender, hooks
}

// wantLog checks that a log of calls ends with want.
func wantLog(t *testing.T, log, want string) {
	t.Helper()
	if !strings.HasSuffix(log, want) {
		t.Fatalf("calls\n%s\nexpected to end with\n%s", log, want)
	}
}

func TestPrepareLocalLend(t *testing.T) {
	e, lender, hooks := newLendTestState(t)
	ctx := context.Background()
	// the lease gets the slice: 64Mi rounded up to a memory block
	noErr(t, e.Prepare(ctx, localCompanion("u1", 64*mib)))
	wantLog(t, strings.Join(lender.calls, "\n"), "lend pod-uid-1/app 2")
	wantLog(t, hooks.log(), "lending l1 cxl-lease-l1\nprepared l1")
	wantDev := lentDevice{
		Driver: device.DriverName, Source: api.SourceLocal, Request: "mem", Device: localDeviceName, Pool: testNode,
		Serial: localSerial, Size: localBlockSize, Mode: api.ModeNUMA, Node: 2,
	}
	if !reflect.DeepEqual(hooks.devs, []lentDevice{wantDev}) {
		t.Fatalf("lent devices %+v", hooks.devs)
	}
	if rec := e.preparedClaims["u1"].Lend; rec == nil || rec.Lease != "l1" || !slices.Equal(rec.Nodes, []int{2}) {
		t.Fatalf("lend record %+v", rec)
	}
	// the companion's own containers get no CDI device, and the spec has
	// none of the companion
	e.wantCDIDevices(t, "u1")
	if cdiDeviceByName(e.cdiSpec(t), "claim-u1-0") != nil {
		t.Fatal("CDI device of a companion claim")
	}
	// the lend is stored with the claims info
	data, err := os.ReadFile(e.claimsFilePath)
	if err != nil || !strings.Contains(string(data), `"lease": "l1"`) {
		t.Fatalf("claims info lacks the lend: %v\n%s", err, data)
	}
	// a repeated Prepare tells the broker again
	noErr(t, e.Prepare(ctx, localCompanion("u1", 64*mib)))
	wantLog(t, hooks.log(), "prepared l1\nlending l1 cxl-lease-l1\nprepared l1")

	noErr(t, e.Unprepare(ctx, "u1"))
	wantLog(t, strings.Join(lender.calls, "\n"), "lend pod-uid-1/app 2\nrevoke pod-uid-1/app 2")
	wantLog(t, hooks.log(), "releasing l1")
	e.f.wantRegionReleased(t, "region0")
}

func TestPrepareLocalLendDax(t *testing.T) {
	e, lender, hooks := newLendTestState(t)
	ctx := context.Background()
	noErr(t, e.Prepare(ctx, localCompanion("u1", 16*mib, localDaxConfig)))
	if len(hooks.devs) != 1 || hooks.devs[0].Mode != api.ModeDax || hooks.devs[0].DaxDevice != "/dev/dax0.0" || hooks.devs[0].Node != -1 {
		t.Fatalf("lent dax devices %+v", hooks.devs)
	}
	// the borrower gets the fd from the broker, the companion nothing
	e.wantCDIDevices(t, "u1")
	noErr(t, e.Unprepare(ctx, "u1"))
	if len(lender.calls) != 0 {
		t.Fatalf("nodes lent for a dax lease: %v", lender.calls)
	}
}

func TestPrepareLocalLendErrors(t *testing.T) {
	e := newLocalTestState(t)
	ctx := context.Background()
	wantErr(t, e.Prepare(ctx, localCompanion("u1", 64*mib)), "lease l1: lending to running containers is disabled (--leases=false)")

	e, lender, hooks := newLendTestState(t)
	// a failed lend rolls back
	lender.lendErr = errors.New("container is steered")
	wantErr(t, e.Prepare(ctx, localCompanion("u2", 64*mib)), "container is steered")
	wantLog(t, hooks.log(), "failed l1: lease l1: lending node 2 to demo/borrower/app: container is steered")
	e.f.wantRegionReleased(t, "region0")
	if e.inUse() || len(e.Prepared) != 0 {
		t.Fatal("not rolled back")
	}

	// only local memdevs are lent
	if devs, ok := e.Allocatable.(device.DevicesInfo); ok {
		devs["dram"] = &device.DeviceInfo{Name: "dram", Dev: &device.DRAM{Nodes: []int{0}, Size: 1 << 30}}
	}
	claim := localCompanion("u4", 64*mib)
	claim.Status.Allocation.Devices.Results[0].Device = "dram"
	wantErr(t, e.Prepare(ctx, claim), "the companion claim of lease l1 needs memdev devices only")

	// a refused companion claim: the device is not touched
	hooks.lendingErr = errors.New("claim demo/cxl-lease-l1: not the companion of lease l1")
	ncmds := len(e.f.commands())
	wantErr(t, e.Prepare(ctx, localCompanion("u5", 64*mib)), "not the companion of lease l1")
	if len(e.f.commands()) != ncmds {
		t.Fatalf("a refused companion touched the memdev:\n%s", e.f.commands()[ncmds:])
	}
}

func TestPreparedClaimsInfo_LocalFields(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), preparedClaimsInfoFileName)
	noErr(t, os.WriteFile(filePath, []byte(`{"u1":{"claimName":"ns/c","devices":[{"requestName":"r","deviceName":"region0-node2","deviceType":"region","mode":"numa","region":"region0","numaNodes":[2],"totalBytes":1,"consumedBytes":1}]}}`), 0600))
	claims, err := getOrCreatePreparedClaimsInfo(filePath)
	noErr(t, err)
	if info := claims["u1"]; info.Lend != nil || info.usesLocalMemdevs() || info.Devices[0].Serial != "" || info.Devices[0].Region != "region0" {
		t.Fatalf("region claim info %+v", info)
	}
	claims["u2"] = &PreparedClaimInfo{
		ClaimName: "ns/c2",
		Devices:   []PreparedDeviceInfo{{DeviceType: device.DeviceTypeMemdev, Mode: api.ModeDax, Serial: "0xc100e2e1", DaxDevice: "/dev/dax0.1", DaxMinor: 1, ReadOnly: true}},
		Lend:      &lendRecord{Lease: "l1", Pod: podRef{Namespace: "ns", Name: "p", UID: "uid"}, Container: "app"},
	}
	noErr(t, writePreparedClaimsInfoToFile(filePath, claims))
	loaded, err := getOrCreatePreparedClaimsInfo(filePath)
	noErr(t, err)
	if !reflect.DeepEqual(loaded["u2"], claims["u2"]) || !loaded["u2"].usesLocalMemdevs() {
		t.Fatalf("loaded %+v", loaded["u2"])
	}
}

// A claim that is prepared already is returned as it is, even without an
// allocation (kubelet after a restart), and only a companion claim tells
// the broker again.
func TestPrepareAlreadyPrepared(t *testing.T) {
	e, lender, hooks := newLendTestState(t)
	ctx := context.Background()
	d := &driver{state: *e.nodeState}
	res := d.prepareResourceClaim(ctx, localCompanion("u1", 64*mib))
	noErr(t, res.Err)
	noErr(t, d.prepareResourceClaim(ctx, localClaim("u2", 16*mib)).Err)
	ncmds := len(e.f.commands())

	again := localCompanion("u1", 64*mib)
	again.Status.Allocation = nil
	res2 := d.prepareResourceClaim(ctx, again)
	if res2.Err != nil || !reflect.DeepEqual(res2, res) {
		t.Fatalf("repeated prepare %+v, expected %+v", res2, res)
	}
	wantLog(t, hooks.log(), "lending l1 cxl-lease-l1\nprepared l1")
	again = localClaim("u2", 16*mib)
	again.Status.Allocation = nil
	noErr(t, d.prepareResourceClaim(ctx, again).Err)
	if len(e.f.commands()) != ncmds || len(lender.calls) != 1 {
		t.Fatalf("repeated prepare touched devices:\n%s\nlender %v", e.f.commands()[ncmds:], lender.calls)
	}
	if n := strings.Count(hooks.log(), "prepared l1"); n != 2 {
		t.Fatalf("hooks %s", hooks.log())
	}
}

// blockingLender is a fakeLender whose LendNode waits until release is
// closed.
type blockingLender struct {
	*fakeLender
	entered chan struct{}
	release chan struct{}
}

func (b *blockingLender) LendNode(ctx context.Context, podUID, name string, node int, holder string) error {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-b.release
	return b.fakeLender.LendNode(ctx, podUID, name, node, holder)
}

func (b *blockingLender) lends() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.calls)
}

// The NRI readers, the publisher and other claims never wait for a Prepare
// that lends, and two Prepares of one claim run one after the other.
func TestPrepareDoesNotBlockReaders(t *testing.T) {
	e := newLocalTestState(t)
	bl := &blockingLender{fakeLender: newFakeLender(), entered: make(chan struct{}, 1), release: make(chan struct{})}
	hooks := &fakeHooks{}
	e.lending.set(bl, hooks)
	ctx := context.Background()
	noErr(t, e.Prepare(ctx, localClaim("u0", 32*mib)))

	done := make(chan error, 2)
	go func() { done <- e.Prepare(ctx, localCompanion("u1", 64*mib)) }()
	select {
	case <-bl.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("Prepare did not lend")
	}
	readers := make(chan error, 1)
	go func() {
		if e.GetClaimInfo("u0") == nil {
			readers <- errors.New("no claim info of u0")
			return
		}
		_ = e.GetMemoryPolicy("u0")
		_ = e.GetResources()
		err := e.Prepare(ctx, localClaim("u2", 2*mib))
		if err == nil {
			err = e.Unprepare(ctx, "u2")
		}
		readers <- err
	}()
	select {
	case err := <-readers:
		noErr(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("claim info, resources or another claim waited for a lend")
	}

	go func() { done <- e.Prepare(ctx, localCompanion("u1", 64*mib)) }()
	select {
	case err := <-done:
		t.Fatalf("a Prepare of u1 finished during the lend of u1: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(bl.release)
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			noErr(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("Prepare of u1 did not finish")
		}
	}
	if n := bl.lends(); n != 1 {
		t.Fatalf("%d lends of u1", n)
	}
	if n := len(e.local.devices[localSerial].Claims["u1"]); n != 1 {
		t.Fatalf("%d slices of u1", n)
	}
	wantLog(t, hooks.log(), "lending l1 cxl-lease-l1\nprepared l1\nlending l1 cxl-lease-l1\nprepared l1")
}

func TestUnprepareLocalFailures(t *testing.T) {
	e := newLocalTestState(t)
	hooks := &fakeHooks{}
	e.lending.set(revokeFailer{fakeLender: newFakeLender()}, hooks)
	ctx := context.Background()

	// a revoke that fails keeps the claim and its memory
	noErr(t, e.Prepare(ctx, localCompanion("u1", 64*mib)))
	wantErr(t, e.Unprepare(ctx, "u1"), "node 2 busy")
	if _, found := e.Prepared["u1"]; !found || strings.Contains(e.f.commands(), "destroy-region") {
		t.Fatalf("claim released after a failed revoke:\n%s", e.f.commands())
	}
	wantLog(t, hooks.log(), "releasing l1")
	e.lending.set(newFakeLender(), hooks)
	noErr(t, e.Unprepare(ctx, "u1"))
	e.f.wantRegionReleased(t, "region0")

	// a slice that fails to delete keeps the claim and its CDI device
	noErr(t, e.Prepare(ctx, localClaim("u2", 8*mib, localDaxConfig)))
	e.f.fail = map[string]error{"bus/dax/devices/dax0.0/size": syscall.EBUSY}
	wantErr(t, e.Unprepare(ctx, "u2"), "releasing slice dax0.0")
	if _, found := e.Prepared["u2"]; !found || cdiDeviceByName(e.cdiSpec(t), "claim-u2-0") == nil {
		t.Fatal("claim or CDI device gone after a failed release")
	}
	noErr(t, e.Unprepare(ctx, "u2"))
	if e.cdiSpec(t) != nil || e.inUse() {
		t.Fatalf("after the retry: in use %v", e.inUse())
	}
}

// A Prepare that cannot write the prepared claims rolls back: the lend is
// revoked, the broker hears the error, the memory is released.
func TestPrepareLocalPersistFailure(t *testing.T) {
	e, lender, hooks := newLendTestState(t)
	ctx := context.Background()
	path := e.claimsFilePath
	e.claimsFilePath = filepath.Join(t.TempDir(), "missing", preparedClaimsInfoFileName)
	wantErr(t, e.Prepare(ctx, localCompanion("u1", 64*mib)), "failed to write prepared claims info")
	wantLog(t, strings.Join(lender.calls, "\n"), "lend pod-uid-1/app 2\nrevoke pod-uid-1/app 2")
	if log := hooks.log(); !strings.Contains(log, "failed l1: failed to write prepared claims info") || strings.Contains(log, "releasing") {
		t.Fatalf("hooks %s", log)
	}
	e.f.wantRegionReleased(t, "region0")
	if _, found := e.Prepared["u1"]; found || e.GetClaimInfo("u1") != nil {
		t.Fatal("claim kept after the rollback")
	}
	e.claimsFilePath = path
	noErr(t, e.Prepare(ctx, localCompanion("u1", 64*mib)))
}

// A Prepare that fails after the lease was marked Lending leaves the lease
// to the retry.
func TestPrepareLocalFailsAfterCheck(t *testing.T) {
	e, _, hooks := newLendTestState(t)
	ctx := context.Background()
	_, err := e.f.exec(ctx, "cxl", "create-region", "-t", "ram", "-d", "decoder0.0", "-m", "mem0")
	noErr(t, err)
	wantErr(t, e.Prepare(ctx, localCompanion("u1", 64*mib)), "did not create")
	if log := hooks.log(); log != "lending l1 cxl-lease-l1" {
		t.Fatalf("hooks %s", log)
	}
	_, err = e.f.exec(ctx, "cxl", "destroy-region", "region0")
	noErr(t, err)
	noErr(t, e.Prepare(ctx, localCompanion("u1", 64*mib)))
	if log := hooks.log(); log != "lending l1 cxl-lease-l1\nlending l1 cxl-lease-l1\nprepared l1" {
		t.Fatalf("hooks %s", log)
	}
}

// TestClaimsCDISpec builds the CDI devices of the allocation results of a
// claim on DRAM and a pre-created region, of a lease claim, and none of a
// companion claim.
func TestClaimsCDISpec(t *testing.T) {
	s := &nodeState{
		NodeState: &helpers.NodeState{Prepared: helpers.ClaimPreparations{"u1": {}, "u2": {}, "u3": {}}},
		preparedClaims: PreparedClaimsInfo{
			// result 1 is of another driver
			"u1": {Devices: []PreparedDeviceInfo{
				{Result: 0, DeviceType: device.DeviceTypeDRAM, Mode: api.ModeNUMA, NUMANodes: []int{0, 1}, TotalBytes: 16 << 30, ConsumedBytes: 64 << 20},
				{Result: 2, DeviceType: device.DeviceTypeRegion, Mode: api.ModeNUMA, Region: "region0", NUMANodes: []int{2}, TotalBytes: 2 << 30, ConsumedBytes: 64 << 20},
			}},
			"u2": {Devices: []PreparedDeviceInfo{{DeviceType: device.DeviceTypeLease}}},
			// a companion claim
			"u3": {Devices: []PreparedDeviceInfo{{DeviceType: device.DeviceTypeMemdev, Mode: api.ModeNUMA, Serial: "0x1", NUMANodes: []int{2}}},
				Lend: &lendRecord{Lease: "l1"}},
		},
	}
	spec := s.buildClaimsCDISpec()
	want := []cdiSpecs.Device{
		{Name: "claim-u1-0", ContainerEdits: cdiSpecs.ContainerEdits{Env: []string{
			"CXL_CLAIM_u1=1", "CXL_DRAM_MODE=numa", "CXL_DRAM_NODES=0-1", "CXL_DRAM_SIZE=67108864"}}},
		{Name: "claim-u1-2", ContainerEdits: cdiSpecs.ContainerEdits{Env: []string{
			"CXL_CLAIM_u1=1", "CXL_REGION0_MODE=numa", "CXL_REGION0_NODE=2", "CXL_REGION0_SIZE=67108864"}}},
		{Name: "claim-u2-0", ContainerEdits: cdiSpecs.ContainerEdits{Env: []string{"CXL_CLAIM_u2=1"}}},
	}
	if spec.Kind != "generic/cxl" || !reflect.DeepEqual(spec.Devices, want) {
		t.Fatalf("CDI spec %+v", spec)
	}
}

// TestRemoveCXLSpecs removes the CDI specs of both drivers, also of their
// earlier names, and keeps the others.
func TestRemoveCXLSpecs(t *testing.T) {
	dir := t.TempDir()
	for name, kind := range map[string]string{"cxl-claims": "generic/cxl", "cxl-local": "generic/cxl", "cxl-pool": "generic/cxl-pool", "gpu": "intel.com/gpu"} {
		data := "cdiVersion: 0.6.0\nkind: " + kind + "\ndevices:\n- name: " + name + "\n  containerEdits:\n    env: [A=1]\n"
		noErr(t, os.WriteFile(filepath.Join(dir, name+".yaml"), []byte(data), 0o600))
	}
	cache, err := cdiapi.NewCache(cdiapi.WithSpecDirs(dir), cdiapi.WithAutoRefresh(false))
	noErr(t, err)
	noErr(t, removeCXLSpecs(cache))
	m, _ := filepath.Glob(filepath.Join(dir, "*"))
	if len(m) != 1 || filepath.Base(m[0]) != "gpu.yaml" {
		t.Fatalf("left %v", m)
	}
}

// A DeviceConfig on a region or DRAM request fails unless it is numa.
func TestPrepareRegionDRAMNUMAOnly(t *testing.T) {
	e := newLocalTestState(t)
	ctx := context.Background()
	devs, ok := e.Allocatable.(device.DevicesInfo)
	if !ok {
		t.Fatal("no devices")
	}
	devs["region0-node2"] = &device.DeviceInfo{Name: "region0-node2", Dev: &nricxl.RegionDevice{Name: "region0", Node: 2, Size: 1 << 30, Enabled: true}}
	devs["dram"] = &device.DeviceInfo{Name: "dram", Dev: &device.DRAM{Nodes: []int{0}, Size: 1 << 30}}
	for _, tc := range []struct{ dev, params, err string }{
		{"region0-node2", localDaxConfig, "claim demo/claim-u1: request mem: DeviceConfig: mode dax: a region is used as numa only"},
		{"region0-node2", localROConfig, "DeviceConfig: mode dax: a region is used as numa only"},
		{"region0-node2", `{"apiVersion":"cxl.generic/v1alpha1","kind":"DeviceConfig","readOnly":true}`, "DeviceConfig: readOnly needs mode dax"},
		{"dram", localDaxConfig, "claim demo/claim-u1: request mem: DeviceConfig: mode dax: a dram is used as numa only"},
		{"dram", `{"apiVersion":"cxl.generic/v1alpha1","kind":"DeviceConfig","readOnly":true}`, "DeviceConfig: readOnly needs mode dax"},
		{"dram", `{"apiVersion":"cxl.generic/v1alpha1","kind":"DeviceConfig","mode":"ram"}`, `DeviceConfig: invalid mode "ram" (numa or dax)`},
		{"region0-node2", `{"apiVersion":"cxl.generic/v1alpha1","kind":"DeviceConfig","mode":"numa"}`, ""},
		{"dram", `{"apiVersion":"cxl.generic/v1alpha1","kind":"DeviceConfig","mode":"numa"}`, ""},
	} {
		claim := localClaim("u1", 64*mib, tc.params)
		claim.Status.Allocation.Devices.Results[0].Device = tc.dev
		err := e.Prepare(ctx, claim)
		switch {
		case tc.err == "" && err != nil:
			t.Errorf("%s %s: %v", tc.dev, tc.params, err)
		case tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)):
			t.Errorf("%s %s: expected error %q, got %v", tc.dev, tc.params, tc.err, err)
		}
		if err == nil {
			noErr(t, e.Unprepare(ctx, "u1"))
		}
	}
	if len(e.Prepared) != 0 {
		t.Fatalf("prepared %v", e.Prepared)
	}
}
