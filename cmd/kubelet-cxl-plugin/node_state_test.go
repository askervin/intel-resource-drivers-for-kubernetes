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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/memorypolicy"
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
	policy, err := parseMemoryPolicyFromClaim(claim)
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
	policy, err := parseMemoryPolicyFromClaim(claim)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if policy != nil {
		t.Fatalf("expected nil policy, got %+v", policy)
	}
}

func TestParseMemoryPolicyFromClaim_ClaimConfig(t *testing.T) {
	cfg := memorypolicy.MemoryPolicyConfig{
		APIVersion:     memorypolicy.APIVersion,
		Kind:           memorypolicy.Kind,
		MemoryUseOrder: "first-dram",
		MinStep:        "128M",
		MaxStep:        "1G",
	}
	claim := makeClaim("uid-2",
		makeOpaqueConfig(device.DriverName, resourcev1.AllocationConfigSourceClaim, cfg),
	)

	policy, err := parseMemoryPolicyFromClaim(claim)
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
		APIVersion:     memorypolicy.APIVersion,
		Kind:           memorypolicy.Kind,
		MemoryUseOrder: "first-cxl",
	}
	claim := makeClaim("uid-3",
		makeOpaqueConfig(device.DriverName, resourcev1.AllocationConfigSourceClass, cfg),
	)

	policy, err := parseMemoryPolicyFromClaim(claim)
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
		APIVersion:     memorypolicy.APIVersion,
		Kind:           memorypolicy.Kind,
		MemoryUseOrder: "first-cxl",
	}
	claimCfg := memorypolicy.MemoryPolicyConfig{
		APIVersion:     memorypolicy.APIVersion,
		Kind:           memorypolicy.Kind,
		MemoryUseOrder: "first-dram",
		MinStep:        "64M",
	}
	claim := makeClaim("uid-4",
		makeOpaqueConfig(device.DriverName, resourcev1.AllocationConfigSourceClass, classCfg),
		makeOpaqueConfig(device.DriverName, resourcev1.AllocationConfigSourceClaim, claimCfg),
	)

	policy, err := parseMemoryPolicyFromClaim(claim)
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
		APIVersion:     memorypolicy.APIVersion,
		Kind:           memorypolicy.Kind,
		MemoryUseOrder: "first-dram",
	}
	claim := makeClaim("uid-5",
		makeOpaqueConfig("other.driver", resourcev1.AllocationConfigSourceClaim, cfg),
	)

	policy, err := parseMemoryPolicyFromClaim(claim)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if policy != nil {
		t.Fatalf("expected nil policy for wrong driver, got %+v", policy)
	}
}

func TestParseMemoryPolicyFromClaim_WrongKindSkipped(t *testing.T) {
	other := map[string]string{
		"apiVersion": memorypolicy.APIVersion,
		"kind":       "SomeOtherConfig",
		"foo":        "bar",
	}
	claim := makeClaim("uid-6",
		makeOpaqueConfig(device.DriverName, resourcev1.AllocationConfigSourceClaim, other),
	)

	policy, err := parseMemoryPolicyFromClaim(claim)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if policy != nil {
		t.Fatalf("expected nil policy for wrong kind, got %+v", policy)
	}
}

func TestParseMemoryPolicyFromClaim_InvalidConfig(t *testing.T) {
	cfg := memorypolicy.MemoryPolicyConfig{
		APIVersion:     memorypolicy.APIVersion,
		Kind:           memorypolicy.Kind,
		MemoryUseOrder: "invalid-order",
	}
	claim := makeClaim("uid-7",
		makeOpaqueConfig(device.DriverName, resourcev1.AllocationConfigSourceClaim, cfg),
	)

	_, err := parseMemoryPolicyFromClaim(claim)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
}

func TestPreparedClaimsInfo_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "preparedClaimsInfo.json")

	// Write claims info.
	claims := PreparedClaimsInfo{
		"uid-1": {
			ClaimName: "test-ns/test-claim",
			Policy: &memorypolicy.MemoryPolicyConfig{
				APIVersion:     memorypolicy.APIVersion,
				Kind:           memorypolicy.Kind,
				MemoryUseOrder: "first-dram",
				MinStep:        "128M",
				MaxStep:        "1G",
			},
			Devices: []PreparedDeviceInfo{
				{
					RequestName:    "req1",
					DeviceName:     "region0",
					DeviceType:     device.DeviceTypeCXLNode,
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
	if dev.DeviceType != device.DeviceTypeCXLNode {
		t.Errorf("expected %s, got %s", device.DeviceTypeCXLNode, dev.DeviceType)
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
