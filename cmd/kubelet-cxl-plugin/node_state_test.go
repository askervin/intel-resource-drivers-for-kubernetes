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

func TestPreparedPolicies_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "preparedPolicies.json")

	// Write policies.
	policies := PreparedPolicies{
		"uid-1": {
			APIVersion:     memorypolicy.APIVersion,
			Kind:           memorypolicy.Kind,
			MemoryUseOrder: "first-dram",
			MinStep:        "128M",
			MaxStep:        "1G",
		},
	}
	if err := writePreparedPoliciesToFile(filePath, policies); err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	// Read them back.
	loaded, err := getOrCreatePreparedPolicies(filePath)
	if err != nil {
		t.Fatalf("failed to load: %v", err)
	}

	if len(loaded) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(loaded))
	}
	p := loaded["uid-1"]
	if p == nil {
		t.Fatal("expected non-nil policy for uid-1")
	}
	if p.MemoryUseOrder != "first-dram" {
		t.Errorf("expected first-dram, got %s", p.MemoryUseOrder)
	}
	if p.MinStep != "128M" {
		t.Errorf("expected 128M, got %s", p.MinStep)
	}
	if p.MaxStep != "1G" {
		t.Errorf("expected 1G, got %s", p.MaxStep)
	}
}

func TestPreparedPolicies_CreateEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "preparedPolicies.json")

	policies, err := getOrCreatePreparedPolicies(filePath)
	if err != nil {
		t.Fatalf("failed: %v", err)
	}
	if len(policies) != 0 {
		t.Fatalf("expected empty policies, got %d", len(policies))
	}

	// Verify the file exists.
	if _, err := os.Stat(filePath); err != nil {
		t.Fatalf("file should exist: %v", err)
	}
}
