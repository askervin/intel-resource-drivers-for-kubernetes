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

package memorypolicy

import (
	"encoding/json"
	"testing"

	"github.com/containers/nri-plugins/pkg/cgmpolmgr"
)

func TestValidate_ValidConfigs(t *testing.T) {
	tests := []struct {
		name   string
		config MemoryPolicyConfig
	}{
		{
			name: "first-dram order",
			config: MemoryPolicyConfig{
				APIVersion:     APIVersion,
				Kind:           Kind,
				MemoryUseOrder: "first-dram",
				MinStep:        "128M",
				MaxStep:        "1G",
			},
		},
		{
			name: "first-cxl order",
			config: MemoryPolicyConfig{
				APIVersion:     APIVersion,
				Kind:           Kind,
				MemoryUseOrder: "first-cxl",
			},
		},
		{
			name: "start-interleaved order",
			config: MemoryPolicyConfig{
				APIVersion:     APIVersion,
				Kind:           Kind,
				MemoryUseOrder: "start-interleaved",
				MinStep:        "64M",
			},
		},
		{
			name: "end-interleaved order",
			config: MemoryPolicyConfig{
				APIVersion:     APIVersion,
				Kind:           Kind,
				MemoryUseOrder: "end-interleaved",
				MaxStep:        "2G",
			},
		},
		{
			name: "waypoints order",
			config: MemoryPolicyConfig{
				APIVersion:     APIVersion,
				Kind:           Kind,
				MemoryUseOrder: "waypoints",
				MemoryUseWaypoints: []cgmpolmgr.MemoryUseWaypoint{
					{
						TargetUsages: []cgmpolmgr.MemoryUseWaypointEntry{
							{MemoryType: "DRAM", Usage: "4G"},
						},
					},
					{
						TargetUsages: []cgmpolmgr.MemoryUseWaypointEntry{
							{MemoryType: "DRAM", Usage: "8G"},
							{MemoryType: "CXL", Usage: "40G"},
						},
					},
				},
				MinStep: "128M",
				MaxStep: "1G",
			},
		},
		{
			name: "no limits specified",
			config: MemoryPolicyConfig{
				APIVersion:     APIVersion,
				Kind:           Kind,
				MemoryUseOrder: "first-dram",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.config.Validate(); err != nil {
				t.Errorf("expected valid config, got error: %v", err)
			}
		})
	}
}

func TestValidate_InvalidConfigs(t *testing.T) {
	tests := []struct {
		name        string
		config      MemoryPolicyConfig
		errContains string
	}{
		{
			name: "wrong apiVersion",
			config: MemoryPolicyConfig{
				APIVersion:     "wrong/v1",
				Kind:           Kind,
				MemoryUseOrder: "first-dram",
			},
			errContains: "unsupported apiVersion",
		},
		{
			name: "wrong kind",
			config: MemoryPolicyConfig{
				APIVersion:     APIVersion,
				Kind:           "WrongKind",
				MemoryUseOrder: "first-dram",
			},
			errContains: "unsupported kind",
		},
		{
			name: "invalid memoryUseOrder",
			config: MemoryPolicyConfig{
				APIVersion:     APIVersion,
				Kind:           Kind,
				MemoryUseOrder: "invalid-order",
			},
			errContains: "invalid memoryUseOrder",
		},
		{
			name: "waypoints order without waypoints",
			config: MemoryPolicyConfig{
				APIVersion:     APIVersion,
				Kind:           Kind,
				MemoryUseOrder: "waypoints",
			},
			errContains: "memoryUseWaypoints must be specified",
		},
		{
			name: "non-waypoints order with waypoints",
			config: MemoryPolicyConfig{
				APIVersion:     APIVersion,
				Kind:           Kind,
				MemoryUseOrder: "first-dram",
				MemoryUseWaypoints: []cgmpolmgr.MemoryUseWaypoint{
					{
						TargetUsages: []cgmpolmgr.MemoryUseWaypointEntry{
							{MemoryType: "DRAM", Usage: "4G"},
						},
					},
				},
			},
			errContains: "memoryUseWaypoints must not be specified",
		},
		{
			name: "invalid minStep",
			config: MemoryPolicyConfig{
				APIVersion:     APIVersion,
				Kind:           Kind,
				MemoryUseOrder: "first-dram",
				MinStep:        "notasize",
			},
			errContains: "invalid minStep",
		},
		{
			name: "invalid maxStep",
			config: MemoryPolicyConfig{
				APIVersion:     APIVersion,
				Kind:           Kind,
				MemoryUseOrder: "first-dram",
				MaxStep:        "notasize",
			},
			errContains: "invalid maxStep",
		},
		{
			name: "minStep exceeds maxStep",
			config: MemoryPolicyConfig{
				APIVersion:     APIVersion,
				Kind:           Kind,
				MemoryUseOrder: "first-dram",
				MinStep:        "2G",
				MaxStep:        "128M",
			},
			errContains: "minStep",
		},
		{
			name: "waypoint with empty target usages",
			config: MemoryPolicyConfig{
				APIVersion:     APIVersion,
				Kind:           Kind,
				MemoryUseOrder: "waypoints",
				MemoryUseWaypoints: []cgmpolmgr.MemoryUseWaypoint{
					{TargetUsages: []cgmpolmgr.MemoryUseWaypointEntry{}},
				},
			},
			errContains: "no target usages",
		},
		{
			name: "waypoint with unknown memory type",
			config: MemoryPolicyConfig{
				APIVersion:     APIVersion,
				Kind:           Kind,
				MemoryUseOrder: "waypoints",
				MemoryUseWaypoints: []cgmpolmgr.MemoryUseWaypoint{
					{
						TargetUsages: []cgmpolmgr.MemoryUseWaypointEntry{
							{MemoryType: "HBM", Usage: "4G"},
						},
					},
				},
			},
			errContains: "unknown memory type",
		},
		{
			name: "waypoint with decreasing usage",
			config: MemoryPolicyConfig{
				APIVersion:     APIVersion,
				Kind:           Kind,
				MemoryUseOrder: "waypoints",
				MemoryUseWaypoints: []cgmpolmgr.MemoryUseWaypoint{
					{
						TargetUsages: []cgmpolmgr.MemoryUseWaypointEntry{
							{MemoryType: "DRAM", Usage: "8G"},
						},
					},
					{
						TargetUsages: []cgmpolmgr.MemoryUseWaypointEntry{
							{MemoryType: "DRAM", Usage: "4G"},
						},
					},
				},
			},
			errContains: "non-decreasing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !contains(err.Error(), tt.errContains) {
				t.Errorf("expected error containing %q, got %q", tt.errContains, err.Error())
			}
		})
	}
}

func TestJSONRoundTrip(t *testing.T) {
	original := MemoryPolicyConfig{
		APIVersion:     APIVersion,
		Kind:           Kind,
		MemoryUseOrder: "first-dram",
		MinStep:        "128M",
		MaxStep:        "1G",
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var restored MemoryPolicyConfig
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if err := restored.Validate(); err != nil {
		t.Errorf("round-tripped config is not valid: %v", err)
	}

	if original.MemoryUseOrder != restored.MemoryUseOrder {
		t.Errorf("MemoryUseOrder mismatch: %q vs %q", original.MemoryUseOrder, restored.MemoryUseOrder)
	}
	if original.MinStep != restored.MinStep {
		t.Errorf("MinStep mismatch: %q vs %q", original.MinStep, restored.MinStep)
	}
	if original.MaxStep != restored.MaxStep {
		t.Errorf("MaxStep mismatch: %q vs %q", original.MaxStep, restored.MaxStep)
	}
}

func TestJSONRoundTripWithWaypoints(t *testing.T) {
	original := MemoryPolicyConfig{
		APIVersion:     APIVersion,
		Kind:           Kind,
		MemoryUseOrder: "waypoints",
		MemoryUseWaypoints: []cgmpolmgr.MemoryUseWaypoint{
			{
				TargetUsages: []cgmpolmgr.MemoryUseWaypointEntry{
					{MemoryType: "DRAM", Usage: "4G"},
				},
			},
			{
				TargetUsages: []cgmpolmgr.MemoryUseWaypointEntry{
					{MemoryType: "DRAM", Usage: "8G"},
					{MemoryType: "CXL", Usage: "40G"},
				},
			},
		},
		MinStep: "128M",
		MaxStep: "1G",
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var restored MemoryPolicyConfig
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if err := restored.Validate(); err != nil {
		t.Errorf("round-tripped config is not valid: %v", err)
	}

	if len(restored.MemoryUseWaypoints) != 2 {
		t.Fatalf("expected 2 waypoints, got %d", len(restored.MemoryUseWaypoints))
	}
	if len(restored.MemoryUseWaypoints[1].TargetUsages) != 2 {
		t.Errorf("expected 2 target usages in waypoint 1, got %d", len(restored.MemoryUseWaypoints[1].TargetUsages))
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstr(s, substr))
}

func containsSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
