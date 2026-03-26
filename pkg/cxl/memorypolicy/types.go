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

// Package memorypolicy defines the user-facing memory policy
// configuration types used in DRA OpaqueDeviceConfiguration for the
// CXL driver. A MemoryPolicyConfig is specified by the user in a
// ResourceClaim's opaque config section and parsed by the driver
// during Prepare.
package memorypolicy

import (
	"fmt"
	"strings"

	"github.com/containers/nri-plugins/pkg/cgmpolmgr"
)

const (
	// APIVersion is the expected apiVersion in the opaque config.
	APIVersion = "cxl.generic/v1alpha1"
	// Kind is the expected kind in the opaque config.
	Kind = "MemoryPolicyConfig"
)

// MemoryPolicyConfig is the user-facing configuration for memory
// ordering policy, specified as OpaqueDeviceConfiguration parameters
// in a ResourceClaim or DeviceClass.
type MemoryPolicyConfig struct {
	APIVersion         string                        `json:"apiVersion"`
	Kind               string                        `json:"kind"`
	MemoryUseOrder     string                        `json:"memoryUseOrder"`
	MemoryUseWaypoints []cgmpolmgr.MemoryUseWaypoint `json:"memoryUseWaypoints,omitempty"`
	MinStep            string                        `json:"minStep,omitempty"`
	MaxStep            string                        `json:"maxStep,omitempty"`
}

// Validate checks that the MemoryPolicyConfig is well-formed.
func (c *MemoryPolicyConfig) Validate() error {
	if c.APIVersion != APIVersion {
		return fmt.Errorf("unsupported apiVersion %q, expected %q", c.APIVersion, APIVersion)
	}
	if c.Kind != Kind {
		return fmt.Errorf("unsupported kind %q, expected %q", c.Kind, Kind)
	}

	order, err := cgmpolmgr.ParseMemoryUseOrder(c.MemoryUseOrder)
	if err != nil {
		return fmt.Errorf("invalid memoryUseOrder: %w", err)
	}

	if order == cgmpolmgr.MemoryUseWaypoints {
		if len(c.MemoryUseWaypoints) == 0 {
			return fmt.Errorf("memoryUseWaypoints must be specified when memoryUseOrder is %q", c.MemoryUseOrder)
		}
		if err := validateWaypoints(c.MemoryUseWaypoints); err != nil {
			return fmt.Errorf("invalid memoryUseWaypoints: %w", err)
		}
	} else if len(c.MemoryUseWaypoints) > 0 {
		return fmt.Errorf("memoryUseWaypoints must not be specified when memoryUseOrder is %q", c.MemoryUseOrder)
	}

	if c.MinStep != "" {
		if _, err := cgmpolmgr.ParseMemorySize(c.MinStep); err != nil {
			return fmt.Errorf("invalid minStep: %w", err)
		}
	}
	if c.MaxStep != "" {
		if _, err := cgmpolmgr.ParseMemorySize(c.MaxStep); err != nil {
			return fmt.Errorf("invalid maxStep: %w", err)
		}
	}

	if c.MinStep != "" && c.MaxStep != "" {
		minBytes, _ := cgmpolmgr.ParseMemorySize(c.MinStep)
		maxBytes, _ := cgmpolmgr.ParseMemorySize(c.MaxStep)
		if minBytes > maxBytes {
			return fmt.Errorf("minStep (%s) must not exceed maxStep (%s)", c.MinStep, c.MaxStep)
		}
	}

	return nil
}

// validateWaypoints checks that waypoints are well-formed: each has
// at least one target usage entry, each entry has a valid memory type
// and parseable usage size, and usage values are non-decreasing across
// waypoints for each memory type.
func validateWaypoints(waypoints []cgmpolmgr.MemoryUseWaypoint) error {
	prevDRAM := uint64(0)
	prevCXL := uint64(0)

	for i, wp := range waypoints {
		if len(wp.TargetUsages) == 0 {
			return fmt.Errorf("waypoint %d: no target usages specified", i)
		}

		dramUsage := prevDRAM
		cxlUsage := prevCXL

		for j, entry := range wp.TargetUsages {
			usageBytes, err := cgmpolmgr.ParseMemorySize(entry.Usage)
			if err != nil {
				return fmt.Errorf("waypoint %d entry %d: invalid usage: %w", i, j, err)
			}

			switch strings.ToUpper(strings.TrimSpace(entry.MemoryType)) {
			case "DRAM":
				dramUsage = usageBytes
			case "CXL":
				cxlUsage = usageBytes
			default:
				return fmt.Errorf("waypoint %d entry %d: unknown memory type %q (valid: DRAM, CXL)", i, j, entry.MemoryType)
			}
		}

		if dramUsage < prevDRAM {
			return fmt.Errorf("waypoint %d: DRAM usage must be non-decreasing (got %d, previous %d)", i, dramUsage, prevDRAM)
		}
		if cxlUsage < prevCXL {
			return fmt.Errorf("waypoint %d: CXL usage must be non-decreasing (got %d, previous %d)", i, cxlUsage, prevCXL)
		}

		prevDRAM = dramUsage
		prevCXL = cxlUsage
	}

	return nil
}
