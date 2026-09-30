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

	"github.com/containers/nri-plugins/pkg/cgmpolmgr"
)

const (
	// APIVersion is the apiVersion of MemoryPolicyConfig opaque
	// device configuration parameters.
	APIVersion = "cxl.generic/v1alpha1"
	// Kind is the kind of MemoryPolicyConfig opaque device
	// configuration parameters.
	Kind = "MemoryPolicyConfig"
)

// MemoryPolicyConfig is the opaque device configuration that tells
// how a container consumes the DRAM and CXL memory of its claims.
type MemoryPolicyConfig struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	cgmpolmgr.Policy
}

// Validate returns an error if the configuration is not usable.
func (c *MemoryPolicyConfig) Validate() error {
	if c.APIVersion != APIVersion {
		return fmt.Errorf("unsupported apiVersion %q, expected %q", c.APIVersion, APIVersion)
	}
	if c.Kind != Kind {
		return fmt.Errorf("unsupported kind %q, expected %q", c.Kind, Kind)
	}
	return c.Policy.Validate()
}
