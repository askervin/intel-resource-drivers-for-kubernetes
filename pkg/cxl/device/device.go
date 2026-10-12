/*
 * Copyright (c) 2024, Intel Corporation.  All Rights Reserved.
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

package device

import (
	"fmt"
	"regexp"

	resourcev1 "k8s.io/api/resource/v1"
)

var (
	PciRegexp = regexp.MustCompile(`[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-7]$`)
	CXLRegexp = regexp.MustCompile(`^cxl[0-9]+$`)
)

const (
	CDIVendor        = "generic"
	CDIClass         = "cxl"
	CDIKind          = CDIVendor + "/" + CDIClass
	DriverName       = CDIClass + "." + CDIVendor
	PCIAddressLength = len("0000:00:00.0")

	CXLDevicePattern = "cxl-[-a-z0-9]*"

	// DeviceTypeCXLNode is the device type for CXL region devices.
	DeviceTypeCXLNode = "cxl-node"
	// DeviceTypeDRAM is the device type for system DRAM devices.
	DeviceTypeDRAM = "dram"
	// DeviceTypeCXLPool is the device type for CXL memory devices
	// attached from a memory pool (driver PoolDriverName).
	DeviceTypeCXLPool = "cxl-pool"

	// PoolDriverName is the default DRA driver name of the pool
	// helper, which prepares CXL memory devices hotplugged from a
	// memory pool. See doc/cxl/POOL.md.
	PoolDriverName = "cxl-pool.generic"
	// PoolCDISpecName is the name of the CDI spec file of the
	// prepared pool devices.
	PoolCDISpecName = "cxl-pool"

	// DeviceTypeDynamic is the device type of the token device
	// "dynamic" in the cxl.generic slice. A claim on it gives the
	// containers that reference it a lease socket through which they
	// can borrow pool memory while running. See doc/cxl/POOL.md.
	DeviceTypeDynamic = "dynamic"
	// DynamicDeviceName is the name of the token device.
	DynamicDeviceName = "dynamic"
	// LeaseCDISpecName is the name of the CDI spec file of the lease
	// devices (socket directory and CLI mounts of token claims).
	LeaseCDISpecName = "cxl-leases"
	// LendConfigKind is the kind of the opaque pool claim configuration
	// that names the lease and the borrowing container of a companion
	// claim (apiVersion <pool driver name>/v1alpha1).
	LendConfigKind = "LendConfig"
	// PoolDeviceConfigKind is the kind of the opaque pool claim
	// configuration that selects how a pool device is prepared: as a
	// NUMA node or as a devdax device, and read-only (apiVersion <pool
	// driver name>/v1alpha1).
	PoolDeviceConfigKind = "PoolDeviceConfig"

	// CapacityMemory is the capacity key used in ResourceSlice device
	// capacity and in ConsumedCapacity. Matches v1.ResourceMemory.
	CapacityMemory resourcev1.QualifiedName = "memory"

	PreparedClaimsFileName = "preparedClaims.json"

	// "classic", for CDI names to be more user-friendly, and readable when discovery code is used by cdi-specs-generator,
	// or "machine" for names to be used by DRA drivers. See determineDeviceName().
	DefaultNamingStyle = "machine"

	// From device-plugin.
	DefaultSysfsRoot    = ""
	DefaultConfigStr    = ""
	SysfsRootEnvVarName = "SYSFS_ROOT"
	ConfigStrEnvVarName = "CONFIG_STR"
)

// DeviceInfo is an internal structure type to store info about discovered device.
type DeviceInfo struct {
	// UID is a unique identifier on node, used in ResourceSlice K8s API object as RFC1123-compliant identifier.
	// Consists of PCIAddress and Model with colons and dots replaced with hyphens, e.g. 0000-01-02-0-0x1234.
	UID       string `json:"uid"`
	Name      string `json:"name"`
	Dev       any
	SysfsPath string `json:"sysfs"`
}

type SystemDRAM struct {
	Name  string
	Nodes []int
	Size  uint64
}

// DynamicToken is the Dev of the token device: it has no hardware, no
// capacity and can be allocated any number of times.
type DynamicToken struct{}

func (g *DeviceInfo) CDIName() string {
	return fmt.Sprintf("%s=%s", CDIKind, g.UID)
}

func (g *DeviceInfo) DeepCopy() *DeviceInfo {
	di := *g
	return &di
}

// DevicesInfo is a dictionary with DeviceInfo.uid being the key.
type DevicesInfo map[string]*DeviceInfo

func (g *DevicesInfo) DeepCopy() DevicesInfo {
	devicesInfoCopy := DevicesInfo{}
	for duid, device := range *g {
		devicesInfoCopy[duid] = device.DeepCopy()
	}
	return devicesInfoCopy
}
