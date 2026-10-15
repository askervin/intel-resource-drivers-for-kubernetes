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

// Package device has the Kubernetes-facing names of the CXL DRA driver:
// driver, device, attribute, capacity, DeviceClass, config, label and CDI
// names, and the devices that cxl.generic publishes.
package device

import (
	resourcev1 "k8s.io/api/resource/v1"
)

const (
	// CDIVendor is the CDI vendor of both drivers.
	CDIVendor = "generic"
	// CDIClass is the CDI class of cxl.generic.
	CDIClass = "cxl"
	// CDIKind is the CDI kind of cxl.generic.
	CDIKind = CDIVendor + "/" + CDIClass
	// PoolCDIClass is the CDI class of the pool driver.
	PoolCDIClass = "cxl-pool"
	// PoolCDIKind is the CDI kind of the pool driver.
	PoolCDIKind = CDIVendor + "/" + PoolCDIClass
	// ClaimsCDISpecName is the CDI spec file of the claims of cxl.generic.
	ClaimsCDISpecName = "cxl-claims"
	// LeasesCDISpecName is the CDI spec file of the lease claims (socket
	// directory and CLI mounts).
	LeasesCDISpecName = "cxl-leases"
	// PoolCDISpecName is the CDI spec file of the claims of the pool
	// driver.
	PoolCDISpecName = "cxl-pool"

	// DriverName is the DRA driver of the devices attached to a node.
	DriverName = CDIClass + "." + CDIVendor
	// PoolDriverName is the default DRA driver name of the pool driver,
	// which prepares CXL memory devices hotplugged from a memory pool.
	// See doc/cxl/POOL.md.
	PoolDriverName = "cxl-pool.generic"

	// DeviceTypeMemdev is a CXL memory device: a slice of a local memdev
	// or a whole pool device per claim.
	DeviceTypeMemdev = "memdev"
	// DeviceTypeRegion is a region that existed before the plugin, online
	// as a NUMA node.
	DeviceTypeRegion = "region"
	// DeviceTypeDRAM is the system DRAM of the node.
	DeviceTypeDRAM = "dram"
	// DeviceTypeLease is the device whose claims give containers the
	// lease socket.
	DeviceTypeLease = "lease"

	// Device attributes.
	AttrType            resourcev1.QualifiedName = "type"
	AttrSource          resourcev1.QualifiedName = "source"
	AttrSerial          resourcev1.QualifiedName = "serial"
	AttrShared          resourcev1.QualifiedName = "shared"
	AttrSize            resourcev1.QualifiedName = "size"
	AttrNode            resourcev1.QualifiedName = "node"
	AttrNodes           resourcev1.QualifiedName = "nodes"
	AttrNodeAffinity    resourcev1.QualifiedName = "nodeAffinity"
	AttrMemoryBlockSize resourcev1.QualifiedName = "memoryBlockSize"

	// CapacityMemory is the memory capacity of a device in bytes.
	// Matches v1.ResourceMemory.
	CapacityMemory resourcev1.QualifiedName = "memory"
	// CapacityHosts is the capacity of a shared pool device: how many
	// nodes may have it attached at a time.
	CapacityHosts resourcev1.QualifiedName = "hosts"

	// DeviceClasses (deployments/cxl/device-classes.yaml).
	ClassLocal      = "cxl-local"
	ClassPool       = "cxl-pool"
	ClassPoolShared = "cxl-pool-shared"
	ClassRegion     = "cxl-region"
	ClassDRAM       = "cxl-dram"
	ClassLease      = "cxl-lease"

	// DeviceConfigKind is the kind of the opaque configuration of how a
	// device is prepared (mode, readOnly), apiVersion <driver>/v1alpha1.
	DeviceConfigKind = "DeviceConfig"

	// LeaseDeviceName is the name of the lease device.
	LeaseDeviceName = "lease"
	// MemdevDevicePrefix is the prefix of the device names of local
	// memdevs ("memdev-" + lower-case hex serial).
	MemdevDevicePrefix = "memdev-"
	// DRAMDeviceName is the name of the DRAM device.
	DRAMDeviceName = "dram"

	// Labels and annotation of the companion claim and pod of a lease.
	LeaseLabel          = "cxl.generic/lease"
	RequesterUIDLabel   = "cxl.generic/requester-uid"
	NodeLabel           = "cxl.generic/node"
	RequesterAnnotation = "cxl.generic/requester" // <namespace>/<pod>/<container>

	PreparedClaimsFileName = "preparedClaims.json"

	// From device-plugin.
	DefaultSysfsRoot    = ""
	DefaultConfigStr    = ""
	SysfsRootEnvVarName = "SYSFS_ROOT"
	ConfigStrEnvVarName = "CONFIG_STR"
)

// DeviceInfo is an internal structure type to store info about discovered device.
type DeviceInfo struct {
	// UID is a unique identifier on node, used in ResourceSlice K8s API object as RFC1123-compliant identifier.
	UID       string `json:"uid"`
	Name      string `json:"name"`
	Dev       any
	SysfsPath string `json:"sysfs"`
}

// DRAM is the Dev of the DRAM device.
type DRAM struct {
	Nodes []int
	Size  uint64
}

// Memdev is the Dev of a local memdev device (DeviceTypeMemdev).
type Memdev struct {
	Name         string // memN
	Serial       uint64
	Size         uint64 // RAM capacity, bytes
	NodeAffinity int    // CPU NUMA node of the memdev, -1 if unknown
	BlockSize    uint64 // memory block size, bytes: slices are multiples of it
}

// LeaseToken is the Dev of the lease device: it has no hardware, no
// capacity and can be allocated any number of times.
type LeaseToken struct{}

// DeepCopy returns a copy of the device info.
func (g *DeviceInfo) DeepCopy() *DeviceInfo {
	di := *g
	return &di
}

// DevicesInfo is a dictionary with DeviceInfo.uid being the key.
type DevicesInfo map[string]*DeviceInfo

// DeepCopy returns a copy of the devices.
func (g *DevicesInfo) DeepCopy() DevicesInfo {
	devicesInfoCopy := DevicesInfo{}
	for duid, device := range *g {
		devicesInfoCopy[duid] = device.DeepCopy()
	}
	return devicesInfoCopy
}
