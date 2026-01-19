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
)

var (
	PciRegexp = regexp.MustCompile(`[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-7]$`)
	CXLRegexp = regexp.MustCompile(`^cxl[0-9]+$`)
)

const (
	// TODO: FIXME: remove if not needed
	SysfsDriverPath = "bus/pci/drivers/cxldriver?"

	CDIVendor        = "intel.com"
	CDIClass         = "cxl"
	CDIKind          = CDIVendor + "/" + CDIClass
	DriverName       = CDIClass + "." + CDIVendor
	PCIAddressLength = len("0000:00:00.0")

	CXLDevicePattern = "cxl[0-9]*"

	PreparedClaimsFileName = "preparedClaims.json"

	// "classic", for CDI names to be more user-friendly, and readable when discovery code is used by cdi-specs-generator,
	// or "machine" for names to be used by DRA drivers. See determineDeviceName().
	DefaultNamingStyle = "machine"

	// From device-plugin.
	DefaultMyFlag1    = "flag1value"
	DefaultMyFlag2    = "flag2Value"
	MyFlag1EnvVarName = "value1"
	MyFlag2EnvVarName = "value2"
)

// DeviceInfo is an internal structure type to store info about discovered device.
type DeviceInfo struct {
	// UID is a unique identifier on node, used in ResourceSlice K8s API object as RFC1123-compliant identifier.
	// Consists of PCIAddress and Model with colons and dots replaced with hyphens, e.g. 0000-01-02-0-0x1234.
	UID        string `json:"uid"`
	PCIAddress string `json:"pciaddress"` // PCI address in Linux DBDF notation for use with sysfs, e.g. 0000:00:00.0
	Model      string `json:"model"`      // PCI device ID
	PCIRoot    string `json:"pciroot"`    // PCI Root complex ID
}

func (g DeviceInfo) CDIName() string {
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
