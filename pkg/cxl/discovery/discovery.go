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

package discovery

import (
	"os"
	"path"
	"strings"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/helpers"

	"k8s.io/klog/v2"
)

// Detect devices from sysfs.
func DiscoverDevices(sysfsDir, namingStyle string) map[string]*device.DeviceInfo {

	sysfsDriverDir := path.Join(sysfsDir, device.SysfsDriverPath)

	devices := make(map[string]*device.DeviceInfo)

	driverDirFiles, err := os.ReadDir(sysfsDriverDir)
	if err != nil {
		if os.IsNotExist(err) {
			klog.V(5).Infof("No Intel CXL devices found on this host. %v does not exist", sysfsDriverDir)
			return devices
		}
		klog.Errorf("could not read sysfs directory %v: %v", driverDirFiles, err)
		return devices
	}

	return scanDevicesFromDriverDirFiles(driverDirFiles, sysfsDriverDir, namingStyle)

}

func scanDevicesFromDriverDirFiles(driverDirFiles []os.DirEntry, sysfsDriverDir string, namingStyle string) map[string]*device.DeviceInfo {
	devices := map[string]*device.DeviceInfo{}
	for _, pciAddress := range driverDirFiles {
		devicePCIAddress := pciAddress.Name()
		// check if file is PCI device
		if !device.PciRegexp.MatchString(devicePCIAddress) {
			continue
		}
		klog.V(5).Infof("Found CXL PCI device: %s", devicePCIAddress)

		driverDeviceDir := path.Join(sysfsDriverDir, devicePCIAddress)
		// Read PCI device ID.
		deviceIdFile := path.Join(driverDeviceDir, "device")
		deviceIdBytes, err := os.ReadFile(deviceIdFile)
		if err != nil {
			klog.Errorf("failed detecting device %v PCI ID: %+v", devicePCIAddress, err)
			continue
		}
		deviceId := strings.TrimSpace(string(deviceIdBytes))

		uid := helpers.DeviceUIDFromPCIinfo(devicePCIAddress, deviceId)
		klog.V(5).Infof("New cxl UID: %v", uid)
		newDeviceInfo := &device.DeviceInfo{
			UID:        uid,
			PCIAddress: devicePCIAddress,
			Model:      deviceId,
			PCIRoot:    helpers.DeterminePCIRoot(driverDeviceDir),
		}

		devices[determineDeviceName(newDeviceInfo, namingStyle)] = newDeviceInfo
	}

	return devices
}

func determineDeviceName(info *device.DeviceInfo, namingStyle string) string {
	if namingStyle == "classic" {
		return "cxl" + info.PCIAddress
	}

	return info.UID
}
