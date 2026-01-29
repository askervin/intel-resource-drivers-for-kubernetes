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
	"fmt"

	externalcxl "github.com/containers/nri-plugins/pkg/cxl"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"

	"k8s.io/klog/v2"
)

// DiscoverDevices discovers CXL regions using the external CXL package.
// It returns a map of CXL node resources, where each resource represents
// memory from a CXL region backed by a NUMA node.
func DiscoverDevices(sysfsDir, namingStyle string) map[string]*device.DeviceInfo {
	devices := make(map[string]*device.DeviceInfo)

	// Use DevicesFromSysfs() from external cxl package
	cxlDevices, err := externalcxl.DevicesFromSysfs(sysfsDir)
	if err != nil {
		klog.Errorf("Failed to discover CXL devices from sysfs: %v", err)
		return devices
	}

	if cxlDevices == nil {
		klog.V(5).Info("No CXL devices found")
		return devices
	}

	// Process each region device
	regionDevices := cxlDevices.GetRegionDevices()
	klog.V(3).Infof("Found %d CXL region devices", len(regionDevices))

	for _, region := range regionDevices {
		// Only process enabled regions with valid NUMA nodes
		if !region.Enabled {
			klog.V(5).Infof("Skipping disabled region %s", region.GetName())
			continue
		}

		node := region.GetNode()
		if node < 0 {
			klog.V(5).Infof("Skipping region %s with invalid NUMA node %d", region.GetName(), node)
			continue
		}

		size := region.GetSize()
		if size == 0 {
			klog.V(5).Infof("Skipping region %s with zero size", region.GetName())
			continue
		}

		// Create a device representing this CXL memory node
		// Using "cxl-node<N>" naming to represent memory on NUMA node N
		deviceName := fmt.Sprintf("cxl-node%d", node)
		klog.V(3).Infof("Discovered CXL region %s: node=%d, size=%d bytes, mode=%s", 
			region.GetName(), node, size, region.GetMode())

		deviceInfo := &device.DeviceInfo{
			UID:          deviceName,
			PCIAddress:   "", // Not applicable for memory nodes
			Model:        region.GetMode(),
			PCIRoot:      "", // Not applicable for memory nodes
			MemorySize:   size,
			MemoryNode:   node,
			RegionName:   region.GetName(),
		}

		devices[deviceName] = deviceInfo
	}

	klog.V(3).Infof("Discovered %d CXL memory nodes", len(devices))
	return devices
}
