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
// memory from CXL regions backed by a NUMA node. If multiple regions map
// to the same NUMA node, their memory sizes are aggregated.
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

	// Process each region device and aggregate by NUMA node
	regionDevices := cxlDevices.GetRegionDevices()
	klog.V(3).Infof("Found %d CXL region devices", len(regionDevices))

	// Track regions per NUMA node for aggregation
	nodeToRegions := make(map[int][]*externalcxl.RegionDevice)

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

		nodeToRegions[node] = append(nodeToRegions[node], region)
	}

	// Create aggregated devices per NUMA node
	for node, regions := range nodeToRegions {
		var totalSize uint64
		var regionNames []string
		var mode string

		for _, region := range regions {
			totalSize += region.GetSize()
			regionNames = append(regionNames, region.GetName())
			if mode == "" {
				mode = region.GetMode()
			}
			klog.V(3).Infof("Discovered CXL region %s: node=%d, size=%d bytes, mode=%s",
				region.GetName(), node, region.GetSize(), region.GetMode())
		}

		// Create a device representing this CXL memory node
		// Using "cxl-node<N>" naming to represent memory on NUMA node N
		deviceName := fmt.Sprintf("cxl-node%d", node)
		deviceInfo := &device.DeviceInfo{
			UID:        deviceName,
			PCIAddress: "", // Not applicable for memory nodes
			Model:      mode,
			PCIRoot:    "", // Not applicable for memory nodes
			MemorySize: totalSize,
			MemoryNode: node,
			RegionName: fmt.Sprintf("%v", regionNames), // Store all region names
		}

		devices[deviceName] = deviceInfo
		klog.V(3).Infof("Created aggregated CXL node %s: regions=%v, total_size=%d bytes",
			deviceName, regionNames, totalSize)
	}

	klog.V(3).Infof("Discovered %d CXL memory nodes", len(devices))
	return devices
}
