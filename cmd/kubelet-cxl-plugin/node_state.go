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
	"context"
	"fmt"
	"time"

	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	"k8s.io/klog/v2"
	cdiapi "tags.cncf.io/container-device-interface/pkg/cdi"
	cdiparser "tags.cncf.io/container-device-interface/pkg/parser"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/cdihelpers"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/helpers"

	nricxl "github.com/containers/nri-plugins/pkg/cxl"
)

type nodeState struct {
	*helpers.NodeState
	config *DriverConfig
}

func newNodeState(detectedDevices *nricxl.Devices, cdiRoot, preparedClaimsFilePath, nodeName string, driverConfig *DriverConfig) (*nodeState, error) {
	// was: detectedDevices map[string]*device.DeviceInfo
	klog.V(5).Info("Refreshing CDI registry")
	if err := cdiapi.Configure(cdiapi.WithSpecDirs(cdiRoot)); err != nil {
		return nil, fmt.Errorf("unable to refresh the CDI registry: %v", err)
	}

	cdiCache := cdiapi.GetDefaultCache()

	devInfo, err := buildDevInfos(driverConfig, detectedDevices)
	if err != nil {
		return nil, fmt.Errorf("failed to filter discovered devices: %v", err)
	}
	if err := cdihelpers.AddDetectedDevicesToCDIRegistry(cdiCache, devInfo, true); err != nil {
		return nil, fmt.Errorf("failed to add detected devices to CDI registry: %v", err)
	}
	time.Sleep(250 * time.Millisecond)

	klog.V(5).Info("Allocatable devices after CDI registry refresh:")
	for duid, ddev := range devInfo {
		klog.V(5).Infof("CDI device: %v : %+v", duid, ddev)
	}

	// TODO: should be only create prepared claims, discard old preparations. Do we even need the snapshot?
	preparedClaims, err := helpers.GetOrCreatePreparedClaims(preparedClaimsFilePath)
	if err != nil {
		klog.Errorf("failed to get prepared claims: %v", err)
		return nil, fmt.Errorf("failed to get prepared claims: %v", err)
	}

	klog.V(5).Info("Creating NodeState")
	// TODO: allocatable should include cdi-described
	state := nodeState{
		NodeState: &helpers.NodeState{
			CdiCache:               cdiCache,
			Allocatable:            devInfo,
			Prepared:               preparedClaims,
			PreparedClaimsFilePath: preparedClaimsFilePath,
			NodeName:               nodeName,
		},
		config: driverConfig,
	}
	fmt.Printf("type ofstate.Allocatable: %T\n", state.Allocatable)

	allocatableDevices, ok := state.Allocatable.(device.DevicesInfo)
	if !ok {
		return nil, fmt.Errorf("unexpected type for state.Allocatable")
	}

	klog.V(5).Infof("Synced state with CDI and CXLAllocationState: %+v", state)
	for duid, ddev := range allocatableDevices {
		klog.V(5).Infof("Allocatable device: %v : %+v", duid, ddev)
	}

	return &state, nil
}

func buildDevInfos(driverConfig *DriverConfig, detectedDevices *nricxl.Devices) (device.DevicesInfo, error) {
	var anyDev interface{}
	devInfos := make(map[string]*device.DeviceInfo)
	for _, regDev := range detectedDevices.RegionDevices {
		klog.V(3).Infof("discovered CXL region device: %+v", regDev)
		ignoreReason := ""
		if !regDev.Enabled {
			ignoreReason = "region is disabled"
		}
		for _, ignoreRegSpec := range driverConfig.IgnoreRegions {
			if ignoreReason != "" {
				break
			}
			if regDev.Name == ignoreRegSpec {
				ignoreReason = fmt.Sprintf("region name %q", ignoreRegSpec)
			}
		}
		for _, ignoreNode := range driverConfig.IgnoreNodes {
			if ignoreReason != "" {
				break
			}
			if regDev.Node == ignoreNode {
				ignoreReason = fmt.Sprintf("region is on node %d", ignoreNode)
			}
		}
		for _, memDev := range regDev.Memories {
			if ignoreReason != "" {
				break
			}
			for _, ignoreDevSpec := range driverConfig.IgnoreDevices {
				if memDev.Name == ignoreDevSpec {
					ignoreReason = fmt.Sprintf("region has a memory device name %q", ignoreDevSpec)
					break
				}
				if memDev.DevName == ignoreDevSpec {
					ignoreReason = fmt.Sprintf("region has a memory uevent device name %q", ignoreDevSpec)
					break
				}
				if fmt.Sprintf("%d:%d", memDev.Major, memDev.Minor) == ignoreDevSpec {
					ignoreReason = fmt.Sprintf("region has a memory device with major:minor %q", ignoreDevSpec)
					break
				}
				serialDec := fmt.Sprintf("%d", memDev.Serial)
				serialHex := fmt.Sprintf("0x%x", memDev.Serial)
				serialHexNoPrefix := fmt.Sprintf("%x", memDev.Serial)
				if serialDec == ignoreDevSpec || serialHex == ignoreDevSpec || serialHexNoPrefix == ignoreDevSpec {
					ignoreReason = fmt.Sprintf("region has a memory device serial %q", ignoreDevSpec)
					break
				}

			}
		}
		if ignoreReason != "" {
			klog.V(4).Infof("- ignoring region device %q: %s", regDev.Name, ignoreReason)
			continue
		}
		cxlDevName := fmt.Sprintf("cxl-%s-node%d", regDev.Name, regDev.Node)
		anyDev = regDev
		devInfo := device.DeviceInfo{
			Name:      cxlDevName,
			CxlDev:    &anyDev,
			SysfsPath: regDev.SysfsPath,
		}
		devInfos[regDev.Name] = &devInfo
	}
	return devInfos, nil
}

func (s *nodeState) GetResources() resourceslice.DriverResources {
	s.Lock()
	defer s.Unlock()

	devices := []resourcev1.Device{}

	allocatableDevices, ok := s.Allocatable.(device.DevicesInfo)
	if !ok {
		klog.Errorf("internal error: unexpected type for state.Allocatable %T", s.Allocatable)
		return resourceslice.DriverResources{}
	}

	for cxlUID, allocatableCXL := range allocatableDevices {
		klog.V(5).Infof("Processing allocatable device %v: %+v", cxlUID, allocatableCXL)
		var cxlDev interface{}
		if allocatableCXL.CxlDev == nil {
			continue
		}
		cxlDev = *allocatableCXL.CxlDev
		switch dev := cxlDev.(type) {
		case *nricxl.RegionDevice:
			node := int64(dev.Node)
			newDevice := resourcev1.Device{
				Name: cxlUID,
				// Populate ResourceSlice.Device.Attributes from device.DeviceInfo.
				Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
					"name": {
						StringValue: &dev.Name,
					},
					"node": {
						IntValue: &node,
					},
				},
				Capacity: map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{
					"size": {
						Value: *resource.NewQuantity(int64(dev.Size), resource.BinarySI),
					},
				},
				AllowMultipleAllocations: ptr(true),
			}
			devices = append(devices, newDevice)
		default:
			klog.Warningf("unexpected device type in state.Allocatable: %T", cxlDev)
		}
	}

	driverResource := resourceslice.DriverResources{
		Pools: map[string]resourceslice.Pool{
			s.NodeName: {
				Slices: []resourceslice.Slice{{
					Devices: devices,
				}}}},
	}

	return driverResource
}

func ptr[T any](v T) *T {
	return &v
}

func (s *nodeState) Prepare(ctx context.Context, claim *resourcev1.ResourceClaim) error {
	// To prevent concurrent writing of prepared claims file and potential data loss.
	s.Lock()
	defer s.Unlock()

	if claim.Status.Allocation == nil {
		return fmt.Errorf("no allocation found in claim %v/%v status", claim.Namespace, claim.Name)
	}

	allocatedDevices, err := s.prepareAllocatedDevices(ctx, claim)
	if err != nil {
		return err
	}

	s.Prepared[string(claim.UID)] = allocatedDevices

	if err = helpers.WritePreparedClaimsToFile(s.PreparedClaimsFilePath, s.Prepared); err != nil {
		klog.Errorf("failed to write prepared claims to file: %v", err)
		return fmt.Errorf("failed to write prepared claims to file: %v", err)
	}

	klog.V(5).Infof("Created prepared claim %v allocation", claim.UID)
	return nil
}

func (s *nodeState) prepareAllocatedDevices(ctx context.Context, claim *resourcev1.ResourceClaim) (allocatedDevices kubeletplugin.PrepareResult, err error) {
	allocatedDevices = kubeletplugin.PrepareResult{}

	for _, allocatedDevice := range claim.Status.Allocation.Devices.Results {
		// ATM the only pool is cluster node's pool: all devices on current node.
		if allocatedDevice.Driver != device.DriverName || allocatedDevice.Pool != s.NodeName {
			klog.Infof("ignoring claim allocation device %+v", allocatedDevice)
			continue
		}

		allocatableDevices, _ := s.Allocatable.(map[string]*device.DeviceInfo)

		allocatableDevice, found := allocatableDevices[allocatedDevice.Device]
		if !found {
			return allocatedDevices, fmt.Errorf("could not find allocatable device %v (pool %v)", allocatedDevice.Device, allocatedDevice.Pool)
		}

		newDevice := kubeletplugin.Device{
			Requests:     []string{allocatedDevice.Request},
			PoolName:     allocatedDevice.Pool,
			DeviceName:   allocatedDevice.Device,
			CDIDeviceIDs: []string{allocatableDevice.CDIName()},
		}
		allocatedDevices.Devices = append(allocatedDevices.Devices, newDevice)

	}

	if len(allocatedDevices.Devices) > 0 {
		cdiName := cdiparser.QualifiedName(device.CDIVendor, device.CDIClass, string(claim.UID))
		allocatedDevices.Devices[0].CDIDeviceIDs = append(allocatedDevices.Devices[0].CDIDeviceIDs, cdiName)
	}

	return allocatedDevices, nil
}
