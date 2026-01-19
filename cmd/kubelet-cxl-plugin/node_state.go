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
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	"k8s.io/klog/v2"
	cdiapi "tags.cncf.io/container-device-interface/pkg/cdi"
	cdiparser "tags.cncf.io/container-device-interface/pkg/parser"

	cdihelpers "github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/cdihelpers"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/helpers"
)

type nodeState struct {
	*helpers.NodeState
	myflag1 string
	myflag2 string
}

func newNodeState(detectedDevices map[string]*device.DeviceInfo, cdiRoot, preparedClaimsFilePath, nodeName, myflag1, myflag2 string) (*nodeState, error) {
	for ddev := range detectedDevices {
		klog.V(3).Infof("new device: %+v", ddev)
	}

	klog.V(5).Info("Refreshing CDI registry")
	if err := cdiapi.Configure(cdiapi.WithSpecDirs(cdiRoot)); err != nil {
		return nil, fmt.Errorf("unable to refresh the CDI registry: %v", err)
	}

	cdiCache := cdiapi.GetDefaultCache()

	// syncDetectedDevicesWithRegistry overrides uid in detecteddevices from existing cdi spec
	if err := cdihelpers.AddDetectedDevicesToCDIRegistry(cdiCache, detectedDevices, true); err != nil {
		return nil, fmt.Errorf("unable to sync detected devices to CDI registry: %v", err)
	}

	time.Sleep(250 * time.Millisecond)

	klog.V(5).Info("Allocatable devices after CDI registry refresh:")
	for duid, ddev := range detectedDevices {
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
			Allocatable:            detectedDevices,
			Prepared:               preparedClaims,
			PreparedClaimsFilePath: preparedClaimsFilePath,
			NodeName:               nodeName,
		},
		myflag1: myflag1,
		myflag2: myflag2,
	}

	allocatableDevices, ok := state.Allocatable.(map[string]*device.DeviceInfo)
	if !ok {
		return nil, fmt.Errorf("unexpected type for state.Allocatable")
	}

	klog.V(5).Infof("Synced state with CDI and CXLAllocationState: %+v", state)
	for duid, ddev := range allocatableDevices {
		klog.V(5).Infof("Allocatable device: %v : %+v", duid, ddev)
	}

	return &state, nil
}

func (s *nodeState) GetResources() resourceslice.DriverResources {
	s.Lock()
	defer s.Unlock()

	devices := []resourcev1.Device{}

	allocatableDevices, _ := s.Allocatable.(map[string]*device.DeviceInfo)
	for cxlUID, allocatableCXL := range allocatableDevices {
		newDevice := resourcev1.Device{
			Name: cxlUID,
			// Populate ResourceSlice.Device.Attributes from device.DeviceInfo.
			Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
				"pciRoot": {
					StringValue: &allocatableCXL.PCIRoot,
				},
			},
		}
		devices = append(devices, newDevice)
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
