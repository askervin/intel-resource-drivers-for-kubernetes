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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	v1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	"k8s.io/klog/v2"
	"k8s.io/utils/cpuset"

	cdiapi "tags.cncf.io/container-device-interface/pkg/cdi"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/cdihelpers"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/memorypolicy"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/helpers"

	nricxl "github.com/containers/nri-plugins/pkg/cxl"
)

// PreparedPolicies maps claim UIDs to their parsed memory policy configs.
type PreparedPolicies map[string]*memorypolicy.MemoryPolicyConfig

type nodeState struct {
	*helpers.NodeState
	config           *DriverConfig
	preparedPolicies PreparedPolicies
	policiesFilePath string
	// podClaims maps pod UID → set of claim UIDs prepared for that pod.
	// Built during Prepare from claim.Status.ReservedFor.
	podClaims map[string]map[string]bool
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

	policiesFilePath := filepath.Join(filepath.Dir(preparedClaimsFilePath), "preparedPolicies.json")
	policies, err := getOrCreatePreparedPolicies(policiesFilePath)
	if err != nil {
		klog.Errorf("failed to get prepared policies: %v", err)
		return nil, fmt.Errorf("failed to get prepared policies: %v", err)
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
		config:           driverConfig,
		preparedPolicies: policies,
		policiesFilePath: policiesFilePath,
		podClaims:        make(map[string]map[string]bool),
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
	cxlNodes := make(map[int]bool)
	devInfos := make(map[string]*device.DeviceInfo)
	for _, regDev := range detectedDevices.RegionDevices {
		klog.V(3).Infof("discovered CXL region device: %+v", regDev)
		ignoreReason := ""
		if !regDev.Enabled {
			ignoreReason = "region is disabled"
		}
		cxlNodes[regDev.Node] = true
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
		devInfo := device.DeviceInfo{
			Name:      cxlDevName,
			Dev:       any(regDev),
			SysfsPath: regDev.SysfsPath,
		}
		devInfos[regDev.Name] = &devInfo
	}
	if !driverConfig.IgnoreDRAMNodes {
		dramDev := &device.SystemDRAM{
			Name: "system-dram",
		}
		for _, node := range detectedDevices.MemoryNodes {
			if cxlNodes[node.ID] {
				continue
			}
			ignoreReason := ""
			if node.Size == 0 {
				ignoreReason = "node has 0 size"
			}
			for _, ignoreNode := range driverConfig.IgnoreNodes {
				if ignoreReason != "" {
					break
				}
				if node.ID == ignoreNode {
					ignoreReason = fmt.Sprintf("node ID %d is in ignoreNodes", ignoreNode)
				}
			}
			if ignoreReason != "" {
				klog.V(4).Infof("- ignoring DRAM on node %d: %s", node.ID, ignoreReason)
				continue
			}
			dramDev.Nodes = append(dramDev.Nodes, node.ID)
			dramDev.Size += node.Size
			klog.V(4).Infof("discovered DRAM memory on node %d: size %d bytes", node.ID, node.Size)
		}
		if dramDev.Size > 0 {
			devInfos["dram"] = &device.DeviceInfo{
				Name: "dram",
				Dev:  any(dramDev),
			}
			klog.V(3).Infof("discovered system DRAM device: %+v", dramDev)
		}
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
		if allocatableCXL.Dev == nil {
			continue
		}
		switch dev := allocatableCXL.Dev.(type) {
		case *nricxl.RegionDevice:
			node := int64(dev.Node)
			newDevice := resourcev1.Device{
				Name: cxlUID,
				// Populate ResourceSlice.Device.Attributes from device.DeviceInfo.
				Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
					"type": {
						StringValue: ptr("cxl-node"),
					},
					"name": {
						StringValue: &dev.Name,
					},
					"node": {
						IntValue: &node,
					},
				},
				Capacity: map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{
					"memory": {
						Value: *resource.NewQuantity(int64(dev.Size), resource.BinarySI),
					},
				},
				NodeAllocatableResourceMappings: map[v1.ResourceName]resourcev1.NodeAllocatableResourceMapping{
					v1.ResourceMemory: {
						CapacityKey: ptr(resourcev1.QualifiedName("memory")),
					},
				},
				AllowMultipleAllocations: ptr(true),
			}
			devices = append(devices, newDevice)
		case *device.SystemDRAM:
			// convert dev.Nodes into a string
			// representation, e.g. "0,1,2", use
			// kubernetes cpusets library to format it.
			nodeSet := cpuset.New(dev.Nodes...)
			nodeList := nodeSet.String()
			newDevice := resourcev1.Device{
				Name: cxlUID,
				Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
					"type": {
						StringValue: ptr("dram"),
					},
					"name": {
						StringValue: &dev.Name,
					},
					"nodes": {
						StringValue: &nodeList,
					},
				},
				Capacity: map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{
					"memory": {
						Value: *resource.NewQuantity(int64(dev.Size), resource.BinarySI),
					},
				},
				NodeAllocatableResourceMappings: map[v1.ResourceName]resourcev1.NodeAllocatableResourceMapping{
					v1.ResourceMemory: {
						CapacityKey: ptr(resourcev1.QualifiedName("memory")),
					},
				},
				AllowMultipleAllocations: ptr(true),
			}
			devices = append(devices, newDevice)
		default:
			klog.Warningf("unexpected device type in state.Allocatable: %T", dev)
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

// Unprepare overrides helpers.NodeState.Unprepare to also clean up
// the memory policy associated with a claim.
func (s *nodeState) Unprepare(ctx context.Context, claimUID string) error {
	s.Lock()
	defer s.Unlock()

	if _, found := s.Prepared[claimUID]; !found {
		return nil
	}

	klog.V(5).Infof("Freeing devices from claim %v", claimUID)
	delete(s.Prepared, claimUID)
	delete(s.preparedPolicies, claimUID)

	// Clean up pod→claim mapping.
	for podUID, claims := range s.podClaims {
		delete(claims, claimUID)
		if len(claims) == 0 {
			delete(s.podClaims, podUID)
		}
	}

	if err := helpers.WritePreparedClaimsToFile(s.PreparedClaimsFilePath, s.Prepared); err != nil {
		return fmt.Errorf("failed to write prepared claims to file: %v", err)
	}

	if err := writePreparedPoliciesToFile(s.policiesFilePath, s.preparedPolicies); err != nil {
		return fmt.Errorf("failed to write prepared policies to file: %v", err)
	}

	return nil
}

func (s *nodeState) Prepare(ctx context.Context, claim *resourcev1.ResourceClaim) error {
	// To prevent concurrent writing of prepared claims file and potential data loss.
	s.Lock()
	defer s.Unlock()

	if claim.Status.Allocation == nil {
		return fmt.Errorf("no allocation found in claim %v/%v status", claim.Namespace, claim.Name)
	}

	allocatedDevices, policy, err := s.prepareAllocatedDevices(ctx, claim)
	if err != nil {
		return err
	}

	s.Prepared[string(claim.UID)] = allocatedDevices
	if policy != nil {
		s.preparedPolicies[string(claim.UID)] = policy
		klog.V(3).Infof("Claim %v has memory policy: order=%s minStep=%s maxStep=%s",
			claim.UID, policy.MemoryUseOrder, policy.MinStep, policy.MaxStep)
	}

	// Record pod→claim mapping from ReservedFor so that NRI
	// CreateContainer can look up policies by pod UID.
	claimUID := string(claim.UID)
	for _, consumer := range claim.Status.ReservedFor {
		if consumer.Resource == "pods" {
			podUID := string(consumer.UID)
			if s.podClaims[podUID] == nil {
				s.podClaims[podUID] = make(map[string]bool)
			}
			s.podClaims[podUID][claimUID] = true
			klog.V(5).Infof("Recorded pod %s (%s) → claim %s mapping", consumer.Name, podUID, claimUID)
		}
	}

	if err = helpers.WritePreparedClaimsToFile(s.PreparedClaimsFilePath, s.Prepared); err != nil {
		klog.Errorf("failed to write prepared claims to file: %v", err)
		return fmt.Errorf("failed to write prepared claims to file: %v", err)
	}

	if err = writePreparedPoliciesToFile(s.policiesFilePath, s.preparedPolicies); err != nil {
		klog.Errorf("failed to write prepared policies to file: %v", err)
		return fmt.Errorf("failed to write prepared policies to file: %v", err)
	}

	klog.V(5).Infof("Created prepared claim %v allocation", claim.UID)
	return nil
}

func (s *nodeState) prepareAllocatedDevices(ctx context.Context, claim *resourcev1.ResourceClaim) (allocatedDevices kubeletplugin.PrepareResult, policy *memorypolicy.MemoryPolicyConfig, err error) {
	allocatedDevices = kubeletplugin.PrepareResult{}

	for _, allocatedDevice := range claim.Status.Allocation.Devices.Results {
		// ATM the only pool is cluster node's pool: all devices on current node.
		if allocatedDevice.Driver != device.DriverName || allocatedDevice.Pool != s.NodeName {
			klog.Infof("ignoring claim allocation device %+v", allocatedDevice)
			continue
		}

		allocatableDevices, ok := s.Allocatable.(device.DevicesInfo)
		if !ok {
			return allocatedDevices, nil, fmt.Errorf("internal error: unexpected type for state.Allocatable %T", s.Allocatable)
		}

		allocatableDevice, found := allocatableDevices[allocatedDevice.Device]
		if !found {
			return allocatedDevices, nil, fmt.Errorf("could not find allocatable device %v (pool %v)", allocatedDevice.Device, allocatedDevice.Pool)
		}

		klog.V(5).Infof("TODO: device %v for claim %v: allocatable device info: %+v", allocatedDevice.Device, claim.UID, allocatableDevice)

		newDevice := kubeletplugin.Device{
			Requests:   []string{allocatedDevice.Request},
			PoolName:   allocatedDevice.Pool,
			DeviceName: allocatedDevice.Device,
			// no need to inject CDI devices // CDIDeviceIDs: []string{allocatableDevice.CDIName()},
		}
		allocatedDevices.Devices = append(allocatedDevices.Devices, newDevice)
	}

	// Parse opaque device configuration for memory policy.
	policy, err = parseMemoryPolicyFromClaim(claim)
	if err != nil {
		return allocatedDevices, nil, fmt.Errorf("failed to parse memory policy config from claim %v/%v: %w", claim.Namespace, claim.Name, err)
	}

	return allocatedDevices, policy, nil
}

// parseMemoryPolicyFromClaim extracts a MemoryPolicyConfig from the
// opaque device configuration entries in a claim's allocation. It
// looks at both claim-level and class-level config entries, with
// claim-level taking precedence.
func parseMemoryPolicyFromClaim(claim *resourcev1.ResourceClaim) (*memorypolicy.MemoryPolicyConfig, error) {
	if claim.Status.Allocation == nil {
		return nil, nil
	}

	var claimPolicy, classPolicy *memorypolicy.MemoryPolicyConfig

	for _, cfg := range claim.Status.Allocation.Devices.Config {
		if cfg.Opaque == nil || cfg.Opaque.Driver != device.DriverName {
			continue
		}

		var candidate memorypolicy.MemoryPolicyConfig
		if err := json.Unmarshal(cfg.Opaque.Parameters.Raw, &candidate); err != nil {
			klog.V(5).Infof("skipping opaque config entry for driver %s: unmarshal error: %v", device.DriverName, err)
			continue
		}

		if candidate.Kind != memorypolicy.Kind {
			klog.V(5).Infof("skipping opaque config entry with kind %q (expected %q)", candidate.Kind, memorypolicy.Kind)
			continue
		}

		if err := candidate.Validate(); err != nil {
			return nil, fmt.Errorf("invalid MemoryPolicyConfig in claim: %w", err)
		}

		switch cfg.Source {
		case resourcev1.AllocationConfigSourceClaim:
			claimPolicy = &candidate
		case resourcev1.AllocationConfigSourceClass:
			if classPolicy == nil {
				classPolicy = &candidate
			}
		}
	}

	// Claim-level config takes precedence over class-level.
	if claimPolicy != nil {
		return claimPolicy, nil
	}
	return classPolicy, nil
}

// GetMemoryPolicy returns the memory policy config for a given claim
// UID, or nil if no policy was configured.
func (s *nodeState) GetMemoryPolicy(claimUID string) *memorypolicy.MemoryPolicyConfig {
	s.Lock()
	defer s.Unlock()
	return s.preparedPolicies[claimUID]
}

// GetMemoryPoliciesForPod returns all memory policies associated with
// a pod, keyed by claim UID. Returns nil if no policies exist.
func (s *nodeState) GetMemoryPoliciesForPod(podUID string) map[string]*memorypolicy.MemoryPolicyConfig {
	s.Lock()
	defer s.Unlock()
	claimUIDs, ok := s.podClaims[podUID]
	if !ok || len(claimUIDs) == 0 {
		return nil
	}
	result := make(map[string]*memorypolicy.MemoryPolicyConfig)
	for claimUID := range claimUIDs {
		if policy := s.preparedPolicies[claimUID]; policy != nil {
			result[claimUID] = policy
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// getOrCreatePreparedPolicies loads the prepared policies from the
// file or creates an empty file.
func getOrCreatePreparedPolicies(filePath string) (PreparedPolicies, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		klog.V(5).Infof("creating empty prepared policies file %v", filePath)
		if err := os.WriteFile(filePath, []byte("{}"), 0600); err != nil {
			return nil, fmt.Errorf("failed creating %v: %v", filePath, err)
		}
		return make(PreparedPolicies), nil
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed reading %v: %v", filePath, err)
	}

	policies := make(PreparedPolicies)
	if err := json.Unmarshal(data, &policies); err != nil {
		return nil, fmt.Errorf("failed parsing %v: %v", filePath, err)
	}

	return policies, nil
}

// writePreparedPoliciesToFile serializes the prepared policies to a JSON file.
func writePreparedPoliciesToFile(filePath string, policies PreparedPolicies) error {
	if policies == nil {
		policies = PreparedPolicies{}
	}
	data, err := json.MarshalIndent(policies, "", "  ")
	if err != nil {
		return fmt.Errorf("prepared policies JSON encoding failed: %v", err)
	}
	return os.WriteFile(filePath, data, 0600)
}
