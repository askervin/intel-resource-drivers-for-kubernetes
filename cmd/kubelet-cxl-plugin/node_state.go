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
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	"k8s.io/klog/v2"
	"k8s.io/utils/cpuset"

	cdiapi "tags.cncf.io/container-device-interface/pkg/cdi"
	cdiparser "tags.cncf.io/container-device-interface/pkg/parser"
	cdiSpecs "tags.cncf.io/container-device-interface/specs-go"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/cdihelpers"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/memorypolicy"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/helpers"

	nricxl "github.com/containers/nri-plugins/pkg/cxl"
)

const (
	// cdiClaimEnvPrefix is the prefix for CDI-injected environment
	// variables that mark which claims are associated with a container.
	// The full env var name is CXL_CLAIM_<claimUID_with_underscores>=1.
	cdiClaimEnvPrefix = "CXL_CLAIM_"

	// cdiClaimSpecName is the name of the CDI spec file used for
	// claim marker devices (separate from the hardware device spec).
	cdiClaimSpecName = "cxl-claims"
)

// PreparedDeviceInfo captures information about one allocated device
// within a claim, collected during Prepare for use by NRI handlers
// and kept in a file for storing and restoring driver state.
type PreparedDeviceInfo struct {
	RequestName    string `json:"requestName"`
	DeviceName     string `json:"deviceName"`               // device UID in the pool
	DeviceType     string `json:"deviceType"`               // device.DeviceTypeCXLNode or device.DeviceTypeDRAM
	SysfsPath      string `json:"sysfsPath,omitempty"`      // sysfs path (CXL regions only)
	NUMANodes      []int  `json:"numaNodes"`                // NUMA node(s) for this device
	NodeAffinities []int  `json:"nodeAffinities,omitempty"` // CPU NUMA affinities of backing memory devices (CXL only)
	TotalBytes     uint64 `json:"totalBytes"`               // total device memory capacity
	ConsumedBytes  int64  `json:"consumedBytes"`            // allocated capacity from ConsumedCapacity
}

// PreparedClaimInfo captures all relevant information about a prepared
// claim: its devices, their types/capacities, and the memory policy.
// Persisted to preparedClaims.json for crash recovery.
type PreparedClaimInfo struct {
	ClaimName string                           `json:"claimName"` // namespace/name
	Policy    *memorypolicy.MemoryPolicyConfig `json:"policy,omitempty"`
	Devices   []PreparedDeviceInfo             `json:"devices"`
}

// PreparedClaimsInfo maps claim UIDs to their full claim information.
type PreparedClaimsInfo map[string]*PreparedClaimInfo

type nodeState struct {
	*helpers.NodeState
	config         *DriverConfig
	preparedClaims PreparedClaimsInfo
	claimsFilePath string
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

	claimsInfoFilePath := filepath.Join(filepath.Dir(preparedClaimsFilePath), "preparedClaimsInfo.json")
	claimsInfo, err := getOrCreatePreparedClaimsInfo(claimsInfoFilePath)
	if err != nil {
		klog.Errorf("failed to get prepared claims info: %v", err)
		return nil, fmt.Errorf("failed to get prepared claims info: %v", err)
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
		config:         driverConfig,
		preparedClaims: claimsInfo,
		claimsFilePath: claimsInfoFilePath,
		podClaims:      make(map[string]map[string]bool),
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
						StringValue: ptr(device.DeviceTypeCXLNode),
					},
					"name": {
						StringValue: &dev.Name,
					},
					"node": {
						IntValue: &node,
					},
				},
				Capacity: map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{
					device.CapacityMemory: {
						Value: *resource.NewQuantity(int64(dev.Size), resource.BinarySI),
					},
				},
				NodeAllocatableResourceMappings: map[v1.ResourceName]resourcev1.NodeAllocatableResourceMapping{
					v1.ResourceMemory: {
						CapacityKey: ptr(device.CapacityMemory),
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
						StringValue: ptr(device.DeviceTypeDRAM),
					},
					"name": {
						StringValue: &dev.Name,
					},
					"nodes": {
						StringValue: &nodeList,
					},
				},
				Capacity: map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{
					device.CapacityMemory: {
						Value: *resource.NewQuantity(int64(dev.Size), resource.BinarySI),
					},
				},
				NodeAllocatableResourceMappings: map[v1.ResourceName]resourcev1.NodeAllocatableResourceMapping{
					v1.ResourceMemory: {
						CapacityKey: ptr(device.CapacityMemory),
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
	delete(s.preparedClaims, claimUID)

	// Clean up pod→claim mapping.
	for podUID, claims := range s.podClaims {
		delete(claims, claimUID)
		if len(claims) == 0 {
			delete(s.podClaims, podUID)
		}
	}

	// Remove CDI claim marker device.
	if err := s.removeCDIClaimMarker(claimUID); err != nil {
		klog.Warningf("failed to remove CDI claim marker for %v: %v", claimUID, err)
	}

	if err := helpers.WritePreparedClaimsToFile(s.PreparedClaimsFilePath, s.Prepared); err != nil {
		return fmt.Errorf("failed to write prepared claims to file: %v", err)
	}

	if err := writePreparedClaimsInfoToFile(s.claimsFilePath, s.preparedClaims); err != nil {
		return fmt.Errorf("failed to write prepared claims info to file: %v", err)
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

	allocatedDevices, claimInfo, err := s.prepareAllocatedDevices(ctx, claim)
	if err != nil {
		return err
	}

	claimUID := string(claim.UID)
	s.Prepared[claimUID] = allocatedDevices
	s.preparedClaims[claimUID] = claimInfo

	if claimInfo.Policy != nil {
		klog.V(3).Infof("Claim %v has memory policy: order=%s minStep=%s maxStep=%s",
			claim.UID, claimInfo.Policy.MemoryUseOrder, claimInfo.Policy.MinStep, claimInfo.Policy.MaxStep)
	}

	// Record pod→claim mapping from ReservedFor.
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

	if err = writePreparedClaimsInfoToFile(s.claimsFilePath, s.preparedClaims); err != nil {
		klog.Errorf("failed to write prepared claims info to file: %v", err)
		return fmt.Errorf("failed to write prepared claims info to file: %v", err)
	}

	klog.V(5).Infof("Created prepared claim %v allocation", claim.UID)
	return nil
}

func (s *nodeState) prepareAllocatedDevices(ctx context.Context, claim *resourcev1.ResourceClaim) (allocatedDevices kubeletplugin.PrepareResult, claimInfo *PreparedClaimInfo, err error) {
	allocatedDevices = kubeletplugin.PrepareResult{}
	claimInfo = &PreparedClaimInfo{
		ClaimName: claim.Namespace + "/" + claim.Name,
	}

	allocatableDevices, ok := s.Allocatable.(device.DevicesInfo)
	if !ok {
		return allocatedDevices, nil, fmt.Errorf("internal error: unexpected type for state.Allocatable %T", s.Allocatable)
	}

	for _, allocatedDevice := range claim.Status.Allocation.Devices.Results {
		// ATM the only pool is cluster node's pool: all devices on current node.
		if allocatedDevice.Driver != device.DriverName || allocatedDevice.Pool != s.NodeName {
			klog.Infof("ignoring claim allocation device %+v", allocatedDevice)
			continue
		}

		allocatableDevice, found := allocatableDevices[allocatedDevice.Device]
		if !found {
			return allocatedDevices, nil, fmt.Errorf("could not find allocatable device %v (pool %v)", allocatedDevice.Device, allocatedDevice.Pool)
		}

		newDevice := kubeletplugin.Device{
			Requests:   []string{allocatedDevice.Request},
			PoolName:   allocatedDevice.Pool,
			DeviceName: allocatedDevice.Device,
			CDIDeviceIDs: []string{
				cdiparser.QualifiedName(device.CDIVendor, device.CDIClass, claimCDIDeviceName(string(claim.UID))),
			},
		}
		allocatedDevices.Devices = append(allocatedDevices.Devices, newDevice)

		// Collect device info for NRI logging and future cgmpolmgr use.
		devInfo := PreparedDeviceInfo{
			RequestName: allocatedDevice.Request,
			DeviceName:  allocatedDevice.Device,
		}
		switch dev := allocatableDevice.Dev.(type) {
		case *nricxl.RegionDevice:
			devInfo.DeviceType = device.DeviceTypeCXLNode
			devInfo.SysfsPath = dev.SysfsPath
			devInfo.NUMANodes = []int{dev.Node}
			devInfo.TotalBytes = dev.Size
			for _, mem := range dev.Memories {
				if mem.NodeAffinity >= 0 {
					devInfo.NodeAffinities = append(devInfo.NodeAffinities, mem.NodeAffinity)
				}
			}
		case *device.SystemDRAM:
			devInfo.DeviceType = device.DeviceTypeDRAM
			devInfo.NUMANodes = dev.Nodes
			devInfo.TotalBytes = dev.Size
		default:
			klog.Warningf("unknown device type %T for %v", allocatableDevice.Dev, allocatedDevice.Device)
		}
		// ConsumedCapacity is populated by the scheduler when
		// DRAConsumableCapacity feature gate is enabled.
		if consumed, ok := allocatedDevice.ConsumedCapacity[device.CapacityMemory]; ok {
			devInfo.ConsumedBytes = consumed.Value()
		}
		claimInfo.Devices = append(claimInfo.Devices, devInfo)

		klog.V(5).Infof("Prepared device %v for claim %v: type=%s numa=%v total=%d consumed=%d",
			allocatedDevice.Device, claim.UID, devInfo.DeviceType,
			devInfo.NUMANodes, devInfo.TotalBytes, devInfo.ConsumedBytes)
	}

	// Create a CDI marker device for this claim so that the container
	// runtime injects a CXL_CLAIM_<uid>=1 env var into containers
	// that reference this claim. The NRI CreateContainer handler uses
	// these env vars to identify which claims belong to a container.
	if err := s.addCDIClaimMarker(string(claim.UID)); err != nil {
		return allocatedDevices, nil, fmt.Errorf("failed to create CDI claim marker for %v: %w", claim.UID, err)
	}

	// Parse opaque device configuration for memory policy.
	claimInfo.Policy, err = parseMemoryPolicyFromClaim(claim)
	if err != nil {
		return allocatedDevices, nil, fmt.Errorf("failed to parse memory policy config from claim %v/%v: %w", claim.Namespace, claim.Name, err)
	}

	return allocatedDevices, claimInfo, nil
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

// GetClaimInfo returns the full prepared claim information for a given
// claim UID, or nil if the claim is not prepared.
func (s *nodeState) GetClaimInfo(claimUID string) *PreparedClaimInfo {
	s.Lock()
	defer s.Unlock()
	return s.preparedClaims[claimUID]
}

// GetMemoryPolicy returns the memory policy config for a given claim
// UID, or nil if no policy was configured.
func (s *nodeState) GetMemoryPolicy(claimUID string) *memorypolicy.MemoryPolicyConfig {
	s.Lock()
	defer s.Unlock()
	info := s.preparedClaims[claimUID]
	if info == nil {
		return nil
	}
	return info.Policy
}

// getOrCreatePreparedClaimsInfo loads the prepared claims info from
// the file or creates an empty file.
func getOrCreatePreparedClaimsInfo(filePath string) (PreparedClaimsInfo, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		klog.V(5).Infof("creating empty prepared claims info file %v", filePath)
		if err := os.WriteFile(filePath, []byte("{}"), 0600); err != nil {
			return nil, fmt.Errorf("failed creating %v: %v", filePath, err)
		}
		return make(PreparedClaimsInfo), nil
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed reading %v: %v", filePath, err)
	}

	claims := make(PreparedClaimsInfo)
	if err := json.Unmarshal(data, &claims); err != nil {
		return nil, fmt.Errorf("failed parsing %v: %v", filePath, err)
	}

	return claims, nil
}

// writePreparedClaimsInfoToFile serializes the prepared claims info to a JSON file.
func writePreparedClaimsInfoToFile(filePath string, claims PreparedClaimsInfo) error {
	if claims == nil {
		claims = PreparedClaimsInfo{}
	}
	data, err := json.MarshalIndent(claims, "", "  ")
	if err != nil {
		return fmt.Errorf("prepared claims info JSON encoding failed: %v", err)
	}
	return os.WriteFile(filePath, data, 0600)
}

// claimCDIDeviceName returns the CDI device name for a claim marker.
// The name is "claim-<claimUID>" which is valid per CDI naming rules.
func claimCDIDeviceName(claimUID string) string {
	return "claim-" + claimUID
}

// claimEnvVarName returns the environment variable name for a claim
// marker. Hyphens in the UID are replaced with underscores since env
// var names don't allow hyphens.
func claimEnvVarName(claimUID string) string {
	return cdiClaimEnvPrefix + strings.ReplaceAll(claimUID, "-", "_")
}

// ClaimUIDFromEnvVar extracts a claim UID from a CXL_CLAIM_* env var
// name, reversing the hyphen→underscore mapping. Returns the UID and
// true if the env var matches the prefix, or "" and false otherwise.
func ClaimUIDFromEnvVar(envVar string) (string, bool) {
	// envVar is "KEY=VALUE"
	parts := strings.SplitN(envVar, "=", 2)
	key := parts[0]
	if !strings.HasPrefix(key, cdiClaimEnvPrefix) {
		return "", false
	}
	uidWithUnderscores := key[len(cdiClaimEnvPrefix):]
	// Restore hyphens. UID format is 8-4-4-4-12 hex digits.
	uid := strings.ReplaceAll(uidWithUnderscores, "_", "-")
	return uid, true
}

// addCDIClaimMarker adds a CDI device entry for the given claim UID
// to the claims CDI spec file. The device injects an env var
// CXL_CLAIM_<uid>=1 into the container.
func (s *nodeState) addCDIClaimMarker(claimUID string) error {
	spec := s.buildClaimMarkerSpec()
	// Add the new claim device.
	devName := claimCDIDeviceName(claimUID)
	spec.Devices = append(spec.Devices, cdiSpecs.Device{
		Name: devName,
		ContainerEdits: cdiSpecs.ContainerEdits{
			Env: []string{claimEnvVarName(claimUID) + "=1"},
		},
	})
	return s.writeClaimMarkerSpec(spec)
}

// removeCDIClaimMarker rebuilds the CDI claim marker spec without the
// given claim UID. Since it rebuilds from s.Prepared (which should
// already have the claim removed), this effectively removes the
// claim's CDI device entry.
func (s *nodeState) removeCDIClaimMarker(claimUID string) error {
	spec := s.buildClaimMarkerSpec()
	return s.writeClaimMarkerSpec(spec)
}

// buildClaimMarkerSpec builds a CDI spec containing marker devices
// for all currently prepared claims. This is built from
// preparedPolicies (not from the existing CDI file) so it is safe
// even after a driver restart where the CDI file may be stale.
func (s *nodeState) buildClaimMarkerSpec() *cdiSpecs.Spec {
	spec := &cdiSpecs.Spec{
		Kind: device.CDIKind,
	}
	for claimUID := range s.Prepared {
		devName := claimCDIDeviceName(claimUID)
		spec.Devices = append(spec.Devices, cdiSpecs.Device{
			Name: devName,
			ContainerEdits: cdiSpecs.ContainerEdits{
				Env: []string{claimEnvVarName(claimUID) + "=1"},
			},
		})
	}
	return spec
}

// writeClaimMarkerSpec writes the claims CDI spec file.
func (s *nodeState) writeClaimMarkerSpec(spec *cdiSpecs.Spec) error {
	cdiVersion, err := cdiapi.MinimumRequiredVersion(spec)
	if err != nil {
		return fmt.Errorf("failed to get minimum required CDI spec version: %v", err)
	}
	spec.Version = cdiVersion

	if len(spec.Devices) == 0 {
		if err := s.CdiCache.RemoveSpec(cdiClaimSpecName); err != nil {
			klog.V(5).Infof("Could not remove CDI claim spec (may not exist): %v", err)
		} else {
			klog.V(5).Info("Removed empty CDI claim marker spec")
		}
		return nil
	}

	if err := s.CdiCache.WriteSpec(spec, cdiClaimSpecName); err != nil {
		return fmt.Errorf("failed to write CDI claim spec: %v", err)
	}
	klog.V(5).Infof("Wrote CDI claim marker spec with %d devices", len(spec.Devices))
	return nil
}
