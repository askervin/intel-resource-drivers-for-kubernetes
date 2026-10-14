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
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
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

	// preparedClaimsInfoFileName is the file of the prepared claims
	// info in the plugin data directory.
	preparedClaimsInfoFileName = "preparedClaimsInfo.json"

	// localEnvPrefix is the prefix of the env of the dax slices of local
	// memdevs (CXL_LOCAL_DAX_<SERIAL> and so on).
	localEnvPrefix = "CXL_LOCAL_"
	// localDevicePrefix is the prefix of the ResourceSlice device names
	// of local memdevs ("cxlmem-" + lower-case hex serial).
	localDevicePrefix = "cxlmem-"
)

// PreparedDeviceInfo captures information about one allocated device
// within a claim, collected during Prepare for use by NRI handlers
// and kept in a file for storing and restoring driver state.
type PreparedDeviceInfo struct {
	RequestName    string `json:"requestName"`
	DeviceName     string `json:"deviceName"`               // device UID in the pool
	DeviceType     string `json:"deviceType"`               // device.DeviceTypeCXLNode, device.DeviceTypeDRAM or device.DeviceTypeCXLDax
	SysfsPath      string `json:"sysfsPath,omitempty"`      // sysfs path (CXL regions only)
	NUMANodes      []int  `json:"numaNodes"`                // NUMA node(s) for this device
	NodeAffinities []int  `json:"nodeAffinities,omitempty"` // CPU NUMA affinities of backing memory devices (CXL only)
	TotalBytes     uint64 `json:"totalBytes"`               // total device memory capacity (local memdev: slice size)
	ConsumedBytes  int64  `json:"consumedBytes"`            // allocated capacity from ConsumedCapacity

	// Serial is the serial (0x<hex>) of a local memdev (cxl-memdev
	// device), whose memory is a NUMA node (DeviceType cxl-node) or a dax
	// slice (cxl-dax).
	Serial    string `json:"serial,omitempty"`
	DaxDevice string `json:"daxDevice,omitempty"` // dax slice: /dev/daxX.Y
	DaxMajor  int    `json:"daxMajor,omitempty"`
	DaxMinor  int    `json:"daxMinor,omitempty"`
	ReadOnly  bool   `json:"readOnly,omitempty"`  // dax slice: the device node is read-only
	CDIDevice string `json:"cdiDevice,omitempty"` // dax slice: its CDI device in the cxl-local spec
}

// PreparedClaimInfo captures all relevant information about a prepared
// claim: its devices, their types/capacities, and the memory policy.
// Persisted to preparedClaims.json for crash recovery.
type PreparedClaimInfo struct {
	ClaimName string                           `json:"claimName"` // namespace/name
	Policy    *memorypolicy.MemoryPolicyConfig `json:"policy,omitempty"`
	Devices   []PreparedDeviceInfo             `json:"devices"`
	// Lend is the lend of a companion claim (a claim with a LendConfig).
	Lend *lendRecord `json:"lend,omitempty"`
}

// PreparedClaimsInfo maps claim UIDs to their full claim information.
type PreparedClaimsInfo map[string]*PreparedClaimInfo

// nodeState is the state of the cxl.generic driver. The mutex of
// helpers.NodeState (Lock) guards the tables: Allocatable, Prepared,
// preparedClaims and podClaims. It is held only while they are read or
// changed, never across device work, lending or file writes: the NRI
// handlers read the claims through it while a container is created.
type nodeState struct {
	*helpers.NodeState
	config         *DriverConfig
	preparedClaims PreparedClaimsInfo
	claimsFilePath string
	// podClaims maps pod UID → set of claim UIDs prepared for that pod.
	// Built during Prepare from claim.Status.ReservedFor.
	podClaims map[string]map[string]bool
	// dynamic opens the lease socket of claims on the token device,
	// nil when lending is disabled.
	dynamic tokenClaims
	// local creates the regions and slices of local memdevs for claims,
	// nil when local memdevs are not managed.
	local *localAllocator
	// lending lends the local memdevs of companion claims to running
	// containers.
	lending *claimLender
	// republish rescans and publishes the devices, nil for none. It is
	// called after a Prepare or Unprepare of a claim on a local memdev.
	republish func()
	// locks serializes the Prepares and Unprepares of each claim, and the
	// writes of the CDI specs and the files of the prepared claims; nil
	// for no serialization.
	locks *nodeLocks
}

// nodeLocks serializes the work on each claim, and the writes of the
// prepared claims.
type nodeLocks struct {
	mu     sync.Mutex
	claims map[string]*claimLock
	write  sync.Mutex
}

// claimLock is the lock of one claim UID.
type claimLock struct {
	sync.Mutex
	users int
}

// lockClaim locks the claim UID and returns the function that unlocks it.
func (l *nodeLocks) lockClaim(uid string) func() {
	if l == nil {
		return func() {}
	}
	l.mu.Lock()
	if l.claims == nil {
		l.claims = map[string]*claimLock{}
	}
	c := l.claims[uid]
	if c == nil {
		c = &claimLock{}
		l.claims[uid] = c
	}
	c.users++
	l.mu.Unlock()
	c.Lock()
	return func() {
		c.Unlock()
		l.mu.Lock()
		if c.users--; c.users == 0 {
			delete(l.claims, uid)
		}
		l.mu.Unlock()
	}
}

// lockWrite locks the writes and returns the function that unlocks them.
func (l *nodeLocks) lockWrite() func() {
	if l == nil {
		return func() {}
	}
	l.write.Lock()
	return l.write.Unlock
}

func newNodeState(devInfo device.DevicesInfo, cdiRoot, preparedClaimsFilePath, nodeName string, driverConfig *DriverConfig, local *localAllocator) (*nodeState, error) {
	klog.V(5).Info("Refreshing CDI registry")
	if err := cdiapi.Configure(cdiapi.WithSpecDirs(cdiRoot)); err != nil {
		return nil, fmt.Errorf("unable to refresh the CDI registry: %v", err)
	}

	cdiCache := cdiapi.GetDefaultCache()

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

	claimsInfoFilePath := filepath.Join(filepath.Dir(preparedClaimsFilePath), preparedClaimsInfoFileName)
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
		local:          local,
		lending:        newClaimLender(device.DriverName),
		locks:          &nodeLocks{},
	}
	fmt.Printf("type ofstate.Allocatable: %T\n", state.Allocatable)

	// The CDI specs of the claims prepared before a restart.
	state.forgetGoneSlices()
	if err := state.persist(); err != nil {
		return nil, err
	}

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

// buildDevInfos returns the devices to publish. poolSerials are the serials
// of pool devices (prepared by the pool helper, or published by a pool
// controller): their regions are skipped (D29), and they are never local
// memdevs.
// Regions on memdevs that the local allocator uses (local) are skipped
// too: the allocator created them, and the memdevs are published as
// cxl-memdev devices with the mode of local.
func buildDevInfos(driverConfig *DriverConfig, detectedDevices *nricxl.Devices, poolSerials map[uint64]bool, local *localSnapshot) (device.DevicesInfo, error) {
	cxlNodes := make(map[int]bool)
	devInfos := make(map[string]*device.DeviceInfo)
	for _, regDev := range detectedDevices.RegionDevices {
		klog.V(3).Infof("discovered CXL region device: %+v", regDev)
		ignoreReason := poolIgnoreReason(regDev, poolSerials)
		if ignoreReason == "" {
			ignoreReason = localIgnoreReason(regDev, local)
		}
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
			if reason := memdevIgnoreReason(driverConfig, memDev); reason != "" {
				ignoreReason = "region has a " + reason
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
	if driverConfig.manageMemdevs() {
		addLocalMemdevs(devInfos, driverConfig, detectedDevices, poolSerials, local)
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
	if driverConfig.PublishDynamicToken {
		devInfos[device.DynamicDeviceName] = &device.DeviceInfo{
			Name: device.DynamicDeviceName,
			Dev:  &device.DynamicToken{},
		}
	}
	return devInfos, nil
}

// poolIgnoreReason returns why a region on a pool device is not published,
// "" if it is not on a pool device.
func poolIgnoreReason(regDev *nricxl.RegionDevice, poolSerials map[uint64]bool) string {
	for _, memDev := range regDev.Memories {
		if poolSerials[memDev.Serial] {
			return fmt.Sprintf("pool device (%s), memory device serial 0x%x", device.PoolDriverName, memDev.Serial)
		}
	}
	return ""
}

// localIgnoreReason returns why a region on a local memdev that the local
// allocator uses is not published, "" if it is not on one.
func localIgnoreReason(regDev *nricxl.RegionDevice, local *localSnapshot) string {
	for _, memDev := range regDev.Memories {
		if _, ok := local.mode(memDev.Serial); ok {
			return fmt.Sprintf("created by the plugin on local memory device %s%x", localDevicePrefix, memDev.Serial)
		}
	}
	return ""
}

// memdevIgnoreReason returns why IgnoreDevices of the configuration
// ignores a memdev, "" if it does not.
func memdevIgnoreReason(driverConfig *DriverConfig, memDev *nricxl.MemoryDevice) string {
	for _, spec := range driverConfig.IgnoreDevices {
		switch spec {
		case memDev.Name:
			return fmt.Sprintf("memory device name %q", spec)
		case memDev.DevName:
			return fmt.Sprintf("memory uevent device name %q", spec)
		case fmt.Sprintf("%d:%d", memDev.Major, memDev.Minor):
			return fmt.Sprintf("memory device with major:minor %q", spec)
		case fmt.Sprintf("%d", memDev.Serial), fmt.Sprintf("0x%x", memDev.Serial), fmt.Sprintf("%x", memDev.Serial):
			return fmt.Sprintf("memory device serial %q", spec)
		}
	}
	return ""
}

// addLocalMemdevs adds the local memdevs that the local allocator can use
// to devInfos: enabled memdevs whose RAM capacity is sure to hold a whole
// memory block, that are neither pool devices nor ignored, and have no
// regions but the allocator's.
func addLocalMemdevs(devInfos device.DevicesInfo, driverConfig *DriverConfig, detectedDevices *nricxl.Devices, poolSerials map[uint64]bool, local *localSnapshot) {
	bs := uint64(0)
	if local != nil {
		bs = local.BlockSize
	}
	regions := map[uint64][]string{}
	for _, regDev := range detectedDevices.RegionDevices {
		for _, memDev := range regDev.Memories {
			regions[memDev.Serial] = append(regions[memDev.Serial], regDev.Name)
		}
	}
	for _, memDev := range detectedDevices.MemoryDevices {
		mode, used := local.mode(memDev.Serial)
		reason := ""
		switch {
		case memDev.RamSize == 0:
			reason = "no RAM capacity"
		case bs == 0:
			reason = "the memory block size is unknown"
		case localUsableSize(memDev.RamSize, bs) == 0:
			reason = fmt.Sprintf("RAM capacity %d is not sure to hold a whole memory block of %d bytes", memDev.RamSize, bs)
		case !memDev.Enabled:
			reason = "memory device is disabled"
		case poolSerials[memDev.Serial]:
			reason = "pool device"
		case len(regions[memDev.Serial]) > 0 && !used:
			reason = fmt.Sprintf("it has regions %v that the plugin did not create", regions[memDev.Serial])
		default:
			reason = memdevIgnoreReason(driverConfig, memDev)
		}
		if reason != "" {
			klog.V(4).Infof("- not publishing memory device %q as a local memory device: %s", memDev.Name, reason)
			continue
		}
		name := fmt.Sprintf("%s%x", localDevicePrefix, memDev.Serial)
		devInfos[name] = &device.DeviceInfo{
			Name: name,
			Dev: &device.CXLMemdev{
				Name:         memDev.Name,
				Serial:       memDev.Serial,
				Size:         memDev.RamSize,
				NodeAffinity: memDev.NodeAffinity,
				BlockSize:    bs,
				Mode:         modeAttribute(mode),
			},
			SysfsPath: memDev.SysfsPath,
		}
		klog.V(3).Infof("discovered local memory device %s: %s serial 0x%x size %d mode %s memory block size %d", name, memDev.Name, memDev.Serial, memDev.RamSize, modeAttribute(mode), bs)
	}
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
				NodeAllocatableResources: memoryNodeAllocatableResources(),
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
				NodeAllocatableResources: memoryNodeAllocatableResources(),
				AllowMultipleAllocations: ptr(true),
			}
			devices = append(devices, newDevice)
		case *device.CXLMemdev:
			devices = append(devices, localMemdevDevice(cxlUID, dev))
		case *device.DynamicToken:
			// The token: no capacity, no node allocatable resources,
			// any number of claims can allocate it.
			devices = append(devices, resourcev1.Device{
				Name: cxlUID,
				Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
					"type": {StringValue: ptr(device.DeviceTypeDynamic)},
					"name": {StringValue: ptr(device.DynamicDeviceName)},
				},
				AllowMultipleAllocations: ptr(true),
			})
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

// localMemdevDevice returns the ResourceSlice device of a local memdev.
func localMemdevDevice(name string, dev *device.CXLMemdev) resourcev1.Device {
	attrs := map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
		"type":   {StringValue: ptr(device.DeviceTypeCXLMemdev)},
		"name":   {StringValue: ptr(dev.Name)},
		"serial": {StringValue: ptr(fmt.Sprintf("0x%x", dev.Serial))},
		"source": {StringValue: ptr(device.SourceLocal)},
		"size":   {IntValue: ptr(int64(dev.Size))},
		"mode":   {StringValue: ptr(dev.Mode)},
		// every slice is a multiple of the memory block size
		"memoryBlockSize": {IntValue: ptr(int64(dev.BlockSize))},
	}
	if dev.NodeAffinity >= 0 {
		attrs["nodeAffinity"] = resourcev1.DeviceAttribute{IntValue: ptr(int64(dev.NodeAffinity))}
	}
	return resourcev1.Device{
		Name:       name,
		Attributes: attrs,
		Capacity: map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{
			device.CapacityMemory: {
				// the whole memory blocks that the region is sure to hold
				Value:         *resource.NewQuantity(int64(localUsableSize(dev.Size, dev.BlockSize)), resource.BinarySI),
				RequestPolicy: localRequestPolicy(dev.Size, dev.BlockSize),
			},
		},
		NodeAllocatableResources: memoryNodeAllocatableResources(),
		AllowMultipleAllocations: ptr(true),
	}
}

// localRequestPolicy returns the memory capacity request policy of a local
// memdev of size bytes with memory blocks of bs bytes: a request is rounded
// up to a multiple of bs, and a request without an amount gets the whole
// blocks that the region is sure to hold (localUsableSize). A memdev of one
// such block has it as the only valid amount. Nil for a memdev without one.
func localRequestPolicy(size, bs uint64) *resourcev1.CapacityRequestPolicy {
	usable := localUsableSize(size, bs)
	if usable == 0 {
		return nil
	}
	whole := resource.NewQuantity(int64(usable), resource.BinarySI)
	if usable < 2*bs {
		// a range needs room for its min and one step
		return &resourcev1.CapacityRequestPolicy{Default: whole, ValidValues: []resource.Quantity{*whole}}
	}
	return &resourcev1.CapacityRequestPolicy{
		Default: whole,
		ValidRange: &resourcev1.CapacityRequestPolicyRange{
			Min:  resource.NewQuantity(int64(bs), resource.BinarySI),
			Step: resource.NewQuantity(int64(bs), resource.BinarySI),
		},
	}
}

// memoryNodeAllocatableResources maps the memory capacity that a
// claim consumes from a device to the node allocatable "memory"
// resource, so that kubelet accounts claimed memory in the pod and
// container cgroups. Memory capacity (device and node) is in bytes,
// hence capacityMultiplier is 1.
//
// Requires DRANodeAllocatableResources feature gate. Without it the
// apiserver drops the field from published ResourceSlices and the
// driver logs "some fields were dropped by the apiserver".
func memoryNodeAllocatableResources() map[v1.ResourceName]resourcev1.NodeAllocatableResource {
	return map[v1.ResourceName]resourcev1.NodeAllocatableResource{
		v1.ResourceMemory: {
			Mapping: &resourcev1.NodeAllocatableMapping{
				CapacityKey:        ptr(device.CapacityMemory),
				CapacityMultiplier: ptr(resource.MustParse("1")),
			},
		},
	}
}

// Unprepare overrides helpers.NodeState.Unprepare to also clean up
// the memory policy associated with a claim, revoke the lend of a
// companion claim and release the memory of local memdevs.
func (s *nodeState) Unprepare(ctx context.Context, claimUID string) error {
	unlock := s.locks.lockClaim(claimUID)
	local, err := s.unprepare(ctx, claimUID)
	unlock()
	if local && s.republish != nil {
		s.republish()
	}
	return err
}

// unprepare unprepares a claim and tells whether the claim used local
// memdevs.
func (s *nodeState) unprepare(ctx context.Context, claimUID string) (bool, error) {
	info, found := s.preparedInfo(claimUID)
	if !found {
		return false, nil
	}
	local := info.usesLocalMemdevs()
	if info != nil && info.Lend != nil {
		s.lending.releasing(info.Lend.Lease)
		// Revoke before the release: offlining memory that the
		// container still has in cpuset.mems leaves a stale node there.
		if err := s.lending.revoke(ctx, info.Lend, info.ClaimName); err != nil {
			return local, fmt.Errorf("claim %s: %w", info.ClaimName, err)
		}
	}
	if local {
		if err := s.local.Release(ctx, claimUID); err != nil {
			return local, fmt.Errorf("claim %s: %w", info.ClaimName, err)
		}
	}

	klog.V(5).Infof("Freeing devices from claim %v", claimUID)
	s.forget(claimUID)
	return local, s.persist()
}

// preparedInfo returns the info of a prepared claim, and whether the claim
// is prepared.
func (s *nodeState) preparedInfo(claimUID string) (*PreparedClaimInfo, bool) {
	s.Lock()
	defer s.Unlock()
	_, found := s.Prepared[claimUID]
	return s.preparedClaims[claimUID], found
}

// allocatable returns the devices of the last scan.
func (s *nodeState) allocatable() (device.DevicesInfo, error) {
	s.Lock()
	defer s.Unlock()
	devs, ok := s.Allocatable.(device.DevicesInfo)
	if !ok {
		return nil, fmt.Errorf("internal error: unexpected type for state.Allocatable %T", s.Allocatable)
	}
	return devs, nil
}

// store adds a prepared claim to the tables.
func (s *nodeState) store(claim *resourcev1.ResourceClaim, result kubeletplugin.PrepareResult, info *PreparedClaimInfo) {
	s.Lock()
	defer s.Unlock()
	claimUID := string(claim.UID)
	s.Prepared[claimUID] = result
	s.preparedClaims[claimUID] = info
	s.recordPods(claim)
}

// forget removes a claim from the tables.
func (s *nodeState) forget(claimUID string) {
	s.Lock()
	defer s.Unlock()
	delete(s.Prepared, claimUID)
	delete(s.preparedClaims, claimUID)

	// Clean up pod to claim mapping.
	for podUID, claims := range s.podClaims {
		delete(claims, claimUID)
		if len(claims) == 0 {
			delete(s.podClaims, podUID)
		}
	}
}

// Prepare prepares the devices of a claim. The local memdevs of a
// companion claim (a claim with a LendConfig) are lent to the running
// container that the LendConfig names.
func (s *nodeState) Prepare(ctx context.Context, claim *resourcev1.ResourceClaim) error {
	unlock := s.locks.lockClaim(string(claim.UID))
	local, err := s.prepare(ctx, claim)
	unlock()
	if local && s.republish != nil {
		s.republish()
	}
	return err
}

// prepare prepares a claim and tells whether the claim uses local memdevs.
// A claim that is prepared already is not touched again: only the lease
// broker hears about a prepared companion claim again.
func (s *nodeState) prepare(ctx context.Context, claim *resourcev1.ResourceClaim) (bool, error) {
	claimUID := string(claim.UID)
	if info, found := s.preparedInfo(claimUID); found {
		klog.V(3).Infof("Claim %s was already prepared, nothing to do", claimUID)
		if info != nil && info.Lend != nil {
			s.lending.reprepared(info.Lend, s.lentDevices(info))
		}
		return false, nil
	}

	if claim.Status.Allocation == nil {
		return false, fmt.Errorf("no allocation found in claim %v/%v status", claim.Namespace, claim.Name)
	}
	allocatable, err := s.allocatable()
	if err != nil {
		return false, err
	}

	local, all := s.localMemdevResults(claim, allocatable)
	lend, err := s.checkLend(claim, local, all)
	if err != nil {
		return false, err
	}

	allocatedDevices, claimInfo, err := s.prepareAllocatedDevices(ctx, claim, allocatable)
	if err == nil && lend != nil {
		err = s.lendClaim(ctx, lend, claimInfo)
		// The memory of a companion claim belongs to the borrower: its
		// own containers are not steered.
		dropCDIDevice(&allocatedDevices, cdiparser.QualifiedName(device.CDIVendor, device.CDIClass, claimCDIDeviceName(claimUID)))
	}
	if err != nil {
		s.releaseLocal(ctx, claim)
		return local > 0, err
	}

	if claimInfo.Policy != nil {
		klog.V(3).Infof("Claim %v has memory policy: order=%s minStep=%s maxStep=%s",
			claim.UID, claimInfo.Policy.MemoryUseOrder, claimInfo.Policy.MinStep, claimInfo.Policy.MaxStep)
	}
	s.store(claim, allocatedDevices, claimInfo)

	if err := s.persist(); err != nil {
		klog.Errorf("%v", err)
		s.rollback(ctx, claim, claimInfo, lend, err)
		return local > 0, err
	}

	if lend != nil {
		s.lending.prepared(lend, s.lentDevices(claimInfo))
	}
	klog.V(5).Infof("Created prepared claim %v allocation", claim.UID)
	return local > 0, nil
}

// releaseLocal releases the memory of local memdevs that a failed Prepare
// of a claim acquired.
func (s *nodeState) releaseLocal(ctx context.Context, claim *resourcev1.ResourceClaim) {
	ctx, cancel := rollbackContext(ctx)
	defer cancel()
	if err := s.local.Release(ctx, string(claim.UID)); err != nil {
		klog.Errorf("claim %v/%v: rollback: %v", claim.Namespace, claim.Name, err)
	}
}

// rollback undoes the Prepare of a claim that failed with cause after the
// claim was stored: it revokes the lend and tells the lease broker why,
// releases the memory of local memdevs and forgets the claim.
func (s *nodeState) rollback(ctx context.Context, claim *resourcev1.ResourceClaim, info *PreparedClaimInfo, lend *lendConfig, cause error) {
	rctx, cancel := rollbackContext(ctx)
	defer cancel()
	if info.Lend != nil {
		if err := s.lending.revoke(rctx, info.Lend, info.ClaimName); err != nil {
			klog.Errorf("claim %s: rollback: %v", info.ClaimName, err)
		}
	}
	if lend != nil {
		s.lending.failed(lend, cause)
	}
	s.releaseLocal(rctx, claim)
	s.forget(string(claim.UID))
	if err := s.persist(); err != nil {
		klog.Errorf("claim %s: rollback: %v", info.ClaimName, err)
	}
}

// recordPods records which pods a claim is reserved for (ReservedFor).
// Called with the tables locked.
func (s *nodeState) recordPods(claim *resourcev1.ResourceClaim) {
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
}

// persist writes the CDI specs and the files of the prepared claims.
func (s *nodeState) persist() error {
	unlock := s.locks.lockWrite()
	defer unlock()
	s.Lock()
	markers, slices := s.buildClaimMarkerSpec(), s.buildLocalCDISpec()
	prepared, infos := maps.Clone(s.Prepared), maps.Clone(s.preparedClaims)
	s.Unlock()
	if err := errors.Join(s.writeCDISpec(markers, cdiClaimSpecName), s.writeCDISpec(slices, device.LocalCDISpecName)); err != nil {
		return err
	}
	if err := helpers.WritePreparedClaimsToFile(s.PreparedClaimsFilePath, prepared); err != nil {
		return fmt.Errorf("failed to write prepared claims to file: %v", err)
	}
	if err := writePreparedClaimsInfoToFile(s.claimsFilePath, infos); err != nil {
		return fmt.Errorf("failed to write prepared claims info to file: %v", err)
	}
	return nil
}

// forgetGoneSlices drops the CDI devices of the dax slices of prepared
// claims that the local allocator does not hold (gone with a reboot), so
// that a stale device number never reaches a container.
func (s *nodeState) forgetGoneSlices() {
	for uid, info := range s.preparedClaims {
		for i := range info.Devices {
			d := &info.Devices[i]
			if d.DeviceType == device.DeviceTypeCXLDax && d.CDIDevice != "" && !s.local.holds(uid, filepath.Base(d.DaxDevice)) {
				klog.Warningf("claim %s: no CDI device for the dax slice %s: the slice is gone", uid, d.DaxDevice)
				d.CDIDevice = ""
			}
		}
	}
}

// localMemdevResults returns the number of allocation results of a claim
// on local memdevs of the node, and the number of all its results on
// devices of the node.
func (s *nodeState) localMemdevResults(claim *resourcev1.ResourceClaim, allocatable device.DevicesInfo) (local, all int) {
	for _, r := range claim.Status.Allocation.Devices.Results {
		if r.Driver != device.DriverName || r.Pool != s.NodeName {
			continue
		}
		all++
		if d := allocatable[r.Device]; d != nil {
			if _, ok := d.Dev.(*device.CXLMemdev); ok {
				local++
			}
		}
	}
	return local, all
}

// checkLend returns the LendConfig of a companion claim, nil if the claim
// has none, and checks that the claim can be lent.
func (s *nodeState) checkLend(claim *resourcev1.ResourceClaim, local, all int) (*lendConfig, error) {
	lending := s.lending
	if lending == nil {
		// lending disabled
		lending = newClaimLender(device.DriverName)
	}
	cfg, err := lending.parse(claim)
	if err != nil {
		return nil, fmt.Errorf("claim %v/%v: %w", claim.Namespace, claim.Name, err)
	}
	if cfg == nil {
		return nil, nil
	}
	if local == 0 || local != all {
		return nil, fmt.Errorf("claim %v/%v: %s needs a %s device", claim.Namespace, claim.Name, device.LendConfigKind, device.DeviceTypeCXLMemdev)
	}
	if err := lending.check(cfg); err != nil {
		return nil, fmt.Errorf("claim %v/%v: %w", claim.Namespace, claim.Name, err)
	}
	return cfg, nil
}

// lendClaim lends the local memdevs of a prepared companion claim and
// records the lend in the claim info.
func (s *nodeState) lendClaim(ctx context.Context, cfg *lendConfig, claimInfo *PreparedClaimInfo) error {
	rec, err := s.lending.lend(ctx, cfg, s.lentDevices(claimInfo))
	if err != nil {
		if rerr := s.lending.revoke(ctx, rec, claimInfo.ClaimName); rerr != nil {
			klog.Errorf("claim %s: %v", claimInfo.ClaimName, rerr)
		}
		s.lending.failed(cfg, err)
		return fmt.Errorf("claim %s: %w", claimInfo.ClaimName, err)
	}
	claimInfo.Lend = rec
	return nil
}

// lentDevices returns the local memdevs of a prepared claim as the lease
// broker needs them.
func (s *nodeState) lentDevices(info *PreparedClaimInfo) []lentDevice {
	var devs []lentDevice
	for _, d := range info.Devices {
		if d.Serial == "" {
			continue
		}
		hex := strings.TrimPrefix(d.Serial, "0x")
		serial, _ := strconv.ParseUint(hex, 16, 64)
		ld := lentDevice{
			Driver:    device.DriverName,
			Source:    device.SourceLocal,
			Request:   d.RequestName,
			Device:    d.DeviceName,
			Pool:      s.NodeName,
			Serial:    serial,
			SerialHex: hex,
			Size:      uint64(d.ConsumedBytes),
			Mode:      device.ModeNUMA,
			ReadOnly:  d.ReadOnly,
			Node:      -1,
		}
		if d.DeviceType == device.DeviceTypeCXLDax {
			ld.Mode, ld.Size, ld.DaxDevice = device.ModeDax, d.TotalBytes, d.DaxDevice
		} else if len(d.NUMANodes) > 0 {
			ld.Node = d.NUMANodes[0]
		}
		devs = append(devs, ld)
	}
	return devs
}

// usesLocalMemdevs tells whether a prepared claim has memory of local
// memdevs.
func (info *PreparedClaimInfo) usesLocalMemdevs() bool {
	if info == nil {
		return false
	}
	for _, d := range info.Devices {
		if d.Serial != "" {
			return true
		}
	}
	return false
}

// dropCDIDevice removes a CDI device from the devices of a prepare result.
func dropCDIDevice(result *kubeletplugin.PrepareResult, id string) {
	for i := range result.Devices {
		result.Devices[i].CDIDeviceIDs = slices.DeleteFunc(result.Devices[i].CDIDeviceIDs, func(s string) bool { return s == id })
	}
}

// prepareAllocatedDevices prepares the devices of a claim from
// allocatableDevices.
func (s *nodeState) prepareAllocatedDevices(ctx context.Context, claim *resourcev1.ResourceClaim, allocatableDevices device.DevicesInfo) (allocatedDevices kubeletplugin.PrepareResult, claimInfo *PreparedClaimInfo, err error) {
	allocatedDevices = kubeletplugin.PrepareResult{}
	claimInfo = &PreparedClaimInfo{
		ClaimName: claim.Namespace + "/" + claim.Name,
	}

	hasToken := false
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

		// Collect device info for NRI logging and future cgmpolmgr use.
		devInfo, err := s.preparedDeviceInfo(ctx, claim, &allocatedDevice, allocatableDevice, claimInfo)
		if err != nil {
			return allocatedDevices, nil, err
		}
		switch devInfo.DeviceType {
		case device.DeviceTypeDynamic:
			hasToken = true
			newDevice.CDIDeviceIDs = append(newDevice.CDIDeviceIDs, leaseCDIDeviceID(string(claim.UID)))
		case device.DeviceTypeCXLDax:
			newDevice.CDIDeviceIDs = append(newDevice.CDIDeviceIDs, cdiparser.QualifiedName(device.CDIVendor, device.CDIClass, devInfo.CDIDevice))
		}
		allocatedDevices.Devices = append(allocatedDevices.Devices, newDevice)
		claimInfo.Devices = append(claimInfo.Devices, devInfo)

		klog.V(5).Infof("Prepared device %v for claim %v: type=%s numa=%v total=%d consumed=%d",
			allocatedDevice.Device, claim.UID, devInfo.DeviceType,
			devInfo.NUMANodes, devInfo.TotalBytes, devInfo.ConsumedBytes)
	}

	// The CDI marker device of the claim (written with the prepared
	// claims) makes the container runtime inject a CXL_CLAIM_<uid>=1 env
	// var into containers that reference this claim. The NRI
	// CreateContainer handler uses these env vars to identify which
	// claims belong to a container.

	// A claim on the token device gets its lease socket.
	if hasToken {
		if s.dynamic == nil {
			return allocatedDevices, nil, fmt.Errorf("claim %v/%v: lending to running containers is disabled (--lend=false)", claim.Namespace, claim.Name)
		}
		if err := s.dynamic.OpenClaim(claim); err != nil {
			return allocatedDevices, nil, fmt.Errorf("claim %v/%v: lease socket: %w", claim.Namespace, claim.Name, err)
		}
	}

	// Parse opaque device configuration for memory policy.
	claimInfo.Policy, err = parseMemoryPolicyFromClaim(claim)
	if err != nil {
		return allocatedDevices, nil, fmt.Errorf("failed to parse memory policy config from claim %v/%v: %w", claim.Namespace, claim.Name, err)
	}

	return allocatedDevices, claimInfo, nil
}

// preparedDeviceInfo prepares the device of an allocation result and
// returns its info. claimInfo has the devices of the claim prepared before
// it.
func (s *nodeState) preparedDeviceInfo(ctx context.Context, claim *resourcev1.ResourceClaim, r *resourcev1.DeviceRequestAllocationResult, allocatable *device.DeviceInfo, claimInfo *PreparedClaimInfo) (PreparedDeviceInfo, error) {
	devInfo := PreparedDeviceInfo{
		RequestName: r.Request,
		DeviceName:  r.Device,
	}
	switch dev := allocatable.Dev.(type) {
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
	case *device.DynamicToken:
		devInfo.DeviceType = device.DeviceTypeDynamic
	case *device.CXLMemdev:
		// the slice is what the claim consumes
		err := s.prepareLocalMemdev(ctx, claim, r, dev, &devInfo, claimInfo)
		return devInfo, err
	default:
		klog.Warningf("unknown device type %T for %v", allocatable.Dev, r.Device)
	}
	// ConsumedCapacity is populated by the scheduler when
	// DRAConsumableCapacity feature gate is enabled.
	if consumed, ok := r.ConsumedCapacity[device.CapacityMemory]; ok {
		devInfo.ConsumedBytes = consumed.Value()
	}
	return devInfo, nil
}

// prepareLocalMemdev acquires a slice of a local memdev for an allocation
// result, as memory of the NUMA node of the memdev or as a dax device by the
// LocalDeviceConfig of the request, and describes it in devInfo: the slice
// size is its total and consumed bytes. claimInfo has the devices of the
// claim prepared before it.
func (s *nodeState) prepareLocalMemdev(ctx context.Context, claim *resourcev1.ResourceClaim, r *resourcev1.DeviceRequestAllocationResult, dev *device.CXLMemdev, devInfo *PreparedDeviceInfo, claimInfo *PreparedClaimInfo) error {
	cfg, err := parseDeviceConfig(claim, device.DriverName, device.LocalDeviceConfigKind, r.Request)
	mode := ""
	if err == nil {
		mode, err = cfg.modeFor(false)
	}
	if err != nil {
		return fmt.Errorf("claim %v/%v: request %s: %w", claim.Namespace, claim.Name, r.Request, err)
	}
	size := dev.Size
	if dev.BlockSize > 0 {
		size = localUsableSize(dev.Size, dev.BlockSize)
	}
	if consumed, ok := r.ConsumedCapacity[device.CapacityMemory]; ok && consumed.Value() > 0 {
		size = uint64(consumed.Value())
	}
	slice, node, err := s.local.Acquire(ctx, string(claim.UID), r.Request, dev.Serial, mode, size, cfg.ReadOnly)
	if err != nil {
		return fmt.Errorf("claim %v/%v: device %s: %w", claim.Namespace, claim.Name, r.Device, err)
	}
	devInfo.Serial = fmt.Sprintf("0x%x", dev.Serial)
	devInfo.SysfsPath = s.local.regionPath(slice.Region)
	devInfo.TotalBytes = slice.Size
	devInfo.ConsumedBytes = int64(slice.Size)
	if dev.NodeAffinity >= 0 {
		devInfo.NodeAffinities = []int{dev.NodeAffinity}
	}
	if mode == device.ModeNUMA {
		devInfo.DeviceType = device.DeviceTypeCXLNode
		devInfo.NUMANodes = []int{node}
		return nil
	}
	devInfo.DeviceType = device.DeviceTypeCXLDax
	devInfo.DaxDevice, devInfo.DaxMajor, devInfo.DaxMinor, devInfo.ReadOnly = slice.DaxDevice, slice.Major, slice.Minor, slice.ReadOnly
	devInfo.CDIDevice = localCDIDeviceName(string(claim.UID), claimInfo)
	return nil
}

// localCDIDeviceName returns the name of the CDI device of the next dax
// slice of a claim: "local-<claim uid>" for the first one, then
// "local-<claim uid>-<n>".
func localCDIDeviceName(claimUID string, claimInfo *PreparedClaimInfo) string {
	n := 0
	for _, d := range claimInfo.Devices {
		if d.DeviceType == device.DeviceTypeCXLDax {
			n++
		}
	}
	if n == 0 {
		return "local-" + claimUID
	}
	return fmt.Sprintf("local-%s-%d", claimUID, n)
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
	return writeJSONFile(filePath, claims)
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

// buildClaimMarkerSpec builds a CDI spec containing marker devices
// for all currently prepared claims. This is built from
// preparedPolicies (not from the existing CDI file) so it is safe
// even after a driver restart where the CDI file may be stale.
// Called with the tables locked.
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

// buildLocalCDISpec returns the CDI spec of the dax slices of local
// memdevs of the prepared claims. Called with the tables locked.
func (s *nodeState) buildLocalCDISpec() *cdiSpecs.Spec {
	spec := &cdiSpecs.Spec{Kind: device.CDIKind}
	for _, uid := range slices.Sorted(maps.Keys(s.preparedClaims)) {
		for _, d := range s.preparedClaims[uid].Devices {
			if d.DeviceType != device.DeviceTypeCXLDax || d.CDIDevice == "" {
				continue
			}
			spec.Devices = append(spec.Devices, daxCDIDevice(d.CDIDevice, localEnvPrefix, strings.TrimPrefix(d.Serial, "0x"),
				d.DaxDevice, d.DaxMajor, d.DaxMinor, d.TotalBytes, d.ReadOnly))
		}
	}
	return spec
}

// writeCDISpec writes a CDI spec file, or removes it when the spec has no
// devices.
func (s *nodeState) writeCDISpec(spec *cdiSpecs.Spec, name string) error {
	cdiVersion, err := cdiapi.MinimumRequiredVersion(spec)
	if err != nil {
		return fmt.Errorf("failed to get minimum required CDI spec version: %v", err)
	}
	spec.Version = cdiVersion

	if len(spec.Devices) == 0 {
		if err := s.CdiCache.RemoveSpec(name); err != nil {
			klog.V(5).Infof("Could not remove CDI spec %s (may not exist): %v", name, err)
		} else {
			klog.V(5).Infof("Removed empty CDI spec %s", name)
		}
		return nil
	}

	if err := s.CdiCache.WriteSpec(spec, name); err != nil {
		return fmt.Errorf("failed to write CDI spec %s: %v", name, err)
	}
	klog.V(5).Infof("Wrote CDI spec %s with %d devices", name, len(spec.Devices))
	return nil
}
