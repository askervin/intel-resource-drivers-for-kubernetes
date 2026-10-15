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

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/api"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/memorypolicy"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/helpers"

	nricxl "github.com/containers/nri-plugins/pkg/cxl"
)

// preparedClaimsInfoFileName is the file of the prepared claims info in
// the plugin data directory.
const preparedClaimsInfoFileName = "preparedClaimsInfo.json"

// PreparedDeviceInfo captures information about one allocated device
// within a claim, collected during Prepare for use by NRI handlers
// and kept in a file for storing and restoring driver state.
type PreparedDeviceInfo struct {
	RequestName string `json:"requestName"`
	DeviceName  string `json:"deviceName"` // device name in the pool
	// Result is the index of the allocation result of the device in the
	// claim, the n of its CDI device claim-<uid>-<n>.
	Result int `json:"result"`
	// DeviceType is device.DeviceTypeMemdev, DeviceTypeRegion,
	// DeviceTypeDRAM or DeviceTypeLease.
	DeviceType string `json:"deviceType"`
	// Mode is api.ModeNUMA or api.ModeDax of a memory device, "" for the
	// lease device.
	Mode           string `json:"mode,omitempty"`
	SysfsPath      string `json:"sysfsPath,omitempty"`      // sysfs path (CXL regions only)
	NUMANodes      []int  `json:"numaNodes"`                // NUMA node(s) for this device
	NodeAffinities []int  `json:"nodeAffinities,omitempty"` // CPU NUMA affinities of backing memory devices (CXL only)
	TotalBytes     uint64 `json:"totalBytes"`               // total device memory capacity (local memdev: slice size)
	ConsumedBytes  int64  `json:"consumedBytes"`            // allocated capacity from ConsumedCapacity

	// Serial is the serial (0x<hex>) of a local memdev.
	Serial string `json:"serial,omitempty"`
	// Region is the name of the region of a region device (region0).
	Region string `json:"region,omitempty"`
	// DaxDevice is /dev/daxX.Y of a dax slice, "" when the slice is gone.
	DaxDevice string `json:"daxDevice,omitempty"`
	DaxMajor  int    `json:"daxMajor,omitempty"`
	DaxMinor  int    `json:"daxMinor,omitempty"`
	ReadOnly  bool   `json:"readOnly,omitempty"` // dax slice: the device node is read-only
}

// PreparedClaimInfo captures all relevant information about a prepared
// claim: its devices, their types/capacities, and the memory policy.
// Persisted to preparedClaims.json for crash recovery.
type PreparedClaimInfo struct {
	ClaimName string                           `json:"claimName"` // namespace/name
	Policy    *memorypolicy.MemoryPolicyConfig `json:"policy,omitempty"`
	Devices   []PreparedDeviceInfo             `json:"devices"`
	// Lend is the lend of a companion claim.
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
	// podClaims maps pod UID -> set of claim UIDs prepared for that pod.
	// Built during Prepare from claim.Status.ReservedFor.
	podClaims map[string]map[string]bool
	// leases opens the lease socket of claims on the lease device, nil
	// when lending is disabled.
	leases leaseClaims
	// local creates the regions and slices of local memdevs for claims,
	// nil when local memdevs are not managed.
	local *localAllocator
	// lending lends the local memdevs of companion claims to running
	// containers.
	lending *companionLender
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

	if err := removeCXLSpecs(cdiCache); err != nil {
		return nil, err
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
		lending:        newCompanionLender(device.DriverName),
		locks:          &nodeLocks{},
	}

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

// removeCXLSpecs removes the CDI specs of kinds device.CDIKind and
// device.PoolCDIKind from the spec directory that the CDI cache writes to.
func removeCXLSpecs(cache *cdiapi.Cache) error {
	var errs []error
	for _, spec := range cache.GetVendorSpecs(device.CDIVendor) {
		if spec.Kind != device.CDIKind && spec.Kind != device.PoolCDIKind {
			continue
		}
		if err := cache.RemoveSpec(filepath.Base(spec.GetPath())); err != nil {
			errs = append(errs, fmt.Errorf("failed to remove old CDI spec %s: %w", spec.GetPath(), err))
		}
	}
	return errors.Join(errs...)
}

// buildDevInfos returns the devices to publish, by device name.
// poolSerials are the serials of pool devices (prepared by the pool
// driver, or published by a pool controller): their regions are skipped
// (D29), and they are never local memdevs. Regions on memdevs that the
// local allocator uses (local) are skipped too: the allocator created
// them, and the memdevs are published as memdev devices.
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
		name := regionDeviceName(regDev)
		devInfos[name] = &device.DeviceInfo{
			Name:      name,
			Dev:       any(regDev),
			SysfsPath: regDev.SysfsPath,
		}
	}
	if driverConfig.manageMemdevs() {
		addLocalMemdevs(devInfos, driverConfig, detectedDevices, poolSerials, local)
	}
	if !driverConfig.IgnoreDRAMNodes {
		dramDev := &device.DRAM{}
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
			devInfos[device.DRAMDeviceName] = &device.DeviceInfo{
				Name: device.DRAMDeviceName,
				Dev:  any(dramDev),
			}
			klog.V(3).Infof("discovered system DRAM device: %+v", dramDev)
		}
	}
	if driverConfig.PublishLeaseDevice {
		devInfos[device.LeaseDeviceName] = &device.DeviceInfo{
			Name: device.LeaseDeviceName,
			Dev:  &device.LeaseToken{},
		}
	}
	return devInfos, nil
}

// regionDeviceName returns the device name of a region: <region>-node<N>.
func regionDeviceName(regDev *nricxl.RegionDevice) string {
	return fmt.Sprintf("%s-node%d", regDev.Name, regDev.Node)
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
		if local.uses(memDev.Serial) {
			return fmt.Sprintf("created by the plugin on local memory device %s", memdevDeviceName(memDev.Serial))
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
		used := local.uses(memDev.Serial)
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
		name := memdevDeviceName(memDev.Serial)
		devInfos[name] = &device.DeviceInfo{
			Name: name,
			Dev: &device.Memdev{
				Name:         memDev.Name,
				Serial:       memDev.Serial,
				Size:         memDev.RamSize,
				NodeAffinity: memDev.NodeAffinity,
				BlockSize:    bs,
			},
			SysfsPath: memDev.SysfsPath,
		}
		klog.V(3).Infof("discovered local memory device %s: %s serial 0x%x size %d memory block size %d", name, memDev.Name, memDev.Serial, memDev.RamSize, bs)
	}
}

// memdevDeviceName returns the device name of a local memdev:
// "memdev-" + lower-case hex serial.
func memdevDeviceName(serial uint64) string {
	return device.MemdevDevicePrefix + strconv.FormatUint(serial, 16)
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

	for name, allocatable := range allocatableDevices {
		klog.V(5).Infof("Processing allocatable device %v: %+v", name, allocatable)
		if allocatable.Dev == nil {
			continue
		}
		switch dev := allocatable.Dev.(type) {
		case *nricxl.RegionDevice:
			devices = append(devices, regionDevice(name, dev))
		case *device.DRAM:
			devices = append(devices, dramDevice(name, dev))
		case *device.Memdev:
			devices = append(devices, localMemdevDevice(name, dev))
		case *device.LeaseToken:
			// no capacity, no node allocatable resources, any number
			// of claims can allocate it
			devices = append(devices, resourcev1.Device{
				Name: name,
				Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
					device.AttrType: {StringValue: ptr(device.DeviceTypeLease)},
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

// memoryCapacity returns the memory capacity of a device of size bytes.
func memoryCapacity(size uint64) map[resourcev1.QualifiedName]resourcev1.DeviceCapacity {
	return map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{
		device.CapacityMemory: {Value: *resource.NewQuantity(int64(size), resource.BinarySI)},
	}
}

// regionDevice returns the ResourceSlice device of a region.
func regionDevice(name string, dev *nricxl.RegionDevice) resourcev1.Device {
	attrs := map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
		device.AttrType:   {StringValue: ptr(device.DeviceTypeRegion)},
		device.AttrSource: {StringValue: ptr(api.SourceLocal)},
		device.AttrNode:   {IntValue: ptr(int64(dev.Node))},
		device.AttrSize:   {IntValue: ptr(int64(dev.Size))},
	}
	if a, ok := regionNodeAffinity(dev); ok {
		attrs[device.AttrNodeAffinity] = resourcev1.DeviceAttribute{IntValue: ptr(int64(a))}
	}
	return resourcev1.Device{
		Name:                     name,
		Attributes:               attrs,
		Capacity:                 memoryCapacity(dev.Size),
		NodeAllocatableResources: memoryNodeAllocatableResources(),
		AllowMultipleAllocations: ptr(true),
	}
}

// regionNodeAffinity returns the CPU NUMA node of the memdevs of a region,
// and whether they all have the same known one.
func regionNodeAffinity(dev *nricxl.RegionDevice) (int, bool) {
	affinity := -1
	for _, mem := range dev.Memories {
		switch {
		case mem.NodeAffinity < 0:
			return -1, false
		case affinity < 0:
			affinity = mem.NodeAffinity
		case mem.NodeAffinity != affinity:
			return -1, false
		}
	}
	return affinity, affinity >= 0
}

// dramDevice returns the ResourceSlice device of the DRAM of the node.
func dramDevice(name string, dev *device.DRAM) resourcev1.Device {
	return resourcev1.Device{
		Name: name,
		Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
			device.AttrType:   {StringValue: ptr(device.DeviceTypeDRAM)},
			device.AttrSource: {StringValue: ptr(api.SourceLocal)},
			device.AttrNodes:  {StringValue: ptr(cpuset.New(dev.Nodes...).String())},
			device.AttrSize:   {IntValue: ptr(int64(dev.Size))},
		},
		Capacity:                 memoryCapacity(dev.Size),
		NodeAllocatableResources: memoryNodeAllocatableResources(),
		AllowMultipleAllocations: ptr(true),
	}
}

// localMemdevDevice returns the ResourceSlice device of a local memdev.
func localMemdevDevice(name string, dev *device.Memdev) resourcev1.Device {
	attrs := map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
		device.AttrType:   {StringValue: ptr(device.DeviceTypeMemdev)},
		device.AttrSource: {StringValue: ptr(api.SourceLocal)},
		device.AttrSerial: {StringValue: ptr(api.FormatSerial(dev.Serial))},
		device.AttrShared: {BoolValue: ptr(false)},
		device.AttrSize:   {IntValue: ptr(int64(dev.Size))},
		// every slice is a multiple of the memory block size
		device.AttrMemoryBlockSize: {IntValue: ptr(int64(dev.BlockSize))},
	}
	if dev.NodeAffinity >= 0 {
		attrs[device.AttrNodeAffinity] = resourcev1.DeviceAttribute{IntValue: ptr(int64(dev.NodeAffinity))}
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
	defer unlock()
	return s.unprepare(ctx, claimUID)
}

// unprepare unprepares a claim.
func (s *nodeState) unprepare(ctx context.Context, claimUID string) error {
	info, found := s.preparedInfo(claimUID)
	if !found {
		return nil
	}
	if info != nil && info.Lend != nil {
		s.lending.releasing(info.Lend.Lease)
		// Revoke before the release: offlining memory that the
		// container still has in cpuset.mems leaves a stale node there.
		if err := s.lending.revoke(ctx, info.Lend, info.ClaimName); err != nil {
			return fmt.Errorf("claim %s: %w", info.ClaimName, err)
		}
	}
	if info.usesLocalMemdevs() {
		if err := s.local.Release(ctx, claimUID); err != nil {
			return fmt.Errorf("claim %s: %w", info.ClaimName, err)
		}
	}

	klog.V(5).Infof("Freeing devices from claim %v", claimUID)
	s.forget(claimUID)
	return s.persist()
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
// companion claim (label device.LeaseLabel) are lent to the running
// container of its lease.
func (s *nodeState) Prepare(ctx context.Context, claim *resourcev1.ResourceClaim) error {
	unlock := s.locks.lockClaim(string(claim.UID))
	defer unlock()
	return s.prepare(ctx, claim)
}

// prepare prepares a claim. A claim that is prepared already is not
// touched again: only the lease broker hears about a prepared companion
// claim again.
func (s *nodeState) prepare(ctx context.Context, claim *resourcev1.ResourceClaim) error {
	claimUID := string(claim.UID)
	if info, found := s.preparedInfo(claimUID); found {
		klog.V(3).Infof("Claim %s was already prepared, nothing to do", claimUID)
		if info != nil && info.Lend != nil {
			s.lending.reprepared(info.Lend, claim, s.lentDevices(info))
		}
		return nil
	}

	if claim.Status.Allocation == nil {
		return fmt.Errorf("no allocation found in claim %v/%v status", claim.Namespace, claim.Name)
	}
	if err := checkConfigKinds(claim, device.DriverName, device.DeviceConfigKind, memorypolicy.Kind); err != nil {
		return fmt.Errorf("claim %v/%v: %w", claim.Namespace, claim.Name, err)
	}
	allocatable, err := s.allocatable()
	if err != nil {
		return err
	}

	local, all := s.localMemdevResults(claim, allocatable)
	lend, err := s.checkLend(claim, local, all)
	if err != nil {
		return err
	}

	allocatedDevices, claimInfo, err := s.prepareAllocatedDevices(ctx, claim, allocatable)
	if err == nil && lend != nil {
		err = s.lendClaim(ctx, lend, claimInfo)
		// The memory of a companion claim belongs to the borrower, and
		// the companion pod references no claim: no CDI devices.
		dropCDIDevices(&allocatedDevices)
	}
	if err != nil {
		s.releaseLocal(ctx, claim)
		return err
	}

	if claimInfo.Policy != nil {
		klog.V(3).Infof("Claim %v has memory policy: order=%s minStep=%s maxStep=%s",
			claim.UID, claimInfo.Policy.MemoryUseOrder, claimInfo.Policy.MinStep, claimInfo.Policy.MaxStep)
	}
	s.store(claim, allocatedDevices, claimInfo)

	if err := s.persist(); err != nil {
		klog.Errorf("%v", err)
		s.rollback(ctx, claim, claimInfo, err)
		return err
	}

	if lend != nil {
		s.lending.prepared(lend.Lease, s.lentDevices(claimInfo))
	}
	klog.V(5).Infof("Created prepared claim %v allocation", claim.UID)
	return nil
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
func (s *nodeState) rollback(ctx context.Context, claim *resourcev1.ResourceClaim, info *PreparedClaimInfo, cause error) {
	rctx, cancel := rollbackContext(ctx)
	defer cancel()
	if info.Lend != nil {
		if err := s.lending.revoke(rctx, info.Lend, info.ClaimName); err != nil {
			klog.Errorf("claim %s: rollback: %v", info.ClaimName, err)
		}
		s.lending.failed(info.Lend.Lease, cause)
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
			klog.V(5).Infof("Recorded pod %s (%s) -> claim %s mapping", consumer.Name, podUID, claimUID)
		}
	}
}

// persist writes the CDI spec and the files of the prepared claims.
func (s *nodeState) persist() error {
	unlock := s.locks.lockWrite()
	defer unlock()
	s.Lock()
	spec := s.buildClaimsCDISpec()
	prepared, infos := maps.Clone(s.Prepared), maps.Clone(s.preparedClaims)
	s.Unlock()
	if err := s.writeCDISpec(spec, device.ClaimsCDISpecName); err != nil {
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

// forgetGoneSlices drops the dax devices of the slices of prepared claims
// that the local allocator does not hold (gone with a reboot), so that a
// stale device number never reaches a container.
func (s *nodeState) forgetGoneSlices() {
	for uid, info := range s.preparedClaims {
		for i := range info.Devices {
			d := &info.Devices[i]
			if d.Mode == api.ModeDax && d.DaxDevice != "" && !s.local.holds(uid, filepath.Base(d.DaxDevice)) {
				klog.Warningf("claim %s: no device node for the dax slice %s: the slice is gone", uid, d.DaxDevice)
				d.DaxDevice, d.DaxMajor, d.DaxMinor = "", 0, 0
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
			if _, ok := d.Dev.(*device.Memdev); ok {
				local++
			}
		}
	}
	return local, all
}

// checkLend returns the lend of a companion claim, nil if the claim is not
// one, and checks that the claim can be lent.
func (s *nodeState) checkLend(claim *resourcev1.ResourceClaim, local, all int) (*lendRecord, error) {
	id := leaseOf(claim)
	if id == "" {
		return nil, nil
	}
	if local == 0 || local != all {
		return nil, fmt.Errorf("claim %v/%v: the companion claim of lease %s needs %s devices only", claim.Namespace, claim.Name, id, device.DeviceTypeMemdev)
	}
	rec, err := s.lending.check(id, claim)
	if err != nil {
		return nil, fmt.Errorf("claim %v/%v: %w", claim.Namespace, claim.Name, err)
	}
	return rec, nil
}

// lendClaim lends the local memdevs of a prepared companion claim and
// records the lend in the claim info.
func (s *nodeState) lendClaim(ctx context.Context, rec *lendRecord, claimInfo *PreparedClaimInfo) error {
	if err := s.lending.lend(ctx, rec, s.lentDevices(claimInfo)); err != nil {
		if rerr := s.lending.revoke(ctx, rec, claimInfo.ClaimName); rerr != nil {
			klog.Errorf("claim %s: %v", claimInfo.ClaimName, rerr)
		}
		s.lending.failed(rec.Lease, err)
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
		if d.DeviceType != device.DeviceTypeMemdev {
			continue
		}
		serial, _ := api.ParseSerial(d.Serial)
		ld := lentDevice{
			Driver:   device.DriverName,
			Source:   api.SourceLocal,
			Request:  d.RequestName,
			Device:   d.DeviceName,
			Pool:     s.NodeName,
			Serial:   serial,
			Size:     d.TotalBytes,
			Mode:     d.Mode,
			ReadOnly: d.ReadOnly,
			Node:     -1,
		}
		if d.Mode == api.ModeDax {
			ld.DaxDevice = d.DaxDevice
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
		if d.DeviceType == device.DeviceTypeMemdev {
			return true
		}
	}
	return false
}

// dropCDIDevices removes the CDI devices from the devices of a prepare
// result.
func dropCDIDevices(result *kubeletplugin.PrepareResult) {
	for i := range result.Devices {
		result.Devices[i].CDIDeviceIDs = nil
	}
}

// prepareAllocatedDevices prepares the devices of a claim from
// allocatableDevices.
func (s *nodeState) prepareAllocatedDevices(ctx context.Context, claim *resourcev1.ResourceClaim, allocatableDevices device.DevicesInfo) (allocatedDevices kubeletplugin.PrepareResult, claimInfo *PreparedClaimInfo, err error) {
	allocatedDevices = kubeletplugin.PrepareResult{}
	claimInfo = &PreparedClaimInfo{
		ClaimName: claim.Namespace + "/" + claim.Name,
	}

	hasLease := false
	for n, allocatedDevice := range claim.Status.Allocation.Devices.Results {
		// ATM the only pool is cluster node's pool: all devices on current node.
		if allocatedDevice.Driver != device.DriverName || allocatedDevice.Pool != s.NodeName {
			klog.Infof("ignoring claim allocation device %+v", allocatedDevice)
			continue
		}

		allocatableDevice, found := allocatableDevices[allocatedDevice.Device]
		if !found {
			return allocatedDevices, nil, fmt.Errorf("could not find allocatable device %v (pool %v)", allocatedDevice.Device, allocatedDevice.Pool)
		}

		// The CDI device of the allocation result (written with the
		// prepared claims) injects the claim marker CXL_CLAIM_<uid>=1,
		// the environment of the result's memory and its dax device
		// node into the containers that reference the result's request.
		// The NRI CreateContainer handler finds the claims of a
		// container by their markers.
		newDevice := kubeletplugin.Device{
			Requests:     []string{allocatedDevice.Request},
			PoolName:     allocatedDevice.Pool,
			DeviceName:   allocatedDevice.Device,
			CDIDeviceIDs: []string{claimCDIDeviceID(string(claim.UID), n)},
		}

		// Collect device info for NRI logging and future cgmpolmgr use.
		devInfo, err := s.preparedDeviceInfo(ctx, claim, &allocatedDevice, allocatableDevice)
		if err != nil {
			return allocatedDevices, nil, err
		}
		devInfo.Result = n
		if devInfo.DeviceType == device.DeviceTypeLease {
			hasLease = true
			newDevice.CDIDeviceIDs = append(newDevice.CDIDeviceIDs, leaseCDIDeviceID(string(claim.UID)))
		}
		allocatedDevices.Devices = append(allocatedDevices.Devices, newDevice)
		claimInfo.Devices = append(claimInfo.Devices, devInfo)

		klog.V(5).Infof("Prepared device %v for claim %v: type=%s mode=%s numa=%v total=%d consumed=%d",
			allocatedDevice.Device, claim.UID, devInfo.DeviceType, devInfo.Mode,
			devInfo.NUMANodes, devInfo.TotalBytes, devInfo.ConsumedBytes)
	}

	// A claim on the lease device gets its lease socket.
	if hasLease {
		if s.leases == nil {
			return allocatedDevices, nil, fmt.Errorf("claim %v/%v: lending to running containers is disabled (--leases=false)", claim.Namespace, claim.Name)
		}
		if err := s.leases.OpenClaim(claim); err != nil {
			return allocatedDevices, nil, fmt.Errorf("claim %v/%v: lease socket: %w", claim.Namespace, claim.Name, err)
		}
	}

	claimInfo.Policy, err = parseMemoryPolicy(claim)
	if err != nil {
		return allocatedDevices, nil, fmt.Errorf("claim %v/%v: %w", claim.Namespace, claim.Name, err)
	}

	return allocatedDevices, claimInfo, nil
}

// preparedDeviceInfo prepares the device of an allocation result and
// returns its info.
func (s *nodeState) preparedDeviceInfo(ctx context.Context, claim *resourcev1.ResourceClaim, r *resourcev1.DeviceRequestAllocationResult, allocatable *device.DeviceInfo) (PreparedDeviceInfo, error) {
	devInfo := PreparedDeviceInfo{
		RequestName: r.Request,
		DeviceName:  r.Device,
	}
	switch dev := allocatable.Dev.(type) {
	case *nricxl.RegionDevice:
		if err := checkNUMAOnly(claim, r, device.DeviceTypeRegion); err != nil {
			return devInfo, err
		}
		devInfo.DeviceType = device.DeviceTypeRegion
		devInfo.Mode = api.ModeNUMA
		devInfo.Region = dev.Name
		devInfo.SysfsPath = dev.SysfsPath
		devInfo.NUMANodes = []int{dev.Node}
		devInfo.TotalBytes = dev.Size
		for _, mem := range dev.Memories {
			if mem.NodeAffinity >= 0 {
				devInfo.NodeAffinities = append(devInfo.NodeAffinities, mem.NodeAffinity)
			}
		}
	case *device.DRAM:
		if err := checkNUMAOnly(claim, r, device.DeviceTypeDRAM); err != nil {
			return devInfo, err
		}
		devInfo.DeviceType = device.DeviceTypeDRAM
		devInfo.Mode = api.ModeNUMA
		devInfo.NUMANodes = dev.Nodes
		devInfo.TotalBytes = dev.Size
	case *device.LeaseToken:
		devInfo.DeviceType = device.DeviceTypeLease
	case *device.Memdev:
		// the slice is what the claim consumes
		err := s.prepareLocalMemdev(ctx, claim, r, dev, &devInfo)
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
// DeviceConfig of the request, and describes it in devInfo: the slice size
// is its total and consumed bytes.
func (s *nodeState) prepareLocalMemdev(ctx context.Context, claim *resourcev1.ResourceClaim, r *resourcev1.DeviceRequestAllocationResult, dev *device.Memdev, devInfo *PreparedDeviceInfo) error {
	cfg := &deviceConfig{}
	_, err := parseConfig(claim, device.DriverName, device.DeviceConfigKind, r.Request, cfg)
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
	devInfo.DeviceType = device.DeviceTypeMemdev
	devInfo.Mode = mode
	devInfo.Serial = api.FormatSerial(dev.Serial)
	devInfo.SysfsPath = s.local.regionPath(slice.Region)
	devInfo.TotalBytes = slice.Size
	devInfo.ConsumedBytes = int64(slice.Size)
	if dev.NodeAffinity >= 0 {
		devInfo.NodeAffinities = []int{dev.NodeAffinity}
	}
	if mode == api.ModeNUMA {
		devInfo.NUMANodes = []int{node}
		return nil
	}
	devInfo.DaxDevice, devInfo.DaxMajor, devInfo.DaxMinor, devInfo.ReadOnly = slice.DaxDevice, slice.Major, slice.Minor, slice.ReadOnly
	return nil
}

// checkNUMAOnly returns an error if the DeviceConfig of an allocation
// result on a device that is used as numa only (what: region or dram) asks
// for another mode or for readOnly.
func checkNUMAOnly(claim *resourcev1.ResourceClaim, r *resourcev1.DeviceRequestAllocationResult, what string) error {
	cfg := &deviceConfig{}
	_, err := parseConfig(claim, device.DriverName, device.DeviceConfigKind, r.Request, cfg)
	if err == nil {
		err = cfg.numaOnly(what)
	}
	if err != nil {
		return fmt.Errorf("claim %v/%v: request %s: %w", claim.Namespace, claim.Name, r.Request, err)
	}
	return nil
}

// parseMemoryPolicy returns the MemoryPolicyConfig of a claim, nil if it
// has none: a claim config replaces a class config.
func parseMemoryPolicy(claim *resourcev1.ResourceClaim) (*memorypolicy.MemoryPolicyConfig, error) {
	if claim.Status.Allocation == nil {
		return nil, nil
	}
	cfg := &memorypolicy.MemoryPolicyConfig{}
	found, err := parseConfig(claim, device.DriverName, memorypolicy.Kind, "", cfg)
	if err != nil || !found {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", memorypolicy.Kind, err)
	}
	return cfg, nil
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

// claimCDIDeviceName returns the CDI device name of the allocation result
// n of a claim: "claim-<uid>-<n>".
func claimCDIDeviceName(claimUID string, n int) string {
	return "claim-" + claimUID + "-" + strconv.Itoa(n)
}

// claimCDIDeviceID returns the qualified CDI device name of the allocation
// result n of a claim of cxl.generic.
func claimCDIDeviceID(claimUID string, n int) string {
	return cdiparser.QualifiedName(device.CDIVendor, device.CDIClass, claimCDIDeviceName(claimUID, n))
}

// claimEnvVarName returns the environment variable name for a claim
// marker. Hyphens in the UID are replaced with underscores since env
// var names don't allow hyphens.
func claimEnvVarName(claimUID string) string {
	return api.ClaimEnvPrefix + strings.ReplaceAll(claimUID, "-", "_")
}

// ClaimUIDFromEnvVar extracts a claim UID from a CXL_CLAIM_* env var
// name, reversing the hyphen->underscore mapping. Returns the UID and
// true if the env var matches the prefix, or "" and false otherwise.
func ClaimUIDFromEnvVar(envVar string) (string, bool) {
	// envVar is "KEY=VALUE"
	parts := strings.SplitN(envVar, "=", 2)
	key := parts[0]
	if !strings.HasPrefix(key, api.ClaimEnvPrefix) {
		return "", false
	}
	uidWithUnderscores := key[len(api.ClaimEnvPrefix):]
	// Restore hyphens. UID format is 8-4-4-4-12 hex digits.
	uid := strings.ReplaceAll(uidWithUnderscores, "_", "-")
	return uid, true
}

// buildClaimsCDISpec builds the CDI spec of all prepared claims but
// companion claims: one device claim-<uid>-<n> per allocation result n with
// the claim marker, the environment of the result's memory device
// (api.Env) and the device node of a dax slice. It is built from the
// prepared claims (not from the existing CDI file), so it is right also
// after a driver restart. Called with the tables locked.
func (s *nodeState) buildClaimsCDISpec() *cdiSpecs.Spec {
	spec := &cdiSpecs.Spec{
		Kind: device.CDIKind,
	}
	for _, claimUID := range slices.Sorted(maps.Keys(s.Prepared)) {
		info := s.preparedClaims[claimUID]
		if info == nil || info.Lend != nil {
			continue
		}
		keys := envKeys{}
		for i := range info.Devices {
			d := &info.Devices[i]
			env := map[string]string{claimEnvVarName(claimUID): "1"}
			edits := cdiSpecs.ContainerEdits{}
			if m, ok := d.memory(keys); ok {
				maps.Copy(env, api.Env(m))
			}
			if d.Mode == api.ModeDax && d.DaxDevice != "" {
				edits.DeviceNodes = []*cdiSpecs.DeviceNode{daxDeviceNode(d.DaxDevice, d.DaxMajor, d.DaxMinor, d.ReadOnly)}
			}
			edits.Env = api.EnvList(env)
			spec.Devices = append(spec.Devices, cdiSpecs.Device{Name: claimCDIDeviceName(claimUID, d.Result), ContainerEdits: edits})
		}
	}
	return spec
}

// memory returns what a prepared memory device gives a container, with
// its env key from keys, and false for the lease device.
func (d *PreparedDeviceInfo) memory(keys envKeys) (api.Memory, bool) {
	m := api.Memory{Mode: d.Mode, Size: d.TotalBytes, Node: -1}
	if d.ConsumedBytes > 0 {
		m.Size = uint64(d.ConsumedBytes)
	}
	if len(d.NUMANodes) > 0 {
		m.Node = d.NUMANodes[0]
	}
	switch d.DeviceType {
	case device.DeviceTypeMemdev:
		serial, err := api.ParseSerial(d.Serial)
		if err != nil {
			return m, false
		}
		m.Key, m.Serial, m.Size = keys.next(api.Key(serial, 1)), d.Serial, d.TotalBytes
		m.Dax, m.ReadOnly = d.DaxDevice, d.ReadOnly
	case device.DeviceTypeRegion:
		m.Key = keys.next(api.RegionKey(d.Region))
	case device.DeviceTypeDRAM:
		m.Key, m.Nodes = keys.next(api.DRAMKey), cpuset.New(d.NUMANodes...).String()
	default:
		return m, false
	}
	return m, true
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
