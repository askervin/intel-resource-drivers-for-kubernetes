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
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	coreclientset "k8s.io/client-go/kubernetes"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/klog/v2"
	"sigs.k8s.io/yaml"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/helpers"
	driverVersion "github.com/intel/intel-resource-drivers-for-kubernetes/pkg/version"

	nricxl "github.com/containers/nri-plugins/pkg/cxl"
)

type driver struct {
	client      coreclientset.Interface
	state       nodeState
	helper      *kubeletplugin.Helper
	config      *DriverConfig
	nriPlugin   *nriPlugin
	udevWatcher *UdevEventWatcher
	sysfsRoot   string
}

// TestabilityConfig holds configuration for injecting fake CXL
// devices for testing without real hardware. Fake devices are
// appended to the real detected devices before buildDevInfos()
// processes them.
type TestabilityConfig struct {
	// FakeRegionDevices lists CXL region devices to inject.
	// Each entry is appended to nricxl.Devices.RegionDevices
	// right after DevicesFromSysfs() returns. Memory devices
	// referenced in Memories are also appended to
	// nricxl.Devices.MemoryDevices.
	FakeRegionDevices []nricxl.RegionDevice `json:"fakeRegionDevices,omitempty"`
	// FakeUdevFifo is a path to a named pipe (FIFO) from which
	// the driver reads fake udev events. Each line is a JSON
	// object whose keys and values are udev event properties.
	// The driver creates the FIFO if it does not exist. Example:
	//   {"ACTION":"add","SUBSYSTEM":"node","DEVPATH":"/devices/system/node/node2"}
	FakeUdevFifo string `json:"fakeUdevFifo,omitempty"`
}

type DriverConfig struct {
	// IgnoreDevices is a list of CXL memory devices. Matches
	// uevent DEVNAME, <major>:<minor>, or serial number in
	// decimal format, or 0x<hex>. Example: ["mem0", "cxl/mem0",
	// "251:0", "3238060771", "0xc100e2e0"]
	IgnoreDevices []string
	// IgnoreRegions is a list of CXL memory regions ["region0"]
	// to be ignored by the driver.
	IgnoreRegions []string
	// IgnoreNodes is a list of NUMA nodes to be ignored by the
	// driver.
	IgnoreNodes []int

	// If IgnoreNew* is true, hotplugged devices, created regions
	// and enabled nodes will be ignored by the driver.
	IgnoreNewMemoryDevices bool
	IgnoreNewRegions       bool
	IgnoreNewNodes         bool

	// If IgnoreDRAMNodes is true, NUMA nodes with DRAM memory
	// will be ignored by the driver. By default, DRAM nodes are
	// exposed in DRAM ResourceSlices.
	IgnoreDRAMNodes bool

	// UdevStableDuration is the quiet period after the last udev
	// event before a rescan is triggered. Parsed as a Go
	// duration string (e.g. "200ms", "1s"). Default: "200ms".
	UdevStableDuration string `json:"udevStableDuration,omitempty"`

	// Testability enables injection of fake CXL devices for
	// testing without real hardware.
	Testability *TestabilityConfig `json:"testability,omitempty"`
}

func getCXLFlags(someFlags any) (*CXLFlags, error) {
	switch v := someFlags.(type) {
	case *CXLFlags:
		return v, nil
	default:
		return &CXLFlags{}, fmt.Errorf("could not parse driver flags as CXLFlags (got type: %T)", v)
	}
}

const defaultUdevStableDuration = 200 * time.Millisecond

// parseUdevStableDuration parses the UdevStableDuration config string.
// Returns the default (200ms) when the string is empty.
func parseUdevStableDuration(s string) (time.Duration, error) {
	if s == "" {
		return defaultUdevStableDuration, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid udevStableDuration %q: %w", s, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("udevStableDuration must not be negative, got %v", d)
	}
	return d, nil
}

func newDriverConfigFromString(configStr string) (*DriverConfig, error) {
	driverConfig := &DriverConfig{}
	if err := yaml.UnmarshalStrict([]byte(configStr), driverConfig); err != nil {
		return nil, fmt.Errorf("failed to parse driver config: %v", err)
	}
	return driverConfig, nil
}

// resolveConfigStr returns the configuration string from either the
// -c flag or the -f flag. It returns an error if both are provided.
func resolveConfigStr(flags *CXLFlags) (string, error) {
	hasStr := flags.ConfigStr != ""
	hasFile := flags.ConfigFile != ""

	if hasStr && hasFile {
		return "", fmt.Errorf("provide configuration as a file (-f) or as a string (-c), but not both")
	}

	if hasFile {
		data, err := os.ReadFile(flags.ConfigFile)
		if err != nil {
			return "", fmt.Errorf("failed to read configuration file %q: %v", flags.ConfigFile, err)
		}
		return string(data), nil
	}

	return flags.ConfigStr, nil
}

// injectFakeDevices appends fake CXL region devices from the
// testability configuration into the detected devices structure.
// Sensible defaults are applied for fields left unset.
func injectFakeDevices(tc *TestabilityConfig, devices *nricxl.Devices) {
	if len(tc.FakeRegionDevices) == 0 {
		return
	}
	klog.Warning("Testability: injecting fake CXL devices — do not use in production")
	for i := range tc.FakeRegionDevices {
		reg := &tc.FakeRegionDevices[i]
		if reg.SysfsPath == "" {
			reg.SysfsPath = fmt.Sprintf("/fake/sys/bus/cxl/devices/%s", reg.Name)
		}
		if reg.Mode == "" {
			reg.Mode = "ram"
		}
		if reg.Node >= 0 {
			reg.Enabled = true
		}
		if reg.OnlineSize == 0 && reg.Enabled {
			reg.OnlineSize = reg.Size
		}
		for j := range reg.Memories {
			mem := reg.Memories[j]
			if mem.SysfsPath == "" {
				mem.SysfsPath = fmt.Sprintf("/fake/sys/bus/cxl/devices/%s", mem.Name)
			}
			if mem.Driver == "" {
				mem.Driver = "cxl_mem"
			}
			if mem.DevName == "" {
				mem.DevName = fmt.Sprintf("cxl/%s", mem.Name)
			}
			mem.Enabled = true
			devices.MemoryDevices = append(devices.MemoryDevices, mem)
		}
		klog.Infof("Testability: injecting fake region %q (node %d, size %d, memories %d)",
			reg.Name, reg.Node, reg.Size, len(reg.Memories))
		devices.RegionDevices = append(devices.RegionDevices, reg)
	}
}

// scanDevices discovers CXL and DRAM devices from sysfs, injects
// fake devices if testability is configured, and builds the filtered
// DevicesInfo map. This is the single place where device scanning
// happens, used both at startup and on udev-triggered rescans.
func scanDevices(sysfsRoot string, driverConfig *DriverConfig) (device.DevicesInfo, error) {
	detectedDevices, err := nricxl.DevicesFromSysfs(sysfsRoot)
	if err != nil {
		return nil, fmt.Errorf("failed to detect devices: %v", err)
	}

	if driverConfig.Testability != nil {
		injectFakeDevices(driverConfig.Testability, detectedDevices)
	}

	if len(detectedDevices.RegionDevices) == 0 {
		klog.Info("No region devices detected")
	}

	devInfos, err := buildDevInfos(driverConfig, detectedDevices)
	if err != nil {
		return nil, fmt.Errorf("failed to build device infos: %v", err)
	}

	return devInfos, nil
}

func newDriver(ctx context.Context, config *helpers.Config) (helpers.Driver, error) {
	driverVersion.PrintDriverVersion(device.DriverName)

	preparedClaimsFilePath := path.Join(config.CommonFlags.KubeletPluginDir, device.PreparedClaimsFileName)

	cxlFlags, err := getCXLFlags(config.DriverFlags)
	if err != nil {
		return nil, fmt.Errorf("getCXLFlags: %w", err)
	}

	configStr, err := resolveConfigStr(cxlFlags)
	if err != nil {
		return nil, fmt.Errorf("resolveConfigStr: %w", err)
	}

	driverConfig, err := newDriverConfigFromString(configStr)
	if err != nil {
		return nil, fmt.Errorf("newDriverConfigFromString: %w", err)
	}

	// Trim "/sys" suffix if present in SysfsRoot
	sysfsRoot := cxlFlags.SysfsRoot
	if len(sysfsRoot) >= 4 && sysfsRoot[len(sysfsRoot)-4:] == "/sys" {
		cxlFlags.SysfsRoot = sysfsRoot[:len(sysfsRoot)-4]
	}

	devInfos, err := scanDevices(sysfsRoot, driverConfig)
	if err != nil {
		return nil, err
	}

	klog.V(3).Info("Creating new NodeState")
	state, err := newNodeState(devInfos, config.CommonFlags.CdiRoot, preparedClaimsFilePath, config.CommonFlags.NodeName, driverConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create new NodeState: %v", err)
	}

	driver := &driver{
		state:     *state,
		client:    config.Coreclient,
		config:    driverConfig,
		sysfsRoot: sysfsRoot,
	}

	klog.Infof(`Starting DRA resource-driver kubelet-plugin
RegistrarDirectoryPath: %v
PluginDataDirectoryPath: %v`,
		config.CommonFlags.KubeletPluginsRegistryDir,
		config.CommonFlags.KubeletPluginDir)

	helper, err := kubeletplugin.Start(
		ctx,
		driver,
		kubeletplugin.KubeClient(config.Coreclient),
		kubeletplugin.NodeName(config.CommonFlags.NodeName),
		kubeletplugin.DriverName(device.DriverName),
		kubeletplugin.RegistrarDirectoryPath(config.CommonFlags.KubeletPluginsRegistryDir),
		kubeletplugin.PluginDataDirectoryPath(config.CommonFlags.KubeletPluginDir),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start kubelet-plugin: %v", err)
	}

	driver.helper = helper

	if err := driver.PublishResourceSlice(ctx); err != nil {
		return nil, fmt.Errorf("startup error: %v", err)
	}

	// Start NRI plugin for container lifecycle hooks.
	nriOpts := NRIOpts{
		Name:   cxlFlags.NRIName,
		Idx:    cxlFlags.NRIIdx,
		Socket: cxlFlags.NRISocket,
	}
	nri, err := startNRIPlugin(ctx, &driver.state, nriOpts)
	if err != nil {
		klog.Warningf("Failed to start NRI plugin (continuing without it): %v", err)
	} else {
		driver.nriPlugin = nri
	}

	// Start udev event watcher for NUMA node hotplug detection.
	if !driverConfig.IgnoreNewNodes {
		stableDuration, err := parseUdevStableDuration(driverConfig.UdevStableDuration)
		if err != nil {
			return nil, err
		}

		watcher, err := NewUdevEventWatcher(stableDuration)
		if err != nil {
			klog.Warningf("Failed to create udev watcher (continuing without it): %v", err)
		} else {
			var fifoPath string
			if driverConfig.Testability != nil {
				fifoPath = driverConfig.Testability.FakeUdevFifo
			}
			if err := watcher.Start(fifoPath); err != nil {
				klog.Warningf("Failed to start udev watcher (continuing without it): %v", err)
			} else {
				driver.udevWatcher = watcher
				go driver.udevRescanLoop(ctx)
			}
		}
	} else {
		klog.V(3).Info("Udev node watcher disabled (ignoreNewNodes=true)")
	}

	klog.V(3).Info("Finished creating new driver")
	return driver, nil
}

func (d *driver) PrepareResourceClaims(ctx context.Context, claims []*resourceapi.ResourceClaim) (map[types.UID]kubeletplugin.PrepareResult, error) {
	klog.V(5).Infof("NodePrepareResource is called: request: %+v", claims)

	response := map[types.UID]kubeletplugin.PrepareResult{}

	for _, claim := range claims {
		response[claim.UID] = d.prepareResourceClaim(ctx, claim)
	}

	return response, nil
}

func (d *driver) prepareResourceClaim(ctx context.Context, claim *resourceapi.ResourceClaim) kubeletplugin.PrepareResult {
	klog.V(5).Infof("NodePrepareResource is called: request: %+v", claim)

	if claimPreparation, found := d.state.Prepared[string(claim.UID)]; found {
		klog.V(3).Infof("Claim %s was already prepared, nothing to do", claim.UID)
		return claimPreparation
	}

	if err := d.state.Prepare(ctx, claim); err != nil {
		return kubeletplugin.PrepareResult{
			Err: err,
		}
	}

	return d.state.Prepared[string(claim.UID)]
}

func (d *driver) UnprepareResourceClaims(ctx context.Context, claims []kubeletplugin.NamespacedObject) (map[types.UID]error, error) {
	klog.V(5).Infof("NodeUnprepareResource is called: number of claims: %d", len(claims))
	response := map[types.UID]error{}

	for _, claim := range claims {

		if err := d.state.Unprepare(ctx, string(claim.UID)); err != nil {
			response[claim.UID] = fmt.Errorf("error freeing devices: %v", err)
			continue
		}

		response[claim.UID] = nil
		klog.V(3).Infof("Freed devices for claim '%v'", claim.UID)

	}

	return response, nil
}

func (d *driver) PublishResourceSlice(ctx context.Context) error {
	resources := d.state.GetResources()
	klog.FromContext(ctx).Info("Publishing resources", "len", len(resources.Pools[d.state.NodeName].Slices[0].Devices))
	klog.V(5).Infof("devices: %+v", resources.Pools[d.state.NodeName].Slices[0].Devices)
	if err := d.helper.PublishResources(ctx, resources); err != nil {
		return fmt.Errorf("error publishing resources: %v", err)
	}

	return nil
}

// udevRescanLoop reads stable-notifications from the udev watcher and
// triggers a resource rescan on each one.
func (d *driver) udevRescanLoop(ctx context.Context) {
	for range d.udevWatcher.Events() {
		d.rescanAndPublish(ctx)
	}
	klog.V(3).Info("udevRescanLoop: exiting (watcher channel closed)")
}

// rescanAndPublish re-discovers hardware resources and republishes
// ResourceSlices if the set of allocatable devices has changed.
func (d *driver) rescanAndPublish(ctx context.Context) {
	klog.Infof("rescanAndPublish: udev event triggered rescan")

	newDevInfos, err := scanDevices(d.sysfsRoot, d.config)
	if err != nil {
		klog.Errorf("rescanAndPublish: %v", err)
		return
	}

	d.state.Lock()
	d.state.Allocatable = newDevInfos
	d.state.Unlock()

	if err := d.PublishResourceSlice(ctx); err != nil {
		klog.Errorf("rescanAndPublish: failed to publish resources: %v", err)
		return
	}

	klog.Infof("rescanAndPublish: published updated resources (%d devices)", len(newDevInfos))
}

func (d *driver) Shutdown(ctx context.Context) error {
	klog.V(5).Info("Shutting down driver")

	if d.udevWatcher != nil {
		d.udevWatcher.Stop()
	}
	if d.nriPlugin != nil {
		d.nriPlugin.Stop()
	}
	d.helper.Stop()

	return nil
}

// HandleError is called by Kubelet when an error occures asyncronously, and
// needs to be communicated to the DRA driver.
//
// This is a mandatory method because drivers should check for errors
// which won't get resolved by retrying and then fail or change the
// slices that they are trying to publish:
// - dropped fields (see [resourceslice.DroppedFieldsError])
// - validation errors (see [apierrors.IsInvalid]).
func (d *driver) HandleError(ctx context.Context, err error, message string) {
	if errors.Is(err, kubeletplugin.ErrRecoverable) {
		// TODO: FIXME: error is ignored ATM, handle it properly.
		klog.FromContext(ctx).Error(err, "DRAPlugin encountered an error.")
	} else {
		klog.FromContext(ctx).Error(err, "Unrecoverable error.")
	}

	runtime.HandleErrorWithContext(ctx, err, message)
}
