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
	"os"
	"path/filepath"
	"strings"
	"testing"

	nricxl "github.com/containers/nri-plugins/pkg/cxl"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/helpers"
)

func TestNewDriverConfigFromString_WithTestability(t *testing.T) {
	configJSON := `{
		"testability": {
			"fakeRegionDevices": [
				{
					"Name": "fakeregion0",
					"Size": 1073741824,
					"Mode": "ram",
					"Node": 2,
					"Memories": [
						{
							"Name": "fakemem0",
							"RamSize": 1073741824,
							"Serial": 12345,
							"NodeAffinity": 0
						}
					]
				}
			]
		}
	}`

	cfg, err := newDriverConfigFromString(configJSON)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Testability == nil {
		t.Fatal("expected non-nil Testability")
	}
	if len(cfg.Testability.FakeRegionDevices) != 1 {
		t.Fatalf("expected 1 fake region, got %d", len(cfg.Testability.FakeRegionDevices))
	}
	reg := cfg.Testability.FakeRegionDevices[0]
	if reg.Name != "fakeregion0" {
		t.Errorf("expected name fakeregion0, got %s", reg.Name)
	}
	if reg.Size != 1073741824 {
		t.Errorf("expected size 1073741824, got %d", reg.Size)
	}
	if reg.Node != 2 {
		t.Errorf("expected node 2, got %d", reg.Node)
	}
	if len(reg.Memories) != 1 {
		t.Fatalf("expected 1 memory device, got %d", len(reg.Memories))
	}
	if reg.Memories[0].Name != "fakemem0" {
		t.Errorf("expected memory name fakemem0, got %s", reg.Memories[0].Name)
	}
	if reg.Memories[0].Serial != 12345 {
		t.Errorf("expected serial 12345, got %d", reg.Memories[0].Serial)
	}
}

func TestNewDriverConfigFromString_WithoutTestability(t *testing.T) {
	configJSON := `{"ignoreDevices": ["mem0"]}`

	cfg, err := newDriverConfigFromString(configJSON)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Testability != nil {
		t.Fatalf("expected nil Testability, got %+v", cfg.Testability)
	}
	if len(cfg.IgnoreDevices) != 1 || cfg.IgnoreDevices[0] != "mem0" {
		t.Errorf("expected IgnoreDevices [mem0], got %v", cfg.IgnoreDevices)
	}
}

func TestNewDriverConfigFromString_EmptyConfig(t *testing.T) {
	cfg, err := newDriverConfigFromString("{}")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Testability != nil {
		t.Fatalf("expected nil Testability for empty config")
	}
}

func TestInjectFakeDevices_Defaults(t *testing.T) {
	tc := &TestabilityConfig{
		FakeRegionDevices: []nricxl.RegionDevice{
			{
				Name: "fakeregion0",
				Size: 1 << 30, // 1 GiB
				Node: 2,
				Memories: []*nricxl.MemoryDevice{
					{
						Name:         "fakemem0",
						RamSize:      1 << 30,
						Serial:       12345,
						NodeAffinity: 0,
					},
				},
			},
		},
	}
	devices := nricxl.NewDevices()

	injectFakeDevices(tc, devices)

	// Verify region was appended
	if len(devices.RegionDevices) != 1 {
		t.Fatalf("expected 1 region, got %d", len(devices.RegionDevices))
	}
	reg := devices.RegionDevices[0]
	if reg.Name != "fakeregion0" {
		t.Errorf("expected name fakeregion0, got %s", reg.Name)
	}
	// Verify defaults applied
	if reg.SysfsPath != "/fake/sys/bus/cxl/devices/fakeregion0" {
		t.Errorf("expected fake sysfs path, got %s", reg.SysfsPath)
	}
	if reg.Mode != "ram" {
		t.Errorf("expected mode ram, got %s", reg.Mode)
	}
	if !reg.Enabled {
		t.Error("expected Enabled=true for node >= 0")
	}
	if reg.OnlineSize != reg.Size {
		t.Errorf("expected OnlineSize=%d, got %d", reg.Size, reg.OnlineSize)
	}

	// Verify memory device was appended to devices.MemoryDevices
	if len(devices.MemoryDevices) != 1 {
		t.Fatalf("expected 1 memory device, got %d", len(devices.MemoryDevices))
	}
	mem := devices.MemoryDevices[0]
	if mem.SysfsPath != "/fake/sys/bus/cxl/devices/fakemem0" {
		t.Errorf("expected fake sysfs path, got %s", mem.SysfsPath)
	}
	if mem.Driver != "cxl_mem" {
		t.Errorf("expected driver cxl_mem, got %s", mem.Driver)
	}
	if mem.DevName != "cxl/fakemem0" {
		t.Errorf("expected devname cxl/fakemem0, got %s", mem.DevName)
	}
	if !mem.Enabled {
		t.Error("expected memory device Enabled=true")
	}
}

func TestInjectFakeDevices_PreservesExplicitValues(t *testing.T) {
	tc := &TestabilityConfig{
		FakeRegionDevices: []nricxl.RegionDevice{
			{
				Name:      "fakeregion0",
				SysfsPath: "/custom/path/fakeregion0",
				Size:      1 << 30,
				Mode:      "pmem",
				Node:      3,
				Memories: []*nricxl.MemoryDevice{
					{
						Name:    "fakemem0",
						Driver:  "custom_driver",
						DevName: "custom/fakemem0",
					},
				},
			},
		},
	}
	devices := nricxl.NewDevices()

	injectFakeDevices(tc, devices)

	reg := devices.RegionDevices[0]
	if reg.SysfsPath != "/custom/path/fakeregion0" {
		t.Errorf("expected custom sysfs path preserved, got %s", reg.SysfsPath)
	}
	if reg.Mode != "pmem" {
		t.Errorf("expected mode pmem preserved, got %s", reg.Mode)
	}

	mem := devices.MemoryDevices[0]
	if mem.Driver != "custom_driver" {
		t.Errorf("expected custom driver preserved, got %s", mem.Driver)
	}
	if mem.DevName != "custom/fakemem0" {
		t.Errorf("expected custom devname preserved, got %s", mem.DevName)
	}
}

func TestInjectFakeDevices_DisabledRegion(t *testing.T) {
	tc := &TestabilityConfig{
		FakeRegionDevices: []nricxl.RegionDevice{
			{
				Name: "fakeregion0",
				Size: 1 << 30,
				Node: -1, // disabled
			},
		},
	}
	devices := nricxl.NewDevices()

	injectFakeDevices(tc, devices)

	reg := devices.RegionDevices[0]
	if reg.Enabled {
		t.Error("expected Enabled=false for node -1")
	}
	if reg.OnlineSize != 0 {
		t.Errorf("expected OnlineSize=0 for disabled region, got %d", reg.OnlineSize)
	}
}

func TestInjectFakeDevices_EmptyConfig(t *testing.T) {
	tc := &TestabilityConfig{}
	devices := nricxl.NewDevices()

	injectFakeDevices(tc, devices)

	if len(devices.RegionDevices) != 0 {
		t.Errorf("expected no regions, got %d", len(devices.RegionDevices))
	}
	if len(devices.MemoryDevices) != 0 {
		t.Errorf("expected no memory devices, got %d", len(devices.MemoryDevices))
	}
}

func TestInjectFakeDevices_AppendsToExistingDevices(t *testing.T) {
	tc := &TestabilityConfig{
		FakeRegionDevices: []nricxl.RegionDevice{
			{
				Name: "fakeregion0",
				Size: 1 << 30,
				Node: 2,
			},
		},
	}
	// Pre-populate with existing devices
	devices := nricxl.NewDevices()
	existingRegion := &nricxl.RegionDevice{Name: "region0", Node: 0, Enabled: true}
	existingMem := &nricxl.MemoryDevice{Name: "mem0", Enabled: true}
	devices.RegionDevices = append(devices.RegionDevices, existingRegion)
	devices.MemoryDevices = append(devices.MemoryDevices, existingMem)

	injectFakeDevices(tc, devices)

	if len(devices.RegionDevices) != 2 {
		t.Fatalf("expected 2 regions (1 existing + 1 fake), got %d", len(devices.RegionDevices))
	}
	if devices.RegionDevices[0].Name != "region0" {
		t.Errorf("expected first region to be existing region0, got %s", devices.RegionDevices[0].Name)
	}
	if devices.RegionDevices[1].Name != "fakeregion0" {
		t.Errorf("expected second region to be fakeregion0, got %s", devices.RegionDevices[1].Name)
	}
}

func TestInjectFakeDevices_MultipleRegions(t *testing.T) {
	tc := &TestabilityConfig{
		FakeRegionDevices: []nricxl.RegionDevice{
			{
				Name: "fakeregion0",
				Size: 1 << 30,
				Node: 2,
				Memories: []*nricxl.MemoryDevice{
					{Name: "fakemem0", RamSize: 1 << 30},
				},
			},
			{
				Name: "fakeregion1",
				Size: 2 << 30,
				Node: 3,
				Memories: []*nricxl.MemoryDevice{
					{Name: "fakemem1", RamSize: 1 << 30},
					{Name: "fakemem2", RamSize: 1 << 30},
				},
			},
		},
	}
	devices := nricxl.NewDevices()

	injectFakeDevices(tc, devices)

	if len(devices.RegionDevices) != 2 {
		t.Fatalf("expected 2 regions, got %d", len(devices.RegionDevices))
	}
	if len(devices.MemoryDevices) != 3 {
		t.Fatalf("expected 3 memory devices, got %d", len(devices.MemoryDevices))
	}
}

func TestBuildDevInfos_WithFakeDevices(t *testing.T) {
	devices := nricxl.NewDevices()
	devices.RegionDevices = []*nricxl.RegionDevice{
		{
			Name:      "fakeregion0",
			SysfsPath: "/fake/sys/bus/cxl/devices/fakeregion0",
			Size:      1 << 30,
			Mode:      "ram",
			Node:      2,
			Enabled:   true,
			Memories: []*nricxl.MemoryDevice{
				{
					Name:         "fakemem0",
					RamSize:      1 << 30,
					Serial:       12345,
					NodeAffinity: 0,
					Enabled:      true,
				},
			},
		},
	}

	driverConfig := &DriverConfig{IgnoreDRAMNodes: true}
	devInfos, err := buildDevInfos(driverConfig, devices, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(devInfos) != 1 {
		t.Fatalf("expected 1 device info, got %d", len(devInfos))
	}

	info, ok := devInfos["fakeregion0-node2"]
	if !ok {
		t.Fatal("expected device info keyed by fakeregion0-node2")
	}
	if info.Name != "fakeregion0-node2" {
		t.Errorf("expected name fakeregion0-node2, got %s", info.Name)
	}
	if info.SysfsPath != "/fake/sys/bus/cxl/devices/fakeregion0" {
		t.Errorf("expected fake sysfs path, got %s", info.SysfsPath)
	}

	// Verify the Dev field contains the RegionDevice
	regDev, ok := info.Dev.(*nricxl.RegionDevice)
	if !ok {
		t.Fatalf("expected Dev to be *nricxl.RegionDevice, got %T", info.Dev)
	}
	if regDev.Size != 1<<30 {
		t.Errorf("expected size 1GiB, got %d", regDev.Size)
	}
}

func TestTestabilityConfig_JSONRoundTrip(t *testing.T) {
	original := &DriverConfig{
		IgnoreDevices: []string{"mem0"},
		Testability: &TestabilityConfig{
			FakeRegionDevices: []nricxl.RegionDevice{
				{
					Name: "fakeregion0",
					Size: 1 << 30,
					Node: 2,
					Mode: "ram",
					Memories: []*nricxl.MemoryDevice{
						{
							Name:         "fakemem0",
							RamSize:      1 << 30,
							Serial:       42,
							NodeAffinity: 0,
						},
					},
				},
			},
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	restored, err := newDriverConfigFromString(string(data))
	if err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if restored.Testability == nil {
		t.Fatal("expected non-nil Testability after round-trip")
	}
	if len(restored.Testability.FakeRegionDevices) != 1 {
		t.Fatalf("expected 1 fake region after round-trip, got %d", len(restored.Testability.FakeRegionDevices))
	}
	reg := restored.Testability.FakeRegionDevices[0]
	if reg.Name != "fakeregion0" || reg.Size != 1<<30 || reg.Node != 2 {
		t.Errorf("unexpected region values after round-trip: %+v", reg)
	}
	if len(reg.Memories) != 1 || reg.Memories[0].Serial != 42 {
		t.Errorf("unexpected memory values after round-trip: %+v", reg.Memories)
	}
}

func TestNewDriverConfigFromString_YAML(t *testing.T) {
	configYAML := `
ignoreDevices:
  - mem0
  - mem1
ignoreRegions:
  - region0
ignoreNodes:
  - 1
  - 3
ignoreNewMemoryDevices: true
ignoreNewRegions: true
ignoreNewNodes: true
ignoreDRAMNodes: true
`
	cfg, err := newDriverConfigFromString(configYAML)
	if err != nil {
		t.Fatalf("unexpected error parsing YAML: %v", err)
	}
	if len(cfg.IgnoreDevices) != 2 || cfg.IgnoreDevices[0] != "mem0" || cfg.IgnoreDevices[1] != "mem1" {
		t.Errorf("expected IgnoreDevices [mem0 mem1], got %v", cfg.IgnoreDevices)
	}
	if len(cfg.IgnoreRegions) != 1 || cfg.IgnoreRegions[0] != "region0" {
		t.Errorf("expected IgnoreRegions [region0], got %v", cfg.IgnoreRegions)
	}
	if len(cfg.IgnoreNodes) != 2 || cfg.IgnoreNodes[0] != 1 || cfg.IgnoreNodes[1] != 3 {
		t.Errorf("expected IgnoreNodes [1 3], got %v", cfg.IgnoreNodes)
	}
	if !cfg.IgnoreDRAMNodes || !cfg.IgnoreNewMemoryDevices || !cfg.IgnoreNewRegions || !cfg.IgnoreNewNodes {
		t.Errorf("expected ignoreDRAMNodes and ignoreNew*: %+v", cfg)
	}
}

// TestDriverConfigKeys checks that the keys of the configuration are the
// lower-camel json tags.
func TestDriverConfigKeys(t *testing.T) {
	cfg := &DriverConfig{IgnoreDevices: []string{"mem0"}, IgnoreRegions: []string{"region0"}, IgnoreNodes: []int{1},
		IgnoreNewMemoryDevices: true, IgnoreNewRegions: true, IgnoreNewNodes: true, IgnoreDRAMNodes: true,
		UdevStableDuration: "1s", ManageMemdevs: ptr(true), PublishLeaseDevice: true}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"ignoreDevices":["mem0"],"ignoreRegions":["region0"],"ignoreNodes":[1],"ignoreNewMemoryDevices":true,"ignoreNewRegions":true,` +
		`"ignoreNewNodes":true,"ignoreDRAMNodes":true,"udevStableDuration":"1s","manageMemdevs":true}`
	if string(data) != want {
		t.Errorf("configuration %s, expected %s", data, want)
	}
	if _, err := newDriverConfigFromString("ignoreDevice: [mem0]"); err == nil {
		t.Error("an unknown key is accepted")
	}
}

func TestNewDriverConfigFromString_YAMLWithTestability(t *testing.T) {
	configYAML := `
testability:
  fakeRegionDevices:
    - Name: fakeregion0
      Size: 1073741824
      Mode: ram
      Node: 2
      Memories:
        - Name: fakemem0
          RamSize: 1073741824
          Serial: 12345
`
	cfg, err := newDriverConfigFromString(configYAML)
	if err != nil {
		t.Fatalf("unexpected error parsing YAML: %v", err)
	}
	if cfg.Testability == nil {
		t.Fatal("expected non-nil Testability")
	}
	if len(cfg.Testability.FakeRegionDevices) != 1 {
		t.Fatalf("expected 1 fake region, got %d", len(cfg.Testability.FakeRegionDevices))
	}
	reg := cfg.Testability.FakeRegionDevices[0]
	if reg.Name != "fakeregion0" {
		t.Errorf("expected name fakeregion0, got %s", reg.Name)
	}
	if reg.Size != 1073741824 {
		t.Errorf("expected size 1073741824, got %d", reg.Size)
	}
	if reg.Node != 2 {
		t.Errorf("expected node 2, got %d", reg.Node)
	}
	if len(reg.Memories) != 1 || reg.Memories[0].Serial != 12345 {
		t.Errorf("unexpected memory: %+v", reg.Memories)
	}
}

func TestResolveConfigStr_StringOnly(t *testing.T) {
	flags := &CXLFlags{ConfigStr: `{"ignoreDevices": ["mem0"]}`}
	result, err := resolveConfigStr(flags)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != flags.ConfigStr {
		t.Errorf("expected config string to be returned as-is, got %q", result)
	}
}

func TestResolveConfigStr_FileOnly(t *testing.T) {
	content := `{"ignoreDevices": ["mem1"]}`
	tmpFile := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	flags := &CXLFlags{ConfigFile: tmpFile}
	result, err := resolveConfigStr(flags)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != content {
		t.Errorf("expected %q, got %q", content, result)
	}
}

func TestResolveConfigStr_BothError(t *testing.T) {
	flags := &CXLFlags{
		ConfigStr:  `{"ignoreDevices": ["mem0"]}`,
		ConfigFile: "/some/file.json",
	}
	_, err := resolveConfigStr(flags)
	if err == nil {
		t.Fatal("expected error when both -c and -f are provided")
	}
	if !strings.Contains(err.Error(), "not both") {
		t.Errorf("expected error about mutual exclusivity, got: %v", err)
	}
}

func TestResolveConfigStr_NeitherReturnsEmpty(t *testing.T) {
	flags := &CXLFlags{}
	result, err := resolveConfigStr(flags)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "" {
		t.Errorf("expected empty string, got %q", result)
	}
}

func TestResolveConfigStr_FileNotFound(t *testing.T) {
	flags := &CXLFlags{ConfigFile: "/nonexistent/path/config.yaml"}
	_, err := resolveConfigStr(flags)
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestResolveConfigStr_YAMLFile(t *testing.T) {
	content := "ignoreDevices:\n  - mem0\nignoreDRAMNodes: true\n"
	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	flags := &CXLFlags{ConfigFile: tmpFile}
	result, err := resolveConfigStr(flags)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cfg, err := newDriverConfigFromString(result)
	if err != nil {
		t.Fatalf("failed to parse config from file: %v", err)
	}
	if len(cfg.IgnoreDevices) != 1 || cfg.IgnoreDevices[0] != "mem0" {
		t.Errorf("expected IgnoreDevices [mem0], got %v", cfg.IgnoreDevices)
	}
	if !cfg.IgnoreDRAMNodes {
		t.Error("expected IgnoreDRAMNodes=true")
	}
}

func TestDriverConfigManageMemdevs(t *testing.T) {
	for _, tc := range []struct {
		config string
		want   bool
	}{
		{"{}", true},
		{"manageMemdevs: true", true},
		{"manageMemdevs: false", false},
		{`{"manageMemdevs": false, "ignoreDevices": ["mem0"]}`, false},
	} {
		cfg, err := newDriverConfigFromString(tc.config)
		if err != nil {
			t.Fatalf("%s: %v", tc.config, err)
		}
		if got := cfg.manageMemdevs(); got != tc.want {
			t.Errorf("%s: manageMemdevs %v, expected %v", tc.config, got, tc.want)
		}
	}
}

func TestNewLocalHelper(t *testing.T) {
	dir := t.TempDir()
	config := &helpers.Config{CommonFlags: &helpers.Flags{KubeletPluginDir: dir}}
	off := &DriverConfig{ManageMemdevs: ptr(false)}
	if local, err := newLocalHelper(context.Background(), config, off, dir); err != nil || local != nil {
		t.Fatalf("unmanaged memdevs: %v %v", local, err)
	}
	// the state of a memdev that is gone is dropped at start
	state := `[{"serial":3238060769,"serialHex":"c100e2e1","memdev":"mem0","region":"region0","node":2,"claims":{"u1":[{"request":"mem","region":"region0","mode":"numa","size":2097152,"dax":"dax0.1"}]}}]`
	if err := os.WriteFile(filepath.Join(dir, localStateFileName), []byte(state), 0600); err != nil {
		t.Fatal(err)
	}
	local, err := newLocalHelper(context.Background(), config, &DriverConfig{}, dir)
	if err != nil || local == nil {
		t.Fatalf("local helper: %v %v", local, err)
	}
	if m := local.Snapshot().Serials; len(m) != 0 {
		t.Fatalf("serials %v of a gone memdev", m)
	}
	if local.mem.SysRoot != dir+"/sys" || local.stateFile != filepath.Join(dir, localStateFileName) {
		t.Fatalf("local helper %+v", local)
	}
	if _, err := os.Stat(filepath.Join(dir, preparedClaimsInfoFileName)); err != nil {
		t.Fatal(err)
	}
}

// Before the kubelet plugin runs, a rescan updates the devices but does not
// publish them (there is no helper yet).
func TestRescanBeforeStart(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"sys/bus/cxl/devices", "sys/devices/system/node"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	d := &driver{
		sysfsRoot: root,
		config:    &DriverConfig{IgnoreDRAMNodes: true, PublishLeaseDevice: true},
		state:     nodeState{NodeState: &helpers.NodeState{NodeName: "n1"}},
	}
	d.rescanAndPublish(context.Background())
	if devs, ok := d.state.Allocatable.(device.DevicesInfo); !ok || devs[device.LeaseDeviceName] == nil {
		t.Fatalf("devices after the rescan: %+v", d.state.Allocatable)
	}
}
