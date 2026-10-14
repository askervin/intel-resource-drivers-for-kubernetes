/*
 * Copyright (c) 2026, Intel Corporation.  All Rights Reserved.
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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/klog/v2"
	cdiSpecs "tags.cncf.io/container-device-interface/specs-go"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
)

// Lending of companion claims, shared by the pool helper and the
// cxl.generic driver: a companion claim carries a LendConfig that names a
// lease of the broker and the running container that borrows the memory
// of the claim's devices (doc/cxl/POOL.md, "Lending to running
// containers").

// lentDevice is one prepared device of a companion claim, as the lease
// broker needs it.
type lentDevice struct {
	Driver    string // DRA driver that prepared the device
	Source    string // device.SourcePool or device.SourceLocal
	Request   string
	Device    string
	Pool      string
	Serial    uint64
	SerialHex string // lower-case hex, no 0x
	Shared    bool
	Size      uint64 // bytes the lease gets: the slice size or the device size
	Mode      string // device.ModeNUMA or device.ModeDax
	ReadOnly  bool
	Node      int    // numa: NUMA node of the memory, else -1
	DaxDevice string // dax: /dev/daxX.Y
}

// leaseHooks is what a driver tells the lease broker about companion
// claims. The hooks are called with the driver's lock held and must not
// call back into the driver.
type leaseHooks interface {
	// OnLending validates the LendConfig against the broker's lease and
	// marks the lease Lending. An error fails the Prepare.
	OnLending(cfg *lendConfig) error
	// OnPrepared marks the lease Ready with the prepared devices.
	OnPrepared(cfg *lendConfig, devs []lentDevice)
	// OnLendFailed records a lend error (kubelet retries the Prepare).
	OnLendFailed(cfg *lendConfig, err error)
	// OnReleasing is called when the companion claim is unprepared.
	OnReleasing(lease string)
}

// lendRecord is the lend of a prepared companion claim, persisted with the
// claim so that Unprepare can revoke it after a plugin restart.
type lendRecord struct {
	Lease     string `json:"lease"`
	Pod       podRef `json:"pod"`
	Container string `json:"container"`
	Nodes     []int  `json:"nodes,omitempty"` // lent NUMA nodes
}

// config returns the LendConfig of a stored lend.
func (l *lendRecord) config() *lendConfig {
	return &lendConfig{Kind: device.LendConfigKind, Lease: l.Lease, Pod: l.Pod, Container: l.Container}
}

// claimLender lends the companion claims of one driver to running
// containers. A nil or zero claimLender has lending disabled.
type claimLender struct {
	driver string
	mu     sync.Mutex
	nodes  Lender     // nil: NRI not connected
	hooks  leaseHooks // nil: lending disabled
}

// newClaimLender returns a claimLender for the companion claims of driver,
// with lending disabled until set is called.
func newClaimLender(driver string) *claimLender {
	return &claimLender{driver: driver}
}

// set enables lending: nodes lends NUMA nodes (nil when NRI is not
// connected, then companion claims fail), hooks is the lease broker (nil
// disables lending).
func (c *claimLender) set(nodes Lender, hooks leaseHooks) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes = nodes
	c.hooks = hooks
}

// get returns the node lender and the hooks.
func (c *claimLender) get() (Lender, leaseHooks) {
	if c == nil {
		return nil, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes, c.hooks
}

// enabled tells if lending to running containers is enabled.
func (c *claimLender) enabled() bool {
	_, hooks := c.get()
	return hooks != nil
}

// parse returns the LendConfig of a companion claim of the driver, nil if
// the claim has none.
func (c *claimLender) parse(claim *resourceapi.ResourceClaim) (*lendConfig, error) {
	if c == nil {
		return nil, errors.New("no lender")
	}
	return parseLendConfig(claim, c.driver)
}

// check checks that a companion claim can be lent, before any device is
// touched, and marks its lease Lending.
func (c *claimLender) check(cfg *lendConfig) error {
	nodes, hooks := c.get()
	if hooks == nil {
		return fmt.Errorf("lease %s: lending to running containers is disabled", cfg.Lease)
	}
	if nodes == nil {
		return fmt.Errorf("lease %s: %w", cfg.Lease, errLendingUnavailable)
	}
	return hooks.OnLending(cfg)
}

// lend lends the NUMA nodes of the numa devices of a companion claim to
// the requesting container, held by the lease. Dax devices are passed as
// an fd by the broker. The returned record lists the nodes lent so far,
// also on error.
func (c *claimLender) lend(ctx context.Context, cfg *lendConfig, devs []lentDevice) (*lendRecord, error) {
	rec := &lendRecord{Lease: cfg.Lease, Pod: cfg.Pod, Container: cfg.Container}
	nodes, _ := c.get()
	for _, d := range devs {
		if d.Mode != device.ModeNUMA || d.Node < 0 {
			continue
		}
		if nodes == nil {
			return rec, fmt.Errorf("lease %s: %w", cfg.Lease, errLendingUnavailable)
		}
		rec.Nodes = append(rec.Nodes, d.Node)
		if err := nodes.LendNode(ctx, cfg.Pod.UID, cfg.Container, d.Node, cfg.Lease); err != nil {
			return rec, fmt.Errorf("lease %s: lending node %d to %s/%s/%s: %w", cfg.Lease, d.Node, cfg.Pod.Namespace, cfg.Pod.Name, cfg.Container, err)
		}
		klog.Infof("%s: lease %s: lent node %d to %s/%s/%s", c.driver, cfg.Lease, d.Node, cfg.Pod.Namespace, cfg.Pod.Name, cfg.Container)
	}
	return rec, nil
}

// revoke revokes the lent nodes of a lend for its lease: a node that
// another lease of the container holds stays lent. Without NRI the nodes
// cannot be revoked: the release then migrates the pages anyway, but the
// stale node stays in the container's cpuset.mems.
func (c *claimLender) revoke(ctx context.Context, rec *lendRecord, claimName string) error {
	if rec == nil || len(rec.Nodes) == 0 {
		return nil
	}
	nodes, _ := c.get()
	if nodes == nil {
		klog.Warningf("%s: claim %s: lease %s: %v, cannot revoke nodes %v from %s/%s/%s",
			c.name(), claimName, rec.Lease, errLendingUnavailable, rec.Nodes, rec.Pod.Namespace, rec.Pod.Name, rec.Container)
		return nil
	}
	var errs []error
	for _, n := range rec.Nodes {
		if err := nodes.RevokeNode(ctx, rec.Pod.UID, rec.Container, n, rec.Lease); err != nil {
			errs = append(errs, fmt.Errorf("lease %s: revoking node %d from %s/%s/%s: %w", rec.Lease, n, rec.Pod.Namespace, rec.Pod.Name, rec.Container, err))
		}
	}
	return errors.Join(errs...)
}

// prepared tells the broker that a companion claim is prepared and lent.
func (c *claimLender) prepared(cfg *lendConfig, devs []lentDevice) {
	if _, hooks := c.get(); hooks != nil {
		hooks.OnPrepared(cfg, devs)
	}
}

// reprepared tells the broker again that a stored companion claim is
// prepared and lent, as kubelet asks to prepare a prepared claim again
// after a plugin restart.
func (c *claimLender) reprepared(rec *lendRecord, devs []lentDevice) {
	_, hooks := c.get()
	if hooks == nil || rec == nil {
		return
	}
	cfg := rec.config()
	if err := hooks.OnLending(cfg); err == nil {
		hooks.OnPrepared(cfg, devs)
	}
}

// failed tells the broker that lending a companion claim failed.
func (c *claimLender) failed(cfg *lendConfig, err error) {
	if _, hooks := c.get(); hooks != nil {
		hooks.OnLendFailed(cfg, err)
	}
}

// releasing tells the broker that the companion claim of a lease is
// being unprepared.
func (c *claimLender) releasing(lease string) {
	if _, hooks := c.get(); hooks != nil {
		hooks.OnReleasing(lease)
	}
}

// name returns the driver name for logs.
func (c *claimLender) name() string {
	if c == nil {
		return ""
	}
	return c.driver
}

// parseLendConfig returns the LendConfig of a companion claim, nil if the
// claim has none. Only claim-level config counts: a class must not lend.
func parseLendConfig(claim *resourceapi.ResourceClaim, driver string) (*lendConfig, error) {
	var found *lendConfig
	for _, c := range claim.Status.Allocation.Devices.Config {
		if c.Opaque == nil || c.Opaque.Driver != driver {
			continue
		}
		var cfg lendConfig
		if err := json.Unmarshal(c.Opaque.Parameters.Raw, &cfg); err != nil {
			return nil, fmt.Errorf("invalid opaque config for %s: %w", driver, err)
		}
		if cfg.Kind != device.LendConfigKind {
			continue
		}
		switch {
		case c.Source != resourceapi.AllocationConfigSourceClaim:
			return nil, fmt.Errorf("%s from %s config is not allowed, only from the claim", device.LendConfigKind, c.Source)
		case cfg.APIVersion != driver+"/v1alpha1":
			return nil, fmt.Errorf("%s: apiVersion %q, expected %q", device.LendConfigKind, cfg.APIVersion, driver+"/v1alpha1")
		case cfg.Lease == "" || cfg.Container == "" || cfg.Pod.UID == "" || cfg.Pod.Name == "":
			return nil, fmt.Errorf("%s: lease, pod.name, pod.uid and container are required", device.LendConfigKind)
		case cfg.Pod.Namespace != claim.Namespace:
			return nil, fmt.Errorf("%s: pod namespace %q is not the claim's namespace %q", device.LendConfigKind, cfg.Pod.Namespace, claim.Namespace)
		case found != nil:
			return nil, fmt.Errorf("more than one %s", device.LendConfigKind)
		}
		found = &cfg
	}
	return found, nil
}

// deviceConfig is the opaque configuration of how a device is prepared:
// kind PoolDeviceConfig of pool devices, LocalDeviceConfig of local
// memdevs (apiVersion <driver>/v1alpha1).
type deviceConfig struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	// Mode is "numa" or "dax"; "" is numa for exclusive devices and dax
	// for shared devices.
	Mode string `json:"mode,omitempty"`
	// ReadOnly makes the dax device node of the container read-only.
	ReadOnly bool `json:"readOnly,omitempty"`
}

// parseDeviceConfig returns the config of kind for driver that applies to
// an allocation result's request: class configs first, then claim configs;
// a later one replaces an earlier one. A config applies to the requests it
// lists (a request "r" also covers its subrequests "r/s"), or to all
// requests if it lists none. Without one it returns the zero config.
func parseDeviceConfig(claim *resourceapi.ResourceClaim, driver, kind, request string) (*deviceConfig, error) {
	parent, _, _ := strings.Cut(request, "/")
	found := &deviceConfig{}
	for _, source := range []resourceapi.AllocationConfigSource{resourceapi.AllocationConfigSourceClass, resourceapi.AllocationConfigSourceClaim} {
		for _, c := range claim.Status.Allocation.Devices.Config {
			if c.Source != source || c.Opaque == nil || c.Opaque.Driver != driver {
				continue
			}
			if len(c.Requests) > 0 && !slices.Contains(c.Requests, request) && !slices.Contains(c.Requests, parent) {
				continue
			}
			var head struct {
				Kind string `json:"kind"`
			}
			if err := json.Unmarshal(c.Opaque.Parameters.Raw, &head); err != nil {
				return nil, fmt.Errorf("invalid opaque config for %s: %w", driver, err)
			}
			if head.Kind != kind {
				continue
			}
			cfg := &deviceConfig{}
			dec := json.NewDecoder(bytes.NewReader(c.Opaque.Parameters.Raw))
			dec.DisallowUnknownFields()
			if err := dec.Decode(cfg); err != nil {
				return nil, fmt.Errorf("invalid %s from %s config: %w", kind, source, err)
			}
			if cfg.APIVersion != driver+"/v1alpha1" {
				return nil, fmt.Errorf("%s: apiVersion %q, expected %q", kind, cfg.APIVersion, driver+"/v1alpha1")
			}
			found = cfg
		}
	}
	return found, nil
}

// modeFor returns the mode of a device with the config: shared devices are
// dax only (two kernels must never online the same memory), and read-only
// is a property of a dax device node.
func (cfg *deviceConfig) modeFor(shared bool) (string, error) {
	mode := cfg.Mode
	switch {
	case mode == "" && shared:
		mode = device.ModeDax
	case mode == "":
		mode = device.ModeNUMA
	case mode != device.ModeNUMA && mode != device.ModeDax:
		return "", fmt.Errorf("%s: invalid mode %q (%s or %s)", cfg.Kind, cfg.Mode, device.ModeNUMA, device.ModeDax)
	}
	switch {
	case shared && mode != device.ModeDax:
		return "", fmt.Errorf("%s: mode %s: a shared device is used as %s only", cfg.Kind, mode, device.ModeDax)
	case cfg.ReadOnly && mode != device.ModeDax:
		return "", fmt.Errorf("%s: readOnly needs mode %s", cfg.Kind, device.ModeDax)
	}
	return mode, nil
}

// daxCDIDevice returns the CDI device of a devdax device: the device node
// and the env <envPrefix>DAX_<SERIAL>, <envPrefix>SIZE_<SERIAL> and
// <envPrefix>SERIAL_<SERIAL> (<SERIAL> is the upper-case serialHex), and
// <envPrefix>READONLY_<SERIAL>=1 when readOnly. A read-only device node
// has permissions "r": the runtime turns that into a device cgroup rule
// that refuses opens for writing, whatever the container's capabilities.
func daxCDIDevice(name, envPrefix, serialHex, daxDevice string, major, minor int, size uint64, readOnly bool) cdiSpecs.Device {
	x := strings.ToUpper(serialHex)
	edits := cdiSpecs.ContainerEdits{
		Env: []string{
			envPrefix + "DAX_" + x + "=" + daxDevice,
			envPrefix + "SIZE_" + x + "=" + strconv.FormatUint(size, 10),
			envPrefix + "SERIAL_" + x + "=0x" + serialHex,
		},
	}
	node := &cdiSpecs.DeviceNode{
		Path:  daxDevice,
		Type:  "c",
		Major: int64(major),
		Minor: int64(minor),
	}
	if readOnly {
		edits.Env = append(edits.Env, envPrefix+"READONLY_"+x+"=1")
		node.Permissions = "r"
	}
	edits.DeviceNodes = []*cdiSpecs.DeviceNode{node}
	return cdiSpecs.Device{Name: name, ContainerEdits: edits}
}
