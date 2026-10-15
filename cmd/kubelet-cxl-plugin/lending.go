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
	"reflect"
	"slices"
	"strings"
	"sync"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/klog/v2"
	cdiSpecs "tags.cncf.io/container-device-interface/specs-go"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/api"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
)

// Lending of companion claims, shared by the pool driver and the
// cxl.generic driver: a companion claim carries the label
// cxl.generic/lease=<id> of a lease of the broker, whose running
// container borrows the memory of the claim's devices (doc/cxl/LEASES.md).

// lentDevice is one prepared device of a companion claim, as the lease
// broker needs it.
type lentDevice struct {
	Driver    string // DRA driver that prepared the device
	Source    string // api.SourcePool or api.SourceLocal
	Request   string
	Device    string
	Pool      string
	Serial    uint64
	Shared    bool
	Size      uint64 // bytes the lease gets: the slice size or the device size
	Mode      string // api.ModeNUMA or api.ModeDax
	ReadOnly  bool
	Node      int    // numa: NUMA node of the memory, else -1
	DaxDevice string // dax: /dev/daxX.Y
}

// leaseHooks is what a driver tells the lease broker about companion
// claims. The hooks are called with the driver's lock held and must not
// call back into the driver.
type leaseHooks interface {
	// OnLending checks that the claim is the companion of the lease,
	// marks the lease Lending and returns the lend of the lease's
	// container (without nodes). An error fails the Prepare.
	OnLending(id string, claim *resourceapi.ResourceClaim) (*lendRecord, error)
	// OnPrepared marks the lease Ready with the prepared devices.
	OnPrepared(id string, devs []lentDevice)
	// OnLendFailed records a lend error (kubelet retries the Prepare).
	OnLendFailed(id string, err error)
	// OnReleasing is called when the companion claim is unprepared.
	OnReleasing(id string)
}

// lendRecord is the lend of a prepared companion claim, persisted with the
// claim so that Unprepare can revoke it after a plugin restart.
type lendRecord struct {
	Lease     string `json:"lease"`
	Pod       podRef `json:"pod"`
	Container string `json:"container"`
	Nodes     []int  `json:"nodes,omitempty"` // lent NUMA nodes
}

// companionLender lends the companion claims of one driver to running
// containers. A nil or zero companionLender has lending disabled.
type companionLender struct {
	driver string
	mu     sync.Mutex
	nodes  nodeLender // nil: NRI not connected
	hooks  leaseHooks // nil: lending disabled
}

// newCompanionLender returns a companionLender for the companion claims of
// driver, with lending disabled until set is called.
func newCompanionLender(driver string) *companionLender {
	return &companionLender{driver: driver}
}

// set enables lending: nodes lends NUMA nodes (nil when NRI is not
// connected, then companion claims fail), hooks is the lease broker (nil
// disables lending).
func (c *companionLender) set(nodes nodeLender, hooks leaseHooks) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes = nodes
	c.hooks = hooks
}

// get returns the node lender and the hooks.
func (c *companionLender) get() (nodeLender, leaseHooks) {
	if c == nil {
		return nil, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes, c.hooks
}

// leaseOf returns the lease id of a companion claim (its label
// device.LeaseLabel), "" for other claims.
func leaseOf(claim *resourceapi.ResourceClaim) string {
	return claim.Labels[device.LeaseLabel]
}

// check checks that a companion claim can be lent, before any device is
// touched, marks its lease Lending and returns the lend of the lease's
// container.
func (c *companionLender) check(id string, claim *resourceapi.ResourceClaim) (*lendRecord, error) {
	nodes, hooks := c.get()
	if hooks == nil {
		return nil, fmt.Errorf("lease %s: lending to running containers is disabled (--leases=false)", id)
	}
	if nodes == nil {
		return nil, fmt.Errorf("lease %s: %w", id, errLendingUnavailable)
	}
	return hooks.OnLending(id, claim)
}

// lend lends the NUMA nodes of the numa devices of a companion claim to
// the container of the lend, held by its lease, and records them in rec.
// Dax devices are passed as an fd by the broker. On error rec lists the
// nodes lent so far.
func (c *companionLender) lend(ctx context.Context, rec *lendRecord, devs []lentDevice) error {
	nodes, _ := c.get()
	for _, d := range devs {
		if d.Mode != api.ModeNUMA || d.Node < 0 {
			continue
		}
		if nodes == nil {
			return fmt.Errorf("lease %s: %w", rec.Lease, errLendingUnavailable)
		}
		rec.Nodes = append(rec.Nodes, d.Node)
		if err := nodes.LendNode(ctx, rec.Pod.UID, rec.Container, d.Node, rec.Lease); err != nil {
			return fmt.Errorf("lease %s: lending node %d to %s/%s/%s: %w", rec.Lease, d.Node, rec.Pod.Namespace, rec.Pod.Name, rec.Container, err)
		}
		klog.Infof("%s: lease %s: lent node %d to %s/%s/%s", c.driver, rec.Lease, d.Node, rec.Pod.Namespace, rec.Pod.Name, rec.Container)
	}
	return nil
}

// revoke revokes the lent nodes of a lend for its lease: a node that
// another lease of the container holds stays lent. Without NRI the nodes
// cannot be revoked: the release then migrates the pages anyway, but the
// stale node stays in the container's cpuset.mems.
func (c *companionLender) revoke(ctx context.Context, rec *lendRecord, claimName string) error {
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

// prepared tells the broker that the companion claim of a lease is
// prepared and lent.
func (c *companionLender) prepared(id string, devs []lentDevice) {
	if _, hooks := c.get(); hooks != nil {
		hooks.OnPrepared(id, devs)
	}
}

// reprepared tells the broker again that a stored companion claim is
// prepared and lent, as kubelet asks to prepare a prepared claim again
// after a plugin restart.
func (c *companionLender) reprepared(rec *lendRecord, claim *resourceapi.ResourceClaim, devs []lentDevice) {
	_, hooks := c.get()
	if hooks == nil || rec == nil {
		return
	}
	if _, err := hooks.OnLending(rec.Lease, claim); err == nil {
		hooks.OnPrepared(rec.Lease, devs)
	}
}

// failed tells the broker that lending the companion claim of a lease
// failed.
func (c *companionLender) failed(id string, err error) {
	if _, hooks := c.get(); hooks != nil {
		hooks.OnLendFailed(id, err)
	}
}

// releasing tells the broker that the companion claim of a lease is
// being unprepared.
func (c *companionLender) releasing(id string) {
	if _, hooks := c.get(); hooks != nil {
		hooks.OnReleasing(id)
	}
}

// name returns the driver name for logs.
func (c *companionLender) name() string {
	if c == nil {
		return ""
	}
	return c.driver
}

// deviceConfig is the opaque configuration of kind DeviceConfig of how a
// device is prepared (apiVersion <driver>/v1alpha1).
type deviceConfig struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	// Mode is "numa" or "dax"; "" is numa for exclusive devices and dax
	// for shared devices.
	Mode string `json:"mode,omitempty"`
	// ReadOnly makes the dax device node of the container read-only.
	ReadOnly bool `json:"readOnly,omitempty"`
}

// parseConfig decodes into out the opaque config of kind for driver that
// applies to an allocation result's request, and reports whether there is
// one: class configs first, then claim configs; a later one replaces an
// earlier one. With a request, a config applies if it lists the request
// (a request "r" also covers its subrequests "r/s") or lists none; with
// an empty request every config of the kind applies. Configs are decoded
// strictly and need apiVersion <driver>/v1alpha1.
//   - out: a pointer to the struct of the kind, zeroed before each config
func parseConfig(claim *resourceapi.ResourceClaim, driver, kind, request string, out any) (bool, error) {
	parent, _, _ := strings.Cut(request, "/")
	found := false
	for _, source := range []resourceapi.AllocationConfigSource{resourceapi.AllocationConfigSourceClass, resourceapi.AllocationConfigSourceClaim} {
		for _, c := range claim.Status.Allocation.Devices.Config {
			if c.Source != source || c.Opaque == nil || c.Opaque.Driver != driver {
				continue
			}
			if request != "" && len(c.Requests) > 0 && !slices.Contains(c.Requests, request) && !slices.Contains(c.Requests, parent) {
				continue
			}
			var head struct {
				APIVersion string `json:"apiVersion"`
				Kind       string `json:"kind"`
			}
			if err := json.Unmarshal(c.Opaque.Parameters.Raw, &head); err != nil {
				return false, fmt.Errorf("invalid opaque %s config for %s: %w", source, driver, err)
			}
			if head.Kind != kind {
				continue
			}
			if head.APIVersion != driver+"/v1alpha1" {
				return false, fmt.Errorf("%s: apiVersion %q, expected %q", kind, head.APIVersion, driver+"/v1alpha1")
			}
			reflect.ValueOf(out).Elem().SetZero()
			dec := json.NewDecoder(bytes.NewReader(c.Opaque.Parameters.Raw))
			dec.DisallowUnknownFields()
			if err := dec.Decode(out); err != nil {
				return false, fmt.Errorf("%s: invalid %s config: %w", kind, source, err)
			}
			found = true
		}
	}
	return found, nil
}

// checkConfigKinds returns an error if an opaque config of driver in the
// allocation of a claim has a kind that is not one of kinds.
func checkConfigKinds(claim *resourceapi.ResourceClaim, driver string, kinds ...string) error {
	for _, c := range claim.Status.Allocation.Devices.Config {
		if c.Opaque == nil || c.Opaque.Driver != driver {
			continue
		}
		var head struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(c.Opaque.Parameters.Raw, &head); err != nil {
			return fmt.Errorf("invalid opaque %s config for %s: %w", c.Source, driver, err)
		}
		if !slices.Contains(kinds, head.Kind) {
			return fmt.Errorf("unknown kind %q (%s)", head.Kind, strings.Join(kinds, " or "))
		}
	}
	return nil
}

// numaOnly returns an error if the config asks a device that is used as
// numa only (what: region or dram) for another mode or for readOnly.
func (cfg *deviceConfig) numaOnly(what string) error {
	mode, err := cfg.modeFor(false)
	if err == nil && mode != api.ModeNUMA {
		err = fmt.Errorf("%s: mode %s: a %s is used as %s only", device.DeviceConfigKind, mode, what, api.ModeNUMA)
	}
	return err
}

// modeFor returns the mode of a device with the config: shared devices are
// dax only (two kernels must never online the same memory), and read-only
// is a property of a dax device node.
func (cfg *deviceConfig) modeFor(shared bool) (string, error) {
	mode := cfg.Mode
	switch {
	case mode == "" && shared:
		mode = api.ModeDax
	case mode == "":
		mode = api.ModeNUMA
	case mode != api.ModeNUMA && mode != api.ModeDax:
		return "", fmt.Errorf("%s: invalid mode %q (%s or %s)", device.DeviceConfigKind, cfg.Mode, api.ModeNUMA, api.ModeDax)
	}
	switch {
	case shared && mode != api.ModeDax:
		return "", fmt.Errorf("%s: mode %s: a shared device is used as %s only", device.DeviceConfigKind, mode, api.ModeDax)
	case cfg.ReadOnly && mode != api.ModeDax:
		return "", fmt.Errorf("%s: readOnly needs mode %s", device.DeviceConfigKind, api.ModeDax)
	}
	return mode, nil
}

// daxDeviceNode returns the CDI device node of a devdax device. A
// read-only node has permissions "r": the runtime turns that into a
// device cgroup rule that refuses opens for writing, whatever the
// container's capabilities.
func daxDeviceNode(path string, major, minor int, readOnly bool) *cdiSpecs.DeviceNode {
	node := &cdiSpecs.DeviceNode{
		Path:  path,
		Type:  "c",
		Major: int64(major),
		Minor: int64(minor),
	}
	if readOnly {
		node.Permissions = "r"
	}
	return node
}

// envKeys counts the memories of each base env key in one claim.
type envKeys map[string]int

// next returns the env key of the next memory with the base key: the
// base key for the first one, then "<base>_<n>" (api.Nth).
func (k envKeys) next(base string) string {
	k[base]++
	return api.Nth(base, k[base])
}
