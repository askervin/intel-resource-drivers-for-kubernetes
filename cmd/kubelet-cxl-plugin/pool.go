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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/klog/v2"
	cdiapi "tags.cncf.io/container-device-interface/pkg/cdi"
	cdiparser "tags.cncf.io/container-device-interface/pkg/parser"
	cdiSpecs "tags.cncf.io/container-device-interface/specs-go"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"

	"github.com/containers/nri-plugins/pkg/cxl/memctl"
)

// The pool helper is a second DRA kubelet plugin in this process. It
// prepares CXL memory devices that a pool controller has hotplugged into
// this node for a claim (contract: doc/cxl/POOL.md): it finds the memdev
// by the serial that the controller wrote into the claim status, creates
// a region on it and returns CDI edits; Unprepare releases the device so
// that the pool can hot-remove it. It never publishes ResourceSlices.

const (
	// poolStateFileName is the prepared pool claims file in the plugin
	// data directory of the pool helper.
	poolStateFileName = "preparedPoolClaims.json"
	// defaultPoolPrepareTimeout is how long Prepare waits for the
	// memdev to appear.
	defaultPoolPrepareTimeout = 30 * time.Second
	// defaultMemdevProbeGrace is how long Prepare waits for the driver
	// of a memdev that is already present. A memdev disabled by an
	// earlier release is enabled again by memctl.CreateRegion.
	defaultMemdevProbeGrace = 2 * time.Second
)

type poolDriver struct {
	name       string // cxl-pool.generic
	helper     *kubeletplugin.Helper
	mem        *memctl.Manager
	cdi        *cdiapi.Cache // the shared default cache, nil: no CDI spec is written
	stateFile  string        // <plugin data dir>/preparedPoolClaims.json
	timeout    time.Duration
	probeGrace time.Duration
	mu         sync.Mutex
	prepared   map[string]*preparedPoolClaim // claim uid -> ...
	inflight   map[uint64]int                // serials being prepared -> count

	// serials is a snapshot of the serials of prepared and inflight
	// devices for the cxl.generic publisher (D29). It has its own lock
	// so that a rescan never waits for a Prepare that waits for udev.
	serialsMu sync.Mutex
	serials   map[uint64]bool
}

type preparedPoolDevice struct {
	Request     string `json:"request"`
	Device      string `json:"device"`
	Pool        string `json:"pool"`
	Serial      uint64 `json:"serial"`
	SerialHex   string `json:"serialHex"`
	Shared      bool   `json:"shared"`
	Size        uint64 `json:"size"`
	Memdev      string `json:"memdev"`
	Region      string `json:"region"`
	Dax         string `json:"dax"`
	DaxDevice   string `json:"daxDevice,omitempty"` // /dev/daxX.Y (shared)
	DaxMajor    int    `json:"daxMajor,omitempty"`
	DaxMinor    int    `json:"daxMinor,omitempty"`
	Node        int    `json:"node"` // NUMA node of the memory (exclusive), -1 if none
	CDIDeviceID string `json:"cdiDeviceID"`
}

type preparedPoolClaim struct {
	ClaimName string                      `json:"claimName"` // namespace/name
	Devices   []preparedPoolDevice        `json:"devices"`
	Result    kubeletplugin.PrepareResult `json:"result"` // what Prepare returned
}

// poolDeviceData is claim.status.devices[].data written by the pool
// controller. Serial, shared and size are required.
type poolDeviceData struct {
	Serial string  `json:"serial"`
	Shared *bool   `json:"shared"`
	Size   *uint64 `json:"size"`
}

// poolDevice is the parsed poolDeviceData of an allocation result.
type poolDevice struct {
	Serial uint64
	Shared bool
	Size   uint64
}

// newPoolDriver returns a pool helper whose prepared claims are loaded
// from stateFile. The kubelet plugin is started separately (start).
func newPoolDriver(name string, mem *memctl.Manager, stateFile string, timeout time.Duration) (*poolDriver, error) {
	if timeout <= 0 {
		timeout = defaultPoolPrepareTimeout
	}
	p := &poolDriver{
		name:       name,
		mem:        mem,
		stateFile:  stateFile,
		timeout:    timeout,
		probeGrace: defaultMemdevProbeGrace,
		prepared:   map[string]*preparedPoolClaim{},
		inflight:   map[uint64]int{},
	}
	if err := p.load(); err != nil {
		return nil, err
	}
	p.updateSerials()
	return p, nil
}

// start sets the CDI cache, rewrites the CDI spec of the loaded claims
// and starts the kubelet plugin.
func (p *poolDriver) start(ctx context.Context, cdi *cdiapi.Cache, opts ...kubeletplugin.Option) error {
	p.mu.Lock()
	p.cdi = cdi
	err := p.writeCDISpec()
	p.mu.Unlock()
	if err != nil {
		return err
	}
	helper, err := kubeletplugin.Start(ctx, p, opts...)
	if err != nil {
		return fmt.Errorf("failed to start kubelet plugin %s: %w", p.name, err)
	}
	p.helper = helper
	klog.Infof("pool helper registered as %s", p.name)
	return nil
}

func (p *poolDriver) stop() {
	if p != nil && p.helper != nil {
		p.helper.Stop()
	}
}

// Serials returns the serials of the memdevs that the pool helper has
// prepared or is preparing. Nil for a nil (disabled) pool helper.
func (p *poolDriver) Serials() map[uint64]bool {
	if p == nil {
		return nil
	}
	p.serialsMu.Lock()
	defer p.serialsMu.Unlock()
	out := make(map[uint64]bool, len(p.serials))
	for s := range p.serials {
		out[s] = true
	}
	return out
}

// updateSerials refreshes the Serials snapshot. Called with p.mu held.
func (p *poolDriver) updateSerials() {
	s := map[uint64]bool{}
	for _, pc := range p.prepared {
		for _, d := range pc.Devices {
			s[d.Serial] = true
		}
	}
	for serial := range p.inflight {
		s[serial] = true
	}
	p.serialsMu.Lock()
	p.serials = s
	p.serialsMu.Unlock()
}

func (p *poolDriver) PrepareResourceClaims(ctx context.Context, claims []*resourceapi.ResourceClaim) (map[types.UID]kubeletplugin.PrepareResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	response := map[types.UID]kubeletplugin.PrepareResult{}
	for _, claim := range claims {
		response[claim.UID] = p.prepareClaim(ctx, claim)
	}
	return response, nil
}

func (p *poolDriver) prepareClaim(ctx context.Context, claim *resourceapi.ResourceClaim) kubeletplugin.PrepareResult {
	uid := string(claim.UID)
	if pc, ok := p.prepared[uid]; ok {
		klog.V(3).Infof("%s: claim %s (%s) was already prepared", p.name, pc.ClaimName, uid)
		return pc.Result
	}
	claimName := claim.Namespace + "/" + claim.Name
	if claim.Status.Allocation == nil {
		return kubeletplugin.PrepareResult{Err: fmt.Errorf("claim %s has no allocation", claimName)}
	}
	pc := &preparedPoolClaim{ClaimName: claimName, Result: kubeletplugin.PrepareResult{Devices: []kubeletplugin.Device{}}}
	// Serials touched by this Prepare stay inflight (hidden from the
	// cxl.generic publisher) until the claim is prepared or rolled back.
	var touched []uint64
	defer func() {
		for _, serial := range touched {
			if p.inflight[serial]--; p.inflight[serial] <= 0 {
				delete(p.inflight, serial)
			}
		}
		p.updateSerials()
	}()
	fail := func(err error) kubeletplugin.PrepareResult {
		err = fmt.Errorf("%s: preparing claim %s: %w", p.name, claimName, err)
		klog.Error(err)
		p.rollback(ctx, uid, touched)
		return kubeletplugin.PrepareResult{Err: err}
	}
	for _, r := range claim.Status.Allocation.Devices.Results {
		if r.Driver != p.name {
			continue
		}
		data, err := poolDataFor(claim, &r)
		if err != nil {
			return fail(err)
		}
		if other := p.preparedDevice(data.Serial, uid); other != nil && other.Shared != data.Shared {
			return fail(fmt.Errorf("device %s serial 0x%x: shared=%v, but another claim prepared it with shared=%v",
				r.Device, data.Serial, data.Shared, other.Shared))
		}
		touched = append(touched, data.Serial)
		p.inflight[data.Serial]++
		p.updateSerials()
		dev, err := p.prepareDevice(ctx, &r, data)
		if err != nil {
			return fail(fmt.Errorf("device %s serial 0x%x: %w", r.Device, data.Serial, err))
		}
		pc.Devices = append(pc.Devices, *dev)
		pc.Result.Devices = append(pc.Result.Devices, kubeletplugin.Device{
			Requests:     []string{r.Request},
			PoolName:     r.Pool,
			DeviceName:   r.Device,
			CDIDeviceIDs: []string{dev.CDIDeviceID},
			ShareID:      r.ShareID,
		})
		klog.Infof("%s: claim %s: prepared %s (serial 0x%x, shared %v) as %s %s dax %s node %d",
			p.name, claimName, r.Device, data.Serial, data.Shared, dev.Memdev, dev.Region, dev.Dax, dev.Node)
	}
	p.prepared[uid] = pc
	if err := p.sync(); err != nil {
		delete(p.prepared, uid)
		res := fail(err)
		if err := p.sync(); err != nil {
			klog.Errorf("%s: %v", p.name, err)
		}
		return res
	}
	return pc.Result
}

// preparedDevice returns a device of a prepared claim other than
// exceptUID with the serial, or nil.
func (p *poolDriver) preparedDevice(serial uint64, exceptUID string) *preparedPoolDevice {
	for uid, pc := range p.prepared {
		if uid == exceptUID {
			continue
		}
		for i := range pc.Devices {
			if pc.Devices[i].Serial == serial {
				return &pc.Devices[i]
			}
		}
	}
	return nil
}

// rollback releases the serials that a failed Prepare touched, unless
// another prepared claim uses them.
func (p *poolDriver) rollback(ctx context.Context, uid string, serials []uint64) {
	done := map[uint64]bool{}
	for _, serial := range serials {
		if done[serial] || p.preparedDevice(serial, uid) != nil {
			continue
		}
		done[serial] = true
		if err := p.release(ctx, serial); err != nil {
			klog.Errorf("%s: rollback of serial 0x%x: %v", p.name, serial, err)
		}
	}
}

// poolDataFor parses the claim.status.devices[] entry of an allocation
// result (matched by driver, pool and device).
func poolDataFor(claim *resourceapi.ResourceClaim, r *resourceapi.DeviceRequestAllocationResult) (*poolDevice, error) {
	for _, st := range claim.Status.Devices {
		if st.Driver != r.Driver || st.Pool != r.Pool || st.Device != r.Device {
			continue
		}
		if st.Data == nil || len(st.Data.Raw) == 0 {
			return nil, fmt.Errorf("status of device %s/%s has no data (written by the pool controller with the Attached condition)", r.Pool, r.Device)
		}
		return parsePoolDeviceData(st.Data.Raw)
	}
	return nil, fmt.Errorf("claim status has no entry for device %s/%s (not attached by the pool controller yet?)", r.Pool, r.Device)
}

func parsePoolDeviceData(raw []byte) (*poolDevice, error) {
	var d poolDeviceData
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, fmt.Errorf("invalid device data %s: %w", string(raw), err)
	}
	if d.Serial == "" || d.Shared == nil || d.Size == nil {
		return nil, fmt.Errorf("device data %s: serial, shared and size are required", string(raw))
	}
	serial, err := strconv.ParseUint(d.Serial, 0, 64)
	if err != nil {
		return nil, fmt.Errorf("device data: invalid serial %q: %w", d.Serial, err)
	}
	return &poolDevice{Serial: serial, Shared: *d.Shared, Size: *d.Size}, nil
}

// waitMemdev returns the memdev with the serial, waiting up to the
// timeout for it to appear.
func (p *poolDriver) waitMemdev(ctx context.Context, serial uint64) (string, error) {
	if name, err := p.mem.FindMemdev(serial); err == nil {
		wctx, cancel := context.WithTimeout(ctx, p.probeGrace)
		defer cancel()
		if n, err := p.mem.WaitMemdev(wctx, serial); err == nil {
			return n, nil
		}
		return name, nil
	}
	wctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	return p.mem.WaitMemdev(wctx, serial)
}

func (p *poolDriver) prepareDevice(ctx context.Context, r *resourceapi.DeviceRequestAllocationResult, data *poolDevice) (*preparedPoolDevice, error) {
	memdev, err := p.waitMemdev(ctx, data.Serial)
	if err != nil {
		return nil, err
	}
	dev := &preparedPoolDevice{
		Request:     r.Request,
		Device:      r.Device,
		Pool:        r.Pool,
		Serial:      data.Serial,
		SerialHex:   fmt.Sprintf("%x", data.Serial),
		Shared:      data.Shared,
		Size:        data.Size,
		Memdev:      memdev,
		Node:        -1,
		CDIDeviceID: poolCDIDeviceID(data.Serial),
	}
	if data.Shared {
		// device_dax only: never online shared memory as RAM.
		ri, err := p.mem.CreateRegion(ctx, memdev, memctl.ModeDevDax, "")
		if err != nil {
			return nil, err
		}
		if ri.Device == "" || ri.Driver != memctl.DriverDeviceDax {
			return nil, fmt.Errorf("%s: %s is bound to %q, not %s", memdev, ri.Dax, ri.Driver, memctl.DriverDeviceDax)
		}
		major, minor, err := p.mem.DaxDevNumbers(ri.Dax)
		if err != nil {
			return nil, err
		}
		dev.Region, dev.Dax, dev.DaxDevice, dev.DaxMajor, dev.DaxMinor = ri.Region, ri.Dax, ri.Device, major, minor
		return dev, nil
	}
	if _, err := p.mem.CreateRegion(ctx, memdev, memctl.ModeRAM, ""); err != nil {
		return nil, err
	}
	ri, err := p.mem.Online(memdev, true)
	if err != nil {
		return nil, err
	}
	dev.Region, dev.Dax, dev.Node = ri.Region, ri.Dax, ri.Node
	if dev.Node < 0 {
		dev.Node = p.blocksNode(ri.Blocks)
	}
	if dev.Node < 0 {
		return nil, fmt.Errorf("%s: cannot find the NUMA node of %s", memdev, ri.Region)
	}
	return dev, nil
}

// blocksNode returns the NUMA node of online memory blocks
// (/sys/devices/system/memory/memoryN/nodeM), -1 if not found.
func (p *poolDriver) blocksNode(blocks []int) int {
	for _, b := range blocks {
		m, _ := filepath.Glob(filepath.Join(p.mem.SysRoot, "devices", "system", "memory", "memory"+strconv.Itoa(b), "node*"))
		for _, path := range m {
			if n, err := strconv.Atoi(strings.TrimPrefix(filepath.Base(path), "node")); err == nil {
				return n
			}
		}
	}
	return -1
}

// release makes the memdev with the serial removable. A memdev that is
// gone is not an error.
func (p *poolDriver) release(ctx context.Context, serial uint64) error {
	memdev, err := p.mem.FindMemdev(serial)
	if errors.Is(err, memctl.ErrNotFound) {
		klog.Infof("%s: memdev with serial 0x%x is gone, nothing to release", p.name, serial)
		return nil
	}
	if err != nil {
		return err
	}
	if err := p.mem.Release(ctx, memdev); err != nil {
		return err
	}
	klog.Infof("%s: released %s (serial 0x%x)", p.name, memdev, serial)
	return nil
}

func (p *poolDriver) UnprepareResourceClaims(ctx context.Context, claims []kubeletplugin.NamespacedObject) (map[types.UID]error, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	response := map[types.UID]error{}
	for _, claim := range claims {
		response[claim.UID] = p.unprepareClaim(ctx, string(claim.UID))
	}
	return response, nil
}

func (p *poolDriver) unprepareClaim(ctx context.Context, uid string) error {
	pc, ok := p.prepared[uid]
	if !ok {
		return nil
	}
	var errs []error
	for _, d := range pc.Devices {
		if p.preparedDevice(d.Serial, uid) != nil {
			klog.Infof("%s: claim %s: serial 0x%x is still used by another claim", p.name, pc.ClaimName, d.Serial)
			continue
		}
		if err := p.release(ctx, d.Serial); err != nil {
			errs = append(errs, fmt.Errorf("releasing %s (serial 0x%x): %w", d.Device, d.Serial, err))
		}
	}
	if len(errs) > 0 {
		err := fmt.Errorf("%s: unpreparing claim %s: %w", p.name, pc.ClaimName, errors.Join(errs...))
		klog.Error(err)
		return err
	}
	delete(p.prepared, uid)
	klog.Infof("%s: unprepared claim %s (%s)", p.name, pc.ClaimName, uid)
	return p.sync()
}

// sync rewrites the CDI spec and the state file from the prepared map.
// Called with p.mu held.
func (p *poolDriver) sync() error {
	p.updateSerials()
	return errors.Join(p.writeCDISpec(), p.save())
}

// poolCDIDeviceName is the CDI device name of a pool device.
func poolCDIDeviceName(serial uint64) string {
	return fmt.Sprintf("pool-%x", serial)
}

func poolCDIDeviceID(serial uint64) string {
	return cdiparser.QualifiedName(device.CDIVendor, device.CDIClass, poolCDIDeviceName(serial))
}

// poolCDIDevice returns the CDI device of a prepared pool device.
func poolCDIDevice(d *preparedPoolDevice) cdiSpecs.Device {
	x := strings.ToUpper(d.SerialHex)
	serial := "0x" + d.SerialHex
	edits := cdiSpecs.ContainerEdits{}
	if d.Shared {
		edits.Env = []string{
			"CXL_SHARED_DAX_" + x + "=" + d.DaxDevice,
			"CXL_SHARED_SIZE_" + x + "=" + strconv.FormatUint(d.Size, 10),
			"CXL_SHARED_SERIAL_" + x + "=" + serial,
		}
		edits.DeviceNodes = []*cdiSpecs.DeviceNode{{
			Path:  d.DaxDevice,
			Type:  "c",
			Major: int64(d.DaxMajor),
			Minor: int64(d.DaxMinor),
		}}
	} else {
		edits.Env = []string{
			"CXL_POOL_NODE_" + x + "=" + strconv.Itoa(d.Node),
			"CXL_POOL_SIZE_" + x + "=" + strconv.FormatUint(d.Size, 10),
			"CXL_POOL_SERIAL_" + x + "=" + serial,
		}
	}
	return cdiSpecs.Device{Name: poolCDIDeviceName(d.Serial), ContainerEdits: edits}
}

// buildCDISpec builds the CDI spec of all prepared pool devices, one CDI
// device per serial.
func (p *poolDriver) buildCDISpec() *cdiSpecs.Spec {
	spec := &cdiSpecs.Spec{Kind: device.CDIKind}
	uids := make([]string, 0, len(p.prepared))
	for uid := range p.prepared {
		uids = append(uids, uid)
	}
	sort.Strings(uids)
	seen := map[uint64]bool{}
	for _, uid := range uids {
		for i := range p.prepared[uid].Devices {
			d := &p.prepared[uid].Devices[i]
			if seen[d.Serial] {
				continue
			}
			seen[d.Serial] = true
			spec.Devices = append(spec.Devices, poolCDIDevice(d))
		}
	}
	sort.Slice(spec.Devices, func(i, j int) bool { return spec.Devices[i].Name < spec.Devices[j].Name })
	return spec
}

// writeCDISpec writes the CDI spec of the prepared devices, or removes
// it when there are none.
func (p *poolDriver) writeCDISpec() error {
	if p.cdi == nil {
		return nil
	}
	spec := p.buildCDISpec()
	if len(spec.Devices) == 0 {
		if err := p.cdi.RemoveSpec(device.PoolCDISpecName); err != nil {
			klog.V(5).Infof("%s: could not remove CDI spec %s (may not exist): %v", p.name, device.PoolCDISpecName, err)
		}
		return nil
	}
	v, err := cdiapi.MinimumRequiredVersion(spec)
	if err != nil {
		return fmt.Errorf("CDI spec version: %w", err)
	}
	spec.Version = v
	if err := p.cdi.WriteSpec(spec, device.PoolCDISpecName); err != nil {
		return fmt.Errorf("failed to write CDI spec %s: %w", device.PoolCDISpecName, err)
	}
	klog.V(5).Infof("%s: wrote CDI spec %s with %d devices", p.name, device.PoolCDISpecName, len(spec.Devices))
	return nil
}

func (p *poolDriver) save() error {
	data, err := json.MarshalIndent(p.prepared, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding prepared pool claims: %w", err)
	}
	tmp := p.stateFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return fmt.Errorf("writing %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, p.stateFile); err != nil {
		return fmt.Errorf("renaming %s: %w", tmp, err)
	}
	return nil
}

func (p *poolDriver) load() error {
	data, err := os.ReadFile(p.stateFile)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading %s: %w", p.stateFile, err)
	}
	if err := json.Unmarshal(data, &p.prepared); err != nil {
		return fmt.Errorf("parsing %s: %w", p.stateFile, err)
	}
	if p.prepared == nil {
		p.prepared = map[string]*preparedPoolClaim{}
	}
	for uid, pc := range p.prepared {
		for _, d := range pc.Devices {
			if _, err := p.mem.FindMemdev(d.Serial); err != nil {
				klog.Warningf("%s: claim %s (%s): memdev with serial 0x%x of %s is gone, keeping it until Unprepare",
					p.name, pc.ClaimName, uid, d.Serial, d.Device)
			}
		}
	}
	klog.Infof("%s: loaded %d prepared claims from %s", p.name, len(p.prepared), p.stateFile)
	return nil
}

// HandleError is called by the kubelet plugin helper on asynchronous
// errors (see driver.HandleError).
func (p *poolDriver) HandleError(ctx context.Context, err error, message string) {
	if errors.Is(err, kubeletplugin.ErrRecoverable) {
		klog.FromContext(ctx).Error(err, "DRAPlugin encountered an error.", "driver", p.name)
	} else {
		klog.FromContext(ctx).Error(err, "Unrecoverable error.", "driver", p.name)
	}
	runtime.HandleErrorWithContext(ctx, err, message)
}

// WatchHealthStatus: the pool helper does not report device health.
func (p *poolDriver) WatchHealthStatus(ctx context.Context, reports chan<- kubeletplugin.DeviceHealthReport) error {
	return kubeletplugin.ErrHealthNotSupported
}
