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
	// defaultRegionRetryDelay is the pause before the one retry of a
	// region creation that failed right after hotplug ("add_dev()
	// failed", seen in plan-3 measurement B).
	defaultRegionRetryDelay = 3 * time.Second
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
	lender     *claimLender                  // lends companion claims
	retryDelay time.Duration

	// serials is a snapshot of the serials of prepared and inflight
	// devices for the cxl.generic publisher (D29). It has its own lock
	// so that a rescan never waits for a Prepare that waits for udev.
	serialsMu sync.Mutex
	serials   map[uint64]bool
}

type preparedPoolDevice struct {
	Request   string `json:"request"`
	Device    string `json:"device"`
	Pool      string `json:"pool"`
	Serial    uint64 `json:"serial"`
	SerialHex string `json:"serialHex"`
	Shared    bool   `json:"shared"`
	Size      uint64 `json:"size"`
	// Mode is device.ModeNUMA or device.ModeDax ("" in state files
	// written before modes existed: dax if DaxDevice is set).
	Mode        string `json:"mode,omitempty"`
	ReadOnly    bool   `json:"readOnly,omitempty"` // dax: the CDI device node allows reads only
	Memdev      string `json:"memdev"`
	Region      string `json:"region"`
	Dax         string `json:"dax"`
	DaxDevice   string `json:"daxDevice,omitempty"` // /dev/daxX.Y (dax mode)
	DaxMajor    int    `json:"daxMajor,omitempty"`
	DaxMinor    int    `json:"daxMinor,omitempty"`
	Node        int    `json:"node"` // NUMA node of the memory (numa mode), -1 if none
	CDIDeviceID string `json:"cdiDeviceID"`
}

// mode returns the mode of a prepared device, also for state files
// written before modes existed.
func (d *preparedPoolDevice) mode() string {
	switch {
	case d.Mode != "":
		return d.Mode
	case d.DaxDevice != "":
		return device.ModeDax
	}
	return device.ModeNUMA
}

// lent returns the device as the lease broker sees it.
func (d *preparedPoolDevice) lent(driver string) lentDevice {
	return lentDevice{
		Driver:    driver,
		Source:    device.SourcePool,
		Request:   d.Request,
		Device:    d.Device,
		Pool:      d.Pool,
		Serial:    d.Serial,
		SerialHex: d.SerialHex,
		Shared:    d.Shared,
		Size:      d.Size,
		Mode:      d.mode(),
		ReadOnly:  d.ReadOnly,
		Node:      d.Node,
		DaxDevice: d.DaxDevice,
	}
}

// lentDevices returns the devices of a prepared claim as the lease
// broker sees them.
func (p *poolDriver) lentDevices(devs []preparedPoolDevice) []lentDevice {
	out := make([]lentDevice, 0, len(devs))
	for i := range devs {
		out = append(out, devs[i].lent(p.name))
	}
	return out
}

type preparedPoolClaim struct {
	ClaimName string                      `json:"claimName"` // namespace/name
	Devices   []preparedPoolDevice        `json:"devices"`
	Result    kubeletplugin.PrepareResult `json:"result"` // what Prepare returned
	Lend      *lendRecord                 `json:"lend,omitempty"`
}

// poolDeviceData is claim.status.devices[].data written by the pool
// controller. Serial, shared and size are required.
type poolDeviceData struct {
	Serial string  `json:"serial"`
	Shared *bool   `json:"shared"`
	Size   *uint64 `json:"size"`
}

// poolDevice is the parsed poolDeviceData of an allocation result, with
// the mode and read-only setting of its PoolDeviceConfig.
type poolDevice struct {
	Serial   uint64
	Shared   bool
	Size     uint64
	Mode     string
	ReadOnly bool
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
		retryDelay: defaultRegionRetryDelay,
		lender:     newClaimLender(name),
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

// driverName returns the DRA driver name of the pool helper, "" for a
// nil (disabled) pool helper.
func (p *poolDriver) driverName() string {
	if p == nil {
		return ""
	}
	return p.name
}

// lending returns the lender of the pool helper's companion claims, nil
// for a nil (disabled) pool helper.
func (p *poolDriver) lending() *claimLender {
	if p == nil {
		return nil
	}
	return p.lender
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
		if pc.Lend != nil {
			p.lender.reprepared(pc.Lend, p.lentDevices(pc.Devices))
		}
		return pc.Result
	}
	claimName := claim.Namespace + "/" + claim.Name
	if claim.Status.Allocation == nil {
		return kubeletplugin.PrepareResult{Err: fmt.Errorf("claim %s has no allocation", claimName)}
	}
	lend, err := p.lender.parse(claim)
	if err != nil {
		return kubeletplugin.PrepareResult{Err: fmt.Errorf("%s: preparing claim %s: %w", p.name, claimName, err)}
	}
	if lend != nil {
		if err := p.lender.check(lend); err != nil {
			err = fmt.Errorf("%s: preparing claim %s: %w", p.name, claimName, err)
			klog.Error(err)
			return kubeletplugin.PrepareResult{Err: err}
		}
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
		cfg, err := parseDeviceConfig(claim, p.name, device.PoolDeviceConfigKind, r.Request)
		if err != nil {
			return fail(fmt.Errorf("request %s: %w", r.Request, err))
		}
		if data.Mode, err = cfg.modeFor(data.Shared); err != nil {
			return fail(fmt.Errorf("device %s serial 0x%x: %w", r.Device, data.Serial, err))
		}
		data.ReadOnly = cfg.ReadOnly
		if err := p.checkSamePreparation(data, uid); err != nil {
			return fail(fmt.Errorf("device %s serial 0x%x: %w", r.Device, data.Serial, err))
		}
		touched = append(touched, data.Serial)
		p.inflight[data.Serial]++
		p.updateSerials()
		dev, err := p.prepareDevice(ctx, &r, data)
		if err != nil && isAddDevError(err) {
			klog.Warningf("%s: claim %s: device %s: %v; retrying once in %v", p.name, claimName, r.Device, err, p.retryDelay)
			select {
			case <-ctx.Done():
			case <-time.After(p.retryDelay):
				dev, err = p.prepareDevice(ctx, &r, data)
			}
		}
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
		klog.Infof("%s: claim %s: prepared %s (serial 0x%x, shared %v, mode %s, read-only %v) as %s %s dax %s node %d",
			p.name, claimName, r.Device, data.Serial, data.Shared, dev.Mode, dev.ReadOnly, dev.Memdev, dev.Region, dev.Dax, dev.Node)
	}
	devs := p.lentDevices(pc.Devices)
	if lend != nil {
		rec, err := p.lender.lend(ctx, lend, devs)
		if err != nil {
			if rerr := p.lender.revoke(ctx, rec, claimName); rerr != nil {
				klog.Errorf("%s: claim %s: %v", p.name, claimName, rerr)
			}
			p.lender.failed(lend, err)
			return fail(err)
		}
		pc.Lend = rec
	}
	p.prepared[uid] = pc
	if err := p.sync(); err != nil {
		delete(p.prepared, uid)
		if pc.Lend != nil {
			if rerr := p.lender.revoke(ctx, pc.Lend, claimName); rerr != nil {
				klog.Errorf("%s: claim %s: %v", p.name, claimName, rerr)
			}
		}
		res := fail(err)
		if err := p.sync(); err != nil {
			klog.Errorf("%s: %v", p.name, err)
		}
		return res
	}
	if lend != nil {
		p.lender.prepared(lend, devs)
	}
	return pc.Result
}

// isAddDevError matches the transient region creation failure right
// after a hotplug.
func isAddDevError(err error) bool {
	return strings.Contains(err.Error(), "add_dev")
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

// checkSamePreparation refuses a device that another claim prepared
// differently: as shared or exclusive, or in another mode. Read-only may
// differ: it is a property of the CDI device node, not of the region.
func (p *poolDriver) checkSamePreparation(data *poolDevice, exceptUID string) error {
	other := p.preparedDevice(data.Serial, exceptUID)
	switch {
	case other == nil:
		return nil
	case other.Shared != data.Shared:
		return fmt.Errorf("shared=%v, but another claim prepared it with shared=%v", data.Shared, other.Shared)
	case other.mode() != data.Mode:
		return fmt.Errorf("mode %s, but another claim prepared it in mode %s", data.Mode, other.mode())
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
		Mode:        data.Mode,
		ReadOnly:    data.ReadOnly,
		Memdev:      memdev,
		Node:        -1,
		CDIDeviceID: poolCDIDeviceID(data.Serial, data.ReadOnly),
	}
	if data.Mode == device.ModeDax {
		// device_dax: never onlined as RAM (shared memory must never be).
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
		dev.Node = p.mem.BlocksNode(ri.Blocks)
	}
	if dev.Node < 0 {
		return nil, fmt.Errorf("%s: cannot find the NUMA node of %s", memdev, ri.Region)
	}
	return dev, nil
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
	if pc.Lend != nil {
		p.lender.releasing(pc.Lend.Lease)
		// Revoke before the release: offlining memory that the
		// container still has in cpuset.mems leaves a stale node there.
		if err := p.lender.revoke(ctx, pc.Lend, pc.ClaimName); err != nil {
			err = fmt.Errorf("%s: unpreparing claim %s: %w", p.name, pc.ClaimName, err)
			klog.Error(err)
			return err
		}
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

// poolCDIDeviceName is the CDI device name of a pool device: one per
// serial, and a second one with a read-only device node ("-ro").
func poolCDIDeviceName(serial uint64, readOnly bool) string {
	if readOnly {
		return fmt.Sprintf("pool-%x-ro", serial)
	}
	return fmt.Sprintf("pool-%x", serial)
}

func poolCDIDeviceID(serial uint64, readOnly bool) string {
	return cdiparser.QualifiedName(device.CDIVendor, device.CDIClass, poolCDIDeviceName(serial, readOnly))
}

// poolCDIDevice returns the CDI device of a prepared pool device. A dax
// device of a shared device sets CXL_SHARED_*, of an exclusive device
// CXL_POOL_* (daxCDIDevice).
func poolCDIDevice(d *preparedPoolDevice) cdiSpecs.Device {
	name := poolCDIDeviceName(d.Serial, d.ReadOnly)
	if d.mode() == device.ModeDax {
		prefix := "CXL_POOL_"
		if d.Shared {
			prefix = "CXL_SHARED_"
		}
		return daxCDIDevice(name, prefix, d.SerialHex, d.DaxDevice, d.DaxMajor, d.DaxMinor, d.Size, d.ReadOnly)
	}
	x := strings.ToUpper(d.SerialHex)
	edits := cdiSpecs.ContainerEdits{
		Env: []string{
			"CXL_POOL_NODE_" + x + "=" + strconv.Itoa(d.Node),
			"CXL_POOL_SIZE_" + x + "=" + strconv.FormatUint(d.Size, 10),
			"CXL_POOL_SERIAL_" + x + "=0x" + d.SerialHex,
		},
	}
	return cdiSpecs.Device{Name: name, ContainerEdits: edits}
}

// buildCDISpec builds the CDI spec of all prepared pool devices, one CDI
// device per serial and access (read-only or not).
func (p *poolDriver) buildCDISpec() *cdiSpecs.Spec {
	spec := &cdiSpecs.Spec{Kind: device.CDIKind}
	uids := make([]string, 0, len(p.prepared))
	for uid := range p.prepared {
		uids = append(uids, uid)
	}
	sort.Strings(uids)
	seen := map[string]bool{}
	for _, uid := range uids {
		for i := range p.prepared[uid].Devices {
			d := &p.prepared[uid].Devices[i]
			name := poolCDIDeviceName(d.Serial, d.ReadOnly)
			if seen[name] {
				continue
			}
			seen[name] = true
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
	return writeJSONFile(p.stateFile, p.prepared)
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
