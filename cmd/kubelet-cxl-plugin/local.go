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
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	"k8s.io/klog/v2"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/api"

	"github.com/containers/nri-plugins/pkg/cxl/memctl"
)

// The local allocator gives the memory of node-local CXL memdevs (the
// memdev devices of cxl.generic) to claims. It creates one devdax
// region per memdev on first use and destroys it after the last use. Every
// allocation result gets a slice of the region: a dax device whose size is
// a multiple of the memory block size. A numa slice is bound to kmem and
// onlined movable, its memory belongs to the NUMA node of the region that
// all numa slices of the memdev share; a dax slice is a devdax device of
// its own. Slices of both modes share a region. See doc/cxl/LOCAL.md.

const (
	// localStateFileName is the state file of the local allocator in the
	// plugin data directory of cxl.generic.
	localStateFileName = "localDevices.json"
	// orphanOwner owns the slices whose release failed (no claim UID is
	// "-"): they are never handed out, and released again before the
	// next slice of their memdev.
	orphanOwner = "-"
	// cxlRegionAlign is the alignment of the physical address of a CXL
	// region in its window: a region starts at most block size -
	// cxlRegionAlign after a memory block boundary.
	cxlRegionAlign = 256 << 20
	// bootIDFile identifies the running kernel (under the sysfs root).
	bootIDFile = "proc/sys/kernel/random/boot_id"
	// rollbackTimeout bounds the cleanup after a failure.
	rollbackTimeout = time.Minute
	// zeroChunk is the size of the windows in which a slice is zeroed.
	zeroChunk = 64 << 20
)

// errNoRoom is returned when the dax region of a memdev has less free
// memory than a slice needs.
var errNoRoom = errors.New("no room for the slice")

// localSlice is the memory of one allocation result on a local memdev: a
// dax device of the region of the memdev.
type localSlice struct {
	Request string `json:"request"`
	Region  string `json:"region"`
	// Mode is api.ModeNUMA (the memory of the slice is system RAM of the
	// NUMA node of the region) or api.ModeDax (a devdax device).
	Mode string `json:"mode"`
	// Size is the slice size in bytes.
	Size uint64 `json:"size"`
	// Dax is the dax device of the slice (daxX.Y).
	Dax       string `json:"dax,omitempty"`
	DaxDevice string `json:"daxDevice,omitempty"` // dax: /dev/daxX.Y
	Major     int    `json:"major,omitempty"`
	Minor     int    `json:"minor,omitempty"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}

// localDevice is a local memdev that the allocator uses.
type localDevice struct {
	Serial    uint64 `json:"serial"`
	SerialHex string `json:"serialHex"`
	Memdev    string `json:"memdev"`
	// Region is the region that the allocator created on the memdev, ""
	// while it is created: then every region on the memdev is the
	// allocator's.
	Region string `json:"region,omitempty"`
	Node   int    `json:"node"`          // NUMA node of the memory of the region, -1 if unknown
	Dax    string `json:"dax,omitempty"` // first dax device of the region
	// Claims are the slices of the claims that use the memdev, by claim
	// UID.
	Claims map[string][]*localSlice `json:"claims"`
}

// localState is the content of the state file: the devices the allocator
// uses, and the boot of the kernel that they belong to.
type localState struct {
	BootID  string         `json:"bootID"`
	Devices []*localDevice `json:"devices"`
}

// localAllocator creates and destroys the regions and dax slices of local
// memdevs for claims, and keeps its state in a file.
type localAllocator struct {
	mem        *memctl.Manager
	stateFile  string
	bootID     string // "" if unknown
	retryDelay time.Duration
	// zero writes zeros to the memory of a dax device: major, minor,
	// size, align.
	zero    func(major, minor int, size, align uint64) error
	mu      sync.Mutex
	devices map[uint64]*localDevice // by serial

	// serialsSnap is a snapshot of the serials of the devices, and
	// blockSize the memory block size in bytes (0 if unknown), for the
	// publisher. They have their own lock so that a rescan never waits
	// for a Prepare.
	snapMu      sync.Mutex
	serialsSnap map[uint64]bool
	blockSize   uint64
}

// localSnapshot is what the publisher of local memdevs needs from the
// local allocator.
type localSnapshot struct {
	// Serials are the serials of the memdevs that the allocator uses or
	// is preparing.
	Serials map[uint64]bool
	// BlockSize is the memory block size in bytes, 0 if unknown.
	BlockSize uint64
}

// uses tells whether the allocator uses the memdev with the serial.
func (l *localSnapshot) uses(serial uint64) bool {
	return l != nil && l.Serials[serial]
}

// newLocalAllocator returns a local allocator with its state loaded from
// stateFile. The devices of a state from another boot than bootID are
// dropped without touching them ("" bootID: the boot is unknown, the state
// is kept).
func newLocalAllocator(mem *memctl.Manager, stateFile, bootID string) (*localAllocator, error) {
	a := &localAllocator{
		mem:        mem,
		stateFile:  stateFile,
		bootID:     bootID,
		retryDelay: defaultRegionRetryDelay,
		zero: func(major, minor int, size, align uint64) error {
			return zeroDax(mem.DevRoot, major, minor, size, align)
		},
		devices: map[uint64]*localDevice{},
	}
	if err := a.load(); err != nil {
		return nil, err
	}
	if _, err := a.readBlockSize(); err != nil {
		klog.Warningf("local: %v: local memory devices are not published until the memory block size is known", err)
	}
	a.updateSnapshot()
	return a, nil
}

// readBlockSize returns the memory block size from sysfs and keeps it for
// Snapshot.
func (a *localAllocator) readBlockSize() (uint64, error) {
	bs, err := a.mem.MemoryBlockSize()
	if err != nil {
		return 0, fmt.Errorf("memory block size: %w", err)
	}
	a.snapMu.Lock()
	a.blockSize = bs
	a.snapMu.Unlock()
	return bs, nil
}

// readBootID returns the boot id of the running kernel, "" if it cannot be
// read.
func readBootID(sysfsRoot string) string {
	b, err := os.ReadFile(filepath.Join(sysfsRoot, "/", bootIDFile))
	if err != nil {
		klog.Warningf("local: cannot read the boot id: %v", err)
		return ""
	}
	return strings.TrimSpace(string(b))
}

// rollbackContext returns a context for cleaning up after a failure that
// the end of ctx does not cancel.
func rollbackContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
}

// Snapshot returns the serials of the memdevs that the allocator uses or
// is preparing, and the memory block size. Nil for a nil (disabled)
// allocator.
func (a *localAllocator) Snapshot() *localSnapshot {
	if a == nil {
		return nil
	}
	a.snapMu.Lock()
	snap := &localSnapshot{Serials: maps.Clone(a.serialsSnap), BlockSize: a.blockSize}
	a.snapMu.Unlock()
	if snap.BlockSize == 0 {
		snap.BlockSize, _ = a.readBlockSize()
	}
	return snap
}

// updateSnapshot refreshes the snapshot of the serials of the devices.
// Called with a.mu held.
func (a *localAllocator) updateSnapshot() {
	m := make(map[uint64]bool, len(a.devices))
	for serial := range a.devices {
		m[serial] = true
	}
	a.snapMu.Lock()
	a.serialsSnap = m
	a.snapMu.Unlock()
}

// Acquire gives a slice of the local memdev with the serial to an
// allocation result of a claim. It returns the slice and the NUMA node of
// a numa slice (-1 for a dax slice). The slice size is size rounded up to
// a multiple of the memory block size.
//   - claimUID, request: the claim and the request of the allocation result
//   - serial: the serial of the memdev
//   - mode: api.ModeNUMA (system RAM of the NUMA node of the memdev) or
//     api.ModeDax (a devdax device of its own)
//   - size: bytes that the allocation result consumes
//   - readOnly: the dax slice is for reading only
func (a *localAllocator) Acquire(ctx context.Context, claimUID, request string, serial uint64, mode string, size uint64, readOnly bool) (*localSlice, int, error) {
	if a == nil {
		return nil, -1, errors.New("local memdevs are not managed (manageMemdevs: false)")
	}
	if mode != api.ModeNUMA && mode != api.ModeDax {
		return nil, -1, fmt.Errorf("invalid mode %q", mode)
	}
	if size == 0 {
		return nil, -1, errors.New("size 0")
	}
	bs, err := a.readBlockSize()
	if err != nil {
		return nil, -1, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	d, err := a.device(ctx, serial, bs)
	if err != nil {
		return nil, -1, err
	}
	s := &localSlice{Request: request, Region: d.Region, Mode: mode, Size: size, ReadOnly: readOnly}
	if err := a.addSlice(ctx, d, claimUID, s, bs); err != nil {
		a.dropUnused(ctx, d)
		return nil, -1, fmt.Errorf("memdev %s (serial 0x%x): %w", d.Memdev, serial, err)
	}
	d.Claims[claimUID] = append(d.Claims[claimUID], s)
	node := -1
	if mode == api.ModeNUMA {
		node = d.Node
	}
	klog.Infof("local: memdev %s (serial 0x%x) %s: claim %s request %s: slice %s mode %s size %d node %d%s",
		d.Memdev, serial, d.Region, claimUID, request, s.Dax, mode, s.Size, node, strings.TrimRight(" "+s.DaxDevice, " "))
	a.updateSnapshot()
	if err := a.save(); err != nil {
		return nil, -1, err
	}
	return s, node, nil
}

// device returns the device with the serial, with its region created
// (memory blocks of bs bytes) and the slices of failed releases released.
func (a *localAllocator) device(ctx context.Context, serial, bs uint64) (*localDevice, error) {
	memdev, err := a.mem.FindMemdev(serial)
	if err != nil {
		return nil, err
	}
	d := a.devices[serial]
	if d != nil {
		d.Memdev = memdev
		if err := a.releaseOrphans(ctx, d); err != nil {
			a.persist()
			return nil, err
		}
		if len(d.Claims) == 0 {
			// leftovers of a failed release or of a restart
			if err := a.releaseRegion(ctx, d); err != nil {
				return nil, err
			}
			d = nil
		}
	}
	if err := a.checkRegions(memdev, d); err != nil {
		return nil, err
	}
	if d != nil {
		return d, nil
	}
	d = &localDevice{
		Serial:    serial,
		SerialHex: fmt.Sprintf("%x", serial),
		Memdev:    memdev,
		Node:      -1,
		Claims:    map[string][]*localSlice{},
	}
	a.devices[serial] = d
	a.updateSnapshot()
	err = a.save()
	if err == nil {
		err = a.createRegion(ctx, d, bs)
	}
	if err == nil {
		err = a.save()
	}
	if err != nil {
		a.dropUnused(ctx, d)
		return nil, err
	}
	return d, nil
}

// checkRegions returns an error if the memdev has a region other than the
// one of the device (d nil: any region).
func (a *localAllocator) checkRegions(memdev string, d *localDevice) error {
	for _, r := range a.mem.Regions(memdev) {
		if d == nil || r != d.Region {
			return fmt.Errorf("memdev %s has region %s that the plugin did not create", memdev, r)
		}
	}
	return nil
}

// createRegion creates the devdax region of a device, and retries once
// after a transient failure. A region that does not start at a boundary of
// the memory blocks of bs bytes is logged.
func (a *localAllocator) createRegion(ctx context.Context, d *localDevice, bs uint64) error {
	start := time.Now()
	ri, err := a.region(ctx, d.Memdev)
	if err != nil && isAddDevError(err) {
		klog.Warningf("local: memdev %s (serial 0x%x): %v; retrying once in %v", d.Memdev, d.Serial, err, a.retryDelay)
		select {
		case <-ctx.Done():
		case <-time.After(a.retryDelay):
			ri, err = a.region(ctx, d.Memdev)
		}
	}
	if err != nil {
		// The memdev had no region before: a region on it now is what
		// the failed creation left.
		if rs := a.mem.Regions(d.Memdev); len(rs) == 1 {
			d.Region = rs[0]
		}
		return fmt.Errorf("memdev %s (serial 0x%x): %w", d.Memdev, d.Serial, err)
	}
	d.Region, d.Dax, d.Node = ri.Region, ri.Dax, ri.Node
	klog.Infof("local: memdev %s (serial 0x%x): created %s (devdax, dax %s, node %d) in %s", d.Memdev, d.Serial, d.Region, d.Dax, d.Node, since(start))
	if r, err := a.mem.RegionRange(d.Region); err == nil && r.Start%bs != 0 {
		whole, end := uint64(0), (r.End+1)/bs*bs
		if start := roundUp(r.Start, bs); end > start {
			whole = end - start
		}
		klog.Warningf("local: memdev %s (serial 0x%x): %s at %v does not start at a memory block boundary (block size %d): slices use only its %d bytes of whole blocks",
			d.Memdev, d.Serial, d.Region, r, bs, whole)
	}
	return nil
}

// region creates the devdax region of a memdev.
func (a *localAllocator) region(ctx context.Context, memdev string) (*memctl.RegionInfo, error) {
	ri, err := a.mem.CreateRegion(ctx, memdev, memctl.ModeDax, "")
	if err != nil {
		return nil, err
	}
	if ri.Driver != memctl.DriverDeviceDax {
		return nil, fmt.Errorf("%s: %s is bound to %q, not %s", memdev, ri.Dax, ri.Driver, memctl.DriverDeviceDax)
	}
	return ri, nil
}

// since returns the time since start in milliseconds for logs ("12.345ms").
func since(start time.Time) string {
	return fmt.Sprintf("%.3fms", float64(time.Since(start))/float64(time.Millisecond))
}

// logSlice logs a step of a slice of a claim that began at start.
func logSlice(d *localDevice, claimUID, step string, s *localSlice, start time.Time, details string) {
	klog.Infof("local: memdev %s (serial 0x%x): claim %s: %s slice %s mode %s size %d%s in %s",
		d.Memdev, d.Serial, claimUID, step, s.Dax, s.Mode, s.Size, details, since(start))
}

// roundUp returns v rounded up to a multiple of step (v if step is 0).
func roundUp(v, step uint64) uint64 {
	if step == 0 || v%step == 0 {
		return v
	}
	return (v/step + 1) * step
}

// addSlice makes the dax device of a slice of a claim in the region of a
// device and sets s.Dax: the first dax device of the region when no claim
// uses it, else a new one, placed first-fit by the kernel. The slice size
// is s.Size rounded up to a multiple of the memory block size bs and of the
// align of the dax region. A slice that first-fit placed across a block
// boundary is placed on the first whole free blocks of the region instead.
// A numa slice is bound to kmem and onlined movable; a dax slice is bound
// to device_dax and zeroed. A slice that cannot be deleted after a failure
// is kept for orphanOwner.
func (a *localAllocator) addSlice(ctx context.Context, d *localDevice, claimUID string, s *localSlice, bs uint64) error {
	avail, align, devs, err := a.mem.DaxRegionInfo(d.Region)
	if err != nil {
		return err
	}
	if size := roundUp(roundUp(s.Size, bs), align); size != s.Size {
		klog.Warningf("local: memdev %s (serial 0x%x): claim %s: slice of %d bytes rounded up to %d (memory block size %d, align %d)",
			d.Memdev, d.Serial, claimUID, s.Size, size, bs, align)
		s.Size = size
	}
	first, err := a.reusable(d, devs, avail, s.Size)
	if err != nil {
		return err
	}
	start := time.Now()
	dax := first
	if first != "" {
		err = a.mem.ResizeDaxDevice(ctx, first, s.Size)
	} else {
		dax, err = a.mem.CreateDaxDevice(ctx, d.Region, s.Size)
	}
	if err == nil {
		dax, err = a.alignSlice(ctx, d, claimUID, dax, s.Size, bs)
	}
	if err == nil {
		s.Dax = dax
		ranges, _ := a.mem.DaxDeviceRanges(dax)
		logSlice(d, claimUID, "created", s, start, fmt.Sprintf(" at %v", ranges))
		if s.Mode == api.ModeNUMA {
			err = a.onlineSlice(ctx, d, claimUID, s, bs)
		} else {
			err = a.bindSlice(ctx, dax, s)
			if err == nil {
				err = a.zeroSlice(d, claimUID, s, align)
			}
		}
	}
	if err != nil && dax != "" {
		rctx, cancel := rollbackContext(ctx)
		defer cancel()
		failed := &localSlice{Region: d.Region, Mode: s.Mode, Size: s.Size, Dax: dax}
		if derr := a.deleteSlice(rctx, d, claimUID, failed); derr != nil {
			klog.Errorf("local: %v; keeping %s for a later release", derr, dax)
			d.Claims[orphanOwner] = append(d.Claims[orphanOwner], failed)
		}
	}
	return err
}

// alignSlice returns the dax device of a slice of size bytes with memory
// on whole memory blocks of bs bytes: dax, or a new device on the first
// whole free blocks of the region, in address order, that replaces dax.
func (a *localAllocator) alignSlice(ctx context.Context, d *localDevice, claimUID, dax string, size, bs uint64) (string, error) {
	ranges, err := a.mem.DaxDeviceRanges(dax)
	if err != nil {
		return dax, err
	}
	aligned := len(ranges) > 0
	for _, r := range ranges {
		aligned = aligned && r.Aligned(bs)
	}
	if aligned {
		return dax, nil
	}
	klog.Warningf("local: memdev %s (serial 0x%x): claim %s: slice %s at %v is not aligned to the memory block size %d, placing it explicitly",
		d.Memdev, d.Serial, claimUID, dax, ranges, bs)
	if err := a.mem.DeleteDaxDevice(ctx, d.Region, dax); err != nil {
		return dax, err
	}
	gaps, err := a.mem.RegionFreeGaps(d.Region)
	if err != nil {
		return "", err
	}
	whole := blockRanges(gaps, size, bs)
	if whole == nil {
		return "", fmt.Errorf("%w: no %d bytes of whole free memory blocks in %s (free %v, memory block size %d)", errNoRoom, size, d.Region, gaps, bs)
	}
	return a.mem.CreateDaxDeviceRanges(ctx, d.Region, whole)
}

// blockRanges returns the parts of the ranges that are whole memory blocks
// of bs bytes, in address order, up to size bytes; nil if they have less.
func blockRanges(ranges []memctl.Range, size, bs uint64) []memctl.Range {
	var out []memctl.Range
	for _, r := range ranges {
		start, end := roundUp(r.Start, bs), (r.End+1)/bs*bs
		if start >= end || size == 0 {
			continue
		}
		n := min(end-start, size)
		out = append(out, memctl.Range{Start: start, End: start + n - 1})
		size -= n
	}
	if size > 0 {
		return nil
	}
	return out
}

// localUsableSize returns the bytes of whole memory blocks of bs bytes
// that a region on a memdev of size bytes is sure to hold, wherever in its
// window it starts.
func localUsableSize(size, bs uint64) uint64 {
	if bs == 0 {
		return 0
	}
	if bs > cxlRegionAlign {
		head := bs - cxlRegionAlign
		if size <= head {
			return 0
		}
		size -= head
	}
	return size / bs * bs
}

// onlineSlice binds the dax device of a numa slice of a claim to kmem,
// onlines its memory blocks movable, and sets the NUMA node of the device
// to the node of the memory.
func (a *localAllocator) onlineSlice(ctx context.Context, d *localDevice, claimUID string, s *localSlice, bs uint64) error {
	start := time.Now()
	// kmem gets this device only: a sysfs bind, no new_id, no daxctl.
	if err := a.mem.SetDaxDriverSysfs(ctx, s.Dax, memctl.DriverKmem); err != nil {
		return err
	}
	if err := a.mem.WaitDaxBlocks(ctx, s.Dax); err != nil {
		return err
	}
	blocks, err := a.mem.DaxDeviceBlocks(s.Dax, bs)
	if err != nil {
		return err
	}
	if err := a.mem.SetBlocksState(blocks, "online_movable"); err != nil {
		return err
	}
	node := a.mem.TargetNode(s.Dax)
	if node < 0 {
		node = a.mem.BlocksNode(blocks)
	}
	if node < 0 {
		return fmt.Errorf("cannot find the NUMA node of %s", s.Dax)
	}
	d.Node = node
	logSlice(d, claimUID, "onlined", s, start, fmt.Sprintf(" blocks %v node %d", blocks, node))
	return nil
}

// reusable returns the first dax device of the region of a device if no
// slice uses it and kmem does not have it ("" otherwise), after checking
// that the dax region has room for a slice of size bytes: avail free bytes,
// and the size of that dax device.
func (a *localAllocator) reusable(d *localDevice, devs []string, avail, size uint64) (string, error) {
	first := ""
	for _, dax := range devs {
		if strings.HasSuffix(dax, ".0") && !d.usesDax(dax) && a.mem.DaxDriver(dax) != memctl.DriverKmem {
			first = dax
			break
		}
	}
	if first != "" {
		cur, err := a.mem.DaxDeviceSize(first)
		if err != nil {
			return "", err
		}
		avail += cur
	}
	if avail < size {
		klog.Errorf("local: memdev %s (serial 0x%x): %d bytes free in %s, a slice needs %d: claims allocated more than the capacity?", d.Memdev, d.Serial, avail, d.Region, size)
		return "", fmt.Errorf("%w: %d bytes free in %s, the slice needs %d", errNoRoom, avail, d.Region, size)
	}
	return first, nil
}

// zeroSlice writes zeros to the memory of a dax slice of a claim, so that
// a claim never sees what an earlier one left there.
func (a *localAllocator) zeroSlice(d *localDevice, claimUID string, s *localSlice, align uint64) error {
	start := time.Now()
	if err := a.zero(s.Major, s.Minor, s.Size, align); err != nil {
		return fmt.Errorf("zeroing %s: %w", s.Dax, err)
	}
	logSlice(d, claimUID, "zeroed", s, start, "")
	return nil
}

// zeroDax writes zeros to the memory of the dax character device
// major:minor of size bytes, through a private device node in a temporary
// directory under devRoot, in windows that are multiples of align.
func zeroDax(devRoot string, major, minor int, size, align uint64) error {
	// A device node can be opened only on a file system without nodev:
	// devRoot (devtmpfs), not the temporary directory (often tmpfs nodev).
	dir, err := os.MkdirTemp(devRoot, ".cxl-local-zero-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	node := filepath.Join(dir, "dax")
	if err := syscall.Mknod(node, syscall.S_IFCHR|0600, int(unix.Mkdev(uint32(major), uint32(minor)))); err != nil {
		return fmt.Errorf("mknod %d:%d: %w", major, minor, err)
	}
	f, err := os.OpenFile(node, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return zeroFile(f, size, align)
}

// zeroFile writes zeros to the first size bytes of a file through shared
// mappings of at most zeroChunk bytes, rounded up to a multiple of align.
func zeroFile(f *os.File, size, align uint64) error {
	chunk := uint64(zeroChunk)
	if align > 0 && chunk%align != 0 {
		chunk = (chunk/align + 1) * align
	}
	for off := uint64(0); off < size; off += chunk {
		n := min(chunk, size-off)
		b, err := syscall.Mmap(int(f.Fd()), int64(off), int(n), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			return fmt.Errorf("mmap %d bytes at %d: %w", n, off, err)
		}
		clear(b)
		if err := syscall.Munmap(b); err != nil {
			return fmt.Errorf("munmap: %w", err)
		}
	}
	return nil
}

// bindSlice binds the dax device of a slice to device_dax and sets the
// device of the slice.
func (a *localAllocator) bindSlice(ctx context.Context, dax string, s *localSlice) error {
	if err := a.mem.SetDaxDriver(ctx, dax, memctl.DriverDeviceDax); err != nil {
		return err
	}
	major, minor, err := a.mem.DaxDevNumbers(dax)
	if err != nil {
		return err
	}
	s.Dax, s.DaxDevice, s.Major, s.Minor = dax, filepath.Join(a.mem.DevRoot, dax), major, minor
	return nil
}

// regionPath returns the sysfs path of a region.
func (a *localAllocator) regionPath(region string) string {
	if a == nil || region == "" {
		return ""
	}
	return filepath.Join(a.mem.SysRoot, "bus", "cxl", "devices", region)
}

// holds tells whether the allocator holds the dax device for a slice of a
// claim.
func (a *localAllocator) holds(claimUID, dax string) bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, d := range a.devices {
		for _, s := range d.Claims[claimUID] {
			if s.Dax == dax {
				return true
			}
		}
	}
	return false
}

// usesDax tells whether a slice of the device has the dax device.
func (d *localDevice) usesDax(dax string) bool {
	for _, own := range d.Claims {
		for _, s := range own {
			if s.Dax == dax {
				return true
			}
		}
	}
	return false
}

// Release releases the slices of a claim (numa slices are offlined), and
// the region of a memdev whose last slice they were. The memdev stays
// enabled. Nothing to release is not an error.
func (a *localAllocator) Release(ctx context.Context, claimUID string) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	var errs []error
	changed := false
	for _, serial := range a.serials() {
		d := a.devices[serial]
		if _, ok := d.Claims[claimUID]; !ok {
			continue
		}
		changed = true
		if err := a.releaseClaim(ctx, d, claimUID); err != nil {
			errs = append(errs, err)
		}
	}
	if changed {
		a.updateSnapshot()
		errs = append(errs, a.save())
	}
	return errors.Join(errs...)
}

// releaseClaim releases the slices of a claim on a device, and the region
// of the device if the claim was its last one. On error the claim keeps
// what is not released yet.
func (a *localAllocator) releaseClaim(ctx context.Context, d *localDevice, claimUID string) error {
	if err := a.deleteSlices(ctx, d, claimUID); err != nil {
		return err
	}
	for uid := range d.Claims {
		if uid != claimUID && uid != orphanOwner {
			delete(d.Claims, claimUID)
			return nil
		}
	}
	// the claim stays until the region is released
	d.Claims[claimUID] = nil
	if err := a.releaseOrphans(ctx, d); err != nil {
		// the region stays for the slices that cannot be released
		delete(d.Claims, claimUID)
		klog.Errorf("local: %v", err)
		return nil
	}
	return a.releaseRegion(ctx, d)
}

// deleteSlices deletes the slices of a claim on a device, the last one
// first. On error the claim keeps the slices that are not deleted.
func (a *localAllocator) deleteSlices(ctx context.Context, d *localDevice, claimUID string) error {
	own := d.Claims[claimUID]
	for len(own) > 0 {
		if err := a.deleteSlice(ctx, d, claimUID, own[len(own)-1]); err != nil {
			d.Claims[claimUID] = own
			return err
		}
		own = own[:len(own)-1]
	}
	return nil
}

// releaseOrphans releases the slices of orphanOwner on a device. On error
// orphanOwner keeps the slices that are not released.
func (a *localAllocator) releaseOrphans(ctx context.Context, d *localDevice) error {
	if err := a.deleteSlices(ctx, d, orphanOwner); err != nil {
		return err
	}
	delete(d.Claims, orphanOwner)
	return nil
}

// keepOrphans gives the slices of a claim on a device, whose release
// failed, to orphanOwner, and forgets the claim.
func (d *localDevice) keepOrphans(claimUID string) {
	if own := d.Claims[claimUID]; len(own) > 0 {
		klog.Warningf("local: memdev %s (serial 0x%x): claim %s: keeping %d slices for a later release", d.Memdev, d.Serial, claimUID, len(own))
		d.Claims[orphanOwner] = append(d.Claims[orphanOwner], own...)
	}
	delete(d.Claims, claimUID)
}

// deleteSlice offlines the memory of a slice of a claim that is bound to
// kmem, and deletes its dax device (the first dax device of the region is
// kept with size 0). A slice without a dax device has nothing of its own.
func (a *localAllocator) deleteSlice(ctx context.Context, d *localDevice, claimUID string, s *localSlice) error {
	if s.Dax == "" {
		return nil
	}
	if err := a.offlineSlice(d, claimUID, s); err != nil {
		return err
	}
	start := time.Now()
	if err := a.mem.DeleteDaxDevice(ctx, d.Region, s.Dax); err != nil {
		return fmt.Errorf("memdev %s (serial 0x%x): releasing slice %s: %w", d.Memdev, d.Serial, s.Dax, err)
	}
	logSlice(d, claimUID, "deleted", s, start, "")
	return nil
}

// offlineSlice offlines the memory blocks of a slice of a claim whose dax
// device is bound to kmem.
func (a *localAllocator) offlineSlice(d *localDevice, claimUID string, s *localSlice) error {
	if a.mem.DaxDriver(s.Dax) != memctl.DriverKmem {
		return nil
	}
	bs, err := a.readBlockSize()
	if err != nil {
		return fmt.Errorf("memdev %s (serial 0x%x): releasing slice %s: %w", d.Memdev, d.Serial, s.Dax, err)
	}
	blocks, err := a.mem.DaxDeviceBlocks(s.Dax, bs)
	if err == nil {
		start := time.Now()
		if err = a.mem.SetBlocksState(blocks, "offline"); err == nil {
			logSlice(d, claimUID, "offlined", s, start, fmt.Sprintf(" blocks %v", blocks))
			return nil
		}
	}
	return fmt.Errorf("memdev %s (serial 0x%x): releasing slice %s: cannot offline its memory (in use?): %w", d.Memdev, d.Serial, s.Dax, err)
}

// releaseRegion releases the region of a device and forgets the device. A
// device without a known region is only forgotten.
func (a *localAllocator) releaseRegion(ctx context.Context, d *localDevice) error {
	start := time.Now()
	if d.Region != "" {
		if err := a.mem.ReleaseRegion(ctx, d.Region); err != nil {
			return fmt.Errorf("memdev %s (serial 0x%x): releasing its region: %w", d.Memdev, d.Serial, err)
		}
	}
	delete(a.devices, d.Serial)
	klog.Infof("local: memdev %s (serial 0x%x): released %s in %s", d.Memdev, d.Serial, d.Region, since(start))
	return nil
}

// dropUnused releases a device that no claim uses after a failed Acquire,
// and saves the state.
func (a *localAllocator) dropUnused(ctx context.Context, d *localDevice) {
	if len(d.Claims) == 0 {
		ctx, cancel := rollbackContext(ctx)
		defer cancel()
		if err := a.releaseRegion(ctx, d); err != nil {
			klog.Errorf("local: %v", err)
		}
	}
	a.persist()
}

// persist refreshes the snapshot and saves the state, logging an error.
func (a *localAllocator) persist() {
	a.updateSnapshot()
	if err := a.save(); err != nil {
		klog.Errorf("local: %v", err)
	}
}

// Reconcile matches the state with the system and the prepared claims:
// devices whose memdev or region is gone are forgotten, slices that are
// gone are dropped, claims that are not in prepared are released (numa
// slices offlined), and so are the slices of failed releases, devices
// without claims and dax devices that belong to no slice. A slice whose
// release fails is kept for orphanOwner.
func (a *localAllocator) Reconcile(ctx context.Context, prepared map[string]bool) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, serial := range a.serials() {
		a.reconcileDevice(ctx, a.devices[serial], prepared)
	}
	a.updateSnapshot()
	if err := a.save(); err != nil {
		klog.Errorf("local: %v", err)
	}
	klog.Infof("local: %d memdevs in use after reconcile", len(a.devices))
}

// reconcileDevice reconciles one device (see Reconcile).
func (a *localAllocator) reconcileDevice(ctx context.Context, d *localDevice, prepared map[string]bool) {
	memdev, err := a.mem.FindMemdev(d.Serial)
	if err != nil {
		klog.Warningf("local: memdev %s (serial 0x%x) is gone, forgetting it and its claims %v", d.Memdev, d.Serial, slices.Sorted(maps.Keys(d.Claims)))
		delete(a.devices, d.Serial)
		return
	}
	d.Memdev = memdev
	if d.Region == "" {
		klog.Warningf("local: memdev %s (serial 0x%x): its region was being created, forgetting it without touching the memdev", memdev, d.Serial)
		delete(a.devices, d.Serial)
		return
	}
	if !slices.Contains(a.mem.Regions(memdev), d.Region) {
		klog.Warningf("local: memdev %s (serial 0x%x): %s is gone, forgetting it and its claims %v", memdev, d.Serial, d.Region, slices.Sorted(maps.Keys(d.Claims)))
		delete(a.devices, d.Serial)
		return
	}
	for _, uid := range slices.Sorted(maps.Keys(d.Claims)) {
		switch {
		case uid == orphanOwner:
			a.dropGoneSlices(d, uid)
		case !prepared[uid]:
			klog.Infof("local: memdev %s (serial 0x%x): claim %s is not prepared, releasing it", memdev, d.Serial, uid)
			if err := a.releaseClaim(ctx, d, uid); err != nil {
				klog.Errorf("local: %v", err)
				d.keepOrphans(uid)
			}
		default:
			a.dropGoneSlices(d, uid)
		}
	}
	if a.devices[d.Serial] != nil {
		if err := a.releaseOrphans(ctx, d); err != nil {
			klog.Errorf("local: %v", err)
		}
	}
	switch {
	case a.devices[d.Serial] == nil:
	case len(d.Claims) == 0:
		if err := a.releaseRegion(ctx, d); err != nil {
			klog.Errorf("local: %v", err)
		}
	default:
		a.deleteOrphans(ctx, d)
	}
}

// deleteOrphans deletes the dax devices of the region of a device that
// belong to no slice and have memory (numa ones are offlined first).
func (a *localAllocator) deleteOrphans(ctx context.Context, d *localDevice) {
	for _, dax := range a.mem.DaxDevices(d.Region) {
		size, err := a.mem.DaxDeviceSize(dax)
		if err != nil || size == 0 || d.usesDax(dax) {
			continue
		}
		s := &localSlice{Mode: api.ModeDax, Size: size, Dax: dax}
		if a.mem.DaxDriver(dax) == memctl.DriverKmem {
			s.Mode = api.ModeNUMA
		}
		klog.Warningf("local: memdev %s (serial 0x%x): %s (mode %s, %d bytes) belongs to no claim, deleting it", d.Memdev, d.Serial, dax, s.Mode, size)
		if err := a.deleteSlice(ctx, d, orphanOwner, s); err != nil {
			klog.Errorf("local: %v; keeping %s for a later release", err, dax)
			s.Region = d.Region
			d.Claims[orphanOwner] = append(d.Claims[orphanOwner], s)
		}
	}
}

// dropGoneSlices drops the slices of a claim whose dax device is gone or
// has another size, and the claim if no slice is left.
func (a *localAllocator) dropGoneSlices(d *localDevice, claimUID string) {
	kept := []*localSlice{}
	for _, s := range d.Claims[claimUID] {
		if s.Dax != "" {
			if size, err := a.mem.DaxDeviceSize(s.Dax); err != nil || size != s.Size {
				klog.Warningf("local: memdev %s (serial 0x%x): claim %s: slice %s of %d bytes is gone, dropping it", d.Memdev, d.Serial, claimUID, s.Dax, s.Size)
				continue
			}
		}
		kept = append(kept, s)
	}
	if len(kept) == 0 {
		delete(d.Claims, claimUID)
		return
	}
	d.Claims[claimUID] = kept
}

// serials returns the serials of the devices in ascending order.
func (a *localAllocator) serials() []uint64 {
	return slices.Sorted(maps.Keys(a.devices))
}

// save writes the state file.
func (a *localAllocator) save() error {
	st := localState{BootID: a.bootID, Devices: make([]*localDevice, 0, len(a.devices))}
	for _, serial := range a.serials() {
		st.Devices = append(st.Devices, a.devices[serial])
	}
	return writeJSONFile(a.stateFile, st)
}

// load reads the state file, if there is one. A list of devices without a
// boot id is read as the state of an unknown boot.
func (a *localAllocator) load() error {
	data, err := os.ReadFile(a.stateFile)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading %s: %w", a.stateFile, err)
	}
	var st localState
	if err := json.Unmarshal(data, &st); err != nil {
		if lerr := json.Unmarshal(data, &st.Devices); lerr != nil {
			return fmt.Errorf("parsing %s: %w", a.stateFile, err)
		}
	}
	if a.bootID != "" && st.BootID != a.bootID {
		klog.Warningf("local: %s is from another boot (%q, this is %q): forgetting its %d memdevs without touching them",
			a.stateFile, st.BootID, a.bootID, len(st.Devices))
		return nil
	}
	for _, d := range st.Devices {
		if d.Claims == nil {
			d.Claims = map[string][]*localSlice{}
		}
		a.devices[d.Serial] = d
	}
	klog.Infof("local: loaded %d memdevs from %s", len(a.devices), a.stateFile)
	return nil
}

// writeJSONFile replaces the file with v in JSON, readable by the owner
// only.
func writeJSONFile(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding %s: %w", path, err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return fmt.Errorf("writing %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("renaming %s: %w", tmp, err)
	}
	return nil
}
