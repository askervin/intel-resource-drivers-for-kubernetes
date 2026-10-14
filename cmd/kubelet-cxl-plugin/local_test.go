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
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"

	"github.com/containers/nri-plugins/pkg/cxl/memctl"
)

const (
	localSerial    = 0xc100e2e1
	localDaxRegion = "bus/cxl/devices/region0/dax_region0"
	localDaxAttrs  = localDaxRegion + "/dax_region"
	mib            = 1 << 20
	// localRegionSize is the size of the local memdev and its region.
	localRegionSize = 1 << 30
	// localRegionStart is the physical address of region0.
	localRegionStart = 0x250000000
	// localBlockSize is the memory block size of the fake sysfs
	// (block_size_bytes "8000000"); memory74 is the first block of
	// region0.
	localBlockSize = 128 * mib
)

// fakeLocalSys is a fakePoolSys whose region0 is a dynamic dax region
// (align 2 MiB) that can be sliced. The kernel's side of the dax bus
// attributes (create, delete, size, mapping, bind, unbind) is faked on
// sysfs writes: dax devices get memory first-fit (mappingN), and memory
// blocks exist while kmem has the memory of a dax device.
type fakeLocalSys struct {
	*fakePoolSys
	nextDax    int              // id of the next dax device that dax_region/create makes
	failCreate int              // number of region creations that fail with add_dev
	fail       map[string]error // sysfs attribute (relative path) -> write error
	failDaxctl error            // error of daxctl
	// failSeq has the results of the next writes of sysfs attributes
	// (relative path), nil for a write that succeeds.
	failSeq map[string][]error
	zeroed  []string // "major:minor size align" of zeroed slices
	zeroErr error    // error of zeroing
	// regionStart and regionSize are the memory of region0.
	regionStart, regionSize uint64
	// bs is the memory block size.
	bs uint64
	// noKmemBlocks: kmem adds no memory blocks.
	noKmemBlocks bool
}

func newFakeLocalSys(t *testing.T) *fakeLocalSys {
	f := &fakeLocalSys{fakePoolSys: newFakePoolSys(t), regionStart: localRegionStart, regionSize: localRegionSize, bs: localBlockSize}
	// a hotplugged memdev has no memory blocks before kmem adds them
	for _, b := range []string{"74", "75"} {
		_ = os.RemoveAll(f.p("devices/system/memory/memory" + b))
	}
	f.plug("0xc100e2e1")
	return f
}

func (f *fakeLocalSys) manager() *memctl.Manager {
	m := f.fakePoolSys.manager()
	m.Exec = f.exec
	m.WriteFile = f.writeAttr
	return m
}

func (f *fakeLocalSys) record(name string, args ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cmds = append(f.cmds, name+" "+strings.Join(args, " "))
}

func (f *fakeLocalSys) exec(ctx context.Context, name string, args ...string) (string, error) {
	switch name + " " + args[0] {
	case "cxl create-region":
		if f.failCreate > 0 {
			f.failCreate--
			f.record(name, args...)
			return "", errors.New("cxl region: cmd_create_region: region0: add_dev() failed")
		}
		out, err := f.fakePoolSys.exec(ctx, name, args...)
		f.write("bus/cxl/devices/region0/resource", fmt.Sprintf("0x%x", f.regionStart))
		f.write("bus/cxl/devices/region0/size", fmt.Sprintf("0x%x", f.regionSize))
		f.write(localDaxAttrs+"/align", strconv.Itoa(2*mib))
		f.write(localDaxAttrs+"/create", "")
		f.write(daxPath+"/dev", "252:0")
		f.setRanges("dax0.0", []memctl.Range{{Start: f.regionStart, End: f.regionStart + f.regionSize - 1}})
		f.available()
		// the kernel binds the dax device of a ram region to kmem
		f.addBlocks("dax0.0")
		f.nextDax = 1
		return out, err
	case "cxl destroy-region":
		m, _ := filepath.Glob(f.p("bus/dax/devices/dax0.*"))
		for _, d := range m {
			if f.daxDriver(filepath.Base(d)) == memctl.DriverKmem {
				f.removeBlocks(filepath.Base(d))
			}
		}
		out, err := f.fakePoolSys.exec(ctx, name, args...)
		for _, d := range m {
			_ = os.Remove(d)
		}
		return out, err
	case "daxctl reconfigure-device":
		f.record(name, args...)
		if f.failDaxctl != nil {
			return "", f.failDaxctl
		}
		dax := args[len(args)-1]
		if f.daxSize(dax) == 0 {
			return "", errors.New("daxctl: " + dax + ": enable failed")
		}
		drv := memctl.DriverKmem
		if strings.Contains(strings.Join(args, " "), "--mode=devdax") {
			drv = memctl.DriverDeviceDax
		}
		f.setDriver(dax, drv)
		return "", nil
	}
	return f.fakePoolSys.exec(ctx, name, args...)
}

func (f *fakeLocalSys) daxSize(dax string) uint64 {
	v, _ := strconv.ParseUint(f.read(localDaxRegion+"/"+dax+"/size"), 10, 64)
	return v
}

func (f *fakeLocalSys) daxDriver(dax string) string {
	t, err := os.Readlink(f.p(localDaxRegion + "/" + dax + "/driver"))
	if err != nil {
		return ""
	}
	return filepath.Base(t)
}

func (f *fakeLocalSys) daxExists(dax string) bool {
	_, err := os.Lstat(f.p("bus/dax/devices/" + dax))
	return err == nil
}

// setDriver binds a dax device to driver ("": unbinds it). The memory of
// kmem becomes memory blocks, offline.
func (f *fakeLocalSys) setDriver(dax, driver string) {
	if f.daxDriver(dax) == memctl.DriverKmem {
		f.removeBlocks(dax)
	}
	_ = os.Remove(f.p(localDaxRegion + "/" + dax + "/driver"))
	if driver == "" {
		return
	}
	f.link(localDaxRegion+"/"+dax+"/driver", "../../../../../dax/drivers/"+driver)
	if driver == memctl.DriverKmem {
		f.addBlocks(dax)
	}
}

// setBlockSize sets the memory block size.
func (f *fakeLocalSys) setBlockSize(bs uint64) {
	f.bs = bs
	f.write("devices/system/memory/block_size_bytes", strconv.FormatUint(bs, 16))
}

// blocks returns the memory blocks that lie wholly in the memory of a dax
// device.
func (f *fakeLocalSys) blocks(dax string) []int {
	var out []int
	for _, r := range f.ranges(dax) {
		for b := (r.Start + f.bs - 1) / f.bs; (b+1)*f.bs-1 <= r.End; b++ {
			out = append(out, int(b))
		}
	}
	return out
}

func blockPath(b int) string {
	return "devices/system/memory/memory" + strconv.Itoa(b)
}

// addBlocks adds the memory blocks of a dax device, offline.
func (f *fakeLocalSys) addBlocks(dax string) {
	if f.noKmemBlocks {
		return
	}
	for _, b := range f.blocks(dax) {
		f.write(blockPath(b)+"/state", "offline")
	}
}

// removeBlocks removes the memory blocks of a dax device, which must be
// offline.
func (f *fakeLocalSys) removeBlocks(dax string) {
	for _, b := range f.blocks(dax) {
		if _, err := os.Stat(f.p(blockPath(b))); err != nil {
			continue
		}
		if st := f.read(blockPath(b) + "/state"); st != "offline" {
			f.t.Errorf("memory%d of %s removed in state %q", b, dax, st)
		}
		_ = os.RemoveAll(f.p(blockPath(b)))
	}
}

// ranges returns the memory of a dax device (its mappingN).
func (f *fakeLocalSys) ranges(dax string) []memctl.Range {
	var out []memctl.Range
	for i := 0; ; i++ {
		m := localDaxRegion + "/" + dax + "/mapping" + strconv.Itoa(i)
		if _, err := os.Stat(f.p(m)); err != nil {
			return out
		}
		start, _ := strconv.ParseUint(strings.TrimPrefix(f.read(m+"/start"), "0x"), 16, 64)
		end, _ := strconv.ParseUint(strings.TrimPrefix(f.read(m+"/end"), "0x"), 16, 64)
		out = append(out, memctl.Range{Start: start, End: end})
	}
}

// setRanges gives a dax device the memory of ranges.
func (f *fakeLocalSys) setRanges(dax string, ranges []memctl.Range) {
	d := localDaxRegion + "/" + dax
	m, _ := filepath.Glob(f.p(d + "/mapping*"))
	for _, dir := range m {
		_ = os.RemoveAll(dir)
	}
	size, start := uint64(0), f.regionStart
	for i, r := range ranges {
		f.write(fmt.Sprintf("%s/mapping%d/start", d, i), fmt.Sprintf("0x%x", r.Start))
		f.write(fmt.Sprintf("%s/mapping%d/end", d, i), fmt.Sprintf("0x%x", r.End))
		size += r.Size()
	}
	if len(ranges) > 0 {
		start = ranges[0].Start
	}
	f.write(d+"/size", strconv.FormatUint(size, 10))
	f.write(d+"/resource", fmt.Sprintf("0x%x", start))
}

// gaps returns the memory of region0 that no dax device has, in address
// order.
func (f *fakeLocalSys) gaps() []memctl.Range {
	var used []memctl.Range
	m, _ := filepath.Glob(f.p(localDaxRegion + "/dax0.*"))
	for _, d := range m {
		used = append(used, f.ranges(filepath.Base(d))...)
	}
	sort.Slice(used, func(i, j int) bool { return used[i].Start < used[j].Start })
	var out []memctl.Range
	next := f.regionStart
	for _, u := range used {
		if u.Start > next {
			out = append(out, memctl.Range{Start: next, End: u.Start - 1})
		}
		next = max(next, u.End+1)
	}
	if end := f.regionStart + f.regionSize - 1; next <= end {
		out = append(out, memctl.Range{Start: next, End: end})
	}
	return out
}

// available returns the bytes of region0 that no dax device holds, and
// writes it to dax_region/available_size.
func (f *fakeLocalSys) available() uint64 {
	free := f.regionSize
	m, _ := filepath.Glob(f.p(localDaxRegion + "/dax0.*"))
	for _, d := range m {
		free -= f.daxSize(filepath.Base(d))
	}
	f.write(localDaxAttrs+"/available_size", strconv.FormatUint(free, 10))
	return free
}

// writeAttr writes a sysfs attribute, doing what the kernel does for the
// attributes of the dax bus.
func (f *fakeLocalSys) writeAttr(path, val string) error {
	rel, err := filepath.Rel(f.root, path)
	if err != nil {
		return err
	}
	if err := f.fail[rel]; err != nil {
		delete(f.fail, rel)
		return err
	}
	if q := f.failSeq[rel]; len(q) > 0 {
		f.failSeq[rel] = q[1:]
		if q[0] != nil {
			return q[0]
		}
	}
	dir, attr := filepath.Dir(rel), filepath.Base(rel)
	switch {
	case dir == localDaxAttrs && attr == "create":
		return f.createDax()
	case dir == localDaxAttrs && attr == "delete":
		return f.deleteDax(val)
	case filepath.Dir(dir) == "bus/dax/devices" && attr == "size":
		return f.resizeDax(filepath.Base(dir), val)
	case filepath.Dir(dir) == "bus/dax/devices" && attr == "mapping":
		return f.mapDax(filepath.Base(dir), val)
	case filepath.Dir(dir) == "bus/dax/drivers":
		return f.driverAttr(filepath.Base(dir), attr, val)
	}
	return os.WriteFile(path, []byte(val), 0o644)
}

// createDax makes a new dax device of size 0 the youngest of region0.
func (f *fakeLocalSys) createDax() error {
	if f.available() == 0 {
		return syscall.ENOSPC
	}
	dax := "dax0." + strconv.Itoa(f.nextDax)
	f.nextDax++
	d := localDaxRegion + "/" + dax
	f.write(d+"/target_node", "2")
	f.write(d+"/dev", "252:"+strings.TrimPrefix(dax, "dax0."))
	f.setRanges(dax, nil)
	f.link("bus/dax/devices/"+dax, "../../../"+d)
	f.write(localDaxAttrs+"/create", dax)
	return nil
}

// deleteDax deletes an unbound dax device of size 0, but never dax0.0.
func (f *fakeLocalSys) deleteDax(dax string) error {
	switch {
	case !f.daxExists(dax):
		return syscall.ENXIO
	case f.daxDriver(dax) != "" || f.daxSize(dax) != 0 || strings.HasSuffix(dax, ".0"):
		return syscall.EBUSY
	}
	_ = os.RemoveAll(f.p(localDaxRegion + "/" + dax))
	_ = os.Remove(f.p("bus/dax/devices/" + dax))
	f.available()
	return nil
}

// resizeDax sets the size of an unbound dax device: it shrinks from the
// end, and grows into the free memory of the region first-fit, extending
// its last range or adding ranges.
func (f *fakeLocalSys) resizeDax(dax, val string) error {
	size, err := strconv.ParseUint(val, 0, 64)
	cur := f.daxSize(dax)
	switch {
	case err != nil || size%(2*mib) != 0:
		return syscall.EINVAL
	case f.daxDriver(dax) != "":
		return syscall.EBUSY
	case size > cur && size-cur > f.available():
		return syscall.ENOSPC
	}
	ranges := f.ranges(dax)
	for drop := cur - min(cur, size); drop > 0; {
		last := &ranges[len(ranges)-1]
		if last.Size() <= drop {
			drop -= last.Size()
			ranges = ranges[:len(ranges)-1]
		} else {
			last.End -= drop
			drop = 0
		}
	}
	for need := size - min(cur, size); need > 0; {
		g := f.gaps()[0]
		n := min(need, g.Size())
		if k := len(ranges); k > 0 && ranges[k-1].End+1 == g.Start {
			ranges[k-1].End += n
		} else {
			ranges = append(ranges, memctl.Range{Start: g.Start, End: g.Start + n - 1})
		}
		f.setRanges(dax, ranges)
		need -= n
	}
	f.setRanges(dax, ranges)
	f.available()
	return nil
}

// mapDax adds the memory "0x<start>-0x<end>" to an unbound dax device, as
// the kernel's mapping attribute does: memory that is taken or outside the
// region fails with ENOMEM, a length that is not a multiple of the align is
// ignored.
func (f *fakeLocalSys) mapDax(dax, val string) error {
	s, e, ok := strings.Cut(val, "-")
	start, err1 := strconv.ParseUint(strings.TrimPrefix(s, "0x"), 16, 64)
	end, err2 := strconv.ParseUint(strings.TrimPrefix(e, "0x"), 16, 64)
	switch {
	case !ok || err1 != nil || err2 != nil || end < start:
		return syscall.EINVAL
	case f.daxDriver(dax) != "":
		return syscall.EBUSY
	case (end-start+1)%(2*mib) != 0:
		return nil
	}
	free := false
	for _, g := range f.gaps() {
		free = free || (g.Start <= start && end <= g.End)
	}
	if !free {
		return syscall.ENOMEM
	}
	f.setRanges(dax, append(f.ranges(dax), memctl.Range{Start: start, End: end}))
	f.available()
	return nil
}

// driverAttr binds and unbinds dax devices.
func (f *fakeLocalSys) driverAttr(driver, attr, dax string) error {
	switch attr {
	case "unbind":
		if f.daxDriver(dax) != driver {
			return syscall.ENODEV
		}
		f.setDriver(dax, "")
	case "new_id", "bind":
		switch {
		case f.daxDriver(dax) != "":
			return syscall.EBUSY
		case f.daxSize(dax) > 0:
			f.setDriver(dax, driver)
		case attr == "bind":
			return syscall.ENXIO
		}
	}
	return nil
}

// wantDax checks the size and the driver of a dax device.
func (f *fakeLocalSys) wantDax(t *testing.T, dax string, size uint64, driver string) {
	t.Helper()
	if !f.daxExists(dax) || f.daxSize(dax) != size || f.daxDriver(dax) != driver {
		t.Fatalf("%s: exists %v, size %d, driver %q; expected size %d, driver %q", dax, f.daxExists(dax), f.daxSize(dax), f.daxDriver(dax), size, driver)
	}
}

// wantRanges checks the memory of a dax device.
func (f *fakeLocalSys) wantRanges(t *testing.T, dax string, want ...memctl.Range) {
	t.Helper()
	if got := f.ranges(dax); !slices.Equal(got, want) {
		t.Fatalf("%s: memory %v, expected %v", dax, got, want)
	}
}

// blockRange returns the memory of n memory blocks from block b.
func blockRange(b, n uint64) memctl.Range {
	return memctl.Range{Start: b * localBlockSize, End: (b+n)*localBlockSize - 1}
}

// wantMemoryBlocks checks that exactly the memory blocks listed exist, in
// state.
func (f *fakeLocalSys) wantMemoryBlocks(t *testing.T, state string, blocks ...int) {
	t.Helper()
	m, _ := filepath.Glob(f.p("devices/system/memory/memory*"))
	got := []string{}
	for _, d := range m {
		got = append(got, filepath.Base(d)+" "+f.read("devices/system/memory/"+filepath.Base(d)+"/state"))
	}
	want := []string{}
	for _, b := range blocks {
		want = append(want, fmt.Sprintf("memory%d %s", b, state))
	}
	sort.Strings(got)
	sort.Strings(want)
	if !slices.Equal(got, want) {
		t.Fatalf("memory blocks %v, expected %v", got, want)
	}
}

// wantSlice checks a slice.
func wantSlice(t *testing.T, got *localSlice, want localSlice) {
	t.Helper()
	if got == nil || *got != want {
		t.Fatalf("slice %+v, expected %+v", got, want)
	}
}

// allocator returns a local allocator on the state file in dir, as after a
// restart.
func (f *fakeLocalSys) allocator(t *testing.T, dir string) *localAllocator {
	t.Helper()
	return f.allocatorOfBoot(t, dir, "boot-1")
}

// allocatorOfBoot returns a local allocator of the boot on the state file in
// dir.
func (f *fakeLocalSys) allocatorOfBoot(t *testing.T, dir, boot string) *localAllocator {
	t.Helper()
	a, err := newLocalAllocator(f.manager(), filepath.Join(dir, localStateFileName), boot)
	noErr(t, err)
	a.retryDelay = time.Millisecond
	a.zero = f.zero
	return a
}

// zero records the zeroing of a slice.
func (f *fakeLocalSys) zero(major, minor int, size, align uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.zeroErr != nil {
		return f.zeroErr
	}
	f.zeroed = append(f.zeroed, fmt.Sprintf("%d:%d %d %d", major, minor, size, align))
	return nil
}

func wantMode(t *testing.T, a *localAllocator, mode string) {
	t.Helper()
	m := a.Modes()
	if got, ok := m[localSerial]; (mode == "" && ok) || (mode != "" && got != mode) {
		t.Fatalf("modes %v, expected %q", m, mode)
	}
}

// acquire acquires a slice of the local memdev and checks the node.
func acquire(t *testing.T, a *localAllocator, uid, mode string, size uint64) *localSlice {
	t.Helper()
	s, node, err := a.Acquire(context.Background(), uid, "mem", localSerial, mode, size, false)
	noErr(t, err)
	if want := map[string]int{device.ModeNUMA: 2, device.ModeDax: -1}[mode]; node != want {
		t.Fatalf("%s: node %d, expected %d", uid, node, want)
	}
	return s
}

// The memory block size is read as hex, and again for every slice.
func TestLocalBlockSize(t *testing.T) {
	f := newFakeLocalSys(t)
	a := f.allocator(t, t.TempDir())
	// "8000000" is hex
	if bs := a.Snapshot().BlockSize; bs != 134217728 {
		t.Fatalf("block size %d of %q", bs, f.read("devices/system/memory/block_size_bytes"))
	}
	// 2 GiB blocks: a 128 MiB claim needs a 2 GiB slice
	f.write("devices/system/memory/block_size_bytes", "80000000")
	_, _, err := a.Acquire(context.Background(), "u1", "mem", localSerial, device.ModeNUMA, 128*mib, false)
	if !errors.Is(err, errNoRoom) || !strings.Contains(err.Error(), "the slice needs 2147483648") {
		t.Fatalf("expected no room for 2 GiB, got %v", err)
	}
	f.wantRegionReleased(t, "region0")
	if bs := a.Snapshot().BlockSize; bs != 2<<30 {
		t.Fatalf("block size %d after the change", bs)
	}
	f.write("devices/system/memory/block_size_bytes", "bogus")
	_, _, err = a.Acquire(context.Background(), "u1", "mem", localSerial, device.ModeDax, 128*mib, false)
	wantErr(t, err, "memory block size")
	// unknown at start: the snapshot reads it again
	_ = os.Remove(f.p("devices/system/memory/block_size_bytes"))
	b := f.allocator(t, t.TempDir())
	if bs := b.Snapshot().BlockSize; bs != 0 {
		t.Fatalf("block size %d without block_size_bytes", bs)
	}
	f.write("devices/system/memory/block_size_bytes", "8000000")
	if bs := b.Snapshot().BlockSize; bs != localBlockSize {
		t.Fatalf("block size %d", bs)
	}
	var none *localAllocator
	if none.Snapshot() != nil || none.Modes() != nil {
		t.Fatal("snapshot of a nil allocator")
	}
}

// Slices of both modes on one region: dax, numa, dax, numa. First-fit
// keeps every slice block-aligned, the blocks of each numa slice are
// onlined and offlined with it, the region goes with the last slice.
func TestLocalMixedSlices(t *testing.T) {
	f := newFakeLocalSys(t)
	a := f.allocator(t, t.TempDir())
	ctx := context.Background()

	// a dax slice of 64 MiB is rounded up to a block: dax0.0 is reused
	wantSlice(t, acquire(t, a, "u1", device.ModeDax, 64*mib),
		localSlice{Request: "mem", Region: "region0", Mode: device.ModeDax, Size: localBlockSize, Dax: "dax0.0", DaxDevice: "/dev/dax0.0", Major: 252})
	f.wantCommand(t, "cxl create-region -t ram -d decoder0.0 -m mem0")
	f.wantDax(t, "dax0.0", localBlockSize, "device_dax")
	f.wantRanges(t, "dax0.0", blockRange(74, 1))
	f.wantMemoryBlocks(t, "")
	wantMode(t, a, device.ModeDax)

	// a numa slice: the next block, onlined movable
	wantSlice(t, acquire(t, a, "u2", device.ModeNUMA, localBlockSize),
		localSlice{Request: "mem", Region: "region0", Mode: device.ModeNUMA, Size: localBlockSize, Dax: "dax0.1"})
	f.wantDax(t, "dax0.1", localBlockSize, "kmem")
	f.wantRanges(t, "dax0.1", blockRange(75, 1))
	f.wantMemoryBlocks(t, "online_movable", 75)
	wantMode(t, a, modeNUMADax)

	// a dax slice of two blocks, and a numa slice of two blocks
	acquire(t, a, "u3", device.ModeDax, 2*localBlockSize)
	f.wantRanges(t, "dax0.2", blockRange(76, 2))
	acquire(t, a, "u4", device.ModeNUMA, 2*localBlockSize-mib)
	f.wantDax(t, "dax0.3", 2*localBlockSize, "kmem")
	f.wantRanges(t, "dax0.3", blockRange(78, 2))
	f.wantMemoryBlocks(t, "online_movable", 75, 78, 79)
	if n := strings.Count(f.commands(), "create-region"); n != 1 {
		t.Fatalf("region created %d times", n)
	}
	if avail := f.available(); avail != localRegionSize-6*localBlockSize {
		t.Fatalf("available %d", avail)
	}

	// a numa release offlines its blocks only, and first-fit fills the gap
	noErr(t, a.Release(ctx, "u2"))
	if f.daxExists("dax0.1") {
		t.Fatal("dax0.1 not deleted")
	}
	f.wantMemoryBlocks(t, "online_movable", 78, 79)
	wantMode(t, a, modeNUMADax)
	acquire(t, a, "u5", device.ModeNUMA, localBlockSize)
	f.wantRanges(t, "dax0.4", blockRange(75, 1))
	f.wantMemoryBlocks(t, "online_movable", 75, 78, 79)

	// a dax release keeps dax0.0 with size 0
	noErr(t, a.Release(ctx, "u1"))
	f.wantDax(t, "dax0.0", 0, "")
	noErr(t, a.Release(ctx, "u4"))
	noErr(t, a.Release(ctx, "u5"))
	f.wantMemoryBlocks(t, "")
	wantMode(t, a, device.ModeDax)
	if strings.Contains(f.commands(), "destroy-region") {
		t.Fatalf("region released while a slice exists:\n%s", f.commands())
	}
	// the last slice takes the region with it, the memdev stays enabled
	noErr(t, a.Release(ctx, "u3"))
	f.wantRegionReleased(t, "region0")
	if _, err := os.Lstat(f.p(fakeMemPath + "/driver")); err != nil {
		t.Fatal("memdev disabled")
	}
	wantMode(t, a, "")
	noErr(t, a.Release(ctx, "u3"))

	// only dax slices are zeroed, after they are bound
	want := "252:0 134217728 2097152,252:2 268435456 2097152"
	if got := strings.Join(f.zeroed, ","); got != want {
		t.Fatalf("zeroed %s, expected %s", got, want)
	}
	// numa slices are bound to kmem through sysfs
	if strings.Contains(f.commands(), "system-ram") {
		t.Fatalf("daxctl bound kmem:\n%s", f.commands())
	}
}

func TestLocalDaxSlices(t *testing.T) {
	f := newFakeLocalSys(t)
	a := f.allocator(t, t.TempDir())
	ctx := context.Background()

	wantSlice(t, acquire(t, a, "u1", device.ModeDax, localBlockSize),
		localSlice{Request: "mem", Region: "region0", Mode: device.ModeDax, Size: localBlockSize, Dax: "dax0.0", DaxDevice: "/dev/dax0.0", Major: 252})
	s, _, err := a.Acquire(ctx, "u2", "mem", localSerial, device.ModeDax, 3*mib, true)
	noErr(t, err)
	wantSlice(t, s, localSlice{Request: "mem", Region: "region0", Mode: device.ModeDax, Size: localBlockSize, Dax: "dax0.1", DaxDevice: "/dev/dax0.1", Major: 252, Minor: 1, ReadOnly: true})
	// a slice larger than the free space
	_, _, err = a.Acquire(ctx, "u3", "mem", localSerial, device.ModeDax, localRegionSize, false)
	if !errors.Is(err, errNoRoom) || f.daxExists("dax0.2") {
		t.Fatalf("expected no room and no new dax device, got %v", err)
	}
	// releasing the first slice keeps dax0.0 with size 0 for the next
	noErr(t, a.Release(ctx, "u1"))
	f.wantDax(t, "dax0.0", 0, "")
	wantSlice(t, acquire(t, a, "u3", device.ModeDax, 16*mib),
		localSlice{Request: "mem", Region: "region0", Mode: device.ModeDax, Size: localBlockSize, Dax: "dax0.0", DaxDevice: "/dev/dax0.0", Major: 252})
	// a second slice of the same claim
	s, _, err = a.Acquire(ctx, "u3", "mem2", localSerial, device.ModeDax, 2*mib, false)
	noErr(t, err)
	if s.Dax != "dax0.2" || len(a.devices[localSerial].Claims["u3"]) != 2 {
		t.Fatalf("second slice %+v", s)
	}
	noErr(t, a.Release(ctx, "u2"))
	noErr(t, a.Release(ctx, "u3"))
	if f.daxExists("dax0.1") || f.daxExists("dax0.2") {
		t.Fatal("slices not deleted")
	}
	f.wantRegionReleased(t, "region0")
	wantMode(t, a, "")
}

// Without daxctl, slices of both modes are bound through sysfs.
func TestLocalSlicesSysfs(t *testing.T) {
	f := newFakeLocalSys(t)
	m := f.manager()
	m.LookPath = func(string) (string, error) { return "", errors.New("no daxctl") }
	a, err := newLocalAllocator(m, filepath.Join(t.TempDir(), localStateFileName), "boot-1")
	noErr(t, err)
	a.zero = f.zero
	acquire(t, a, "u1", device.ModeDax, 8*mib)
	f.wantDax(t, "dax0.0", localBlockSize, "device_dax")
	acquire(t, a, "u2", device.ModeNUMA, 8*mib)
	f.wantDax(t, "dax0.1", localBlockSize, "kmem")
	f.wantMemoryBlocks(t, "online_movable", 75)
	noErr(t, a.Release(context.Background(), "u2"))
	noErr(t, a.Release(context.Background(), "u1"))
	f.wantRegionReleased(t, "region0")
	if strings.Contains(f.commands(), "daxctl") {
		t.Fatalf("daxctl used:\n%s", f.commands())
	}
}

// A slice that first-fit places across a block boundary is placed on the
// first whole free blocks, combining free ranges. The region starts 64 MiB
// before memory74 (with 128 MiB blocks real regions are block-aligned:
// this exercises the placement only).
func TestLocalPlacement(t *testing.T) {
	f := newFakeLocalSys(t)
	// 512 MiB: a 64 MiB head, whole blocks 74, 75 and 76, a 64 MiB tail
	f.regionStart, f.regionSize = localRegionStart-64*mib, 512*mib
	a := f.allocator(t, t.TempDir())
	ctx := context.Background()

	// first-fit would give dax0.0 the head of the region
	acquire(t, a, "u1", device.ModeNUMA, localBlockSize)
	f.wantDax(t, "dax0.0", 0, "")
	f.wantDax(t, "dax0.1", localBlockSize, "kmem")
	f.wantRanges(t, "dax0.1", blockRange(74, 1))
	f.wantMemoryBlocks(t, "online_movable", 74)
	// dax slices too: never the head, never half a block
	acquire(t, a, "u2", device.ModeDax, localBlockSize)
	f.wantRanges(t, "dax0.2", blockRange(75, 1))
	acquire(t, a, "u3", device.ModeNUMA, localBlockSize)
	f.wantRanges(t, "dax0.3", blockRange(76, 1))
	f.wantMemoryBlocks(t, "online_movable", 74, 76)
	// 128 MiB is free, in the head and the tail: no whole block
	_, _, err := a.Acquire(ctx, "u4", "mem", localSerial, device.ModeDax, localBlockSize, false)
	if !errors.Is(err, errNoRoom) || !strings.Contains(err.Error(), "no 134217728 bytes of whole free memory blocks in region0") {
		t.Fatalf("expected no whole blocks, got %v", err)
	}
	if devs := f.manager().DaxDevices("region0"); !slices.Equal(devs, []string{"dax0.0", "dax0.1", "dax0.2", "dax0.3"}) {
		t.Fatalf("dax devices %v", devs)
	}
	f.wantDax(t, "dax0.0", 0, "")

	// two free blocks apart: a numa slice of two ranges, onlined and
	// offlined
	noErr(t, a.Release(ctx, "u1"))
	noErr(t, a.Release(ctx, "u3"))
	f.wantMemoryBlocks(t, "")
	acquire(t, a, "u5", device.ModeNUMA, 2*localBlockSize)
	f.wantRanges(t, "dax0.4", blockRange(74, 1), blockRange(76, 1))
	f.wantDax(t, "dax0.4", 2*localBlockSize, "kmem")
	f.wantMemoryBlocks(t, "online_movable", 74, 76)
	noErr(t, a.Release(ctx, "u5"))
	f.wantMemoryBlocks(t, "")
	noErr(t, a.Release(ctx, "u2"))
	f.wantRegionReleased(t, "region0")
}

// Memory blocks larger than the region alignment (256 MiB): a region can
// start up to block size - 256 MiB after a block boundary. The memdev
// publishes the whole blocks it is sure to have, and serves numa and dax
// claims in any order on them.
func TestLocalLargeBlocks(t *testing.T) {
	f := newFakeLocalSys(t)
	bs := uint64(512 * mib)
	f.setBlockSize(bs)
	// 2 GiB from 256 MiB after a block boundary: whole blocks 19, 20, 21
	f.regionSize = 2 << 30
	if f.regionStart%bs != 256*mib {
		t.Fatalf("region start 0x%x", f.regionStart)
	}
	block := func(b uint64) memctl.Range { return memctl.Range{Start: b * bs, End: (b+1)*bs - 1} }
	if p := localRequestPolicy(f.regionSize, bs); p.Default.String() != "1536Mi" || p.ValidRange.Step.String() != "512Mi" {
		t.Fatalf("request policy %+v", p)
	}
	a := f.allocator(t, t.TempDir())
	ctx := context.Background()
	acquire(t, a, "u1", device.ModeDax, bs)
	acquire(t, a, "u2", device.ModeNUMA, bs)
	acquire(t, a, "u3", device.ModeDax, bs)
	f.wantRanges(t, "dax0.1", block(19))
	f.wantRanges(t, "dax0.2", block(20))
	f.wantRanges(t, "dax0.3", block(21))
	f.wantMemoryBlocks(t, "online_movable", 20)
	// the scheduler admits no fourth block
	_, _, err := a.Acquire(ctx, "u4", "mem", localSerial, device.ModeNUMA, bs, false)
	if !errors.Is(err, errNoRoom) {
		t.Fatalf("expected no room, got %v", err)
	}
	// the released blocks serve the other mode
	noErr(t, a.Release(ctx, "u2"))
	noErr(t, a.Release(ctx, "u1"))
	acquire(t, a, "u5", device.ModeDax, bs)
	acquire(t, a, "u6", device.ModeNUMA, bs)
	f.wantRanges(t, "dax0.4", block(19))
	f.wantRanges(t, "dax0.5", block(20))
	f.wantMemoryBlocks(t, "online_movable", 20)
	for _, uid := range []string{"u3", "u5", "u6"} {
		noErr(t, a.Release(ctx, uid))
	}
	f.wantRegionReleased(t, "region0")
	f.wantMemoryBlocks(t, "")
}

// Failures while a slice is placed on whole blocks, bound or onlined leave
// nothing behind.
func TestLocalPlacementFailures(t *testing.T) {
	f := newFakeLocalSys(t)
	f.regionStart, f.regionSize = localRegionStart-64*mib, 512*mib
	a := f.allocator(t, t.TempDir())
	ctx := context.Background()

	// the misplaced first-fit device cannot be deleted
	f.failSeq = map[string][]error{"bus/dax/devices/dax0.0/size": {nil, syscall.EIO}}
	_, _, err := a.Acquire(ctx, "u1", "mem", localSerial, device.ModeNUMA, localBlockSize, false)
	wantErr(t, err, "dax0.0: set size 0: input/output error")
	f.wantRegionReleased(t, "region0")
	wantMode(t, a, "")

	acquire(t, a, "u1", device.ModeDax, localBlockSize)
	f.wantRanges(t, "dax0.1", blockRange(74, 1))
	// the placed device cannot be mapped
	f.fail = map[string]error{"bus/dax/devices/dax0.2/mapping": syscall.EIO}
	_, _, err = a.Acquire(ctx, "u2", "mem", localSerial, device.ModeNUMA, localBlockSize, false)
	wantErr(t, err, "dax0.2: map 0x258000000-0x25fffffff: input/output error")
	if f.daxExists("dax0.2") {
		t.Fatal("a device that could not be mapped is left")
	}
	// the placed device cannot be bound to kmem: it goes, dax0.0 stays
	f.fail = map[string]error{"bus/dax/drivers/kmem/bind": syscall.EIO}
	_, _, err = a.Acquire(ctx, "u2", "mem", localSerial, device.ModeNUMA, localBlockSize, false)
	wantErr(t, err, "bind dax0.3 to kmem: input/output error")
	if f.daxExists("dax0.3") {
		t.Fatal("a placed device that could not be bound is left")
	}
	f.wantDax(t, "dax0.0", 0, "")
	f.wantDax(t, "dax0.1", localBlockSize, "device_dax")
	// the free memory of the region cannot be read
	f.write("bus/cxl/devices/region0/size", "bogus")
	_, _, err = a.Acquire(ctx, "u2", "mem", localSerial, device.ModeDax, localBlockSize, false)
	wantErr(t, err, "cannot parse region region0")
	f.write("bus/cxl/devices/region0/size", fmt.Sprintf("0x%x", f.regionSize))
	f.wantDax(t, "dax0.0", 0, "")
	if devs := f.manager().DaxDevices("region0"); len(devs) != 2 {
		t.Fatalf("dax devices %v", devs)
	}
	if len(a.devices[localSerial].Claims) != 1 {
		t.Fatalf("claims %+v", a.devices[localSerial].Claims)
	}
	f.wantMemoryBlocks(t, "")

	// the memory blocks of kmem never appear
	f.noKmemBlocks = true
	tctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, _, err = a.Acquire(tctx, "u2", "mem", localSerial, device.ModeNUMA, localBlockSize, false)
	wantErr(t, err, "0 of 1 memory blocks appeared: context deadline exceeded")
	f.noKmemBlocks = false
	if devs := f.manager().DaxDevices("region0"); len(devs) != 2 {
		t.Fatalf("dax devices %v", devs)
	}
	noErr(t, a.Release(ctx, "u1"))
	f.wantRegionReleased(t, "region0")
}

// A slice whose release failed is never handed out again: it is kept for
// orphanOwner, released before the next slice of its memdev, and kmem is
// never unbound from online memory.
func TestLocalFailedReleases(t *testing.T) {
	f := newFakeLocalSys(t)
	dir := t.TempDir()
	a := f.allocator(t, dir)
	ctx := context.Background()
	acquire(t, a, "u1", device.ModeNUMA, localBlockSize)
	acquire(t, a, "u2", device.ModeDax, localBlockSize)
	f.wantDax(t, "dax0.0", localBlockSize, "kmem")
	f.wantMemoryBlocks(t, "online_movable", 74)

	// restart: u1 is not prepared, and its memory cannot be offlined
	busy := []error{syscall.EBUSY, syscall.EBUSY}
	f.failSeq = map[string][]error{blockPath(74) + "/state": busy}
	a = f.allocator(t, dir)
	a.Reconcile(ctx, map[string]bool{"u2": true})
	d := a.devices[localSerial]
	if len(d.Claims[orphanOwner]) != 1 || d.Claims[orphanOwner][0].Dax != "dax0.0" || d.Claims["u1"] != nil {
		t.Fatalf("claims after reconcile %+v", d.Claims)
	}
	wantMode(t, a, modeNUMADax)
	data, _ := os.ReadFile(filepath.Join(dir, localStateFileName))
	if !strings.Contains(string(data), `"-": [`) {
		t.Fatalf("state file lacks the orphan:\n%s", data)
	}
	// the next slice fails while the orphan cannot be released
	f.failSeq = map[string][]error{blockPath(74) + "/state": {syscall.EBUSY}}
	_, _, err := a.Acquire(ctx, "u3", "mem", localSerial, device.ModeDax, localBlockSize, false)
	wantErr(t, err, "releasing slice dax0.0: cannot offline its memory (in use?)")
	f.wantDax(t, "dax0.0", localBlockSize, "kmem")
	f.wantMemoryBlocks(t, "online_movable", 74)
	// then it is released, and dax0.0 is reused
	s := acquire(t, a, "u3", device.ModeDax, localBlockSize)
	if s.Dax != "dax0.0" || len(a.devices[localSerial].Claims[orphanOwner]) != 0 {
		t.Fatalf("slice %+v, claims %+v", s, a.devices[localSerial].Claims)
	}
	f.wantMemoryBlocks(t, "")
	wantMode(t, a, device.ModeDax)

	// a numa slice whose rollback cannot offline it
	f.failSeq = map[string][]error{
		blockPath(77) + "/state": {syscall.EIO},
		blockPath(76) + "/state": {nil, syscall.EBUSY},
	}
	_, _, err = a.Acquire(ctx, "u4", "mem", localSerial, device.ModeNUMA, 2*localBlockSize, false)
	wantErr(t, err, "memory77: online_movable: input/output error")
	if own := a.devices[localSerial].Claims[orphanOwner]; len(own) != 1 || own[0].Dax != "dax0.2" {
		t.Fatalf("orphans %+v", a.devices[localSerial].Claims)
	}
	f.wantDax(t, "dax0.2", 2*localBlockSize, "kmem")
	// the last claim keeps the region for the orphan; the next Acquire
	// releases the orphan first
	noErr(t, a.Release(ctx, "u2"))
	f.failSeq = map[string][]error{blockPath(76) + "/state": {syscall.EBUSY}}
	noErr(t, a.Release(ctx, "u3"))
	if strings.Contains(f.commands(), "destroy-region") || len(a.devices[localSerial].Claims) != 1 {
		t.Fatalf("region released with an orphan, claims %+v:\n%s", a.devices[localSerial].Claims, f.commands())
	}
	acquire(t, a, "u5", device.ModeNUMA, localBlockSize)
	if f.daxExists("dax0.2") || len(a.devices[localSerial].Claims) != 1 {
		t.Fatalf("orphan dax0.2 not released, claims %+v", a.devices[localSerial].Claims)
	}
	noErr(t, a.Release(ctx, "u5"))
	f.wantRegionReleased(t, "region0")
	f.wantMemoryBlocks(t, "")
}

// A kmem device with online memory that the state does not have (a crash
// after the online) is not reused, and kmem is not unbound from it; a
// restart offlines and deletes it.
func TestLocalUntrackedKmem(t *testing.T) {
	f := newFakeLocalSys(t)
	dir := t.TempDir()
	a := f.allocator(t, dir)
	ctx := context.Background()
	acquire(t, a, "u1", device.ModeNUMA, localBlockSize)
	acquire(t, a, "u2", device.ModeDax, localBlockSize)
	delete(a.devices[localSerial].Claims, "u1")
	s := acquire(t, a, "u3", device.ModeDax, localBlockSize)
	if s.Dax == "dax0.0" {
		t.Fatalf("dax0.0 with online memory reused: %+v", s)
	}
	f.wantDax(t, "dax0.0", localBlockSize, "kmem")
	f.wantMemoryBlocks(t, "online_movable", 74)
	wantErr(t, a.mem.ResizeDaxDevice(ctx, "dax0.0", 0), "dax0.0: memory blocks [74] are not offline")
	a = f.allocator(t, dir)
	a.Reconcile(ctx, map[string]bool{"u2": true, "u3": true})
	f.wantDax(t, "dax0.0", 0, "")
	f.wantMemoryBlocks(t, "")
}

// A numa slice that fails to offline keeps its claim until a retry;
// other claims are not affected.
func TestLocalNUMAReleaseFailure(t *testing.T) {
	f := newFakeLocalSys(t)
	a := f.allocator(t, t.TempDir())
	ctx := context.Background()
	acquire(t, a, "u1", device.ModeNUMA, 2*localBlockSize)
	f.fail = map[string]error{"devices/system/memory/memory75/state": syscall.EBUSY}
	err := a.Release(ctx, "u1")
	wantErr(t, err, "releasing slice dax0.0: cannot offline its memory (in use?)")
	wantErr(t, err, "device or resource busy")
	if f.read(blockPath(74)+"/state") != "offline" || f.read(blockPath(75)+"/state") != "online_movable" {
		t.Fatalf("memory74 %q, memory75 %q", f.read(blockPath(74)+"/state"), f.read(blockPath(75)+"/state"))
	}
	f.wantDax(t, "dax0.0", 2*localBlockSize, "kmem")
	if len(a.devices[localSerial].Claims["u1"]) != 1 {
		t.Fatalf("claims %+v", a.devices[localSerial].Claims)
	}
	// another numa claim gets a slice of its own meanwhile
	acquire(t, a, "u2", device.ModeNUMA, localBlockSize)
	f.wantRanges(t, "dax0.1", blockRange(76, 1))
	noErr(t, a.Release(ctx, "u1"))
	f.wantMemoryBlocks(t, "online_movable", 76)
	if strings.Contains(f.commands(), "destroy-region") {
		t.Fatalf("region destroyed while u2 uses it:\n%s", f.commands())
	}
	// a slice that fails to delete after the offline
	f.fail = map[string]error{"bus/dax/devices/dax0.1/size": syscall.EBUSY}
	wantErr(t, a.Release(ctx, "u2"), "releasing slice dax0.1")
	f.wantMemoryBlocks(t, "")
	f.wantDax(t, "dax0.1", localBlockSize, "")
	noErr(t, a.Release(ctx, "u2"))
	f.wantRegionReleased(t, "region0")
}

func TestLocalRegionErrors(t *testing.T) {
	f := newFakeLocalSys(t)
	dir := t.TempDir()
	a := f.allocator(t, dir)
	ctx := context.Background()

	// a transient region creation failure is retried once
	f.failCreate = 1
	acquire(t, a, "u1", device.ModeNUMA, localBlockSize)
	if n := strings.Count(f.commands(), "create-region"); n != 2 {
		t.Fatalf("region created %d times:\n%s", n, f.commands())
	}
	noErr(t, a.Release(ctx, "u1"))

	// two failures make the claim fail and leave nothing
	f.failCreate = 2
	_, _, err := a.Acquire(ctx, "u1", "mem", localSerial, device.ModeNUMA, localBlockSize, false)
	wantErr(t, err, "add_dev")
	wantMode(t, a, "")

	// a region that the plugin did not create
	_, err = f.exec(ctx, "cxl", "create-region", "-t", "ram", "-d", "decoder0.0", "-m", "mem0")
	noErr(t, err)
	_, _, err = a.Acquire(ctx, "u1", "mem", localSerial, device.ModeDax, localBlockSize, false)
	wantErr(t, err, "memdev mem0 has region region0 that the plugin did not create")
	wantMode(t, a, "")

	// unknown memdev, bad arguments, disabled allocator
	_, _, err = a.Acquire(ctx, "u1", "mem", 0x1234, device.ModeDax, localBlockSize, false)
	if !errors.Is(err, memctl.ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
	_, _, err = a.Acquire(ctx, "u1", "mem", localSerial, "ram", localBlockSize, false)
	wantErr(t, err, `invalid mode "ram"`)
	_, _, err = a.Acquire(ctx, "u1", "mem", localSerial, device.ModeDax, 0, false)
	wantErr(t, err, "size 0")
	var none *localAllocator
	_, _, err = none.Acquire(ctx, "u1", "mem", localSerial, device.ModeDax, localBlockSize, false)
	wantErr(t, err, "manageMemdevs: false")
	noErr(t, none.Release(ctx, "u1"))
	none.Reconcile(ctx, nil)
}

func TestLocalStateAndReconcile(t *testing.T) {
	f := newFakeLocalSys(t)
	dir := t.TempDir()
	a := f.allocator(t, dir)
	ctx := context.Background()
	acquire(t, a, "u1", device.ModeDax, localBlockSize)
	acquire(t, a, "u2", device.ModeNUMA, localBlockSize)
	acquire(t, a, "u3", device.ModeDax, localBlockSize)
	st, err := os.Stat(filepath.Join(dir, localStateFileName))
	if err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("state file: %v %v", st, err)
	}
	data, _ := os.ReadFile(filepath.Join(dir, localStateFileName))
	for _, want := range []string{`"serialHex": "c100e2e1"`, `"region": "region0"`, `"mode": "numa+dax"`, `"mode": "numa"`, `"dax": "dax0.1"`, `"daxDevice": "/dev/dax0.2"`} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("state file lacks %s:\n%s", want, data)
		}
	}

	// restart: the numa claim u2 is not prepared any more: its memory is
	// offlined and its slice deleted
	a = f.allocator(t, dir)
	wantMode(t, a, modeNUMADax)
	a.Reconcile(ctx, map[string]bool{"u1": true, "u3": true})
	if f.daxExists("dax0.1") || len(a.devices[localSerial].Claims) != 2 {
		t.Fatalf("after reconcile: %+v", a.devices[localSerial])
	}
	f.wantMemoryBlocks(t, "")
	wantMode(t, a, device.ModeDax)

	// restart: a dax device that belongs to no claim (a crash before the
	// state was saved) is deleted, a kmem one offlined first
	m := f.manager()
	dax, err := m.CreateDaxDevice(ctx, "region0", localBlockSize)
	noErr(t, err)
	noErr(t, m.SetDaxDriverSysfs(ctx, dax, memctl.DriverKmem))
	noErr(t, m.SetBlocksState(f.blocks(dax), "online_movable"))
	a = f.allocator(t, dir)
	a.Reconcile(ctx, map[string]bool{"u1": true, "u3": true})
	if f.daxExists(dax) {
		t.Fatalf("orphan %s not deleted", dax)
	}
	f.wantMemoryBlocks(t, "")
	f.wantDax(t, "dax0.2", localBlockSize, "device_dax")

	// restart: the slice of u1 is gone (size changed), u3 is not prepared:
	// the region goes
	noErr(t, m.ResizeDaxDevice(ctx, "dax0.0", 2*localBlockSize))
	a = f.allocator(t, dir)
	a.Reconcile(ctx, map[string]bool{"u1": true})
	wantMode(t, a, "")
	f.wantRegionReleased(t, "region0")

	// restart: a region of the state with no claims is destroyed
	acquire(t, a, "u1", device.ModeNUMA, localBlockSize)
	a.devices[localSerial].Claims = map[string][]*localSlice{}
	noErr(t, a.save())
	a = f.allocator(t, dir)
	wantMode(t, a, modeNone)
	a.Reconcile(ctx, map[string]bool{"u1": true})
	wantMode(t, a, "")
	f.wantRegionReleased(t, "region0")
	f.wantMemoryBlocks(t, "")

	// restart: the region is gone (reboot) or the memdev is gone
	acquire(t, a, "u1", device.ModeNUMA, localBlockSize)
	noErr(t, a.mem.SetBlocksState(f.blocks("dax0.0"), "offline"))
	_, err = f.exec(ctx, "cxl", "destroy-region", "region0")
	noErr(t, err)
	a = f.allocator(t, dir)
	a.Reconcile(ctx, map[string]bool{"u1": true})
	wantMode(t, a, "")
	acquire(t, a, "u1", device.ModeDax, localBlockSize)
	f.unplug()
	a = f.allocator(t, dir)
	a.Reconcile(ctx, map[string]bool{"u1": true})
	wantMode(t, a, "")

	// a broken state file
	f.write("broken/"+localStateFileName, "{")
	_, err = newLocalAllocator(f.manager(), f.p("broken/"+localStateFileName), "boot-1")
	wantErr(t, err, "parsing")
}

// A state file with a mode per memdev and none per slice: the slices get
// the mode of their memdev. A numa memdev of it has its whole region
// online: more numa claims share it, a dax claim finds no room.
func TestLocalStateMigration(t *testing.T) {
	f := newFakeLocalSys(t)
	dir := t.TempDir()
	ctx := context.Background()
	// the region of a numa memdev: dax0.0 is kmem with all memory online
	_, err := f.exec(ctx, "cxl", "create-region", "-t", "ram", "-d", "decoder0.0", "-m", "mem0")
	noErr(t, err)
	noErr(t, f.manager().SetBlocksState(f.blocks("dax0.0"), "online_movable"))
	noErr(t, os.WriteFile(filepath.Join(dir, localStateFileName), []byte(`{"bootID":"boot-1","devices":[
		{"serial":3238060769,"serialHex":"c100e2e1","memdev":"mem0","region":"region0","mode":"numa","node":2,"dax":"dax0.0",
		 "claims":{"u1":[{"request":"mem","region":"region0","size":67108864}]}}]}`), 0o600))
	a := f.allocator(t, dir)
	wantMode(t, a, device.ModeNUMA)
	if s := a.devices[localSerial].Claims["u1"][0]; s.Mode != device.ModeNUMA || s.Dax != "" {
		t.Fatalf("migrated slice %+v", s)
	}
	a.Reconcile(ctx, map[string]bool{"u1": true, "u2": true})
	wantMode(t, a, device.ModeNUMA)
	ncreate := strings.Count(f.commands(), "create-region")
	wantSlice(t, acquire(t, a, "u2", device.ModeNUMA, localBlockSize),
		localSlice{Request: "mem", Region: "region0", Mode: device.ModeNUMA, Size: localBlockSize})
	_, _, err = a.Acquire(ctx, "u3", "mem", localSerial, device.ModeDax, localBlockSize, false)
	if !errors.Is(err, errNoRoom) {
		t.Fatalf("expected no room for a dax slice, got %v", err)
	}
	f.wantDax(t, "dax0.0", localRegionSize, "kmem")
	if strings.Count(f.commands(), "create-region") != ncreate {
		t.Fatalf("region created again:\n%s", f.commands())
	}
	noErr(t, a.Release(ctx, "u1"))
	noErr(t, a.Release(ctx, "u2"))
	f.wantRegionReleased(t, "region0")
	f.wantMemoryBlocks(t, "")

	// a dax memdev: its slices are dax slices
	acquire(t, a, "u1", device.ModeDax, localBlockSize)
	acquire(t, a, "u2", device.ModeDax, localBlockSize)
	var st localState
	data, err := os.ReadFile(filepath.Join(dir, localStateFileName))
	noErr(t, err)
	noErr(t, json.Unmarshal(data, &st))
	for _, own := range st.Devices[0].Claims {
		own[0].Mode = ""
	}
	noErr(t, writeJSONFile(filepath.Join(dir, localStateFileName), st))
	a = f.allocator(t, dir)
	wantMode(t, a, device.ModeDax)
	for _, uid := range []string{"u1", "u2"} {
		if s := a.devices[localSerial].Claims[uid][0]; s.Mode != device.ModeDax || s.Dax == "" {
			t.Fatalf("migrated slice %+v", s)
		}
	}
	noErr(t, a.Release(ctx, "u2"))
	noErr(t, a.Release(ctx, "u1"))
	f.wantRegionReleased(t, "region0")
}

// wantRegionReleased checks that the region was released last, and the
// memdev not disabled.
func (f *fakeLocalSys) wantRegionReleased(t *testing.T, region string) {
	t.Helper()
	if !strings.HasSuffix(f.commands(), "cxl disable-region "+region+"\ncxl destroy-region "+region) {
		t.Fatalf("%s not released:\n%s", region, f.commands())
	}
}

func TestLocalSliceFailures(t *testing.T) {
	f := newFakeLocalSys(t)
	a := f.allocator(t, t.TempDir())
	ctx := context.Background()

	// the region does not come up in devdax mode: the region that the
	// failed creation left goes again
	f.failDaxctl = errors.New("daxctl: enable failed")
	_, _, err := a.Acquire(ctx, "u1", "mem", localSerial, device.ModeDax, 8*mib, false)
	wantErr(t, err, "enable failed")
	f.wantRegionReleased(t, "region0")
	wantMode(t, a, "")
	f.failDaxctl = nil

	// zeroing fails: the same
	f.zeroErr = errors.New("mmap: no such device")
	_, _, err = a.Acquire(ctx, "u1", "mem", localSerial, device.ModeDax, 8*mib, false)
	wantErr(t, err, "zeroing dax0.0: mmap: no such device")
	f.wantRegionReleased(t, "region0")
	wantMode(t, a, "")
	f.zeroErr = nil

	// kmem cannot be bound, or the memory not onlined: the same, no
	// memory is left online
	f.fail = map[string]error{"bus/dax/drivers/kmem/bind": syscall.EIO}
	_, _, err = a.Acquire(ctx, "u1", "mem", localSerial, device.ModeNUMA, 8*mib, false)
	wantErr(t, err, "bind dax0.0 to kmem: input/output error")
	f.wantRegionReleased(t, "region0")
	f.fail = map[string]error{"devices/system/memory/memory75/state": syscall.EIO}
	_, _, err = a.Acquire(ctx, "u1", "mem", localSerial, device.ModeNUMA, 2*localBlockSize, false)
	wantErr(t, err, "memory75: online_movable: input/output error")
	f.wantRegionReleased(t, "region0")
	f.wantMemoryBlocks(t, "")
	wantMode(t, a, "")

	// a later slice fails to resize, to bind or to online: only that
	// slice goes
	acquire(t, a, "u1", device.ModeDax, 8*mib)
	f.fail = map[string]error{"bus/dax/devices/dax0.1/size": syscall.EIO}
	_, _, err = a.Acquire(ctx, "u2", "mem", localSerial, device.ModeDax, 8*mib, false)
	wantErr(t, err, "input/output error")
	if f.daxExists("dax0.1") {
		t.Fatal("a slice that failed to resize was left")
	}
	f.fail = nil
	f.failDaxctl = errors.New("daxctl: enable failed")
	_, _, err = a.Acquire(ctx, "u2", "mem", localSerial, device.ModeDax, 8*mib, false)
	wantErr(t, err, "enable failed")
	if f.daxExists("dax0.2") {
		t.Fatal("a slice that failed to bind was left")
	}
	f.failDaxctl = nil
	f.fail = map[string]error{"devices/system/memory/memory76/state": syscall.EIO}
	_, _, err = a.Acquire(ctx, "u2", "mem", localSerial, device.ModeNUMA, 2*localBlockSize, false)
	wantErr(t, err, "memory76: online_movable")
	if f.daxExists("dax0.3") {
		t.Fatal("a slice that failed to online was left")
	}
	f.wantMemoryBlocks(t, "")
	f.wantDax(t, "dax0.0", localBlockSize, "device_dax")
	if len(a.devices[localSerial].Claims) != 1 {
		t.Fatalf("claims %+v", a.devices[localSerial].Claims)
	}
	wantMode(t, a, device.ModeDax)

	// a slice that fails to delete stays with its claim until a retry
	f.fail = map[string]error{localDaxAttrs + "/delete": syscall.EBUSY}
	acquire(t, a, "u3", device.ModeDax, 8*mib)
	wantErr(t, a.Release(ctx, "u3"), "releasing slice")
	if !f.daxExists("dax0.4") || len(a.devices[localSerial].Claims["u3"]) != 1 {
		t.Fatalf("after a failed release: %+v", a.devices[localSerial].Claims)
	}
	f.fail = nil
	noErr(t, a.Release(ctx, "u3"))
	if f.daxExists("dax0.4") {
		t.Fatal("slice not deleted on the retry")
	}
}

// The state of another boot is forgotten, and an entry whose region was
// being created is dropped, both without touching the memdev.
func TestLocalStateOfAnotherBoot(t *testing.T) {
	f := newFakeLocalSys(t)
	dir := t.TempDir()
	a := f.allocator(t, dir)
	ctx := context.Background()
	acquire(t, a, "u1", device.ModeNUMA, localBlockSize)
	data, _ := os.ReadFile(filepath.Join(dir, localStateFileName))
	if !strings.Contains(string(data), `"bootID": "boot-1"`) {
		t.Fatalf("state file lacks the boot id:\n%s", data)
	}
	ncmds := len(f.commands())

	// an unknown boot keeps the state, another boot forgets it
	c := f.allocatorOfBoot(t, dir, "")
	wantMode(t, c, device.ModeNUMA)
	b := f.allocatorOfBoot(t, dir, "boot-2")
	wantMode(t, b, "")
	b.Reconcile(ctx, map[string]bool{"u1": true})
	if len(f.commands()) != ncmds {
		t.Fatalf("the state of another boot touched the memdev:\n%s", f.commands()[ncmds:])
	}

	// an entry without a region
	noErr(t, os.WriteFile(filepath.Join(dir, localStateFileName),
		[]byte(`{"bootID":"boot-1","devices":[{"serial":3238060769,"serialHex":"c100e2e1","memdev":"mem0","mode":"numa","node":-1,"claims":{}}]}`), 0o600))
	a = f.allocator(t, dir)
	wantMode(t, a, modeNone)
	a.Reconcile(ctx, nil)
	wantMode(t, a, "")
	if len(f.commands()) != ncmds {
		t.Fatalf("an entry without a region touched the memdev:\n%s", f.commands()[ncmds:])
	}

	// a list of devices without a boot id is the state of an unknown boot
	f.write("list/"+localStateFileName, `[{"serial":3238060769,"serialHex":"c100e2e1","memdev":"mem0","region":"region0","mode":"numa","node":2,"claims":{"u1":[{"request":"mem","region":"region0","size":2097152}]}}]`)
	l := f.allocatorOfBoot(t, f.p("list"), "boot-1")
	wantMode(t, l, "")
	l = f.allocatorOfBoot(t, f.p("list"), "")
	wantMode(t, l, device.ModeNUMA)
}

func TestZeroFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "slice")
	data := make([]byte, 6*mib+512)
	for i := range data {
		data[i] = 0xa5
	}
	noErr(t, os.WriteFile(path, data, 0o600))
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	noErr(t, err)
	defer file.Close()
	noErr(t, zeroFile(file, 6*mib, 4*mib))
	got, err := os.ReadFile(path)
	noErr(t, err)
	for i := 0; i < 6*mib; i++ {
		if got[i] != 0 {
			t.Fatalf("byte %d is %#x", i, got[i])
		}
	}
	if got[6*mib] != 0xa5 {
		t.Fatal("zeroed past the size")
	}
	wantErr(t, zeroDax(t.TempDir(), 0, 0, mib, 2*mib), "")
}
