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
	"errors"
	"maps"
	"slices"
	"testing"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"

	nricxl "github.com/containers/nri-plugins/pkg/cxl"
)

// slice returns a ResourceSlice of the driver whose devices have the
// attributes.
func slice(name, driver string, attrs ...map[resourceapi.QualifiedName]resourceapi.DeviceAttribute) *resourceapi.ResourceSlice {
	rs := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       resourceapi.ResourceSliceSpec{Driver: driver, Pool: resourceapi.ResourcePool{Name: name, ResourceSliceCount: 1}},
	}
	for i, a := range attrs {
		rs.Spec.Devices = append(rs.Spec.Devices, resourceapi.Device{Name: name + "-" + string(rune('a'+i)), Attributes: a})
	}
	return rs
}

func serialAttr(key, serial string) map[resourceapi.QualifiedName]resourceapi.DeviceAttribute {
	return map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{resourceapi.QualifiedName(key): {StringValue: ptr(serial)}}
}

// poolTestDevices are local memdevs with the serial of a pool device
// (0xc1ee0001) and of a free local device.
func poolTestDevices() *nricxl.Devices {
	pooled := localMemdev()
	pooled.Name, pooled.Serial = "mem1", 0xc1ee0001
	return &nricxl.Devices{MemoryDevices: []*nricxl.MemoryDevice{localMemdev(), pooled}}
}

func scanNames(t *testing.T, ctx context.Context, cfg *DriverConfig, ps *poolSlices) []string {
	t.Helper()
	scanConfig, serials := scanInput(ctx, cfg, nil, ps)
	devs, err := buildDevInfos(scanConfig, poolTestDevices(), serials, &localSnapshot{BlockSize: localBlockSize})
	noErr(t, err)
	return slices.Sorted(maps.Keys(devs))
}

func TestPoolSlicesSerials(t *testing.T) {
	client := fake.NewClientset(
		slice("pool", device.PoolDriverName, serialAttr("serial", "0xC1EE0001"), serialAttr("serial", "bogus"), nil),
		slice("other", "other.example.com", serialAttr("other.example.com/serial", "3238060770")),
		slice("node1", device.DriverName, serialAttr("serial", "0xc100e2e1")),
	)
	ps := newPoolSlices(client)
	ctx := context.Background()
	serials, known := ps.Serials(ctx)
	if !known || ps.Failing() || !maps.Equal(serials, map[uint64]bool{0xc1ee0001: true, 3238060770: true}) {
		t.Fatalf("serials %v known %v failing %v", serials, known, ps.Failing())
	}
	// a pool device is not a local memdev, the others are
	cfg := &DriverConfig{IgnoreDRAMNodes: true}
	if got := scanNames(t, ctx, cfg, ps); !slices.Equal(got, []string{localDeviceName}) {
		t.Fatalf("published %v", got)
	}

	// an API failure keeps the serials of the last list
	client.PrependReactor("list", "resourceslices", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver unavailable")
	})
	serials, known = ps.Serials(ctx)
	if !known || !ps.Failing() || !serials[0xc1ee0001] {
		t.Fatalf("after a failed list: serials %v known %v failing %v", serials, known, ps.Failing())
	}
	if got := scanNames(t, ctx, cfg, ps); !slices.Equal(got, []string{localDeviceName}) {
		t.Fatalf("published %v after a failed list", got)
	}
	if newPoolSlices(nil) != nil {
		t.Fatal("poolSlices without a client")
	}
}

func TestPoolSlicesFirstListFails(t *testing.T) {
	client := fake.NewClientset(slice("pool", device.PoolDriverName, serialAttr("serial", "0xc1ee0001")))
	fail := true
	client.PrependReactor("list", "resourceslices", func(k8stesting.Action) (bool, runtime.Object, error) {
		if fail {
			return true, nil, errors.New("apiserver unavailable")
		}
		return false, nil, nil
	})
	ps := newPoolSlices(client)
	ctx := context.Background()
	cfg := &DriverConfig{IgnoreDRAMNodes: true}
	// nothing is known: no local memdev at all, regions stay as they are
	if got := scanNames(t, ctx, cfg, ps); len(got) != 0 || !ps.Failing() {
		t.Fatalf("published %v before the first successful list (failing %v)", got, ps.Failing())
	}
	if cfg.ManageMemdevs != nil {
		t.Fatal("the configuration was changed")
	}
	fail = false
	if got := scanNames(t, ctx, cfg, ps); !slices.Equal(got, []string{localDeviceName}) || ps.Failing() {
		t.Fatalf("published %v after the first successful list (failing %v)", got, ps.Failing())
	}
	// without pool controllers (no client) everything is known
	if got := scanNames(t, ctx, cfg, nil); !slices.Equal(got, []string{localDeviceName, "memdev-c1ee0001"}) {
		t.Fatalf("published %v without a client", got)
	}
}
