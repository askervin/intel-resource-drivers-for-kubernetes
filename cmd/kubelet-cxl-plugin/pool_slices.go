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
	"maps"
	"strconv"
	"sync"
	"time"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreclientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
)

// A memdev whose serial a pool controller publishes in its ResourceSlices
// is a pool device, also before the pool helper prepares it for a claim:
// cxl.generic never publishes it as a local memdev.

// poolListRetry is how often the devices of pool controllers are listed
// again while the last list failed.
const poolListRetry = 30 * time.Second

// poolSlices finds the serials of the devices in the ResourceSlices of the
// DRA drivers other than cxl.generic.
type poolSlices struct {
	list    func(ctx context.Context) ([]resourceapi.ResourceSlice, error)
	mu      sync.Mutex
	serials map[uint64]bool // of the last successful list
	known   bool            // a list succeeded
	failing bool            // the last list failed
}

// newPoolSlices returns a poolSlices that lists ResourceSlices through the
// client, nil without a client.
func newPoolSlices(client coreclientset.Interface) *poolSlices {
	if client == nil {
		return nil
	}
	return &poolSlices{
		list: func(ctx context.Context) ([]resourceapi.ResourceSlice, error) {
			l, err := client.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{})
			if err != nil {
				return nil, err
			}
			return l.Items, nil
		},
	}
}

// Serials lists the ResourceSlices and returns the serials of the devices
// of drivers other than cxl.generic, and whether they are known. After a
// failed list it returns the serials of the last successful one, and none
// before the first one succeeded (not known). A nil poolSlices knows that
// there are none.
func (p *poolSlices) Serials(ctx context.Context) (map[uint64]bool, bool) {
	if p == nil {
		return nil, true
	}
	ctx, cancel := context.WithTimeout(ctx, apiTimeout)
	defer cancel()
	items, err := p.list(ctx)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		p.failing = true
		if p.known {
			klog.Warningf("listing the ResourceSlices of pool controllers: %v; using the serials of the last list", err)
		} else {
			klog.Warningf("listing the ResourceSlices of pool controllers: %v; no local memory device is published until a list succeeds", err)
		}
		return maps.Clone(p.serials), p.known
	}
	serials := map[uint64]bool{}
	for i := range items {
		rs := &items[i]
		if rs.Spec.Driver == device.DriverName {
			continue
		}
		for j := range rs.Spec.Devices {
			if s := deviceSerial(&rs.Spec.Devices[j], rs.Spec.Driver); s != "" {
				if v, err := strconv.ParseUint(s, 0, 64); err == nil {
					serials[v] = true
				}
			}
		}
	}
	p.serials, p.known, p.failing = serials, true, false
	return maps.Clone(serials), true
}

// Failing tells whether the last list failed.
func (p *poolSlices) Failing() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.failing
}

// scanInput returns the configuration and the pool serials of a scan: the
// serials of the memdevs that the pool helper prepares and of the devices
// that pool controllers publish. While the devices of the pool controllers
// are not known, the configuration publishes no local memdevs.
func scanInput(ctx context.Context, driverConfig *DriverConfig, pool *poolDriver, slices *poolSlices) (*DriverConfig, map[uint64]bool) {
	serials := pool.Serials()
	if serials == nil {
		serials = map[uint64]bool{}
	}
	remote, known := slices.Serials(ctx)
	for s := range remote {
		serials[s] = true
	}
	if known || !driverConfig.manageMemdevs() {
		return driverConfig, serials
	}
	klog.Warning("not publishing local memory devices: the devices of the pool controllers are not known")
	cfg := *driverConfig
	cfg.ManageMemdevs = ptr(false)
	return &cfg, serials
}

// retryPoolList rescans the devices every poolListRetry while the last list
// of the devices of pool controllers failed, until ctx ends.
func (d *driver) retryPoolList(ctx context.Context) {
	t := time.NewTicker(poolListRetry)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if d.poolSlices.Failing() {
				d.rescanAndPublish(ctx)
			}
		}
	}
}
