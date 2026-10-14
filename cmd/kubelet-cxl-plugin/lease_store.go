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
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
)

// leasesFileName is the lease store in the cxl.generic plugin data
// directory.
const leasesFileName = "leases.json"

// Lease states. A lease is written in state Creating before any API
// object exists, so that a restart can clean up after a crash.
const (
	leaseCreating  = "Creating"  // stored, companion objects not created yet
	leasePending   = "Pending"   // companion claim and pod exist
	leaseLending   = "Lending"   // companion claim being prepared, lend in progress
	leaseReady     = "Ready"     // lent (exclusive) or fd available (shared)
	leaseReleasing = "Releasing" // revoking, companion being deleted
	leaseFailed    = "Failed"    // see Error; released by the client or by cleanup
	// leaseReleased is never stored: the response state of a release
	// that completed (the lease is gone).
	leaseReleased = "Released"
)

// Lease kinds (request "kind").
const (
	leaseKindMemory = "memory" // exclusive memory, lent as a NUMA node or passed as a devdax fd
	leaseKindShared = "shared" // shared pool device, passed as a devdax fd
)

// leaseSourceAny is the source of a memory lease that takes a local memdev
// if one fits, else pool memory (device.SourceLocal, device.SourcePool).
const leaseSourceAny = "any"

// podRef identifies a pod.
type podRef struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
}

// tokenClaim is a prepared claim on the token device: it owns the
// socket <data dir>/leases/<uid>/cxl.sock.
type tokenClaim struct {
	UID       string   `json:"uid"`
	Namespace string   `json:"namespace"`
	Name      string   `json:"name"`
	Pods      []podRef `json:"pods"` // reservedFor at Prepare (refreshed on demand)
}

func (c *tokenClaim) hasPod(uid string) bool {
	for _, p := range c.Pods {
		if p.UID == uid {
			return true
		}
	}
	return false
}

// lease is one request of a container.
type lease struct {
	ID         string `json:"id"`
	TokenClaim string `json:"tokenClaim"` // uid of the token claim whose socket received it
	RequestID  string `json:"requestId,omitempty"`
	Kind       string `json:"kind"`
	// Source is the requested source (local, pool or any), and the
	// source of the device once Ready ("" in stores written before
	// sources existed: pool).
	Source string `json:"source,omitempty"`
	Size   string `json:"size,omitempty"` // requested quantity (memory)
	// Serial is 0x<lower-case hex>: the device of a shared lease, the
	// requested device of a memory lease, or the device of a Ready lease.
	Serial string `json:"serial,omitempty"`
	// Mode is numa or dax ("" in stores written before modes existed:
	// numa for memory, dax for shared leases).
	Mode      string    `json:"mode,omitempty"`
	Exclusive bool      `json:"exclusive,omitempty"` // shared: all of the device's hosts capacity
	Hosts     string    `json:"hosts,omitempty"`     // exclusive: the hosts capacity requested
	ReadOnly  bool      `json:"readOnly,omitempty"`  // dax: the fd is opened read-only
	Pod       podRef    `json:"pod"`
	Container string    `json:"container"`
	State     string    `json:"state"`
	Error     string    `json:"error,omitempty"`
	Node      int       `json:"node"` // lent NUMA node, -1 if none
	Bytes     uint64    `json:"bytes,omitempty"`
	Dax       string    `json:"dax,omitempty"` // /dev/daxX.Y (dax mode)
	Created   time.Time `json:"created"`
}

// source returns the source of a lease, also for stores written before
// sources existed.
func (l *lease) source() string {
	if l.Source == "" {
		return device.SourcePool
	}
	return l.Source
}

// companionName is the name of the companion claim and pod of a lease.
func companionName(id string) string {
	return "cxl-lease-" + id
}

// leaseStore is the persisted lease table. Not safe for concurrent use:
// the broker serializes access with its mutex.
type leaseStore struct {
	path   string
	Claims map[string]*tokenClaim `json:"claims"` // token claim uid -> ...
	Leases map[string]*lease      `json:"leases"` // lease id -> ...
}

// loadLeaseStore reads the store at path, an empty store if it does
// not exist.
func loadLeaseStore(path string) (*leaseStore, error) {
	s := &leaseStore{path: path}
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	if err == nil {
		if err := json.Unmarshal(data, s); err != nil {
			return nil, fmt.Errorf("parsing %s: %w", path, err)
		}
	}
	if s.Claims == nil {
		s.Claims = map[string]*tokenClaim{}
	}
	if s.Leases == nil {
		s.Leases = map[string]*lease{}
	}
	return s, nil
}

// save writes the store atomically.
func (s *leaseStore) save() error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding leases: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return fmt.Errorf("writing %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("renaming %s: %w", tmp, err)
	}
	return nil
}

// leasesOf returns the leases received on the token claim's socket.
func (s *leaseStore) leasesOf(claimUID string) []*lease {
	var out []*lease
	for _, l := range s.Leases {
		if l.TokenClaim == claimUID {
			out = append(out, l)
		}
	}
	return out
}
