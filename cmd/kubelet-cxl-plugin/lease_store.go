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
	"encoding/json"
	"fmt"
	"os"
	"time"

	"k8s.io/klog/v2"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/api"
)

// leasesFileName is the lease store in the cxl.generic plugin data
// directory.
const leasesFileName = "leases.json"

// podRef identifies a pod.
type podRef struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
}

// leaseClaim is a prepared claim on the lease device: it owns the socket
// <data dir>/leases/<uid>/cxl.sock.
type leaseClaim struct {
	UID       string   `json:"uid"`
	Namespace string   `json:"namespace"`
	Name      string   `json:"name"`
	Pods      []podRef `json:"pods"` // reservedFor at Prepare (refreshed on demand)
}

// hasPod tells whether the pod with the uid is one of the claim's pods.
func (c *leaseClaim) hasPod(uid string) bool {
	for _, p := range c.Pods {
		if p.UID == uid {
			return true
		}
	}
	return false
}

// lease is one request of a container. A lease is stored in state
// Creating before any API object exists, so that a restart can clean up
// after a crash. The states are api.State*; Released is never stored.
type lease struct {
	ID string `json:"id"`
	// Claim is the uid of the lease claim whose socket received it.
	Claim     string `json:"claim"`
	RequestID string `json:"requestId,omitempty"`
	// Source is the requested source ("": local, then pool), and the
	// source of the device once Ready.
	Source string `json:"source,omitempty"`
	Mode   string `json:"mode"`   // effective: api.ModeNUMA or api.ModeDax
	Shared bool   `json:"shared"` // one host of a shared device
	// Hosts is the hosts capacity requested when a shared device is taken
	// exclusively (all of it), "" otherwise.
	Hosts    string `json:"hosts,omitempty"`
	ReadOnly bool   `json:"readOnly,omitempty"` // dax: the fd is opened read-only
	Size     string `json:"size,omitempty"`     // requested quantity
	// Serial is 0x<lower-case hex>: the requested device, or the device
	// of a Ready lease.
	Serial    string    `json:"serial,omitempty"`
	Pod       podRef    `json:"pod"`
	Container string    `json:"container"`
	State     string    `json:"state"`
	Error     string    `json:"error,omitempty"`
	Node      int       `json:"node"` // lent NUMA node, -1 if none
	Bytes     uint64    `json:"bytes,omitempty"`
	Dax       string    `json:"dax,omitempty"` // /dev/daxX.Y (dax mode)
	Created   time.Time `json:"created"`
}

// deviceShared tells whether the device of a lease is a shared device:
// one of its hosts, or all of them.
func (l *lease) deviceShared() bool {
	return l.Shared || l.Hosts != ""
}

// memory returns what a Ready lease gives its container, false before.
func (l *lease) memory() (api.Memory, bool) {
	serial, err := api.ParseSerial(l.Serial)
	if l.State != api.StateReady || err != nil {
		return api.Memory{}, false
	}
	return api.Memory{
		Key:      api.Key(serial, 1),
		Serial:   l.Serial,
		Shared:   l.deviceShared(),
		Mode:     l.Mode,
		Size:     l.Bytes,
		Node:     l.Node,
		Dax:      l.Dax,
		ReadOnly: l.ReadOnly,
	}, true
}

// companionName is the name of the companion claim and pod of a lease.
func companionName(id string) string {
	return "cxl-lease-" + id
}

// leaseStore is the persisted lease table. Not safe for concurrent use:
// the broker serializes access with its mutex.
type leaseStore struct {
	path   string
	Claims map[string]*leaseClaim `json:"claims"` // lease claim uid -> ...
	Leases map[string]*lease      `json:"leases"` // lease id -> ...
}

// loadLeaseStore reads the store at path: an empty store if it does not
// exist or cannot be decoded (strictly).
func loadLeaseStore(path string) (*leaseStore, error) {
	s := &leaseStore{path: path}
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	if err == nil {
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		if err := dec.Decode(s); err != nil {
			klog.Errorf("lease broker: parsing %s: %v; starting with an empty lease table", path, err)
			s = &leaseStore{path: path}
		}
	}
	if s.Claims == nil {
		s.Claims = map[string]*leaseClaim{}
	}
	if s.Leases == nil {
		s.Leases = map[string]*lease{}
	}
	return s, nil
}

// save writes the store atomically.
func (s *leaseStore) save() error {
	return writeJSONFile(s.path, s)
}

// leasesOf returns the leases received on the lease claim's socket.
func (s *leaseStore) leasesOf(claimUID string) []*lease {
	var out []*lease
	for _, l := range s.Leases {
		if l.Claim == claimUID {
			out = append(out, l)
		}
	}
	return out
}
