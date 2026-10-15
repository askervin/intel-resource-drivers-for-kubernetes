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

// Package api is what containers see of the CXL DRA driver: the lease
// protocol of the socket $CXL_LEASE_SOCKET that cxl-request speaks, and
// the names of the environment variables of the memory that claims and
// leases give a container. It has no Kubernetes dependencies. See
// doc/cxl/LEASES.md (protocol) and doc/cxl/USAGE.md (environment).
package api

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// SocketEnv names the lease socket in containers of lease claims.
	SocketEnv = "CXL_LEASE_SOCKET"
	// DefaultSocket is the path of the lease socket in containers.
	DefaultSocket = "/run/cxl/cxl.sock"
	// LeaseEnv is the lease id in the environment of cxl-request --exec.
	LeaseEnv = "CXL_LEASE"
	// ClaimEnvPrefix is the prefix of the claim marker
	// CXL_CLAIM_<uid with underscores>=1 of the claims of both drivers.
	ClaimEnvPrefix = "CXL_CLAIM_"

	// SourceLocal is memory of a CXL device attached to the node.
	SourceLocal = "local"
	// SourcePool is memory of a device of a memory pool.
	SourcePool = "pool"

	// ModeNUMA gives memory as system RAM of a NUMA node.
	ModeNUMA = "numa"
	// ModeDax gives memory as a devdax device (claims) or its fd
	// (leases).
	ModeDax = "dax"

	// Operations of a request.
	OpRequest = "request"
	OpRelease = "release"
	OpList    = "list"
	OpFd      = "fd"
	OpEnv     = "env"

	// States of a lease.
	StateCreating  = "Creating"  // stored, companion objects not created yet
	StatePending   = "Pending"   // companion claim and pod exist
	StateLending   = "Lending"   // companion claim being prepared
	StateReady     = "Ready"     // memory lent or dax fd available
	StateReleasing = "Releasing" // companion being deleted
	StateFailed    = "Failed"    // see Error
	StateReleased  = "Released"  // gone (response state only)

	// DRAMKey is the env key of the DRAM device.
	DRAMKey = "DRAM"
)

// Request is one request line on a lease socket. Only Op is required.
type Request struct {
	Op        string `json:"op"`
	Source    string `json:"source,omitempty"`
	Mode      string `json:"mode,omitempty"`
	Shared    bool   `json:"shared,omitempty"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
	Size      string `json:"size,omitempty"`
	Serial    string `json:"serial,omitempty"`
	Wait      string `json:"wait,omitempty"`
	RequestID string `json:"requestId,omitempty"`
	Lease     string `json:"lease,omitempty"`
	All       bool   `json:"all,omitempty"`
}

// Response is one response line on a lease socket. The response of a
// Ready dax lease to request and fd carries the open dax fd
// (SCM_RIGHTS).
type Response struct {
	Lease    string            `json:"lease,omitempty"`
	State    string            `json:"state,omitempty"`
	Source   string            `json:"source,omitempty"`
	Mode     string            `json:"mode,omitempty"`
	Shared   bool              `json:"shared"`
	ReadOnly bool              `json:"readOnly"`
	Size     uint64            `json:"size,omitempty"`
	Serial   string            `json:"serial,omitempty"`
	Node     *int              `json:"node,omitempty"`
	Dax      string            `json:"dax,omitempty"`
	Env      map[string]string `json:"env,omitempty"`
	Error    string            `json:"error,omitempty"`
	Leases   *[]Response       `json:"leases,omitempty"`
}

// EffectiveMode returns the mode of a request: Mode, or dax for shared
// memory and numa otherwise when Mode is empty.
func (r *Request) EffectiveMode() string {
	switch {
	case r.Mode != "":
		return r.Mode
	case r.Shared:
		return ModeDax
	}
	return ModeNUMA
}

// Validate returns an error if the fields of the request do not fit
// together or have invalid values.
func (r *Request) Validate() error {
	if r.Wait != "" {
		if d, err := time.ParseDuration(r.Wait); err != nil || d < 0 {
			return fmt.Errorf("invalid wait %q", r.Wait)
		}
	}
	switch r.Op {
	case OpRequest:
		return r.validateRequest()
	case OpRelease:
		if (r.Lease == "") == !r.All {
			return errors.New("release takes --lease ID or --all")
		}
	case OpFd, OpEnv:
		if r.Lease == "" {
			return fmt.Errorf("%s needs --lease ID", r.Op)
		}
	case OpList:
	default:
		return fmt.Errorf("unknown op %q (%s, %s, %s, %s or %s)", r.Op, OpRequest, OpRelease, OpList, OpFd, OpEnv)
	}
	return nil
}

// validateRequest returns an error if the fields of an OpRequest do not
// fit together or have invalid values.
func (r *Request) validateRequest() error {
	if err := r.validateValues(); err != nil {
		return err
	}
	switch {
	case r.Shared && r.Mode == ModeNUMA:
		return errors.New("shared memory is never onlined as RAM: use mode dax")
	case r.Shared && r.Source == SourceLocal:
		return errors.New("local devices are not shareable; share a ResourceClaim between pods instead")
	case r.ReadOnly && r.EffectiveMode() != ModeDax:
		return errors.New("readOnly needs mode dax")
	case !r.Shared && r.Serial == "" && r.Size == "":
		return errors.New("size or serial is required")
	}
	if r.Serial != "" {
		if _, err := ParseSerial(r.Serial); err != nil {
			return err
		}
	}
	return nil
}

// validateValues returns an error if the source or the mode of a request
// is invalid.
func (r *Request) validateValues() error {
	switch {
	case r.Source != "" && r.Source != SourceLocal && r.Source != SourcePool:
		return fmt.Errorf("invalid source %q (%s or %s)", r.Source, SourceLocal, SourcePool)
	case r.Mode != "" && r.Mode != ModeNUMA && r.Mode != ModeDax:
		return fmt.Errorf("invalid mode %q (%s or %s)", r.Mode, ModeNUMA, ModeDax)
	}
	return nil
}

// Memory is what a claim or a lease gives a container of one device:
//   - Key: the env key (Key, RegionKey, DRAMKey)
//   - Serial: 0x<hex> of a memdev, "" for regions and DRAM
//   - Shared: the memdev may be mapped by several hosts at a time
//   - Mode: ModeNUMA or ModeDax
//   - Size: bytes that the claim or lease got
//   - Node: NUMA node of numa memory without Nodes
//   - Nodes: NUMA nodes of DRAM (cpuset syntax)
//   - Dax: /dev/daxX.Y of dax memory
//   - ReadOnly: dax memory allows reads only
type Memory struct {
	Key      string
	Serial   string
	Shared   bool
	Mode     string
	Size     uint64
	Node     int
	Nodes    string
	Dax      string
	ReadOnly bool
}

// Key returns the env key of the n-th memory of the memdev with the serial
// in one claim: the serial in upper-case hex, with "_<n>" for n > 1.
func Key(serial uint64, n int) string {
	return Nth(strings.ToUpper(strconv.FormatUint(serial, 16)), n)
}

// RegionKey returns the env key of a region: its name in upper case
// (REGION0).
func RegionKey(region string) string {
	return strings.Map(func(c rune) rune {
		switch {
		case c >= 'a' && c <= 'z':
			return c - 'a' + 'A'
		case c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
			return c
		}
		return '_'
	}, region)
}

// Nth returns the env key of the n-th memory with the key in one claim:
// key for n <= 1, else key_<n>.
func Nth(key string, n int) string {
	if n <= 1 {
		return key
	}
	return key + "_" + strconv.Itoa(n)
}

// Env returns the environment variables CXL_<KEY>_<FIELD> of a memory:
// SERIAL and SHARED of memdevs, MODE and SIZE always, NODE or NODES of
// numa memory, DAX and READONLY of dax memory.
func Env(m Memory) map[string]string {
	p := "CXL_" + m.Key + "_"
	env := map[string]string{
		p + "MODE": m.Mode,
		p + "SIZE": strconv.FormatUint(m.Size, 10),
	}
	if m.Serial != "" {
		env[p+"SERIAL"] = m.Serial
		env[p+"SHARED"] = boolEnv(m.Shared)
	}
	switch {
	case m.Mode == ModeDax:
		if m.Dax != "" {
			env[p+"DAX"] = m.Dax
		}
		env[p+"READONLY"] = boolEnv(m.ReadOnly)
	case m.Nodes != "":
		env[p+"NODES"] = m.Nodes
	default:
		env[p+"NODE"] = strconv.Itoa(m.Node)
	}
	return env
}

// EnvList returns env as sorted KEY=VALUE strings.
func EnvList(env map[string]string) []string {
	out := make([]string, 0, len(env))
	for k, v := range env {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

// ExecEnv returns the environment of a command that runs with a lease, as
// sorted KEY=VALUE strings: env with every CXL_<KEY>_DAX replaced by
// CXL_<KEY>_FD=<fd> when fd >= 0, and CXL_LEASE=<lease>.
func ExecEnv(env map[string]string, lease string, fd int) []string {
	out := make(map[string]string, len(env)+1)
	for k, v := range env {
		if base, ok := strings.CutSuffix(k, "_DAX"); ok && fd >= 0 && strings.HasPrefix(k, "CXL_") {
			out[base+"_FD"] = strconv.Itoa(fd)
			continue
		}
		out[k] = v
	}
	out[LeaseEnv] = lease
	return EnvList(out)
}

// ParseSerial returns the value of a serial in 0x hex or decimal.
func ParseSerial(s string) (uint64, error) {
	var v uint64
	var err error
	if h, ok := strings.CutPrefix(strings.ToLower(s), "0x"); ok {
		v, err = strconv.ParseUint(h, 16, 64)
	} else {
		v, err = strconv.ParseUint(s, 10, 64)
	}
	if err != nil {
		return 0, fmt.Errorf("invalid serial %q", s)
	}
	return v, nil
}

// FormatSerial returns a serial as 0x<lower-case hex>.
func FormatSerial(v uint64) string {
	return "0x" + strconv.FormatUint(v, 16)
}

// boolEnv returns "1" for true and "0" for false.
func boolEnv(b bool) string {
	if b {
		return "1"
	}
	return "0"
}
