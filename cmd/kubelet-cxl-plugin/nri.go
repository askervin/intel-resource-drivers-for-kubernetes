/*
 * Copyright (c) 2025, Intel Corporation.  All Rights Reserved.
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
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/containerd/nri/pkg/api"
	"github.com/containerd/nri/pkg/stub"
	"github.com/containers/nri-plugins/pkg/cgmpolmgr"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/memorypolicy"

	"k8s.io/klog/v2"
)

// NRIOpts holds NRI plugin configuration from CLI flags.
type NRIOpts struct {
	Name   string
	Idx    string
	Socket string
}

// cgroupV2MountPoint is the default mount point for cgroup v2.
// TODO: make this configurable or auto-detect from /proc/mounts.
const cgroupV2MountPoint = "/sys/fs/cgroup"

// nriPlugin implements the NRI CreateContainer and RemoveContainer
// interfaces, sharing access to the DRA driver's nodeState.
type nriPlugin struct {
	stub     stub.Stub
	state    *nodeState
	pending  map[string]*cgmpolmgr.ManagerConfig // key: containerID; single validated config awaiting Start
	managers map[string]*cgmpolmgr.Manager       // key: containerID; running manager
	mu       sync.Mutex                          // protects pending and managers
}

// startNRIPlugin creates and starts the NRI plugin. It connects to
// the container runtime's NRI socket and begins listening for
// container lifecycle events. The plugin runs in the background;
// call Stop() to shut it down.
func startNRIPlugin(ctx context.Context, state *nodeState, opts NRIOpts) (*nriPlugin, error) {
	p := &nriPlugin{
		state:    state,
		pending:  make(map[string]*cgmpolmgr.ManagerConfig),
		managers: make(map[string]*cgmpolmgr.Manager),
	}

	stubOpts := []stub.Option{
		stub.WithOnClose(p.onClose),
	}
	if opts.Name != "" {
		stubOpts = append(stubOpts, stub.WithPluginName(opts.Name))
	}
	if opts.Idx != "" {
		stubOpts = append(stubOpts, stub.WithPluginIdx(opts.Idx))
	}
	if opts.Socket != "" {
		stubOpts = append(stubOpts, stub.WithSocketPath(opts.Socket))
	}

	var err error
	p.stub, err = stub.New(p, stubOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create NRI plugin stub: %w", err)
	}

	if err := p.stub.Start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start NRI plugin: %w", err)
	}

	klog.V(3).Info("NRI plugin started")
	return p, nil
}

// Stop shuts down the NRI plugin and all active cgroup managers.
func (p *nriPlugin) Stop() {
	p.mu.Lock()
	for key, mgr := range p.managers {
		mgr.Stop()
		delete(p.managers, key)
	}
	for key := range p.pending {
		delete(p.pending, key)
	}
	p.mu.Unlock()

	if p.stub != nil {
		p.stub.Stop()
		klog.V(3).Info("NRI plugin stopped")
	}
}

func (p *nriPlugin) onClose() {
	klog.Warning("NRI connection to the runtime lost")
}

// CreateContainer is called by the container runtime (via NRI) when a
// new container is about to be created. It aggregates all CXL claims
// for the container and builds a single ManagerConfig that covers all
// claimed memory. Returns an error if a user-specified memory policy
// is invalid, blocking container creation. The manager is created and
// started later in StartContainer, once the container exists and its
// cgroup directory is available.
func (p *nriPlugin) CreateContainer(_ context.Context, pod *api.PodSandbox, ctr *api.Container) (*api.ContainerAdjustment, []*api.ContainerUpdate, error) {
	ctrName := pod.GetNamespace() + "/" + pod.GetName() + "/" + ctr.GetName()
	ctrID := ctr.GetId()

	klog.V(3).Infof("NRI CreateContainer: ctr=%s id=%s", ctrName, ctrID)

	// Scan container env vars for CDI-injected claim markers.
	claimUIDs := extractClaimUIDs(ctr.GetEnv())
	if len(claimUIDs) == 0 {
		klog.V(5).Infof("NRI CreateContainer: ctr=%s: no CXL claims", ctrName)
		return nil, nil, nil
	}

	klog.V(5).Infof("NRI CreateContainer: ctr=%s has %d CXL claim(s): %v", ctrName, len(claimUIDs), claimUIDs)

	// Aggregate all devices across all claims and collect the
	// user-specified policy (if any). Only one policy per
	// container is supported — multiple conflicting policies are
	// rejected.
	dramNodeSet := make(map[int]bool)
	cxlNodeSet := make(map[int]bool)
	var dramQuota, cxlQuota uint64
	var userPolicy *memorypolicy.MemoryPolicyConfig

	for _, claimUID := range claimUIDs {
		info := p.state.GetClaimInfo(claimUID)
		if info == nil {
			klog.V(5).Infof("NRI CreateContainer: ctr=%s claim=%s: no claim info (driver may have restarted)",
				ctrName, claimUID)
			continue
		}

		klog.V(5).Infof("NRI CreateContainer: ctr=%s claim=%s (%s) devices=%d",
			ctrName, claimUID, info.ClaimName, len(info.Devices))

		for i, dev := range info.Devices {
			klog.V(5).Infof("NRI CreateContainer: ctr=%s claim=%s device[%d]: name=%s type=%s sysfs=%s numa=%v affinities=%v total=%s consumed=%s request=%s",
				ctrName, claimUID, i,
				dev.DeviceName, dev.DeviceType, dev.SysfsPath, dev.NUMANodes, dev.NodeAffinities,
				formatBytes(dev.TotalBytes), formatBytes(uint64(dev.ConsumedBytes)),
				dev.RequestName)
			switch dev.DeviceType {
			case device.DeviceTypeDRAM:
				for _, n := range dev.NUMANodes {
					dramNodeSet[n] = true
				}
				dramQuota += uint64(dev.ConsumedBytes)
			case device.DeviceTypeCXLNode:
				for _, n := range dev.NUMANodes {
					cxlNodeSet[n] = true
				}
				cxlQuota += uint64(dev.ConsumedBytes)
			}
		}

		// Collect user-specified policy.
		if info.Policy != nil && info.Policy.MemoryUseOrder != "" {
			klog.V(5).Infof("NRI CreateContainer: ctr=%s claim=%s policy: order=%s waypoints=%v minStep=%s maxStep=%s",
				ctrName, claimUID,
				info.Policy.MemoryUseOrder, info.Policy.MemoryUseWaypoints,
				info.Policy.MinStep, info.Policy.MaxStep)
			if userPolicy != nil {
				return nil, nil, fmt.Errorf("NRI CreateContainer: ctr=%s: multiple claims specify memory policies, only one is allowed per container",
					ctrName)
			}
			userPolicy = info.Policy
		}
	}

	// If no memory devices were found at all, nothing to manage.
	if len(dramNodeSet) == 0 && len(cxlNodeSet) == 0 {
		klog.V(5).Infof("NRI CreateContainer: ctr=%s: no DRAM or CXL devices in claims", ctrName)
		return nil, nil, nil
	}

	// Validate user-specified policy if present.
	if userPolicy != nil {
		if err := validatePolicy(userPolicy); err != nil {
			return nil, nil, fmt.Errorf("NRI CreateContainer: ctr=%s: invalid policy: %w",
				ctrName, err)
		}
	}

	// Build a single ManagerConfig for this container.
	cfg, err := buildManagerConfig("", ctrName, dramNodeSet, cxlNodeSet, dramQuota, cxlQuota, userPolicy)
	if err != nil {
		return nil, nil, fmt.Errorf("NRI CreateContainer: ctr=%s: failed to build manager config: %w",
			ctrName, err)
	}

	p.mu.Lock()
	p.pending[ctrID] = cfg
	p.mu.Unlock()

	klog.V(5).Infof("NRI CreateContainer: ctr=%s: prepared cgmpolmgr configuration (order=%s, dramNodes=%s, cxlNodes=%s, dramQuota=%s, cxlQuota=%s)",
		ctrName, cfg.MemoryUseOrder, cfg.DRAMNodes, cfg.CXLNodes, cfg.DRAMQuota, cfg.CXLQuota)

	return nil, nil, nil
}

// RemoveContainer is called by the container runtime (via NRI) when a
// container is being removed. It stops any active cgroup managers for
// the container's claims.
func (p *nriPlugin) RemoveContainer(_ context.Context, pod *api.PodSandbox, ctr *api.Container) error {
	ctrName := pod.GetNamespace() + "/" + pod.GetName() + "/" + ctr.GetName()
	ctrID := ctr.GetId()

	claimUIDs := extractClaimUIDs(ctr.GetEnv())
	if len(claimUIDs) == 0 {
		klog.V(5).Infof("NRI RemoveContainer: ctr=%s: no CXL claims", ctrName)
		return nil
	}

	klog.V(5).Infof("NRI RemoveContainer: ctr=%s releasing claims: %v", ctrName, claimUIDs)

	p.stopContainerManagers(ctrID, ctrName)

	return nil
}

// StartContainer is called by the container runtime (via NRI) after a
// container has been started. It resolves the real cgroup filesystem
// path, creates a cgmpolmgr.Manager from the pending config stored in
// CreateContainer, and starts it.
func (p *nriPlugin) StartContainer(_ context.Context, pod *api.PodSandbox, ctr *api.Container) error {
	ctrName := pod.GetNamespace() + "/" + pod.GetName() + "/" + ctr.GetName()
	ctrID := ctr.GetId()
	ctrPid := ctr.GetPid()

	p.mu.Lock()
	cfg, hasPending := p.pending[ctrID]
	if hasPending {
		delete(p.pending, ctrID)
	}
	p.mu.Unlock()

	if !hasPending {
		klog.V(3).Infof("NRI StartContainer: ctr=%s id=%s: no memory claims to be managed", ctrName, ctrID)
		return nil
	}

	// Resolve the container's cgroup directory in the filesystem.
	cgroupDir, err := resolveContainerCgroupDir(ctr)
	if err != nil {
		klog.Errorf("NRI StartContainer: ctr=%s id=%s: failed to resolve cgroup directory: %v",
			ctrName, ctrID, err)
		return nil
	}

	klog.V(3).Infof("NRI StartContainer: ctr=%s id=%s pid=%d cgroupsPath=%s", ctrName, ctrID, ctrPid, cgroupDir)

	cfg.CgroupPath = cgroupDir

	mgr, err := cgmpolmgr.NewManager(*cfg)
	if err != nil {
		klog.Warningf("NRI StartContainer: ctr=%s: failed to create cgroup manager (order=%s): %v",
			ctrName, cfg.MemoryUseOrder, err)
		return nil
	}

	if err := mgr.Initialize(); err != nil {
		klog.Warningf("NRI StartContainer: ctr=%s: failed to initialize cgroup manager: %v",
			ctrName, err)
		return nil
	}

	if err := mgr.Start(); err != nil {
		klog.Warningf("NRI StartContainer: ctr=%s: failed to start cgroup manager: %v",
			ctrName, err)
		return nil
	}

	p.mu.Lock()
	p.managers[ctrID] = mgr
	p.mu.Unlock()

	klog.V(3).Infof("NRI StartContainer: ctr=%s: started cgmpolmgr (order=%s, cgroup=%s)",
		ctrName, cfg.MemoryUseOrder, cfg.CgroupPath)

	return nil
}

// StopContainer is called by the container runtime (via NRI) when a
// container is being stopped. Stops cgroup managers as a safety net
// in case RemoveContainer is not called.
func (p *nriPlugin) StopContainer(_ context.Context, pod *api.PodSandbox, ctr *api.Container) ([]*api.ContainerUpdate, error) {
	ctrName := pod.GetNamespace() + "/" + pod.GetName() + "/" + ctr.GetName()
	ctrID := ctr.GetId()
	klog.V(3).Infof("NRI StopContainer: ctr=%s", ctrName)

	p.stopContainerManagers(ctrID, ctrName)

	return nil, nil
}

// stopContainerManagers stops and removes the cgroup manager and
// pending config associated with the given container.
func (p *nriPlugin) stopContainerManagers(ctrID string, ctrName string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.pending, ctrID)

	if mgr, ok := p.managers[ctrID]; ok {
		mgr.Stop()
		delete(p.managers, ctrID)
		klog.V(3).Infof("NRI: stopped cgmpolmgr for ctr=%s", ctrName)
	}
}

// validatePolicy checks that a MemoryPolicyConfig has valid fields
// suitable for cgmpolmgr. Returns nil if policy is nil (no policy
// means no steering requested).
func validatePolicy(p *memorypolicy.MemoryPolicyConfig) error {
	if p == nil {
		return nil
	}

	if p.MemoryUseOrder == "" {
		return fmt.Errorf("memoryUseOrder is required")
	}

	// Verify the order string is parseable.
	if _, err := cgmpolmgr.ParseMemoryUseOrder(p.MemoryUseOrder); err != nil {
		return err
	}

	if strings.EqualFold(p.MemoryUseOrder, "waypoints") && len(p.MemoryUseWaypoints) == 0 {
		return fmt.Errorf("memoryUseWaypoints must be non-empty when memoryUseOrder is \"waypoints\"")
	}

	// Validate step sizes if provided.
	var minStep, maxStep uint64
	var err error
	if p.MinStep != "" {
		minStep, err = cgmpolmgr.ParseMemorySize(p.MinStep)
		if err != nil {
			return fmt.Errorf("invalid minStep %q: %w", p.MinStep, err)
		}
		if minStep == 0 {
			return fmt.Errorf("minStep must be positive")
		}
	}
	if p.MaxStep != "" {
		maxStep, err = cgmpolmgr.ParseMemorySize(p.MaxStep)
		if err != nil {
			return fmt.Errorf("invalid maxStep %q: %w", p.MaxStep, err)
		}
		if maxStep == 0 {
			return fmt.Errorf("maxStep must be positive")
		}
	}
	if minStep > 0 && maxStep > 0 && maxStep < minStep {
		return fmt.Errorf("maxStep (%d) must be >= minStep (%d)", maxStep, minStep)
	}

	return nil
}

// buildManagerConfig creates a cgmpolmgr.ManagerConfig from
// aggregated device data and an optional user policy. The cgroupPath
// parameter is stored as-is; pass an empty string if it will be
// resolved later (e.g. in StartContainer). cgroupName is a pretty
// name used in log messages.
//
// When no user policy is specified, the function picks a default:
//   - CXL-only  → "first-cxl"
//   - DRAM-only → "first-dram"
//   - Both      → "start-interleaved"
//
// MinStep and MaxStep default to the total requested memory if the
// user has not specified a policy.
func buildManagerConfig(cgroupPath, cgroupName string,
	dramNodeSet, cxlNodeSet map[int]bool,
	dramQuota, cxlQuota uint64,
	policy *memorypolicy.MemoryPolicyConfig,
) (*cgmpolmgr.ManagerConfig, error) {

	hasDRAM := len(dramNodeSet) > 0 && dramQuota > 0
	hasCXL := len(cxlNodeSet) > 0 && cxlQuota > 0

	if !hasDRAM && !hasCXL {
		return nil, fmt.Errorf("no DRAM or CXL devices with non-zero quota")
	}

	// Determine memory use order and step sizes.
	var order, minLimit, maxLimit string
	var waypoints []cgmpolmgr.MemoryUseWaypoint

	if policy != nil {
		order = policy.MemoryUseOrder
		minLimit = policy.MinStep
		maxLimit = policy.MaxStep
		waypoints = policy.MemoryUseWaypoints
	} else {
		// Pick default order based on which memory types are present.
		switch {
		case hasCXL && !hasDRAM:
			order = "first-cxl"
		case hasDRAM && !hasCXL:
			order = "first-dram"
		default:
			order = "start-interleaved"
		}
		totalBytes := dramQuota + cxlQuota
		defaultStep := strconv.FormatUint(totalBytes, 10)
		minLimit = defaultStep
		maxLimit = defaultStep
	}

	cfg := &cgmpolmgr.ManagerConfig{
		CgroupPath:         cgroupPath,
		CgroupName:         cgroupName,
		MemoryUseOrder:     order,
		MemoryUseWaypoints: waypoints,
		MinLimit:           minLimit,
		MaxLimit:           maxLimit,
	}

	if hasDRAM {
		cfg.DRAMNodes = formatNodeset(dramNodeSet)
		cfg.DRAMQuota = strconv.FormatUint(dramQuota, 10)
	} else {
		cfg.DRAMQuota = "0"
	}

	if hasCXL {
		cfg.CXLNodes = formatNodeset(cxlNodeSet)
		cfg.CXLQuota = strconv.FormatUint(cxlQuota, 10)
	} else {
		cfg.CXLQuota = "0"
	}

	return cfg, nil
}

// formatNodeset formats a set of NUMA node IDs as a sorted
// comma-separated string (e.g. "0,2,4").
func formatNodeset(nodes map[int]bool) string {
	sorted := make([]int, 0, len(nodes))
	for n := range nodes {
		sorted = append(sorted, n)
	}
	sort.Ints(sorted)
	parts := make([]string, len(sorted))
	for i, n := range sorted {
		parts[i] = strconv.Itoa(n)
	}
	return strings.Join(parts, ",")
}

// resolveContainerCgroupDir determines the cgroup v2 filesystem
// directory for a container. It reads /proc/<pid>/cgroup to find the
// actual cgroup path the kernel assigned to the container's init
// process, which is more reliable than the NRI-provided CgroupsPath
// (which may use systemd slice notation).
func resolveContainerCgroupDir(ctr *api.Container) (string, error) {
	pid := ctr.GetPid()
	if pid == 0 {
		return "", fmt.Errorf("container PID is 0, cannot resolve cgroup")
	}

	// Read /proc/<pid>/cgroup. For cgroup v2, there's a single
	// line of the form "0::<path>".
	procCgroupPath := fmt.Sprintf("/proc/%d/cgroup", pid)
	data, err := os.ReadFile(procCgroupPath)
	if err != nil {
		return "", fmt.Errorf("failed to read %s: %w", procCgroupPath, err)
	}

	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		// cgroup v2 line: "0::<path>"
		parts := strings.SplitN(line, ":", 3)
		if len(parts) == 3 && parts[0] == "0" && parts[1] == "" {
			cgroupRelPath := parts[2]
			absPath := filepath.Join(cgroupV2MountPoint, cgroupRelPath)
			klog.V(5).Infof("resolveContainerCgroupDir: pid=%d /proc cgroup line=%q -> absPath=%s",
				pid, line, absPath)
			return absPath, nil
		}
	}

	return "", fmt.Errorf("no cgroup v2 entry found in %s (content: %q)", procCgroupPath, string(data))
}

// extractClaimUIDs scans an environment variable list for CDI-injected
// CXL_CLAIM_* markers and returns the unique claim UIDs. Duplicates
// are possible because each device in a claim carries the same CDI
// marker, and kubelet doesn't deduplicate CDI device IDs.
func extractClaimUIDs(envVars []string) []string {
	seen := make(map[string]bool)
	var uids []string
	for _, env := range envVars {
		if uid, ok := ClaimUIDFromEnvVar(env); ok && !seen[uid] {
			seen[uid] = true
			uids = append(uids, uid)
		}
	}
	return uids
}

// formatBytes formats a byte count as a human-readable string using
// binary units (KiB, MiB, GiB).
func formatBytes(bytes uint64) string {
	const (
		kib = 1024
		mib = 1024 * kib
		gib = 1024 * mib
	)
	switch {
	case bytes >= gib:
		return fmt.Sprintf("%.1fGiB", float64(bytes)/float64(gib))
	case bytes >= mib:
		return fmt.Sprintf("%.1fMiB", float64(bytes)/float64(mib))
	case bytes >= kib:
		return fmt.Sprintf("%.1fKiB", float64(bytes)/float64(kib))
	default:
		return fmt.Sprintf("%dB", bytes)
	}
}
