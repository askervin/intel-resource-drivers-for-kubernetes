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
	pending  map[string][]cgmpolmgr.CgroupConfig // key: containerID; validated configs awaiting Start
	managers map[string]*cgmpolmgr.Manager        // key: containerID:claimUID; running managers
	mu       sync.Mutex                            // protects pending and managers
}

// startNRIPlugin creates and starts the NRI plugin. It connects to
// the container runtime's NRI socket and begins listening for
// container lifecycle events. The plugin runs in the background;
// call Stop() to shut it down.
func startNRIPlugin(ctx context.Context, state *nodeState, opts NRIOpts) (*nriPlugin, error) {
	p := &nriPlugin{
		state:    state,
		pending:  make(map[string][]cgmpolmgr.CgroupConfig),
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
// new container is about to be created. It validates memory policies
// and builds CgroupConfig for claims that request memory steering.
// Returns an error if a memory policy is requested but invalid,
// blocking container creation. Managers are created and started later
// in StartContainer, once the container is confirmed to exist and its
// cgroup directory is available.
func (p *nriPlugin) CreateContainer(_ context.Context, pod *api.PodSandbox, ctr *api.Container) (*api.ContainerAdjustment, []*api.ContainerUpdate, error) {
	podName := pod.GetNamespace() + "/" + pod.GetName()
	ctrName := ctr.GetName()
	ctrID := ctr.GetId()

	klog.V(3).Infof("NRI CreateContainer: pod=%s ctr=%s id=%s", podName, ctrName, ctrID)

	// Debug: log cgroup-related info from NRI at this stage.
	klog.V(3).Infof("NRI CreateContainer: pod=%s ctr=%s ctr.Linux.CgroupsPath=%q",
		podName, ctrName, ctr.GetLinux().GetCgroupsPath())
	if podLinux := pod.GetLinux(); podLinux != nil {
		klog.V(3).Infof("NRI CreateContainer: pod=%s pod.Linux.CgroupParent=%q pod.Linux.CgroupsPath=%q",
			podName, podLinux.GetCgroupParent(), podLinux.GetCgroupsPath())
	}

	// Scan container env vars for CDI-injected claim markers.
	claimUIDs := extractClaimUIDs(ctr.GetEnv())
	if len(claimUIDs) == 0 {
		klog.V(5).Infof("NRI CreateContainer: no CXL claims for pod=%s ctr=%s", podName, ctrName)
		return nil, nil, nil
	}

	klog.V(5).Infof("NRI CreateContainer: pod=%s ctr=%s has %d CXL claim(s): %v", podName, ctrName, len(claimUIDs), claimUIDs)

	var pendingConfigs []cgmpolmgr.CgroupConfig

	// Look up full claim information from shared state.
	for _, claimUID := range claimUIDs {
		info := p.state.GetClaimInfo(claimUID)
		if info == nil {
			klog.V(5).Infof("NRI CreateContainer: pod=%s ctr=%s claim=%s: no claim info (driver may have restarted)",
				podName, ctrName, claimUID)
			continue
		}

		klog.V(5).Infof("NRI CreateContainer: pod=%s ctr=%s claim=%s (%s) devices=%d",
			podName, ctrName, claimUID, info.ClaimName, len(info.Devices))

		for i, dev := range info.Devices {
			klog.V(5).Infof("NRI CreateContainer: pod=%s ctr=%s claim=%s device[%d]: name=%s type=%s sysfs=%s numa=%v affinities=%v total=%s consumed=%s request=%s",
				podName, ctrName, claimUID, i,
				dev.DeviceName, dev.DeviceType, dev.SysfsPath, dev.NUMANodes, dev.NodeAffinities,
				formatBytes(dev.TotalBytes), formatBytes(uint64(dev.ConsumedBytes)),
				dev.RequestName)
		}

		if info.Policy != nil {
			klog.V(5).Infof("NRI CreateContainer: pod=%s ctr=%s claim=%s policy: order=%s waypoints=%v minStep=%s maxStep=%s",
				podName, ctrName, claimUID,
				info.Policy.MemoryUseOrder, info.Policy.MemoryUseWaypoints,
				info.Policy.MinStep, info.Policy.MaxStep)
		}

		// Skip claims without a memory policy.
		if info.Policy == nil || info.Policy.MemoryUseOrder == "" {
			continue
		}

		if err := validatePolicy(info); err != nil {
			return nil, nil, fmt.Errorf("NRI CreateContainer: pod=%s ctr=%s claim=%s: invalid policy: %w",
				podName, ctrName, claimUID, err)
		}

		// Build config with a placeholder cgroup path. The real
		// filesystem path is resolved in StartContainer where the
		// cgroup directory exists and we can read pid/cgroup.
		cgCfg, err := buildCgroupConfig("", info)
		if err != nil {
			return nil, nil, fmt.Errorf("NRI CreateContainer: pod=%s ctr=%s claim=%s: failed to build cgroup config: %w",
				podName, ctrName, claimUID, err)
		}

		pendingConfigs = append(pendingConfigs, *cgCfg)

		klog.V(3).Infof("NRI CreateContainer: pod=%s ctr=%s claim=%s: validated policy (order=%s), will start cgmpolmgr in StartContainer",
			podName, ctrName, claimUID, info.Policy.MemoryUseOrder)
	}

	if len(pendingConfigs) > 0 {
		p.mu.Lock()
		p.pending[ctrID] = pendingConfigs
		p.mu.Unlock()
	}

	return nil, nil, nil
}

// RemoveContainer is called by the container runtime (via NRI) when a
// container is being removed. It stops any active cgroup managers for
// the container's claims.
func (p *nriPlugin) RemoveContainer(_ context.Context, pod *api.PodSandbox, ctr *api.Container) error {
	podName := pod.GetNamespace() + "/" + pod.GetName()
	ctrName := ctr.GetName()
	ctrID := ctr.GetId()

	claimUIDs := extractClaimUIDs(ctr.GetEnv())
	if len(claimUIDs) == 0 {
		klog.V(5).Infof("NRI RemoveContainer: no CXL claims for pod=%s ctr=%s", podName, ctrName)
		return nil
	}

	klog.V(5).Infof("NRI RemoveContainer: pod=%s ctr=%s releasing claims: %v", podName, ctrName, claimUIDs)

	p.stopContainerManagers(ctrID, podName, ctrName)

	return nil
}

// StartContainer is called by the container runtime (via NRI) after a
// container has been started. It resolves the real cgroup filesystem
// path, creates cgmpolmgr.Manager instances from the pending configs
// stored in CreateContainer, and starts them.
func (p *nriPlugin) StartContainer(_ context.Context, pod *api.PodSandbox, ctr *api.Container) error {
	podName := pod.GetNamespace() + "/" + pod.GetName()
	ctrName := ctr.GetName()
	ctrID := ctr.GetId()
	ctrPid := ctr.GetPid()

	klog.V(3).Infof("NRI StartContainer: pod=%s ctr=%s id=%s pid=%d", podName, ctrName, ctrID, ctrPid)

	// Debug: log cgroup paths available now.
	klog.V(3).Infof("NRI StartContainer: pod=%s ctr=%s ctr.Linux.CgroupsPath=%q",
		podName, ctrName, ctr.GetLinux().GetCgroupsPath())

	p.mu.Lock()
	configs, hasPending := p.pending[ctrID]
	if hasPending {
		delete(p.pending, ctrID)
	}
	p.mu.Unlock()

	if !hasPending {
		return nil
	}

	// Resolve the container's cgroup directory in the filesystem.
	cgroupDir, err := resolveContainerCgroupDir(ctr)
	if err != nil {
		klog.Warningf("NRI StartContainer: pod=%s ctr=%s: failed to resolve cgroup directory: %v",
			podName, ctrName, err)
		return nil
	}

	klog.V(3).Infof("NRI StartContainer: pod=%s ctr=%s: resolved cgroup dir=%s", podName, ctrName, cgroupDir)

	for i := range configs {
		cfg := &configs[i]
		cfg.Path = cgroupDir

		mgr, err := cgmpolmgr.NewManager(*cfg)
		if err != nil {
			klog.Warningf("NRI StartContainer: pod=%s ctr=%s: failed to create cgroup manager (order=%s): %v",
				podName, ctrName, cfg.MemoryUseOrder, err)
			continue
		}

		if err := mgr.Initialize(); err != nil {
			klog.Warningf("NRI StartContainer: pod=%s ctr=%s: failed to initialize cgroup manager: %v",
				podName, ctrName, err)
			continue
		}

		if err := mgr.Start(); err != nil {
			klog.Warningf("NRI StartContainer: pod=%s ctr=%s: failed to start cgroup manager: %v",
				podName, ctrName, err)
			continue
		}

		key := fmt.Sprintf("%s:%d", ctrID, i)
		p.mu.Lock()
		p.managers[key] = mgr
		p.mu.Unlock()

		klog.V(3).Infof("NRI StartContainer: pod=%s ctr=%s: started cgmpolmgr (order=%s, cgroup=%s)",
			podName, ctrName, cfg.MemoryUseOrder, cfg.Path)
	}

	return nil
}

// StopContainer is called by the container runtime (via NRI) when a
// container is being stopped. Stops cgroup managers as a safety net
// in case RemoveContainer is not called.
func (p *nriPlugin) StopContainer(_ context.Context, pod *api.PodSandbox, ctr *api.Container) ([]*api.ContainerUpdate, error) {
	podName := pod.GetNamespace() + "/" + pod.GetName()
	ctrName := ctr.GetName()
	ctrID := ctr.GetId()
	klog.V(3).Infof("NRI StopContainer: pod=%s ctr=%s", podName, ctrName)

	p.stopContainerManagers(ctrID, podName, ctrName)

	return nil, nil
}

// stopContainerManagers stops and removes all cgroup managers and
// pending configs associated with the given container.
func (p *nriPlugin) stopContainerManagers(ctrID string, podName, ctrName string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.pending, ctrID)

	prefix := ctrID + ":"
	for key, mgr := range p.managers {
		if strings.HasPrefix(key, prefix) {
			mgr.Stop()
			delete(p.managers, key)
			klog.V(3).Infof("NRI: stopped cgmpolmgr for pod=%s ctr=%s key=%s", podName, ctrName, key)
		}
	}
}

// validatePolicy checks that a PreparedClaimInfo has a valid memory
// policy suitable for cgmpolmgr. Returns nil if info.Policy is nil
// (no policy means no steering requested).
func validatePolicy(info *PreparedClaimInfo) error {
	if info.Policy == nil {
		return nil
	}

	p := info.Policy

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

	// Verify DRAM and CXL devices are present.
	hasDRAM := false
	hasCXL := false
	for _, dev := range info.Devices {
		switch dev.DeviceType {
		case device.DeviceTypeDRAM:
			hasDRAM = true
		case device.DeviceTypeCXLNode:
			hasCXL = true
		}
	}
	if !hasDRAM {
		return fmt.Errorf("no DRAM devices in claim, memory steering requires both DRAM and CXL")
	}
	if !hasCXL {
		return fmt.Errorf("no CXL devices in claim, memory steering requires both DRAM and CXL")
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

// buildCgroupConfig creates a cgmpolmgr.CgroupConfig from the
// claim's device/policy information. The cgroupPath parameter is
// stored as-is in the config's Path field; pass an empty string if
// the path will be resolved later (e.g. in StartContainer).
func buildCgroupConfig(cgroupPath string, info *PreparedClaimInfo) (*cgmpolmgr.CgroupConfig, error) {
	// Aggregate DRAM and CXL NUMA nodes and quotas.
	dramNodeSet := make(map[int]bool)
	cxlNodeSet := make(map[int]bool)
	var dramQuota, cxlQuota uint64

	for _, dev := range info.Devices {
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

	if len(dramNodeSet) == 0 {
		return nil, fmt.Errorf("no DRAM NUMA nodes found in claim devices")
	}
	if len(cxlNodeSet) == 0 {
		return nil, fmt.Errorf("no CXL NUMA nodes found in claim devices")
	}

	return &cgmpolmgr.CgroupConfig{
		Path:               cgroupPath,
		MemoryUseOrder:     info.Policy.MemoryUseOrder,
		MemoryUseWaypoints: info.Policy.MemoryUseWaypoints,
		DRAMNodes:          formatNodeset(dramNodeSet),
		CXLNodes:           formatNodeset(cxlNodeSet),
		DRAMQuota:          strconv.FormatUint(dramQuota, 10),
		CXLQuota:           strconv.FormatUint(cxlQuota, 10),
		MinLimit:           info.Policy.MinStep,
		MaxLimit:           info.Policy.MaxStep,
	}, nil
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
