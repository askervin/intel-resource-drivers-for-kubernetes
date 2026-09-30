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

// pendingSteering is the memory steering of a container that has
// been created but not started yet.
type pendingSteering struct {
	name string
	plan *cgmpolmgr.Plan
}

// nriPlugin implements the NRI container lifecycle hooks that start
// and stop memory steering of containers with CXL claims.
type nriPlugin struct {
	stub     stub.Stub
	state    *nodeState
	pending  map[string]pendingSteering    // key: containerID
	managers map[string]*cgmpolmgr.Manager // key: containerID
	mu       sync.Mutex                    // protects pending and managers
}

// klogLogger writes cgmpolmgr log messages with klog.
type klogLogger struct{}

func (klogLogger) Debugf(format string, args ...any) { klog.V(4).Infof(format, args...) }
func (klogLogger) Infof(format string, args ...any)  { klog.Infof(format, args...) }
func (klogLogger) Warnf(format string, args ...any)  { klog.Warningf(format, args...) }
func (klogLogger) Errorf(format string, args ...any) { klog.Errorf(format, args...) }

// startNRIPlugin creates and starts the NRI plugin. It connects to
// the container runtime's NRI socket and begins listening for
// container lifecycle events.
func startNRIPlugin(ctx context.Context, state *nodeState, opts NRIOpts) (*nriPlugin, error) {
	p := &nriPlugin{
		state:    state,
		pending:  make(map[string]pendingSteering),
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
// of the container into one memory steering plan and stores it until
// StartContainer. Returns an error if the claims specify conflicting
// memory policies or no plan can be built, blocking container
// creation.
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

	mem, userPolicy, err := p.claimedMemory(ctrName, claimUIDs)
	if err != nil {
		return nil, nil, fmt.Errorf("NRI CreateContainer: ctr=%s: %w", ctrName, err)
	}
	if len(mem.DRAMNodes) == 0 && len(mem.CXLNodes) == 0 {
		klog.V(5).Infof("NRI CreateContainer: ctr=%s: no DRAM or CXL devices in claims", ctrName)
		return nil, nil, nil
	}

	policy := defaultPolicy(mem)
	if userPolicy != nil {
		policy = *userPolicy
	}
	plan, err := policy.Plan(mem)
	if err != nil {
		return nil, nil, fmt.Errorf("NRI CreateContainer: ctr=%s: failed to build memory steering plan: %w",
			ctrName, err)
	}

	p.mu.Lock()
	p.pending[ctrID] = pendingSteering{name: ctrName, plan: plan}
	p.mu.Unlock()

	klog.V(5).Infof("NRI CreateContainer: ctr=%s: prepared memory steering (order=%s, dramNodes=%v, cxlNodes=%v, %s)",
		ctrName, policy.MemoryUseOrder, mem.DRAMNodes, mem.CXLNodes, plan)

	return nil, nil, nil
}

// RemoveContainer is called by the container runtime (via NRI) when a
// container is being removed. It stops memory steering of the
// container.
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
// container has been started. It resolves the cgroup directory of the
// container and starts steering its memory along the plan stored in
// CreateContainer.
func (p *nriPlugin) StartContainer(_ context.Context, pod *api.PodSandbox, ctr *api.Container) error {
	ctrName := pod.GetNamespace() + "/" + pod.GetName() + "/" + ctr.GetName()
	ctrID := ctr.GetId()
	ctrPid := ctr.GetPid()

	p.mu.Lock()
	pending, hasPending := p.pending[ctrID]
	if hasPending {
		delete(p.pending, ctrID)
	}
	p.mu.Unlock()

	if !hasPending {
		klog.V(3).Infof("NRI StartContainer: ctr=%s id=%s: no memory claims to be managed", ctrName, ctrID)
		return nil
	}

	cgroupDir, err := resolveContainerCgroupDir(ctr)
	if err != nil {
		klog.Errorf("NRI StartContainer: ctr=%s id=%s: failed to resolve cgroup directory: %v",
			ctrName, ctrID, err)
		return nil
	}

	klog.V(3).Infof("NRI StartContainer: ctr=%s id=%s pid=%d cgroupsPath=%s", ctrName, ctrID, ctrPid, cgroupDir)

	mgr, err := cgmpolmgr.New(cgroupDir, pending.plan,
		cgmpolmgr.WithName(pending.name),
		cgmpolmgr.WithLogger(klogLogger{}))
	if err != nil {
		klog.Warningf("NRI StartContainer: ctr=%s: failed to create memory steering manager: %v",
			ctrName, err)
		return nil
	}

	if err := mgr.Start(); err != nil {
		klog.Warningf("NRI StartContainer: ctr=%s: failed to start memory steering: %v",
			ctrName, err)
		return nil
	}

	p.mu.Lock()
	p.managers[ctrID] = mgr
	p.mu.Unlock()

	klog.V(3).Infof("NRI StartContainer: ctr=%s: started memory steering (cgroup=%s)", ctrName, cgroupDir)

	return nil
}

// StopContainer is called by the container runtime (via NRI) when a
// container is being stopped. Stops memory steering as a safety net
// in case RemoveContainer is not called.
func (p *nriPlugin) StopContainer(_ context.Context, pod *api.PodSandbox, ctr *api.Container) ([]*api.ContainerUpdate, error) {
	ctrName := pod.GetNamespace() + "/" + pod.GetName() + "/" + ctr.GetName()
	ctrID := ctr.GetId()
	klog.V(3).Infof("NRI StopContainer: ctr=%s", ctrName)

	p.stopContainerManagers(ctrID, ctrName)

	return nil, nil
}

// stopContainerManagers stops and removes the memory steering manager
// and the pending plan of the container.
func (p *nriPlugin) stopContainerManagers(ctrID string, ctrName string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.pending, ctrID)

	if mgr, ok := p.managers[ctrID]; ok {
		mgr.Stop()
		delete(p.managers, ctrID)
		klog.V(3).Infof("NRI: stopped memory steering for ctr=%s", ctrName)
	}
}

// claimedMemory returns the memory types and quotas of the devices in
// the claims of a container, and the memory policy specified in the
// claims, if any. Returns an error if more than one claim specifies a
// policy.
func (p *nriPlugin) claimedMemory(ctrName string, claimUIDs []string) (cgmpolmgr.MemoryTypes, *cgmpolmgr.Policy, error) {
	dramNodeSet := make(map[int]bool)
	cxlNodeSet := make(map[int]bool)
	var mem cgmpolmgr.MemoryTypes
	var userPolicy *cgmpolmgr.Policy

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
				mem.DRAMQuota += dev.ConsumedBytes
			case device.DeviceTypeCXLNode:
				for _, n := range dev.NUMANodes {
					cxlNodeSet[n] = true
				}
				mem.CXLQuota += dev.ConsumedBytes
			}
		}

		if info.Policy != nil && info.Policy.MemoryUseOrder != "" {
			klog.V(5).Infof("NRI CreateContainer: ctr=%s claim=%s policy: order=%s waypoints=%v minStep=%s maxStep=%s",
				ctrName, claimUID,
				info.Policy.MemoryUseOrder, info.Policy.MemoryUseWaypoints,
				info.Policy.MinStep, info.Policy.MaxStep)
			if userPolicy != nil {
				return mem, nil, fmt.Errorf("multiple claims specify memory policies, only one is allowed per container")
			}
			userPolicy = &info.Policy.Policy
		}
	}
	mem.DRAMNodes = sortedNodes(dramNodeSet)
	mem.CXLNodes = sortedNodes(cxlNodeSet)
	return mem, userPolicy, nil
}

// defaultPolicy returns the memory policy of containers whose claims
// specify none: the only memory type present is consumed alone, and
// DRAM and CXL are interleaved when both are present. Step sizes take
// their defaults from the plan.
func defaultPolicy(mem cgmpolmgr.MemoryTypes) cgmpolmgr.Policy {
	switch {
	case len(mem.CXLNodes) == 0:
		return cgmpolmgr.Policy{MemoryUseOrder: cgmpolmgr.MemoryUseFirstDRAM.String()}
	case len(mem.DRAMNodes) == 0:
		return cgmpolmgr.Policy{MemoryUseOrder: cgmpolmgr.MemoryUseFirstCXL.String()}
	default:
		return cgmpolmgr.Policy{MemoryUseOrder: cgmpolmgr.MemoryUseStartInterleaved.String()}
	}
}

// sortedNodes returns the nodes of the set in ascending order.
func sortedNodes(nodes map[int]bool) []int {
	sorted := make([]int, 0, len(nodes))
	for n := range nodes {
		sorted = append(sorted, n)
	}
	sort.Ints(sorted)
	return sorted
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
