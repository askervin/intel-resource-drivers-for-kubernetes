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

	"github.com/containerd/nri/pkg/api"
	"github.com/containerd/nri/pkg/stub"

	"k8s.io/klog/v2"
)

// NRIOpts holds NRI plugin configuration from CLI flags.
type NRIOpts struct {
	Name   string
	Idx    string
	Socket string
}

// nriPlugin implements the NRI CreateContainer and RemoveContainer
// interfaces, sharing access to the DRA driver's nodeState.
type nriPlugin struct {
	stub  stub.Stub
	state *nodeState
}

// startNRIPlugin creates and starts the NRI plugin. It connects to
// the container runtime's NRI socket and begins listening for
// container lifecycle events. The plugin runs in the background;
// call Stop() to shut it down.
func startNRIPlugin(ctx context.Context, state *nodeState, opts NRIOpts) (*nriPlugin, error) {
	p := &nriPlugin{
		state: state,
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

// Stop shuts down the NRI plugin.
func (p *nriPlugin) Stop() {
	if p.stub != nil {
		p.stub.Stop()
		klog.V(3).Info("NRI plugin stopped")
	}
}

func (p *nriPlugin) onClose() {
	klog.Warning("NRI connection to the runtime lost")
}

// CreateContainer is called by the container runtime (via NRI) when a
// new container is about to be created. It identifies which resource
// claims belong to this specific container by scanning CDI-injected
// CXL_CLAIM_* environment variables, then looks up full claim info
// from in-process state.
func (p *nriPlugin) CreateContainer(_ context.Context, pod *api.PodSandbox, ctr *api.Container) (*api.ContainerAdjustment, []*api.ContainerUpdate, error) {
	podName := pod.GetNamespace() + "/" + pod.GetName()
	ctrName := ctr.GetName()

	klog.V(3).Infof("NRI CreateContainer: pod=%s ctr=%s", podName, ctrName)

	// Scan container env vars for CDI-injected claim markers.
	claimUIDs := extractClaimUIDs(ctr.GetEnv())
	if len(claimUIDs) == 0 {
		klog.V(5).Infof("NRI CreateContainer: no CXL claims for pod=%s ctr=%s", podName, ctrName)
		return nil, nil, nil
	}

	klog.V(5).Infof("NRI CreateContainer: pod=%s ctr=%s has %d CXL claim(s): %v", podName, ctrName, len(claimUIDs), claimUIDs)

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
	}

	// TODO: Start cgmpolmgr.Manager for this container's cgroup
	// using the resolved policies combined with DRAM/CXL node IDs
	// and quotas from the allocation results.

	return nil, nil, nil
}

// RemoveContainer is called by the container runtime (via NRI) when a
// container is being removed. It logs which claims were associated
// with the container for diagnostic purposes.
func (p *nriPlugin) RemoveContainer(_ context.Context, pod *api.PodSandbox, ctr *api.Container) error {
	podName := pod.GetNamespace() + "/" + pod.GetName()
	ctrName := ctr.GetName()

	claimUIDs := extractClaimUIDs(ctr.GetEnv())
	if len(claimUIDs) == 0 {
		klog.V(5).Infof("NRI RemoveContainer: no CXL claims for pod=%s ctr=%s", podName, ctrName)
		return nil
	}

	klog.V(5).Infof("NRI RemoveContainer: pod=%s ctr=%s releasing claims: %v", podName, ctrName, claimUIDs)

	// TODO: Stop cgmpolmgr.Manager for this container.

	return nil
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
