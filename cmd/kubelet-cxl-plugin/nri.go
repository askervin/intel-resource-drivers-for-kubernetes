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
// new container is about to be created. This is where we look up the
// memory policy for the pod's resource claims and will eventually
// configure cgroup memory steering.
func (p *nriPlugin) CreateContainer(_ context.Context, pod *api.PodSandbox, ctr *api.Container) (*api.ContainerAdjustment, []*api.ContainerUpdate, error) {
	podUID := pod.GetUid()
	podName := pod.GetNamespace() + "/" + pod.GetName()
	ctrName := ctr.GetName()

	klog.V(3).Infof("NRI CreateContainer: pod=%s ctr=%s uid=%s", podName, ctrName, podUID)

	// Look up memory policies for all claims prepared for this pod.
	policies := p.state.GetMemoryPoliciesForPod(podUID)
	if len(policies) == 0 {
		klog.V(5).Infof("NRI CreateContainer: no memory policies for pod %s", podName)
		return nil, nil, nil
	}

	for claimUID, policy := range policies {
		klog.V(3).Infof("NRI CreateContainer: pod=%s ctr=%s claim=%s policy: order=%s minStep=%s maxStep=%s",
			podName, ctrName, claimUID, policy.MemoryUseOrder, policy.MinStep, policy.MaxStep)
	}

	// TODO: Start cgmpolmgr.Manager for this container's cgroup
	// using the resolved policies combined with DRAM/CXL node IDs
	// and quotas from the allocation results.

	return nil, nil, nil
}

// RemoveContainer is called by the container runtime (via NRI) when a
// container is being removed. This is where we will stop the cgroup
// memory policy manager for the container.
func (p *nriPlugin) RemoveContainer(_ context.Context, pod *api.PodSandbox, ctr *api.Container) error {
	podName := pod.GetNamespace() + "/" + pod.GetName()
	ctrName := ctr.GetName()

	klog.V(3).Infof("NRI RemoveContainer: pod=%s ctr=%s", podName, ctrName)

	// TODO: Stop cgmpolmgr.Manager for this container.

	return nil
}
