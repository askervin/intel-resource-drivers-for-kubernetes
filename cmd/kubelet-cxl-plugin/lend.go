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

// Lend primitives: lending NUMA nodes of CXL memory to running
// containers by growing and shrinking their cpuset.mems through NRI
// UpdateContainers. A node is lent on behalf of holders (leases): devices
// under one CXL host bridge share a NUMA node, so two leases of a
// container can hold the same node, and the node stays lent until its
// last holder revokes it.
//
// Locking: p.mu (nri.go) protects the container table and is taken by
// the NRI event handlers. It is never held across UpdateContainers:
// containerd serializes NRI events and unsolicited updates with one
// lock, so an update issued while an event handler waits for p.mu
// would deadlock until the request timeout. p.lendMu serializes
// LendNode and RevokeNode so that two concurrent lends to the same
// container cannot compute cpuset.mems from the same stale lent set.
// For the same reason LendNode and RevokeNode must not be called
// synchronously from an NRI event handler, and the OnContainerGone and
// OnContainerStarted callbacks run in their own goroutines.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/containerd/nri/pkg/api"
	"k8s.io/klog/v2"
	"k8s.io/utils/cpuset"
)

// containerRef is a running container known to the NRI stub.
type containerRef struct {
	PodUID, PodNamespace, PodName, Name, ID string
	Pid                                     uint32 // init pid at StartContainer (0 if unknown)
	CgroupPath                              string // absolute cgroup v2 dir, as resolveContainerCgroupDir
	BaseMems                                string // cpuset.mems.effective at start, e.g. "0-1"
}

// nodeLender lends NUMA nodes to running containers through NRI.
type nodeLender interface {
	// ContainerByPid finds the container whose cgroup contains pid
	// (/proc/<pid>/cgroup prefix match against CgroupPath).
	ContainerByPid(pid int) (*containerRef, bool)
	// Container by pod UID and container name.
	Container(podUID, name string) (*containerRef, bool)
	// LendNode lends NUMA node to the container on behalf of holder: the
	// node is in the container's cpuset.mems, set through NRI
	// UpdateContainers (mems = BaseMems + all nodes that have a
	// holder). Idempotent per node and holder.
	LendNode(ctx context.Context, podUID, name string, node int, holder string) error
	// RevokeNode drops holder of the node; the node leaves cpuset.mems
	// (pages migrate back) when it has no holder left. Idempotent per
	// node and holder; a container that is gone is not an error.
	RevokeNode(ctx context.Context, podUID, name string, node int, holder string) error
	// LentNodes returns the nodes currently lent to the container.
	LentNodes(podUID, name string) []int
	// OnContainerGone registers a callback for StopContainer/RemoveContainer
	// of a container that has lends or a lease socket.
	OnContainerGone(func(ref containerRef))
	// OnContainerStarted registers a callback for StartContainer.
	OnContainerStarted(func(ref containerRef))
}

var _ nodeLender = (*nriPlugin)(nil)

// errSteered is returned by LendNode for containers whose memory is
// steered by cgmpolmgr because of a local CXL claim.
var errSteered = errors.New("container is memory-steered by a local CXL claim; lending is not supported together with steering yet")

// procDir is the proc filesystem root (a variable for tests).
var procDir = "/proc"

// cgroupRoot is the cgroup v2 mount point (a variable for tests).
var cgroupRoot = cgroupV2MountPoint

// nriStub is the part of stub.Stub that nriPlugin uses.
type nriStub interface {
	Start(context.Context) error
	Stop()
	UpdateContainers([]*api.ContainerUpdate) ([]*api.ContainerUpdate, error)
}

// trackedContainer is an entry of the container table.
type trackedContainer struct {
	ref     containerRef
	seq     uint64        // creation order, the newest wins in Container()
	started bool          // StartContainer seen (or running at Synchronize)
	synced  bool          // learned from Synchronize, not from Create/Start
	steered bool          // has DRAM/CXL claims steered by cgmpolmgr
	base    cpuset.CPUSet // BaseMems parsed
	lent    lentNodes     // nodes lent to this container id
}

// lentNodes maps the nodes lent to a container to their holders.
type lentNodes map[int]map[string]bool

// nodes returns the lent nodes that have a holder.
func (l lentNodes) nodes() cpuset.CPUSet {
	nodes := make([]int, 0, len(l))
	for n, holders := range l {
		if len(holders) > 0 {
			nodes = append(nodes, n)
		}
	}
	return cpuset.New(nodes...)
}

// with returns the holders of node after holder lends (lend) or revokes
// (!lend) it.
func (l lentNodes) with(node int, holder string, lend bool) map[string]bool {
	holders := make(map[string]bool, len(l[node])+1)
	for h := range l[node] {
		holders[h] = true
	}
	if lend {
		holders[holder] = true
	} else {
		delete(holders, holder)
	}
	return holders
}

// after returns the lent nodes that have a holder when node has holders.
func (l lentNodes) after(node int, holders map[string]bool) cpuset.CPUSet {
	if len(holders) > 0 {
		return l.nodes().Union(cpuset.New(node))
	}
	return l.nodes().Difference(cpuset.New(node))
}

// set records the holders of node.
func (l lentNodes) set(node int, holders map[string]bool) lentNodes {
	if l == nil {
		l = lentNodes{}
	}
	if len(holders) == 0 {
		delete(l, node)
	} else {
		l[node] = holders
	}
	return l
}

// initLendState initializes the container table of the plugin.
func (p *nriPlugin) initLendState() {
	if p.ctrs == nil {
		p.ctrs = make(map[string]*trackedContainer)
	}
	if p.syncedC == nil {
		p.syncedC = make(chan struct{})
	}
}

// WaitSynchronized waits until the first NRI Synchronize has filled
// the container table, or ctx is done. Not part of nodeLender: lease code
// that runs at plugin start may call it before reconciling.
func (p *nriPlugin) WaitSynchronized(ctx context.Context) error {
	p.mu.Lock()
	p.initLendState()
	c := p.syncedC
	p.mu.Unlock()
	select {
	case <-c:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("NRI not synchronized yet: %w", ctx.Err())
	}
}

// trackCreated adds a created container to the table. Caller holds p.mu.
func (p *nriPlugin) trackCreated(pod *api.PodSandbox, ctr *api.Container) *trackedContainer {
	p.initLendState()
	id := ctr.GetId()
	if tc, ok := p.ctrs[id]; ok {
		return tc
	}
	p.seq++
	tc := &trackedContainer{
		ref: containerRef{
			PodUID:       pod.GetUid(),
			PodNamespace: pod.GetNamespace(),
			PodName:      pod.GetName(),
			Name:         ctr.GetName(),
			ID:           id,
		},
		seq: p.seq,
	}
	p.ctrs[id] = tc
	return tc
}

// trackStarted records pid, cgroup and base memory nodes of a started
// container. Caller holds p.mu. Returns the entry.
func (p *nriPlugin) trackStarted(pod *api.PodSandbox, ctr *api.Container) *trackedContainer {
	tc := p.trackCreated(pod, ctr)
	tc.started = true
	tc.ref.Pid = ctr.GetPid()
	if tc.ref.CgroupPath == "" {
		if dir, err := resolveContainerCgroupDir(ctr); err == nil {
			tc.ref.CgroupPath = dir
		} else {
			klog.V(3).Infof("NRI lend: ctr=%s id=%s: no cgroup directory: %v", tc.name(), tc.ref.ID, err)
		}
	}
	tc.base = p.baseMems(tc, ctr)
	tc.ref.BaseMems = tc.base.String()
	return tc
}

// baseMems returns the memory nodes a container has of its own: its
// cpuset.mems.effective (or, if unreadable, the cpuset.mems of its
// spec), minus the nodes currently lent to any container by this
// plugin. An unconstrained container started while a lent CXL node is
// online would otherwise count that node as its own. Caller holds p.mu.
func (p *nriPlugin) baseMems(tc *trackedContainer, ctr *api.Container) cpuset.CPUSet {
	var mems string
	if tc.ref.CgroupPath != "" {
		if data, err := os.ReadFile(filepath.Join(tc.ref.CgroupPath, "cpuset.mems.effective")); err == nil {
			mems = strings.TrimSpace(string(data))
		} else {
			klog.V(3).Infof("NRI lend: ctr=%s: %v", tc.name(), err)
		}
	}
	if mems == "" {
		mems = ctr.GetLinux().GetResources().GetCpu().GetMems()
	}
	set, err := cpuset.Parse(mems)
	if err != nil {
		klog.Warningf("NRI lend: ctr=%s: invalid memory nodes %q: %v", tc.name(), mems, err)
		return cpuset.New()
	}
	return set.Difference(p.allLentNodes())
}

// allLentNodes returns the union of the nodes lent to any container.
// Caller holds p.mu.
func (p *nriPlugin) allLentNodes() cpuset.CPUSet {
	all := cpuset.New()
	for _, tc := range p.ctrs {
		all = all.Union(tc.lent.nodes())
	}
	return all
}

// untrack removes a container from the table and returns its entry,
// or nil if it was not tracked. Caller holds p.mu.
func (p *nriPlugin) untrack(id string) *trackedContainer {
	tc, ok := p.ctrs[id]
	if !ok {
		return nil
	}
	delete(p.ctrs, id)
	return tc
}

func (tc *trackedContainer) name() string {
	return tc.ref.PodNamespace + "/" + tc.ref.PodName + "/" + tc.ref.Name
}

// snapshot returns a copy of the reference of the container.
func (tc *trackedContainer) snapshot() containerRef {
	ref := tc.ref
	ref.BaseMems = tc.base.String()
	return ref
}

// fireGone runs the OnContainerGone callbacks in the background.
// Caller holds p.mu.
func (p *nriPlugin) fireGone(tc *trackedContainer) {
	if tc == nil {
		return
	}
	if lent := tc.lent.nodes(); !lent.IsEmpty() {
		klog.V(3).Infof("NRI lend: ctr=%s id=%s gone with lent nodes %s", tc.name(), tc.ref.ID, lent)
	}
	ref := tc.snapshot()
	for _, fn := range p.goneFns {
		go fn(ref)
	}
}

// fireStarted runs the OnContainerStarted callbacks in the background.
// Caller holds p.mu.
func (p *nriPlugin) fireStarted(tc *trackedContainer) {
	ref := tc.snapshot()
	for _, fn := range p.startedFns {
		go fn(ref)
	}
}

// containerGone removes a stopped or removed container from the table
// and fires OnContainerGone once.
func (p *nriPlugin) containerGone(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.initLendState()
	p.fireGone(p.untrack(id))
}

// Synchronize implements stub.SynchronizeInterface. It rebuilds the
// container table from the runtime's pods and containers when the
// plugin (re)connects. Containers that are no longer present are
// reported gone. Lent sets of containers that remain (a reconnect of a
// running plugin) are kept. After a plugin restart the lent sets are
// empty and BaseMems is cpuset.mems.effective, which still includes the
// nodes lent before the restart; they are dropped from BaseMems when
// the lease code re-applies (LendNode) or revokes (RevokeNode) them.
func (p *nriPlugin) Synchronize(_ context.Context, pods []*api.PodSandbox, ctrs []*api.Container) ([]*api.ContainerUpdate, error) {
	podByID := make(map[string]*api.PodSandbox, len(pods))
	for _, pod := range pods {
		podByID[pod.GetId()] = pod
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.initLendState()

	seen := make(map[string]bool, len(ctrs))
	for _, ctr := range ctrs {
		state := ctr.GetState()
		if state != api.ContainerState_CONTAINER_CREATED &&
			state != api.ContainerState_CONTAINER_RUNNING &&
			state != api.ContainerState_CONTAINER_PAUSED {
			continue
		}
		pod, ok := podByID[ctr.GetPodSandboxId()]
		if !ok {
			pod = &api.PodSandbox{Id: ctr.GetPodSandboxId()}
		}
		seen[ctr.GetId()] = true
		_, known := p.ctrs[ctr.GetId()]
		var tc *trackedContainer
		if state == api.ContainerState_CONTAINER_CREATED {
			tc = p.trackCreated(pod, ctr)
		} else {
			tc = p.trackStarted(pod, ctr)
		}
		if !known {
			tc.synced = true
			tc.steered = p.hasSteeredClaims(tc.name(), ctr)
		}
		klog.V(4).Infof("NRI Synchronize: ctr=%s id=%s pid=%d cgroup=%s baseMems=%s steered=%v",
			tc.name(), tc.ref.ID, tc.ref.Pid, tc.ref.CgroupPath, tc.base, tc.steered)
	}
	for id := range p.ctrs {
		if !seen[id] {
			p.fireGone(p.untrack(id))
		}
	}

	select {
	case <-p.syncedC:
	default:
		close(p.syncedC)
	}
	klog.V(3).Infof("NRI Synchronize: %d pods, %d containers tracked", len(pods), len(p.ctrs))
	return nil, nil
}

// hasSteeredClaims tells whether CreateContainer would have planned
// memory steering for the container (claims with DRAM or CXL nodes).
func (p *nriPlugin) hasSteeredClaims(ctrName string, ctr *api.Container) bool {
	claimUIDs := extractClaimUIDs(ctr.GetEnv())
	if len(claimUIDs) == 0 || p.state == nil {
		return false
	}
	mem, _, err := p.claimedMemory(ctrName, claimUIDs)
	if err != nil {
		return true
	}
	return len(mem.DRAMNodes) > 0 || len(mem.CXLNodes) > 0
}

// ContainerByPid implements nodeLender.
func (p *nriPlugin) ContainerByPid(pid int) (*containerRef, bool) {
	cgPath, err := pidCgroupDir(pid)
	if err != nil {
		klog.V(4).Infof("NRI lend: pid %d: %v", pid, err)
		return nil, false
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	var best *trackedContainer
	for _, tc := range p.ctrs {
		dir := tc.ref.CgroupPath
		match := dir != "" && (cgPath == dir || strings.HasPrefix(cgPath, dir+"/"))
		if !match && dir == "" && tc.ref.ID != "" {
			// cgroup not resolved: runtimes name the scope after the id
			// (cri-containerd-<id>.scope, crio-<id>.scope).
			match = strings.Contains(cgPath, tc.ref.ID)
		}
		if match && (best == nil || len(dir) > len(best.ref.CgroupPath)) {
			best = tc
		}
	}
	if best == nil {
		return nil, false
	}
	ref := best.snapshot()
	return &ref, true
}

// pidCgroupDir returns the absolute cgroup v2 directory of a process.
func pidCgroupDir(pid int) (string, error) {
	path := filepath.Join(procDir, fmt.Sprint(pid), "cgroup")
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if rel, ok := strings.CutPrefix(line, "0::"); ok {
			return filepath.Join(cgroupRoot, rel), nil
		}
	}
	return "", fmt.Errorf("no cgroup v2 entry in %s", path)
}

// lookup returns the newest container of the pod with the name.
// Caller holds p.mu.
func (p *nriPlugin) lookup(podUID, name string) *trackedContainer {
	var best *trackedContainer
	for _, tc := range p.ctrs {
		if tc.ref.PodUID == podUID && tc.ref.Name == name && (best == nil || tc.seq > best.seq) {
			best = tc
		}
	}
	return best
}

// Container implements nodeLender.
func (p *nriPlugin) Container(podUID, name string) (*containerRef, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	tc := p.lookup(podUID, name)
	if tc == nil {
		return nil, false
	}
	ref := tc.snapshot()
	return &ref, true
}

// LentNodes implements nodeLender.
func (p *nriPlugin) LentNodes(podUID, name string) []int {
	p.mu.Lock()
	defer p.mu.Unlock()
	tc := p.lookup(podUID, name)
	if tc == nil {
		return nil
	}
	return tc.lent.nodes().List()
}

// OnContainerGone implements nodeLender.
func (p *nriPlugin) OnContainerGone(fn func(ref containerRef)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.goneFns = append(p.goneFns, fn)
}

// OnContainerStarted implements nodeLender.
func (p *nriPlugin) OnContainerStarted(fn func(ref containerRef)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.startedFns = append(p.startedFns, fn)
}

// LendNode implements nodeLender.
func (p *nriPlugin) LendNode(ctx context.Context, podUID, name string, node int, holder string) error {
	if node < 0 {
		return fmt.Errorf("invalid NUMA node %d", node)
	}
	return p.changeLend(ctx, podUID, name, node, holder, true)
}

// RevokeNode implements nodeLender.
func (p *nriPlugin) RevokeNode(ctx context.Context, podUID, name string, node int, holder string) error {
	if node < 0 {
		return fmt.Errorf("invalid NUMA node %d", node)
	}
	return p.changeLend(ctx, podUID, name, node, holder, false)
}

// changeLend adds (lend) or removes (!lend) holder of node in the lent
// nodes of a container and applies cpuset.mems = base + the nodes that
// have a holder through NRI. The node is a CXL node, so it is also
// dropped from the base set (it is there after a plugin restart, see
// Synchronize).
func (p *nriPlugin) changeLend(ctx context.Context, podUID, name string, node int, holder string, lend bool) error {
	op := "revoke"
	if lend {
		op = "lend"
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p.lendMu.Lock()
	defer p.lendMu.Unlock()

	p.mu.Lock()
	if p.stub == nil {
		p.mu.Unlock()
		return fmt.Errorf("%s node %d: lending unavailable: NRI not connected", op, node)
	}
	p.initLendState()
	tc := p.lookup(podUID, name)
	if tc == nil {
		p.mu.Unlock()
		if !lend {
			klog.V(3).Infof("NRI lend: revoke node %d from pod %s container %s: container gone", node, podUID, name)
			return nil
		}
		return fmt.Errorf("lend node %d: container %q of pod %s not found", node, name, podUID)
	}
	ctrName, id := tc.name(), tc.ref.ID
	if lend && (tc.steered || p.managers[id] != nil || p.pending[id].plan != nil) {
		p.mu.Unlock()
		return fmt.Errorf("lend node %d to %s: %w", node, ctrName, errSteered)
	}
	if !tc.started {
		p.mu.Unlock()
		if !lend {
			return nil
		}
		return fmt.Errorf("lend node %d to %s: container not started yet", node, ctrName)
	}

	nodeSet := cpuset.New(node)
	base := tc.base.Difference(nodeSet)
	holders := tc.lent.with(node, holder, lend)
	lent := tc.lent.after(node, holders)
	needed := !sameSet(lent, tc.lent.nodes()) || !sameSet(base, tc.base)
	if !lend && !needed && len(holders) == 0 {
		// Not lent as far as we know, but a lend from before a plugin
		// restart that is not re-applied still shows in cpuset.mems.
		needed = cgroupMemsContain(tc.ref.CgroupPath, node)
	}
	if !needed {
		tc.lent = tc.lent.set(node, holders)
		p.mu.Unlock()
		klog.V(4).Infof("NRI lend: %s node %d for %s: ctr=%s: cpuset.mems unchanged (base=%s lent=%s, node holders %d)",
			op, node, holder, ctrName, base, lent, len(holders))
		return nil
	}
	if base.IsEmpty() {
		p.mu.Unlock()
		return fmt.Errorf("%s node %d: ctr=%s: base memory nodes unknown, refusing to set cpuset.mems without them",
			op, node, ctrName)
	}
	mems := base.Union(lent).String()
	s := p.stub
	p.mu.Unlock()

	klog.V(3).Infof("NRI lend: %s node %d for %s: ctr=%s id=%s cpuset.mems=%s (base=%s lent=%s)", op, node, holder, ctrName, id, mems, base, lent)
	if err := updateMems(s, id, mems); err != nil {
		return fmt.Errorf("%s node %d: ctr=%s: %w", op, node, ctrName, err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if cur, ok := p.ctrs[id]; ok && cur == tc {
		tc.base = base
		tc.ref.BaseMems = base.String()
		tc.lent = tc.lent.set(node, holders)
	}
	return nil
}

// updateMems sets cpuset.mems of a container through NRI.
func updateMems(s nriStub, id, mems string) error {
	failed, err := s.UpdateContainers([]*api.ContainerUpdate{{
		ContainerId: id,
		Linux: &api.LinuxContainerUpdate{
			Resources: &api.LinuxResources{
				Cpu: &api.LinuxCPU{Mems: mems},
			},
		},
	}})
	if err != nil {
		return fmt.Errorf("NRI UpdateContainers(cpuset.mems=%s): %w", mems, err)
	}
	for _, f := range failed {
		if f.GetContainerId() == id {
			return fmt.Errorf("NRI UpdateContainers(cpuset.mems=%s): update failed in the runtime", mems)
		}
	}
	return nil
}

// cgroupMemsContain tells whether cpuset.mems of a cgroup contains node.
func cgroupMemsContain(dir string, node int) bool {
	if dir == "" {
		return false
	}
	data, err := os.ReadFile(filepath.Join(dir, "cpuset.mems"))
	if err != nil {
		return false
	}
	set, err := cpuset.Parse(strings.TrimSpace(string(data)))
	return err == nil && set.Contains(node)
}

// sameSet compares sets by content (CPUSet.Equals tells a zero set
// from an empty one).
func sameSet(a, b cpuset.CPUSet) bool {
	return a.IsSubsetOf(b) && b.IsSubsetOf(a)
}
