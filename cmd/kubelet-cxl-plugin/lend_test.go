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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/containers/nri-plugins/pkg/cgmpolmgr"
)

// ltStub is a fake NRI stub that records UpdateContainers.
type ltStub struct {
	mu      sync.Mutex
	updates []string // "<id> mems=<mems>"
	fail    bool     // report the update as failed
	err     error    // return this error
}

func (s *ltStub) Start(context.Context) error { return nil }
func (s *ltStub) Stop()                       {}
func (s *ltStub) UpdateContainers(u []*api.ContainerUpdate) ([]*api.ContainerUpdate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	if s.fail {
		return u, nil
	}
	for _, cu := range u {
		s.updates = append(s.updates, cu.GetContainerId()+" mems="+cu.GetLinux().GetResources().GetCpu().GetMems())
	}
	return nil, nil
}

func (s *ltStub) taken() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.updates
	s.updates = nil
	return out
}

// ltEnv is a plugin with a fake stub and fake /proc and cgroupfs.
type ltEnv struct {
	t     *testing.T
	p     *nriPlugin
	stub  *ltStub
	root  string
	gone  chan containerRef
	start chan containerRef
}

func newLtEnv(t *testing.T) *ltEnv {
	t.Helper()
	root := t.TempDir()
	oldProc, oldCg := procDir, cgroupRoot
	procDir, cgroupRoot = filepath.Join(root, "proc"), filepath.Join(root, "cg")
	t.Cleanup(func() { procDir, cgroupRoot = oldProc, oldCg })

	e := &ltEnv{
		t:     t,
		stub:  &ltStub{},
		root:  root,
		gone:  make(chan containerRef, 16),
		start: make(chan containerRef, 16),
	}
	e.p = &nriPlugin{
		stub:     e.stub,
		pending:  make(map[string]pendingSteering),
		managers: make(map[string]*cgmpolmgr.Manager),
	}
	e.p.initLendState()
	e.p.OnContainerGone(func(ref containerRef) { e.gone <- ref })
	e.p.OnContainerStarted(func(ref containerRef) { e.start <- ref })
	return e
}

// proc makes /proc/<pid>/cgroup point to cgroup rel and returns the
// absolute cgroup directory, with cpuset.mems.effective = effMems and
// cpuset.mems = mems.
func (e *ltEnv) proc(pid uint32, rel, effMems, mems string) string {
	e.t.Helper()
	pdir := filepath.Join(procDir, fmt.Sprint(pid))
	cg := filepath.Join(cgroupRoot, rel)
	for _, d := range []string{pdir, cg} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			e.t.Fatal(err)
		}
	}
	write := func(path, data string) {
		if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
			e.t.Fatal(err)
		}
	}
	write(filepath.Join(pdir, "cgroup"), "0::/"+rel+"\n")
	write(filepath.Join(cg, "cpuset.mems.effective"), effMems+"\n")
	write(filepath.Join(cg, "cpuset.mems"), mems+"\n")
	return cg
}

func ltPod(uid string) *api.PodSandbox {
	return &api.PodSandbox{Id: "sb-" + uid, Uid: uid, Namespace: "ns", Name: "pod-" + uid}
}

func ltCtr(pod *api.PodSandbox, id, name string, pid uint32) *api.Container {
	return &api.Container{Id: id, PodSandboxId: pod.GetId(), Name: name, Pid: pid}
}

// run creates and starts a container whose cgroup has effMems.
func (e *ltEnv) run(pod *api.PodSandbox, id, name string, pid uint32, effMems string) *api.Container {
	e.t.Helper()
	e.proc(pid, "kubepods/"+pod.GetUid()+"/"+id, effMems, "")
	ctr := ltCtr(pod, id, name, pid)
	ctx := context.Background()
	if _, _, err := e.p.CreateContainer(ctx, pod, ctr); err != nil {
		e.t.Fatal(err)
	}
	if err := e.p.StartContainer(ctx, pod, ctr); err != nil {
		e.t.Fatal(err)
	}
	e.wait(e.start, id)
	return ctr
}

func (e *ltEnv) wait(c chan containerRef, id string) containerRef {
	e.t.Helper()
	select {
	case ref := <-c:
		if ref.ID != id {
			e.t.Fatalf("callback for %q, expected %q", ref.ID, id)
		}
		return ref
	case <-time.After(5 * time.Second):
		e.t.Fatalf("no callback for %q", id)
	}
	return containerRef{}
}

func (e *ltEnv) noCallback(c chan containerRef) {
	e.t.Helper()
	select {
	case ref := <-c:
		e.t.Fatalf("unexpected callback for %q", ref.ID)
	case <-time.After(50 * time.Millisecond):
	}
}

func (e *ltEnv) expectUpdates(want ...string) {
	e.t.Helper()
	got := e.stub.taken()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		e.t.Fatalf("updates %q, expected %q", got, want)
	}
}

func TestLendContainerTable(t *testing.T) {
	e := newLtEnv(t)
	ctx := context.Background()
	pod := ltPod("u1")
	ctr := ltCtr(pod, "c1", "app", 101)
	cg := e.proc(101, "kubepods/u1/c1", "0-1", "")

	if _, _, err := e.p.CreateContainer(ctx, pod, ctr); err != nil {
		t.Fatal(err)
	}
	ref, ok := e.p.Container("u1", "app")
	if !ok || ref.ID != "c1" || ref.PodNamespace != "ns" || ref.PodName != "pod-u1" || ref.Pid != 0 {
		t.Fatalf("created: %+v %v", ref, ok)
	}
	if err := e.p.LendNode(ctx, "u1", "app", 2); err == nil || !strings.Contains(err.Error(), "not started") {
		t.Fatalf("lend before start: %v", err)
	}

	if err := e.p.StartContainer(ctx, pod, ctr); err != nil {
		t.Fatal(err)
	}
	started := e.wait(e.start, "c1")
	want := containerRef{PodUID: "u1", PodNamespace: "ns", PodName: "pod-u1", Name: "app", ID: "c1",
		Pid: 101, CgroupPath: cg, BaseMems: "0-1"}
	if started != want {
		t.Fatalf("started %+v, expected %+v", started, want)
	}
	if ref, _ := e.p.Container("u1", "app"); *ref != want {
		t.Fatalf("Container %+v, expected %+v", *ref, want)
	}

	// The init pid, and a pid in a sub-cgroup of the container.
	if ref, ok := e.p.ContainerByPid(101); !ok || ref.ID != "c1" {
		t.Fatalf("ContainerByPid(101): %+v %v", ref, ok)
	}
	e.proc(102, "kubepods/u1/c1/sub", "0-1", "")
	if ref, ok := e.p.ContainerByPid(102); !ok || ref.ID != "c1" {
		t.Fatalf("ContainerByPid(102): %+v %v", ref, ok)
	}
	// Prefix match is by path element, not by string.
	e.proc(103, "kubepods/u1/c10", "0-1", "")
	if ref, ok := e.p.ContainerByPid(103); ok {
		t.Fatalf("ContainerByPid(103) matched %+v", ref)
	}
	if _, ok := e.p.ContainerByPid(999); ok {
		t.Fatal("ContainerByPid(999) matched")
	}

	// Stop fires gone once; Remove after Stop does not fire again.
	if _, err := e.p.StopContainer(ctx, pod, ctr); err != nil {
		t.Fatal(err)
	}
	if gone := e.wait(e.gone, "c1"); gone.BaseMems != "0-1" || gone.Pid != 101 {
		t.Fatalf("gone %+v", gone)
	}
	if err := e.p.RemoveContainer(ctx, pod, ctr); err != nil {
		t.Fatal(err)
	}
	e.noCallback(e.gone)
	if _, ok := e.p.Container("u1", "app"); ok {
		t.Fatal("container still known after stop")
	}

	// Remove without Stop fires gone too.
	e.run(pod, "c2", "app", 201, "0")
	if err := e.p.RemoveContainer(ctx, pod, ltCtr(pod, "c2", "app", 201)); err != nil {
		t.Fatal(err)
	}
	e.wait(e.gone, "c2")
	e.expectUpdates()
}

func TestLendMemsArithmetic(t *testing.T) {
	e := newLtEnv(t)
	ctx := context.Background()
	e.run(ltPod("u1"), "c1", "app", 101, "0-1")

	steps := []struct {
		lend bool
		node int
		want string // update, "" for none
		lent []int
	}{
		{true, 2, "c1 mems=0-2", []int{2}},
		{true, 2, "", []int{2}}, // idempotent
		{true, 4, "c1 mems=0-2,4", []int{2, 4}},
		{false, 2, "c1 mems=0-1,4", []int{4}},
		{false, 2, "", []int{4}}, // idempotent
		{false, 4, "c1 mems=0-1", []int{}},
		{false, 4, "", []int{}},
	}
	for i, s := range steps {
		var err error
		if s.lend {
			err = e.p.LendNode(ctx, "u1", "app", s.node)
		} else {
			err = e.p.RevokeNode(ctx, "u1", "app", s.node)
		}
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if s.want == "" {
			e.expectUpdates()
		} else {
			e.expectUpdates(s.want)
		}
		if got := e.p.LentNodes("u1", "app"); !reflect.DeepEqual(got, s.lent) {
			t.Fatalf("step %d: lent %v, expected %v", i, got, s.lent)
		}
	}
	if ref, _ := e.p.Container("u1", "app"); ref.BaseMems != "0-1" {
		t.Fatalf("BaseMems changed: %q", ref.BaseMems)
	}
	if err := e.p.LendNode(ctx, "u1", "app", -1); err == nil {
		t.Fatal("negative node accepted")
	}
}

func TestLendGoneContainer(t *testing.T) {
	e := newLtEnv(t)
	ctx := context.Background()
	pod := ltPod("u1")
	ctr := e.run(pod, "c1", "app", 101, "0")
	if err := e.p.LendNode(ctx, "u1", "app", 3); err != nil {
		t.Fatal(err)
	}
	e.expectUpdates("c1 mems=0,3")

	if _, err := e.p.StopContainer(ctx, pod, ctr); err != nil {
		t.Fatal(err)
	}
	e.wait(e.gone, "c1")
	if err := e.p.RevokeNode(ctx, "u1", "app", 3); err != nil {
		t.Fatalf("revoke of a gone container: %v", err)
	}
	e.expectUpdates()
	if err := e.p.LendNode(ctx, "u1", "app", 3); err == nil {
		t.Fatal("lend to a gone container succeeded")
	}
	if got := e.p.LentNodes("u1", "app"); len(got) != 0 {
		t.Fatalf("lent nodes of a gone container: %v", got)
	}

	// The restarted container (new id) starts without lends.
	e.run(pod, "c1b", "app", 111, "0")
	if got := e.p.LentNodes("u1", "app"); len(got) != 0 {
		t.Fatalf("restarted container has lends: %v", got)
	}
	if err := e.p.LendNode(ctx, "u1", "app", 3); err != nil {
		t.Fatal(err)
	}
	e.expectUpdates("c1b mems=0,3")
}

func TestLendRefusedWhenSteered(t *testing.T) {
	e := newLtEnv(t)
	ctx := context.Background()
	e.run(ltPod("u1"), "c1", "app", 101, "0-1")
	e.run(ltPod("u2"), "c2", "app", 201, "0-1")

	e.p.mu.Lock()
	e.p.managers["c1"] = &cgmpolmgr.Manager{}
	e.p.ctrs["c2"].steered = true
	e.p.mu.Unlock()
	t.Cleanup(func() { delete(e.p.managers, "c1") })

	for _, uid := range []string{"u1", "u2"} {
		err := e.p.LendNode(ctx, uid, "app", 2)
		if !errors.Is(err, errSteered) {
			t.Fatalf("pod %s: expected steering refusal, got %v", uid, err)
		}
		if !strings.Contains(err.Error(), "lending is not supported together with steering yet") {
			t.Fatalf("unclear error: %v", err)
		}
		if err := e.p.RevokeNode(ctx, uid, "app", 2); err != nil {
			t.Fatalf("revoke on a steered container: %v", err)
		}
	}
	e.expectUpdates()
}

func TestLendUpdateFailures(t *testing.T) {
	e := newLtEnv(t)
	ctx := context.Background()
	e.run(ltPod("u1"), "c1", "app", 101, "0")

	e.stub.fail = true
	if err := e.p.LendNode(ctx, "u1", "app", 2); err == nil || !strings.Contains(err.Error(), "failed") {
		t.Fatalf("failed update not reported: %v", err)
	}
	e.stub.fail, e.stub.err = false, errors.New("ttrpc closed")
	if err := e.p.LendNode(ctx, "u1", "app", 2); err == nil || !strings.Contains(err.Error(), "ttrpc closed") {
		t.Fatalf("update error not reported: %v", err)
	}
	if got := e.p.LentNodes("u1", "app"); len(got) != 0 {
		t.Fatalf("failed lends recorded: %v", got)
	}
	e.stub.err = nil
	if err := e.p.LendNode(ctx, "u1", "app", 2); err != nil {
		t.Fatal(err)
	}
	e.expectUpdates("c1 mems=0,2")

	// No stub: NRI not connected.
	e.p.mu.Lock()
	e.p.stub = nil
	e.p.mu.Unlock()
	if err := e.p.LendNode(ctx, "u1", "app", 3); err == nil || !strings.Contains(err.Error(), "NRI not connected") {
		t.Fatalf("expected NRI not connected, got %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := e.p.LendNode(cctx, "u1", "app", 3); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

func TestLendNeverWithoutBaseMems(t *testing.T) {
	e := newLtEnv(t)
	ctx := context.Background()
	pod := ltPod("u1")
	// No cgroup and no spec mems: base unknown.
	ctr := ltCtr(pod, "c1", "app", 0)
	if _, _, err := e.p.CreateContainer(ctx, pod, ctr); err != nil {
		t.Fatal(err)
	}
	if err := e.p.StartContainer(ctx, pod, ctr); err != nil {
		t.Fatal(err)
	}
	e.wait(e.start, "c1")
	if err := e.p.LendNode(ctx, "u1", "app", 2); err == nil || !strings.Contains(err.Error(), "base memory nodes unknown") {
		t.Fatalf("lend without base: %v", err)
	}
	// The only own node is the node being lent.
	e.run(ltPod("u2"), "c2", "app", 201, "2")
	if err := e.p.LendNode(ctx, "u2", "app", 2); err == nil || !strings.Contains(err.Error(), "base memory nodes unknown") {
		t.Fatalf("lend of the only node: %v", err)
	}
	e.expectUpdates()

	// Spec mems is the fallback when cgroupfs is unreadable.
	pod3 := ltPod("u3")
	ctr3 := ltCtr(pod3, "c3", "app", 0)
	ctr3.Linux = &api.LinuxContainer{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Mems: "1"}}}
	if _, _, err := e.p.CreateContainer(ctx, pod3, ctr3); err != nil {
		t.Fatal(err)
	}
	if err := e.p.StartContainer(ctx, pod3, ctr3); err != nil {
		t.Fatal(err)
	}
	e.wait(e.start, "c3")
	if err := e.p.LendNode(ctx, "u3", "app", 2); err != nil {
		t.Fatal(err)
	}
	e.expectUpdates("c3 mems=1-2")
}

func TestLendBaseExcludesNodesLentToOthers(t *testing.T) {
	e := newLtEnv(t)
	ctx := context.Background()
	e.run(ltPod("u1"), "c1", "app", 101, "0-1")
	if err := e.p.LendNode(ctx, "u1", "app", 2); err != nil {
		t.Fatal(err)
	}
	e.expectUpdates("c1 mems=0-2")
	// An unconstrained container started now sees node 2 as effective.
	e.run(ltPod("u2"), "c2", "app", 201, "0-2")
	if ref, _ := e.p.Container("u2", "app"); ref.BaseMems != "0-1" {
		t.Fatalf("BaseMems %q includes a node lent to another container", ref.BaseMems)
	}
}

func TestLendSynchronize(t *testing.T) {
	e := newLtEnv(t)
	ctx := context.Background()
	p1, p2 := ltPod("u1"), ltPod("u2")
	// Before a restart node 2 was lent to c1, node 3 to c2 (and the
	// lease of c2 is not re-applied), c3 is only created.
	cg1 := e.proc(101, "kubepods/u1/c1", "0-2", "0-2")
	e.proc(201, "kubepods/u2/c2", "0,3", "0,3")
	c1 := ltCtr(p1, "c1", "app", 101)
	c1.State = api.ContainerState_CONTAINER_RUNNING
	c2 := ltCtr(p2, "c2", "app", 201)
	c2.State = api.ContainerState_CONTAINER_RUNNING
	c3 := ltCtr(p2, "c3", "init", 0)
	c3.State = api.ContainerState_CONTAINER_CREATED
	c4 := ltCtr(p2, "c4", "old", 0)
	c4.State = api.ContainerState_CONTAINER_STOPPED

	syncCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	if err := e.p.WaitSynchronized(syncCtx); err == nil {
		t.Fatal("synchronized before Synchronize")
	}
	cancel()
	if _, err := e.p.Synchronize(ctx, []*api.PodSandbox{p1, p2}, []*api.Container{c1, c2, c3, c4}); err != nil {
		t.Fatal(err)
	}
	if err := e.p.WaitSynchronized(ctx); err != nil {
		t.Fatal(err)
	}
	e.noCallback(e.start)

	ref, ok := e.p.Container("u1", "app")
	want := containerRef{PodUID: "u1", PodNamespace: "ns", PodName: "pod-u1", Name: "app", ID: "c1",
		Pid: 101, CgroupPath: cg1, BaseMems: "0-2"}
	if !ok || *ref != want {
		t.Fatalf("synced %+v, expected %+v", ref, want)
	}
	if _, ok := e.p.Container("u2", "old"); ok {
		t.Fatal("stopped container tracked")
	}
	if ref, ok := e.p.ContainerByPid(201); !ok || ref.ID != "c2" {
		t.Fatalf("ContainerByPid after sync: %+v %v", ref, ok)
	}

	// Re-applying the lend drops node 2 from BaseMems.
	if err := e.p.LendNode(ctx, "u1", "app", 2); err != nil {
		t.Fatal(err)
	}
	e.expectUpdates("c1 mems=0-2")
	if ref, _ := e.p.Container("u1", "app"); ref.BaseMems != "0-1" {
		t.Fatalf("BaseMems after re-lend %q", ref.BaseMems)
	}
	if err := e.p.RevokeNode(ctx, "u1", "app", 2); err != nil {
		t.Fatal(err)
	}
	e.expectUpdates("c1 mems=0-1")

	// A revoke without a re-lend still shrinks cpuset.mems.
	if err := e.p.RevokeNode(ctx, "u2", "app", 3); err != nil {
		t.Fatal(err)
	}
	e.expectUpdates("c2 mems=0")

	// A later Synchronize (reconnect) keeps lends and reports
	// containers that vanished.
	if err := e.p.LendNode(ctx, "u1", "app", 5); err != nil {
		t.Fatal(err)
	}
	e.expectUpdates("c1 mems=0-1,5")
	if _, err := e.p.Synchronize(ctx, []*api.PodSandbox{p1}, []*api.Container{c1}); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for range 2 {
		select {
		case ref := <-e.gone:
			got[ref.ID] = true
		case <-time.After(5 * time.Second):
			t.Fatal("missing gone callback")
		}
	}
	if !got["c2"] || !got["c3"] {
		t.Fatalf("gone after resync: %v", got)
	}
	if lent := e.p.LentNodes("u1", "app"); !reflect.DeepEqual(lent, []int{5}) {
		t.Fatalf("lent after resync: %v", lent)
	}
}

func TestLendConcurrent(t *testing.T) {
	e := newLtEnv(t)
	ctx := context.Background()
	e.run(ltPod("u1"), "c1", "app", 101, "0")
	var wg sync.WaitGroup
	for n := 1; n <= 8; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			if err := e.p.LendNode(ctx, "u1", "app", n); err != nil {
				t.Error(err)
			}
			_ = e.p.LentNodes("u1", "app")
			_, _ = e.p.ContainerByPid(101)
		}(n)
	}
	wg.Wait()
	if lent := e.p.LentNodes("u1", "app"); !reflect.DeepEqual(lent, []int{1, 2, 3, 4, 5, 6, 7, 8}) {
		t.Fatalf("lent %v", lent)
	}
	ups := e.stub.taken()
	if len(ups) != 8 || ups[7] != "c1 mems=0-8" {
		t.Fatalf("updates %q", ups)
	}
}
