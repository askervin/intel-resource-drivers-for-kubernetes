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
	"net"
	"os"
	"path/filepath"
	"sort"
	"time"

	"golang.org/x/sys/unix"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/klog/v2"
	cdiapi "tags.cncf.io/container-device-interface/pkg/cdi"
	cdiparser "tags.cncf.io/container-device-interface/pkg/parser"
	cdiSpecs "tags.cncf.io/container-device-interface/specs-go"

	"github.com/intel/intel-resource-drivers-for-kubernetes/pkg/cxl/device"
)

// The token device "dynamic" of the cxl.generic slice: a claim on it
// gives the containers that reference the claim a lease socket. Prepare
// creates <data dir>/leases/<claim uid>/cxl.sock and returns the CDI
// device generic/cxl=lease-<claim uid>, which bind-mounts that
// directory at /run/cxl and the cxl-request CLI at /usr/local/bin.
// Requests on the socket are served by the broker (broker.go).

const (
	// leaseDirName is the directory of the per-claim socket directories
	// in the cxl.generic plugin data directory.
	leaseDirName = "leases"
	// leaseSocketName is the socket in a claim's directory.
	leaseSocketName = "cxl.sock"
	// leaseContainerDir is where the claim's directory is mounted.
	leaseContainerDir = "/run/cxl"
	// leaseSocketEnv names the socket inside the container.
	leaseSocketEnv = "CXL_LEASE_SOCKET"
	// cxlRequestContainerPath and cxlReleaseContainerPath are where the
	// CLI is mounted (the same binary; it acts on its argv[0]).
	cxlRequestContainerPath = "/usr/local/bin/cxl-request"
	cxlReleaseContainerPath = "/usr/local/bin/cxl-release"

	defaultCXLRequestBin  = "/usr/local/bin/cxl-request"
	defaultLeaseWait      = 120 * time.Second
	defaultCompanionImage = "registry.k8s.io/pause:3.10"

	// selinuxContainerFileLabel lets container processes use the socket
	// directory on SELinux hosts (best effort, ignored elsewhere).
	selinuxContainerFileLabel = "system_u:object_r:container_file_t:s0"
)

// tokenClaims is what nodeState needs from the broker for claims on the
// token device.
type tokenClaims interface {
	// OpenClaim records the claim and starts its socket. Idempotent.
	OpenClaim(claim *resourceapi.ResourceClaim) error
	// CloseClaim releases all leases of the claim, then stops the
	// socket and removes the directory. Returns an error while leases
	// are still being released (kubelet retries Unprepare). Unknown
	// claims are not an error.
	CloseClaim(ctx context.Context, uid string) error
}

var _ tokenClaims = (*broker)(nil)

// leaseCDIDeviceName is the CDI device name of a token claim.
func leaseCDIDeviceName(claimUID string) string {
	return "lease-" + claimUID
}

func leaseCDIDeviceID(claimUID string) string {
	return cdiparser.QualifiedName(device.CDIVendor, device.CDIClass, leaseCDIDeviceName(claimUID))
}

// leaseDir is the host directory of a token claim.
func (b *broker) leaseDir(claimUID string) string {
	return filepath.Join(b.cfg.DataDir, leaseDirName, claimUID)
}

// leaseCDIDevice returns the CDI device of a token claim.
func (b *broker) leaseCDIDevice(claimUID string) cdiSpecs.Device {
	edits := cdiSpecs.ContainerEdits{
		Env: []string{leaseSocketEnv + "=" + leaseContainerDir + "/" + leaseSocketName},
		Mounts: []*cdiSpecs.Mount{{
			HostPath:      b.leaseDir(claimUID),
			ContainerPath: leaseContainerDir,
			Type:          "bind",
			Options:       []string{"rbind", "rw"},
		}},
	}
	// A missing bind mount source fails the container creation, so the
	// CLI is mounted only if it exists. The socket works without it.
	if b.cfg.RequestBin != "" {
		if _, err := os.Stat(b.cfg.RequestBin); err == nil {
			for _, p := range []string{cxlRequestContainerPath, cxlReleaseContainerPath} {
				edits.Mounts = append(edits.Mounts, &cdiSpecs.Mount{
					HostPath:      b.cfg.RequestBin,
					ContainerPath: p,
					Type:          "bind",
					Options:       []string{"bind", "ro"},
				})
			}
		} else {
			klog.Warningf("lease broker: cxl-request CLI %s not mounted into containers: %v", b.cfg.RequestBin, err)
		}
	}
	return cdiSpecs.Device{Name: leaseCDIDeviceName(claimUID), ContainerEdits: edits}
}

// writeCDISpec writes the CDI spec of all token claims, or removes it
// when there are none. Called with b.mu held.
func (b *broker) writeCDISpec() error {
	if b.cfg.CDI == nil {
		return nil
	}
	spec := &cdiSpecs.Spec{Kind: device.CDIKind}
	uids := make([]string, 0, len(b.store.Claims))
	for uid := range b.store.Claims {
		uids = append(uids, uid)
	}
	sort.Strings(uids)
	for _, uid := range uids {
		spec.Devices = append(spec.Devices, b.leaseCDIDevice(uid))
	}
	if len(spec.Devices) == 0 {
		if err := b.cfg.CDI.RemoveSpec(device.LeaseCDISpecName); err != nil {
			klog.V(5).Infof("lease broker: could not remove CDI spec %s (may not exist): %v", device.LeaseCDISpecName, err)
		}
		return nil
	}
	v, err := cdiapi.MinimumRequiredVersion(spec)
	if err != nil {
		return fmt.Errorf("CDI spec version: %w", err)
	}
	spec.Version = v
	if err := b.cfg.CDI.WriteSpec(spec, device.LeaseCDISpecName); err != nil {
		return fmt.Errorf("failed to write CDI spec %s: %w", device.LeaseCDISpecName, err)
	}
	return nil
}

// OpenClaim implements tokenClaims.
func (b *broker) OpenClaim(claim *resourceapi.ResourceClaim) error {
	uid := string(claim.UID)
	tc := &tokenClaim{UID: uid, Namespace: claim.Namespace, Name: claim.Name, Pods: reservedPods(claim)}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.store.Claims[uid] = tc
	if err := b.store.save(); err != nil {
		return err
	}
	if err := b.writeCDISpec(); err != nil {
		return err
	}
	if err := b.listen(uid); err != nil {
		return err
	}
	klog.Infof("lease broker: claim %s/%s (%s): lease socket %s for pods %v",
		claim.Namespace, claim.Name, uid, filepath.Join(b.leaseDir(uid), leaseSocketName), tc.Pods)
	return nil
}

// reservedPods returns the pods in the claim's reservedFor.
func reservedPods(claim *resourceapi.ResourceClaim) []podRef {
	var pods []podRef
	for _, c := range claim.Status.ReservedFor {
		if c.Resource == "pods" && c.APIGroup == "" {
			pods = append(pods, podRef{Namespace: claim.Namespace, Name: c.Name, UID: string(c.UID)})
		}
	}
	return pods
}

// CloseClaim implements tokenClaims.
func (b *broker) CloseClaim(ctx context.Context, uid string) error {
	b.mu.Lock()
	tc := b.store.Claims[uid]
	var ids []string
	if tc != nil {
		for _, l := range b.store.leasesOf(uid) {
			ids = append(ids, l.ID)
		}
	}
	b.mu.Unlock()
	if tc == nil {
		return nil
	}
	for _, id := range ids {
		b.startRelease(id, "token claim unprepared (pod done)")
	}
	ctx, cancel := context.WithTimeout(ctx, b.closeTimeout)
	defer cancel()
	remaining := func() int {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.store.leasesOf(uid))
	}
	if err := b.waitFor(ctx, func() bool { return remaining() == 0 }); err != nil {
		return fmt.Errorf("claim %s/%s: still releasing %d leases, retry", tc.Namespace, tc.Name, remaining())
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if l := b.listeners[uid]; l != nil {
		_ = l.Close()
		delete(b.listeners, uid)
	}
	delete(b.store.Claims, uid)
	errs := []error{b.store.save(), b.writeCDISpec()}
	if err := os.RemoveAll(b.leaseDir(uid)); err != nil {
		errs = append(errs, err)
	}
	klog.Infof("lease broker: claim %s/%s (%s): lease socket closed", tc.Namespace, tc.Name, uid)
	return errors.Join(errs...)
}

// listen starts the socket of a token claim if it is not running.
// Called with b.mu held.
func (b *broker) listen(uid string) error {
	if b.listeners[uid] != nil {
		return nil
	}
	dir := b.leaseDir(uid)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0755); err != nil {
		return err
	}
	sock := filepath.Join(dir, leaseSocketName)
	_ = os.Remove(sock)
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: sock, Net: "unix"})
	if err != nil {
		return fmt.Errorf("listening on %s: %w", sock, err)
	}
	// The socket is removed explicitly (CloseClaim): a plugin that
	// stops must leave the path to the next instance.
	l.SetUnlinkOnClose(false)
	if err := os.Chmod(sock, 0666); err != nil {
		_ = l.Close()
		return err
	}
	selinuxLabel(dir, sock)
	b.listeners[uid] = l
	b.wg.Add(1)
	go b.accept(uid, l)
	return nil
}

// selinuxLabel labels the socket directory and socket for containers
// when SELinux is enabled. Errors are logged only.
func selinuxLabel(paths ...string) {
	if _, err := os.Stat("/sys/fs/selinux/enforce"); err != nil {
		return
	}
	for _, p := range paths {
		if err := unix.Lsetxattr(p, "security.selinux", []byte(selinuxContainerFileLabel), 0); err != nil {
			klog.Warningf("lease broker: cannot label %s %s: %v", p, selinuxContainerFileLabel, err)
		}
	}
}
