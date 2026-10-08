# CXL memory pools: the cxl-pool.generic driver

CAUTION: experimental. Interfaces may change.

A CXL memory pool (a CXL switch with memory devices behind it, managed by
a fabric manager) can hot-add a memory device to a host and hot-remove it
again. With Dynamic Resource Allocation, such devices can be allocated to
pods like any other device: the scheduler picks a device and a node, a
*pool controller* attaches the device to that node, and the node plugin
makes the memory usable for the pod.

This document is the contract between the two sides:

- a **pool controller** (cluster level, one per pool): publishes the pool
  devices, attaches and detaches them, and reports the result in the
  claim status. It talks to the pool's management interface, so it is
  specific to the pool product. This repository does not include one.
- **kubelet-cxl-plugin** (node level, this repository): next to its
  `cxl.generic` driver for node-local CXL memory, it registers a second
  DRA driver, `cxl-pool.generic`, that prepares hotplugged pool devices.
  It knows nothing about the pool: it only needs the device serial that
  the controller writes into the claim status.

A reference controller, `fake-cxl-pool-controller`, drives a qemu based
fake pool in
[nri-plugins/scripts/testing/fake-cxl-pool](https://github.com/containers/nri-plugins/tree/main/scripts/testing/fake-cxl-pool).

## Overview

```
 pool controller
   publishes a ResourceSlice of pool devices (driver cxl-pool.generic),
   attaches a device to the node the scheduler picked,
   writes claim.status.devices[] (condition Attached=True + data),
   detaches the device after the claim is deallocated.
 kube-scheduler
   allocates a pool device, pins the claim to a node (bindsToNode),
   waits for the binding condition, binds the pod.
 kubelet-cxl-plugin, driver cxl-pool.generic
   waits for the memory device with the serial from the claim status,
   creates a region on it and returns CDI edits;
   on unprepare, releases the device so that the pool can hot-remove it.
```

Kubernetes v1.37 or later. Device binding conditions
(`DRADeviceBindingConditions`), device status (`DRAResourceClaimDeviceStatus`)
and consumable capacity (`DRAConsumableCapacity`) are used; they are on by
default in v1.37. Pool devices do not use `nodeAllocatableResources`.

## What the pool controller publishes

One cluster-scoped ResourceSlice (no node name) under driver
`cxl-pool.generic`:

```yaml
apiVersion: resource.k8s.io/v1
kind: ResourceSlice
spec:
  driver: cxl-pool.generic
  pool: {name: fake-cxl-pool, generation: N, resourceSliceCount: 1}
  allNodes: true                 # or a nodeSelector of the nodes the pool can reach
  devices:
  - name: pooled0                # device name, a DNS label
    attributes:
      source: {string: pool}
      pool:   {string: default}            # pool-side pool name
      serial: {string: "0xc1ee0001"}       # /sys/bus/cxl/devices/memN/serial, lower-case hex
      shared: {bool: false}
      size:   {int: 536870912}             # bytes
    capacity:
      memory: {value: 512Mi}               # exclusive devices only
    bindsToNode: true
    bindingConditions: ["cxl-pool.generic/Attached"]
    bindingFailureConditions: ["cxl-pool.generic/AttachFailed"]
  - name: shared0
    attributes:
      source: {string: pool}
      pool:   {string: default}
      serial: {string: "0xc1ae0001"}
      shared: {bool: true}
      size:   {int: 268435456}
    allowMultipleAllocations: true
    capacity:
      hosts: {value: "4", requestPolicy: {default: "1", validValues: ["1"]}}  # max attached nodes
    bindsToNode: true
    bindingConditions: ["cxl-pool.generic/Attached"]
    bindingFailureConditions: ["cxl-pool.generic/AttachFailed"]
```

- *Exclusive* devices (`shared: false`) are attached to one node at a
  time and become system RAM there. Their size is the `memory` capacity,
  so claims can request an amount.
- *Shared* devices (`shared: true`) can be attached to several nodes at
  once; all of them see the same bytes. Never put the size of a shared
  device into `capacity`: every allocation would consume it. The size is
  an attribute; `hosts` limits how many nodes may attach it.
- `bindsToNode` makes the scheduler pin the claim to the node it picked.
  One claim covers one node, so N nodes sharing a device means N claims.

## DeviceClasses and claims

[deployments/cxl/pool/device-classes.yaml](../../deployments/cxl/pool/device-classes.yaml):

```yaml
apiVersion: resource.k8s.io/v1
kind: DeviceClass
metadata: {name: cxl-pool-memory}
spec:
  selectors:
  - cel: {expression: 'device.driver == "cxl-pool.generic" && device.attributes["cxl-pool.generic"].shared == false'}
---
apiVersion: resource.k8s.io/v1
kind: DeviceClass
metadata: {name: cxl-shared-memory}
spec:
  selectors:
  - cel: {expression: 'device.driver == "cxl-pool.generic" && device.attributes["cxl-pool.generic"].shared == true'}
```

Claims:

```yaml
apiVersion: resource.k8s.io/v1
kind: ResourceClaim
metadata: {name: pooled-memory}
spec:
  devices:
    requests:
    - name: mem
      exactly:
        deviceClassName: cxl-pool-memory
        capacity: {requests: {memory: 512Mi}}     # a device with at least this much
---
apiVersion: resource.k8s.io/v1
kind: ResourceClaim
metadata: {name: shared-memory}
spec:
  devices:
    requests:
    - name: mem
      exactly:
        deviceClassName: cxl-shared-memory
        selectors:
        - cel: {expression: 'device.attributes["cxl-pool.generic"].serial == "0xc1ae0001"'}
```

Complete examples with pods:
[pooled-memory-pod.yaml](../../deployments/cxl/pool/examples/pooled-memory-pod.yaml),
[shared-memory-pod.yaml](../../deployments/cxl/pool/examples/shared-memory-pod.yaml).

## What the scheduler does

Nothing to configure. The scheduler tries pools without binding
conditions first, so node-local `cxl.generic` devices win when they
satisfy a request. It writes `status.allocation` (results with driver,
pool, device and the binding conditions; `shareID` and `consumedCapacity`
for shared devices) with `nodeSelector` = the picked node, emits the pod
event `BindingConditionsPending` and checks the claim every few seconds
until the binding timeout (default 600 s). If a failure condition becomes
True or the timeout expires, the claim is deallocated and allocated again.

## What the pool controller writes

After the pool has attached the device to the node, the controller adds
an entry to `claim.status.devices` (`UpdateStatus` on the `/status`
subresource):

```yaml
status:
  devices:
  - driver: cxl-pool.generic
    pool: fake-cxl-pool
    device: shared0
    shareID: <from the allocation result, shared devices only>
    conditions:
    - type: cxl-pool.generic/Attached
      status: "True"
      reason: Attached
      message: "shared0 attached to node1"
      lastTransitionTime: <now>
    data:
      serial: "0xc1ae0001"       # REQUIRED: string, 0x hex or decimal
      shared: true               # REQUIRED
      size: 268435456            # REQUIRED, bytes
      # anything else is informational, for example:
      host: node1
      attachment: shared0@node1
```

The node plugin matches the entry by (driver, pool, device) and reads
only `serial`, `shared` and `size` from `data`.

On failure the controller sets `cxl-pool.generic/AttachFailed` to True
instead (reasons used by the reference controller: `NoHost`,
`AttachError`, `Timeout`, `Conflict`) and detaches anything that was half
attached. The controller detaches a device only after the claim has been
deallocated.

RBAC of the controller:

```yaml
rules:
- {apiGroups: [resource.k8s.io], resources: [resourceslices], verbs: [get, list, watch, create, update, patch, delete]}
- {apiGroups: [resource.k8s.io], resources: [resourceclaims], verbs: [get, list, watch]}
- {apiGroups: [resource.k8s.io], resources: [resourceclaims/status], verbs: [get, patch, update]}
- {apiGroups: [resource.k8s.io], resources: [resourceclaims/driver], verbs: ["arbitrary-node:patch", "arbitrary-node:update"], resourceNames: ["cxl-pool.generic"]}
- {apiGroups: [""], resources: [nodes], verbs: [get, list, watch]}
- {apiGroups: [""], resources: [events], verbs: [create, patch]}
```

## What the node plugin does

kubelet-cxl-plugin registers the `cxl-pool.generic` driver with kubelet
(socket `/var/lib/kubelet/plugins/cxl-pool.generic/dra.sock`). It never
publishes ResourceSlices.

| Flag | Environment | Default | |
|:-----|:------------|:--------|:-|
| `--pool-driver-name` | `POOL_DRIVER_NAME` | `cxl-pool.generic` | empty disables the pool driver |
| `--pool-prepare-timeout` | `POOL_PREPARE_TIMEOUT` | `30s` | how long to wait for the memory device to appear |

NodePrepareResources, for each allocated device of the driver:

1. Read `serial`, `shared` and `size` from the claim status entry. A
   missing entry or missing data is an error; kubelet retries.
2. Wait until a CXL memory device with the serial appears
   (`/sys/bus/cxl/devices/memN/serial`).
3. Exclusive device: create a ram region on it under the root decoder of
   its host bridge, bind the dax device to `kmem`, online all memory
   blocks `online_movable` (movable, so that the memory can be removed
   again). The memory becomes a new NUMA node N. The container gets the
   CDI device `generic/cxl=pool-<serial>` with the environment

   ```
   CXL_POOL_NODE_<SERIAL>=N
   CXL_POOL_SIZE_<SERIAL>=<bytes>
   CXL_POOL_SERIAL_<SERIAL>=0x<serial>
   ```

   The memory is not added to the container's memory policy: a container
   whose cgroup allows only ZONE_MOVABLE memory cannot start. Workloads
   use the node number, for example with `numactl` or `mbind`.

   ??? TODO: make sure the container is allowed to use the memory node
   (update cpuset.mems), and other containers are not.

   Shared device: create a ram region, bind the dax device to
   `device_dax`. The memory is never onlined: two kernels using the same
   bytes as RAM would corrupt each other. The container gets the device
   node `/dev/daxX.Y` and

   ```
   CXL_SHARED_DAX_<SERIAL>=/dev/daxX.Y
   CXL_SHARED_SIZE_<SERIAL>=<bytes>
   CXL_SHARED_SERIAL_<SERIAL>=0x<serial>
   ```

   Applications `mmap` the device (`MAP_SHARED`, aligned to the device
   alignment, typically 2 MiB) and agree on the layout of the memory
   themselves.

   `<SERIAL>` is the serial in upper-case hex without `0x`, `<serial>` in
   lower-case hex.
4. Return the CDI device and remember the claim in
   `/var/lib/kubelet/plugins/cxl-pool.generic/preparedPoolClaims.json`.
   A repeated prepare of a prepared claim returns the stored result; a
   region that already exists on the device (another claim on this node
   for the same shared device, or a restart) is reused. If preparing one
   device of a claim fails, the devices prepared for it are released
   again.

NodeUnprepareResources: when no other prepared claim on the node uses the
device, offline its memory (exclusive), disable and destroy its region
and disable the memory device. This is what lets the pool hot-remove it.
A device that is already gone is not an error.

The `cxl.generic` driver never publishes a region on a memory device that
the pool driver prepared, so pool memory is not offered twice.

## Node requirements

- `cxl` (ndctl) and `daxctl` command line tools. The driver container
  image does not include them yet, so the pool driver currently needs
  kubelet-cxl-plugin running as a host process (for example a systemd
  unit); a sysfs-only implementation is a planned improvement.
- Kernel with CXL regions (`CONFIG_CXL_REGION`), `CONFIG_DEV_DAX`,
  `CONFIG_DEV_DAX_KMEM`, `CONFIG_FS_DAX` (devdax mmap) and
  `CONFIG_MEMORY_HOTREMOVE`.
- `/sys/devices/system/memory/auto_online_blocks` should be `offline`,
  so that the kernel never onlines shared devices as RAM on its own.

## Ordering and failure semantics

- Attach before bind: the controller sets Attached only after the pool
  confirmed the attachment. NodePrepare then normally finds the memory
  device already present, or waits for it briefly.
- Release before detach: kubelet calls NodeUnprepare while it terminates
  the pod, before the pod object is gone; the claim is deallocated only
  after that, and the controller detaches only deallocated devices.
- **Force deletion hazard**: `kubectl delete pod --force --grace-period=0`
  removes the pod object at once. The claim may then be deallocated and
  the device detached while the node still uses the memory. The host
  cannot complete the hot-remove; the device stays half removed until the
  host reboots. This is not handled; do not force-delete pods that use
  pool memory.
- A second exclusive claim while all devices are taken stays pending in
  the scheduler (no device); it is not a failure.
- Controller restart: the controller recomputes the attachments from the
  claims and the pool, and re-confirms existing ones.
- Node plugin restart: the prepared claims file restores the prepared
  devices; regions are found again by serial.
