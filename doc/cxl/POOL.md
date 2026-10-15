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
      type:   {string: memdev}
      source: {string: pool}
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
      type:   {string: memdev}
      source: {string: pool}
      serial: {string: "0xc1ae0001"}
      shared: {bool: true}
      size:   {int: 268435456}
    allowMultipleAllocations: true
    capacity:
      hosts: {value: "4", requestPolicy: {default: "1", validValues: ["1", "4"]}}  # max attached nodes
    bindsToNode: true
    bindingConditions: ["cxl-pool.generic/Attached"]
    bindingFailureConditions: ["cxl-pool.generic/AttachFailed"]
```

| attribute | value | |
|:----------|:------|:-|
| `type` | `memdev` | required |
| `source` | `pool` | required |
| `serial` | `0x` and lower-case hex | required: `/sys/bus/cxl/devices/memN/serial` of the device on the hosts |
| `shared` | bool | required: whether several hosts may have the device attached at a time |
| `size` | bytes | required |

These are the attributes that `cxl.generic` publishes for local memdevs
too ([USAGE.md](USAGE.md#devices-of-cxlgeneric)), so that selectors and
environment variables are the same for both sources. The DeviceClasses
select pool devices by `type` and `shared`, the lease broker finds a
device by `serial` and reads its `shared` and `hosts`, and `cxl.generic`
never publishes a memdev whose serial a pool device has. A controller may
publish further attributes under a domain of its own (for example
`pool.example.com/model`); the reference controller publishes none.

- *Exclusive* devices (`shared: false`) are attached to one node at a
  time. Their size is the `memory` capacity, so claims can request an
  amount.
- *Shared* devices (`shared: true`) can be attached to several nodes at
  once; all of them see the same bytes. Never put the size of a shared
  device into `capacity`: every allocation would consume it. The size is
  an attribute; `hosts` limits how many nodes may attach it.
- `bindsToNode` makes the scheduler pin the claim to the node it picked.
  One claim covers one node, so N nodes sharing a device means N claims.
- A claim of a shared device consumes one of its `hosts` by default. A
  claim that requests all of them (`validValues` has the value of `hosts`)
  has the device exclusively: the scheduler allocates the device to no
  other claim until it is deallocated, and the controller attaches it
  exclusively, so that no other cluster sharing the pool gets it either
  (the reference controller makes an exclusive attachment of
  fake-cxl-pool). That is the writer of single-writer data sharing, see
  [Options of pool devices](#options-of-pool-devices).

## DeviceClasses and claims

Two classes of
[deployments/cxl/device-classes.yaml](../../deployments/cxl/device-classes.yaml)
select pool devices:

| class | selects | gets |
|:------|:--------|:-----|
| `cxl-pool` | `type == "memdev"`, `shared == false` | a whole exclusive pool device: a new NUMA node (default) or a devdax device (`mode: dax`) |
| `cxl-pool-shared` | `type == "memdev"`, `shared == true` | a shared pool device as devdax; one of its `hosts` by default, all of them with `capacity.requests.hosts` |

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
        deviceClassName: cxl-pool
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
        deviceClassName: cxl-pool-shared
        selectors:
        - cel: {expression: 'device.attributes["cxl-pool.generic"].serial == "0xc1ae0001"'}
```

Complete examples with pods:
[pool-numa.yaml](../../deployments/cxl/examples/pool-numa.yaml),
[pool-dax.yaml](../../deployments/cxl/examples/pool-dax.yaml),
[pool-shared-writer-readers.yaml](../../deployments/cxl/examples/pool-shared-writer-readers.yaml),
[local-then-pool-dax.yaml](../../deployments/cxl/examples/local-then-pool-dax.yaml)
(local memory if a local memdev has room, else pool memory).

### Options of pool devices

An opaque configuration of driver `cxl-pool.generic`, kind `DeviceConfig`
(the same fields as for `cxl.generic`, see
[USAGE.md](USAGE.md#deviceconfig)), selects how the node plugin prepares
a pool device. It may come from the claim or from its DeviceClass (the
claim's wins), and applies to the requests it lists, or to all requests
of the driver:

```yaml
spec:
  devices:
    requests: [...]
    config:
    - opaque:
        driver: cxl-pool.generic
        parameters: {apiVersion: cxl-pool.generic/v1alpha1, kind: DeviceConfig, mode: dax, readOnly: true}
```

An opaque configuration of `cxl-pool.generic` of another kind fails the
Prepare with `unknown kind "..." (DeviceConfig)`.

| field | values | |
|---|---|---|
| `mode` | `numa`, `dax` | `numa`: the memory becomes a NUMA node (exclusive devices only, their default). `dax`: a devdax device `/dev/daxX.Y` that containers `mmap` (the default and only mode of shared devices). |
| `readOnly` | `false`, `true` | `dax` only: the container gets the device node with CDI permissions `r`, so its device cgroup lets it open the device for reading only. |

So every pool device can be used as a dax device: shared devices always,
exclusive devices with `mode: dax`. Exclusive access to a shared device is
a capacity request of all its `hosts`:

| pool device | claim | gets |
|---|---|---|
| exclusive | (nothing) | a NUMA node |
| exclusive | `mode: dax` | a dax device of its own |
| shared | (nothing) | a dax device, shared with the other claims of the device |
| shared | `capacity.requests.hosts: "4"` | a dax device, nobody else has the device meanwhile |
| shared or exclusive dax | `readOnly: true` | the dax device, read-only |

Read-only is enforced by the kernel for every process of the container,
whatever its capabilities, except in privileged containers (their device
cgroup allows everything). It is a property of the claim, not of the
device: another claim of the same device may write, and the readers see
it. It keeps readers from writing by mistake; consistency of the data is
the job of exclusive access. Pool appliances have no read-only attach
either (UnifabriX Maxview 2.0 has none): whatever a host attaches, it can
write.

Single-writer data sharing: the writer claims the shared device with all
of its `hosts` and writes; the readers claim it with `readOnly: true`. A
reader that asks while the writer has the device waits (its pod or lease
stays pending) until the writer's claim is gone.

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
      # anything else is informational; the reference controller adds:
      host: node1
      hostUUID: <the host's uuid>
      attachment: shared0@node1
      qemuDeviceId: <qemu device id>
      exclusive: true            # only for an exclusive attachment of a shared device
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

NodePrepareResources checks first that every opaque configuration of
the driver in the claim and its classes is a `DeviceConfig`
(`unknown kind "..." (DeviceConfig)` otherwise), then, for each allocated
device of the driver:

1. Read `serial`, `shared` and `size` from the claim status entry. A
   missing entry or missing data is an error; kubelet retries.
2. Read the `DeviceConfig` of the request; an invalid one fails the
   Prepare ([USAGE.md](USAGE.md#deviceconfig)).
3. Wait until a CXL memory device with the serial appears
   (`/sys/bus/cxl/devices/memN/serial`).
4. Mode `numa` (exclusive devices): create a ram region on it under the
   root decoder of its host bridge, bind the dax device to `kmem`, online
   all memory blocks `online_movable` (movable, so that the memory can be
   removed again). The memory becomes a new NUMA node N.

   The pool driver does not steer the container: N is not added to its
   memory policy or its `cpuset.mems` (a container whose cgroup allows
   only ZONE_MOVABLE memory cannot start), and other containers whose
   cpusets allow every node may allocate from N too. Workloads use the
   node number from the environment, for example with `numactl` or
   `mbind` ([USAGE.md](USAGE.md#steering)).

   Mode `dax` (shared devices, and exclusive devices with `mode: dax`):
   create a ram region, bind the dax device to `device_dax`. The memory
   is never onlined: two kernels using the same bytes as RAM would corrupt
   each other. The container gets the device node `/dev/daxX.Y`, with
   permissions `r` for a read-only claim. Applications `mmap` the device
   (`MAP_SHARED`, aligned to the device alignment, typically 2 MiB) and
   agree on the layout of the memory themselves.
5. Return the device's CDI device `generic/cxl-pool=claim-<claim uid>-<n>`
   (spec file `cxl-pool`, n the index of the allocation result in the
   claim) with the claim marker `CXL_CLAIM_<uid>=1`, the device node of a
   `dax` device and the variables of
   [USAGE.md](USAGE.md#environment-variables):

   ```
   CXL_<KEY>_SERIAL=0x<serial>
   CXL_<KEY>_SHARED=0|1
   CXL_<KEY>_MODE=numa|dax
   CXL_<KEY>_SIZE=<bytes>
   CXL_<KEY>_NODE=<N>                 (numa)
   CXL_<KEY>_DAX=/dev/daxX.Y          (dax)
   CXL_<KEY>_READONLY=0|1             (dax)
   ```

   `<KEY>` is the serial in upper-case hex without `0x` (`<KEY>_2`, ...
   for a second device with the same serial in the claim), `<serial>` in
   lower-case hex. A container gets the devices of the requests it
   references only. The marker does not make the container steered: the
   NRI plugin has no steering information for pool claims. A companion
   claim of a lease gets no CDI device ([LEASES.md](LEASES.md)). The
   prepared claim is stored in
   `/var/lib/kubelet/plugins/cxl-pool.generic/preparedPoolClaims.json`.
   A repeated prepare of a prepared claim returns the stored result; a
   region that already exists on the device (another claim on this node
   for the same shared device, or a restart) is reused. A claim that wants
   a device in another mode than another claim on the node prepared it
   (or as exclusive when it is shared) fails. If preparing one
   device of a claim fails, the devices prepared for it are released
   again.

NodeUnprepareResources: when no other prepared claim on the node uses the
device, offline its memory (mode `numa`), disable and destroy its region
and disable the memory device. This is what lets the pool hot-remove it.
A device that is already gone is not an error.

The `cxl.generic` driver publishes neither a memory device that the pool
driver prepared or whose serial a pool controller publishes, nor a region
on such a device, so pool memory is not offered twice
([LOCAL.md](LOCAL.md#what-the-plugin-publishes)).

## Leases

A running container can lease a pool device without a claim of its own:
`cxl-request request --source pool --size 512Mi` in a container with a
claim on `cxl-lease` makes the node plugin allocate the device for a
companion pod and lend it to the container, as a NUMA node or as an open
dax file descriptor. See [LEASES.md](LEASES.md).

## Node requirements

- `cxl` (ndctl) and `daxctl` command line tools. The driver container
  image does not include them, so the pool driver needs
  kubelet-cxl-plugin running as a host process (for example a systemd
  unit, see [USAGE.md](USAGE.md#as-a-host-process)).
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
- `cxl create-region` right after a hotplug can fail once with
  "add_dev() failed"; the pool driver retries a region creation once
  after 3 s.
