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

## Lending to running containers

CAUTION: prototype. A pod's claims are fixed when it is created, so a
running container cannot get a pool device through its own claims. It
can *borrow* one instead: the node plugin allocates the device for a
*companion pod* and lends it to the container.

### Opting in

kubelet-cxl-plugin (`--pool-lend`, on by default when the pool driver is
enabled) publishes a token device `dynamic` in the node's `cxl.generic`
ResourceSlice: attributes `type: dynamic`, `name: dynamic`, no capacity,
`allowMultipleAllocations: true`, so any number of pods can claim it.
Install the DeviceClass
[`deployments/cxl/pool/device-class-dynamic.yaml`](../../deployments/cxl/pool/device-class-dynamic.yaml)
(`cxl-dynamic`) and claim it from the pod, see
[`examples/borrower-pod.yaml`](../../deployments/cxl/pool/examples/borrower-pod.yaml).
A DeviceClass that selects every `cxl.generic` device would match the
token too; such classes should add
`device.attributes["cxl.generic"].type != "dynamic"`.

NodePrepare of a claim on the token creates
`<plugin data dir>/leases/<claim uid>/cxl.sock` (mode 0666; the directory
0755, labelled `container_file_t` when SELinux is enabled) and returns,
besides the usual `CXL_CLAIM_<uid>` marker, the CDI device
`generic/cxl=lease-<claim uid>` (spec file `cxl-leases`):

- bind mount (rbind, rw) of the directory at `/run/cxl`,
- bind mounts (ro) of the static CLI (`--cxl-request-bin`, default
  `/usr/local/bin/cxl-request` on the host) at
  `/usr/local/bin/cxl-request` and `/usr/local/bin/cxl-release`; they are
  left out, with a warning, if the file does not exist,
- env `CXL_LEASE_SOCKET=/run/cxl/cxl.sock`.

Only containers that list the claim in `resources.claims` get them. The
directory, not the socket, is mounted, so a restarted plugin can create a
new socket in it that the running containers see.

### Flow

```
container: cxl-request memory 512Mi
  -> plugin: authenticate the caller, store lease L (Creating)
  -> create ResourceClaim cxl-lease-L, then Pod cxl-lease-L   (Pending)
  -> scheduler allocates a pool device, the pool controller attaches it,
     binding conditions release the bind
  -> kubelet prepares cxl-lease-L (pool driver): region, online movable,
     NUMA node N; LendConfig names lease L and the container   (Lending)
  -> NRI UpdateContainers: cpuset.mems of THAT container = its own nodes + N
  -> response {"lease":"L","state":"Ready","node":N,...}        (Ready)
container: cxl-release L
  -> revoke (cpuset.mems shrinks, the kernel migrates the pages back)
  -> delete Pod and ResourceClaim cxl-lease-L                  (Releasing)
  -> kubelet unprepares (offline, destroy region, disable memdev),
     the claim is deallocated and deleted, the controller detaches
  -> lease dropped                                              (Released)
```

A shared device is the same, with class `cxl-shared-memory` and a serial
selector, but nothing is lent through cgroups: the plugin opens
`/dev/daxX.Y` (O_RDWR) and passes the open file descriptor to the
container over the socket (SCM_RIGHTS). The container needs no device
node and no device cgroup rule; the device cgroup is checked only at
open(). `cxl-request shared <serial> --exec CMD` runs CMD with the fd as
fd 3.

The companion objects live in the requester's namespace:

- ResourceClaim `cxl-lease-<id>`: one request `lease`
  (`exactly: {deviceClassName: cxl-pool-memory, capacity.requests.memory: <size>}`
  or `{deviceClassName: cxl-shared-memory, selectors: [serial == ...]}`),
  opaque config for `cxl-pool.generic`:
  `{apiVersion: cxl-pool.generic/v1alpha1, kind: LendConfig, lease, pod: {namespace, name, uid}, container}`,
  ownerReference to the requester pod (controller: true).
- Pod `cxl-lease-<id>`: ownerReference to the requester pod, required
  node affinity `metadata.name In [<node>]`, `tolerations: [{operator: Exists}]`,
  restartPolicy Always, one container `pause` (`--companion-image`,
  default `registry.k8s.io/pause:3.10`) with requests = limits
  cpu 100m, memory 8Mi (Guaranteed; 1m made the pause container take ~12 s to start), no `resources.claims`, and
  `spec.resourceClaims: [{name: lease, resourceClaimName: cxl-lease-<id>}]`.
  kubelet prepares a pod's claims even if no container references them.
- Both carry the labels `cxl-pool.generic/lease=<id>`,
  `cxl-pool.generic/requester-uid=<pod uid>`, `cxl-pool.generic/node=<node>`
  and the annotation `cxl-pool.generic/requester=<ns>/<pod>/<container>`.

The pool driver accepts a LendConfig only from the claim (not from a
class), only for the claim's namespace, and only if the lease exists in
the node plugin's lease table for the same pod and container, so a
tenant cannot lend to another container by writing a LendConfig.

### Protocol

`SOCK_STREAM` unix socket, one JSON object per line in each direction;
a connection may carry several requests.

| request | meaning |
|---|---|
| `{"op":"lease","kind":"memory","size":"512Mi","wait":"120s","requestId":"r1"}` | exclusive memory, lent as a NUMA node |
| `{"op":"lease","kind":"shared","serial":"0xc1ae0001","wait":"120s"}` | shared device, fd passed |
| `{"op":"release","lease":"<id>","wait":"120s"}` | release one lease of the pod |
| `{"op":"release","all":true}` | release all leases of the pod |
| `{"op":"list"}` | leases of the pod |
| `{"op":"fd","lease":"<id>"}` | re-send the fd of a Ready shared lease |

Response: `{"lease":"<8 hex>","state":"...","kind":"memory|shared","node":N,"size":<bytes>,"serial":"0x..","dax":"/dev/daxX.Y","error":"..."}`;
`list` and `release all` answer `{"leases":[...]}`. States: Creating,
Pending, Lending, Ready, Releasing, Failed, and Released (in responses
only: the lease is gone). The response of a Ready shared lease to `lease`
and `fd` carries the dax fd as SCM_RIGHTS on the same write.

- `wait` (default `--lease-wait-default`, 120s) blocks until Ready or
  Failed. At the timeout the lease is released and reported Failed
  `timeout: ...`. `"wait":"0s"` returns the current state at once and
  keeps the lease.
- `requestId`: a repeated request with the same id from the same
  container returns the same lease (a retry after a lost connection).
- `size` in a response is the device size once Ready (it may be larger
  than requested), the requested size before.
- A release waits up to `wait` for the companion objects to disappear and
  answers Released, or Releasing if they are still going.
- Errors: `{"error":"lending unavailable: NRI not connected"}`,
  `{"error":"not a container of the claim's pod"}`, invalid requests.
  A lend that fails (for example the container is memory-steered, see
  below, or not running) makes the lease Failed with that message, and
  it is released.

Caller authentication: the socket identifies the token claim, and the
claim's `reservedFor` the pod. SO_PEERCRED gives the caller's pid; the
NRI plugin maps it through `/proc/<pid>/cgroup` to a container. The
container's pod must be a pod of the claim. A lease is bound to that pod
and container: memory is lent to the calling container only; any
container of the same pod with the socket may list and release it.

### CLI

`cxl-request` is a static binary (`CGO_ENABLED=0 go build ./cmd/cxl-request`);
it uses `$CXL_LEASE_SOCKET` and prints the JSON response line.

```
cxl-request memory <size> [--wait D] [--request-id ID]
cxl-request shared <serial> [--wait D] [--exec CMD ARGS...]
cxl-request fd <lease> --exec CMD ARGS...
cxl-release <lease>|--all [--wait D]     (also: cxl-request release ...)
cxl-request list
```

With `--exec`, CMD runs with the dax fd as fd 3 and `CXL_LEASE_FD=3`,
`CXL_LEASE`, `CXL_LEASE_DAX` (the host path, informational) and
`CXL_LEASE_SIZE`; the exit code of CMD is passed through, signals are
forwarded. Without `--exec` the received fd is closed. Exit codes: 0 ok,
1 error, 2 usage, 3 lease Failed or not Ready in time (or not Released).

### Life cycle

- The lease table `leases.json` in the `cxl.generic` plugin data
  directory is written before any object is created. The pool driver
  stores the lend with the companion claim in `preparedPoolClaims.json`,
  so it can revoke after a restart.
- Requester container stops: if the pod is being deleted, finished, gone
  or has restartPolicy Never, its leases are released. Otherwise they
  are kept, and exclusive lends are re-applied when the container starts
  again (a shared fd is gone with the process: request it again with
  `cxl-request fd <lease> --exec ...`).
- Requester pod deleted: the companion pods are garbage collected
  (ownerReference); NodeUnprepare of the token claim releases all leases
  of the pod first, and fails with "still releasing N leases" (kubelet
  retries) if that takes longer than 30 s.
- Companion pod deleted or evicted behind the plugin's back: the pool
  driver revokes before it releases the device; the lease becomes Failed
  and is dropped by the reconcile.
- Reconcile, at start (after NRI synchronized) and every 60 s: restarts
  interrupted releases, releases leases left in Creating, leases whose
  companion pod is gone or finished and leases whose requester will not
  come back, re-applies exclusive lends of Ready leases (this is how a
  plugin restart restores them), and deletes companion pods and claims of
  the node that have no lease.
- Plugin restart: the token claims' sockets are re-created in the
  mounted directories, leases continue.

### Limitations

- Memory limits: lent memory counts against the container's `memory.max`
  and the pod cgroup. Without a raise the extra node adds locality, not
  room. Use requester pods without a memory limit, or raise it with
  `pods/resize`. Node allocatable never includes pool memory.
- cpuset.mems has one owner: the lend goes through NRI UpdateContainers.
  A container that has a local CXL claim (memory steered by cgmpolmgr)
  cannot borrow: the lend is refused with "container is memory-steered by
  a local CXL claim; lending is not supported together with steering
  yet". Both components would own the container's cpuset.mems, and
  cgmpolmgr writes cgroupfs directly, which the next NRI update reverts.
  nri-resource-policy or the kubelet memory manager would conflict the
  same way. A movable pool node is never the only node in cpuset.mems.
- Release is cooperative. Releasing a shared lease while the device is
  still mapped makes the holder get SIGBUS at its next access; munmap
  first. A passed fd cannot be revoked short of release, and can be
  duplicated or passed on inside the pod. An exclusive release blocks
  (kubelet retries, the companion stays Terminating) while the pages
  cannot move.
- Latency: about 5-7 s per request (the scheduler's binding condition
  poll), about 8 s for a release.
- `cxl create-region` right after a hotplug can fail once with
  "add_dev() failed"; the pool driver retries a region creation once
  after 3 s.
- RBAC: the plugin creates and deletes pods and resourceclaims in every
  namespace (`deployments/cxl/base/resource-driver.yaml`); companions count
  against the tenant's ResourceQuota. A ValidatingAdmissionPolicy that
  pins the companion shape and node is future work.
- The plugin must run in the host pid namespace with the NRI socket (as a
  host process; the shipped DaemonSet has neither). SELinux labels of the
  socket are set best effort and unverified.
- Leases follow the pod object: a finished but undeleted pod keeps its
  leases until its token claim is unprepared.
- Do not force-delete a companion pod while it holds a device (the
  force deletion hazard above).
