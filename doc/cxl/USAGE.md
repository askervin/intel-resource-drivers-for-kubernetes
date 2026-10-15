# How to deploy and use the Intel CXL resource driver

CAUTION: experimental. Interfaces may change.

`kubelet-cxl-plugin` runs two DRA drivers on every node: `cxl.generic` for the memory of the
node (local CXL memory devices, CXL regions, DRAM, the lease device) and `cxl-pool.generic` for
the devices of a CXL memory pool. The words used below (`source`, `type`, `shared`, `mode`,
`readOnly`, `serial`, `size`, `hosts`) are defined in [README.md](README.md#vocabulary).

- [Requirements](#requirements)
- [Deploy](#deploy)
- [Devices of cxl.generic](#devices-of-cxlgeneric)
- [DeviceClasses](#deviceclasses)
- [Opaque configurations](#opaque-configurations)
- [Steering](#steering)
- [Environment variables](#environment-variables)
- [What the memory contains](#what-the-memory-contains)
- [Example claims](#example-claims)
- [kubelet-cxl-plugin flags](#kubelet-cxl-plugin-flags)
- [Configuration file](#configuration-file)
- [Files on the node](#files-on-the-node)

Local memory devices are described in [LOCAL.md](LOCAL.md), pool devices and the pool controller
contract in [POOL.md](POOL.md), memory for running containers in [LEASES.md](LEASES.md).

## Requirements

- Kubernetes v1.37 or later. Claimed memory is accounted against the node's allocatable memory
  with the `DRANodeAllocatableResources` feature gate. Pool devices use device binding
  conditions (`DRADeviceBindingConditions`), device status (`DRAResourceClaimDeviceStatus`) and
  consumable capacity (`DRAConsumableCapacity`); `firstAvailable` requests use
  `DRAPrioritizedList`. See [CLUSTER_SETUP.md](../CLUSTER_SETUP.md).
- A container runtime with CDI support (containerd 1.7 or newer with CDI enabled, CRI-O 1.23 or
  newer) and, for steering and leases, with NRI enabled.
- cgroup v2.
- Kernel with CXL regions (`CONFIG_CXL_REGION`), `CONFIG_DEV_DAX`, `CONFIG_DEV_DAX_KMEM`,
  `CONFIG_MEMORY_HOTREMOVE`, and `CONFIG_FS_DAX` for mode `dax` (without it every devdax `mmap`
  fails).
- The `cxl` (ndctl) tool for local memdevs and pool devices, `daxctl` for pool devices
  ([LOCAL.md](LOCAL.md#node-requirements), [POOL.md](POOL.md#node-requirements)).
- `/sys/devices/system/memory/auto_online_blocks` set to `offline`: the plugin onlines the
  memory of `numa` slices and pool devices itself, and the kernel must never online shared
  devices as RAM.

## Deploy

### Kustomize

```bash
kubectl apply -k deployments/cxl
```

creates the namespace `intel-cxl-resource-driver`, the DaemonSet of the kubelet plugin, its
ServiceAccount, RBAC and ValidatingAdmissionPolicy
([deployments/cxl/base](../../deployments/cxl/base)), and the six DeviceClasses
([deployments/cxl/device-classes.yaml](../../deployments/cxl/device-classes.yaml)). The overlay
[deployments/cxl/overlays/nfd_labeled_nodes](../../deployments/cxl/overlays/nfd_labeled_nodes)
deploys the same without the DeviceClasses and restricts the DaemonSet to nodes that Node
Feature Discovery labels with `intel.feature.node.kubernetes.io/cxl: "true"`; the classes are
applied separately:

```bash
kubectl apply -k deployments/cxl/overlays/nfd_labeled_nodes
kubectl apply -f deployments/cxl/device-classes.yaml
```

The DaemonSet runs the plugin in an image that contains only `kubelet-cxl-plugin`: it has no
`cxl` and `daxctl` tools, no NRI socket and not the host PID namespace. In it the plugin
publishes regions and DRAM and prepares their claims, but does not steer containers. Local
memdevs and pool devices need the tools; steering and leases need NRI and the host PID namespace
(the lease broker maps the process ids of its callers to containers). In the DaemonSet, turn
them off with the environment variables `CONFIG_STR={"manageMemdevs": false}`,
`POOL_DRIVER_NAME=` (empty) and `LEASES=false`, or run the plugin as a host process.

### As a host process

Run `kubelet-cxl-plugin` on the node, for example as a systemd unit, with a kubeconfig that has
the rights of the ClusterRole in
[deployments/cxl/base/resource-driver.yaml](../../deployments/cxl/base/resource-driver.yaml):

```bash
kubectl apply -f deployments/cxl/device-classes.yaml
install -m 755 bin/kubelet-cxl-plugin bin/cxl-request /usr/local/bin/
echo '{}' > /etc/kubelet-cxl-plugin.yaml
systemd-run --unit kubelet-cxl-plugin -E KUBECONFIG=/etc/kubernetes/kubelet-cxl-plugin.conf \
    /usr/local/bin/kubelet-cxl-plugin --node-name "$(hostname)" -f /etc/kubelet-cxl-plugin.yaml
```

`cxl-request` must be a static binary (it runs in the containers of lease claims); building
both binaries is described in [BUILD.md](BUILD.md).

## Devices of cxl.generic

The plugin publishes one ResourceSlice per node (pool name = node name) under the driver
`cxl.generic`:

```yaml
- name: memdev-c100e2e1              # "memdev-" + serial in lower-case hex
  attributes:
    type:            {string: memdev}
    source:          {string: local}
    serial:          {string: "0xc100e2e1"}
    shared:          {bool: false}
    size:            {int: 1073741824}          # RAM capacity of the memdev
    nodeAffinity:    {int: 0}                   # CPU NUMA node; absent if unknown
    memoryBlockSize: {int: 134217728}           # every slice is a multiple of it
  capacity:
    memory: {value: 1Gi, requestPolicy: {default: 1Gi, validRange: {min: 128Mi, step: 128Mi}}}
  allowMultipleAllocations: true
  nodeAllocatableResources: {memory: {mapping: {capacityKey: memory, capacityMultiplier: "1"}}}
- name: region0-node2                # region0, online as NUMA node 2
  attributes:
    type:            {string: region}
    source:          {string: local}
    node:            {int: 2}
    size:            {int: 2147483648}
    nodeAffinity:    {int: 0}                   # if all memdevs of the region agree
  capacity: {memory: {value: 2Gi}}
  allowMultipleAllocations: true
  nodeAllocatableResources: {memory: {mapping: {capacityKey: memory, capacityMultiplier: "1"}}}
- name: dram
  attributes:
    type:            {string: dram}
    source:          {string: local}
    nodes:           {string: "0-1"}            # cpuset syntax
    size:            {int: 16777216000}
  capacity: {memory: {value: 16000Mi}}
  allowMultipleAllocations: true
  nodeAllocatableResources: {memory: {mapping: {capacityKey: memory, capacityMultiplier: "1"}}}
- name: lease
  attributes:
    type:            {string: lease}
  allowMultipleAllocations: true
```

| attribute | devices | value |
|:----------|:--------|:------|
| `type` | all | `memdev`, `region`, `dram` or `lease` |
| `source` | `memdev`, `region`, `dram` | `local` (pool devices have `pool`) |
| `size` | `memdev`, `region`, `dram` | bytes: the RAM capacity of a memdev, the size of a region, the DRAM of the nodes |
| `serial` | `memdev` | `0x` and lower-case hex |
| `shared` | `memdev` | `false`: `cxl.generic` publishes no device that several hosts share |
| `node` | `region` | the NUMA node of the region |
| `nodes` | `dram` | the DRAM NUMA nodes, cpuset syntax |
| `nodeAffinity` | `memdev`, `region` | the CPU NUMA node of the device; absent if unknown (for a region: unless all its memdevs agree) |
| `memoryBlockSize` | `memdev` | the memory block size of the node in bytes |

- `memdev-<serial hex>`, type `memdev`: a CXL memory device without a region; every claim gets
  a slice of it, created when the claim is prepared ([LOCAL.md](LOCAL.md)).
- `<region>-node<N>`, type `region`: a region that exists already (created by the BIOS or an
  admin) and is online as NUMA node N; claims consume its capacity and their containers are
  steered to N.
- `dram`, type `dram`: one device for all DRAM nodes, for steering; absent with
  `ignoreDRAMNodes`.
- `lease`, type `lease`: the device of lease claims ([LEASES.md](LEASES.md)); absent with
  `--leases=false`.

Every memory device has `capacity.memory` and `allowMultipleAllocations`, so that many claims
share it; with `DRANodeAllocatableResources`, `nodeAllocatableResources` maps the claimed memory
to the node's allocatable memory. Pool devices are published by the pool controller under
`cxl-pool.generic` with the same memdev attributes (`type`, `source`, `serial`, `shared`,
`size`), see [POOL.md](POOL.md#what-the-pool-controller-publishes).

Write selectors that test `type` first. Every device has `type`, every memory device has
`source` and `size`; the other attributes exist on some devices only, and a selector that reads
an attribute a device does not have fails the allocation of the claim. The shipped classes
guarantee the attributes their devices have, so selectors in claims on them may use those.

## DeviceClasses

[deployments/cxl/device-classes.yaml](../../deployments/cxl/device-classes.yaml):

| class | selects | gets |
|:------|:--------|:-----|
| `cxl-local` | `cxl.generic`, `type == "memdev"` | a slice of a local memdev: RAM of its NUMA node (mode `numa`, default) or a devdax device (`DeviceConfig` `mode: dax`) |
| `cxl-pool` | `cxl-pool.generic`, `type == "memdev"`, `shared == false` | a whole exclusive pool device: a new NUMA node (default) or a devdax device (`mode: dax`) |
| `cxl-pool-shared` | `cxl-pool.generic`, `type == "memdev"`, `shared == true` | a shared pool device as devdax; one of its `hosts` by default, all of them (exclusive) with `capacity.requests.hosts` |
| `cxl-region` | `cxl.generic`, `type == "region"` | capacity of a pre-created CXL NUMA node; the container is steered to it |
| `cxl-dram` | `cxl.generic`, `type == "dram"` | DRAM capacity for steering |
| `cxl-lease` | `cxl.generic`, `type == "lease"` | the lease socket and `cxl-request` |

For example:

```yaml
apiVersion: resource.k8s.io/v1
kind: DeviceClass
metadata: {name: cxl-pool}
spec:
  selectors:
  - cel: {expression: 'device.driver == "cxl-pool.generic" && device.attributes["cxl-pool.generic"].type == "memdev" && device.attributes["cxl-pool.generic"].shared == false'}
```

The classes carry no configuration: how a device is used (`mode`, `readOnly`) is a
`DeviceConfig` of the claim. Memory from either source, local first, is a `firstAvailable`
request with a subrequest per class ([example 9](#example-claims)).

## Opaque configurations

### DeviceConfig

Both drivers take the same configuration for the devices of a request:

```yaml
config:
- opaque:
    driver: cxl.generic                 # or cxl-pool.generic
    parameters:
      apiVersion: cxl.generic/v1alpha1  # <driver>/v1alpha1
      kind: DeviceConfig
      mode: dax
      readOnly: true
```

| field | values | default | |
|:------|:-------|:--------|:-|
| `mode` | `numa`, `dax` | `numa`; `dax` for shared devices | `numa`: the memory is online as system RAM of a NUMA node. `dax`: the container gets a devdax device `/dev/daxX.Y` of its own to `mmap`. Shared devices are `dax` only. |
| `readOnly` | `false`, `true` | `false` | `dax` only: the container's device node has CDI permissions `r`, so its device cgroup allows opening it for reading only. |

- `apiVersion` is the driver name followed by `/v1alpha1`; `driver` and `apiVersion` change
  together.
- Unknown fields are refused, and so is another apiVersion. An opaque configuration of the
  driver with a kind it does not know fails the Prepare before any device is touched:
  `unknown kind "..." (DeviceConfig or MemoryPolicyConfig)` for `cxl.generic`,
  `unknown kind "..." (DeviceConfig)` for `cxl-pool.generic` (a configuration without a kind
  gives `unknown kind ""`).
- Regions and DRAM (`cxl-region`, `cxl-dram`) are used as `numa` only: a `DeviceConfig` for
  them may say `mode: numa` and nothing else. In a claim that has such requests next to `dax`
  memdev requests, list the memdev requests in the `requests` of the `DeviceConfig`.
- A configuration comes from the DeviceClass or from the claim; the claim's comes later and
  replaces the class's. It applies to the requests it lists (a request `r` also covers its
  subrequests `r/s`) or, without `requests`, to all requests of its driver.
- Errors fail the Prepare of the claim (kubelet retries, the pod does not start); the error
  of the Prepare contains:
  - `DeviceConfig: invalid mode "..." (numa or dax)`
  - `DeviceConfig: mode numa: a shared device is used as dax only`
  - `DeviceConfig: readOnly needs mode dax`
  - `DeviceConfig: mode dax: a region is used as numa only`, `DeviceConfig: mode dax: a dram
    is used as numa only`
  - `DeviceConfig: apiVersion "...", expected "<driver>/v1alpha1"`
  - `DeviceConfig: invalid FromClaim config: json: unknown field "..."` (`FromClass` for a
    config of the DeviceClass)

| device | DeviceConfig | the container gets |
|:-------|:-------------|:-------------------|
| local memdev (`cxl-local`) | none | a slice online as RAM of the memdev's NUMA node |
| local memdev | `mode: dax` | a devdax slice of its own |
| exclusive pool device (`cxl-pool`) | none | the device online as a new NUMA node |
| exclusive pool device | `mode: dax` | the device as devdax |
| shared pool device (`cxl-pool-shared`) | none | the device as devdax, shared with the other claims of the device |
| shared pool device, `capacity.requests.hosts` = all | none | the device as devdax, no other claim has it meanwhile |
| any of the `dax` rows | `readOnly: true` | the same, the device node read-only |

Read-only is enforced by the kernel for every process of the container, whatever its
capabilities, except in privileged containers (their device cgroup allows everything). It is a
property of the claim, not of the device: another claim of the same device may write, and the
readers see it.

### MemoryPolicyConfig

`cxl.generic` only: how the memory of a container is spread over the DRAM and CXL nodes of its
claims while it allocates ([Steering](#steering)).

```yaml
config:
- opaque:
    driver: cxl.generic
    parameters:
      apiVersion: cxl.generic/v1alpha1
      kind: MemoryPolicyConfig
      memoryUseOrder: first-dram
```

| field | values | |
|:------|:-------|:-|
| `memoryUseOrder` | `first-dram`, `first-cxl`, `start-interleaved`, `end-interleaved`, `waypoints` | `first-dram`: the DRAM quota first, then CXL; `first-cxl` the other way round; `start-interleaved`: DRAM and CXL interleaved until the smaller quota is used, then the rest of the other; `end-interleaved`: the excess of the larger quota first, then interleaved; `waypoints`: along `memoryUseWaypoints` |
| `memoryUseWaypoints` | list of `{targetUsages: [{memoryType: DRAM or CXL, usage: <quantity>}]}` | target usage per memory type at each waypoint; a type that is not listed keeps its usage of the previous waypoint; usages never decrease |
| `minStep` | quantity | the smallest amount of new allocations after which the route is re-evaluated; default `maxStep` |
| `maxStep` | quantity | the largest amount of new allocations after which the route is re-evaluated; default the total quota |

A `MemoryPolicyConfig` belongs to a claim, not to a request: it applies to the containers that
reference the claim, whatever its `requests` say, and one container may have only one claim with
a policy. It may come from the DeviceClass or from the claim; the later one replaces the earlier.
Unknown fields, another apiVersion and an invalid policy fail the Prepare of the claim with
`MemoryPolicyConfig: ...`; an unknown kind with `unknown kind "..." (DeviceConfig or
MemoryPolicyConfig)`.

## Steering

A container that references `cxl.generic` claims in mode `numa` when it is created is steered:
the NRI plugin of `kubelet-cxl-plugin` finds the claims by their marker
(`CXL_CLAIM_<uid>`), takes the consumed capacity of every `dram` device as the DRAM quota and of
every `region` device and `numa` memdev slice as the CXL quota, and steers the container's
allocations along the claim's `MemoryPolicyConfig`. Without one, a container with only DRAM or
only CXL uses that one (`first-dram`, `first-cxl`), and one with both interleaves them
(`start-interleaved`). The steering adds the nodes to the container's `cpuset.mems` as the plan
reaches them (memory is never migrated), sets the memory policy of the container's threads, and
throttles each step with `memory.high`.

Pool devices in mode `numa` are not steered: the container gets the node number in
`CXL_<KEY>_NODE` and binds its memory to it itself, for example with `numactl --membind` or
`mbind`. The pool driver does not change `cpuset.mems`, so other containers whose cpusets allow
every node may allocate from the node too.

Memory for running containers (leases) in mode `numa` is lent through `cpuset.mems` instead; a
steered container cannot take such a lease ([LEASES.md](LEASES.md#limitations)).

## Environment variables

Every memory device of a claim, and the memory of a lease, gives a container the same set of
variables:

| variable | value | present for |
|:---------|:------|:------------|
| `CXL_<KEY>_SERIAL` | `0x<hex>` | memdevs (local and pool) |
| `CXL_<KEY>_SHARED` | `0` or `1` | memdevs: `1` for a shared device, also when the claim or lease uses it exclusively |
| `CXL_<KEY>_MODE` | `numa` or `dax` | every memory device |
| `CXL_<KEY>_SIZE` | bytes | every memory device: what the claim or lease got (the slice of a local memdev, the whole pool device, the consumed capacity of a region or DRAM) |
| `CXL_<KEY>_NODE` | NUMA node | mode `numa`: memdev slices, pool devices, regions |
| `CXL_<KEY>_NODES` | cpuset syntax | `dram` |
| `CXL_<KEY>_DAX` | `/dev/daxX.Y` | mode `dax`: the device node of a claim; of a lease, the host path (informational: the container has no such node) |
| `CXL_<KEY>_FD` | file descriptor number | mode `dax`, the command of `cxl-request request --exec` and `cxl-request fd --exec`, instead of `DAX` |
| `CXL_<KEY>_READONLY` | `0` or `1` | mode `dax` |

`<KEY>`:

- memdev: the serial in upper-case hex without `0x` (`C100E2E1`);
- region: `REGION<n>` from the region name (`REGION0`);
- DRAM: `DRAM`.

The second and later devices with the same key in one claim (two slices of one memdev, two
requests on one region) get `_2`, `_3`, ... appended: `C100E2E1_2`, `REGION0_2`. The count runs
over the allocation results of the claim, whatever requests a container references.

Programs find what they got by scanning `CXL_*_MODE`.

The variables and device nodes come from CDI devices, one per allocation result of a claim:
`generic/cxl=claim-<uid>-<n>` (spec file `cxl-claims`) and `generic/cxl-pool=claim-<uid>-<n>`
(spec file `cxl-pool`), n being the index of the result in the claim's allocation (so the n of
one driver may have gaps). Each has the claim marker and the variables and device node of its
own result only. kubelet gives a container the CDI devices of the requests it references
(`resources.claims[].request`), all of them without a request: a container that references
only the read-only request of a claim gets only that request's device node and variables.

Other variables:

| variable | |
|:---------|:-|
| `CXL_CLAIM_<uid>=1` | the claim marker (the claim uid with `_` for `-`), in every CDI device of both drivers, internal: the NRI plugin finds the claims of a container by it; pool claims have no steering information and are not steered |
| `CXL_LEASE_SOCKET=/run/cxl/cxl.sock` | containers of claims on `cxl-lease` ([LEASES.md](LEASES.md)) |
| `CXL_LEASE=<id>` | the output of `cxl-request env` and the commands of `cxl-request ... --exec` |

Two claims of the same memdev in one container set the same names; the container sees one value
of each.

## What the memory contains

- Mode `numa`, any source: the memory is system RAM; the kernel gives a process only zeroed
  pages.
- Local `dax` slices: the plugin writes zeros to the whole slice before the claim gets it, so a
  claim never reads what an earlier claim left there.
- Exclusive pool devices in mode `dax`: the content is kept. Nothing clears the device between
  claims, so a claim, maybe on another node, reads what the previous one wrote.
- Shared pool devices: the same bytes for every host that has the device attached, as long as
  the pool keeps the device.

## Example claims

[deployments/cxl/examples](../../deployments/cxl/examples) has a complete claim and pod for each
case. The pods of claims 1-10 use their claim as `mem` the same way:

```yaml
apiVersion: v1
kind: Pod
metadata: {name: consumer}
spec:
  resourceClaims:
  - {name: mem, resourceClaimName: <claim>}       # or resourceClaimTemplateName
  containers:
  - name: app
    image: docker.io/library/python:3-alpine
    command: ["sh", "-c", "env | grep ^CXL_ | sort; sleep infinity"]
    resources: {claims: [{name: mem}]}
```

The claims show `spec.devices` and the environment the container sees, with the local memdev
`0xc100e2e1`, the exclusive pool device `0xc1ee0001` and the shared pool device `0xc1ae0001`.

1. 128Mi of local CXL memory as RAM
   ([local-numa.yaml](../../deployments/cxl/examples/local-numa.yaml)):

   ```yaml
   requests: [{name: mem, exactly: {deviceClassName: cxl-local, capacity: {requests: {memory: 128Mi}}}}]
   ```

   ```
   CXL_C100E2E1_MODE=numa
   CXL_C100E2E1_NODE=2
   CXL_C100E2E1_SERIAL=0xc100e2e1
   CXL_C100E2E1_SHARED=0
   CXL_C100E2E1_SIZE=134217728
   CXL_CLAIM_<uid>=1
   ```

2. A 64Mi read-only dax slice of a local memdev; with 128 MiB memory blocks the slice is 128Mi
   ([local-dax-readonly.yaml](../../deployments/cxl/examples/local-dax-readonly.yaml)):

   ```yaml
   requests: [{name: mem, exactly: {deviceClassName: cxl-local, capacity: {requests: {memory: 64Mi}}}}]
   config:
   - opaque: {driver: cxl.generic, parameters: {apiVersion: cxl.generic/v1alpha1, kind: DeviceConfig, mode: dax, readOnly: true}}
   ```

   ```
   CXL_C100E2E1_DAX=/dev/dax0.1
   CXL_C100E2E1_MODE=dax
   CXL_C100E2E1_READONLY=1
   CXL_C100E2E1_SERIAL=0xc100e2e1
   CXL_C100E2E1_SHARED=0
   CXL_C100E2E1_SIZE=134217728
   CXL_CLAIM_<uid>=1
   ```

   One ResourceClaim (not a template) that several pods reference gives all of them the same
   slice ([local-dax-shared-claim.yaml](../../deployments/cxl/examples/local-dax-shared-claim.yaml)).

3. A specific local device: add to the request

   ```yaml
   selectors: [{cel: {expression: 'device.attributes["cxl.generic"].serial == "0xc100e2e1"'}}]
   ```

4. 512Mi of pool memory as RAM: a whole exclusive device of at least 512Mi
   ([pool-numa.yaml](../../deployments/cxl/examples/pool-numa.yaml)):

   ```yaml
   requests: [{name: mem, exactly: {deviceClassName: cxl-pool, capacity: {requests: {memory: 512Mi}}}}]
   ```

   ```
   CXL_C1EE0001_MODE=numa
   CXL_C1EE0001_NODE=3
   CXL_C1EE0001_SERIAL=0xc1ee0001
   CXL_C1EE0001_SHARED=0
   CXL_C1EE0001_SIZE=536870912
   CXL_CLAIM_<uid>=1
   ```

5. An exclusive pool device as dax: claim 4 plus
   ([pool-dax.yaml](../../deployments/cxl/examples/pool-dax.yaml))

   ```yaml
   config:
   - opaque: {driver: cxl-pool.generic, parameters: {apiVersion: cxl-pool.generic/v1alpha1, kind: DeviceConfig, mode: dax}}
   ```

   ```
   CXL_C1EE0001_DAX=/dev/dax0.0
   CXL_C1EE0001_MODE=dax
   CXL_C1EE0001_READONLY=0
   ...
   ```

6. The shared device `0xc1ae0001`, one of its hosts
   ([pool-shared-writer-readers.yaml](../../deployments/cxl/examples/pool-shared-writer-readers.yaml)
   has the writer of 7 and the readers of 8):

   ```yaml
   requests: [{name: mem, exactly: {deviceClassName: cxl-pool-shared, selectors: [{cel: {expression: 'device.attributes["cxl-pool.generic"].serial == "0xc1ae0001"'}}]}}]
   ```

   ```
   CXL_C1AE0001_DAX=/dev/dax0.0
   CXL_C1AE0001_MODE=dax
   CXL_C1AE0001_READONLY=0
   CXL_C1AE0001_SERIAL=0xc1ae0001
   CXL_C1AE0001_SHARED=1
   CXL_C1AE0001_SIZE=268435456
   CXL_CLAIM_<uid>=1
   ```

7. The same device exclusively (the writer of single-writer sharing): claim 6 plus, in
   `exactly`,

   ```yaml
   capacity: {requests: {hosts: "4"}}
   ```

8. The same device read-only (a reader): claim 6 plus

   ```yaml
   config:
   - opaque: {driver: cxl-pool.generic, parameters: {apiVersion: cxl-pool.generic/v1alpha1, kind: DeviceConfig, readOnly: true}}
   ```

   ```
   CXL_C1AE0001_READONLY=1
   ...
   ```

   A reader that asks while the writer has the device waits (its pod stays pending) until the
   writer's claim is gone.

9. 256Mi as dax, local if a local memdev has room, else pool
   ([local-then-pool-dax.yaml](../../deployments/cxl/examples/local-then-pool-dax.yaml)):

   ```yaml
   requests:
   - name: mem
     firstAvailable:
     - {name: local, deviceClassName: cxl-local, capacity: {requests: {memory: 256Mi}}}
     - {name: pool,  deviceClassName: cxl-pool,  capacity: {requests: {memory: 256Mi}}}
   config:
   - opaque: {driver: cxl.generic,      parameters: {apiVersion: cxl.generic/v1alpha1,      kind: DeviceConfig, mode: dax}}
   - opaque: {driver: cxl-pool.generic, parameters: {apiVersion: cxl-pool.generic/v1alpha1, kind: DeviceConfig, mode: dax}}
   ```

10. Steering to a pre-created CXL node after DRAM
    ([region-dram-policy.yaml](../../deployments/cxl/examples/region-dram-policy.yaml)):

    ```yaml
    requests:
    - {name: dram, exactly: {deviceClassName: cxl-dram,   capacity: {requests: {memory: 64Mi}}}}
    - {name: cxl,  exactly: {deviceClassName: cxl-region, capacity: {requests: {memory: 64Mi}}}}
    config:
    - opaque: {driver: cxl.generic, parameters: {apiVersion: cxl.generic/v1alpha1, kind: MemoryPolicyConfig, memoryUseOrder: first-dram}}
    ```

    ```
    CXL_CLAIM_<uid>=1
    CXL_DRAM_MODE=numa
    CXL_DRAM_NODES=0-1
    CXL_DRAM_SIZE=67108864
    CXL_REGION0_MODE=numa
    CXL_REGION0_NODE=2
    CXL_REGION0_SIZE=67108864
    ```

11. A pod that may request memory while it runs; a template, so that the claim follows the pod
    ([lease.yaml](../../deployments/cxl/examples/lease.yaml), [LEASES.md](LEASES.md)):

    ```yaml
    apiVersion: resource.k8s.io/v1
    kind: ResourceClaimTemplate
    metadata: {name: cxl-lease}
    spec: {spec: {devices: {requests: [{name: lease, exactly: {deviceClassName: cxl-lease}}]}}}
    ```

    The container gets `CXL_LEASE_SOCKET=/run/cxl/cxl.sock`, `/usr/local/bin/cxl-request` and
    the claim marker.

## kubelet-cxl-plugin flags

| flag | environment | default | |
|:-----|:------------|:--------|:-|
| `--node-name` | `NODE_NAME` | (required) | the node the plugin runs on |
| `--kubeconfig` | `KUBECONFIG` | in-cluster config | |
| `--cdi-root` | `CDI_ROOT` | `/etc/cdi` | where the CDI spec files are written |
| `--sysfs` | `SYSFS_ROOT` | (empty: the host's `/sys`) | sysfs mount point, for testing |
| `--config-str`, `-c` | `CONFIG_STR` | (empty) | the [configuration](#configuration-file) as a JSON or YAML string |
| `--config-file`, `-f` | | | the configuration file, JSON or YAML; not together with `-c` |
| `--nri-name` | `NRI_PLUGIN_NAME` | `kubelet-cxl-plugin` | NRI plugin name |
| `--nri-idx` | `NRI_PLUGIN_IDX` | `30` | NRI plugin index (place in the plugin chain, higher is later) |
| `--nri-socket` | `NRI_SOCKET` | (empty: the runtime's default) | NRI socket path |
| `--pool-driver-name` | `POOL_DRIVER_NAME` | `cxl-pool.generic` | DRA driver name of the pool driver; empty disables it |
| `--pool-prepare-timeout` | `POOL_PREPARE_TIMEOUT` | `30s` | how long the pool driver waits for a hotplugged memdev |
| `--leases` | `LEASES` | `true` | lend memory to running containers: publish the `lease` device, serve lease sockets, create companions (needs NRI) |
| `--lease-cli` | `LEASE_CLI` | `/usr/local/bin/cxl-request` | host path of the static `cxl-request`, mounted into the containers of lease claims |
| `--lease-wait` | `LEASE_WAIT` | `120s` | how long a lease request waits for Ready when it does not say |
| `--lease-companion-image` | `LEASE_COMPANION_IMAGE` | `registry.k8s.io/pause:3.10` | image of the companion pods of leases |

`--kube-api-qps`, `--kube-api-burst`, `--feature-gates` and the logging flags (`-v`) are common
to the drivers of this repository; `kubelet-cxl-plugin --help` lists them all.

Without NRI (the socket cannot be reached) the plugin runs on: claims are prepared, containers
are not steered, and lease requests fail with `lending unavailable: NRI not connected`.

## Configuration file

`-f FILE` (or `-c STRING`), JSON or YAML; unknown keys are refused. All keys are optional:

```yaml
ignoreDevices: ["mem0", "0xc100e2e0"]
manageMemdevs: true
udevStableDuration: 200ms
```

| key | type | default | |
|:----|:-----|:--------|:-|
| `ignoreDevices` | list of strings | | CXL memory devices to leave alone: device name (`mem0`, `cxl/mem0`), `major:minor` (`251:0`), or serial in decimal or `0x` hex |
| `ignoreRegions` | list of strings | | CXL regions to leave alone (`region0`) |
| `ignoreNodes` | list of integers | | NUMA nodes to leave alone |
| `ignoreNewMemoryDevices` | bool | `false` | ignore memdevs that are hotplugged after the start |
| `ignoreNewRegions` | bool | `false` | ignore regions that are created after the start |
| `ignoreNewNodes` | bool | `false` | ignore NUMA nodes that appear after the start |
| `ignoreDRAMNodes` | bool | `false` | publish no `dram` device |
| `udevStableDuration` | duration | `200ms` | quiet period after the last udev event before the devices are scanned again |
| `manageMemdevs` | bool | `true` | publish the local memdevs that have no region and create their regions for claims ([LOCAL.md](LOCAL.md)) |
| `testability` | object | | fake devices for testing without CXL hardware: `fakeRegionDevices`, `fakeUdevFifo` |

## Files on the node

| file | |
|:-----|:-|
| `/var/lib/kubelet/plugins/cxl.generic/preparedClaims.json` | prepared claims of `cxl.generic` (kubelet plugin helper) |
| `/var/lib/kubelet/plugins/cxl.generic/preparedClaimsInfo.json` | devices, memory policy and lend of every prepared claim of `cxl.generic` |
| `/var/lib/kubelet/plugins/cxl.generic/localDevices.json` | regions and slices of local memdevs ([LOCAL.md](LOCAL.md)) |
| `/var/lib/kubelet/plugins/cxl.generic/leases.json` | the lease table ([LEASES.md](LEASES.md)) |
| `/var/lib/kubelet/plugins/cxl.generic/leases/<claim uid>/cxl.sock` | the lease socket of a lease claim |
| `/var/lib/kubelet/plugins/cxl-pool.generic/preparedPoolClaims.json` | prepared claims of `cxl-pool.generic` ([POOL.md](POOL.md)) |
| `<cdi root>/cxl-claims.yaml` | CDI kind `generic/cxl`: a device `claim-<uid>-<n>` per allocation result n of a prepared `cxl.generic` claim, with the marker, the result's variables and its dax device node |
| `<cdi root>/cxl-leases.yaml` | CDI kind `generic/cxl`: a device `lease-<uid>` per lease claim |
| `<cdi root>/cxl-pool.yaml` | CDI kind `generic/cxl-pool`: a device `claim-<uid>-<n>` per allocation result n of a prepared pool claim, likewise |

Companion claims of leases get no CDI device ([LEASES.md](LEASES.md#companion-objects)).

At start the plugin deletes every CDI spec of the kinds `generic/cxl` and `generic/cxl-pool` and
writes the three files again from the prepared claims.
