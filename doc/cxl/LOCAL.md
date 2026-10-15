# Local CXL memory devices

CAUTION: experimental. Interfaces may change.

A CXL memory device (memdev) attached to a node, at boot or by hotplug,
holds no usable memory until a region is created on it. The `cxl.generic`
driver of kubelet-cxl-plugin publishes such free devices in the node's
ResourceSlice and creates and destroys their regions on demand: every
claim gets a slice of a device, as memory of a NUMA node or as a devdax
device, slices of both kinds share a device, and a running container can
lease local memory and give it back
(`cxl-request request --source local --size 64Mi`, see [LEASES.md](LEASES.md)).

Regions that exist already (created by the BIOS or an admin, onlined as
RAM) are published as `region` devices ([USAGE.md](USAGE.md#devices-of-cxlgeneric));
this document is about the devices the plugin manages itself.

## What the plugin publishes

A memdev is published as a device of type `memdev` when it is enabled
(bound to the `cxl_mem` driver; the kernel enables a hotplugged memdev,
and the plugin never disables a local one), has RAM capacity, is not a
pool device (prepared by the pool driver, see [POOL.md](POOL.md)), is not
excluded by `ignoreDevices` (name, `cxl/memN`, `major:minor` or serial)
and has no region that the plugin did not create itself. A memdev with a
foreign region is left alone: its RAM region is a `region` device, a
foreign devdax region is not published. Destroy the region to let the
plugin manage the device.

Local and pool devices are told apart by serial. A memdev is a pool
device when the pool driver prepares it for a claim, or when its serial
is the `serial` attribute (or `<driver>/serial`) of a device in a
ResourceSlice of any DRA driver other than `cxl.generic`: a pool
controller publishes its devices before it attaches one to a node, so a
pool device is never offered as a local one, not even between its attach
and its Prepare, or when its pod never starts. The plugin lists the
ResourceSlices at every scan. When a list fails it keeps the serials of
the last successful one; until a list has succeeded it publishes no
`memdev` device at all (regions are published as usual), and it tries
again every 30 seconds while the list fails.

```yaml
- name: memdev-c100e2e1               # "memdev-" + serial in lower-case hex
  attributes:
    type:            {string: memdev}
    source:          {string: local}
    serial:          {string: "0xc100e2e1"}
    shared:          {bool: false}
    size:            {int: 1073741824}
    nodeAffinity:    {int: 0}
    memoryBlockSize: {int: 134217728}
  capacity:
    memory:
      value: 1Gi
      requestPolicy:
        default: 1Gi                  # the whole device
        validRange: {min: 128Mi, step: 128Mi}   # the memory block size
  allowMultipleAllocations: true
  nodeAllocatableResources: {memory: {mapping: {capacityKey: memory, capacityMultiplier: "1"}}}
```

| Attribute | |
|:--|:--|
| `type` | `memdev` |
| `source` | `local` (pool devices have `source: pool`) |
| `serial` | serial of the memdev, `0x` and lower-case hex, as the pool controller publishes serials |
| `shared` | `false`: a local memdev is used by this node only |
| `size` | RAM capacity of the memdev in bytes |
| `nodeAffinity` | CPU NUMA node of the memdev; absent if unknown |
| `memoryBlockSize` | memory block size of the node in bytes: every slice is a multiple of it |

Every slice, `numa` or `dax`, is a multiple of the memory block size of
the node, `/sys/devices/system/memory/block_size_bytes` (hex without
`0x`: `8000000` is 128 MiB; 128 MiB in the e2e VMs, up to 2 GiB on servers
with much RAM), and lies on whole memory blocks. Capacity `memory` is
what whole blocks of the device's region are sure to hold: the RAM
capacity rounded down to whole blocks. With blocks larger than 256 MiB
(the alignment of CXL regions in their window) a region can start up to
block size - 256 MiB after a block boundary, so that head is subtracted
first: floor((RAM - (block size - 256 MiB)) / block size) blocks. The
request policy makes the scheduler account exactly what the plugin
carves: requests are rounded up to the block size
(`requestPolicy.validRange` min and step); a request without an amount
consumes `requestPolicy.default`, the whole capacity. The API server
requires a `default` with a `validRange`, and a range needs room for its
`min` and one `step`: a capacity of one block has `validValues: [<block
size>]` instead. A memdev without the room for one whole block is not
published, and no memdev is published while the block size cannot be
read. `nodeAllocatableResources` maps the capacity to the node's memory as
for `region` devices (with the `DRANodeAllocatableResources` feature
gate only).

The driver configuration file (`/etc/kubelet-cxl-plugin.yaml`, `-f`)
option `manageMemdevs: false` turns all of this off: free memdevs are not
published and no regions are created.

## Modes

The DeviceClass `cxl-local` selects every local memdev. Every claim of a
device gets a slice of its own, in one of two modes, and slices of both
modes share a device at the same time:

- `numa` (default): the slice is onlined as system RAM in movable memory
  blocks of the NUMA node N of the device. All `numa` slices of a device
  (and of the devices under one CXL host bridge) are on the same node N;
  the containers are isolated by NUMA node (`cpuset.mems`), not per slice.
- `dax`: the slice is a character device `/dev/daxX.Y` of its own.

The mode, and `readOnly` for `dax` slices, is a `DeviceConfig` of the
claim ([USAGE.md](USAGE.md#deviceconfig)):

```yaml
config:
- opaque:
    driver: cxl.generic
    parameters:
      apiVersion: cxl.generic/v1alpha1
      kind: DeviceConfig
      mode: dax          # numa (default) or dax
      readOnly: true     # dax only: the device node allows reads only
```

### Slices and memory blocks

A slice is a dax device of the device's devdax region. Memory that is
onlined as RAM must start and end at memory block boundaries (the kernel's
kmem driver adds only the whole blocks of a dax device). Because every
slice is a multiple of the block size, the kernel's first-fit placement of
dax devices keeps every slice and every free gap of a block-aligned region
block-aligned, also after releases (a slice larger than any gap gets
several ranges, each block-aligned). The plugin checks every range of
every slice, `dax` slices too, before it binds a driver
(`daxX.Y/mappingN`). A slice that first-fit placed across a block boundary
(a region that does not start at a block boundary) is deleted and created
again on the first whole free blocks of the region, combining several
free ranges if one is not enough (`daxX.Y/mapping`), so that the head and
tail of such a region are never used and no slice splits a block. The
plugin logs it, and a region that does not start at a block boundary when
it is created:

```
local: memdev mem1 (serial 0xc100e2e1): claim <uid>: slice dax0.2 at [0x...] is not aligned to the memory block size 536870912, placing it explicitly
local: memdev mem1 (serial 0xc100e2e1): region0 at 0x250000000-0x2cfffffff does not start at a memory block boundary (block size 536870912): slices use only its 1610612736 bytes of whole blocks
```

### Node size

The memory of node N is the sum of the online `numa` slices: N grows by
the block size for every block of a `numa` slice that is prepared, and
shrinks when the slice is released (N disappears from `numactl -H` or
has 0 kB when the last `numa` slice is gone). `dax` slices add nothing to
N. Capacity that no claim has stays offline. The containers whose claims
or leases are on N share it; `memory.max` and cgmpolmgr's quota of each
container (the slice size of its claims) bound what a container uses.

All regions under one CXL host bridge share one NUMA node (the node of
the host bridge's memory window): two memdevs under one host bridge are
one node to the containers.

### Claims

| | claim on `cxl-local` |
|:--|:--|
| 128Mi as NUMA memory | `exactly: {deviceClassName: cxl-local, capacity: {requests: {memory: 128Mi}}}` |
| a 128Mi dax slice | the same and a `DeviceConfig` with `mode: dax` |
| a read-only slice | a `DeviceConfig` with `mode: dax, readOnly: true` |
| a specific device | add `selectors: [{cel: {expression: 'device.attributes["cxl.generic"].serial == "0xc100e2e1"'}}]` |
| the whole device | no capacity request |
| a slice shared by pods | one ResourceClaim (not a template) that several pods reference |

Requests are rounded up to the memory block size (64Mi gets a 128Mi slice
with 128 MiB blocks). Pods that reference the same ResourceClaim land on
the node of the claim's device; kubelet prepares the claim once, so all of
them get the same slice.

Examples in [`deployments/cxl/examples`](../../deployments/cxl/examples):
[`local-numa.yaml`](../../deployments/cxl/examples/local-numa.yaml),
[`local-dax-readonly.yaml`](../../deployments/cxl/examples/local-dax-readonly.yaml),
[`local-dax-shared-claim.yaml`](../../deployments/cxl/examples/local-dax-shared-claim.yaml)
(two pods, one claim).

## What Prepare does

For every allocated `memdev` device of a claim:

1. The mode and read-only setting come from the `DeviceConfig`, the
   size from the allocation's consumed capacity (the whole device if
   there is none), rounded up to a multiple of the memory block size
   (the block size is read again for every slice). A region the plugin
   left without claims (after a restart) is destroyed first.
2. The first claim of a device creates a devdax region on the memdev.
3. The claim's slice: the region's dax device `daxX.0` when no slice uses
   it and kmem does not have it, else a new dax device of the dax region,
   placed first-fit by the kernel and checked for whole blocks (see
   above). A dax region with less free memory than the slice fails the
   Prepare with "no room for the slice" (the scheduler accounts the
   capacity: this is not expected).
4. `numa`: the slice is bound to kmem through sysfs, its memory blocks
   are onlined movable, and N is the slice's `target_node`. The claim has
   node N with the slice size as its total and consumed bytes: containers
   that reference it are steered to N exactly as for `region` devices
   ([USAGE.md](USAGE.md#steering)).
5. `dax`: the slice is bound to `device_dax`. The plugin writes zeros to
   the whole slice before the claim gets it, so a claim never reads what
   an earlier claim left there (this takes size / memory write bandwidth,
   about 30 ms for 64 MiB and 120 ms for 256 MiB of DRAM). A read-only
   slice has device node permissions `r`: the device cgroup refuses opens
   for writing, whatever the container's capabilities. Applications
   `mmap` the device (`MAP_SHARED`, aligned to the device alignment,
   2 MiB) and agree on the layout of the memory themselves.
6. The slice's CDI device `generic/cxl=claim-<claim uid>-<n>` (spec file
   `cxl-claims`, n the index of the allocation result in the claim) has
   the claim marker `CXL_CLAIM_<uid>=1`, the device node of a `dax` slice
   and the variables of [USAGE.md](USAGE.md#environment-variables):

   ```
   CXL_<KEY>_SERIAL=0x<serial>
   CXL_<KEY>_SHARED=0
   CXL_<KEY>_MODE=numa                (or dax)
   CXL_<KEY>_SIZE=<slice bytes>
   CXL_<KEY>_NODE=<N>                 (numa)
   CXL_<KEY>_DAX=/dev/daxX.Y          (dax)
   CXL_<KEY>_READONLY=0|1             (dax)
   ```

   `<KEY>` is the serial in upper-case hex without `0x`, `<serial>` in
   lower-case hex; the second and later slices of the same memdev in the
   claim have `<KEY>_2`, `<KEY>_3`, ... A container gets the devices of
   the requests it references only: of a claim with a read-write and a
   read-only request, a container that references the read-only one
   gets only the read-only device node.

A Prepare that fails releases what it took for the claim. Preparing a
claim that is prepared already touches no device. The plugin's use of the
devices is stored in `localDevices.json` in the `cxl.generic` plugin data
directory, with the mode of every slice.

Unprepare: a `numa` slice's memory blocks are offlined (only the blocks
of that slice), then the slice is unbound, set to size 0 and deleted
(`daxX.0` is kept with size 0); a `dax` slice is deleted the same way.
When the last slice of a device is gone, of either mode, its region is
destroyed. The memdev stays enabled (unlike a pool device): the device is
back in its initial state. An offline that
fails because pages cannot move fails the Unprepare and keeps the slice;
kubelet retries (the pages of other containers on the movable node
migrate on a later try). kmem is never unbound from memory that is not
offline: the kernel could not remove it, and it would stay online until a
reboot.

A slice whose release fails where no claim is left to retry it (a
restart's release of a claim that is not prepared, the rollback of a
failed Prepare, a dax device that belongs to no slice) is kept in the
state under the owner `-` (no claim UID is `-`). Such a slice is never
handed out again; the next Prepare on the device releases it first, and
fails with the release error while it cannot, and so does every restart.
The region stays while such a slice exists.

The plugin logs every step of a slice with its dax device, mode, size and
duration:

```
local: memdev mem1 (serial 0xc100e2e1): created region0 (devdax, dax dax0.0, node 2) in 112.031ms
local: memdev mem1 (serial 0xc100e2e1): claim <uid>: created slice dax0.1 mode numa size 134217728 at [0x258000000-0x25fffffff] in 3.208ms
local: memdev mem1 (serial 0xc100e2e1): claim <uid>: onlined slice dax0.1 mode numa size 134217728 blocks [75] node 2 in 41.002ms
local: memdev mem1 (serial 0xc100e2e1): claim <uid>: zeroed slice dax0.0 mode dax size 134217728 in 60.415ms
local: memdev mem1 (serial 0xc100e2e1): claim <uid>: offlined slice dax0.1 mode numa size 134217728 blocks [75] in 20.117ms
local: memdev mem1 (serial 0xc100e2e1): claim <uid>: deleted slice dax0.1 mode numa size 134217728 in 5.420ms
local: memdev mem1 (serial 0xc100e2e1): released region0 in 90.733ms
```

Restart: the state file is read again; regions are found by the memdev
serial and slices by name; a slice that is gone is dropped, the slices of
claims that are not in the prepared claims (`preparedClaimsInfo.json`)
are released (`numa` slices offlined first), and so are the slices of the
owner `-`, dax devices of the region that belong to no slice are deleted,
and a region of the plugin without claims is destroyed.
Regions do not survive a node reboot: the state file records the boot id
of the kernel (`/proc/sys/kernel/random/boot_id`), and the state of
another boot is dropped without touching any region, so that a region
that firmware or an admin created under the same name is never taken for
the plugin's. A `dax` slice that is gone gets no device node and no
`CXL_<KEY>_DAX` in its CDI device.

## Limitations

- One region per device, no interleaving across devices.
- `numa` capacity is online per claim but not isolated per claim:
  `cpuset.mems` is per node, all containers with `numa` claims or leases
  on the node share its memory; `memory.max` and cgmpolmgr bound each.
- Slice sizes are multiples of the memory block size, also for `dax`
  slices: a 64Mi dax claim gets 128Mi with 128 MiB blocks, 2Gi with 2 GiB
  blocks.
- The memory of node N can be a little less than its `numa` slices when
  the kernel takes the struct pages of hotplugged memory from the memory
  itself (`memory_hotplug.memmap_on_memory`).
- With memory blocks larger than 256 MiB a device publishes one block
  less than its RAM capacity (for a RAM capacity of whole blocks): its
  region may start after a block boundary, and only whole blocks hold
  slices. An unaligned end needs no such room: it holds less than a block,
  which the rounding down excludes already.
- Two claims of one device in one container set the same
  `CXL_<KEY>_*` names (the device nodes of `dax` slices differ).
- With `DRANodeAllocatableResources`, a dax claim raises the container's
  memory limit by the slice size although dax memory is not charged to
  the cgroup.
- Leases of local memory have the limitations of every lease
  ([LEASES.md](LEASES.md#limitations)): memory limits of the requester,
  no `numa` lease for a steered container, SIGBUS after a release of a
  mapped dax fd.
- A region that an admin creates on a memdev while the plugin uses it is
  not published as a `region` device until the plugin has released the
  memdev: the plugin hides every region of the memdevs it uses.
- A memdev that is shared with other hosts but not managed by a pool
  controller looks like a local device. Onlining it as RAM corrupts the
  other hosts' data: exclude it with `ignoreDevices`.
- Hot-removing a local device that is in use is not supported; the kernel
  refuses to remove a device with an online region.

## Node requirements

- Kernel with CXL regions (`CONFIG_CXL_REGION`), `CONFIG_DEV_DAX`,
  `CONFIG_DEV_DAX_KMEM`, `CONFIG_MEMORY_HOTREMOVE`, and `CONFIG_FS_DAX`
  for the `dax` mode (without it every devdax `mmap` fails).
- The `cxl` (ndctl) tool. `daxctl` is optional: dax drivers are switched
  with `daxctl` when it is installed, else through sysfs; slices are made
  through sysfs.
- `/sys/devices/system/memory/auto_online_blocks` should be `offline`:
  the plugin onlines the blocks of `numa` slices movable itself (kmem is
  bound through sysfs, not with `daxctl`). Region creation also adds the
  whole region through kmem for a moment (the kernel binds the dax device
  of a new RAM region to kmem) before the plugin switches it to
  `device_dax`: with `online` the kernel onlines the whole region in a
  kernel zone, and the offline before the switch can fail.
