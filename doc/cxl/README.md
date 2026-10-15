# Intel CXL resource driver for Kubernetes

CAUTION: This is a beta / non-production software, do not use on production clusters.

## About resource driver

The DRA driver publishes ResourceSlices, the scheduler allocates the resources, and the resource
driver's kubelet-plugin ensures that the allocated devices are prepared and available for Pods.

One process, `kubelet-cxl-plugin`, runs two DRA drivers on every node:

- `cxl.generic` publishes the memory of the node in a ResourceSlice of the node: CXL memory
  devices (memdevs) whose regions the plugin creates on demand, CXL regions that exist already,
  the DRAM nodes, and the `lease` device that lets running containers request memory. It also
  steers the memory of containers to the NUMA nodes of their claims.
- `cxl-pool.generic` prepares the devices of a CXL memory pool. A pool controller (cluster
  level, specific to the pool product) publishes the pool devices and attaches them to the node
  that the scheduler picked; the node plugin makes the hotplugged memory usable for the pod.

## Supported Kubernetes Versions

Kubernetes v1.37 is the minimum supported version.

| Branch            | Kubernetes branch/version       | Status      | DRA                            |
|:------------------|:--------------------------------|:------------|:-------------------------------|
| v0.1.0            | Kubernetes v1.37+               | Supported   | Structured Parameters          |

The driver needs the `DRANodeAllocatableResources` feature gate to account claimed memory against
the node's allocatable memory.

## Vocabulary

The same words name the same thing in device attributes, DeviceClasses, opaque configurations,
environment variables, the `cxl-request` CLI and its protocol.

| word | values | meaning |
|:-----|:-------|:--------|
| `source` | `local`, `pool` | who provides the memory: `local` a device attached to the node, managed by `cxl.generic`; `pool` a device of a memory pool, attached by a pool controller, prepared by `cxl-pool.generic` |
| `type` | `memdev`, `region`, `dram`, `lease` | what a published device is: `memdev` a CXL memory device whose region the plugin creates when a claim needs it; `region` a region that existed before the plugin, online as a NUMA node; `dram` the DRAM nodes of the system; `lease` the device whose claim lets a container request memory while it runs |
| `shared` | `true`, `false` | `true`: several hosts may map the same bytes at a time (a pool device with a `hosts` capacity); such memory is never onlined as RAM. `false`: one host at a time |
| `mode` | `numa`, `dax` | how a container gets the memory: `numa` as system RAM of a NUMA node; `dax` as a devdax device of its own (claims) or an open file descriptor (leases). Default `numa`; shared memory is `dax` only |
| `readOnly` | `true`, `false` | `dax` only: the device node (claims) or the file descriptor (leases) allows reads only |
| `serial` | `0x` and lower-case hex | `/sys/bus/cxl/devices/memN/serial`, the identity of a memdev in both drivers; upper-case hex without `0x` in environment variable names |
| `size` | bytes, or a quantity such as `512Mi` | attribute `size` and `CXL_<KEY>_SIZE` are bytes; requests and the `--size` of `cxl-request` take a quantity, "at least this much" |
| `hosts` | integer | capacity of a shared pool device: how many nodes may have it attached at a time. A claim consumes 1 by default; all of them is exclusive use |
| lease | 8 hex digits | one request of a running container for memory, served through the lease socket; its memory comes from a companion claim |

## DeviceClasses

[`deployments/cxl/device-classes.yaml`](../../deployments/cxl/device-classes.yaml) has six
classes. They select device kinds only; `mode` and `readOnly` are set with a `DeviceConfig`
(see [USAGE.md](USAGE.md#opaque-configurations)).

| class | gets |
|:------|:-----|
| `cxl-local` | a slice of a local memdev: RAM of its NUMA node (default) or a devdax device (`mode: dax`) |
| `cxl-pool` | a whole exclusive pool device: a new NUMA node (default) or a devdax device (`mode: dax`) |
| `cxl-pool-shared` | a shared pool device as devdax; one of its `hosts` by default, all of them (exclusive) with `capacity.requests.hosts` |
| `cxl-region` | capacity of a pre-created CXL NUMA node; the container is steered to it |
| `cxl-dram` | DRAM capacity for steering |
| `cxl-lease` | the lease socket and `cxl-request` in the container |

## Documentation

- [How to setup a Kubernetes cluster with DRA enabled](../CLUSTER_SETUP.md)
- [How to deploy and use Intel CXL resource driver](USAGE.md): devices, classes, configurations,
  environment variables, example claims, flags
- [Local CXL memory devices: regions and slices on demand](LOCAL.md)
- [CXL memory pools: the cxl-pool.generic driver and the pool controller contract](POOL.md)
- [Leases: memory for running containers, the cxl-request CLI and its protocol](LEASES.md)
- Optional: [How to build Intel CXL resource driver container image and host binaries](BUILD.md)
