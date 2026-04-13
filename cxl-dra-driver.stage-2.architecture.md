# Architecture: CXL Fabric-Attached Memory with DRA

Architectural plan for implementing CXL DRA Driver Stage 2: driver-managed CXL Memory Pool Server provisioning and node memory hotplug/hotremove.

## Problem

A pod requires 1 TB of CXL memory. The node doesn't have it locally, but a 64 TB CXL memory pool is reachable over the fabric. The driver must:
1. Advertise fabric memory as allocatable resources
2. When a pod is scheduled, hotplug the requested CXL memory to the target node
3. Ensure only the requesting container can use the hotplugged memory
4. Release and detach memory when the pod terminates

## Research Findings

### CSI Architecture (Storage Parallel)

CSI solved the "provision from external pool" problem with a **controller/node plugin split**:

- **Controller plugin** (centralized): `CreateVolume()` provisions storage from a backend pool, `ControllerPublishVolume()` attaches it to a node. Runs as a Deployment with sidecars (external-provisioner, external-attacher).
- **Node plugin** (per-node DaemonSet): `NodePublishVolume()` mounts the provisioned storage into a pod.
- **Late binding** (`WaitForFirstConsumer`): scheduler picks the node first, then provisioning happens for that specific node. `CSIStorageCapacity` CRDs let the scheduler filter nodes by available capacity.
- **Key pattern**: Scheduler decides placement → Controller provisions for that node → Node plugin prepares for the pod.

### DRA Architecture (Kubernetes 1.37)

DRA has evolved differently from CSI. Key findings:

**KEP-4381 (Structured Parameters)** — the current/only model (KEP-3063 control-plane controller was **withdrawn** in K8s 1.32):
- **Scheduler allocates deterministically** from published ResourceSlices using CEL expressions
- **No driver-side allocation controller** — the scheduler is the allocator
- Drivers publish `ResourceSlice` objects describing devices with attributes and capacities
- Scheduler selects specific devices, writes allocation to `ResourceClaim.Status.Allocation`

**Network-attached device support** is explicitly designed into DRA:
- `ResourceSlice.Spec.Pool` supports `NodeSelector` (not just `NodeName`) — devices available to matching nodes
- `AllNodes: true` for devices available everywhere
- `PerDeviceNodeSelection: true` for per-device placement control

**BindingConditions** — the mechanism for external provisioning:
- ResourceSlice devices can declare `BindingConditions: ["Provisioned"]`
- Scheduler allocates the device but **defers pod binding** until the driver sets the condition to True in `AllocatedDeviceStatus.Conditions[]`
- This lets a driver trigger external provisioning after allocation but before the pod starts
- `BindingFailureConditions: ["ProvisioningFailed"]` for error signaling

**NodePrepareResources** — the per-node provisioning hook:
- Called by kubelet after scheduling; driver can hotplug/attach devices here
- Must be idempotent (kubelet may retry)
- KEP guidance: "do as little work as possible" — heavy provisioning should happen during allocation phase
- Returns CDI device IDs for container runtime injection

**Dynamic ResourceSlice updates**:
- Driver increments `Pool.Generation`, updates ResourceSlices
- Scheduler uses only the highest-generation slices
- `resourceslice.Controller.Update()` handles create/update/delete automatically

### NVIDIA DRA Driver Patterns

NVIDIA's DRA driver (`kubernetes-sigs/dra-driver-nvidia-gpu`) provides two models:

**GPU kubelet plugin** — pure node-local (like our current CXL driver):
- Discovers local GPUs from NVML
- Publishes per-node ResourceSlices
- NodePrepareResources just validates and returns CDI IDs
- No controller, no external provisioning

**ComputeDomain kubelet plugin** — closer to fabric model:
- Manages Multi-Node NVLink (MNNVL) across nodes
- Has a `ComputeDomainManager` that coordinates cross-node resources
- Explicitly excludes channel devices from node-local ResourceSlices (published "from the control plane" instead)
- Uses `kubeletplugin.Serialize(false)` to handle codependent prepare() calls
- Retries with work queue and timeout (45s) in PrepareResourceClaims
- Key insight: even NVIDIA's cross-node model still uses node-local kubelet plugins, with coordination happening through Kubernetes API objects

### CXL Fabric Memory Hotplug Sequence

When CXL memory is attached from fabric to a host:
1. **CXL device bind** → `cxl/mem<N>` appears in sysfs
2. **CXL region creation** → kernel creates `region<N>`
3. **Memory block onlining** → kernel brings blocks online
4. **NUMA node appears** → `/sys/devices/system/node/node<N>`

Our UdevEventWatcher already handles all of these with debounce. The key gap is: **who triggers step 1?**

## Proposed Architecture

### Component Overview

```
┌────────────────────────────────────────────────────────────────┐
│ Control Plane                                                  │
│                                                                │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ cxl-fabric-controller (NEW — Deployment, 1 replica)      │  │
│  │                                                          │  │
│  │  Watches: ResourceClaims for "cxl-fabric.intel.com"      │  │
│  │  Publishes: ResourceSlices for fabric memory pool        │  │
│  │  Talks to: CXL Fabric Manager API                        │  │
│  │                                                          │  │
│  │  On allocation (scheduler wrote claim.status.allocation):│  │
│  │    1. Call Fabric Manager: attach <size> to <node>        │  │
│  │    2. Wait for attachment confirmation                    │  │
│  │    3. Set BindingCondition "Provisioned" = True           │  │
│  │       on AllocatedDeviceStatus                            │  │
│  │                                                          │  │
│  │  On deallocation:                                         │  │
│  │    1. Call Fabric Manager: detach <device> from <node>     │  │
│  │    2. Clear BindingCondition                               │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                │
│  ┌──────────────────────┐                                      │
│  │ Kubernetes Scheduler │                                      │
│  │  Evaluates CEL       │                                      │
│  │  selectors against   │                                      │
│  │  fabric pool devices │                                      │
│  └──────────────────────┘                                      │
└────────────────────────────────────────────────────────────────┘
         │                            │
         │ ResourceSlices             │ ResourceClaim.Status
         │ (fabric pool devices)      │ (allocation + conditions)
         ▼                            ▼
┌────────────────────────────────────────────────────────────────┐
│ Node (kubelet-cxl-plugin DaemonSet — EXTENDED)                 │
│                                                                │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ kubelet-cxl-plugin (existing + extended)                  │  │
│  │                                                          │  │
│  │  Publishes: ResourceSlices for LOCAL CXL + DRAM          │  │
│  │  (existing behavior, unchanged)                          │  │
│  │                                                          │  │
│  │  NodePrepareResources (extended):                         │  │
│  │    If claim is for fabric memory:                         │  │
│  │      1. Wait for hotplugged NUMA node to appear           │  │
│  │         (UdevEventWatcher detects it)                     │  │
│  │      2. Verify memory size matches claim                  │  │
│  │      3. Configure memory policy (cgmpolmgr)               │  │
│  │      4. Return CDI device IDs binding container to        │  │
│  │         the hotplugged NUMA node                          │  │
│  │                                                          │  │
│  │  NodeUnprepareResources (extended):                        │  │
│  │    If claim is for fabric memory:                         │  │
│  │      1. Stop cgmpolmgr for the container                  │  │
│  │      2. Signal ready for detach (node side)                │  │
│  │                                                          │  │
│  │  UdevEventWatcher: detects hotplugged CXL memory          │  │
│  │  rescanAndPublish: updates LOCAL ResourceSlices            │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                │
│  ┌───────────────────────────────┐                             │
│  │ CXL Fabric Manager Agent     │                             │
│  │ (vendor-specific, out of     │                             │
│  │  scope — could be a daemon   │                             │
│  │  or PCIe switch firmware)    │                             │
│  └───────────────────────────────┘                             │
└────────────────────────────────────────────────────────────────┘
```

### Lifecycle: Pod Requests 1 TB CXL Fabric Memory

```
1. SETUP (one-time)
   cxl-fabric-controller starts, queries Fabric Manager for pool inventory.
   Publishes ResourceSlice:
     pool: "cxl-fabric"
     nodeSelector: {matchLabels: {cxl-fabric: "connected"}}
     devices:
       - name: "fabric-slot-0"
         capacity: {memory: "1Ti"}
         attributes: {type: "cxl-fabric", fabricId: "fab-001"}
         bindingConditions: ["Provisioned"]
       - name: "fabric-slot-1" ...
       ... (64 slots × 1 TB each)

2. USER creates ResourceClaim
   requests: [{name: "my-memory", selector: "capacity.memory >= 1Ti"}]
   → references DeviceClass "cxl-fabric.intel.com"

3. SCHEDULER allocates
   Evaluates CEL selectors against fabric pool ResourceSlices.
   Picks "fabric-slot-7" for the claim.
   Writes claim.status.allocation = {devices: [{pool: "cxl-fabric",
     device: "fabric-slot-7", ...}]}
   But does NOT bind the pod yet — bindingCondition "Provisioned"
   is not satisfied.

4. cxl-fabric-controller SEES allocation
   Watches claim.status.allocation changes.
   Calls Fabric Manager API: "attach fabric-slot-7 to node worker-3"
   Fabric Manager programs the CXL switch / fabric.
   CXL memory device appears on worker-3 (PCIe hotplug).
   Controller sets AllocatedDeviceStatus condition:
     {type: "Provisioned", status: "True"}

5. SCHEDULER proceeds with binding
   Sees all bindingConditions satisfied → binds pod to worker-3.

6. KUBELET on worker-3 calls NodePrepareResources
   kubelet-cxl-plugin receives the claim.
   UdevEventWatcher has already detected the new NUMA node.
   Plugin verifies the NUMA node matches the fabric device.
   Creates CDI spec binding container to the new NUMA node.
   Starts cgmpolmgr to enforce memory policy.
   Returns CDI device IDs → container runtime injects them.

7. POD RUNS
   Container sees the 1 TB NUMA node.
   Memory allocations directed to it via memory policy.

8. POD TERMINATES
   Kubelet calls NodeUnprepareResources.
   Plugin stops cgmpolmgr, removes CDI spec.
   cxl-fabric-controller sees claim deallocated.
   Calls Fabric Manager: "detach fabric-slot-7 from worker-3"
   CXL memory device disappears (PCIe hot-remove).
   UdevEventWatcher detects removal, rescanAndPublish updates
   local ResourceSlices.
   Controller marks fabric-slot-7 as available again.
```

### Key Design Decisions

#### 1. Two Drivers, One DriverName or Two?

**Recommended: Two separate driver names.**

| Component | Driver Name | Scope |
|-----------|-------------|-------|
| kubelet-cxl-plugin (existing) | `cxl.intel.com` | Node-local CXL + DRAM |
| cxl-fabric-controller (new) | `cxl-fabric.intel.com` | Fabric pool memory |

Rationale: The scheduler treats ResourceSlices from different drivers independently. A pod can request both local and fabric memory via separate claims. The kubelet plugin handles NodePrepare for both driver names (it registers for both).

Alternative: Single driver name with both node-local and fabric pools. Simpler but tighter coupling.

#### 2. How Does the Controller Know Which Node?

After scheduler writes `claim.status.allocation`, the allocation includes a `NodeSelector` or the claim is bound to a specific node. The controller reads this to determine which node to provision memory for.

More precisely: the scheduler runs the `Reserve` phase, selects devices, and writes the allocation. The pod is bound to a node. The controller watches for claims whose allocation references its devices and whose pod has been assigned to a node.

#### 3. Memory Isolation: Only the Requesting Container Uses It

The existing CDI + cgmpolmgr mechanism handles this:

- **CDI device injection**: Container runtime makes only the hotplugged NUMA node visible/accessible to the container
- **Memory policy enforcement**: cgmpolmgr binds process memory allocations to the specific NUMA node(s)
- **cgroup memory limits**: Standard Kubernetes cgroup enforcement limits total consumption

No other container on the node should use the hotplugged memory because:
- It's not in any other container's memory policy
- The kernel's default policy won't use a new NUMA node unless explicitly requested
- cgmpolmgr can be configured with an exclusive waypoint

#### 4. What Needs to Change in kubelet-cxl-plugin

| Change | Description |
|--------|-------------|
| NodePrepare extension | Handle claims from `cxl-fabric.intel.com` — wait for hotplugged device, verify, prepare |
| Register for fabric driver | `kubeletplugin.Start()` also registers as handler for `cxl-fabric.intel.com` |
| Fabric claim awareness | Distinguish local vs fabric claims in Prepare/Unprepare |
| Signal coordination | After fabric device appears (udev), match it to a pending fabric claim |

#### 5. The Fabric Manager API (Out of Scope)

The CXL Fabric Manager is vendor-specific hardware/firmware. The controller needs a Go client for it. The API surface needed:

```go
type FabricManager interface {
    // ListPools returns available memory pools with sizes
    ListPools(ctx context.Context) ([]MemoryPool, error)
    // Attach provisions memory from a pool to a target node
    Attach(ctx context.Context, poolID string, size uint64, targetNode string) (AttachResult, error)
    // Detach releases provisioned memory from a node
    Detach(ctx context.Context, attachmentID string) error
    // Status returns current attachment state
    Status(ctx context.Context, attachmentID string) (AttachmentStatus, error)
}
```

Implementations would exist for specific fabric hardware (e.g., CXL switch vendors, Intel fabric managers).

### What Already Exists vs What's New

| Component | Status | Notes |
|-----------|--------|-------|
| kubelet-cxl-plugin | ✅ Exists | Node-local CXL/DRAM discovery, DRA integration |
| UdevEventWatcher | ✅ Exists | Detects hotplug via udev with debounce |
| rescanAndPublish | ✅ Exists | Updates ResourceSlices on hardware changes |
| scanDevices | ✅ Exists | Single scan pipeline |
| cgmpolmgr | ✅ Exists | Memory policy enforcement per container |
| cxl-fabric-controller | ❌ New | Central controller for fabric pool management |
| Fabric Manager client | ❌ New | Go client for vendor-specific fabric API |
| BindingConditions support | ❌ New | Controller sets conditions on allocated claims |
| Multi-driver registration | ❌ New | kubelet plugin handles both local + fabric claims |
| Fabric claim handling in NodePrepare | ❌ New | Wait for hotplug, match to claim, prepare |

### Risks and Open Questions

1. **BindingConditions maturity**: This API is in Kubernetes 1.37 alpha. Need to verify it's stable enough to build on.

2. **Hotplug timing**: Between controller calling Fabric Manager and the NUMA node appearing could take seconds to minutes. NodePrepare must handle this gracefully (poll with timeout).

3. **Failure modes**: What if fabric attach fails? Controller must set BindingFailureCondition. What if it succeeds but the node plugin can't see the memory? Need reconciliation.

4. **Memory scrubbing**: When fabric memory is detached and reattached to a different node, the previous tenant's data may remain. Need zeroing/scrubbing at the fabric level.

5. **Scalability**: A 64 TB pool as 64 × 1 TB slots = 64 devices in ResourceSlice. Reasonable. But finer granularity (e.g., 1 GB slices = 65,536 devices) may hit ResourceSlice size limits.

6. **Partial allocation**: Can a pod request 500 GB from a 1 TB slot? The structured parameters model supports capacity-based allocation with `CountRange` and capacity attributes.

7. **Multi-node interleaving**: What if a workload needs memory interleaved across multiple fabric-attached CXL devices? This maps to multiple claims with a topology constraint.

## Summary

The architecture follows CSI's proven controller/node split pattern, adapted for DRA's structured parameters model:

- **cxl-fabric-controller** (new): Publishes fabric pool as ResourceSlices, provisions memory after scheduler allocation, uses BindingConditions to gate pod startup
- **kubelet-cxl-plugin** (extended): Handles NodePrepare for fabric claims by waiting for hotplugged NUMA nodes and configuring memory isolation
- **Scheduler**: Makes allocation decisions using CEL against structured device attributes — no custom scheduling logic needed
- **Fabric Manager**: Vendor-specific API for actual CXL switch programming — abstracted behind a Go interface

The existing udev watching, rescan, and cgmpolmgr infrastructure provides the node-side foundation. The main new work is the fabric controller and the coordination between allocation → attachment → preparation.
