# Leases: memory for running containers

CAUTION: prototype. Interfaces may change.

A pod's claims are fixed when it is created, so a running container cannot
get more memory through its own claims. It can *lease* memory instead: the
node plugin allocates a device for a *companion pod* and lends it to the
container, as a NUMA node added to the container's `cpuset.mems` (mode
`numa`) or as an open file descriptor of a devdax device (mode `dax`). The
memory is a slice of a local memdev (`cxl.generic`, [LOCAL.md](LOCAL.md))
or a pool device (`cxl-pool.generic`, [POOL.md](POOL.md)), local first
unless the request names a source.

- [Opting in](#opting-in)
- [Flow](#flow)
- [cxl-request](#cxl-request)
- [Protocol](#protocol)
- [Companion objects](#companion-objects)
- [Life cycle](#life-cycle)
- [Limitations](#limitations)

## Opting in

kubelet-cxl-plugin (`--leases`, environment `LEASES`, on by default)
publishes the device `lease` in the node's `cxl.generic` ResourceSlice:
attribute `type: lease`, no capacity, `allowMultipleAllocations: true`, so
any number of pods can claim it. The DeviceClass `cxl-lease` of
[deployments/cxl/device-classes.yaml](../../deployments/cxl/device-classes.yaml)
selects it. A pod that may lease memory claims it through a template, so
that the claim follows the pod
([lease.yaml](../../deployments/cxl/examples/lease.yaml)):

```yaml
apiVersion: resource.k8s.io/v1
kind: ResourceClaimTemplate
metadata: {name: cxl-lease}
spec: {spec: {devices: {requests: [{name: lease, exactly: {deviceClassName: cxl-lease}}]}}}
---
apiVersion: v1
kind: Pod
metadata: {name: borrower}
spec:
  resourceClaims:
  - {name: lease, resourceClaimTemplateName: cxl-lease}
  containers:
  - name: app
    image: docker.io/library/python:3-alpine
    command: ["sleep", "infinity"]
    resources: {claims: [{name: lease}]}
```

Leases need NRI (the plugin lends nodes through NRI and maps callers to
containers) and therefore a host process ([USAGE.md](USAGE.md#deploy)).
Leasing local memory needs no pool driver; with `--pool-driver-name ""` a
pool lease fails with `pool driver disabled (--pool-driver-name)`.

NodePrepare of a claim on `lease` creates
`/var/lib/kubelet/plugins/cxl.generic/leases/<claim uid>/cxl.sock` (mode
0666; the directory 0755, labelled `container_file_t` when SELinux is
enabled) and returns for the allocation result of the lease device its
CDI device `generic/cxl=claim-<claim uid>-<n>` (spec file `cxl-claims`,
only the claim marker) and the CDI device `generic/cxl=lease-<claim uid>`
(spec file `cxl-leases`):

- bind mount (rbind, rw) of the directory at `/run/cxl`,
- bind mount (ro) of the static CLI (`--lease-cli`, default
  `/usr/local/bin/cxl-request` on the host) at
  `/usr/local/bin/cxl-request`; it is left out, with a warning, if the
  file does not exist,
- env `CXL_LEASE_SOCKET=/run/cxl/cxl.sock`.

Only containers that list the claim in `resources.claims` get them. The
directory, not the socket, is mounted, so a restarted plugin can create a
new socket in it that the running containers see.

## Flow

```
container: cxl-request request --size 512Mi
  -> plugin: authenticate the caller, store lease L                 (Creating)
  -> create ResourceClaim cxl-lease-L, then Pod cxl-lease-L          (Pending)
  -> the scheduler allocates a slice of a local memdev or, if none has
     room, a pool device (the pool controller attaches it, the binding
     condition releases the bind)
  -> kubelet prepares cxl-lease-L: a slice or a region, online movable,
     NUMA node N; the label cxl.generic/lease=L names the lease       (Lending)
  -> NRI UpdateContainers: cpuset.mems of THAT container = its own nodes + N
  -> response {"lease":"L","state":"Ready","node":N,...}              (Ready)
container: cxl-request release --lease L
  -> revoke (cpuset.mems shrinks, the kernel migrates the pages back)
  -> delete Pod and ResourceClaim cxl-lease-L                        (Releasing)
  -> kubelet unprepares (offline, delete the slice or destroy the region;
     a pool device is disabled, deallocated and detached)
  -> lease dropped                                                    (Released)
```

A lease in mode `dax` is the same, but nothing is lent through cgroups:
the plugin opens `/dev/daxX.Y` (O_RDWR, or O_RDONLY for a read-only
lease) and passes the open file descriptor to the container over the
socket (SCM_RIGHTS). The container needs no device node and no device
cgroup rule; the device cgroup is checked only at open().
`cxl-request request --exec CMD` (and `cxl-request fd --exec CMD`) runs
CMD with the fd as fd 3. A local `dax` lease gets a devdax slice of a
local memdev of its own, a pool one a whole exclusive device or a shared
device.

A read-only lease's fd allows mappings for reading only: a writable
mapping (`mmap` with `PROT_WRITE`, or `mprotect` to it) fails with EACCES,
a store into the mapping raises SIGSEGV, and reopening the fd through
`/proc/self/fd` is refused by the device cgroup, whatever the
capabilities of the container.

A shared pool device is `dax` only. `--shared` takes one of its `hosts`;
a request that names a shared device by `--serial` without `--shared`
takes all of them, so that nobody else has the device meanwhile (the
writer of single-writer sharing; the plugin reads the number of hosts
from the pool's ResourceSlice). A local slice is never shared through a
lease: several pods share it by referencing one ResourceClaim
([LOCAL.md](LOCAL.md#claims)).

A local lease waits for no attach and no binding condition: it is Ready a
few seconds after the request. A `numa` lease reports node N and the
slice or device size (`"node"`, `"size"`); a local slice grows N while
the lease is Ready.

## cxl-request

`cxl-request` is a static binary (see [BUILD.md](BUILD.md)) that talks to
`$CXL_LEASE_SOCKET`. The first word is the operation; every other input is
`--opt` or `--opt ARG` (`--opt=ARG` too); `--exec` ends the options, the
rest of the line is the command.

```
cxl-request request [--source local|pool] [--mode numa|dax] [--shared] [--read-only]
                    [--size SIZE] [--serial SERIAL] [--wait DURATION] [--request-id ID]
                    [--exec CMD [ARG...]]
cxl-request release (--lease ID | --all) [--wait DURATION]
cxl-request list
cxl-request fd  --lease ID [--wait DURATION] --exec CMD [ARG...]
cxl-request env --lease ID [--wait DURATION] [--exec CMD [ARG...]]
cxl-request help
```

| operation | |
|:----------|:-|
| `request` | create a lease, wait until it is Ready, print the response; with `--exec` run CMD with the lease's environment (and its fd for `dax`) |
| `release` | release a lease of the pod, or all of them, and wait until Released |
| `list` | print the leases of the pod |
| `fd` | get the fd of a Ready `dax` lease again and run CMD with it, for example after CMD restarted |
| `env` | print the lease's environment as `KEY=VALUE` lines, or run CMD with it |

| option | values, default | |
|:-------|:----------------|:-|
| `--source` | `local`, `pool`; omitted: local first, then pool | which devices |
| `--mode` | `numa`, `dax`; omitted: `numa`, or `dax` for a shared device | how the memory is given |
| `--shared` | default off | take one host of a shared pool device; without it a lease uses its device exclusively (all hosts of a shared device, or an exclusive device) |
| `--read-only` | default off | `dax`: the fd allows reads only |
| `--size` | quantity, for example `512Mi` | at least this much: a capacity request on exclusive devices, a size selector on shared devices |
| `--serial` | `0x` hex or decimal | this device |
| `--wait` | Go duration; omitted: the plugin's `--lease-wait` (`120s`); `0s`: do not wait | `request`, `fd`, `env`: until Ready; `release`: until Released |
| `--request-id` | string | the same id from the same container returns the same lease: repeat the whole command after a lost connection |
| `--lease` | 8 hex digits | the lease of `release`, `fd` and `env` |
| `--all` | | `release`: every lease of the pod |
| `--exec` | CMD ARG... | run CMD with the lease's environment |

The defaults are the safe ones: exclusive, read-write, `numa`, local
first, then pool.
A request needs `--size`, `--serial` or `--shared`.

`cxl-request` refuses these command lines with exit code 2 before
anything is sent; the plugin checks requests from other clients the same
way and answers `{"error": ...}`:

| case | message |
|:-----|:--------|
| `--source` other than `local`, `pool` | `invalid source "..." (local or pool)` |
| `--mode` other than `numa`, `dax` | `invalid mode "..." (numa or dax)` |
| `--shared` with `--mode numa` | `shared memory is never onlined as RAM: use mode dax` |
| `--shared` with `--source local` | `local devices are not shareable; share a ResourceClaim between pods instead` |
| `--read-only` without `--mode dax` or `--shared` | `readOnly needs mode dax` |
| `request` without `--shared`, `--serial` and `--size` | `size or serial is required` |
| `--serial` not a number | `invalid serial "..."` |
| `--exec` with `release` or `list` | `--exec is for request, fd and env` |
| `fd` without `--exec` | `fd needs --exec` |
| `fd` or `env` without `--lease` | `fd needs --lease ID`, `env needs --lease ID` |
| `release` with neither or both of `--lease` and `--all` | `release takes --lease ID or --all` |
| `--wait` not a duration, or negative | `invalid wait "..."` |

The plugin refuses further requests that only it can judge:

| case | message |
|:-----|:--------|
| `--size` not a positive quantity | `invalid size "..."` |
| `--shared` with the serial of a device that is not shared (an exclusive pool device, a local memdev) | `device 0x...: not shareable` |
| `--source local` with the serial of a shared device | `device 0x...: not a local device` |
| `--mode numa` with the serial of a shared device | `shared memory is never onlined as RAM: use mode dax` |
| the serial of a shared device without `--shared` (all its hosts), when its ResourceSlice has no `hosts` capacity | `device <name> (serial 0x...) has no "hosts" capacity` |
| a pool request (`--source pool` or `--shared`) while the pool driver is disabled | `pool driver disabled (--pool-driver-name)` |
| any request while NRI is not connected | `lending unavailable: NRI not connected` |
| `fd` on a lease in mode `numa` | `lease <id> is not a dax lease; use env` |

A serial that names a shared device makes the mode `dax` when `--mode`
is omitted; the CLI cannot know that, so `--read-only` needs `--shared`
or `--mode dax` with it. The plugin's refusals are a JSON line with
`error` on stdout (on stderr for `env` and with `--exec`) and exit code 1,
3 for `fd` on a `numa` lease.

Unknown operations and options, an option of another operation, and an
option without its value are usage errors too. `help` (or `-h`,
`--help`) prints the usage.

Output: `request`, `release` and `list` print the plugin's JSON response
line. `env` prints the variables of the lease
([USAGE.md](USAGE.md#environment-variables), with `CXL_<KEY>_DAX` for
`dax`) and `CXL_LEASE=<id>` as `KEY=VALUE` lines, sorted, so that
`eval "$(cxl-request env --lease ID)"` works. Without `--exec` the fd of a
`dax` lease is not kept.

With `--exec`, CMD runs once the lease is Ready, with the environment of
`cxl-request` plus the variables of the lease and `CXL_LEASE=<id>`. For
`request --exec` and `fd --exec` of a `dax` lease, the fd is fd 3 of CMD
and `CXL_<KEY>_FD=3` takes the place of `CXL_<KEY>_DAX`; `env --exec`
passes no fd and keeps `CXL_<KEY>_DAX`. A `numa` lease has no fd: CMD
gets `CXL_<KEY>_NODE` and the rest. CMD's exit code is passed through
(128+signal when it is killed by a signal), and SIGTERM, SIGINT, SIGHUP
and SIGQUIT to `cxl-request` are forwarded to CMD.

Exit codes: 0 ok; 1 error (no socket, connection, invalid response, an
error from the plugin, CMD could not start); 2 usage; 3 the lease is
Failed, not Ready within `--wait` (`request`, `fd`, `env`; with
`--exec` CMD is not started), not Released within `--wait` (`release`),
or `fd` on a lease that is not `dax`. Without `--exec`,
`request --wait 0s` exits with 0 whatever the state of the lease, unless
it is Failed. With `--exec` it exits with 3 unless the lease is Ready at
once. Without
`$CXL_LEASE_SOCKET`, `cxl-request` exits with 1 and
`CXL_LEASE_SOCKET is not set (the pod needs a claim on DeviceClass cxl-lease)`.

Examples, with the local memdev `0xc100e2e1`, the exclusive pool device
`0xc1ee0001` and the shared pool device `0xc1ae0001`:

```
cxl-request request --size 512Mi
  -> {"lease":"1a2b3c4d","state":"Ready","source":"local","mode":"numa","shared":false,"readOnly":false,
      "size":536870912,"serial":"0xc100e2e1","node":2,"env":{"CXL_C100E2E1_MODE":"numa",...}}
cxl-request request --source pool --size 512Mi --mode dax --exec python3 app.py
  -> app.py sees CXL_C1EE0001_FD=3 CXL_C1EE0001_MODE=dax CXL_C1EE0001_SIZE=536870912
     CXL_C1EE0001_SERIAL=0xc1ee0001 CXL_C1EE0001_SHARED=0 CXL_C1EE0001_READONLY=0 CXL_LEASE=1a2b3c4d
cxl-request request --source local --size 64Mi --mode dax --read-only --exec prog
cxl-request request --serial 0xc1ae0001                       # the shared device, exclusively (writer)
cxl-request request --serial 0xc1ae0001 --shared --read-only --exec prog   # a reader
cxl-request request --shared --size 256Mi                     # any shared device of 256Mi or more
cxl-request request --size 1Gi --wait 0s --request-id r1      # asynchronous: Pending
cxl-request env --lease 1a2b3c4d                              # KEY=VALUE lines (DAX, not FD)
cxl-request fd --lease 1a2b3c4d --wait 60s --exec prog        # the fd again after a restart of prog
cxl-request env --lease 1a2b3c4d --exec numactl --membind=2 prog   # a numa lease: CXL_C100E2E1_NODE=2
cxl-request list
cxl-request release --lease 1a2b3c4d
cxl-request release --all
```

## Protocol

`SOCK_STREAM` unix socket, one JSON object per line in each direction; a
connection may carry several requests. The fields of a request mirror the
options of `cxl-request`; all are optional except `op`. Requests are
decoded strictly: an unknown field is answered with
`invalid request: json: unknown field "..."`, an unknown `op` with
`unknown op "..." (request, release, list, fd or env)`.

| field | values |
|:------|:-------|
| `op` | `request`, `release`, `list`, `fd`, `env` |
| `source` | `local`, `pool`; omitted: local first, then pool |
| `mode` | `numa`, `dax` |
| `shared` | bool |
| `readOnly` | bool |
| `size` | quantity string |
| `serial` | string, `0x` hex or decimal |
| `wait` | Go duration string |
| `requestId` | string |
| `lease` | lease id |
| `all` | bool |

For example:

| request | |
|:--------|:-|
| `{"op":"request","size":"512Mi","wait":"120s","requestId":"r1"}` | 512Mi, local first, then pool, lent as a NUMA node |
| `{"op":"request","source":"pool","size":"512Mi","mode":"dax","readOnly":true}` | an exclusive pool device as dax, read-only fd passed |
| `{"op":"request","source":"local","size":"64Mi","mode":"dax"}` | a dax slice of a local memdev, fd passed |
| `{"op":"request","source":"local","size":"64Mi","serial":"0xc100e2e1"}` | memory of the device with the serial |
| `{"op":"request","serial":"0xc1ae0001","shared":true}` | one host of a shared device, fd passed |
| `{"op":"request","serial":"0xc1ae0001"}` | the shared device, nobody else meanwhile, fd passed |
| `{"op":"release","lease":"1a2b3c4d","wait":"120s"}` | release one lease of the pod |
| `{"op":"release","all":true}` | release all leases of the pod |
| `{"op":"list"}` | leases of the pod |
| `{"op":"fd","lease":"1a2b3c4d"}` | send the fd of a Ready dax lease again |
| `{"op":"env","lease":"1a2b3c4d"}` | the lease with its environment |

Response (`shared` and `readOnly` are always present, the other fields
only when they have a value):

| field | |
|:------|:-|
| `lease` | id, 8 hex digits |
| `state` | `Creating`, `Pending`, `Lending`, `Ready`, `Releasing`, `Failed`, `Released` (in responses only: the lease is gone) |
| `source` | `local` or `pool`: the requested one, the allocated one once Ready; omitted while undecided |
| `mode` | `numa` or `dax`, effective |
| `shared` | bool, effective: `false` for a lease that uses a shared device exclusively |
| `readOnly` | bool |
| `size` | bytes: once Ready the size of the slice or the device (a pool device may be larger than requested), the requested size before |
| `serial` | `0x` hex, once known |
| `node` | NUMA node, mode `numa` once Ready |
| `dax` | `/dev/daxX.Y`, mode `dax` once Ready; informational, the container has no such device node |
| `env` | once Ready: the variables a claim of this memory would inject ([USAGE.md](USAGE.md#environment-variables)), with `DAX` for `dax` leases |
| `error` | message |
| `leases` | list of responses, for `list` and `release` with `all` |

The response of a Ready `dax` lease to `request` and `fd` carries the dax
fd as SCM_RIGHTS on the same write.

- `wait` blocks until Ready or Failed (`release`: until Released). At the
  timeout of a `request` the lease is released and reported Failed
  `timeout: not Ready after <wait>`; `fd` and `env` report the state at
  their timeout and keep the lease. `"wait":"0s"` returns the current
  state at once and keeps the lease.
- `requestId`: a repeated request with the same id from the same
  container returns the same lease (a retry after a lost connection).
- A release waits up to `wait` for the companion objects to disappear and
  answers Released, or Releasing if they are still going.
- `fd` serves the container that requested the lease only; `env`,
  `list` and `release` serve every container of the pod.
- Errors: the validation messages of [cxl-request](#cxl-request),
  `not a container of the claim's pod`, `no such lease` (`release`),
  `no such lease of this container` (`fd`, `env`), and the errors of the
  lend. A lend that fails (for example the container is memory-steered,
  see [Limitations](#limitations), or not running) makes the lease Failed
  with that message, and it is released.

Caller authentication: the socket identifies the lease claim, and the
claim's `reservedFor` the pod. SO_PEERCRED gives the caller's pid; the
NRI plugin maps it through `/proc/<pid>/cgroup` to a container. The
container's pod must be a pod of the claim. A lease is bound to that pod
and container: memory is lent to the calling container only; any
container of the same pod with the socket may list and release it.

## Companion objects

The companion objects of lease `<id>` live in the requester's namespace:

- ResourceClaim `cxl-lease-<id>` with one request `lease`:
  - one source: `exactly` with the class of the source;
  - no `source`: `firstAvailable: [{name: local, ...}, {name: pool, ...}]`,
    without `local` for a shared device (`--shared`, or a `--serial` of a
    shared device: `cxl.generic` publishes no shared device) and without
    `pool` when the pool driver is disabled; a single source left is an
    `exactly` request.

  The class is `cxl-local` for local memory, `cxl-pool` for an exclusive
  pool device and `cxl-pool-shared` for a shared one (when `--serial` names
  a device, the plugin reads from the ResourceSlices whether it is shared
  and how many hosts it has). The selectors are
  `serial == "0x..."` when a serial is given and `size >= <bytes>` for a
  shared device when a size is given. The capacity request is `memory:
  <size>` on exclusive devices, `hosts: <all>` when a shared device is
  taken exclusively, and none for a shared lease (one host). Every source
  driver gets a `DeviceConfig` with the effective `mode` and `readOnly`,
  so that the claim shows what was asked; in a `firstAvailable` request
  each applies to its subrequest (`requests: [lease/local]`,
  `[lease/pool]`). The claim has an ownerReference to the requester pod
  (controller: true).
- Pod `cxl-lease-<id>`: ownerReference to the requester pod, required
  node affinity `metadata.name In [<node>]`, `tolerations: [{operator: Exists}]`,
  restartPolicy Always, `automountServiceAccountToken: false`,
  `enableServiceLinks: false`, `terminationGracePeriodSeconds: 1`, one
  container `pause` (`--lease-companion-image`,
  default `registry.k8s.io/pause:3.10`) with requests = limits cpu 100m,
  memory 16Mi (Guaranteed; runc init needs about 9 MiB, and with 1m CPU
  the pause container takes about 12 s to start), no `resources.claims`,
  and `spec.resourceClaims: [{name: lease, resourceClaimName: cxl-lease-<id>}]`.
  kubelet prepares a pod's claims even if no container references them.
  Both drivers return no CDI device for a companion claim, so nothing of
  its memory reaches the pause container, which is not steered either.
- Both carry the labels `cxl.generic/lease=<id>`,
  `cxl.generic/requester-uid=<pod uid>`, `cxl.generic/node=<node>` and
  the annotation `cxl.generic/requester=<ns>/<pod>/<container>`.

A claim with the label `cxl.generic/lease` is a companion claim. Before
it touches a device, the driver that prepares it checks that the lease is
in the lease table and is being created (or Lending or Ready, for a
prepare again after a restart), that the claim's name is
`cxl-lease-<id>` and its namespace the requester pod's, and that the
claim has an ownerReference to the requester pod. A claim that carries
the label and fails a check fails its Prepare with
`claim <ns>/<name>: not the companion of lease <id>` (kubelet retries; a
pod that references such a claim never starts); the companion of a lease
that is Releasing or Failed fails with `lease <id> is <state>`. With
`--leases=false` every companion claim fails with
`lease <id>: lending to running containers is disabled (--leases=false)`.
So a tenant can create a claim with the label only in its own namespace,
and cannot make it the companion of a lease it did not request.

## Life cycle

- The lease table `leases.json` in the `cxl.generic` plugin data
  directory is written before any object is created. The driver that
  prepared the companion claim stores the lend with it
  (`preparedPoolClaims.json` of the pool driver, `preparedClaimsInfo.json`
  of `cxl.generic`), so it can revoke after a restart. A lease table that
  cannot be decoded (strictly) is logged, and the plugin starts with an
  empty one.
- Requester container stops: if the pod is being deleted, finished, gone
  or has restartPolicy Never, its leases are released. Otherwise they
  are kept, and `numa` lends are applied again when the container starts
  again (a dax fd is gone with the process: get it again with
  `cxl-request fd --lease ID --exec ...`).
- Requester pod deleted: the companion pods are garbage collected
  (ownerReference); NodeUnprepare of the lease claim starts the release of
  all leases of the pod, waits until their nodes are revoked and the
  deletion of their companions is requested, and fails with "still
  releasing N leases, retry" (kubelet retries) if that takes longer than
  5 s. It does not wait for the companions to be gone: kubelet unprepares
  the companion claim of a local lease through the same driver
  (`cxl.generic`), one call at a time. A lease stays Releasing until its
  companion pod and claim are gone, also across a plugin restart.
- Companion pod deleted or evicted behind the plugin's back: the driver
  of the device revokes before it releases the device; the lease becomes
  Failed and is dropped by the reconcile.
- Reconcile, at start (after NRI synchronized) and every 60 s: restarts
  interrupted releases, releases leases left in Creating, leases whose
  companion pod is gone or finished and leases whose requester will not
  come back, applies the `numa` lends of Ready leases again (this is how a
  plugin restart restores them), and deletes the pods and claims labelled
  `cxl.generic/node=<node>` and `cxl.generic/lease` that are named
  `cxl-lease-<value of the lease label>` and have no lease in the table.
- Plugin restart: the sockets of the lease claims are created again in
  the mounted directories, leases continue.

## Limitations

- Memory limits: lent memory counts against the container's `memory.max`
  and the pod cgroup. Without a raise the extra node adds locality, not
  room. Use requester pods without a memory limit, or raise it with
  `pods/resize`. Node allocatable never includes pool memory.
- cpuset.mems has one owner: the lend goes through NRI UpdateContainers.
  A node is lent per lease: memory devices under one CXL host bridge share
  a NUMA node, and when two leases of a container hold the same node, it
  stays in the container's cpuset.mems until the last of them is released.
  A container that has a claim of `cxl.generic` with memory in mode
  `numa` (a `numa` memdev slice, a region or DRAM: memory steered by
  cgmpolmgr, [USAGE.md](USAGE.md#steering)) cannot take a `numa` lease:
  the lend is refused with "container is memory-steered by a local CXL
  claim; lending is not supported together with steering yet", and the
  lease becomes Failed. `dax` leases lend no node and are not affected.
  Both components would own the container's cpuset.mems, and cgmpolmgr
  writes cgroupfs directly, which the next NRI update reverts.
  nri-resource-policy or the kubelet memory manager would conflict the
  same way. A movable node is never the only node in cpuset.mems.
- Release is cooperative. Releasing a `dax` lease while the device is
  still mapped makes the holder get SIGBUS at its next access; munmap
  first. A passed fd cannot be revoked short of release, and can be
  duplicated or passed on inside the pod. A `numa` release blocks
  (kubelet retries, the companion stays Terminating) while the pages
  cannot move.
- Latency: about 5-7 s per pool request (the scheduler's binding
  condition poll), about 8 s for a release. A local lease takes a few
  seconds.
- RBAC: the plugin creates and deletes pods and resourceclaims in every
  namespace ([deployments/cxl/base/resource-driver.yaml](../../deployments/cxl/base/resource-driver.yaml));
  companions count against the tenant's ResourceQuota. No admission
  policy limits what the plugin's account may create to companions on its
  own node.
- The plugin must run in the host pid namespace with the NRI socket (as a
  host process; the shipped DaemonSet has neither). SELinux labels of the
  socket are set best effort and unverified.
- Leases follow the pod object: a finished but undeleted pod keeps its
  leases until its lease claim is unprepared.
- Do not force-delete a companion pod while it holds a device (the force
  deletion hazard of [POOL.md](POOL.md#ordering-and-failure-semantics)).
