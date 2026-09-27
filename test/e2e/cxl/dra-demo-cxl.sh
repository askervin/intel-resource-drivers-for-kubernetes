#!/bin/bash

# This is a demo script to show how to use DRA with a CXL memory
# driver, using the kubelet-cxl-plugin as an example driver.
#
# The script connects to a server that is running k8s single-node
# cluster and has CXL memory attached.
#
# The script runs commands to deploy the driver, create resource
# claims and pods that use them, and verify that the driver is
# working.
#
# Kubernetes v1.37 is the minimum supported version.
#
# One way to setup such a server in a virtual machine is using
# nri-plugins e2e tests:
#
# git clone https://github.com/containers/nri-plugins
# cd nri-plugins/test/e2e
# ./run_tests.sh memory.test-suite/memory-policy/n4-cxl
#
# Usage:
#
# vm=n4-cxl-fedora-43-containerd ./dra-demo-cxl.sh
#
# Environment variables:
#
#   vm            host to ssh into, required
#   DEBUG=1       print commands executed in the vm
#   INTERACTIVE=1 drop to an interactive prompt before cleanup
#   SKIP_CLEANUP=1 leave the namespace, claims and driver running

NAMESPACE="dra-demo-cxl"

# Feature gates the demo needs but that are off by default, in
# kube-apiserver --feature-gates=.... command line syntax. The demo
# also needs DRAConsumableCapacity and DRAPartitionableDevices, which
# are enabled by default.
FEATURE_GATES="DRANodeAllocatableResources=true"

# Memory amounts requested in the resource claims. The CXL requests
# must fit in a single CXL region (device) on the node, and their sum
# must fit in the total CXL capacity published by the driver.
CXL_MEM_SMALL="32Mi"   # cxl-memory-claim
CXL_MEM_MEDIUM="64Mi"  # dram-then-cxl-claim, cxl-dram-interleave-claim
DRAM_MEM_SMALL="64Mi"  # cxl-dram-interleave-claim
DRAM_MEM_MEDIUM="128Mi" # dram-then-cxl-claim
DRAM_MEM_LARGE="1Gi"   # sys-memory-claim

SSH_OPTS="-o StrictHostKeyChecking=No -o ControlMaster=auto -o ControlPersist=120 -o ControlPath=~/.ssh/%r@%h-%p"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR=$(cd "$SCRIPT_DIR"; git rev-parse --show-toplevel)

CXL_DRIVER_BIN="$PROJECT_DIR/bin/kubelet-cxl-plugin"
CXL_DRIVER_NAME="$(basename "$CXL_DRIVER_BIN")"

TMPFILE_CMD="$SCRIPT_DIR/output/dra-demo-cxl/vmsh.output"

if [[ "$DEBUG" == "1" ]]; then
    VM_SSH_DEBUG_PREFIX="set -x; "
fi

# Sections below run in subshells, where a plain "exit 1" would only
# end the subshell. Signal the main shell so that error() always stops
# the whole script.
MAIN_PID=$$
trap 'exit 1' USR1

error() {
    echo "dra-demo-cxl.sh error: $*" >&2
    [[ "$BASHPID" == "$MAIN_PID" ]] || kill -s USR1 "$MAIN_PID"
    exit 1
}

[[ -x "$CXL_DRIVER_BIN" ]] || error "CXL driver binary not found or not executable: $CXL_DRIVER_BIN
build it with: cd $PROJECT_DIR && go build -o bin/kubelet-cxl-plugin ./cmd/kubelet-cxl-plugin"

mkdir -p "$(dirname $TMPFILE_CMD)"; echo > "$TMPFILE_CMD" || error "cannot write to temp file $TMPFILE_CMD"

section() {
    echo ""
    echo "##########################################"
    echo "# $1"
    echo ""
}

log() {
    echo "$@" >&2
}

interactive() {
    local target="$vm"
    local targetprompt="@$target"
    echo "Entering the interactive mode until \"exit\"."
    while read -e -p "dra-demo-cxl.sh$targetprompt> " -a commands; do
        if [[ "${commands[0]}" == "exit" ]]; then
            break
        fi
        if [[ "${commands[0]}" == "target:" ]]; then
            target="${commands[1]}"
            targetprompt="@$target"
            continue
        fi
        if [[ -z "$target" ]]; then
            eval "${commands[@]}"
        else
            vm=$target vmsh "${commands[*]}"
        fi
    done
}

## For copy-pasting vmsh commands directly to a prompt in vm,
## start by copy-pasting:
#
# vmsh() { ( [ -n "$2" ] && bash -c "$2" ) || ( bash -c "$1"; [ -z "$2" ] || bash -c "$2" ) }

vmsh() {
    local command="$1"
    local verify="$2"
    local exit_status start_t end_t
    log "# vmsh target: $vm"
    [[ -n "$vm" ]] || {
        error "vmsh: ERROR (vm not defined)"
    }
    [[ -z "$verify" ]] || {
        log "# vmsh verify: $verify"
        ssh $SSH_OPTS "$vm" sudo bash -l <<< "$VM_SSH_DEBUG_PREFIX $verify" && {
            if [[ -z "$command" ]]; then
                log "# vmsh: ACK-NOOP (verified, no need to run command)"
            else
                log "# vmsh: ACK-SKIP (verified, no need to run command)"
            fi
            return
        }
        if [[ -z "$command" ]]; then
            log "# vmsh: NACK-NOOP (verify failed, no command to run)"
            return 1
        fi
    }

    log "# vmsh command: $command"
    start_t=$EPOCHREALTIME
    ssh $SSH_OPTS $SSH_OPT "$vm" sudo bash -l <<< "$VM_SSH_DEBUG_PREFIX $command" | stdbuf -oL tee "$TMPFILE_CMD"
    exit_status=${PIPESTATUS[0]}
    end_t=$EPOCHREALTIME
    COMMAND_OUTPUT=$(< "$TMPFILE_CMD" )
    log -n "# vmsh command exit_status: $exit_status duration: "
    log "$(awk "END{print $end_t - $start_t}" < /dev/null)"

    [[ -z "$verify" ]] || {
        if ssh $SSH_OPTS "$vm" sudo bash -l <<< "$VM_SSH_DEBUG_PREFIX $verify"; then
            log "# vmsh: ACK (command executed, outcome verified)"
        else
            log "# vmsh: NACK (command executed, verification exit status $?)"
        fi
    }
    return $exit_status
}

vmwaitsh() {
    local command="$1"
    local timeout="${timeout:-60}"
    vmsh "" "timeout $timeout bash -c 'until ( $command ); do sleep 1; done'"
}

create-cluster() {
    error "no, you don't want to create a cluster right now.... creating cxl vm cluster not implemented yet"

    # Following would create a multinode cluster using govm (qemu)
    # with github.com/intel/memtierd/test/e2e/run.sh script.

    export topology='[{"mem":"8G","cores":"2"}]'
    export distro=fedora
    export k8s=latest

    (
        vm=c0m-fedora code=exit ./run.sh test && \
        vm=c0w1-fedora k8smaster=c0m-fedora code=exit ./run.sh test && \
        vm=c0w2-fedora k8smaster=c0m-fedora code=exit ./run.sh test
    ) || {
        error "creating cluster failed"
    }
    govm-ssh-hosts
}

check-feature-gates() {
    echo 'If k8s is older than v1.37, build and install a new enough one:
    git clone --depth=1 -b v1.37.0 https://github.com/kubernetes/kubernetes &&
    cd kubernetes &&
    make quick-release-images &&
    make WHAT="cmd/kubelet cmd/kubeadm cmd/kubectl" &&
    scp _output/release-images/amd64/kube*.tar '"$vm"': &&
    scp _output/bin/{kubelet,kubeadm,kubectl} '"$vm"':
    ssh '"$vm"' "
        sudo mv -v kubelet kubectl kubeadm $(dirname $(command -v kubectl))/ &&
        for component in apiserver controller-manager scheduler proxy; do sudo ctr -n k8s.io images import kube-$component.tar; done"
    # and finally
    kubeadm upgrade apply v1.37.0 --force
    '

    # kube-apiserver and kube-scheduler are static pods: kubelet
    # restarts them automatically when their manifest changes.
    vmsh "sed -i '/^    - kube-apiserver\$/a\    - --feature-gates=$FEATURE_GATES' /etc/kubernetes/manifests/kube-apiserver.yaml" \
         "grep -- '--feature-gates=$FEATURE_GATES' /etc/kubernetes/manifests/kube-apiserver.yaml"

    vmsh "sed -i '/^    - kube-scheduler\$/a\    - --feature-gates=$FEATURE_GATES' /etc/kubernetes/manifests/kube-scheduler.yaml" \
         "grep -- '--feature-gates=$FEATURE_GATES' /etc/kubernetes/manifests/kube-scheduler.yaml"

    # The kubelet needs the same gates in its own configuration, in
    # KubeletConfiguration featureGates syntax, and a restart. Restart
    # it only if the configuration really changed.
    vmsh "md5sum /var/lib/kubelet/config.yaml"
    local kubelet_config_before="$COMMAND_OUTPUT"

    vmsh "tee -a /var/lib/kubelet/config.yaml <<< 'featureGates:'" \
         "grep '^featureGates:' /var/lib/kubelet/config.yaml"

    # Note the double slash in ${FEATURE_GATES//,/ }: with a single
    # slash bash would replace only the first comma.
    local fgateval fgate val
    for fgateval in ${FEATURE_GATES//,/ }; do
        fgate=${fgateval%=*}; val=${fgateval#*=}
        vmsh "tee -a /var/lib/kubelet/config.yaml <<< '  $fgate: $val'" \
             "grep '^  $fgate: $val\$' /var/lib/kubelet/config.yaml"
    done

    vmsh "md5sum /var/lib/kubelet/config.yaml"
    if [[ "$COMMAND_OUTPUT" != "$kubelet_config_before" ]]; then
        vmsh "systemctl restart kubelet"
    fi

    # Wait until the control plane is back up with the new gates. The
    # gate states are readable from the kube-apiserver metrics, where
    # a trailing 1 means enabled. Note that vmwaitsh wraps the command
    # in single quotes, so it must not contain any.
    timeout=180 vmwaitsh 'kubectl get --raw /metrics 2>/dev/null | grep -qE "DRANodeAllocatableResources.* 1"' ||
        error "kube-apiserver did not come up with DRANodeAllocatableResources enabled"

    vmsh "kubectl get --raw /metrics | grep -E 'kubernetes_feature_enabled.*(DRANodeAllocatableResources|DRAConsumableCapacity|DRAPartitionableDevices)'"
}

check-cxl-memory-zones() {
    # CXL memory hotplugged with auto_online_blocks=online_movable ends
    # up in ZONE_MOVABLE. A cgroup whose cpuset.mems has only such a
    # node cannot satisfy GFP_HIGHUSER allocations, so a container that
    # gets only CXL memory fails to start with
    #
    #   OCI runtime start failed: cannot start an already running container
    #
    # and "page allocation failure ... mems_allowed=2" in dmesg. Move
    # the memory of CPU-less (CXL) NUMA nodes to ZONE_NORMAL.
    vmsh '
        rc=0
        for nodedir in /sys/devices/system/node/node*; do
            # skip DRAM nodes, they have CPUs
            [[ -n "$(< $nodedir/cpulist)" ]] && continue
            for memblk in $nodedir/memory*; do
                [[ -f $memblk/state ]] || continue
                if [[ "$(< $memblk/state)" == "online" && "$(< $memblk/valid_zones)" == "Movable" ]]; then
                    echo "re-onlining $memblk from ZONE_MOVABLE into ZONE_NORMAL"
                    echo offline > $memblk/state || { echo "offlining $memblk failed"; rc=1; continue; }
                    echo online_kernel > $memblk/state || { echo "online_kernel $memblk failed"; rc=1; continue; }
                fi
                echo "$memblk: state=$(< $memblk/state) zone=$(< $memblk/valid_zones)"
            done
        done
        exit $rc' \
         '
        for nodedir in /sys/devices/system/node/node*; do
            [[ -n "$(< $nodedir/cpulist)" ]] && continue
            for memblk in $nodedir/memory*; do
                [[ -f $memblk/state ]] || continue
                [[ "$(< $memblk/state)" == "online" ]] || continue
                [[ "$(< $memblk/valid_zones)" == "Movable" ]] && exit 1
            done
        done
        exit 0' ||
        error "failed to move CXL memory blocks into ZONE_NORMAL"

    vmsh "numactl -H"
}


if [[ -z "$vm" ]]; then
    error "specify vm=NAME-OF-HOST where to ssh and run cluster commands"
fi

SSH_OPT="-o ConnectTimeout=2s" vmsh "exit 42"
if [[ "$?" != "42" ]]; then
    echo "about to create cluster after 5 seconds..."
    sleep 5
    create-cluster
fi

vmsh "whoami"

# Using DRA as presented in
# https://kubernetes.io/docs/tutorials/cluster-management/install-use-dra/

section "enable DRA feature gates"
(
    check-feature-gates
)

section "make CXL memory usable as normal (non-movable) memory"
(
    check-cxl-memory-zones
)

section "check the initial DRA state of the cluster"
(
    vmsh "
         kubectl get deviceclasses
         kubectl get resourceslices
         kubectl get resourceclaims -A
         kubectl get resourceclaimtemplates -A
         "
)


section "deploy the CXL DRA driver"
(
    vmsh "kubectl create namespace $NAMESPACE" \
         "kubectl get namespaces $NAMESPACE -o yaml"

    vmsh "kubectl apply -f - <<EOF
apiVersion: resource.k8s.io/v1
kind: DeviceClass
metadata:
  name: cxl-memory-class
spec:
  selectors:
  - cel:
      expression: \"device.driver == 'cxl.generic' && device.attributes['cxl.generic'].type == 'cxl-node'\"
---
apiVersion: resource.k8s.io/v1
kind: DeviceClass
metadata:
  name: dram-memory-class
spec:
  selectors:
  - cel:
      expression: \"device.driver == 'cxl.generic' && device.attributes['cxl.generic'].type == 'dram'\"
EOF" \
         "kubectl get deviceclass cxl-memory-class -o yaml && kubectl get deviceclass dram-memory-class -o yaml"

    log "driver install"
    scp $SSH_OPTS "$CXL_DRIVER_BIN" "$vm:/tmp/$CXL_DRIVER_NAME" || error "failed to copy CXL driver binary to VM"
    vmsh "install -m 755 /tmp/$CXL_DRIVER_NAME /usr/local/bin/$CXL_DRIVER_NAME" \
         "cmp -s /tmp/$CXL_DRIVER_NAME /usr/local/bin/$CXL_DRIVER_NAME"

    log "create driver configuration"
    # Run the driver on the real CXL hardware of the node. FAKE_CXL=1
    # injects a fake CXL region instead, for nodes without CXL memory.
    # The fake region's Node must be a real, memory-backed NUMA node
    # for the steering part of the demo to work.
    if [[ "$FAKE_CXL" == "1" ]]; then
        vmsh 'tee /root/cxl-driver-config.yaml <<EOF
testability:
  fakeRegionDevices:
  - Name: fakeregion0
    Size: 1073741824
    Mode: ram
    Node: 1
    Memories:
    - Name: fakemem0
      RamSize: 1073741824
      Serial: 12345
      NodeAffinity: 0
EOF' \
             '[[ -n "$(find -H /root/cxl-driver-config.yaml -prune -newermt 2\ seconds\ ago )" ]]'
    else
        vmsh 'tee /root/cxl-driver-config.yaml <<< "{}"' \
             'grep -qx "{}" /root/cxl-driver-config.yaml'
    fi

    log "launch the driver"
    vmsh "( KUBECONFIG=/root/.kube/config NODE_NAME=\$(hostname) /usr/local/bin/$CXL_DRIVER_NAME --node-name \$(hostname) -f /root/cxl-driver-config.yaml -v 5 >& /root/$CXL_DRIVER_NAME.output ) </dev/null >&/dev/null &
         sleep 5" \
         "pgrep -f /usr/local/bin/$CXL_DRIVER_NAME"

    vmsh "pgrep -f /usr/local/bin/$CXL_DRIVER_NAME" || {
        vmsh "tail -30 /root/$CXL_DRIVER_NAME.output"
        error "driver not running"
    }
)

section "check that the driver publishes CXL and DRAM devices"
(
    vmwaitsh 'kubectl get resourceslices -o jsonpath="{.items}" | jq -e "length > 0"'

    vmsh "kubectl get resourceslices -o yaml"

    # The driver publishes one device per enabled CXL region plus one
    # aggregate "dram" device for all DRAM NUMA nodes.
    vmsh "" 'kubectl get resourceslices -o json |
             jq -e ".items[].spec.devices | map(select(.attributes.type.string == \"cxl-node\")) | length > 0"' ||
        error "no cxl-node devices in published resource slices"

    vmsh "" 'kubectl get resourceslices -o json |
             jq -e ".items[].spec.devices | map(select(.attributes.type.string == \"dram\")) | length > 0"' ||
        error "no dram devices in published resource slices"

    vmsh "kubectl get resourceslices -o json |
          jq -r '.items[].spec.devices[] | select(.attributes.type.string == \"cxl-node\") |
                 \"CXL device \\(.name) on NUMA node \\(.attributes.node.int): \\(.capacity.memory.value)\"'"

    # All claims below request CXL memory from a single CXL device, so
    # the largest device must have room for all of them together.
    cxl_needed=$(( $(numfmt --from=iec "${CXL_MEM_SMALL%i}") + 3 * $(numfmt --from=iec "${CXL_MEM_MEDIUM%i}") ))
    vmsh "kubectl get resourceslices -o json |
          jq -r '.items[].spec.devices[] | select(.attributes.type.string == \"cxl-node\") | .capacity.memory.value'"
    cxl_largest=0
    for capacity in $COMMAND_OUTPUT; do
        # capacity is a k8s quantity, e.g. "256Mi" or "262144Ki"
        bytes=$(numfmt --from=iec "${capacity%i}") || continue
        (( bytes > cxl_largest )) && cxl_largest=$bytes
    done
    (( cxl_largest >= cxl_needed )) ||
        error "the largest CXL device has $cxl_largest bytes, the demo claims need $cxl_needed bytes:
lower CXL_MEM_SMALL and CXL_MEM_MEDIUM, or add CXL memory to the node"

    # The driver maps the memory capacity a claim consumes to the node
    # allocatable "memory" resource. The apiserver drops this field
    # silently if the DRANodeAllocatableResources feature gate is off,
    # and the driver then logs "some fields were dropped by the
    # apiserver".
    vmsh "kubectl get resourceslices -o json |
          jq -r '.items[].spec.devices[] | \"\\(.name): \\(.nodeAllocatableResources)\"'"

    vmsh "" "kubectl get resourceslices -o yaml | grep -q nodeAllocatableResources" ||
        error "the driver published no nodeAllocatableResources:
is the DRANodeAllocatableResources feature gate enabled?"
)

section "create resource claims and a pod that uses them"
(
    vmsh "cat > /root/cxl-memory-claim.yaml <<EOF
apiVersion: resource.k8s.io/v1
kind: ResourceClaim
metadata:
  name: cxl-memory-claim
spec:
  devices:
    requests:
    - name: some-cxl-memory
      exactly:
        deviceClassName: cxl-memory-class
        capacity:
          requests:
            memory: $CXL_MEM_SMALL
EOF
kubectl apply -n $NAMESPACE -f /root/cxl-memory-claim.yaml" \
         "kubectl get resourceclaim cxl-memory-claim -n $NAMESPACE -o yaml"

    vmsh "cat > /root/sys-memory-claim.yaml <<EOF
apiVersion: resource.k8s.io/v1
kind: ResourceClaim
metadata:
  name: sys-memory-claim
spec:
  devices:
    requests:
    - name: some-dram-memory
      exactly:
        deviceClassName: dram-memory-class
        capacity:
          requests:
            memory: $DRAM_MEM_LARGE
EOF
kubectl apply -n $NAMESPACE -f /root/sys-memory-claim.yaml" \
         "kubectl get resourceclaim sys-memory-claim -n $NAMESPACE -o yaml"

    vmsh "cat > /root/dram-then-cxl-claim.yaml <<EOF
apiVersion: resource.k8s.io/v1
kind: ResourceClaim
metadata:
  name: dram-then-cxl-claim
spec:
  devices:
    requests:
    - name: some-cxl-memory
      exactly:
        deviceClassName: cxl-memory-class
        capacity:
          requests:
            memory: $CXL_MEM_MEDIUM
    - name: some-dram-memory
      exactly:
        deviceClassName: dram-memory-class
        capacity:
          requests:
            memory: $DRAM_MEM_MEDIUM
    config:
    - opaque:
        driver: cxl.generic
        parameters:
          apiVersion: cxl.generic/v1alpha1
          kind: MemoryPolicyConfig
          memoryUseOrder: \"first-dram\"
          minStep: \"16M\"
          maxStep: \"32M\"
EOF
kubectl apply -n $NAMESPACE -f /root/dram-then-cxl-claim.yaml" \
         "kubectl get resourceclaim dram-then-cxl-claim -n $NAMESPACE -o yaml"

    vmsh "cat > /root/cxl-dram-interleave-claim.yaml <<EOF
apiVersion: resource.k8s.io/v1
kind: ResourceClaim
metadata:
  name: cxl-dram-interleave-claim
spec:
  devices:
    requests:
    - name: some-cxl-memory
      exactly:
        deviceClassName: cxl-memory-class
        capacity:
          requests:
            memory: $CXL_MEM_MEDIUM
    - name: some-dram-memory
      exactly:
        deviceClassName: dram-memory-class
        capacity:
          requests:
            memory: $DRAM_MEM_SMALL
    config:
    - opaque:
        driver: cxl.generic
        parameters:
          apiVersion: cxl.generic/v1alpha1
          kind: MemoryPolicyConfig
          memoryUseOrder: \"start-interleaved\"
          minStep: \"16M\"
          maxStep: \"64M\"
EOF
kubectl apply -n $NAMESPACE -f /root/cxl-dram-interleave-claim.yaml" \
         "kubectl get resourceclaim cxl-dram-interleave-claim -n $NAMESPACE -o yaml"

    vmsh "cat > /root/single-claim-containers-pod.yaml <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: single-claims-containers-pod
spec:
  containers:
  - name: ctr0
    image: busybox
    command: [\"sh\", \"-c\", \"env; sleep 3600\"]
    resources:
      claims:
      - name: cxl-memory
  - name: ctr1
    image: busybox
    command: [\"sh\", \"-c\", \"env; sleep 3600\"]
    resources:
      claims:
      - name: sys-memory
  - name: ctr2
    image: busybox
    command: [\"sh\", \"-c\", \"env; sleep 3600\"]
    resources:
      claims:
      - name: both-memories
  - name: ctr3
    image: busybox
    command: [\"sh\", \"-c\", \"env; sleep 3600\"]
    # no claims
  - name: ctr4
    image: busybox
    command: [\"sh\", \"-c\", \"env; sleep 3600\"]
    resources:
      claims:
      - name: both-memories-interleaved
  terminationGracePeriodSeconds: 2
  resourceClaims:
  - name: cxl-memory
    resourceClaimName: cxl-memory-claim
  - name: sys-memory
    resourceClaimName: sys-memory-claim
  - name: both-memories
    resourceClaimName: dram-then-cxl-claim
  - name: both-memories-interleaved
    resourceClaimName: cxl-dram-interleave-claim
EOF
kubectl apply -n $NAMESPACE -f /root/single-claim-containers-pod.yaml" \
         "kubectl get pod single-claims-containers-pod -n $NAMESPACE -o yaml"

    timeout=120 vmwaitsh "kubectl get pod single-claims-containers-pod -n $NAMESPACE -o jsonpath={.status.phase} | grep -q Running" ||
        error "pod single-claims-containers-pod did not start"

    # Every container that references a claim gets a CDI-injected
    # CXL_CLAIM_<claim-uid>=1 environment variable. ctr3 has no claims
    # and must not get one.
    vmwaitsh "kubectl logs single-claims-containers-pod -c ctr0 -n $NAMESPACE | grep -E CXL"

    vmsh "for c in ctr0 ctr1 ctr2 ctr3 ctr4; do
              echo -n \"\$c: \"
              kubectl logs single-claims-containers-pod -c \$c -n $NAMESPACE | grep -E '^CXL_CLAIM' | tr '\n' ' '
              echo
          done"

    vmsh "kubectl get resourceclaims -n $NAMESPACE"

    # With DRAConsumableCapacity enabled the scheduler records how
    # much of each device's capacity every claim consumes. The driver
    # turns that into DRAM/CXL quotas for the container.
    vmsh "kubectl get resourceclaims -n $NAMESPACE -o json |
          jq -r '.items[] | \"\\(.metadata.name): \" +
                 ([.status.allocation.devices.results[] |
                   \"\\(.request)=\\(.device)/\\(.consumedCapacity.memory)\"] | join(\" \"))'"

    # Per-container memory policy from the driver. cpuset.mems are the
    # NUMA nodes the container may use, memory.high is the next
    # steering threshold. memory.max stays "max": kubelet adds claimed
    # memory to container limits, and these pods set none.
    vmsh "podslice() {
              local uid=\$(kubectl get pod single-claims-containers-pod -n $NAMESPACE -o jsonpath={.metadata.uid})
              echo /sys/fs/cgroup/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod\${uid//-/_}.slice
          }
          ctrcgroup() {
              local cid=\$(kubectl get pod single-claims-containers-pod -n $NAMESPACE -o json |
                           jq -r \".status.containerStatuses[]|select(.name==\\\"\$1\\\").containerID\" | sed s#containerd://##)
              echo \$(podslice)/cri-containerd-\$cid.scope
          }
          for c in ctr0 ctr1 ctr2 ctr3 ctr4; do
              cg=\$(ctrcgroup \$c)
              echo \"\$c cpuset.mems=\$(cat \$cg/cpuset.mems) memory.high=\$(cat \$cg/memory.high) memory.max=\$(cat \$cg/memory.max)\"
          done
          cg=\$(podslice)
          echo \"pod memory.min=\$(cat \$cg/memory.min) memory.low=\$(cat \$cg/memory.low) memory.high=\$(cat \$cg/memory.high) memory.max=\$(cat \$cg/memory.max)\""

    # Node allocatable memory accounted for each claim, and the
    # containers that reference it.
    vmsh "kubectl get pod -n $NAMESPACE single-claims-containers-pod -o json |
          jq -r '.status.nodeAllocatableResourceClaimStatuses[] |
                 \"\\(.resourceClaimName) for \\(.containers | join(\",\")): \" +
                 ([.mapping[] | \"\\(.name)=\\(.quantity)\"] | join(\" \"))'"

    vmsh "" "kubectl get pod -n $NAMESPACE single-claims-containers-pod -o yaml | grep -q nodeAllocatableResourceClaimStatuses" ||
        error "pod status has no nodeAllocatableResourceClaimStatuses: claimed memory is not
accounted against the node allocatable memory"
)

section "steer memory use of a growing workload from DRAM to CXL"
(
    # memgrow allocates anonymous memory in steps and touches one byte
    # per page. It writes bytes one at a time on purpose: vectorized
    # stores are unreliable on qemu-emulated CXL memory.
    vmsh 'cat > /root/memgrow.c <<\MEMGROW
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>

int main(int argc, char **argv)
{
	long step_mb = argc > 1 ? atol(argv[1]) : 8;
	long total_mb = argc > 2 ? atol(argv[2]) : 128;
	long sleep_s = argc > 3 ? atol(argv[3]) : 2;
	long hold_s = argc > 4 ? atol(argv[4]) : 600;
	long done = 0;

	while (done < total_mb) {
		long chunk = step_mb;
		if (done + chunk > total_mb)
			chunk = total_mb - done;
		size_t sz = (size_t)chunk << 20;
		char *p = mmap(NULL, sz, PROT_READ | PROT_WRITE,
			       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
		if (p == MAP_FAILED) {
			perror("mmap");
			return 1;
		}
		for (size_t off = 0; off < sz; off += 4096) {
			volatile char *q = p + off;
			*q = 1;
		}
		done += chunk;
		printf("memgrow: %ld MiB resident\n", done);
		fflush(stdout);
		sleep(sleep_s);
	}
	printf("memgrow: target reached, holding %ld s\n", hold_s);
	fflush(stdout);
	sleep(hold_s);
	return 0;
}
MEMGROW
command -v gcc >/dev/null || dnf install -y gcc glibc-static
rpm -q glibc-static >/dev/null || dnf install -y glibc-static
gcc -static -O0 -o /usr/local/bin/memgrow /root/memgrow.c' \
         'test -x /usr/local/bin/memgrow' ||
        error "failed to build memgrow"

    # A claim of 64Mi DRAM + 64Mi CXL with memoryUseOrder first-dram:
    # the workload should fill DRAM first and spill over to CXL only
    # after the DRAM quota is used up.
    vmsh "kubectl apply -n $NAMESPACE -f - <<EOF
apiVersion: resource.k8s.io/v1
kind: ResourceClaim
metadata:
  name: steer-dram-then-cxl-claim
spec:
  devices:
    requests:
    - name: some-cxl-memory
      exactly:
        deviceClassName: cxl-memory-class
        capacity:
          requests:
            memory: $CXL_MEM_MEDIUM
    - name: some-dram-memory
      exactly:
        deviceClassName: dram-memory-class
        capacity:
          requests:
            memory: $DRAM_MEM_SMALL
    config:
    - opaque:
        driver: cxl.generic
        parameters:
          apiVersion: cxl.generic/v1alpha1
          kind: MemoryPolicyConfig
          memoryUseOrder: \"first-dram\"
          minStep: \"16M\"
          maxStep: \"16M\"
EOF" \
         "kubectl get resourceclaim steer-dram-then-cxl-claim -n $NAMESPACE -o yaml"

    vmsh "kubectl apply -n $NAMESPACE -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: memgrow-first-dram
spec:
  restartPolicy: Never
  terminationGracePeriodSeconds: 2
  containers:
  - name: memgrow
    image: busybox
    command: [\"/host/memgrow\", \"8\", \"112\", \"2\", \"600\"]
    volumeMounts:
    - name: hostbin
      mountPath: /host
    resources:
      claims:
      - name: steer-memory
  volumes:
  - name: hostbin
    hostPath:
      path: /usr/local/bin
      type: Directory
  resourceClaims:
  - name: steer-memory
    resourceClaimName: steer-dram-then-cxl-claim
EOF" \
         "kubectl get pod memgrow-first-dram -n $NAMESPACE -o yaml"

    # Follow the memory of the container per memory type while it
    # grows. Expect growth on the DRAM nodes until the DRAM quota is
    # reached, then on the CXL node.
    #
    # The watcher goes into a file on the vm: quoting it into a single
    # vmsh command would be unreadable. <<\WATCH keeps the remote shell
    # from expanding it. The script must not contain single quotes,
    # because vmsh passes the whole command as a single-quoted string.
    vmsh 'cat > /root/cxl-numa-watch.sh <<\WATCH
#!/bin/bash
# Print the anonymous memory of a container per memory type while its
# workload grows. NUMA nodes without CPUs are taken to be CXL nodes.
namespace=$1
pod=$2
samples=${3:-40}
interval=${4:-2}

cxlnodes=""
for nodedir in /sys/devices/system/node/node*; do
    [[ -n "$(< $nodedir/cpulist)" ]] && continue
    cxlnodes="$cxlnodes ${nodedir##*/node}"
done
echo "CPU-less (CXL) NUMA nodes:$cxlnodes"

cg=""
printf "%-8s %-10s %-10s %-10s %-12s %s\n" TIME anon-DRAM anon-CXL anon-total memory.high cpuset.mems
for ((sample = 0; sample < samples; sample++)); do
    if [[ -z "$cg" ]]; then
        uid=$(kubectl get pod "$pod" -n "$namespace" -o jsonpath={.metadata.uid} 2>/dev/null)
        cid=$(kubectl get pod "$pod" -n "$namespace" -o json 2>/dev/null |
                  jq -r ".status.containerStatuses[0].containerID // \"\"" | sed s#containerd://##)
        [[ -n "$cid" ]] &&
            cg=/sys/fs/cgroup/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod${uid//-/_}.slice/cri-containerd-$cid.scope
    fi
    if [[ -n "$cg" && -d "$cg" ]]; then
        read -ra fields <<< "$(grep ^anon $cg/memory.numa_stat)"
        dram=0
        cxl=0
        for field in "${fields[@]:1}"; do   # fields[0] is "anon", rest are N<node>=<bytes>
            node=${field%%=*}
            node=${node#N}
            bytes=${field#*=}
            if [[ " $cxlnodes " == *" $node "* ]]; then
                cxl=$((cxl + bytes))
            else
                dram=$((dram + bytes))
            fi
        done
        printf "%-8s %-10s %-10s %-10s %-12s %s\n" "$(date +%T)" \
               "$((dram / 1048576))Mi" "$((cxl / 1048576))Mi" "$(((dram + cxl) / 1048576))Mi" \
               "$(< $cg/memory.high)" "$(< $cg/cpuset.mems)"
    else
        printf "%-8s (waiting for the container)\n" "$(date +%T)"
    fi
    sleep "$interval"
done
WATCH
chmod +x /root/cxl-numa-watch.sh'

    vmsh "/root/cxl-numa-watch.sh $NAMESPACE memgrow-first-dram 40 2"

    vmsh "kubectl logs memgrow-first-dram -n $NAMESPACE | tail -3"

    vmsh "grep -E 'memgrow|nextStep returned|Updated cpuset.mems|NUMA memory distribution' /root/$CXL_DRIVER_NAME.output | tail -20"

    # The steering worked if the container ended up with memory on
    # both the DRAM and the CXL nodes.
    vmsh "" "uid=\$(kubectl get pod memgrow-first-dram -n $NAMESPACE -o jsonpath='{.metadata.uid}')
             cid=\$(kubectl get pod memgrow-first-dram -n $NAMESPACE -o json |
                    jq -r '.status.containerStatuses[0].containerID' | sed 's#containerd://##')
             cg=/sys/fs/cgroup/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod\${uid//-/_}.slice/cri-containerd-\$cid.scope
             grep -q ',' \$cg/cpuset.mems || grep -q '-' \$cg/cpuset.mems" ||
        error "memory was never steered to a second memory type"
)

if [[ "$INTERACTIVE" == "1" ]]; then
    log "there's nothing more but cleanup ahead"
    interactive
fi

if [[ "$SKIP_CLEANUP" == "1" ]]; then
    log "SKIP_CLEANUP=1, leaving the namespace, claims and the driver in place"
    exit 0
fi

section "delete the pods"
(
    vmsh "kubectl delete pod single-claims-containers-pod memgrow-first-dram -n $NAMESPACE --now"

    # No single quotes in the command: vmwaitsh wraps it in them.
    vmwaitsh "kubectl get resourceclaim cxl-memory-claim -n $NAMESPACE -o json | jq -e \".status == {}\""
)

section "cleanup"
(
    vmsh "
         kubectl delete namespace $NAMESPACE
         kubectl delete deviceclasses cxl-memory-class dram-memory-class
         pkill -f /usr/local/bin/$CXL_DRIVER_NAME
         "
)
