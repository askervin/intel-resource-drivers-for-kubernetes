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

NAMESPACE="dra-demo-cxl"

# feature gates in kube-apiserver --feature-gates=.... command line syntax
FEATURE_GATES="DRANodeAllocatableResources=true,DRAConsumableCapacity=true,DRAPartitionableDevices=true"

SSH_OPTS="-o StrictHostKeyChecking=No -o ControlMaster=auto -o ControlPersist=120 -o ControlPath=~/.ssh/%r@%h-%p"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR=$(cd "$SCRIPT_DIR"; git rev-parse --show-toplevel)

CXL_DRIVER_BIN="$PROJECT_DIR/bin/kubelet-cxl-plugin"

TMPFILE_CMD="$SCRIPT_DIR/output/dra-demo-cxl/vmsh.output"

if [[ "$DEBUG" == "1" ]]; then
    VMSH_DEBUG_PREFIX="set -x; "
fi

error() {
    echo "dra-demo-cxl.sh error: $*" >&2
    exit 1
}

[[ -x "$CXL_DRIVER_BIN" ]] || error "CXL driver binary not found or not executable: $CXL_DRIVER_BIN"

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
    ssh $SSH_OPT "$vm" sudo bash -l <<< "$VM_SSH_DEBUG_PREFIX $command" | stdbuf -oL tee "$TMPFILE_CMD"
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
    echo 'If k8s is too old, build a recent enough:
    git clone --depth=1 -b v1.36.0-beta0 https://github.com/kubernetes/kubernetes &&
    cd kubernetes &&
    make quick-release-images &&
    make WHAT="cmd/kubelet cmd/kubeadm cmd/kubectl" &&
    scp _output/release-images/amd64/kube*.tar n4-cxl-fedora-43-containerd: &&
    scp _output/bin/{kubelet,kubeadm,kubectl} n4-cxl-fedora-43-containerd:
    ssh n4-cxl-fedora-43-containerd "
        sudo mv -v kubelet kubectl kubeadm $(dirname $(command -v kubectl))/ &&
        for component in apiserver controller-manager scheduler proxy; do sudo ctr -n k8s.io images import kube-$component.tar; done"
    # and finally
    kubeadm upgrade apply v1.36.0-beta.0 --force
    '

    vmsh "sudo sed -i '/^    - kube-apiserver/a\    - --feature-gates=$FEATURE_GATES' /etc/kubernetes/manifests/kube-apiserver.yaml" \
         "sudo grep '$FEATURE_GATES' /etc/kubernetes/manifests/kube-apiserver.yaml"

    vmsh "sudo sed -i '/^    - kube-scheduler/a\    - --feature-gates=$FEATURE_GATES' /etc/kubernetes/manifests/kube-scheduler.yaml" \
         "sudo grep '$FEATURE_GATES' /etc/kubernetes/manifests/kube-scheduler.yaml"

    vmsh "sudo tee -a  /var/lib/kubelet/config.yaml <<< 'featureGates:'" \
         "grep 'featureGates:' /var/lib/kubelet/config.yaml"

    for fgateval in ${FEATURE_GATES/,/ }; do
        fgate=${fgateval%=*}; val=${fgateval#*=}
        vmsh "sudo tee -a /var/lib/kubelet/config.yaml <<< '  $fgate: $val'" \
             "grep '$fgate: $val' /var/lib/kubelet/config.yaml"
    done
}


if [[ -z "$vm" ]]; then
    error "specify vm=NAME-OF-HOST where to ssh and run cluster commands"
fi

echo "Dropping to interactive mode, try check-feature-gates, to start with"
interactive

SSH_OPT="-o ConnectTimeout=2s" vmsh "exit 42"
if [[ "$?" != "42" ]]; then
    echo "exit status: $? from vm"
    echo "about to create cluster after 5 seconds..."
    sleep 5
    create-cluster
fi

vmsh "whoami"

# Using DRA as presented in
# https://kubernetes.io/docs/tutorials/cluster-management/install-use-dra/

(
    vmsh "
         kubectl get deviceclasses
         kubectl get resourceslices
         kubectl get resourceclaims -A
         kubectl get resourceclaimtemplates -A
         "

    # vmsh "dnf -y install docker && systemctl start docker" \
    #      "systemctl is-active docker" ||
    #     error "failed to install docker"
)


section "deploy dra-tutorial driver"
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

    log "you should have deviceclasses now. 'exit' to continue"
    interactive

    log "skip driver install, you should have it running already"
    # rsync -av "$CXL_DRIVER_BIN" "$vm:" || error "failed to copy CXL driver binary to VM"

    # vmsh "( KUBECONFIG=/root/.kube/config ./$(basename $CXL_DRIVER_BIN) --node-name \$(hostname) -c '{}' -v 5 >& $(basename $CXL_DRIVER_BIN).output ) </dev/null >&/dev/null &" \
    #      "pgrep $(basename $CXL_DRIVER_BIN)"

    # vmsh "kubectl apply --server-side -f http://k8s.io/examples/dra/driver-install/serviceaccount.yaml" \
    #      "kubectl get serviceaccount -n dra-tutorial dra-example-driver-service-account -o yaml"

    # vmsh "kubectl apply --server-side -f http://k8s.io/examples/dra/driver-install/clusterrole.yaml" \
    #      "kubectl get clusterrole dra-example-driver-role -o yaml"

    # vmsh "kubectl apply --server-side -f http://k8s.io/examples/dra/driver-install/clusterrolebinding.yaml" \
    #      "kubectl get clusterrolebinding dra-example-driver-role-binding -o yaml"

    # vmsh "kubectl apply --server-side -f http://k8s.io/examples/dra/driver-install/priorityclass.yaml" \
    #      "kubectl get priorityclass dra-driver-high-priority -o yaml"

    # vmsh "kubectl apply --server-side -f http://k8s.io/examples/dra/driver-install/daemonset.yaml" \
    #      "kubectl get daemonset -n dra-tutorial dra-example-driver-kubeletplugin -o yaml"
)

section "check that the driver is running"
(
    # vmsh "kubectl get pods -n dra-tutorial -o wide"

    vmwaitsh 'kubectl get resourceslices -o jsonpath="{.items}" | jq -e "length > 0"'

    # vmsh "kubectl get \$(kubectl get resourceslices -o name | grep c0w1-fedora) -o yaml"
)

section "create a resource claim and a pod that uses it"
(

    vmsh "kubectl apply -n $NAMESPACE -f - <<EOF
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
            memory: 100Mi
EOF" \
         "kubectl get resourceclaim cxl-memory-claim -n $NAMESPACE -o yaml"

    vmsh "kubectl apply -n $NAMESPACE -f - <<EOF
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
            memory: 1Gi
EOF" \
         "kubectl get resourceclaim sys-memory-claim -n $NAMESPACE -o yaml"

    vmsh "kubectl apply -n $NAMESPACE -f - <<EOF
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
            memory: 128Mi
    - name: some-dram-memory
      exactly:
        deviceClassName: dram-memory-class
        capacity:
          requests:
            memory: 384Mi
    config:
    - opaque:
        driver: cxl.generic
        parameters:
          apiVersion: cxl.generic/v1alpha1
          kind: MemoryPolicyConfig
          memoryUseOrder: \"first-dram\"
          minStep: \"64M\"
          maxStep: \"128M\"
EOF" \
         "kubectl get resourceclaim dram-then-cxl-claim -n $NAMESPACE -o yaml"

    log "you should have resource claims now. 'exit' to continue"
    interactive

    # vmsh "kubectl apply --server-side -f http://k8s.io/examples/dra/driver-install/example/resourceclaim.yaml" \
        #      "kubectl get resourceclaim some-gpu -n dra-tutorial -o yaml"

    vmsh "kubectl apply -n $NAMESPACE -f -<<EOF
apiVersion: v1
kind: Pod
metadata:
  name: cxl-memory-pod
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
  resourceClaims:
  - name: cxl-memory
    resourceClaimName: cxl-memory-claim
  - name: sys-memory
    resourceClaimName: sys-memory-claim
  - name: both-memories
    resourceClaimName: dram-then-cxl-claim
EOF" \
         "kubectl get pod cxl-memory-pod -n $NAMESPACE -o yaml"

    vmwaitsh "kubectl logs cxl-memory-pod -c ctr0 -n $NAMESPACE | grep -E CXL"

    # vmsh "kubectl apply --server-side -f http://k8s.io/examples/dra/driver-install/example/pod.yaml" \
    #      "kubectl get pod pod0 -n dra-tutorial -o yaml"

    # vmwaitsh "kubectl logs pod0 -c ctr0 -n dra-tutorial | grep -E 'GPU_DEVICE_[0-9]+='"

    interactive

    vmsh "kubectl get resourceclaims -n $NAMESPACE"

    vmsh "kubectl get resourceclaim cxl-memory-claim -n $NAMESPACE -o yaml"

    # TODO: verify that resource mapping from cxl and dram to node native memory works
    # look for pod .status.nodeAllocatableResourceClaimStatuses?
    # WEIRD:
    # in kube-scheduler log there is print from
    # 	logger.V(5).Info("Patched pod status with NodeAllocatableResourceClaimStatuses", "pod", klog.KObj(pod), "status", targetStatus.NodeAllocatableResourceClaimStatuses)
    # line 432 in nodeallocatabledynamicresources.go

    vmsh "echo 'You might be running buggy kubelet: pod .status.nodeAllocatableResources does not exist, could be accidentaly dropped (not copied on status update) by kubelet'" \
         "kubectl get pod -n dra-demo-cxl cxl-memory-pod  -o yaml | grep nodeAllocatableResource -A10"


)

# section "peek under the hood"
# (
#     vmsh "kubectl logs -l app.kubernetes.io/name=dra-example-driver -n dra-tutorial"
# )

section "delete the pod"
(
    vmsh "kubectl delete pod cxl-memory-pod -n $NAMESPACE --now"

    vmwaitsh "kubectl get resourceclaim cxl-memory-claim -n $NAMESPACE -o json | jq -e '.status == {}'"

    # vmsh "kubectl logs -l app.kubernetes.io/name=dra-example-driver -n dra-tutorial"
)

section "cleanup"
(
    vmsh "
         kubectl delete namespace $NAMESPACE
         kubectl delete deviceclasses cxl-memory-class dram-memory-class
         # kubectl delete clusterrole dra-example-driver-role
         # kubectl delete clusterrolebinding dra-example-driver-role-binding
         # kubectl delete priorityclass dra-driver-high-priority
         "
)
