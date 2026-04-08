indent4() {
    sed 's/^/    /g'
}

hr-if-hr() {
    if [[ "$hr" == "1" ]]; then
        numhr -b0
    else
        cat
    fi
}

hrecho() {
    if [[ "$hr" == "1" ]]; then
        echo "$@"
    fi
}

watch-statuses() {
    while true; do
        statuses > /tmp/$USER.dra-demo.statuses
        clear
        cat /tmp/$USER.dra-demo.statuses
        sleep 2
    done
}

statuses() {
    echo ""
    echo "Hardware topology:"
    (
        echo -e "Node\tSize\tUsed"
        numactl -H | awk '/size:/{size=$4}/free:/{free=$4;print "node"$2"\t"size"Mi\t"(size-free)"Mi"}'
    ) | numhr -r | numdelta -c2 -M nodesizeused | numhr | column -t | indent4

    echo ""
    echo "ResourceSlices: (what can be consumed)"
    (
        echo -e "Node\tDevice\tMemory"
        kubectl get resourceslices.resource.k8s.io -o json | jq -r '.items[] | (first(.metadata.ownerReferences[] | select(.kind=="Node") | .name)) as $node | .spec.devices[] | [$node, .name, .capacity.memory.value] | @tsv'
    ) | numhr -r | numhr | column -tR3 | indent4

    echo ""
    echo "ResourceClaims: (what is consumed)"
    (
        hr=1 resourceclaims-consumed
    ) | indent4

    echo ""
    echo "Pods: (who consumes)"
    (
        echo -e "Pod\tNativeMemory\tContainers"
        for pod in $(kubectl get pods -n dra-demo-cxl -o name); do
            kubectl get $pod -n dra-demo-cxl -o json \
                | jq -r '.metadata.name as $pod
                        | .status.nodeAllocatableResourceClaimStatuses[]
                        | [$pod,.resources.memory, "\(.containers|join(","))"]
                        | @tsv'
        done
    ) | column -tR2 | indent4

}

resourceclaims-consumed() {
    (
        hrecho -e "Claim\tDeviceClass\tMemory\tPod"
        for claim in $(kubectl get resourceclaims.resource.k8s.io -n dra-demo-cxl -o name); do
            kubectl get -n dra-demo-cxl $claim -o json \
                | jq -r '.metadata.name as $name
                         | (first(.status?.reservedFor[]? | .name) // "n/a") as $pod
                         | .spec.devices.requests[].exactly
                         | [$name, .deviceClassName, .capacity.requests.memory, $pod] | @tsv'
        done
    ) | numhr -r | hr-if-hr | column -tR3
}

resourceclaims-coords() {
    resourceclaims-consumed | awk 'BEGIN{print "to coords..."}{print}'

}

# errmsg: print the name of the caller bash functionand the message to stderr
errmsg() {
    local caller="${FUNCNAME[1]}"
    echo "error: $caller: $*" >&2
}

ctr-waypoints() {
    # extracts waypoints from driver.log file from stdin and prints them to stdout in format:
    # namespace/pod/container [wp0node0usage wp0node1usage ...] [wp1node0usage0 wp1node1usage ...] ...
    #
    # parse this information from real log lines like:
    # 1775047182.322506 DEBUG cgmpolmgr dra-demo-cxl/use-both-memories-pod/ctr0: NewManager: created with waypoints [{ map[1:134217728]} { map[0:402653184 1:134217728]}] for cgroup /sys/fs/cgroup/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod15b54f11_0dc3_4e0c_b0e0_81c9bdedd291.slice/cri-containerd-d329b169d1c420f723eb4474c68d4a08bd62dae6b20de993ea8644e92d6353ec.scope
    # where { map[1:134217728]} means that the first waypoint has 134217728 bytes of memory allocated on node 1, but 0 on node 0.

    # Extract waypoints from driver log.
    # Look for lines with "NewManager: created waypoints". Then extract the namespace/pod/container,
    # and finally loop through every map[<node>:<usage> <node>:<usage>...] and every
    # node:usage pair to print the usage for each node in the format.
    python -c '
import sys, re
pattern = re.compile(r"(\S+/\S+/\S+): NewManager: created with waypoints \[(.+)\]")
for line in sys.stdin:
    match = pattern.search(line)
    if match:
        name = match.group(1)
        waypoints_str = match.group(2)
        # waypoints_str is something like "{ map[1:134217728]} { map[0:402653184 1:134217728]}"
        # We want to extract the usage for each node for each waypoint.
        waypoints = []
        for wp in re.findall(r"\{ ?map\[([^\]]+)\] ?\}", waypoints_str):
            node_usage = {int(node): int(usage) for node, usage in re.findall(r"(\d+):(\d+)", wp)}
            waypoints.append(node_usage)
        # Now we have a list of waypoints, where each waypoint is a dict of node:usage.
        # We want to print them in the format: name [wp0node0usage wp0node1usage ...] [wp1node0usage0 wp1node1usage ...] ...
        max_node = max((node for wp in waypoints for node in wp.keys()), default=-1)
        output = name
        for wp in waypoints:
            # fill 0 usage for every node that is not present in the waypoint
            wp = {node: wp.get(node, 0) for node in range(max_node + 1)}
            output += " [" + " ".join(str(wp.get(node, 0)) for node in sorted(wp.keys())) + "]"
        print(output)
'
}

ctr-waypoints-coords() {
    # Take ctr-waypoints output and convert it to "coords" program input
    # Works for 2 first nodes, only.
    # input format:
    #   namespace/pod/container [wp0node0usage wp0node1usage] [wp1node0usage0 wp1node1usage] ...
    # output format:
    #   c.new_line('pod/container', xys=[(wp0node0usage, wp0node1usage), (wp1node0usage, wp1node1usage), ...])
    ctr-waypoints "$@" | python -c '
MiB=1024*1024
import sys
for line in sys.stdin:
    if " [" not in line:
        continue
    name, waypoints_str = line.split(" [", 1)
    name = name.split("/", 2)[-1]  # take only pod/container
    waypoints = [(0, 0)]  # default waypoint with 0 usage on both nodes
    for wp in waypoints_str.split("] ["):
        wp = wp.strip("[] \n")
        if not wp:
            continue
        node_usages = list(map(int, wp.split()))
        if len(node_usages) < 2:
            continue
        waypoints.append((node_usages[0]//MiB, node_usages[1]//MiB))
    if len(waypoints) > 1:
        print(f"c.new_line(\"{name}\", xys={waypoints})")
'
}

ctr-mem-distribution() {
    # Parse driver.log from stdin for lines like:
    # 1775047182.326754 DEBUG cgmpolmgr dra-demo-cxl/use-both-memories-pod/ctr0: NUMA memory distribution: [0:4096 1:2912256]
    # Output format:
    #  namespace/pod/container [4096 2912256]
    python -c '
import sys, re
pattern = re.compile(r"(\S+/\S+/\S+): NUMA memory distribution: \[([^\]]+)\]")
for line in sys.stdin:
    match = pattern.search(line)
    if match:
        name = match.group(1)
        distribution_str = match.group(2)
        # distribution_str is something like "0:4096 1:2912256"
        node_usage = {int(node): int(usage) for node, usage in re.findall(r"(\d+):(\d+)", distribution_str)}
        output = name + " [" + " ".join(str(node_usage.get(node, 0)) for node in range(max(node_usage.keys()) + 1)) + "]"
        print(output)
'
}

ctr-mem-distribution-coords() {
    # Take ctr-mem-distribution output and convert it to "coords" program input
    # Works for 2 first nodes, only.
    # input format:
    #   namespace/pod/container [node0usage node1usage]
    # output format:
    #   c.new_line('pod/container', xys=[(node0usage, node1usage)])
    ctr-mem-distribution "$@" | python -c '
MiB=1024*1024
import sys
t = 0
for line in sys.stdin:
    if " [" not in line:
        continue
    name, distribution_str = line.split(" [", 1)
    name = name.split("/", 2)[-1]  # take only pod/container
    node_usages = list(map(int, distribution_str.strip("[] \n").split()))
    if len(node_usages) < 2:
        continue
    t += 1
    print(f"c.new_point(\"{name}\", text=\"t{t}\", xy=({node_usages[0]//MiB}, {node_usages[1]//MiB}))")
'
}

ctr-coords() {
    # Read driver.log from stdin and extract waypoints and memory
    # distribution, then print them in "coords" program input format
    # in the order of appearance in the log.
    while read line; do
        if [[ "$line" == *"NewManager: created with waypoints"* ]]; then
            echo "$line" | ctr-waypoints-coords
        elif [[ "$line" == *"NUMA memory distribution"* ]]; then
            echo "$line" | ctr-mem-distribution-coords
        fi
    done
}

line-delay() {
    # Read lines from stdin and print them to stdout with a delay between lines.
    local delay_s=${1:-0.5}
    while read line; do
        echo "$line"
        sleep "$delay_s"
    done
}
