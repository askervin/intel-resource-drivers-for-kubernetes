# Intel CXL resource driver for Kubernetes

CAUTION: This is a beta / non-production software, do not use on production clusters.

## About resource driver

The DRA driver publishes ResourceSlices, the scheduler allocates the resources, and the resource
driver's kubelet-plugin ensures that the allocated devices are prepared and available for Pods.

## Supported Kubernetes Versions

Kubernetes v1.37 is the minimum supported version.

| Branch            | Kubernetes branch/version       | Status      | DRA                            |
|:------------------|:--------------------------------|:------------|:-------------------------------|
| v0.1.0            | Kubernetes v1.37+               | Supported   | Structured Parameters          |

The driver needs the `DRANodeAllocatableResources` feature gate to account claimed memory against
the node's allocatable memory.

## Documentation

- [How to setup a Kubernetes cluster with DRA enabled](../CLUSTER_SETUP.md)
- [How to deploy and use Intel CXL resource driver](USAGE.md)
- Optional: [How to build Intel CXL resource driver container image](BUILD.md)
