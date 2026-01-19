# Intel CXL resource driver for Kubernetes

CAUTION: This is a beta / non-production software, do not use on production clusters.

## About resource driver

With structured parameters (K8s v1.31+), the DRA driver publishes ResourceSlice, scheduler allocates
the resources and resource driver's kubelet-plugin ensures that the allocated devices are prepared
and available for Pods.

DRA API graduated to GA with v1 resource.k8s.io API in K8s v1.34, backwards compatibility may vary
depending on features enabled.

## Supported Kubernetes Versions

Supported Kubernetes versions are listed below:

| Branch            | Kubernetes branch/version       | Status      | DRA                            |
|:------------------|:--------------------------------|:------------|:-------------------------------|
| v0.1.0            | Kubernetes v1.35+               | Supported   | Structured Parameters          |

## Documentation

- [How to setup a Kubernetes cluster with DRA enabled](../CLUSTER_SETUP.md)
- [How to deploy and use Intel CXL resource driver](USAGE.md)
- Optional: [How to build Intel CXL resource driver container image](BUILD.md)
