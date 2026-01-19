# How to build Intel CXL Resource Driver container image

## Platforms supported

- Linux

## Prerequisites

- Docker or Podman.

## Building

`Makefile` automates this, only required tool is Docker or Podman.
To build the container image locally, from the root of this Git repository:
```bash
make cxl-container-build
```

It is possible to specify custom registry, container image name, and version (tag) as separate
variables to override any part of release container image URL in the build command, e.g.:
```bash
REGISTRY=myregistry CXL_IMAGE_NAME=myimage CXL_IMAGE_VERSION=myversion make cxl-container-build
```

or whole resulting image URL (this will ignore REGISTRY, CXL_IMAGE_NAME, CXL_IMAGE_VERSION even if specified):
```bash
CXL_IMAGE_TAG=myregistry/myimagename:myversion make cxl-container-build
```

To build the container image and push image to the destination registry straight away:
```bash
REGISTRY=registry.local make cxl-container-push
```
or
```bash
CXL_IMAGE_TAG=registry.local/intel-cxl-resource-driver:latest make cxl-container-push
```
