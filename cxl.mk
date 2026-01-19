# Copyright (c) 2024, Intel Corporation.  All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


CXL_VERSION ?= v0.1.0
CXL_IMAGE_NAME ?= intel-cxl-resource-driver
CXL_IMAGE_VERSION ?= $(CXL_VERSION)
CXL_IMAGE_TAG ?= $(REGISTRY)/$(CXL_IMAGE_NAME):$(CXL_IMAGE_VERSION)

CXL_BINARIES = \
bin/kubelet-cxl-plugin

CXL_COMMON_SRC = \
$(COMMON_SRC) \
pkg/cxl/cdihelpers/*.go \
pkg/cxl/device/*.go \
pkg/cxl/discovery/*.go

CXL_LDFLAGS = ${LDFLAGS} -extldflags ${EXT_LDFLAGS} -X ${PKG}/pkg/version.version=${CXL_VERSION}

.PHONY: cxl
cxl: $(CXL_BINARIES)

bin/kubelet-cxl-plugin: cmd/kubelet-cxl-plugin/*.go $(CXL_COMMON_SRC)
	CGO_ENABLED=0 GOOS=linux GOARCH=${ARCH} \
	  go build -a -ldflags "${CXL_LDFLAGS}" -mod vendor -o $@ ./cmd/kubelet-cxl-plugin

.PHONY: cxl-container-build
cxl-container-build: cleanall vendor
	@echo "Building cxl resource driver container..."
	$(DOCKER) build --pull --platform="linux/$(ARCH)" -t $(CXL_IMAGE_TAG) \
	--build-arg LOCAL_LICENSES=$(LOCAL_LICENSES) \
	--build-arg http_proxy=$(http_proxy) \
	--build-arg https_proxy=$(https_proxy) \
	--build-arg no_proxy=$(no_proxy) \
	-f Dockerfile.cxl .

.PHONY: cxl-container-push
cxl-container-push: cxl-container-build
	$(DOCKER) push $(CXL_IMAGE_TAG)
