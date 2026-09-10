.DEFAULT_GOAL := help

GO ?= go
GOFMT ?= gofmt

ENVTEST_K8S_VERSION ?= 1.36.2

BINARY_PATH ?= bin/hyperfleet-applier

BUILD_DATE ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GIT_SHA ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
APP_VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "0.0.0-dev")

GOFLAGS ?= -trimpath
LDFLAGS := -s -w \
	-X main.version=$(APP_VERSION) \
	-X main.commit=$(GIT_SHA) \
	-X main.date=$(BUILD_DATE)


CONFIG ?= configs/applier.yaml
KUBE_CONFIG_PATH ?= $(if $(KUBECONFIG),$(KUBECONFIG),$(HOME)/.kube/config)

# Go tool info

LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)


# Invoke a pinned tool: $(call gotool,name)
# All tools share tools/go.mod with Go 1.24+ tool directives.
TOOL_MOD := tools/go.mod
gotool = "$(GO)" tool -modfile="$(TOOL_MOD)" $(1)

# Tool shortcuts
HELM_DOCS := $(call gotool,helm-docs)
GOLANGCI_LINT := $(call gotool,golangci-lint)
SETUP_ENVTEST := $(call gotool,setup-envtest)
KUBECONFORM := $(call gotool,kubeconform)


.PHONY: help
help: ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
##@ Development

CGO_ENABLED ?= 1

.PHONY: build
build: ## Build the applier binary	
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BINARY_PATH) ./cmd

.PHONY: run
run: build ## Run the applier service
	./$(BINARY_PATH) serve \
		--config "$(CONFIG)" \
		--kubernetes-kube-config-path "$(KUBE_CONFIG_PATH)"

.PHONY: test
test: ## Run unit tests
	$(GO) test -v -race -coverprofile=coverage.out ./...

# Removal of -i to the setup-envtest command is intentional
# If we run this envtest in CI, we want to load them from the $(PWD)/bin since we don't have access to the home directory.
# Without --bin-dir and -i setup-envtest installs the binaries in ~/.local/share/kubebuilder-envtest/
.PHONY: setup-envtest
setup-envtest: $(LOCALBIN) ## Download the envtest binaries (etcd, kube-apiserver) into the local bin directory.
	$(SETUP_ENVTEST) use '$(ENVTEST_K8S_VERSION)' --bin-dir $(LOCALBIN) -p path

.PHONY: test-envtest
test-envtest: fmt vet setup-envtest ## Run envtest-backed integration tests against a real kube-apiserver
	KUBEBUILDER_ASSETS="$$($(SETUP_ENVTEST) use '$(ENVTEST_K8S_VERSION)' --bin-dir $(LOCALBIN) -p path)" go test -race -tags envtest ./... -run Envtest -v

.PHONY: fmt
fmt: ## Format Go code
	$(GOFMT) -s -w .

.PHONY: gofmt
gofmt: fmt ## Alias for fmt


.PHONY: fmt-check
fmt-check: ## Check if code is formatted
	@diff=$$($(GOFMT) -s -d .); \
	if [ -n "$$diff" ]; then \
		echo "Code is not formatted. Run 'make fmt' to fix:"; \
		echo "$$diff"; \
		exit 1; \
	fi

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

.PHONY: go-vet
go-vet: vet ## Alias for vet

.PHONY: lint
lint: ## Run golangci-lint
	$(GOLANGCI_LINT) run

.PHONY: verify
verify: fmt-check vet test-helm ## Run all verification checks

.PHONY: lint-check
lint-check: fmt-check vet ## Run static code analysis

##@ Dependencies

.PHONY: tidy
tidy: ## Tidy go.mod
	$(GO) mod tidy

.PHONY: tools
tools: ## Ensure tool dependencies are up to date
	cd tools && "$(GO)" mod tidy

.PHONY: verify-tools
verify-tools: tools ## Fail in CI if tool module drifted
	@git diff --exit-code HEAD -- tools/go.mod tools/go.sum || (echo "tool modules out of date; run 'make tools'" && exit 1)

.PHONY: download 

download: ## Download dependencies
	$(GO) mod download


##@ Container Images
# =============================================================================
# Image Configuration
# =============================================================================
QUAY_REPO := openshift-hyperfleet
IMAGE_REGISTRY ?= quay.io/$(QUAY_REPO)
IMAGE_NAME ?= hyperfleet-applier
IMAGE_TAG ?= $(APP_VERSION)
IMG ?= $(IMAGE_REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)
PLATFORM ?= linux/amd64

BASE_IMAGE ?= registry.access.redhat.com/ubi9-micro:latest
# Auto-detect container tool (podman preferred when available)
CONTAINER_TOOL ?= $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)

.PHONY: check-container-tool
check-container-tool:
ifndef CONTAINER_TOOL
	@echo "Error: No container tool found (podman or docker)"
	@echo ""
	@echo "Please install one of:"
	@echo "  brew install podman   # macOS"
	@echo "  brew install docker   # macOS"
	@echo "  dnf install podman    # Fedora/RHEL"
	@exit 1
endif

# Build container image (multi-stage build, no local binary needed)
.PHONY: image
image: check-container-tool ## Build container image with configurable registry/tag
	@echo "Building container image $(IMG)..."
	$(CONTAINER_TOOL) build \
		--platform $(PLATFORM) \
		--build-arg GIT_SHA=$(GIT_SHA) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		--build-arg BASE_IMAGE=$(BASE_IMAGE) \
		--build-arg APP_VERSION=$(APP_VERSION) \
		-t $(IMG) .
	@echo "Image built: $(IMG)"

.PHONY: image-push
image-push: check-container-tool ## Push container image to registry
	@echo "Pushing image $(IMG)..."
	$(CONTAINER_TOOL) push $(IMG)
	@echo "Image pushed: $(IMG)"

.PHONY: check-quay-user
check-quay-user:
ifeq ($(strip $(QUAY_USER)),)
	@echo "Error: QUAY_USER is not set"
	@echo ""
	@echo "Usage: QUAY_USER=myuser make image-dev"
	@exit 1
endif

# Usage: QUAY_USER=myuser make image-dev
# Dev image configuration - set QUAY_USER to push to personal registry
DEV_TAG ?= dev-$(GIT_SHA)
QUAY_USER ?=
DEV_BASE_IMAGE ?= registry.access.redhat.com/ubi9/ubi-minimal:latest

.PHONY: image-dev
image-dev: QUAY_REPO = $(QUAY_USER)
image-dev: IMAGE_TAG = $(DEV_TAG)
image-dev: BASE_IMAGE = $(DEV_BASE_IMAGE)
image-dev: check-quay-user image image-push ## Build and push dev image to dev Quay registry (requires QUAY_USER)

##@ Helm Charts

HELM ?= helm

.PHONY: helm-docs
helm-docs: ## Generate Helm chart README from values.yaml annotations
	$(HELM_DOCS) --chart-search-root=charts --sort-values-order=file

.PHONY: verify-helm-docs
verify-helm-docs: ## Verify chart README is up to date
	$(HELM_DOCS) --chart-search-root=charts --sort-values-order=file
	@git diff --exit-code charts/README.md > /dev/null 2>&1 || \
		(echo "ERROR: charts/README.md is out of date. Run 'make helm-docs' and commit the result." && exit 1)

.PHONY: test-helm
test-helm: verify-helm-docs ## Test Helm charts (lint, template, validate, kubeconform)
	@HELM="$(HELM)" KUBECONFORM="$(KUBECONFORM)" ./scripts/test-helm.sh
