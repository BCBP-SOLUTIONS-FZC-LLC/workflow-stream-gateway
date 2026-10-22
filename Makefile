SHELL := /bin/bash

ifneq ($(wildcard .env),)
  include .env
  export
endif

GOLANGCI_VERSION    := v2.12.2
GOVULNCHECK_VERSION := v1.1.4
GOARCHLINT_VERSION   := v1.15.0
HADOLINT_VERSION    := v2.12.0
TRIVY_VERSION       := 0.71.2
KUBECONFORM_VERSION := 0.8.0
ACTIONLINT_VERSION  := v1.7.12

TOOLS_DIR    := .tools
BIN_DIR      := bin
COVERAGE_DIR := .coverage
MODULE       := github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway

GOLANGCI    := $(TOOLS_DIR)/golangci-lint
GOVULNCHECK := $(TOOLS_DIR)/govulncheck
GOARCHLINT  := $(TOOLS_DIR)/go-arch-lint
HADOLINT    := $(TOOLS_DIR)/hadolint
TRIVY       := $(TOOLS_DIR)/trivy
KUBECONFORM := $(TOOLS_DIR)/kubeconform
ACTIONLINT  := $(TOOLS_DIR)/actionlint
HELM        ?= helm

BUILD_VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS       := -X main.version=$(BUILD_VERSION)

COVER_PROFILE   := $(COVERAGE_DIR)/coverage.out
COVER_HTML      := $(COVERAGE_DIR)/coverage.html
COVER_THRESHOLD := 90

# internal/observability keeps the error-wrapping branches of the telemetry
# initialisation, which fail only when a platform library refuses its own identity.
COVER_PKG_FLOORS := internal/observability:85

.PHONY: test-integration merge-coverage helm-lint lint-workflows pin-base-images metrics-lint metrics-inventory all tools setup install-hooks build dev test test-ci \
        cover cover-func cover-html cover-check cover-check-pkg \
        tidy fmt-check fix check lint lint-fix arch-lint vuln \
        docker-build docker-lint docker-trivy docker-check smoke-test \
        clean help

all: build

## setup: Copy .env.example -> .env (if missing) and install git hooks
setup:
	@[ -f .env ] || cp .env.example .env
	@$(MAKE) install-hooks

## install-hooks: Install .githooks/pre-commit into .git/hooks
install-hooks:
	@mkdir -p .git/hooks
	@cp .githooks/pre-commit .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "✓ installed git hooks"

## tools: Install dev tooling into .tools/
tools:
	@mkdir -p $(TOOLS_DIR)
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh \
		| sh -s -- -b $(PWD)/$(TOOLS_DIR) $(GOLANGCI_VERSION)
	GOBIN=$(PWD)/$(TOOLS_DIR) go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
	GOBIN=$(PWD)/$(TOOLS_DIR) go install github.com/fe3dback/go-arch-lint@$(GOARCHLINT_VERSION)
	@OS=$$(uname -s); ARCH=$$(uname -m); \
	if [ "$$OS" = "Darwin" ]; then \
	    if command -v brew >/dev/null 2>&1; then \
	        brew install hadolint >/dev/null && cp "$$(brew --prefix)/bin/hadolint" $(TOOLS_DIR)/hadolint; \
	    else \
	        echo "hadolint: brew not found on macOS — install manually: brew install hadolint"; exit 1; \
	    fi; \
	else \
	    curl -sLo $(TOOLS_DIR)/hadolint \
	      "https://github.com/hadolint/hadolint/releases/download/$(HADOLINT_VERSION)/hadolint-$$OS-$$ARCH" && \
	    chmod +x $(TOOLS_DIR)/hadolint; \
	fi
	@OS=$$(uname -s); ARCH=$$(uname -m); \
	if [ "$$OS" = "Darwin" ] && [ "$$ARCH" = "arm64" ]; then TRIVY_OS_ARCH="macOS-ARM64"; \
	elif [ "$$OS" = "Darwin" ]; then TRIVY_OS_ARCH="macOS-64bit"; \
	elif [ "$$ARCH" = "aarch64" ] || [ "$$ARCH" = "arm64" ]; then TRIVY_OS_ARCH="Linux-ARM64"; \
	else TRIVY_OS_ARCH="Linux-64bit"; fi; \
	curl -sLo /tmp/trivy.tar.gz \
	  "https://github.com/aquasecurity/trivy/releases/download/v$(TRIVY_VERSION)/trivy_$(TRIVY_VERSION)_$$TRIVY_OS_ARCH.tar.gz" && \
	tar -xzf /tmp/trivy.tar.gz -C $(TOOLS_DIR) trivy && rm /tmp/trivy.tar.gz
	GOBIN=$(PWD)/$(TOOLS_DIR) go install github.com/rhysd/actionlint/cmd/actionlint@$(ACTIONLINT_VERSION)
	@OS=$$(uname -s | tr '[:upper:]' '[:lower:]'); ARCH=$$(uname -m); \
	if [ "$$ARCH" = "aarch64" ]; then ARCH="arm64"; fi; \
	if [ "$$ARCH" = "x86_64" ]; then ARCH="amd64"; fi; \
	curl -sLo /tmp/kubeconform.tar.gz \
	  "https://github.com/yannh/kubeconform/releases/download/v$(KUBECONFORM_VERSION)/kubeconform-$$OS-$$ARCH.tar.gz" && \
	tar -xzf /tmp/kubeconform.tar.gz -C $(TOOLS_DIR) kubeconform && rm /tmp/kubeconform.tar.gz
	@echo "✓ tools installed to $(TOOLS_DIR)/"

## build: Compile the server binary to bin/
build:
	@mkdir -p $(BIN_DIR)
	go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/workflow-stream-gateway ./cmd/server
	@echo "✓ binary: $(BIN_DIR)/workflow-stream-gateway"

## dev: Run the service locally (requires .env populated, or docker-compose's Valkey)
dev:
	go run ./cmd/server

## test: Run unit tests with race detector and coverage
test:
	@mkdir -p $(COVERAGE_DIR)
	go test -race -count=1 \
	    -coverpkg=./internal/... \
	    -coverprofile=$(COVERAGE_DIR)/unit.out -covermode=atomic \
	    ./internal/...
	@go tool cover -func=$(COVERAGE_DIR)/unit.out | awk '/^total:/{print "total:", $$NF}'

## test-integration: Run integration tests; containers start through testcontainers-go
test-integration:
	@mkdir -p $(COVERAGE_DIR)
	TESTCONTAINERS_RYUK_DISABLED=true \
	go test -race -count=1 -tags integration \
	    -coverpkg=./internal/... \
	    -coverprofile=$(COVERAGE_DIR)/integration.out -covermode=atomic \
	    ./test/integration/...

## merge-coverage: Merge the unit and integration profiles into coverage.out
merge-coverage:
	@python3 scripts/merge_coverage.py $(COVERAGE_DIR)/unit.out $(COVERAGE_DIR)/integration.out | grep -v "/obstest/" > $(COVER_PROFILE)
	@echo "✓ $(COVER_PROFILE) merged from unit + integration suites"

## test-ci: Run unit and integration suites with merged coverage
test-ci: test test-integration merge-coverage
	@go tool cover -func=$(COVER_PROFILE) | awk '/^total:/{print "total:", $$NF}'

## cover: Print total coverage from the last test-ci run
cover:
	@[ -f $(COVER_PROFILE) ] || { echo "no profile — run 'make test-ci' first"; exit 1; }
	@go tool cover -func=$(COVER_PROFILE) | awk '/^total:/{print "total:", $$NF}'

## cover-func: Print per-function coverage breakdown
cover-func:
	@[ -f $(COVER_PROFILE) ] || { echo "no profile — run 'make test-ci' first"; exit 1; }
	@go tool cover -func=$(COVER_PROFILE)

## cover-html: Open an HTML coverage report
cover-html:
	@[ -f $(COVER_PROFILE) ] || { echo "no profile — run 'make test-ci' first"; exit 1; }
	go tool cover -html=$(COVER_PROFILE) -o $(COVER_HTML)
	@open $(COVER_HTML) 2>/dev/null || xdg-open $(COVER_HTML) 2>/dev/null || true

## cover-check-pkg: Per-package coverage gate — each package must meet its floor (run 'make test-ci' first)
cover-check-pkg:
	@[ -f $(COVER_PROFILE) ] || { echo "no profile — run 'make test-ci' first"; exit 1; }
	@awk \
	  -v module="$(MODULE)/" \
	  -v floors="$(subst \,,$(COVER_PKG_FLOORS))" \
	  -v global="$(COVER_THRESHOLD)" \
	  'BEGIN { \
	    n=split(floors,pairs," "); \
	    for(i=1;i<=n;i++){split(pairs[i],kv,":");thresh[kv[1]]=kv[2]+0} \
	  } \
	  /^mode:/{next} \
	  { \
	    key=$$1; stmts=$$2+0; count=$$3+0; \
	    blk_stmts[key]=stmts; blk_count[key]+=count; \
	    path=key; sub(/:.*$$/,"",path); sub(module,"",path); sub(/\/[^\/]+$$/,"",path); \
	    blk_pkg[key]=path \
	  } \
	  END { \
	    for(key in blk_stmts){ \
	      pkg=blk_pkg[key]; \
	      tot[pkg]+=blk_stmts[key]; \
	      if(blk_count[key]>0) cov[pkg]+=blk_stmts[key] \
	    } \
	    fail=0; \
	    for(pkg in tot){ \
	      if(tot[pkg]==0)continue; \
	      pct=cov[pkg]*100/tot[pkg]; \
	      floor=(pkg in thresh)?thresh[pkg]:global; \
	      if(pct<floor){printf "✗  %-58s %5.1f%% (need %d%%)\n",pkg,pct,floor; fail=1} \
	      else{printf "✓  %-58s %5.1f%%\n",pkg,pct} \
	    } \
	    exit fail \
	  }' $(COVER_PROFILE)

## cover-check: Per-package coverage gate (runs the unit suite automatically)
cover-check: test-ci cover-check-pkg
	@go tool cover -func=$(COVER_PROFILE) | awk '/^total:/{print "total (informational):", $$NF}'
	@echo "✓ coverage ok"

## tidy: Run go mod tidy
tidy:
	go mod tidy

## fmt-check: Verify gofmt formatting (read-only)
fmt-check:
	@files=$$(gofmt -l cmd/ internal/); if [ -n "$$files" ]; then echo "gofmt violations (run 'make fix'):"; echo "$$files"; exit 1; fi

## fix: Auto-fix formatting and lint issues
fix:
	gofmt -w cmd/ internal/
	$(GOLANGCI) run --fix ./...

## check: Full local CI pass — fmt, lint, arch-lint, vet, coverage-gated tests
check:
	@$(MAKE) fmt-check
	$(GOLANGCI) run ./...
	$(GOARCHLINT) check --project-path .
	go vet ./cmd/... ./internal/...
	@$(MAKE) metrics-lint
	@$(MAKE) helm-lint
	@$(MAKE) cover-check
	@echo "✓ all checks passed"

## lint: Run golangci-lint (read-only)
lint:
	$(GOLANGCI) run ./...

## lint-fix: Run golangci-lint with auto-fix
lint-fix:
	$(GOLANGCI) run --fix ./...

## arch-lint: Enforce the domain/usecase/adapter/cmd import-direction rules
arch-lint:
	$(GOARCHLINT) check --project-path .

## metrics-lint: Observability standard: collector naming, metricslint on a real scrape, alert and doc references, inventory drift
metrics-lint:
	@bash .github/scripts/check-metric-naming.sh
	@bash .github/scripts/metricslint.sh

## metrics-inventory: Regenerate docs/observability/metric-registry.md from the registry and a real scrape
metrics-inventory:
	@METRICS_INVENTORY_WRITE=1 bash .github/scripts/metricslint.sh

## lint-workflows: Lint GitHub Actions workflow YAML
lint-workflows:
	$(ACTIONLINT)

HELM_LINT_VALUES := --set env.VALKEY_ADDR=valkey:6379 \
	  --set-string secretValues.VALKEY_PASSWORD=placeholder \
	  --set-string secretValues.WORKFLOW_STREAM_GATEWAY_TOKEN=placeholder \
	  --set prometheusRule.enabled=true

## helm-lint: Lint the Helm chart and validate its rendered manifests against the Kubernetes API schema
helm-lint:
	$(HELM) lint deploy/helm $(HELM_LINT_VALUES)
	@mkdir -p $(TOOLS_DIR)
	$(HELM) template deploy/helm $(HELM_LINT_VALUES) > $(TOOLS_DIR)/helm-rendered.yaml
	$(KUBECONFORM) -strict -skip PrometheusRule,ServiceMonitor,HTTPRoute,SecurityPolicy $(TOOLS_DIR)/helm-rendered.yaml
	@echo "✓ helm chart valid"

## pin-base-images: Resolve current digests for the Dockerfile base images and pin them
pin-base-images:
	@GOLANG_DIGEST=$$(docker buildx imagetools inspect golang:1.26-alpine | awk '/^Digest:/{print $$2; exit}'); \
	DISTROLESS_DIGEST=$$(docker buildx imagetools inspect gcr.io/distroless/static-debian12:nonroot | awk '/^Digest:/{print $$2; exit}'); \
	sed -E -i.bak "s|^(FROM golang:1\.26-alpine)(@sha256:[0-9a-f]+)?( AS [A-Za-z0-9_-]+)?|\\1@$$GOLANG_DIGEST\\3|" Dockerfile; \
	sed -E -i.bak "s|^(FROM gcr\.io/distroless/static-debian12:nonroot)(@sha256:[0-9a-f]+)?( AS [A-Za-z0-9_-]+)?|\\1@$$DISTROLESS_DIGEST\\3|" Dockerfile; \
	rm -f Dockerfile.bak; \
	echo "Pinned base images"

## vuln: Run govulncheck against dependencies
vuln:
	$(GOVULNCHECK) ./...

IMAGE_TAG ?= local

## docker-build: Build the workflow-stream-gateway container image (requires GO_PRIVATE_TOKEN in env)
docker-build:
	docker buildx build \
	  --secret id=go_private_token,env=GO_PRIVATE_TOKEN \
	  --build-arg BUILD_VERSION=$(IMAGE_TAG) \
	  --load \
	  -t workflow-stream-gateway:$(IMAGE_TAG) .

## docker-lint: Lint the Dockerfile with hadolint
docker-lint:
	$(HADOLINT) --config .hadolint.yaml Dockerfile

## docker-trivy: Scan source and dependencies for HIGH/CRITICAL CVEs
docker-trivy:
	$(TRIVY) fs . --severity HIGH,CRITICAL --ignore-unfixed --exit-code 1

## smoke-test: Build the image and check it end to end against a real Valkey (requires GO_PRIVATE_TOKEN in env)
smoke-test:
	bash scripts/smoke-test.sh

## docker-check: Dockerfile lint + dependency CVE scan (no image build required)
docker-check: docker-lint docker-trivy
	@echo "✓ all container checks passed"

## clean: Remove build and coverage artifacts
clean:
	rm -rf $(BIN_DIR) $(COVERAGE_DIR)

## help: Show this help
help:
	@grep -E '^## ' Makefile | sed 's/## /  /'
