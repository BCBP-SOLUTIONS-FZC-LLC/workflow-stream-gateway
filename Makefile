SHELL := /bin/bash

ifneq ($(wildcard .env),)
  include .env
  export
endif

GOLANGCI_VERSION    := v2.12.2
GOVULNCHECK_VERSION := v1.1.4
ARCHLINT_VERSION     := v1.11.4
HADOLINT_VERSION    := v2.12.0
TRIVY_VERSION       := 0.71.2

TOOLS_DIR    := .tools
BIN_DIR      := bin
COVERAGE_DIR := .coverage
MODULE       := github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway

GOLANGCI    := $(TOOLS_DIR)/golangci-lint
GOVULNCHECK := $(TOOLS_DIR)/govulncheck
ARCHLINT    := $(TOOLS_DIR)/go-arch-lint
HADOLINT    := $(TOOLS_DIR)/hadolint
TRIVY       := $(TOOLS_DIR)/trivy

BUILD_VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS       := -X main.version=$(BUILD_VERSION)

COVER_PROFILE   := $(COVERAGE_DIR)/coverage.out
COVER_HTML      := $(COVERAGE_DIR)/coverage.html
COVER_THRESHOLD := 90

.PHONY: all tools setup install-hooks build dev test test-ci \
        cover cover-func cover-html cover-check \
        tidy fmt-check fix check lint lint-fix arch-lint vuln \
        docker-build docker-lint docker-trivy docker-check \
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
	GOBIN=$(PWD)/$(TOOLS_DIR) go install github.com/fe3dback/go-arch-lint@$(ARCHLINT_VERSION)
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

## test-ci: Run the unit suite (alias kept for CI-workflow parity with sibling repos)
test-ci: test
	@cp $(COVERAGE_DIR)/unit.out $(COVER_PROFILE)

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

## cover-check: Run test-ci and gate on the global coverage threshold
cover-check: test-ci
	@pct=$$(go tool cover -func=$(COVER_PROFILE) | awk '/^total:/{gsub("%","",$$NF); print $$NF}'); \
	echo "total: $$pct% (threshold $(COVER_THRESHOLD)%)"; \
	awk -v p="$$pct" -v t="$(COVER_THRESHOLD)" 'BEGIN { exit !(p+0 >= t+0) }' || \
	  { echo "✗ coverage below threshold"; exit 1; }
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
	$(ARCHLINT) check --project-path .
	go vet ./cmd/... ./internal/...
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
	$(ARCHLINT) check --project-path .

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

## docker-check: Dockerfile lint + dependency CVE scan (no image build required)
docker-check: docker-lint docker-trivy
	@echo "✓ all container checks passed"

## clean: Remove build and coverage artifacts
clean:
	rm -rf $(BIN_DIR) $(COVERAGE_DIR)

## help: Show this help
help:
	@grep -E '^## ' Makefile | sed 's/## /  /'
