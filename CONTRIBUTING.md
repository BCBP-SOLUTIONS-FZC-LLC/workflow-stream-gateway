# Contributing to workflow-stream-gateway

## Prerequisites

| Tool | Install |
| --- | --- |
| Go 1.26+ | [go.dev/dl](https://go.dev/dl) |
| Docker, for integration tests and `docker compose` | [docs.docker.com](https://docs.docker.com/get-docker/) |
| Private module access | `go env -w GOPRIVATE=github.com/BCBP-SOLUTIONS-FZC-LLC/*` and SSH access to the organisation |

## Workflow

```bash
make tools && make setup
make check          # what CI gates on, locally
```

The pre-commit hook runs tidy, format, lint and arch-lint.

## Conventions

- Dependency direction is `domain ← usecase ← adapter`; `make arch-lint` enforces it.
- Non-test code carries no comments except `//go:` and `//nolint` directives.
- Logs, metrics and traces go through `platform-gincommon`, and events through `platform-events`. New metrics are `workflow_stream_gateway_*` collectors built in `internal/observability`, and `make metrics-lint` must pass.
- A fix starts with a test of the correct behaviour that fails, then the change.
- Never add a `replace` directive to `go.mod`.

## Adding a consumed event type

Add the type to the routing in the fan-out use case, its message to `api/asyncapi.yaml`, and its route in the consumer's topology; the parity test fails until the first two agree. A new recipient dimension needs its own Valkey key shape in the LLD.

## Specifications and documents

A change to a route updates `docs/swagger/openapi.yaml`. A change to behaviour updates the LLD, the README, `ARCHITECTURE.md` and the CHANGELOG in the same pull request; the in-repo LLD and the OpenAPI stay content-identical to the design repository copies.

## Pull requests

Open pull requests as drafts while iterating, fill every section of the template, and add a CHANGELOG entry under `[Unreleased]` for any change to code or contracts. Tags are cut after merge, never inside a pull request.
