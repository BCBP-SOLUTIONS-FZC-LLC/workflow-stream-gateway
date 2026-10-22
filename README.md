# workflow-stream-gateway

Real-time dashboard push for the BCBP Workflow Engine: it projects the Execution Service's task and instance events to the browser as Server-Sent Events, so a dashboard reflects state changes without polling.

**Repository:** `github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-stream-gateway`  
**Module:** Go 1.26.10 · private module · a containerised microservice not yet deployed to any live environment

---

## Mental model

```text
Execution Service ──SNS──► wf-workflow-dash-q ──► Workflow-Events Consumer ──HTTP──► Stream Gateway ──► Valkey Streams
Browser ──ticket mint──► Stream Gateway ◄──SSE stream (ticket)── Browser
```

| Concern | What this service owns | What it does NOT own |
| --- | --- | --- |
| **Tickets** | Minting a short-lived ticket from the gateway identity headers, rate-limited per user | Authentication: the gateway validates the JWT and injects the identity |
| **Fan-out** | Appending each forwarded event to the Valkey Stream of every recipient it concerns | Deciding what an event means: the Execution Service owns the payloads |
| **Streaming** | Backfill from a resume cursor, live tail, heartbeats, the per-user connection cap | Delivery guarantees beyond the retention window: a late client gets a resync marker |
| **Ingestion** | The internal endpoint the shared consumer forwards to | Consuming SQS, dedup and retries: the Workflow-Events Consumer owns them |

**Source of truth for two contracts:** the HTTP surface in [docs/swagger/openapi.yaml](docs/swagger/openapi.yaml) and the consumed event types in [api/asyncapi.yaml](api/asyncapi.yaml).

---

## Why this service exists

A dashboard needs push, and a browser `EventSource` cannot set an `Authorization` header. Keeping a small stateless service for ticketed long-lived connections isolates their scaling signal, open connection count, from the request/response services, and keeps the Execution Service free of per-user stream state.

It is stateless. Valkey is the only store, it publishes no events and keeps no outbox, and it has no database.

---

## Contents

- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Project layout](#project-layout) and [Architecture](#architecture)
- [API overview](#api-overview)
- [Deployment](#deployment), [Observability](#observability), [Security](#security)
- [Testing](#testing), [CI](#ci), [Versioning and releases](#versioning-and-releases)
- [Out of scope](#out-of-scope) and [See also](#see-also)

> **Architecture** — [ARCHITECTURE.md](ARCHITECTURE.md). **Contributing** — [CONTRIBUTING.md](CONTRIBUTING.md).

---

## Private Module Access

This service consumes private Go modules from the `github.com/BCBP-SOLUTIONS-FZC-LLC/*` organization (`platform-events/v2`, `platform-gincommon`).

```bash
go env -w GOPRIVATE=github.com/BCBP-SOLUTIONS-FZC-LLC/*
git config --global url."ssh://git@github.com/".insteadOf "https://github.com/"
```

---

## Quick start

```bash
make tools     # pinned tooling
make setup     # .env from .env.example, pre-commit hook
docker compose up valkey
make dev       # reads .env
```

`docker compose up` starts Valkey and the service. `/healthz` answers as soon as the process is up, with no Valkey dependency; `/readyz` answers once Valkey is reachable, so a Valkey blip removes the pod from rotation without restarting it.

## Configuration

`.env.example` lists every variable.

| Variable | Default | Notes |
| --- | --- | --- |
| `APP_ENV` | `dev` | One of `local`, `dev`, `test`, `staging`, `prod`; any other value fails startup |
| `HTTP_PORT` / `METRICS_PORT` | `8080` / `9090` | `/metrics` has its own listener, so network policy scopes each port |
| `OTEL_SERVICE_NAME` | `workflow-stream-gateway` | The `service` metric label and trace service name |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | A collector address; `none` or unset disables export |
| `VALKEY_ADDR` / `VALKEY_PASSWORD` | `localhost:6379` / empty | |
| `VALKEY_DIAL_TIMEOUT` / `_READ_TIMEOUT` / `_WRITE_TIMEOUT` | `2s` / `1s` / `1s` | |
| `WORKFLOW_STREAM_GATEWAY_TOKEN` | empty | Guards the internal endpoint with `x-internal-token`; required outside dev and local |
| `TICKET_TTL` | `60s` | |
| `TICKET_MINT_RATE_MAX` / `TICKET_MINT_RATE_WINDOW` | `10` / `60s` | Per-user mint rate |
| `STREAM_RETENTION_WINDOW` / `STREAM_KEY_TTL` | `10m` / `15m` | The key lifetime must exceed the retention window |
| `CONNECTION_CAP_PER_USER` | `5` | Concurrent streams per tenant and user |
| `HEARTBEAT_INTERVAL` | `15s` | |

A malformed number or duration fails startup rather than falling back to the default.

---

## Project layout

```text
cmd/server                       composition root
internal/domain                  ticket and frame types
internal/usecase                 ticket mint and validation, fan-out, stream serving
internal/adapter/inbound/http    router, handlers, middleware
internal/adapter/outbound/valkey tickets, streams, counters
internal/observability           metrics and telemetry initialisation
internal/config                  environment loading and validation
api/asyncapi.yaml, docs/swagger  consumed events, HTTP surface
deploy/helm                      chart
test/integration                 adapters against a real Valkey
```

## Architecture

Dependency direction is `domain ← usecase ← adapter`, enforced by `.go-arch-lint.yml`. `platform-gincommon` owns logging, tracing, HTTP middleware and every metric constructor; `platform-events` owns the event envelope this service parses.

---

## API overview

| Route | Caller | Notes |
| --- | --- | --- |
| `POST /api/v1/stream/tickets` | Browser through the gateway | Gateway identity headers required |
| `GET /api/v1/stream` | Browser `EventSource` | Authenticated by the ticket in the query; resumes from `Last-Event-ID` |
| `POST /api/v1/internal/events` | The Workflow-Events Consumer | `x-internal-token`; unknown types are acknowledged with 200 |

Tests fail when the OpenAPI and the router list different operations, or the AsyncAPI and the routed event types differ.

---

## Deployment

One image from the `Dockerfile`, base images pinned by digest (`make pin-base-images`). The chart in `deploy/helm` has a Deployment with hard anti-affinity, a PDB, a NetworkPolicy that names the gateway and the consumer, an optional `ServiceMonitor` and `PrometheusRule`, and an optional HPA. The scaling signal is the open connection count, which needs a custom metrics adapter, so autoscaling is off by default. `make helm-lint` validates the rendered chart.

## Observability

Every series carries `domain` (`workflow`), `service` (`workflow-stream-gateway`) and `environment`. Platform-shared series come from the libraries, service metrics are `workflow_stream_gateway_*`, and [docs/observability/metric-registry.md](docs/observability/metric-registry.md) is generated from a real scrape. `make metrics-lint` fails on a collector built outside `internal/observability`, a name the standard rejects, an alert naming a retired metric, or a stale inventory.

## Security

- The ticket is the only source of connection identity on the stream route; a client-supplied identity is never read.
- The internal endpoint compares `x-internal-token` in constant time and caps request bodies at 10 MiB.
- Per-user connection and mint-rate caps live in Valkey; the connection counter cannot go negative.
- Container: distroless, non-root, read-only root filesystem, all capabilities dropped.

---

## Testing

| Command | What it does |
| --- | --- |
| `make test` | Unit tests with the race detector |
| `make test-integration` | Valkey adapters against a real container |
| `make test-ci` | Both, with merged coverage |
| `make check` | Format, lint, arch-lint, vet, metrics-lint, helm-lint, tests and the coverage gate |
| `make lint-workflows` / `make vuln` / `make docker-check` | Workflow lint, dependency vulnerabilities, Dockerfile lint and scan |

## CI

Pull requests and pushes run `ci.yml`: a `ci-gate` job skips draft pull requests and those labelled `skip-ci`, then `validate-quality` and `validate-test`. Pull requests that change only Markdown or `docs/` skip CI. `changelog-check.yml` requires a CHANGELOG entry on pull requests that touch code or contracts.

## Versioning and releases

Releases are Git tags `vMAJOR.MINOR.PATCH` cut on the merged commit after the CHANGELOG `[Unreleased]` section moves to a versioned heading. The `Release` workflow refuses a tag with no CHANGELOG entry, builds, scans and signs the image, and creates the GitHub Release. See [VERSIONING.md](VERSIONING.md) and [CHANGELOG.md](CHANGELOG.md).

---

## Out of scope

| Concern | Owner |
| --- | --- |
| Event payload schemas | Workflow Execution Service |
| SQS consumption, dedup, retries | The shared Workflow-Events Consumer |
| JWT validation and identity headers | The platform gateway |
| Tenant membership and roles | Org & Membership Service |

This repository carries no `LICENSE` file: it is private and proprietary to BCBP Solutions.

## See also

- [ARCHITECTURE.md](ARCHITECTURE.md) · [CONTRIBUTING.md](CONTRIBUTING.md) · [VERSIONING.md](VERSIONING.md) · [CHANGELOG.md](CHANGELOG.md)
- The stream-gateway LLD in [docs/lld/](docs/lld/), content-identical to the design repository copy
