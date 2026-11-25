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
| **Fan-out** | Appending each forwarded event to the Valkey Stream of every user and department its `recipients` list names | Deciding who an event concerns or what it means: each publisher owns its payloads |
| **Streaming** | Backfill from a resume cursor, live tail, heartbeats, the per-user connection cap | Delivery guarantees beyond the retention window: a late client gets a resync marker |
| **Ingestion** | The internal endpoint the publishers' consumers forward to | Consuming SQS, dedup and retries: the forwarding consumer owns them |

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
| `VALKEY_POOL_SIZE` | `20` | Connections for tickets, ingest, counters and readiness |
| `VALKEY_STREAM_POOL_SIZE` | `1000` | Connections for open streams, one each while it waits for events; kept apart so open streams never starve ingest or ticket minting |
| `TICKET_TTL` | `60s` | |
| `TICKET_MINT_RATE_MAX` / `TICKET_MINT_RATE_WINDOW` | `10` / `60s` | Per-user mint rate |
| `STREAM_RETENTION_WINDOW` / `STREAM_KEY_TTL` | `10m` / `15m` | The key lifetime must exceed the retention window |
| `CONNECTION_CAP_PER_USER` | `5` | Concurrent streams per tenant and user |
| `HEARTBEAT_INTERVAL` | `15s` | |
| `ROUTING_CONFIG_PATH` | `config/routing.yaml` | The routing file; the chart mounts it from its `routing` values |
| `CALLER_IDENTITY_CHECK` | `true` | Each routing section accepts only its own callers, read from the mesh's `X-Forwarded-Client-Cert`; `false` only with `APP_ENV` `local`, `dev` or `test` (docker compose and the smoke test) |

A malformed number or duration fails startup rather than falling back to the default.

### Routing file

[config/routing.yaml](config/routing.yaml) decides which events are rendered and to whom: one section per publishing service, matched by the envelope `source`, each with its `callers` (the mesh identities, `<namespace>/<service account>`, allowed to post its events; another caller gets 403), `tenant_admins` (whether its events also reach the tenant's administrators) and its event types. Every routed payload names its readers in a required `recipients` list of `{kind, id}`, `kind` `user` or `department` and `id` a lower-case UUID; another shape is refused with 400. A pair of source and type it does not list is acknowledged with 200 and ignored. The browser receives each event as `<service>.<type>`, for example `workflow.WorkflowTaskCreated`. The image does not contain the file: mount it and set `ROUTING_CONFIG_PATH`, as the chart, docker compose and the smoke test do. The file is read at start-up and a missing or malformed one fails startup; the chart carries the same content as its `routing` values (a test checks they match) and rolls the pods when it changes.

To add a publishing service, add a section with its `source`, `callers`, `tenant_admins` and events to both files, and document its messages in `api/asyncapi.yaml`. Its consumer then needs a caller entry in the chart's `authorizationPolicy.internalRoutes` and, from another namespace, `networkPolicy.otherNamespaceCallers`; a service that uses the shared Workflow-Events Consumer needs its queue and routes in that consumer's topology instead.

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
| `POST /api/v1/internal/events` | The Workflow-Events Consumer and the LLM subsystem's consumer | System caller identity (`x-user-id` and `x-tenant-roles: iam-system`), else `401`; a pair of source and type the routing file does not list is acknowledged with 200 |

Tests fail when the OpenAPI and the router list different operations, or the AsyncAPI channels and the routing file's sections list different event types.

---

## Deployment

One image from the `Dockerfile`, base images pinned by digest (`make pin-base-images`). The chart in `deploy/helm` has a Deployment with hard anti-affinity, a PDB, a NetworkPolicy that names the gateway and the consumer, an Istio `AuthorizationPolicy` (`authorizationPolicy.*`, on by default) that admits every caller outside `/api/v1/internal/*` and, per route group (`authorizationPolicy.internalRoutes`), only that group's service accounts on its exact path: the event consumer's on `/api/v1/internal/events` (a placeholder to confirm with Platform). The render refuses `authorizationPolicy.enabled: false` when `env.APP_ENV` is `staging` or `prod`, unless `authorizationPolicy.allowDisabled: true`; the policy needs a sidecar and mutual TLS on both ends (`PeerAuthentication` is Platform's). The Service ports carry `appProtocol: http`, an optional `ServiceMonitor` and `PrometheusRule`, an optional HPA, and one HTTPRoute with its own SecurityPolicy per exposed path: the ticket route merges with the Gateway's JWT and authorisation filter, the stream route replaces it because the ticket is its only credential. The scaling signal is the open connection count, which needs a custom metrics adapter, so autoscaling is off by default. `make helm-lint` validates the rendered chart and both policies.

## Observability

Every series carries `domain` (`workflow`), `service` (`workflow-stream-gateway`) and `environment`. Platform-shared series come from the libraries, service metrics are `workflow_stream_gateway_*`, and [docs/observability/metric-registry.md](docs/observability/metric-registry.md) is generated from a real scrape. `make metrics-lint` fails on a collector built outside `internal/observability`, a name the standard rejects, an alert naming a retired metric, or a stale inventory.

## Security

- The ticket is the only source of connection identity on the stream route; a client-supplied identity is never read.
- The internal endpoint admits only the system caller identity (`x-tenant-roles` holds `iam-system`, `x-user-id` is set) and caps request bodies at 10 MiB. The headers are not a credential: mesh mutual TLS names the caller, and an Istio `AuthorizationPolicy` admits only the event consumer on `/api/v1/internal/events`. With the policy off, the header check is the only gate besides the NetworkPolicy. The router matches the raw path and answers `404` for an encoded slash, so `/api/v1/internal%2Fevents` reaches no handler. A refused caller is counted in `workflow_stream_gateway_internal_auth_rejected_total{reason}`.
- Per-user connection and mint-rate caps live in Valkey; the connection counter cannot go negative, both counters always carry an expiry, and a refused connect leaves the count unchanged.
- Container: distroless, non-root, read-only root filesystem, all capabilities dropped.

---

## Testing

| Command | What it does |
| --- | --- |
| `make test` | Unit tests with the race detector |
| `make test-integration` | Valkey adapters against a real container |
| `make test-ci` | Both, with merged coverage |
| `make check` | Format, lint, arch-lint, vet, metrics-lint, helm-lint, the design-sync script's test, tests and the coverage gate |
| `make design-sync-check` | Compare the LLD and both specs with their design copies (`DESIGN_DIR=<design checkout>`) |
| `make lint-workflows` / `make vuln` / `make docker-check` | Workflow lint, dependency vulnerabilities, Dockerfile lint and scan |
| `make smoke-test` | Build the image and check it end to end against a real Valkey (needs `GO_PRIVATE_TOKEN`) |

## CI

Pull requests and pushes run `ci.yml`: a `ci-gate` job decides whether to skip (a draft, a change to documentation only, or the `skip-ci` label), then `validate-quality` and `validate-test`, an image job that lints the Dockerfile and builds the image, a Trivy scan for fixable HIGH and CRITICAL findings, and a smoke test (`make smoke-test`) that runs the image against Valkey, mints a ticket, opens a stream and checks that an ingested event reaches it. A skipped run still reports every required check as skipped, so nothing is left waiting, and marking a draft ready starts CI. `changelog-check.yml` requires a CHANGELOG entry on pull requests that touch code or contracts.

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
