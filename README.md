# workflow-stream-gateway

Real-time Server-Sent Events (SSE) dashboard push for the Workflow Engine platform. Projects Execution Service's outbound domain events to the browser so a tenant's dashboard reflects task/instance state changes without polling.

Full design: `design/LLD/workflow_stream_gateway.md` (this repo implements that document; see it for the exact Valkey key schema, REST contracts, and design rationale). See also `ARCHITECTURE.md` (once written) and `CONTRIBUTING.md`.

This service is stateless — Valkey is its only store (tickets, per-user event streams, connection/rate counters) — and runs no SQS/SNS consumer of its own. Event ingestion arrives via the platform's shared Workflow-Events Consumer (`event_consumer`) through a standard `POST /internal/events` handler.

## Configuration

| Env var | Default | Notes |
|---|---|---|
| `APP_ENV` | `dev` | `prod` enforces `WORKFLOW_STREAM_GATEWAY_TOKEN` non-empty |
| `HTTP_PORT` | `8080` | |
| `METRICS_PORT` | `9090` | `/metrics`, on a separate listener from the API port — NetworkPolicy scopes each independently |
| `VALKEY_ADDR` | `localhost:6379` | |
| `VALKEY_PASSWORD` | (empty) | |
| `VALKEY_DIAL_TIMEOUT` / `VALKEY_READ_TIMEOUT` / `VALKEY_WRITE_TIMEOUT` | `2s` / `1s` / `1s` | |
| `WORKFLOW_STREAM_GATEWAY_TOKEN` | (empty) | Guards `POST /internal/events` (`x-internal-token`); empty disables the check — dev only |
| `TICKET_TTL` | `60s` | SSE ticket lifetime (LLD §5.1) |
| `TICKET_MINT_RATE_MAX` / `TICKET_MINT_RATE_WINDOW` | `10` / `60s` | Per-user ticket-mint rate cap (LLD §9.3) |
| `STREAM_RETENTION_WINDOW` | `10m` | Backfill window (LLD §8) |
| `STREAM_KEY_TTL` | `15m` | Must exceed `STREAM_RETENTION_WINDOW` |
| `CONNECTION_CAP_PER_USER` | `5` | Concurrent SSE connections per `(tenant_id, user_id)` (LLD §9.3) |
| `HEARTBEAT_INTERVAL` | `15s` | SSE keep-alive comment interval (LLD §7) |

## Local development

```
docker compose up
```

Brings up Valkey and the service. `curl localhost:8080/healthz` should return `200` once Valkey is reachable.

To run against a Valkey you already have locally: `make dev` (reads `.env` if present).

## Running tests

```
make test       # unit tests, race detector, coverage
make cover      # print total coverage from the last run
```

## Other Makefile targets

Run `make help` for the full list — `check` (fmt + lint + arch-lint + vet + coverage gate), `lint`/`lint-fix`, `arch-lint`, `vuln`, `docker-build`/`docker-lint`/`docker-trivy`/`docker-check`.

## CI

`.github/workflows/ci.yml` fans out to `validate-quality.yml` (fmt, tidy, vet, lint, arch-lint, vuln) and `validate-test.yml` (build, test, coverage gate).

## Deployment

`deploy/helm/` — Helm chart (`helm lint`/`helm template` clean, resources schema-validated via `kubeconform`).

- **NetworkPolicy** is a named-service ingress allow-list, not a blanket same-namespace `podSelector: {}` — only the gateway/ingress-controller namespace and `event_consumer`'s own pod selector may reach the API port; the monitoring namespace may reach the separate metrics port and nothing else.
- **`/metrics`** is served on its own port (`METRICS_PORT`) from day one, matching `iam-user-profile`'s pattern rather than `definition_service`'s shared-port one.
- **`GET /api/v1/stream`** is included in the optional `HTTPRoute` (`ingress.enabled: true`) alongside `POST /api/v1/stream/tickets` — both need to route through the same gateway. `POST /internal/events` is never in the `HTTPRoute`; `event_consumer` reaches it in-cluster via the `Service` directly. Carving `GET /api/v1/stream` out of the gateway's standard JWT/`ext_authz` chain (LLD §5.2/§10.3) is Gateway-level configuration outside this chart entirely — a cross-team item (LLD Appendix B), not something any service's own `HTTPRoute`/`SecurityPolicy` resolves.
- **`SecurityPolicy`** (optional, CORS only) deliberately has no rate-limit knob — Envoy Gateway configures that via a separate `BackendTrafficPolicy` CRD, not a field on `SecurityPolicy`; both `definition_service`'s and `iam-user-profile`'s own charts carry that same incorrect field (harmless only because it's disabled by default there too).
- **Autoscaling** is disabled by default: this service scales on open SSE connection count (LLD §10.2), not CPU/RPS, and the exact metric/target is an open LLD item — see `values.yaml`'s `autoscaling` comment before enabling it.
