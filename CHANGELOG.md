# Changelog

All notable changes to `workflow-stream-gateway` are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

- The LLD counts nine task-scoped and seven instance-scoped event types in its stream and fan-out sections, as the code does (LLD rev 1.13).
- The gateway renders sixteen Execution Service event types, up from eleven: `WorkflowTaskClaimed`, `WorkflowTaskSuperseded` and `WorkflowTaskFailed` fan out to their assignees and department, and `WorkflowInstanceDegraded` and `WorkflowInstanceFailed` go to the instance's starter. `WorkflowInstanceStarted` and `WorkflowInstanceForceRouted` stay unknown types (LLD rev 1.12).
- Problem documents carry `trace_id` and `request_id`, equal to the `X-Trace-ID` and `X-Request-ID` response headers; the OpenAPI declares them (LLD rev 1.11).
- A path no route matches answers a `not-found` problem document instead of plain text.
- The eight defect-class linters run (`bodyclose`, `noctx`, `nilerr`, `gosec`, `contextcheck`, `gocritic`, `nolintlint`, `sqlclosecheck`), with build tags; two deliberate nil returns on a client closing its stream carry their reason.
- Every alert carries a `runbook` annotation naming its entry in `docs/observability/runbooks.md`; a test holds each to a documented entry.
- `make design-sync-check` (local, `DESIGN_DIR`) compares the LLD and both specs with their design copies; `make check` runs the script's own test.
- `workflow_stream_gateway_*` collectors built through `platform-gincommon`, with the `platform_*` series from the platform libraries in every scrape; `make metrics-lint`, a generated `docs/observability/metric-registry.md` and a scrape test.
- `api/asyncapi.yaml` documenting the 11 consumed event types, with tests that fail when the OpenAPI differs from the router or the AsyncAPI from the routed event types.
- A 10 MiB request body limit on the ticket and internal ingest routes (`413`).
- A connection-count refresh on every wake-up of an open stream, and integration tests of every Valkey adapter against a real Valkey.
- A `PrometheusRule` in the chart (availability, errors, panics, Valkey outage, ingest errors, cap rejections), the `OTEL_*` settings, `helm-lint`, `lint-workflows` and a Dockerfile digest pin check.
- A release workflow (changelog gate, image build, Cosign signature, GitHub Release), a changelog check, CODEOWNERS, issue and pull request templates, `.editorconfig`, a pre-commit hook, ARCHITECTURE and VERSIONING.

### Fixed

- The stream route's policy renders whenever the routes are exposed, with CORS only when `ingress.securityPolicy.cors.enabled`; it rendered only with CORS, so a same-origin deployment left the stream route behind the gateway's JWT and every `EventSource` got a 401. The stream route also sets `timeouts.request: 0s`, since the gateway's default 15 s request timeout ends a stream.
- `docs/swagger/openapi.yaml` documents `GET /healthz` and `GET /readyz`, as Definition's and Execution's specs do, and the parity test compares them with the router instead of skipping them.
- The Dockerfile left `~/.git-credentials`, with `GO_PRIVATE_TOKEN`, in a builder layer, and the release build exported that layer to a registry cache. The credential is written, used by `go mod download` and removed in one `RUN`, the release build no longer reads or exports a registry cache, and `make docker-lint` and the CI image job run `scripts/dockerfile-credentials-check.sh`. Builder caches of earlier releases held the token, so `GO_PRIVATE_TOKEN` needs rotating.
- `docs/swagger/openapi.yaml` documents the `401` the ticket mint answers when the gateway identity headers are missing; a test holds the documented statuses to the router's answer.
- The chart's one SecurityPolicy covered both routes and set no merge type, so enabling it replaced the Gateway's JWT validation and authorisation filter on the ticket route as well. The ticket and stream paths are now separate HTTPRoutes: the ticket policy merges with the Gateway's, the stream policy replaces it (the ticket is that route's only credential). `ingress.hosts[].paths` is replaced by `ingress.paths.tickets` and `ingress.paths.stream`; `make helm-policy-check`, part of `helm-lint`, holds both properties.

### Changed

- CI: the coverage step runs `make cover-check-pkg`, so each package is held to its floor (it only printed the total before). The PR summary comment shows the total without a made-up limit.
- The `PR summary` job posts the sticky pull request comment with the job results and the coverage figure (`.github/scripts/pr-summary.js`, as the other workflow repositories do); it only printed a line before. The test job uploads the merged coverage profile for it.
- The LLD (revision 1.14) matches its own revision history in the header and names Execution's dedup table as the `platform-events` inbox table `processed_events`.
- `make lint` forbids `database/sql`, `log`, `log/slog`, `zap`, the OpenTelemetry SDK and exporters and the Prometheus client outside `internal/observability`, plus direct pool and connection construction.
- The internal-token refusal's `code` is `unauthorized`, lowercase like every other refusal; it was `UNAUTHORIZED`.
- The NetworkPolicy selects the platform namespaces `gateway` (Envoy Gateway data plane) and `observability` (Prometheus), the names IAM's charts use, in place of `envoy-gateway-system` and `monitoring`; `make helm-namespace-check` holds it.
- `platform-events/v2` is `v2.2.1` (consumer-side dead-letter fixes, Go 1.26.10; no API change).
- CI runs the three checks the organisation requires and this repository lacked: `Build image (cache)` (Dockerfile lint and image build), `Trivy CVE scan` and `Smoke tests` (`make smoke-test`: the image against a real Valkey, a ticket minted, a stream opened and an ingested event received on it).
- The module requires Go 1.26.10: `govulncheck` reports GO-2026-6809 (`crypto/x509`) on 1.26.9; the base image digest follows.
- `validate-test.yml` parses: a step name with an unquoted colon broke the workflow and the release workflow that calls it.
- **Platform libraries** `platform-events/v2` v2.1.0 and `platform-gincommon` v1.6.0; Go 1.26.9. Service metrics are prefixed `workflow_stream_gateway_`; the former `sse_*` and `internal_events_ingest_total` names are replaced.
- `APP_ENV` must be one of `local`, `dev`, `test`, `staging`, `prod`; a malformed numeric or duration value fails startup instead of falling back to its default; the internal token is required outside dev and local, not only in prod.
- The connection counter is released through a script that never leaves it negative, so a stray release cannot raise the per-user cap.
- Servers set read-header and idle timeouts; the stream route has no write timeout.
- Non-test code carries no comments.
