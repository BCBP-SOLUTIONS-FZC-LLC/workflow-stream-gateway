# Changelog

All notable changes to `workflow-stream-gateway` are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Fixed

- Live delivery no longer skips events: each read starts after the entry last delivered on each stream (beginning from the newest entry at connect, the resume cursor or the last backfilled entry), instead of waiting only for entries newer than the read itself, so an event written while a frame was being sent is delivered.
- The ticket-mint rate counter and the connection counter set their expiry in the same atomic step as the increment. A failed second command used to leave a counter without an expiry, refusing the user for good; the connection cap is now checked before the increment, so a refused connect leaves the count unchanged.
- A replica told to stop ends its open streams at their next wake-up and waits one heartbeat interval plus five seconds (at least ten), and always shuts down both listeners. Shutdown with a connected browser used to run into the timeout and exit with an error.
- The resume cursor names every tailed stream that holds an entry from the first frame, so a reconnect after a resync no longer resyncs again; and a payload with line breaks is written as one `data:` line per payload line, so it can no longer start another SSE field or end the event early.
- A problem response names the path as it was sent: a refused `/api/v1/internal%2Fevents` used to read "no route matches POST /api/v1/internal/events", a path that does have a route.
- A ticket is minted only when the tenant, user and every department id are lower-case UUIDs; any other value is refused with 401, since these ids form the Valkey keys and a key separator in one could name another tenant's key.

### Changed

- `make test` and `make test-integration` run and count `cmd/`, which no target ran before; start-up, shutdown and exit codes of the server are tested.
- Tests: request-body limit, readiness, unmatched routes, ticket edge cases, stream failures, metric pre-initialisation and Valkey adapter boundaries. The Valkey integration test that slept before appending now waits until the reader is blocked.
- The OpenAPI envelope `type` no longer lists the old twelve dotted-lowercase types: a type is PascalCase and matched with `source` against the routing file, and a type its section does not list is accepted and ignored with 200.
- CI: the PR summary reports a failed or cancelled gate as failed instead of "All checks passed", and counts a run as skipped only when the gate says so.
- Every routed payload names its readers in `recipients`, a list of `{kind, id}` (`kind` `user` or `department`); a `user` entry reaches that user, a `department` entry that department's tier, and a repeated entry is written once. The routing file names no payload fields: each section lists its `source`, `tenant_admins` (whether its events also reach the tenant's administrators) and its event types.
- Each routing section names its `callers`, the mesh identities (`<namespace>/<service account>`) allowed to post its events; the pod's sidecar sets `X-Forwarded-Client-Cert` (`SANITIZE_SET`), and another caller is refused with 403 (`code: forbidden`) and counted as `caller_not_allowed` in `workflow_stream_gateway_internal_auth_rejected_total`. `CALLER_IDENTITY_CHECK=false` switches the check off, accepted only with `APP_ENV` `local`, `dev` or `test`; docker compose and the smoke test set it. `recipients` is required, and every `id` is a lower-case UUID.
- The `llm` section routes the ten LLM generation types (source `llm-retrieval-generation`) as `llm.<type>`, forwarded by the LLM subsystem's own consumer, and does not reach the tenant's administrators. The chart admits that consumer on the internal events route (`authorizationPolicy.internalRoutes`, `networkPolicy.otherNamespaceCallers`), with its namespace a placeholder until the platform team sets it.
- Routing comes from a routing file read at start-up (`config/routing.yaml`, `ROUTING_CONFIG_PATH`; the chart's `routing` values, mounted from a ConfigMap whose change rolls the pods). It holds one section per publishing service, matched by the envelope `source`, each listing its event types; the shipped `workflow` section routes the sixteen Execution Service types. A pair of source and type the file does not list is acknowledged with 200 and ignored. A malformed file fails start-up. Tender joins as a section once its events are defined.
- Dependencies: OpenTelemetry moves from v1.46.0 to v1.47.0 (fixes GO-2026-6975 and GO-2026-6974) and `otelhttp` to v0.72.0; both are indirect, through `platform-gincommon`.
- The SSE `event:` line and the `event_type` metric labels are `<service>.<type>`, for example `workflow.WorkflowTaskCreated`.
- A payload that is not a JSON object (including `null`), or a `recipients` list of another shape, is refused with 400.
- CI: marking a draft ready runs CI, adding or removing `skip-ci` re-runs the gate, and a draft, a documentation-only change or `skip-ci` skip the same way with every required check still reporting. The `paths-ignore` filters are gone, and a change under `docs/observability/` (read by the metric and alert checks) is not treated as documentation only. The PR summary counts a skipped run as passed. Images stay published on a version tag only (LLD rev 1.18).

### Added

- A request the internal-route caller check refuses is counted in `workflow_stream_gateway_internal_auth_rejected_total{reason}` (`missing_user` or `missing_role`). The caller identity `iam-system` is one constant (`domain.SystemCaller`). A test holds that a ticket mint with the gateway identity and no `iam-system` role still succeeds.
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

- `workflow_stream_gateway_internal_events_ingest_total` labels an event type the service does not handle `unknown`; the raw type is request-controlled text and let a caller create a series per value.
- The router matches the raw request path, as the mesh policy does, and answers `404` for a path with an encoded slash. Before, `/api/v1/internal%2Fevents` was routed as the internal route while the policy saw a path outside its internal rule.
- The stream route's policy renders whenever the routes are exposed, with CORS only when `ingress.securityPolicy.cors.enabled`; it rendered only with CORS, so a same-origin deployment left the stream route behind the gateway's JWT and every `EventSource` got a 401. The stream route also sets `timeouts.request: 0s`, since the gateway's default 15 s request timeout ends a stream.
- `docs/swagger/openapi.yaml` documents `GET /healthz` and `GET /readyz`, as Definition's and Execution's specs do, and the parity test compares them with the router instead of skipping them.
- The Dockerfile left `~/.git-credentials`, with `GO_PRIVATE_TOKEN`, in a builder layer, and the release build exported that layer to a registry cache. The credential is written, used by `go mod download` and removed in one `RUN`, the release build no longer reads or exports a registry cache, and `make docker-lint` and the CI image job run `scripts/dockerfile-credentials-check.sh`. Builder caches of earlier releases held the token, so `GO_PRIVATE_TOKEN` needs rotating.
- `docs/swagger/openapi.yaml` documents the `401` the ticket mint answers when the gateway identity headers are missing; a test holds the documented statuses to the router's answer.
- The chart's one SecurityPolicy covered both routes and set no merge type, so enabling it replaced the Gateway's JWT validation and authorisation filter on the ticket route as well. The ticket and stream paths are now separate HTTPRoutes: the ticket policy merges with the Gateway's, the stream policy replaces it (the ticket is that route's only credential). `ingress.hosts[].paths` is replaced by `ingress.paths.tickets` and `ingress.paths.stream`; `make helm-policy-check`, part of `helm-lint`, holds both properties.

### Changed

- Docs only: the LLD (1.17) states that the reassigned event reaches the new assignee alone when it names no old assignee.
- `api/asyncapi.yaml`: `old_user_id` is optional on the reassigned event, as the Execution Service omits it when nobody held the task; the gateway already skips the missing recipient and still delivers to the new assignee and the department.
- Docs only: the in-repo LLD, the AsyncAPI and the OpenAPI match the design repository again. No code changed.
- Chart: per-route caller rules (`authorizationPolicy.internalRoutes` replaces `internalCallers`; the `events` group names the exact path `/api/v1/internal/events`), the render refuses a disabled policy in staging and prod unless `authorizationPolicy.allowDisabled`, `NOTES.txt` states the sidecar and mutual TLS requirement, `appProtocol: http` on the Service ports, and `make helm-lint` runs chart tests that decode the rendered policy, check the ServiceAccount name for any release name and walk the chart for the retired token.
- The internal route admits the system caller identity (`x-tenant-roles` holds `iam-system` and `x-user-id` is set) instead of the shared `x-internal-token`: `WORKFLOW_STREAM_GATEWAY_TOKEN` is removed (setting, startup requirement, chart secret key, compose, smoke test), the chart ships an Istio `AuthorizationPolicy` (`authorizationPolicy.*`, the consumer's service account is a placeholder to confirm with Platform), and the OpenAPI and LLD copies follow the design repository (LLD rev 1.15). Status codes are unchanged; the `401` detail is `missing or invalid internal caller identity`.
- `platform-pgcommon/v2` moves from v2.0.0 to v2.0.2 (a patch release; an indirect module here).
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
