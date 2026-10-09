# Changelog

All notable changes to `workflow-stream-gateway` are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

- `workflow_stream_gateway_*` collectors built through `platform-gincommon`, with the `platform_*` series from the platform libraries in every scrape; `make metrics-lint`, a generated `docs/observability/metric-registry.md` and a scrape test.
- `api/asyncapi.yaml` documenting the 11 consumed event types, with tests that fail when the OpenAPI differs from the router or the AsyncAPI from the routed event types.
- A 10 MiB request body limit on the ticket and internal ingest routes (`413`).
- A connection-count refresh on every wake-up of an open stream, and integration tests of every Valkey adapter against a real Valkey.
- A `PrometheusRule` in the chart (availability, errors, panics, Valkey outage, ingest errors, cap rejections), the `OTEL_*` settings, `helm-lint`, `lint-workflows` and a Dockerfile digest pin check.
- A release workflow (changelog gate, image build, Cosign signature, GitHub Release), a changelog check, CODEOWNERS, issue and pull request templates, `.editorconfig`, a pre-commit hook, ARCHITECTURE and VERSIONING.

### Changed

- **Platform libraries** `platform-events/v2` v2.1.0 and `platform-gincommon` v1.6.0; Go 1.26.9. Service metrics are prefixed `workflow_stream_gateway_`; the former `sse_*` and `internal_events_ingest_total` names are replaced.
- `APP_ENV` must be one of `local`, `dev`, `test`, `staging`, `prod`; a malformed numeric or duration value fails startup instead of falling back to its default; the internal token is required outside dev and local, not only in prod.
- The connection counter is released through a script that never leaves it negative, so a stray release cannot raise the per-user cap.
- Servers set read-header and idle timeouts; the stream route has no write timeout.
- Non-test code carries no comments.
