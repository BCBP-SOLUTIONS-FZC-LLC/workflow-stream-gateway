# Contributing

## Prerequisites

- Go (see `go.mod` for the version)
- Docker (for `docker compose up` and the integration image build)

## Development setup

```
go mod download
make dev
```

## Project layout

```
cmd/server/                                # main() — HTTP server only, no consumer goroutine
internal/
├── domain/                                # ticket, connection, event-frame types
├── usecase/                                # ticket mint/validate, stream handling, fan-out
├── adapter/
│   ├── inbound/http/                       # ticket mint + SSE stream + /internal/events handlers
│   └── outbound/valkey/                    # ticket store, streams, counters
└── config/
deploy/helm/                                # Helm chart
```

Layering is enforced by `.go-arch-lint.yml` (`make arch-lint`): `adapter`/`cmd` may depend on `usecase`/`domain`; `usecase` may only depend on `domain`.

## Extending the service

### Adding a new consumed event type

1. Add the wire type to the routing table this repo expects from `event_consumer`'s `config/topology.yaml` (see `docs/lld/workflow_stream_gateway.md` §3.1 for the authoritative list).
2. Add the recipient-resolution rule to the fan-out usecase (LLD §6) if it doesn't already fit an existing event class.
3. No new Valkey key shape is needed unless the event introduces a new routing dimension (e.g. tenant-wide broadcast, LLD Appendix B).

### Testing conventions

- Unit tests live alongside the code they test (`_test.go`).
- Ticket lifecycle, fan-out correctness, reconnect/backfill, and isolation each have a dedicated test focus — see LLD §13 for the exact scenarios expected.

## PR process

Open a PR against `main`. `validate-quality` and `validate-test` (CI) must pass. Keep `go.mod`/`go.sum` tidy — CI fails on drift.
