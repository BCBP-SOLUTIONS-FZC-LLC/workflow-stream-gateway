# Architecture

How `workflow-stream-gateway` is built. Behaviour is specified in the LLD in [docs/lld/](docs/lld/).

## Process

One binary, `cmd/server`, with two listeners: the API on `:8080` and metrics on `:9090`. There is no background consumer, no database and no Temporal dependency. Telemetry initialisation runs first, so the identity (`domain`, `service`, `environment`) is fixed before any collector exists.

## Layers

```text
domain ← usecase ← adapter
```

- `internal/domain`: the ticket and stream frame types.
- `internal/usecase`: ticket minting and validation, fan-out recipient resolution, and the stream loop (backfill, live tail, heartbeat, resync marker).
- `internal/adapter/inbound/http`: the Gin router, handlers and middleware.
- `internal/adapter/outbound/valkey`: the ticket store, the Stream reads and writes, and the connection and rate counters.

## Data flow

1. The browser mints a ticket: the router checks the gateway identity, the use case applies the per-user rate limit and stores the identity snapshot under a random token.
2. The browser opens `GET /api/v1/stream?ticket=…`. The handler validates the ticket, acquires a slot in the per-user connection counter and serves frames until the client disconnects. Each wake-up of the loop refreshes the counter, and the slot is released when the connection ends.
3. The Workflow-Events Consumer forwards an event to `POST /api/v1/internal/events`. The use case resolves recipients from the payload (assignees, department, starter, tenant administrators) and appends the raw envelope to each recipient's Stream, trimmed to the retention window.
4. A reconnect carries a cursor of one position per tailed Stream. A position older than the retention window yields a resync marker.

## Valkey keys

| Key | Holds |
| --- | --- |
| `ssegw:ticket:<token>` | The identity snapshot of a ticket |
| `sse:ticketrate:<tenant>:<user>` | The fixed-window mint counter |
| `sse:conncount:<tenant>:<user>` | The open-connection counter |
| `sse:events:<tenant>:<user>` | A user's personal Stream |
| `sse:events:broadcast:<tenant>[:<department>]` | The tenant-wide and per-department Streams |

## Observability

Collectors are built in `internal/observability` with `platform-gincommon` constructors. The request timeout middleware wraps the ticket and internal groups only, so it never cuts off an open stream. [docs/observability/](docs/observability/) documents the tiers and the generated inventory.

## Specifications

| File | Describes | Checked by |
| --- | --- | --- |
| `docs/swagger/openapi.yaml` | The HTTP surface | a parity test against the router |
| `api/asyncapi.yaml` | The consumed event types | a parity test against the routed types |
