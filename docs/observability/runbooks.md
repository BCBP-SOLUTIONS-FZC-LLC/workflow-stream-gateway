# Runbooks — workflow-stream-gateway

Each alert in `deploy/helm/templates/prometheusrule.yaml` carries a `runbook` annotation that names one entry below.

## Availability

### StreamGatewayDown / StreamGatewayReplicasMissing

`runbook: workflow-stream-gateway/availability`

No replica has been scraped as healthy for 2 minutes, or fewer than two have been for 5 minutes. Browsers lose their live dashboard stream, and the event consumer's forwards to `/api/v1/internal/events` fail and redeliver.

1. `kubectl get pods -l app.kubernetes.io/name=workflow-stream-gateway` and `kubectl describe pod` for the failing replica: crash loop, image pull, readiness.
2. Readiness depends on Valkey: if `/readyz` fails, follow the Valkey entry below.
3. Check the logs of the last restart for a configuration error at startup.

## Errors

### StreamGatewayHighErrorRate

`runbook: workflow-stream-gateway/errors`

More than 5% of requests answered 5xx for 5 minutes.

1. Split the errors by route in `platform_http_requests_total`: ticket minting, the stream, or event ingest.
2. Read the problem documents' `trace_id` in the logs to follow a failing request.
3. A Valkey outage shows here as well; check `StreamGatewayValkeyUnavailable`.

### StreamGatewayHTTPPanic

`runbook: workflow-stream-gateway/panic`

A handler panicked and was recovered. The panic's stack trace is in the logs of that replica at the alert's time. Find it, then fix the handler; a recovered panic ends only that request.

### StreamGatewayValkeyUnavailable

`runbook: workflow-stream-gateway/valkey`

Connections or ticket mints are being refused because Valkey cannot be reached, so browsers cannot open or resume a stream.

1. Check Valkey's own health and the `VALKEY_ADDR` the chart sets.
2. Check the NetworkPolicy egress to Valkey from the gateway's namespace.
3. Once Valkey answers, clients reconnect with their last cursor and backfill.

### StreamGatewayEventIngestErrors

`runbook: workflow-stream-gateway/event-ingest`

`POST /api/v1/internal/events` keeps failing for forwarded events, so the event consumer redelivers them and may dead-letter them.

1. Split `workflow_stream_gateway_internal_events_ingest_total{result="error"}` by `event_type` and read the logs for the cause.
2. A Valkey write failure shows here too; check the Valkey entry.
3. Messages the consumer dead-lettered are replayed from its dead-letter queue once the cause is fixed (event consumer runbook).

## Capacity

### StreamGatewayConnectionCapRejections

`runbook: workflow-stream-gateway/connection-cap`

Users keep hitting the per-user connection cap for 15 minutes: a browser opening a stream per tab, or a client reconnecting without closing.

1. Find the users in the logs of the refused connections.
2. If the usage is legitimate, raise `CONNECTION_CAP_PER_USER` in the chart values.
