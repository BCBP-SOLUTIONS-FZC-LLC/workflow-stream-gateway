#!/usr/bin/env bash
# Smoke test of the built image against a real Valkey: the gateway becomes
# ready, a ticket is minted from gateway identity headers, a stream opened
# with it receives an event ingested for that user, and the internal route
# refuses a request without the token.
set -euo pipefail

cd "$(dirname "$0")/.."

: "${GO_PRIVATE_TOKEN:?GO_PRIVATE_TOKEN is required to build the image}"
image="${SMOKE_IMAGE:-workflow-stream-gateway:smoke}"
net="sg-smoke-$$"
token="smoke-internal-token"
port="${SMOKE_PORT:-18080}"
stream_out=$(mktemp)
token_file=$(mktemp)
printf '%s' "$GO_PRIVATE_TOKEN" >"$token_file"

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    docker logs "$net-gw" 2>&1 | tail -n 60 || true
  fi
  docker rm -f "$net-gw" "$net-valkey" >/dev/null 2>&1 || true
  docker network rm "$net" >/dev/null 2>&1 || true
  rm -f "$stream_out" "$token_file"
  exit "$status"
}
trap cleanup EXIT

if [ -z "${SMOKE_IMAGE:-}" ]; then
  DOCKER_BUILDKIT=1 docker build -q --secret id=go_private_token,src="$token_file" -t "$image" . >/dev/null
fi

docker network create "$net" >/dev/null
docker run -d --name "$net-valkey" --network "$net" valkey/valkey:8-alpine >/dev/null
docker run -d --name "$net-gw" --network "$net" -p "$port:8080" \
  -e APP_ENV=test -e VALKEY_ADDR="$net-valkey:6379" \
  -e WORKFLOW_STREAM_GATEWAY_TOKEN="$token" -e OTEL_EXPORTER_OTLP_ENDPOINT=none \
  "$image" >/dev/null

base="http://localhost:$port"
for _ in $(seq 1 60); do
  if curl -fsS "$base/readyz" >/dev/null 2>&1; then ready=1; break; fi
  sleep 1
done
[ "${ready:-0}" = 1 ] || { echo "smoke: the gateway never became ready"; exit 1; }
echo "smoke: the gateway is ready"

tenant=$(uuidgen | tr '[:upper:]' '[:lower:]')
user=$(uuidgen | tr '[:upper:]' '[:lower:]')
ticket=$(curl -fsS -X POST "$base/api/v1/stream/tickets" \
  -H "x-tenant-id: $tenant" -H "x-user-id: $user" -H "x-tenant-roles: tenant_member" \
  | sed -n 's/.*"ticket":"\([^"]*\)".*/\1/p')
[ -n "$ticket" ] || { echo "smoke: no ticket was minted"; exit 1; }
echo "smoke: a ticket was minted"

curl -sN --max-time 12 "$base/api/v1/stream?ticket=$ticket" >"$stream_out" &
reader=$!
sleep 2

status=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$base/api/v1/internal/events" -d '{}')
[ "$status" = 401 ] || [ "$status" = 403 ] || { echo "smoke: the internal route answered $status without a token"; exit 1; }

id=$(uuidgen | tr '[:upper:]' '[:lower:]')
event=$(printf '{"id":"%s","type":"WorkflowTaskCreated","source":"workflow-execution-service","time":"%s","tenant_id":"%s","data":{"assignee_user_ids":["%s"],"department_id":"%s"}}' \
  "$id" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$tenant" "$user" "$(uuidgen | tr '[:upper:]' '[:lower:]')")
status=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$base/api/v1/internal/events" \
  -H "x-internal-token: $token" -H "Content-Type: application/json" -d "$event")
[ "$status" = 200 ] || { echo "smoke: ingest answered $status"; exit 1; }

for _ in $(seq 1 10); do
  if grep -q "$id" "$stream_out"; then delivered=1; break; fi
  sleep 1
done
kill "$reader" 2>/dev/null || true
wait "$reader" 2>/dev/null || true
[ "${delivered:-0}" = 1 ] || { echo "smoke: the stream never carried event $id"; cat "$stream_out"; exit 1; }
echo "smoke: event $id reached the user's stream"
