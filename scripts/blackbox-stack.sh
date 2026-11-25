#!/usr/bin/env bash
# Starts or stops the stack the black-box checks (make hurl, make fuzz-api, make fuzz-internal-api)
# run against: the compose Valkey and the gateway built from this checkout, with the caller identity
# check off (no mesh sets the caller locally). The checks write events, so use a throwaway Valkey
# (the compose one is).
#
#   scripts/blackbox-stack.sh up     build and start everything, wait until it answers
#   scripts/blackbox-stack.sh down   stop everything
#
# Ports: 8080 the API, 9090 its metrics, 6380 Valkey. Logs and the binary go to .coverage/blackbox.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
work="$root/.coverage/blackbox"
pidfile="$work/gateway.pid"
project="workflow-stream-gateway-blackbox"

up() {
  mkdir -p "$work"
  (cd "$root" && docker compose -p "$project" up -d --wait --wait-timeout 120 valkey)
  (cd "$root" && go build -o "$work/gateway" ./cmd/server)
  (
    cd "$root"
    export APP_ENV=dev CALLER_IDENTITY_CHECK=false OTEL_EXPORTER_OTLP_ENDPOINT=none \
      VALKEY_ADDR=localhost:6380 HTTP_PORT=8080 METRICS_PORT=9090 \
      ROUTING_CONFIG_PATH=config/routing.yaml HEARTBEAT_INTERVAL=2s
    "$work/gateway" >"$work/gateway.log" 2>&1 &
    echo $! >"$pidfile"
  )
  for _ in $(seq 1 60); do
    if curl -fsS -o /dev/null http://localhost:8080/readyz 2>/dev/null; then return 0; fi
    sleep 1
  done
  echo "the gateway did not become ready on :8080" >&2
  tail -n 30 "$work/gateway.log" >&2 || true
  return 1
}

down() {
  if [ -f "$pidfile" ]; then
    local pid
    pid="$(cat "$pidfile")"
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 30); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 1
    done
    kill -9 "$pid" 2>/dev/null || true
    rm -f "$pidfile"
  fi
  (cd "$root" && docker compose -p "$project" down -v --remove-orphans >/dev/null 2>&1) || true
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  *) echo "usage: $0 up|down" >&2; exit 2 ;;
esac
