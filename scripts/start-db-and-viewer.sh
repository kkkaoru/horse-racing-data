#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POSTGRES_APP_DIR="$ROOT_DIR/apps/local-postgresql"
VIEWER_APP_DIR="$ROOT_DIR/apps/pc-keiba-viewer"
VIEWER_PORT=443
LIVE_RELAY_PORT="${PC_KEIBA_PRODUCTION_LIVE_RELAY_PORT:-3010}"
VIEWER_DATABASE_TARGET="${PC_KEIBA_DATABASE_TARGET:-local}"
POSTGRES_START_PID=""
POSTGRES_START_LOG=""

load_viewer_env() {
  if [[ -f "$VIEWER_APP_DIR/.env.local" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "$VIEWER_APP_DIR/.env.local"
    set +a
  fi
}

is_viewer_running() {
  lsof -nP -iTCP:"$VIEWER_PORT" -sTCP:LISTEN >/dev/null 2>&1
}

is_live_relay_running() {
  lsof -nP -iTCP:"$LIVE_RELAY_PORT" -sTCP:LISTEN >/dev/null 2>&1
}

start_postgres_in_background() {
  local log_dir="$POSTGRES_APP_DIR/tmp"
  mkdir -p "$log_dir"
  POSTGRES_START_LOG="$log_dir/start-db.log"
  echo "Starting local-postgresql in background... (logs: $POSTGRES_START_LOG)"
  bun --cwd "$POSTGRES_APP_DIR" start >"$POSTGRES_START_LOG" 2>&1 &
  POSTGRES_START_PID=$!
}

wait_for_postgres_ready() {
  if [[ -z "$POSTGRES_START_PID" ]]; then
    return 0
  fi
  echo "Waiting for local-postgresql to become ready..."
  if wait "$POSTGRES_START_PID"; then
    echo "local-postgresql is ready."
    return 0
  fi
  echo "ERROR: local-postgresql failed to start. Last lines of $POSTGRES_START_LOG:" >&2
  tail -n 30 "$POSTGRES_START_LOG" >&2 || true
  return 1
}

start_live_relay_in_background() {
  load_viewer_env
  if [[ -z "${PC_KEIBA_ACCESS_CLIENT_ID:-}" || -z "${PC_KEIBA_ACCESS_CLIENT_SECRET:-}" ]]; then
    echo "Skipping production live relay. Set PC_KEIBA_ACCESS_CLIENT_ID/SECRET in .env.local."
    return
  fi
  if is_live_relay_running; then
    echo "production live relay is already listening on port $LIVE_RELAY_PORT. Skipping relay start."
    return
  fi
  echo "Starting production live relay on port $LIVE_RELAY_PORT..."
  bun --cwd "$VIEWER_APP_DIR" dev:production-live-relay >/dev/null 2>&1 &
}

load_viewer_env

start_postgres_in_background

start_live_relay_in_background

if ! wait_for_postgres_ready; then
  exit 1
fi

if is_viewer_running; then
  echo "pc-keiba-viewer is already listening on port $VIEWER_PORT. Skipping viewer start."
  exit 0
fi

echo "Starting pc-keiba-viewer (dev:https) on port $VIEWER_PORT with PC_KEIBA_DATABASE_TARGET=$VIEWER_DATABASE_TARGET..."
exec env PC_KEIBA_DATABASE_TARGET="$VIEWER_DATABASE_TARGET" bun --cwd "$VIEWER_APP_DIR" dev:https
