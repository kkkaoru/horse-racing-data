#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POSTGRES_APP_DIR="$ROOT_DIR/apps/local-postgresql"
VIEWER_APP_DIR="$ROOT_DIR/apps/pc-keiba-viewer"
VIEWER_PORT=443
LIVE_RELAY_PORT="${PC_KEIBA_PRODUCTION_LIVE_RELAY_PORT:-3010}"

load_viewer_env() {
  if [[ -f "$VIEWER_APP_DIR/.env.local" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "$VIEWER_APP_DIR/.env.local"
    set +a
  fi
}

is_postgres_running() {
  docker compose \
    --env-file "$POSTGRES_APP_DIR/.env" \
    --project-directory "$POSTGRES_APP_DIR" \
    ps --status running --services 2>/dev/null |
    grep -qx "postgres"
}

is_viewer_running() {
  lsof -nP -iTCP:"$VIEWER_PORT" -sTCP:LISTEN >/dev/null 2>&1
}

is_live_relay_running() {
  lsof -nP -iTCP:"$LIVE_RELAY_PORT" -sTCP:LISTEN >/dev/null 2>&1
}

start_postgres_in_background() {
  echo "Starting local-postgresql in background..."
  bun --cwd "$POSTGRES_APP_DIR" start >/dev/null 2>&1 &
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

if is_postgres_running; then
  echo "local-postgresql is already running. Skipping DB start."
else
  start_postgres_in_background
fi

start_live_relay_in_background

if is_viewer_running; then
  echo "pc-keiba-viewer is already listening on port $VIEWER_PORT. Skipping viewer start."
  exit 0
fi

echo "Starting pc-keiba-viewer (dev:https) on port $VIEWER_PORT..."
exec env PC_KEIBA_DATABASE_TARGET=local bun --cwd "$VIEWER_APP_DIR" dev:https
