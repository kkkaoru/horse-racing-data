#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POSTGRES_APP_DIR="$ROOT_DIR/apps/local-postgresql"
VIEWER_APP_DIR="$ROOT_DIR/apps/pc-keiba-viewer"
REPLICA_ENV_FILE="$POSTGRES_APP_DIR/.env.replica"
VIEWER_PORT=443
LIVE_RELAY_PORT="${PC_KEIBA_PRODUCTION_LIVE_RELAY_PORT:-3010}"
VIEWER_DATABASE_TARGET=""
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

load_replica_env() {
  if [[ -f "$REPLICA_ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$REPLICA_ENV_FILE"
    set +a
  fi
}

resolve_viewer_database_target() {
  if [[ -n "${PC_KEIBA_DATABASE_TARGET:-}" ]]; then
    VIEWER_DATABASE_TARGET="$PC_KEIBA_DATABASE_TARGET"
  elif [[ -n "${DATABASE_URL_NEON:-}" || -n "${NEON_DATABASE_URL:-}" ]]; then
    VIEWER_DATABASE_TARGET="neon"
  else
    VIEWER_DATABASE_TARGET="local"
  fi

  if [[ "$VIEWER_DATABASE_TARGET" == "neon" ]]; then
    if [[ -z "${DATABASE_URL_NEON:-}" && -n "${NEON_DATABASE_URL:-}" ]]; then
      export DATABASE_URL_NEON="$NEON_DATABASE_URL"
    fi
    if [[ -z "${DATABASE_URL_NEON:-}" ]]; then
      echo "DATABASE_URL_NEON or NEON_DATABASE_URL is required for PC_KEIBA_DATABASE_TARGET=neon." >&2
      exit 1
    fi
  fi
}

should_start_postgres() {
  [[ "$VIEWER_DATABASE_TARGET" == "local" ]] ||
    [[ "$VIEWER_DATABASE_TARGET" == "cloudflare" &&
      "${PC_KEIBA_ALLOW_CLOUDFLARE_DB_IN_NEXT_DEV:-}" != "1" ]]
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
load_replica_env
resolve_viewer_database_target

if should_start_postgres; then
  if is_postgres_running; then
    echo "local-postgresql is already running. Skipping DB start."
  else
    start_postgres_in_background
  fi
else
  echo "Using PC_KEIBA_DATABASE_TARGET=$VIEWER_DATABASE_TARGET. Skipping local-postgresql start."
fi

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
