#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POSTGRES_APP_DIR="$ROOT_DIR/apps/local-postgresql"
VIEWER_APP_DIR="$ROOT_DIR/apps/pc-keiba-viewer"
VIEWER_PORT="${PORT:-3000}"

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

if is_postgres_running; then
  echo "local-postgresql is already running. Skipping DB start."
else
  echo "Starting local-postgresql..."
  bun --cwd "$POSTGRES_APP_DIR" start
fi

if is_viewer_running; then
  echo "pc-keiba-viewer is already listening on port $VIEWER_PORT. Skipping viewer start."
  exit 0
fi

echo "Starting pc-keiba-viewer on port $VIEWER_PORT..."
exec bun --cwd "$VIEWER_APP_DIR" dev:local
