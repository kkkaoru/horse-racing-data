#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -f "$APP_DIR/.env" ]]; then
  echo "Missing $APP_DIR/.env. Copy .env.example first." >&2
  exit 1
fi

set -a
# shellcheck source=/dev/null
. "$APP_DIR/.env"
set +a

exec_args=()
if [[ ! -t 0 || $# -gt 0 ]]; then
  exec_args=(-T)
fi

docker compose --env-file "$APP_DIR/.env" --project-directory "$APP_DIR" exec "${exec_args[@]}" postgres psql \
  -U "${POSTGRES_USER}" \
  -d "${POSTGRES_DB}" \
  "$@"
