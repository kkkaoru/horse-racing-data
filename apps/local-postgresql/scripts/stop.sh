#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -f "$APP_DIR/.env" ]]; then
  echo "Missing $APP_DIR/.env. Copy .env.example first." >&2
  exit 1
fi

container stop horse-racing-local-postgresql 2>/dev/null || true
container delete horse-racing-local-postgresql 2>/dev/null || true
