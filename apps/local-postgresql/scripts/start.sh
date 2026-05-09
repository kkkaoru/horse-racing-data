#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -f "$APP_DIR/.env" ]]; then
  echo "Missing $APP_DIR/.env. Copy .env.example first." >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required. Install Docker CLI and start a Docker-compatible daemon such as Colima." >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  if command -v colima >/dev/null 2>&1; then
    echo "Docker daemon not reachable. Starting Colima..." >&2
    colima start
  else
    echo "Docker daemon not reachable and colima is not installed. Start a Docker-compatible daemon and retry." >&2
    exit 1
  fi
fi

docker compose --env-file "$APP_DIR/.env" --project-directory "$APP_DIR" up -d
docker compose --env-file "$APP_DIR/.env" --project-directory "$APP_DIR" ps
