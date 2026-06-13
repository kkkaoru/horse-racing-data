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

ensure_colima() {
  if docker info >/dev/null 2>&1; then
    return 0
  fi

  if ! command -v colima >/dev/null 2>&1; then
    echo "Docker daemon not reachable and colima is not installed. Start a Docker-compatible daemon and retry." >&2
    exit 1
  fi

  echo "Docker daemon not reachable. Starting Colima..." >&2
  if colima start; then
    return 0
  fi

  echo "Colima failed to start. The VM may be in a stale state after host sleep. Forcing a clean restart..." >&2
  colima stop --force >/dev/null 2>&1 || true
  colima start
}

wait_for_healthy() {
  local cid
  cid=$(docker compose --env-file "$APP_DIR/.env" --project-directory "$APP_DIR" ps -q postgres)
  if [[ -z "$cid" ]]; then
    echo "postgres container was not created by docker compose up." >&2
    exit 1
  fi

  local retries="${POSTGRES_HEALTH_RETRIES:-24}"
  local attempt=0
  local status
  while [[ "$attempt" -lt "$retries" ]]; do
    status=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}unknown{{end}}' "$cid")
    if [[ "$status" == "healthy" ]]; then
      echo "local-postgresql is healthy."
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 5
  done

  echo "postgres did not become healthy within $((retries * 5))s." >&2
  docker compose --env-file "$APP_DIR/.env" --project-directory "$APP_DIR" logs --tail 30 postgres >&2
  exit 1
}

ensure_colima

docker compose --env-file "$APP_DIR/.env" --project-directory "$APP_DIR" up -d
wait_for_healthy
docker compose --env-file "$APP_DIR/.env" --project-directory "$APP_DIR" ps
