#!/usr/bin/env bash
# Ensure a Docker-compatible daemon is reachable.
# ONLY for tools that speak the Docker API (wrangler Containers deploy/dev).
# Local PG / predict batch / local image rebuild must use Apple Container CLI.
set -euo pipefail

SHADOW_PG_NAME="horse-racing-local-postgresql"

docker_info_ok() {
  docker info >/dev/null 2>&1
}

stop_local_pg_shadow() {
  if ! command -v docker >/dev/null 2>&1; then
    return 0
  fi
  if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -Fxq "$SHADOW_PG_NAME"; then
    echo "Stopping docker shadow of $SHADOW_PG_NAME so Apple container keeps :15432." >&2
    docker stop "$SHADOW_PG_NAME" >/dev/null 2>&1 || true
    docker rm "$SHADOW_PG_NAME" >/dev/null 2>&1 || true
  fi
}

if ! command -v docker >/dev/null 2>&1; then
  echo "docker CLI is required for wrangler Containers. Install docker + colima." >&2
  exit 1
fi

if docker_info_ok; then
  stop_local_pg_shadow
  exit 0
fi

if ! command -v colima >/dev/null 2>&1; then
  echo "Docker daemon is not reachable and colima is not installed." >&2
  echo "wrangler Containers need a Docker API; start colima or install it." >&2
  exit 1
fi

echo "Docker daemon not reachable; starting colima for wrangler Containers..." >&2
colima start

retries=24
attempt=0
while ((attempt < retries)); do
  if docker_info_ok; then
    stop_local_pg_shadow
    exit 0
  fi
  attempt=$((attempt + 1))
  sleep 5
done

echo "colima start finished but docker info still failed" >&2
exit 1
