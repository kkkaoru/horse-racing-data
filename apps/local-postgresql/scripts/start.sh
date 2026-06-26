#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

CONTAINER_NAME="horse-racing-local-postgresql"
IMAGE="docker.io/pgvector/pgvector:0.8.1-pg18"

# --- 1. Check .env exists ---
if [[ ! -f "$APP_DIR/.env" ]]; then
  echo "Missing $APP_DIR/.env. Copy .env.example first." >&2
  exit 1
fi

# --- 2. Check container CLI is installed ---
if ! command -v container >/dev/null 2>&1; then
  echo "Apple Container CLI is required. Install from https://github.com/apple/container" >&2
  exit 1
fi

# --- 3. Ensure container system is running ---
ensure_container_system() {
  if container system status >/dev/null 2>&1; then
    return 0
  fi

  echo "Container system not running. Starting..." >&2
  container system start
}

# --- 4. Load .env to get port/bind vars ---
load_env() {
  local key value
  while IFS='=' read -r key value; do
    # Skip comments and blank lines
    [[ -z "$key" || "$key" =~ ^# ]] && continue
    # Strip surrounding quotes from value
    value="${value%\"}"
    value="${value#\"}"
    value="${value%\'}"
    value="${value#\'}"
    export "$key=$value"
  done < "$APP_DIR/.env"
}

load_env

POSTGRES_HOST_BIND="${POSTGRES_HOST_BIND:-0.0.0.0}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
PGPORT="${PGPORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-horse_racing}"
POSTGRES_DB="${POSTGRES_DB:-horse_racing}"

# --- 9. Wait for healthy (pg_isready via container exec) ---
wait_for_healthy() {
  local retries="${POSTGRES_HEALTH_RETRIES:-24}"
  local attempt=0

  echo "Waiting for PostgreSQL to become ready..."
  while [[ "$attempt" -lt "$retries" ]]; do
    if container exec "$CONTAINER_NAME" pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
      echo "local-postgresql is healthy."
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 5
  done

  echo "postgres did not become healthy within $((retries * 5))s." >&2
  container inspect "$CONTAINER_NAME" >&2 2>/dev/null || true
  exit 1
}

ensure_container_system

# --- 5/6. Check if container is already running or exists ---
if container list 2>/dev/null | grep -q "$CONTAINER_NAME"; then
  echo "$CONTAINER_NAME is already running."
  wait_for_healthy
  container list
  exit 0
fi

# Remove stopped container if it exists
if container list --all 2>/dev/null | grep -q "$CONTAINER_NAME"; then
  echo "Removing stopped container $CONTAINER_NAME..." >&2
  container delete "$CONTAINER_NAME" >/dev/null 2>&1 || true
fi

# --- 7. Pull image ---
echo "Pulling image $IMAGE..."
container image pull "$IMAGE"

# --- 8. Run container ---
echo "Starting $CONTAINER_NAME..."
container run -d \
  --name "$CONTAINER_NAME" \
  --user 999:999 \
  -v "$APP_DIR/data/postgres:/var/lib/postgresql/data" \
  -v "$APP_DIR/initdb:/docker-entrypoint-initdb.d" \
  -p "${POSTGRES_HOST_BIND}:${POSTGRES_PORT}:${PGPORT}" \
  -m 20G \
  -c 12 \
  --env-file "$APP_DIR/.env" \
  -e "PGDATA=/var/lib/postgresql/data" \
  "$IMAGE" \
  postgres \
    -c wal_level=logical \
    -c max_wal_senders=10 \
    -c max_replication_slots=10 \
    -c shared_buffers=6GB \
    -c effective_cache_size=18GB \
    -c work_mem=64MB \
    -c maintenance_work_mem=1GB \
    -c wal_buffers=16MB \
    -c random_page_cost=1.1 \
    -c effective_io_concurrency=256 \
    -c max_wal_size=4GB \
    -c min_wal_size=1GB \
    -c checkpoint_timeout=15min \
    -c checkpoint_completion_target=0.9 \
    -c wal_compression=lz4 \
    -c max_worker_processes=16 \
    -c max_parallel_workers=12 \
    -c max_parallel_workers_per_gather=8 \
    -c max_parallel_maintenance_workers=4 \
    -c jit=on \
    -c default_statistics_target=200

# --- 9/10. Wait for healthy and show status ---
wait_for_healthy
container list
