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

# Keep the Apple Container VM below the old 20 GiB ceiling. The 14 GiB cap is
# the measured two-reader peak (10.98 GiB) plus 25% safety headroom.
# The TypeScript source is unit-tested and is also the operator-visible source
# of truth for these coupled container/PostgreSQL settings.
eval "$(bun run "$APP_DIR/scripts/postgres-resource-config.ts")"

POSTGRES_HOST_BIND="${POSTGRES_HOST_BIND:-0.0.0.0}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
PGPORT="${PGPORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-horse_racing}"
POSTGRES_DB="${POSTGRES_DB:-horse_racing}"

# --- 5. Container state helpers ---
container_is_running() {
  container list --quiet 2>/dev/null | grep -Fxq "$CONTAINER_NAME"
}

container_exists() {
  container list --all --quiet 2>/dev/null | grep -Fxq "$CONTAINER_NAME"
}

show_container_diagnostics() {
  echo "--- container logs: $CONTAINER_NAME ---" >&2
  container logs -n 80 "$CONTAINER_NAME" >&2 2>/dev/null || true
  echo "--- container inspect: $CONTAINER_NAME ---" >&2
  container inspect "$CONTAINER_NAME" >&2 2>/dev/null || true
}

# --- 6. Wait for healthy (pg_isready via container exec) ---
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
  show_container_diagnostics
  return 1
}

apply_role_resource_limits() {
  # Historical ALTER ROLE tuning survives container recreation because PGDATA
  # is bind-mounted. Re-assert the bounded values so role-level settings cannot
  # silently override the 14 GiB container budget on new connections.
  container exec "$CONTAINER_NAME" psql \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -v ON_ERROR_STOP=1 \
    -c "ALTER ROLE CURRENT_USER SET work_mem = '$POSTGRES_WORK_MEM';" \
    -c "ALTER ROLE CURRENT_USER SET effective_cache_size = '$POSTGRES_EFFECTIVE_CACHE_SIZE';" \
    -c "ALTER ROLE CURRENT_USER SET max_parallel_workers_per_gather = '$POSTGRES_MAX_PARALLEL_WORKERS_PER_GATHER';" \
    >/dev/null
}

delete_existing_container() {
  if ! container_exists; then
    return 0
  fi

  echo "Deleting existing container $CONTAINER_NAME..." >&2
  container delete --force "$CONTAINER_NAME" >/dev/null 2>&1 || true
}

start_existing_container_if_possible() {
  if container_is_running; then
    echo "$CONTAINER_NAME is already running."
    wait_for_healthy
    return $?
  fi

  if ! container_exists; then
    return 1
  fi

  echo "Starting existing container $CONTAINER_NAME..."
  if ! container start "$CONTAINER_NAME"; then
    echo "Existing container $CONTAINER_NAME could not be started." >&2
    show_container_diagnostics
    return 1
  fi

  wait_for_healthy
}

run_new_container() {
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
    -m "$POSTGRES_CONTAINER_MEMORY" \
    -c "$POSTGRES_CONTAINER_CPUS" \
    --env-file "$APP_DIR/.env" \
    -e "PGDATA=/var/lib/postgresql/data" \
    "$IMAGE" \
    postgres \
      -c wal_level=logical \
      -c max_wal_senders=10 \
      -c max_replication_slots=10 \
      -c "shared_buffers=$POSTGRES_SHARED_BUFFERS" \
      -c "effective_cache_size=$POSTGRES_EFFECTIVE_CACHE_SIZE" \
      -c "work_mem=$POSTGRES_WORK_MEM" \
      -c "maintenance_work_mem=$POSTGRES_MAINTENANCE_WORK_MEM" \
      -c wal_buffers=16MB \
      -c random_page_cost=1.1 \
      -c effective_io_concurrency=256 \
      -c max_wal_size=4GB \
      -c min_wal_size=1GB \
      -c checkpoint_timeout=15min \
      -c checkpoint_completion_target=0.9 \
      -c wal_compression=lz4 \
      -c "max_worker_processes=$POSTGRES_MAX_WORKER_PROCESSES" \
      -c "max_parallel_workers=$POSTGRES_MAX_PARALLEL_WORKERS" \
      -c "max_parallel_workers_per_gather=$POSTGRES_MAX_PARALLEL_WORKERS_PER_GATHER" \
      -c "max_parallel_maintenance_workers=$POSTGRES_MAX_PARALLEL_MAINTENANCE_WORKERS" \
      -c jit=on \
      -c default_statistics_target=200 \
      -c fsync=on \
      -c full_page_writes=on \
      -c synchronous_commit=on \
      -c wal_log_hints=on \
      -c log_checkpoints=on \
      -c log_statement=ddl

  wait_for_healthy
}

run_index_health_repair() {
  # amcheck intentionally reads every btree and can fill almost the entire
  # Apple Container page cache (~13.4/14 GiB on the 2026-08-25 measurement).
  # Starting an already-healthy database must stay cheap; run the full check
  # only as an explicit maintenance action.
  if [[ "${POSTGRES_RUN_INDEX_HEALTH_REPAIR:-0}" != "1" ]]; then
    echo "Skipping index-health repair (set POSTGRES_RUN_INDEX_HEALTH_REPAIR=1 for maintenance)."
    return 0
  fi
  if ! command -v bun >/dev/null 2>&1; then
    echo "bun not found; skipping index-health repair. Install bun or run: bun --cwd $APP_DIR indexes:repair:quick" >&2
    return 0
  fi
  echo "Running quick index-health repair (amcheck + REINDEX, never DROP INDEX)..."
  (
    cd "$APP_DIR"
    bun run indexes:repair:quick
  )
}

ensure_container_system

if start_existing_container_if_possible; then
  apply_role_resource_limits
  run_index_health_repair
  container list
  exit 0
fi

delete_existing_container
run_new_container
apply_role_resource_limits
run_index_health_repair
container list
