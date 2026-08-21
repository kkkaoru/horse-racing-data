#!/usr/bin/env bash
# Ensure a Docker-compatible daemon is reachable.
# ONLY for tools that speak the Docker API (wrangler Containers deploy/dev).
# Local PG / predict batch / local image rebuild must use Apple Container CLI.
#
# `ensure-docker-compat.sh` without arguments is a backward-compatible
# preflight that leaves the daemon running. Pass `-- command ...` to own the
# Colima lifecycle and stop it automatically when that command exits.
set -euo pipefail

SHADOW_PG_NAME="horse-racing-local-postgresql"
COLIMA_STARTED_BY_SCRIPT=0

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

ensure_docker_compat() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker CLI is required for wrangler Containers. Install docker + colima." >&2
    return 1
  fi

  if docker_info_ok; then
    stop_local_pg_shadow
    return 0
  fi

  if ! command -v colima >/dev/null 2>&1; then
    echo "Docker daemon is not reachable and colima is not installed." >&2
    echo "wrangler Containers need a Docker API; start colima or install it." >&2
    return 1
  fi

  if colima status >/dev/null 2>&1; then
    echo "Colima is already running but Docker is not ready; waiting for Docker..." >&2
  else
    echo "Docker daemon not reachable; starting colima for wrangler Containers..." >&2
    # 4 CPU / 8 GiB: enough for wrangler Containers image builds, without
    # competing with Apple Container's local PostgreSQL on a 48 GiB host.
    # Do not raise this back to 16 GiB; that VM shows ~25 GiB host RSS.
    COLIMA_STARTED_BY_SCRIPT=1
    colima start --cpus 4 --memory 8
  fi

  local retries=24
  local attempt=0
  while ((attempt < retries)); do
    if docker_info_ok; then
      stop_local_pg_shadow
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 5
  done

  echo "colima start finished but docker info still failed" >&2
  return 1
}

stop_colima_if_started() {
  if [[ "$COLIMA_STARTED_BY_SCRIPT" != "1" ]]; then
    return 0
  fi

  echo "Stopping colima after Docker-backed command to release VM RAM..." >&2
  if colima stop; then
    if ! colima status >/dev/null 2>&1; then
      return 0
    fi
    echo "colima stop returned before the VM was stopped; forcing shutdown..." >&2
  else
    echo "colima stop failed; forcing the Colima VM shutdown..." >&2
  fi

  colima stop --force || true
  if colima status >/dev/null 2>&1; then
    echo "Colima is still running after the forced stop." >&2
    return 1
  fi
}

cleanup_colima() {
  local exit_code=$?
  trap - EXIT
  stop_colima_if_started || exit_code=$?
  exit "$exit_code"
}

run_with_docker_compat() {
  trap cleanup_colima EXIT
  if ! ensure_docker_compat; then
    return 1
  fi
  "$@"
}

if [[ "$#" -eq 0 ]]; then
  # Backward-compatible preflight mode. Callers that need lifecycle cleanup
  # must pass the command after `--`.
  ensure_docker_compat
  exit $?
fi

if [[ "$1" == "--" ]]; then
  shift
fi

if [[ "$#" -eq 0 ]]; then
  echo "Usage: $0 [--] command [args...]" >&2
  exit 2
fi

run_with_docker_compat "$@"
