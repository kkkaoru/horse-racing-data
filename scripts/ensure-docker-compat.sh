#!/usr/bin/env bash
# Ensure a Docker-compatible daemon is reachable.
# ONLY for tools that speak the Docker API (wrangler Containers deploy/dev).
# Local PG / predict batch / local image rebuild must use Apple Container CLI.
#
# A command is mandatory so this script always owns a bounded Colima lease and
# stops a VM it started. A preflight-only mode previously left Colima resident
# indefinitely and is intentionally unsupported.
set -euo pipefail

SHADOW_PG_NAME="horse-racing-local-postgresql"
COLIMA_STARTED_BY_SCRIPT=0
CHILD_PID=""
COLIMA_MEMORY_GIB="${COLIMA_MEMORY_GIB:-4}"

validate_colima_memory() {
  if [[ ! "$COLIMA_MEMORY_GIB" =~ ^[0-9]+$ ]] \
    || ((COLIMA_MEMORY_GIB < 2 || COLIMA_MEMORY_GIB > 8)); then
    echo "COLIMA_MEMORY_GIB must be an integer from 2 through 8." >&2
    return 1
  fi
}

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
  if ! validate_colima_memory; then
    return 1
  fi
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
    # VZ backs guest pages on demand, while this value is the guest ceiling.
    # Four GiB is the default for the current Wrangler image build;
    # bounded 2-8 GiB overrides support future workloads without reviving the
    # old 16-20+ GiB resident-VM failure mode.
    COLIMA_STARTED_BY_SCRIPT=1
    colima start --cpus 4 --memory "$COLIMA_MEMORY_GIB"
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

forward_signal_and_exit() {
  local signal="$1"
  local exit_code="$2"
  if [[ -n "$CHILD_PID" ]] && kill -0 "$CHILD_PID" >/dev/null 2>&1; then
    kill -"$signal" "$CHILD_PID" >/dev/null 2>&1 || true
    wait "$CHILD_PID" >/dev/null 2>&1 || true
  fi
  exit "$exit_code"
}

run_with_docker_compat() {
  trap cleanup_colima EXIT
  trap 'forward_signal_and_exit HUP 129' HUP
  trap 'forward_signal_and_exit INT 130' INT
  trap 'forward_signal_and_exit TERM 143' TERM
  if ! ensure_docker_compat; then
    return 1
  fi
  "$@" &
  CHILD_PID=$!
  local command_exit=0
  wait "$CHILD_PID" || command_exit=$?
  CHILD_PID=""
  return "$command_exit"
}

if [[ "$#" -eq 0 ]]; then
  echo "Usage: $0 [--] command [args...]" >&2
  echo "A bounded command is required; preflight-only mode would leak Colima RAM." >&2
  exit 2
fi

if [[ "$1" == "--" ]]; then
  shift
fi

if [[ "$#" -eq 0 ]]; then
  echo "Usage: $0 [--] command [args...]" >&2
  exit 2
fi

run_with_docker_compat "$@"
