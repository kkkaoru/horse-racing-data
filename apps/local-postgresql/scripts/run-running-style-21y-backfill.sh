#!/usr/bin/env bash
# =============================================================================
# run-running-style-21y-backfill.sh
# -----------------------------------------------------------------------------
# Launches the 21y running-style feature backfill driver
# (apps/pc-keiba-viewer/src/scripts/finish-position-features/generate-running-style-local.ts)
# using parameters from running-style-backfill.env.
#
# Usage:
#   ./run-running-style-21y-backfill.sh [ENV_FILE] [--dry-run]
#
#   ENV_FILE   Optional path to an env file. Defaults to
#              "$(dirname "$0")/running-style-backfill.env".
#   --dry-run  Print the composed bun command WITHOUT executing it. The
#              dry-run path also skips the running-driver guard so it is
#              safe to invoke while the production driver is active.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_ENV_FILE="${SCRIPT_DIR}/running-style-backfill.env"

DRY_RUN=0
ENV_FILE="${DEFAULT_ENV_FILE}"

for arg in "$@"; do
  case "${arg}" in
    --dry-run)
      DRY_RUN=1
      ;;
    *)
      ENV_FILE="${arg}"
      ;;
  esac
done

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "[error] env file not found: ${ENV_FILE}" >&2
  exit 1
fi

REPO_ROOT="$(git rev-parse --show-toplevel)"
export REPO_ROOT

set -a
# shellcheck source=/dev/null
. "${ENV_FILE}"
set +a

REQUIRED_VARS=(
  PHASE_A_CONCURRENCY
  DUCKDB_MEMORY_LIMIT_PER_CHUNK
  DUCKDB_THREADS
  DUCKDB_MEMORY_LIMIT
  PG_URL
  RUNNING_STYLE_FEATURE_VERSION
  MODEL_VERSION_JRA
  MODEL_VERSION_NAR
  MODEL_FLATBIN_JRA
  RS_P_FROM_FLATBIN_JRA
  MODEL_FLATBIN_NAR
  CHUNK_GRANULARITY
  IGNORE_NIGHT_WINDOW
  LOG_FILE
)

for var in "${REQUIRED_VARS[@]}"; do
  if [[ -z "${!var:-}" ]]; then
    echo "[error] required env var not set: ${var}" >&2
    exit 1
  fi
done

if [[ "${DRY_RUN}" -eq 0 ]]; then
  EXISTING_PIDS="$(pgrep -f 'generate-running-style-local.ts' || true)"
  if [[ -n "${EXISTING_PIDS}" ]]; then
    echo "[warn] generate-running-style-local.ts already running (pid(s): ${EXISTING_PIDS//$'\n'/ }); aborting." >&2
    exit 2
  fi
fi

DRIVER_SCRIPT="apps/pc-keiba-viewer/src/scripts/finish-position-features/generate-running-style-local.ts"

BUN_CMD=(
  bun run "${REPO_ROOT}/${DRIVER_SCRIPT}"
  --pg-url "${PG_URL}"
  --running-style-feature-version "${RUNNING_STYLE_FEATURE_VERSION}"
  --model-version-jra "${MODEL_VERSION_JRA}"
  --model-flatbin-jra "${MODEL_FLATBIN_JRA}"
  --rs-p-from-flatbin-jra "${RS_P_FROM_FLATBIN_JRA}"
  --model-version-nar "${MODEL_VERSION_NAR}"
  --model-flatbin-nar "${MODEL_FLATBIN_NAR}"
  --threads "${DUCKDB_THREADS}"
  --memory-limit "${DUCKDB_MEMORY_LIMIT}"
  --chunk-granularity "${CHUNK_GRANULARITY}"
  --phase-a-concurrency "${PHASE_A_CONCURRENCY}"
  --memory-limit-per-chunk "${DUCKDB_MEMORY_LIMIT_PER_CHUNK}"
  --ignore-night-window "${IGNORE_NIGHT_WINDOW}"
)

if [[ "${DRY_RUN}" -eq 1 ]]; then
  printf '[dry-run] env file: %s\n' "${ENV_FILE}"
  printf '[dry-run] log target: %s\n' "${LOG_FILE}"
  printf '[dry-run] command:\n'
  printf '  %q' "${BUN_CMD[@]}"
  printf '\n'
  exit 0
fi

echo "[info] launching driver; log -> ${LOG_FILE}"
exec "${BUN_CMD[@]}" >>"${LOG_FILE}" 2>&1
