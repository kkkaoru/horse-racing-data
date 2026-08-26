#!/usr/bin/env bash
# Daily bounded Cloudflare R2 SQL -> local DuckDB weather synchronization.
# Invoked by racing-venue-weather-daily.plist at JST 06:00 every day.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
LOG_DIR="${HOME}/Library/Logs/racing-venue-weather"
LOG_FILE="${LOG_DIR}/$(date +%Y%m%d_%H%M%S).log"
ROOT_ENV_FILE="${REPO_ROOT}/.env"

mkdir -p "${LOG_DIR}"

# Rotate logs older than 30 days
find "${LOG_DIR}" -name "*.log" -mtime +30 -delete 2>/dev/null || true

{
  echo "=== racing-venue-weather-daily $(date -Iseconds) ==="
  if [[ ! -f "${ROOT_ENV_FILE}" ]]; then
    echo "missing Cloudflare credential file: ${ROOT_ENV_FILE}" >&2
    exit 1
  fi
  set -a
  # shellcheck source=/dev/null
  source "${ROOT_ENV_FILE}"
  set +a
  if [[ -z "${WRANGLER_R2_SQL_AUTH_TOKEN:-${CLOUDFLARE_DEBUG_TOKEN:-}}" ]]; then
    echo "missing WRANGLER_R2_SQL_AUTH_TOKEN or CLOUDFLARE_DEBUG_TOKEN" >&2
    exit 1
  fi
  cd "${REPO_ROOT}"
  bun run --filter venue-weather sync:weather-local -- \
    --data-dir data \
    --lookback-days 7
  echo "=== done $(date -Iseconds) ==="
} 2>&1 | tee -a "${LOG_FILE}"
