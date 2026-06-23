#!/usr/bin/env bash
# Daily cron wrapper: fetch today's weather for all racing venues via Open-Meteo.
# Invoked by racing-venue-weather-daily.plist at JST 06:00 every day.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
LOG_DIR="${HOME}/Library/Logs/racing-venue-weather"
LOG_FILE="${LOG_DIR}/$(date +%Y%m%d_%H%M%S).log"

mkdir -p "${LOG_DIR}"

# Rotate logs older than 30 days
find "${LOG_DIR}" -name "*.log" -mtime +30 -delete 2>/dev/null || true

{
  echo "=== racing-venue-weather-daily $(date -Iseconds) ==="
  cd "${REPO_ROOT}/scripts"
  uv run python racing_venue_weather.py --mode daily --db-dir "${HOME}/.horse-racing"
  echo "=== done $(date -Iseconds) ==="
} 2>&1 | tee -a "${LOG_FILE}"
