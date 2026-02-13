#!/usr/bin/env bash
# Maintenance script for R2 Data Catalog table management
# Usage: bash scripts/maintenance.sh <command>
# Commands: compaction-enable, snapshot-enable, status
# Requires: CLOUDFLARE_API_TOKEN and R2_BUCKET_NAME environment variables

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT_DIR="$(cd "${APP_DIR}/../.." && pwd)"

# Load environment variables
# shellcheck source=/dev/null
source "${ROOT_DIR}/.env"
# shellcheck source=/dev/null
source "${APP_DIR}/.env" 2>/dev/null || true

COMPACTION_TARGET_SIZE=256
SNAPSHOT_OLDER_THAN_DAYS=7
SNAPSHOT_RETAIN_LAST=5

require_var() {
  local var_name="${1}"
  local var_value="${!var_name:-}"
  if [ -z "${var_value}" ]; then
    echo "Error: ${var_name} is not set in .env" >&2
    exit 1
  fi
}

compaction_enable() {
  require_var "CLOUDFLARE_API_TOKEN"
  require_var "R2_BUCKET_NAME"
  echo "Enabling compaction for bucket: ${R2_BUCKET_NAME}"
  bunx wrangler r2 bucket catalog compaction enable "${R2_BUCKET_NAME}" \
    --target-size "${COMPACTION_TARGET_SIZE}" \
    --token "${CLOUDFLARE_API_TOKEN}"
  echo "Compaction enabled with target size: ${COMPACTION_TARGET_SIZE}MB"
}

snapshot_enable() {
  require_var "CLOUDFLARE_API_TOKEN"
  require_var "R2_BUCKET_NAME"
  echo "Enabling snapshot expiration for bucket: ${R2_BUCKET_NAME}"
  bunx wrangler r2 bucket catalog snapshot-expiration enable "${R2_BUCKET_NAME}" \
    --older-than-days "${SNAPSHOT_OLDER_THAN_DAYS}" \
    --retain-last "${SNAPSHOT_RETAIN_LAST}" \
    --token "${CLOUDFLARE_API_TOKEN}"
  echo "Snapshot expiration enabled: older than ${SNAPSHOT_OLDER_THAN_DAYS} days, retain last ${SNAPSHOT_RETAIN_LAST}"
}

show_status() {
  require_var "R2_BUCKET_NAME"
  echo "Catalog status for bucket: ${R2_BUCKET_NAME}"
  bunx wrangler r2 bucket catalog get "${R2_BUCKET_NAME}"
}

main() {
  local command="${1:-}"

  case "${command}" in
    compaction-enable)
      compaction_enable
      ;;
    snapshot-enable)
      snapshot_enable
      ;;
    status)
      show_status
      ;;
    *)
      echo "Usage: bash scripts/maintenance.sh <command>"
      echo "Commands:"
      echo "  compaction-enable  - Enable automatic file compaction"
      echo "  snapshot-enable    - Enable snapshot expiration"
      echo "  status             - Show catalog settings"
      exit 1
      ;;
  esac
}

main "$@"
