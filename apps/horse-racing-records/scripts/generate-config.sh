#!/usr/bin/env bash
# Generates wrangler.deploy.toml from environment variables
# Sources: root .env + apps/horse-racing-records/.env
# Usage: bash scripts/generate-config.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT_DIR="$(cd "${APP_DIR}/../.." && pwd)"

# Load environment variables
set -a
# shellcheck source=/dev/null
source "${ROOT_DIR}/.env"
# shellcheck source=/dev/null
source "${APP_DIR}/.env" 2>/dev/null || true
set +a

require_var() {
  local var_name="${1}"
  local var_value="${!var_name:-}"
  if [ -z "${var_value}" ]; then
    echo "Error: ${var_name} is not set in .env" >&2
    exit 1
  fi
}

require_var "CLOUDFLARE_ACCOUNT_ID"
require_var "WORKER_NAME"
require_var "R2_BUCKET_NAME"

cat > "${APP_DIR}/wrangler.deploy.toml" << EOF
#:schema node_modules/wrangler/config-schema.json
name = "${WORKER_NAME}"
account_id = "${CLOUDFLARE_ACCOUNT_ID}"
main = "src/app.ts"
compatibility_date = "2025-02-07"
compatibility_flags = ["nodejs_compat", "allow_eval_during_startup"]

[[rules]]
type = "CompiledWasm"
globs = ["**/*.wasm"]
fallthrough = false

[[r2_buckets]]
binding = "R2_BUCKET"
bucket_name = "${R2_BUCKET_NAME}"
EOF

echo "Generated wrangler.deploy.toml in ${APP_DIR}"
