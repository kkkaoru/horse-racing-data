#!/usr/bin/env bash
# Deploy horse-racing-records worker
# 1. Generates wrangler.deploy.toml from .env
# 2. Deploys worker (clears old vars)
# 3. Uploads secrets to Cloudflare
# Usage: bash scripts/deploy.sh

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

echo "=== Step 1: Generate wrangler.deploy.toml ==="
bash "${SCRIPT_DIR}/generate-config.sh"

cd "${APP_DIR}"

echo "=== Step 2: Deploy worker ==="
bunx wrangler deploy --config wrangler.deploy.toml

echo "=== Step 3: Upload secrets ==="
# Build secrets JSON from env vars
SECRETS_JSON=$(cat << EOF
{
  "CLOUDFLARE_ACCOUNT_ID": "${CLOUDFLARE_ACCOUNT_ID}",
  "R2_BUCKET_NAME": "${R2_BUCKET_NAME}",
  "ICEBERG_NAMESPACE": "${ICEBERG_NAMESPACE}",
  "CATALOG_URI": "${CATALOG_URI}",
  "R2_SQL_ENDPOINT": "${R2_SQL_ENDPOINT}",
  "CLOUDFLARE_API_TOKEN": "${CLOUDFLARE_API_TOKEN}",
  "R2_ACCESS_KEY_ID": "${R2_ACCESS_KEY_ID:-}",
  "R2_SECRET_ACCESS_KEY": "${R2_SECRET_ACCESS_KEY:-}",
  "SKIP_MTLS": "${SKIP_MTLS:-1}"
}
EOF
)

echo "${SECRETS_JSON}" | bunx wrangler secret bulk --config wrangler.deploy.toml

echo "=== Deploy complete ==="
