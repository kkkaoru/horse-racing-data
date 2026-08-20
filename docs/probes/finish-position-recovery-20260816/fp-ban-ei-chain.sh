#!/usr/bin/env bash
set -euo pipefail
REPO=/Users/kkk4oru/ghq/github.com/kkkaoru/horse-racing-data
set -a
# shellcheck disable=SC1091
source "$REPO/.env"
set +a
export NEON_DATABASE_URL="$NEON_PRIMARY_URL"
export MODELS_DIR="$REPO/apps/finish-position-predict-container/models"
export PIPELINE_DIR="$REPO/apps/pc-keiba-viewer/src/scripts"
export SOURCE_DATABASE_URL='r2-catalog://pc-keiba'
export RUN_DATE=20260816
export PREDICT_DAYS_AHEAD=0
export PREDICT_CATEGORIES='ban-ei'
export PIPELINE_FORCE_MEMORY_GB=8
export PIPELINE_FORCE_THREADS=4
export R2_CATALOG_TOKEN="${R2_CATALOG_TOKEN:-${CLOUDFLARE_DEBUG_TOKEN:-}}"
export R2_CATALOG_URI='https://catalog.cloudflarestorage.com/78109ec18c7c85b194b19fb32e3bb149/pc-keiba-r2-catalog'
export R2_CATALOG_WAREHOUSE='78109ec18c7c85b194b19fb32e3bb149_pc-keiba-r2-catalog'
unset PREDICT_SERVE_MODE
cd "$REPO/apps/finish-position-predict-container"
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) start ban-ei one-shot"
uv run --with 'duckdb==1.5.5' python src/predict_upcoming.py
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) ban-ei one-shot exit=$?"
