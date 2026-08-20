#!/usr/bin/env bash
# Rebuild DuckDB base WITH --target-race 04:01. Does not touch feat-jra-*.
set -euo pipefail
REPO=/Users/kkk4oru/ghq/github.com/kkkaoru/horse-racing-data
OUT=/tmp/fp-pedigree-target-0401
LOG="$REPO/docs/probes/finish-position-recovery-20260816/run-pedigree-target-0401.log"
set -a
# shellcheck disable=SC1091
source "$REPO/.env"
set +a
export PIPELINE_FORCE_MEMORY_GB=8
export PIPELINE_FORCE_THREADS=4
export R2_CATALOG_TOKEN="${R2_CATALOG_TOKEN:-${CLOUDFLARE_DEBUG_TOKEN:-}}"
export R2_CATALOG_URI='https://catalog.cloudflarestorage.com/78109ec18c7c85b194b19fb32e3bb149/pc-keiba-r2-catalog'
export R2_CATALOG_WAREHOUSE='78109ec18c7c85b194b19fb32e3bb149_pc-keiba-r2-catalog'
export PATH="$REPO/apps/pc-keiba-viewer/.venv/bin:$PATH"
mkdir -p "$OUT/base" "$OUT/spill"
rm -rf "$OUT/base" "$OUT/spill"
mkdir -p "$OUT/base" "$OUT/spill"
{
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) start target-race=04:01 out=$OUT"
  python "$REPO/apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py" \
    --category jra \
    --target-date 20260816 \
    --days-ahead 0 \
    --pg-url r2-catalog://pc-keiba \
    --output-dir "$OUT/base" \
    --allow-empty-targets \
    --target-race 04:01 \
    --temp-dir "$OUT/spill" \
    --threads 4 \
    --memory-limit 8GB
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) done rc=$?"
} 2>&1 | tee "$LOG"
