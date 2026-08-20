#!/usr/bin/env bash
# Read-only of /tmp/predict-upcoming/feat-jra-*. Writes only under OUT.
# Logs go to both LOG and stdout so nohup.out is not empty.
set -euo pipefail
REPO=/Users/kkk4oru/ghq/github.com/kkkaoru/horse-racing-data
DIR="$REPO/apps/pc-keiba-viewer/src/scripts/finish-position-features"
SRC=/tmp/predict-upcoming/feat-jra-layer-0
OUT=/tmp/fp-racechain-0401
LOG="$REPO/docs/probes/finish-position-recovery-20260816/measure-jra-racechain-0401.log"
TARGET_RACE=04:01
INPUT="$OUT/input-13"

if [[ ! -d "$SRC" ]]; then
  echo "missing input $SRC" >&2
  exit 1
fi
if [[ ! -f "$INPUT/race_year=2026/data_0.parquet" ]]; then
  echo "missing sliced 13-row input $INPUT" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
source "$REPO/.env"
set +a
export PIPELINE_FORCE_MEMORY_GB=8
export PIPELINE_FORCE_THREADS=4
export R2_CATALOG_TOKEN="${R2_CATALOG_TOKEN:-${CLOUDFLARE_DEBUG_TOKEN:-}}"
export R2_CATALOG_URI='https://catalog.cloudflarestorage.com/78109ec18c7c85b194b19fb32e3bb149/pc-keiba-r2-catalog'
export R2_CATALOG_WAREHOUSE='78109ec18c7c85b194b19fb32e3bb149_pc-keiba-r2-catalog'

rm -rf "$OUT/r1" "$OUT/r2" "$OUT/r3" "$OUT/r4" "$OUT/r5"

run_layer() {
  local idx=$1
  local script=$2
  local in_dir=$3
  local out_dir=$4
  shift 4
  mkdir -p "$out_dir"
  local start end elapsed rc
  start=$(date +%s)
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) start $idx $script in=$in_dir"
  set +e
  (
    cd "$DIR"
    uv run --with 'duckdb==1.5.5' python "$script" \
      --input-dir "$in_dir" \
      --output-dir "$out_dir" \
      --pg-url r2-catalog://pc-keiba \
      --from-date 20100101 \
      --target-race "$TARGET_RACE" \
      "$@"
  )
  rc=$?
  set -e
  end=$(date +%s)
  elapsed=$((end - start))
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) done $idx rc=$rc ${elapsed}s"
  if [[ "$rc" -ne 0 ]]; then
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) FAIL $idx rc=$rc"
    exit "$rc"
  fi
}

{
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) measure start sliced13=$INPUT target=$TARGET_RACE"
  run_layer 1 add-market-signal-features.py "$INPUT" "$OUT/r1"
  run_layer 2 add-near-miss-features.py "$OUT/r1" "$OUT/r2" --threads 4 --memory-limit 8GB
  run_layer 3 add-baba-pedigree-affinity-features.py "$OUT/r2" "$OUT/r3" --threads 4 --memory-limit 8GB
  run_layer 4 add-relationship-r1-features.py "$OUT/r3" "$OUT/r4" --category jra --threads 4 --memory-limit 8GB
  run_layer 5 add-jra-jockey-pedigree-cell-features.py "$OUT/r4" "$OUT/r5" --threads 4 --memory-limit 8GB
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) measure complete"
} 2>&1 | tee -a "$LOG"
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) wrapper_exit=${PIPESTATUS[0]}" | tee -a "$LOG"
