#!/usr/bin/env bash
# Fixed, conservative early-card comparisons; no final odds/weight/weather/going.
set -euo pipefail
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$APP_DIR/../.." && pwd)"
ROOT="$REPO_ROOT/docs/finish-position-accuracy/experiments/20260912-local-pg"
cd "$APP_DIR"
export PYTHONPATH="$APP_DIR/src/scripts:$APP_DIR/../finish-position-predict-container/src"
for year in ${RICH_YEARS:-2026 2020 2021 2022 2023 2024 2025}; do
  for arm in native corrected-history; do
    if [[ "$(TZ=Asia/Tokyo date +%Y%m%d%H%M)" -gt "202609120849" ]]; then
      echo 'Stop: no new fits after 08:50 JST.'
      exit 0
    fi
    output="$ROOT/rich-early-ablation-v1/83/$year/$arm"
    if [[ -f "$output/report.json" ]]; then continue; fi
    extra=(--year "$year")
    if [[ "$arm" == corrected-history ]]; then extra+=(--include-history); fi
    echo "START rich-early year=$year arm=$arm"
    uv run python -m learning.rich_history_ablation \
      --history /private/tmp/horse-nar-banei-0912/causal-features-raw2026.parquet \
      --native "$ROOT/rich-history-v1/83/final/race_year=*/*.parquet" \
      --retired "$ROOT/rich-history-retired-v1/83/retired-base/race_year=*/*.parquet" \
      --metadata "$REPO_ROOT/apps/finish-position-predict-container/models/finish-position/ban-ei/banei-cb-v9-sim-2011/metadata.json" \
      --output "$output" "${extra[@]}"
  done
done
