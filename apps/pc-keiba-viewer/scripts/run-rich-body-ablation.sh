#!/usr/bin/env bash
# Same fixed early-card recipe, with a distinct causally decoded body predictor.
set -euo pipefail
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd "$APP_DIR/../.." && pwd)/docs/finish-position-accuracy/experiments/20260912-local-pg"
cd "$APP_DIR"
export PYTHONPATH="$APP_DIR/src/scripts:$APP_DIR/../finish-position-predict-container/src"
for year in ${RICH_YEARS:-2026 2020 2021 2022 2023 2024 2025}; do
  for arm in native corrected-history; do
    if [[ "$(TZ=Asia/Tokyo date +%Y%m%d%H%M)" -gt "202609120849" ]]; then exit 0; fi
    output="$ROOT/rich-early-body-ablation-v1/83/$year/$arm"
    if [[ -f "$output/report.json" ]]; then continue; fi
    extra=(--year "$year")
    if [[ "$arm" == corrected-history ]]; then extra+=(--include-history); fi
    echo "START rich-early-body year=$year arm=$arm"
    uv run python -m learning.rich_history_ablation \
      --history /private/tmp/horse-nar-banei-0912/causal-features-raw2026.parquet \
      --native "$ROOT/body-corrected-native-v1/native.parquet" \
      --retired "$ROOT/body-corrected-native-v1/retired.parquet" \
      --metadata "$ROOT/body-corrected-native-v1/metadata.json" \
      --output "$output" "${extra[@]}"
  done
done
