#!/usr/bin/env bash
# Resume fixed-budget local-PG history ablations without scratch output files.
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$APP_DIR/../.." && pwd)"
FEATURES="${1:?Usage: run-history-ablation.sh /absolute/local-pg-features.parquet}"
OBJECTIVE="${ABLATION_OBJECTIVE:-winner}"
case "$OBJECTIVE" in
  winner) run_name="ablation-v1" ;;
  top5) run_name="top5-ablation-v1" ;;
  *) echo "Unknown ABLATION_OBJECTIVE: $OBJECTIVE" >&2; exit 1 ;;
esac
RELATIVE_SPEED="${ABLATION_RELATIVE_SPEED:-0}"
case "$RELATIVE_SPEED" in
  0) ;;
  1) run_name="relative-$run_name" ;;
  *) echo "ABLATION_RELATIVE_SPEED must be 0 or 1" >&2; exit 1 ;;
esac
OUTPUT_ROOT="$REPO_ROOT/docs/finish-position-accuracy/experiments/20260912-local-pg/$run_name"
STOP_AT_JST="${STOP_AT_JST:-202609120850}"
read -r -a venues <<< "${ABLATION_VENUES:-83 54 55}"
read -r -a years <<< "${ABLATION_YEARS:-2020 2021 2022 2023 2024 2025 2026}"

if [[ "$FEATURES" != /* || ! -f "$FEATURES" ]]; then
  echo "An existing absolute local-PG feature file is required." >&2
  exit 1
fi

cd "$APP_DIR"
export PYTHONPATH="$APP_DIR/src/scripts${PYTHONPATH:+:$PYTHONPATH}"
for venue in "${venues[@]}"; do
  for year in "${years[@]}"; do
    for arm in no-speed speed; do
      output="$OUTPUT_ROOT/$venue/$year/$arm"
      if [[ -f "$output/report.json" && -f "$output/model.cbm" && -f "$output/predictions.parquet" ]]; then
        continue
      fi
      if [[ "$(TZ=Asia/Tokyo date +%Y%m%d%H%M)" -ge "$STOP_AT_JST" ]]; then
        echo "Stopped starting new fits at the configured JST deadline."
        exit 0
      fi
      args=(--features "$FEATURES" --output "$output" --venue "$venue" --year "$year" --objective "$OBJECTIVE")
      if [[ "$arm" == speed ]]; then
        args+=(--include-speed)
      fi
      if [[ "$RELATIVE_SPEED" == 1 ]]; then
        args+=(--include-relative-speed)
      fi
      echo "START venue=$venue year=$year arm=$arm"
      uv run python -m learning.history_ablation "${args[@]}"
    done
  done
done
