#!/usr/bin/env bash
# Refresh both frozen research arms and require exact cohort agreement with baseline.
set -euo pipefail
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd "$APP_DIR/../.." && pwd)/docs/finish-position-accuracy/experiments/20260912-local-pg"
FEATURES="${1:?Provide the existing refreshed local-PG causal feature parquet}"
cd "$APP_DIR"
export PYTHONPATH="$APP_DIR/src/scripts${PYTHONPATH:+:$PYTHONPATH}"
for arm in no-speed speed; do
  output="$ROOT/refreshed-evaluation-v1/83/$arm"
  uv run python -m learning.rescore_history_ablation \
    --features "$FEATURES" --model-dir "$ROOT/ablation-v1/83/2026/$arm" \
    --output "$output" --venue 83 --from-date 20260523 --to-date 20260907
  uv run python -m learning.paired_rank_evaluation \
    --baseline "$ROOT/frozen-production-v1/83/predictions.parquet" \
    --candidate "$output/predictions.parquet" \
    --output "$output/paired-vs-frozen-production.json"
done
