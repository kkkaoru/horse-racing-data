#!/usr/bin/env bash
# Correct the stale evaluation cohort without refitting any frozen compact model.
set -euo pipefail
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd "$APP_DIR/../.." && pwd)/docs/finish-position-accuracy/experiments/20260912-local-pg"
cd "$APP_DIR"
export PYTHONPATH="$APP_DIR/src/scripts"
for venue in 83 54 55; do
  for arm in no-speed speed; do
    uv run python -m learning.rescore_history_ablation \
      --features /private/tmp/horse-nar-banei-0912/causal-features-raw2026.parquet \
      --model-dir "$ROOT/ablation-v1/$venue/2026/$arm" \
      --output "$ROOT/full-2026-evaluation-v1/$venue/$arm" \
      --venue "$venue" --from-date 20260101 --to-date 20260911
  done
done
