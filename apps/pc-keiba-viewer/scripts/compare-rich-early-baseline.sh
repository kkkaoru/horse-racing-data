#!/usr/bin/env bash
# Explicit post-training date window; paired evaluator still rejects cohort gaps.
set -euo pipefail
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd "$APP_DIR/../.." && pwd)/docs/finish-position-accuracy/experiments/20260912-local-pg"
cd "$APP_DIR"
export PYTHONPATH="$APP_DIR/src/scripts:$APP_DIR/../finish-position-predict-container/src"
for experiment in rich-early-ablation-v1 rich-early-body-ablation-v1; do
  for arm in native corrected-history; do
    output="$ROOT/$experiment/83/2026/$arm"
    uv run python -c '
import sys
from pathlib import Path
import polars as pl
root = Path(sys.argv[1])
frame = pl.read_parquet(root / "predictions.parquet")
frame.filter(pl.col("race_date").is_between(pl.lit("20260523"), pl.lit("20260907"))).write_parquet(root / "matched-early-replay.parquet")
' "$output"
    uv run python -m learning.paired_rank_evaluation \
      --baseline "$ROOT/frozen-early-replay-v1/83/predictions.parquet" \
      --candidate "$output/matched-early-replay.parquet" \
      --output "$output/paired-vs-early-replay.json"
  done
done
