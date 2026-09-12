#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(git -C "$ROOT" rev-parse --show-toplevel)"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="$REPO/apps/pc-keiba-viewer/src/scripts:$REPO/apps/finish-position-predict-container/src"
MODEL="$REPO/apps/finish-position-predict-container/models/finish-position/ban-ei/banei-cb-v9-sim-2011"
for MODE in early retrospective; do
  if [[ "$MODE" == early ]]; then
    INPUT="$ROOT/early-baseline-replay.parquet"
    OLD="$ROOT/frozen-early-replay-v1/83/predictions.parquet"
  else
    INPUT="$ROOT/production-baseline-features/83/final/race_year=*/*.parquet"
    OLD="$ROOT/frozen-production-v1/83/predictions.parquet"
  fi
  OUTPUT="$ROOT/frozen-pandas-$MODE-v1/83"
  if [[ -e "$OUTPUT/report.json" ]]; then
    printf 'Refusing to overwrite completed replay: %s\n' "$OUTPUT" >&2
    exit 1
  fi
  uv run --no-sync --project "$REPO/apps/pc-keiba-viewer" python -m learning.frozen_production_evaluation \
    --features "$INPUT" --model-dir "$MODEL" --output "$OUTPUT" --frame-loader pandas
  uv run --no-sync --project "$REPO/apps/pc-keiba-viewer" python -m learning.paired_rank_evaluation \
    --baseline "$OLD" --candidate "$OUTPUT/predictions.parquet" --output "$OUTPUT/paired-loader-effect.json"
done
