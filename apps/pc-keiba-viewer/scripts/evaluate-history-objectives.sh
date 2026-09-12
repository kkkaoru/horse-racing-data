#!/usr/bin/env bash
# Compare objectives with identical entrants/features, then speed within Top5 loss.
set -euo pipefail
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd "$APP_DIR/../.." && pwd)/docs/finish-position-accuracy/experiments/20260912-local-pg"
CANDIDATE_RUN="${COMPARISON_RUN:-top5-ablation-v1}"
case "$CANDIDATE_RUN" in
  top5-ablation-v1) output_run="paired-evaluation-v1"; contrast="objective" ;;
  relative-ablation-v1) output_run="paired-relative-v1"; contrast="relative-feature" ;;
  *) echo "Unsupported COMPARISON_RUN: $CANDIDATE_RUN" >&2; exit 1 ;;
esac
cd "$APP_DIR"
export PYTHONPATH="$APP_DIR/src/scripts${PYTHONPATH:+:$PYTHONPATH}"
for year in 2024 2025; do
  for arm in no-speed speed; do
    uv run python -m learning.paired_rank_evaluation \
      --baseline "$ROOT/ablation-v1/83/$year/$arm/predictions.parquet" \
      --candidate "$ROOT/$CANDIDATE_RUN/83/$year/$arm/predictions.parquet" \
      --output "$ROOT/$output_run/$year/$arm-$contrast.json"
  done
  uv run python -m learning.paired_rank_evaluation \
    --baseline "$ROOT/$CANDIDATE_RUN/83/$year/no-speed/predictions.parquet" \
    --candidate "$ROOT/$CANDIDATE_RUN/83/$year/speed/predictions.parquet" \
    --output "$ROOT/$output_run/$year/speed-feature.json"
done
