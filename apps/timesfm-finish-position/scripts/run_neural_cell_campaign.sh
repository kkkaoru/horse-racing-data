#!/usr/bin/env bash
# Train the MPS neural cell ranker on a production feature snapshot for every
# evaluation year, then run the production per-cell Top1..Top5 comparison.
#
# Usage:
#   scripts/run_neural_cell_campaign.sh <category> <features-root> <metadata> <output-root>
set -euo pipefail
cd "$(dirname "$0")/.."

CATEGORY=${1:?category}
FEATURES_ROOT=${2:?features root}
METADATA=${3:?model metadata}
OUTPUT_ROOT=${4:?output root}

case "$CATEGORY" in
  jra) FOLDER=prophet-jra-lab ;;
  nar | ban-ei) FOLDER=prophet-lab ;;
  *) echo "unsupported category: $CATEGORY" >&2; exit 2 ;;
esac

TRENDS=tmp/neural-cell-trends
mkdir -p "$TRENDS/$FOLDER"

for YEAR in 2024 2025 2026; do
  .venv/bin/python scripts/train_neural_cell_blend.py \
    --category "$CATEGORY" \
    --features-root "$FEATURES_ROOT" \
    --metadata "$METADATA" \
    --train-from 2000 \
    --test-year "$YEAR" \
    --epochs 30 \
    --output-dir "$OUTPUT_ROOT" \
    --artifact "$OUTPUT_ROOT/neural-cell-blend-$CATEGORY-$YEAR.json"
  cp -f "$OUTPUT_ROOT/neural-trends-$YEAR.parquet" \
    "$TRENDS/$FOLDER/prophet-entity-trends-$YEAR.parquet"
done

PYTHONPATH=../finish-position-predict-container/src:src .venv/bin/python \
  scripts/optimize_prophet_cell_policy.py \
  --trends-root "$TRENDS" \
  --trends-2026 "$CATEGORY=$OUTPUT_ROOT/neural-trends-2026.parquet" \
  --output "results/neural-cell-policy-$CATEGORY-evaluation-2024-2026.json"
