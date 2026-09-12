#!/usr/bin/env bash
# Reuse production's six layers that supply the frozen 130-feature Ban-ei model.
# These reproduce booster inputs only, not the complete serving calculation.
# In particular, Prophet's later post-score adjustment is NOT applied here.
set -euo pipefail
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FEATURE_RUN="${FEATURE_RUN:-production-baseline-features}"
case "$FEATURE_RUN" in
  production-baseline-features|rich-history-v1|rich-history-retired-v1|production-upcoming-features|production-upcoming-single-race) ;;
  *) echo "Unsupported FEATURE_RUN" >&2; exit 1 ;;
esac
HISTORY_FROM_DATE="${HISTORY_FROM_DATE:-20100101}"
TO_DATE="${TO_DATE:-20260911}"
ROOT="$(cd "$APP_DIR/../.." && pwd)/docs/finish-position-accuracy/experiments/20260912-local-pg/$FEATURE_RUN/83"
: "${PG_URL:?Set PG_URL to the authorized local PostgreSQL connection}"
case "$PG_URL" in
  postgresql://*@192.168.64.2:5432/horse_racing|postgresql://*@localhost:5432/horse_racing) ;;
  *) echo "Only the local horse_racing database is allowed." >&2; exit 1 ;;
esac
cd "$APP_DIR"
LAYER_DIR="$APP_DIR/src/scripts/finish-position-features"
export PIPELINE_MAX_MEMORY_GB=4 PIPELINE_MAX_THREADS=2
input="${BASE_INPUT:-$ROOT/base}"
run_layer() {
  local script="$1"
  local name="$2"
  shift 2
  local output="$ROOT/$name"
  export PIPELINE_SPILL_TEMP_DIR="$ROOT/spill-$name"
  uv run python "$LAYER_DIR/$script" --input-dir "$input" --output-dir "$output" \
    --pg-url "$PG_URL" --from-date "$HISTORY_FROM_DATE" "$@"
  input="$output"
}
run_layer add-grade-race-lineage-features.py lineage --config "$LAYER_DIR/lineage-races/ban-ei.json" --to-date "$TO_DATE"
run_layer add-head-to-head-features.py head-to-head
run_layer add-baba-pedigree-affinity-features.py baba-pedigree
run_layer add-banei-futan-class-features.py futan-class
run_layer add-banei-grade-career-features.py grade-career
run_layer add-similar-race-features.py final --category ban-ei --threads 2 --memory-limit 4GB
