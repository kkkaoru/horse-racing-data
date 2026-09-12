#!/usr/bin/env bash
# Rebuild the existing production base feature recipe from LOCAL PostgreSQL.
# This is a baseline parity investigation, not candidate training or promotion.
set -euo pipefail
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd "$APP_DIR/../.." && pwd)/docs/finish-position-accuracy/experiments/20260912-local-pg"
FEATURE_RUN="${FEATURE_RUN:-production-baseline-features}"
case "$FEATURE_RUN" in
  production-baseline-features|rich-history-v1|rich-history-retired-v1|production-upcoming-features|production-upcoming-single-race) ;;
  *) echo "Unsupported FEATURE_RUN" >&2; exit 1 ;;
esac
FEATURE_CATEGORY="${FEATURE_CATEGORY:-ban-ei}"
case "$FEATURE_CATEGORY" in
  ban-ei|nar) ;;
  *) echo "Unsupported FEATURE_CATEGORY" >&2; exit 1 ;;
esac
FROM_DATE="${FROM_DATE:-20260519}"
TO_DATE="${TO_DATE:-20260911}"
date_args=(--from-date "$FROM_DATE" --to-date "$TO_DATE")
if [[ -n "${TARGET_DATE:-}" ]]; then
  date_args=(--target-date "$TARGET_DATE" --days-ahead 0)
fi
if [[ -n "${TARGET_RACE:-}" ]]; then
  date_args+=(--target-race "$TARGET_RACE")
fi
: "${PG_URL:?Set PG_URL to the authorized local PostgreSQL connection}"
case "$PG_URL" in
  postgresql://*@192.168.64.2:5432/horse_racing|postgresql://*@localhost:5432/horse_racing) ;;
  *) echo "This investigation accepts only the local horse_racing database." >&2; exit 1 ;;
esac
mkdir -p "$ROOT/$FEATURE_RUN/83"
cd "$APP_DIR"
uv run python src/scripts/finish_position_features_duckdb.py \
  --category "$FEATURE_CATEGORY" "${date_args[@]}" \
  --pg-url "$PG_URL" --threads 2 --memory-limit 8GB \
  --output-dir "$ROOT/$FEATURE_RUN/83/base" \
  --temp-dir "$ROOT/$FEATURE_RUN/83/working-storage" \
  --status-file "$ROOT/$FEATURE_RUN/83/status.json" \
  --log-file "$ROOT/$FEATURE_RUN/83/build.jsonl"
