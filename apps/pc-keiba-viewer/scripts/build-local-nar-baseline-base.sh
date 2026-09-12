#!/usr/bin/env bash
# Reconstruct actual NAR fallback inputs from local PostgreSQL only.
set -euo pipefail
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd "$APP_DIR/../.." && pwd)/docs/finish-position-accuracy/experiments/20260912-local-pg/production-baseline-features/nar"
: "${PG_URL:?Set the authorized local PG_URL}"
case "$PG_URL" in
  postgresql://*@192.168.64.2:5432/horse_racing|postgresql://*@localhost:5432/horse_racing) ;;
  *) echo 'Only the local horse_racing database is allowed.' >&2; exit 1 ;;
esac
cd "$APP_DIR"
uv run python src/scripts/finish_position_features_duckdb.py \
  --category nar --from-date 20260519 --to-date 20260911 \
  --pg-url "$PG_URL" --threads 2 --memory-limit 8GB \
  --output-dir "$ROOT/base" --temp-dir "$ROOT/working-storage" \
  --status-file "$ROOT/status.json" --log-file "$ROOT/build.jsonl"
