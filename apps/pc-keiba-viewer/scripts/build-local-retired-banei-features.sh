#!/usr/bin/env bash
# Recover old-venue observations required by the seed-horse training scope.
set -euo pipefail
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$APP_DIR/../.." && pwd)"
export FEATURE_RUN=rich-history-retired-v1 FEATURE_CATEGORY=nar
export FROM_DATE=20050101 TO_DATE=20061231 HISTORY_FROM_DATE=20000101
bash "$APP_DIR/scripts/build-local-banei-production-base.sh"
cd "$REPO_ROOT"
uv run --project "$APP_DIR" python -c '
from pathlib import Path
import duckdb
path = Path("docs/finish-position-accuracy/experiments/20260912-local-pg/select-retired-banei-base.sql")
with duckdb.connect() as connection:
    connection.execute(path.read_text(encoding="utf-8"))
'
export BASE_INPUT="$REPO_ROOT/docs/finish-position-accuracy/experiments/20260912-local-pg/rich-history-retired-v1/83/retired-base"
bash "$APP_DIR/scripts/build-local-banei-production-layers.sh"
