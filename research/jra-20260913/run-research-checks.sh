#!/usr/bin/env bash
# Preserve each check generation; do not use the app's broad ellipsis exclusion.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
RUN_ID="${1:?Supply a new check generation name}"
[[ "$RUN_ID" =~ ^[a-z0-9-]+$ ]] || exit 2
export PYTHONPATH="$ROOT/research/jra-20260913:$ROOT/apps/finish-position-predict-container/src"
export COVERAGE_FILE="$ROOT/research/jra-20260913/logs/coverage-$RUN_ID"
PY="$ROOT/apps/timesfm-finish-position/.venv/bin"
FILES=(research/jra-20260913/*.py)
bash research/chronos2/run-local.sh "$PY/ruff" check --config apps/finish-position-predict-container/pyproject.toml "${FILES[@]}"
bash research/chronos2/run-local.sh "$PY/ruff" format --check --config apps/finish-position-predict-container/pyproject.toml "${FILES[@]}"
bash research/chronos2/run-local.sh "$PY/basedpyright" --project apps/finish-position-predict-container/pyproject.toml --pythonpath "$PY/python" "${FILES[@]}"
bash research/chronos2/run-local.sh "$PY/pytest" -p no:cacheprovider --basetemp="research/jra-20260913/test-$RUN_ID" --cov=viewer_mcp --cov=race_rank_review --cov=dedicated_cells --cov=fold_ranker --cov-config=research/jra-20260913/coverage.toml --cov-report=term-missing --cov-report="json:research/jra-20260913/logs/coverage-$RUN_ID.json" --cov-fail-under=95 research/jra-20260913/test_*.py
