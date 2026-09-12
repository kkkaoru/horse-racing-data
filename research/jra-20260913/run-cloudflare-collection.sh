#!/usr/bin/env bash
# Run local gates, then collect read-only Cloudflare evidence with original pages.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
PY="$ROOT/apps/timesfm-finish-position/.venv/bin"
export PYTHONPATH="$ROOT/research/jra-20260913"
export COVERAGE_FILE="$ROOT/research/jra-20260913/logs/client-coverage-001"
FILES=(research/jra-20260913/viewer_mcp.py research/jra-20260913/test_viewer_mcp.py research/jra-20260913/test_viewer_mcp_paging.py)
bash research/chronos2/run-local.sh "$PY/ruff" check --config apps/finish-position-predict-container/pyproject.toml "${FILES[@]}"
bash research/chronos2/run-local.sh "$PY/ruff" format --check --config apps/finish-position-predict-container/pyproject.toml "${FILES[@]}"
bash research/chronos2/run-local.sh "$PY/basedpyright" --project apps/finish-position-predict-container/pyproject.toml --pythonpath "$PY/python" "${FILES[@]}"
bash research/chronos2/run-local.sh "$PY/pytest" -p no:cacheprovider --basetemp=research/jra-20260913/test-work-001 --cov=viewer_mcp --cov-config=apps/finish-position-predict-container/pyproject.toml --cov-report=term-missing --cov-report=json:research/jra-20260913/logs/client-coverage-001.json --cov-fail-under=95 research/jra-20260913/test_viewer_mcp.py research/jra-20260913/test_viewer_mcp_paging.py
bash research/jra-20260913/run-viewer-read.sh --tool get_daily_finish_predictions --arguments '{"year":"2026","month":"09","day":"12","source":"jra"}' --complete --output research/jra-20260913/cloudflare-002/predictions-20260912.json
for venue in 06 09; do
  for race in 01 02 03 04 05 06 07 08 09 10 11 12; do
    bash research/jra-20260913/run-viewer-read.sh --tool get_race_section --arguments "{\"year\":\"2026\",\"month\":\"09\",\"day\":\"12\",\"keibajoCode\":\"$venue\",\"raceNumber\":\"$race\",\"section\":\"results\"}" --complete --output "research/jra-20260913/cloudflare-002/results-20260912-$venue-$race.json"
  done
done
bash research/jra-20260913/run-viewer-read.sh --tool get_daily_finish_predictions --arguments '{"year":"2026","month":"09","day":"13","source":"jra"}' --complete --output research/jra-20260913/cloudflare-002/predictions-20260913.json
printf 'CLOUDFLARE_COLLECTION_COMPLETE\n'
