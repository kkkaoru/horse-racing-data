#!/usr/bin/env bash
# Fresh read-only forecast availability check near the09:00 deadline; no outcomes.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
OUTPUT=research/jra-20260913/final-forecast-check-001/predictions-20260913.json
if [ -e "$OUTPUT" ]; then
  printf 'Refusing to overwrite retained forecast snapshot\n' >&2
  exit 1
fi
bash research/jra-20260913/run-viewer-read.sh \
  --tool get_daily_finish_predictions \
  --arguments '{"year":"2026","month":"09","day":"13","source":"jra"}' \
  --complete --output "$OUTPUT"
