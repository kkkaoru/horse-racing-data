#!/usr/bin/env bash
# Existing application credential stays in the environment, never command arguments.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
set -a
# shellcheck source=/dev/null
source "$ROOT/apps/pc-keiba-viewer/.env.local"
set +a
exec bash "$ROOT/research/chronos2/run-local.sh" \
  "$ROOT/apps/timesfm-finish-position/.venv/bin/python" \
  "$ROOT/research/jra-20260913/viewer_mcp.py" "$@"
