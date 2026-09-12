#!/usr/bin/env bash
# Keep campaign runtime files in this checkout and retain true FP32 reference math.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export CHRONOS_CAMPAIGN_ROOT="$ROOT/.cache/chronos2"
export HF_HOME="$CHRONOS_CAMPAIGN_ROOT/huggingface"
export UV_CACHE_DIR="$ROOT/.cache/uv"
export TMPDIR="$CHRONOS_CAMPAIGN_ROOT/tmp"
export MLX_ENABLE_TF32=0
mkdir -p "$HF_HOME" "$UV_CACHE_DIR" "$TMPDIR" "$CHRONOS_CAMPAIGN_ROOT/logs"
if [[ $# -eq 0 ]]; then
  printf 'Usage: %s <command> [arguments...]\n' "$0" >&2
  exit 2
fi
cd "$ROOT/apps/timesfm-finish-position"
exec "$@"
