#!/usr/bin/env bash
# Keep new research outputs, tool state and temporary files inside this repository.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export CHRONOS_CAMPAIGN_ROOT="$ROOT/research/chronos2"
export HF_HOME="$CHRONOS_CAMPAIGN_ROOT/runtime/huggingface"
export HF_HUB_CACHE="$HF_HOME/hub"
export HF_HUB_OFFLINE=1
export TRANSFORMERS_OFFLINE=1
export UV_CACHE_DIR="$CHRONOS_CAMPAIGN_ROOT/runtime/uv"
export XDG_CACHE_HOME="$CHRONOS_CAMPAIGN_ROOT/runtime/tool-state"
export TORCH_HOME="$CHRONOS_CAMPAIGN_ROOT/runtime/torch"
export MPLCONFIGDIR="$CHRONOS_CAMPAIGN_ROOT/runtime/matplotlib"
export TMPDIR="$CHRONOS_CAMPAIGN_ROOT/runtime/scratch"
export TMP="$TMPDIR"
export TEMP="$TMPDIR"
export PYTHONDONTWRITEBYTECODE=1
export MLX_ENABLE_TF32=0
mkdir -p "$HF_HUB_CACHE" "$UV_CACHE_DIR" "$XDG_CACHE_HOME" "$TORCH_HOME" "$MPLCONFIGDIR" "$TMPDIR"
if [[ $# -eq 0 ]]; then
  printf 'Usage: %s <command> [arguments...]\n' "$0" >&2
  exit 2
fi
cd "$ROOT"
exec "$@"
