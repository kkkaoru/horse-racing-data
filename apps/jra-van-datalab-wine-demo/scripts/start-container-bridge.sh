#!/usr/bin/env bash
set -euo pipefail

app_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
token_file="$app_dir/.native-cache/container-bridge-token"

if [[ -z ${JRA_VAN_DATALAB_KEY:-} ]]; then
  printf 'JRA_VAN_DATALAB_KEY is required by the host bridge.\n' >&2
  exit 1
fi
if ! command -v openssl >/dev/null 2>&1; then
  printf 'openssl is required to generate an ephemeral bridge token.\n' >&2
  exit 1
fi

umask 077
openssl rand -hex 32 >"$token_file"
export JRA_VAN_BRIDGE_TOKEN
JRA_VAN_BRIDGE_TOKEN=$(<"$token_file")
cd "$app_dir"
exec uv run python -m src.bridge --host 0.0.0.0 --port "${JRA_VAN_BRIDGE_PORT:-56532}"
