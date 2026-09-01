#!/usr/bin/env bash
set -euo pipefail

app_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
architecture=${1:-}
shift || true
engine=${CONTAINER_ENGINE:-docker}
token_file="$app_dir/.native-cache/container-bridge-token"

case "$architecture" in
  amd64)
    context=${JRA_VAN_AMD64_CONTEXT:-colima-jvlink-amd64}
    ;;
  arm64)
    context=${JRA_VAN_ARM64_CONTEXT:-colima}
    ;;
  *)
    printf 'Usage: %s amd64|arm64 [demo options]\n' "${0##*/}" >&2
    exit 2
    ;;
esac

if [[ ! -s $token_file ]]; then
  printf 'Host bridge token is missing. Start start-container-bridge.sh first.\n' >&2
  exit 1
fi
image=${JRA_VAN_BRIDGE_IMAGE:-jra-van-datalab-wine-demo:bridge-$architecture}
output_dir="$app_dir/data/container-$architecture"
mkdir -p "$output_dir"

"$engine" --context "$context" run --rm \
  --platform "linux/$architecture" \
  --env "JRA_VAN_BRIDGE_URL=${JRA_VAN_BRIDGE_URL:-http://host.docker.internal:56532}" \
  --env "JRA_VAN_BRIDGE_TOKEN=$(<"$token_file")" \
  --volume "$output_dir:/data" \
  "$image" "$@"
