#!/usr/bin/env bash
set -euo pipefail

app_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
architecture=${1:-}
engine=${CONTAINER_ENGINE:-docker}

case "$architecture" in
  amd64)
    context=${JRA_VAN_AMD64_CONTEXT:-colima-jvlink-amd64}
    ;;
  arm64)
    context=${JRA_VAN_ARM64_CONTEXT:-colima}
    ;;
  *)
    printf 'Usage: %s amd64|arm64\n' "${0##*/}" >&2
    exit 2
    ;;
esac

image=${JRA_VAN_BRIDGE_IMAGE:-jra-van-datalab-wine-demo:bridge-$architecture}
"$engine" --context "$context" build \
  --platform "linux/$architecture" \
  --file "$app_dir/Dockerfile.bridge" \
  --tag "$image" \
  "$app_dir"
actual=$("$engine" --context "$context" image inspect --format '{{.Architecture}}' "$image")
if [[ $actual != "$architecture" ]]; then
  printf 'Expected %s image, got %s.\n' "$architecture" "$actual" >&2
  exit 1
fi
printf 'Container bridge image ready: image=%s architecture=%s context=%s\n' \
  "$image" "$actual" "$context"
