#!/usr/bin/env bash
set -euo pipefail

app_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
installer="$app_dir/sdk/JVLinkSetup.exe"
engine=${CONTAINER_ENGINE:-docker}
image=${JRA_VAN_DEMO_IMAGE:-jra-van-datalab-wine-demo:local}
engine_args=()

if ! command -v "$engine" >/dev/null 2>&1; then
  printf 'Container engine not found: %s (set CONTAINER_ENGINE to docker or podman)\n' "$engine" >&2
  exit 1
fi
if [[ $engine == docker && $(uname -s) == Darwin && $(uname -m) == arm64 ]]; then
  context=${JRA_VAN_DOCKER_CONTEXT:-colima-jvlink-amd64}
  if ! docker context inspect "$context" >/dev/null 2>&1; then
    printf 'Apple Silicon requires the x86_64 QEMU profile. Run: bun run --filter jra-van-datalab-wine-demo setup:macos\n' >&2
    exit 1
  fi
  engine_args=(--context "$context")
fi
if [[ ! -f "$installer" ]]; then
  "$app_dir/scripts/prepare-sdk.sh"
fi

"$engine" "${engine_args[@]}" build --platform linux/amd64 --tag "$image" "$app_dir"
