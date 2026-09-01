#!/usr/bin/env bash
set -euo pipefail

app_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
engine=${CONTAINER_ENGINE:-docker}
image=${JRA_VAN_DEMO_IMAGE:-jra-van-datalab-wine-demo:local}
engine_args=()

if [[ -z ${JRA_VAN_DATALAB_KEY:-} ]]; then
  printf 'JRA_VAN_DATALAB_KEY is not set.\n' >&2
  exit 1
fi
if [[ $engine == docker && $(uname -s) == Darwin && $(uname -m) == arm64 ]]; then
  context=${JRA_VAN_DOCKER_CONTEXT:-colima-jvlink-amd64}
  engine_args=(--context "$context")
fi
if ! "$engine" "${engine_args[@]}" image inspect "$image" >/dev/null 2>&1; then
  "$app_dir/scripts/build.sh"
fi
mkdir -p "$app_dir/data"

"$engine" "${engine_args[@]}" run --rm --platform linux/amd64 \
  --env JRA_VAN_DATALAB_KEY \
  --volume "$app_dir/data:/data" \
  "$image" "$@"
