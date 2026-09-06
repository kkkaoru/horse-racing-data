#!/usr/bin/env bash
set -euo pipefail

app_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
reference_dir="$app_dir/../jra-van-datalab-wine-demo"
source=${1:-$reference_dir/sdk/JVLinkSetup.exe}
destination="$app_dir/sdk/JVLinkSetup.exe"

if [[ ! -f $source ]]; then
  printf 'JVLinkSetup.exe not found. Prepare the reference demo first or pass its path.\n' >&2
  exit 1
fi
cp "$source" "$destination"
printf 'Prepared private SDK installer: %s\n' "$destination"
