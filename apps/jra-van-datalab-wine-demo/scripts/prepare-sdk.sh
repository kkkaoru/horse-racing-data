#!/usr/bin/env bash
set -euo pipefail

app_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
repo_dir=$(cd "$app_dir/../.." && pwd)
default_sdk="$repo_dir/temp/JRA-VAN_Data_Lab_SDK_Ver5_0_0_64bit/JV-Link/JV-Link.exe"
source_sdk=${1:-$default_sdk}
destination="$app_dir/sdk/JV-Link.exe"
setup="$app_dir/sdk/JVLinkSetup.exe"

if [[ ! -f "$source_sdk" ]]; then
  printf 'JV-Link installer not found: %s\n' "$source_sdk" >&2
  printf 'Pass its path as the first argument.\n' >&2
  exit 1
fi

if ! command -v bsdtar >/dev/null 2>&1; then
  printf 'bsdtar (libarchive) is required to extract the SDK installer.\n' >&2
  exit 1
fi

cp "$source_sdk" "$destination"
bsdtar -xOf "$destination" JVLinkSetup.exe >"$setup"
printf 'Prepared private SDK installer: %s\n' "$setup"
