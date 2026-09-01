#!/usr/bin/env bash
set -euo pipefail

app_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
compiler=${JRA_VAN_MINGW_CXX:-x86_64-w64-mingw32-g++}
output_dir="$app_dir/.native-cache/prefix/drive_c/JVClient"

if ! command -v "$compiler" >/dev/null 2>&1; then
  printf 'Install the MinGW cross compiler with: brew install mingw-w64\n' >&2
  exit 1
fi
if [[ ! -d $app_dir/.native-cache/prefix ]]; then
  printf 'Native prefix is missing. Run setup:native first.\n' >&2
  exit 1
fi

mkdir -p "$output_dir"
"$compiler" \
  -std=c++20 \
  -Os \
  -Wall \
  -Wextra \
  -Werror \
  -municode \
  -static \
  -s \
  "$app_dir/native/jvlink_demo.cpp" \
  -lole32 \
  -loleaut32 \
  -luuid \
  -o "$output_dir/jvlink-demo.exe"

printf 'Native JV-Link client built: %s\n' "$output_dir/jvlink-demo.exe"
