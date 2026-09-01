#!/usr/bin/env bash
set -euo pipefail

app_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cache_dir="$app_dir/.native-cache"
prefix="$cache_dir/prefix"
wine_root="$cache_dir/wine-11.16/Wine Staging.app/Contents/Resources/wine"

native_client="$prefix/drive_c/JVClient/jvlink-demo.exe"
python_client="$prefix/drive_c/Python313/python.exe"
if [[ ! -x $wine_root/bin/wine || (! -f $native_client && ! -x $python_client) ]]; then
  printf 'Native runtime is not ready. Run setup:native and build:native-client first.\n' >&2
  exit 1
fi
if [[ -z ${JRA_VAN_DATALAB_KEY:-} ]]; then
  printf 'JRA_VAN_DATALAB_KEY is required.\n' >&2
  exit 1
fi

export PATH="$wine_root/bin:$PATH"
export WINEPREFIX="$prefix"
export WINEARCH=win64
export WINEDEBUG=${WINEDEBUG:--all}
export MVK_CONFIG_LOG_LEVEL=0
export LANG=ja_JP.UTF-8
export LC_ALL=ja_JP.UTF-8

cleanup() {
  wineserver -k >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

wine net.exe start JVLink64Agent >/dev/null 2>&1 || true
if ! wine sc.exe query JVLink64Agent | grep -Eq 'STATE[[:space:]]+:[[:space:]]+4[[:space:]]+RUNNING'; then
  printf 'JVLink64Agent did not start. Re-run the setup:native script.\n' >&2
  exit 1
fi

windows_app="Z:${app_dir//\//\\}"
if [[ -f $native_client ]]; then
  command=(
    'C:\JVClient\jvlink-demo.exe'
    --output "$windows_app\\data\\native-records.txt"
    --save-path 'C:\JVData'
    "$@"
  )
  process_pattern='C:\\JVClient\\jvlink-demo.exe'
else
  command=(
    'C:\Python313\python.exe'
    "$windows_app\\src\\demo.py"
    --output "$windows_app\\data\\native-records.txt"
    --save-path 'C:\JVData'
    "$@"
  )
  process_pattern='C:\\Python313\\python.exe.*demo.py'
fi

if [[ ${JRA_VAN_NATIVE_UI:-0} == 1 ]]; then
  wine explorer /desktop=JVLinkJapanese,1024x768 "${command[@]}" &
  desktop_pid=$!
  child_started=0
  for _attempt in {1..200}; do
    if pgrep -f "$process_pattern" >/dev/null; then
      child_started=1
      break
    fi
    sleep 0.1
  done
  if ((child_started == 0)); then
    printf 'JV-Link demo did not start in the Wine desktop.\n' >&2
    exit 1
  fi
  while pgrep -f "$process_pattern" >/dev/null; do
    sleep 0.25
  done
  wait "$desktop_pid" || true
else
  wine "${command[@]}"
fi
