#!/usr/bin/env bash
set -euo pipefail

Xvfb "$DISPLAY" -screen 0 1024x768x24 -nolisten tcp >/tmp/xvfb.log 2>&1 &
xvfb_pid=$!
cleanup() {
  wineserver --kill >/dev/null 2>&1 || true
  kill "$xvfb_pid" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

for _attempt in {1..50}; do
  if [[ -S /tmp/.X11-unix/X99 ]]; then
    break
  fi
  sleep 0.1
done

exec wine 'C:\Python313\python.exe' 'Z:\app\src\demo.py' \
  --output 'Z:\data\records.txt' "$@"
