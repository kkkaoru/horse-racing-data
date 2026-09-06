#!/usr/bin/env bash
set -euo pipefail

export DISPLAY=:99
export WINEARCH=win64
export WINEDEBUG=-all
export WINEPREFIX=/var/lib/jvlink/prefix

ready_path=/tmp/jvlink-ready
rm -f "$ready_path"
python3 /opt/jvlink/server.py >/tmp/server.log 2>&1 &
server_pid=$!
xvfb_pid=""
x11vnc_pid=""
websockify_pid=""
cleanup() {
  if [[ -f $ready_path ]]; then
    /opt/jvlink/state.sh checkpoint || true
  fi
  pids=("$server_pid")
  [[ -n $websockify_pid ]] && pids+=("$websockify_pid")
  [[ -n $x11vnc_pid ]] && pids+=("$x11vnc_pid")
  [[ -n $xvfb_pid ]] && pids+=("$xvfb_pid")
  kill "${pids[@]}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

/opt/jvlink/state.sh restore
Xvfb "$DISPLAY" -screen 0 1280x800x24 -nolisten tcp >/tmp/xvfb.log 2>&1 &
xvfb_pid=$!
for _attempt in {1..100}; do
  [[ -S /tmp/.X11-unix/X99 ]] && break
  sleep 0.1
done
wineserver --persistent
wine net.exe start JVLink64Agent >/dev/null 2>&1 || true
if ! wine sc.exe query JVLink64Agent \
  | grep -Eq 'STATE[[:space:]]+:[[:space:]]+4[[:space:]]+RUNNING'; then
  printf 'JVLink64Agent service did not start.\n' >&2
  exit 1
fi
x11vnc -display "$DISPLAY" -localhost -forever -shared -nopw >/tmp/x11vnc.log 2>&1 &
x11vnc_pid=$!
websockify --web /usr/share/novnc 6080 localhost:5900 >/tmp/websockify.log 2>&1 &
websockify_pid=$!
touch "$ready_path"

wait "$server_pid"
