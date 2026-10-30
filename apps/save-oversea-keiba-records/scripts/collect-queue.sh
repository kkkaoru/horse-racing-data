#!/bin/bash
# Serial history collection over a queue file (lines: "kind id [tag]"), resumable after a reboot.
# Usage: scripts/collect-queue.sh BASE_DIR QUEUE_FILE [PAGE_BUDGET]
# BASE_DIR holds plans/<key>.json and receives history/<key>, state.txt and collect.log.
# One collector at a time; pacing and the proxy are enforced by the history CLI. Blocked
# entities are logged and skipped (never auto-retried); touch BASE_DIR/PAUSE to pause.
set -u
BASE="$1"
QUEUE="$2"
BUDGET="${3:-400}"
APP="$(cd "$(dirname "$0")/.." && pwd)"
LOG="$BASE/collect.log"
mkdir -p "$BASE/history"
cd "$APP" || exit 1
for round in $(seq 1 60); do
  paused=0
  while read -r kind id tag; do
    while [ -f "$BASE/PAUSE" ]; do sleep 20; done
    [ -z "$kind" ] && continue
    key="$kind-$id${tag:+-$tag}"
    grep -q "^done $key\$\|^blocked $key\$" "$BASE/state.txt" 2>/dev/null && continue
    out=$(nice bun run src/history-main.ts collect "$BASE/plans/$key.json" "$BASE/history/$key" "$BUDGET" 2>&1)
    code=$?
    echo "$(date -u +%FT%TZ) round=$round $key exit=$code $(echo "$out" | tail -1 | head -c 300)" >> "$LOG"
    if [ $code -eq 0 ]; then echo "done $key" >> "$BASE/state.txt"; fi
    # Exit 1 with archived pages is a real block; without pages it is transient (proxy down), so retry.
    if [ $code -eq 1 ] && [ -d "$BASE/history/$key/pages" ]; then echo "blocked $key" >> "$BASE/state.txt"; fi
    if [ $code -eq 1 ] && [ ! -d "$BASE/history/$key/pages" ]; then paused=1; sleep 30; fi
    if [ $code -eq 2 ]; then paused=1; fi
  done < "$QUEUE"
  [ $paused -eq 0 ] && break
done
echo "$(date -u +%FT%TZ) queue $QUEUE finished" >> "$LOG"
