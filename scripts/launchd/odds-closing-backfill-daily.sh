#!/usr/bin/env bash
# Daily NAR closing-odds backfill safety net.
#
# Driven by the LaunchAgent at scripts/launchd/com.kkk4oru.odds-closing-backfill.plist
# (JST 22:30 daily, well after the last NAR race finishes). Runs in two phases:
#
#   1. `apps/sync-realtime-data/src/scripts/backfill-nar-realtime-date.ts`
#      re-fetches the post-race entries / odds / results / weights from
#      keiba.go for every NAR race on the target JST date and writes them to
#      the LEGACY `sync-realtime-data` D1 (odds_snapshots / race_*_snapshots).
#
#   2. `apps/sync-realtime-data/src/scripts/transfer-odds-old-to-hot.ts`
#      copies the legacy `odds_snapshots` rows into the HOT
#      `sync-realtime-data-hot` D1 that the viewer reads from, then purges
#      the per-race `odds:latest:nar:YYYYMMDD:NN:RR` KV mirror so the next
#      viewer fetch returns fresh closing odds.
#
# This is a safety net for the case where the hot worker's per-minute odds
# polling cron entered a silent-death state during the day (as happened on
# 2026-06-22 between 14:23 JST and 21:00 JST) — the closing odds get
# re-captured from keiba.go and reconciled with the hot D1.
#
# Manual invocation (today's JST date):
#   bash scripts/launchd/odds-closing-backfill-daily.sh
# or with an explicit date:
#   RUN_DATE=20260622 bash scripts/launchd/odds-closing-backfill-daily.sh
#
# Lock coordination:
#   Holds /tmp/odds-closing-backfill.lock (atomic mkdir) so an interactive
#   manual fire cannot race the scheduled JST 22:30 run.
set -euo pipefail

# Resolve repo root from this script's location (scripts/launchd -> repo root).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

# Constants.
LOG_FILE="/tmp/odds-closing-backfill.log"
LOCK_DIR="/tmp/odds-closing-backfill.lock"
BACKFILL_SCRIPT="apps/sync-realtime-data/src/scripts/backfill-nar-realtime-date.ts"
TRANSFER_SCRIPT="apps/sync-realtime-data/src/scripts/transfer-odds-old-to-hot.ts"

# Single-writer lock. mkdir is atomic on macOS (test-and-set in one syscall).
# If lock is held, exit 0 with a log — a concurrent run would just hit the
# same upstream and waste keiba.go bandwidth.
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  printf '%s [odds-closing-backfill] lock %s held; another run in progress, skipping\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$LOCK_DIR" >> "$LOG_FILE"
  exit 0
fi
trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT

# JST = UTC+9. `date -u +%Y%m%d -v+9H` adds 9 h to current UTC and formats as
# YYYYMMDD = "today in JST" regardless of the Mac's local timezone. RUN_DATE
# env override is honoured for manual reruns.
RUN_DATE="${RUN_DATE:-$(date -u -v+9H +%Y%m%d)}"

# tee everything from here on to the rolling log. The plist captures the raw
# stdout/stderr to its own files; this rolling log is the per-tick record.
exec > >(tee -a "$LOG_FILE") 2>&1

log() {
  printf '%s [odds-closing-backfill] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

fail() {
  local msg="$1"
  log "ERROR: $msg"
  exit 1
}

log "RUN_DATE=$RUN_DATE REPO_ROOT=$REPO_ROOT"

# Phase 1: closing-odds backfill into legacy D1.
log "phase 1: backfilling closing odds from keiba.go into legacy D1..."
if ! bun "$BACKFILL_SCRIPT" "$RUN_DATE"; then
  fail "phase 1 failed: $BACKFILL_SCRIPT $RUN_DATE"
fi
log "phase 1 OK"

# Phase 2: transfer legacy -> hot D1 and purge odds:latest KV mirror.
log "phase 2: transferring legacy odds_snapshots into hot D1 + purging odds:latest KV..."
if ! bun "$TRANSFER_SCRIPT" "$RUN_DATE"; then
  fail "phase 2 failed: $TRANSFER_SCRIPT $RUN_DATE"
fi
log "phase 2 OK"

log "SUCCESS RUN_DATE=$RUN_DATE"
exit 0
