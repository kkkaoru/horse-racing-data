#!/usr/bin/env bash
# Hourly race-prediction completeness guard.
#
# Driven by the LaunchAgent at scripts/launchd/com.kkk4oru.race-prediction-guard.plist:
#   * JST 00:00 .. 09:00 (10 hourly fires) -> guard TODAY JST
#   * JST 21:00 .. 23:00 (3  hourly fires) -> guard TOMORROW JST
# Total 13 fires/day. Re-runs are idempotent: Neon prediction tables are the
# state of truth — if every distinct race_key for TARGET_DATE_ISO already has
# at least one row in the respective table, the guard exits without kicking.
#
# Two prediction kinds are guarded:
#   1. running-style (脚質)   -> Cloudflare Worker job (POST /api/jobs).
#   2. finish-position (着順)  -> local docker pipeline via
#                                 scripts/launchd/finish-position-predict-daily.sh.
#
# Source of truth for "expected races":
#   Cloudflare D1 sync-realtime-data.realtime_race_sources where
#   substr(race_start_at_jst, 1, 10) = TARGET_DATE_ISO. race_start_at_jst is
#   ISO 8601 with +09:00 offset, so substr(...,1,10) is the JST calendar date.
#
# Source of truth for "actually predicted":
#   Neon Postgres race_running_style_model_predictions /
#   race_finish_position_model_predictions, both keyed by the quadruple
#   (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango). The matching D1
#   columns have the same names + same TEXT format (year=YYYY, monthDay=MMDD).
#
# Locks:
#   /tmp/race-prediction-guard.lock  -- guard-level (single concurrent guard)
#   /tmp/finish-position-predict.lock -- shared with finish-position-predict-daily.sh
#                                        so JST 03:00 cron and the hourly kick
#                                        cannot race the same docker run.
#
# Manual / dry-run:
#   DRY_RUN=1 bash scripts/launchd/race-prediction-guard.sh
#   DRY_RUN=1 FORCE_HOUR=05 bash scripts/launchd/race-prediction-guard.sh   # today
#   DRY_RUN=1 FORCE_HOUR=22 bash scripts/launchd/race-prediction-guard.sh   # tomorrow
set -euo pipefail

# Resolve repo root from this script's location (scripts/launchd -> repo root).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

# Constants.
LOG_DIR="/Users/kkk4oru/Library/Logs/race-prediction-guard"
GUARD_LOCK_DIR="/tmp/race-prediction-guard.lock"
FINISH_LOCK_DIR="/tmp/finish-position-predict.lock"
NEON_ENV_FILE="apps/local-postgresql/.env.replica"
DEV_VARS_FILE="apps/sync-realtime-data/.dev.vars"
D1_BINDING_NAME="sync-realtime-data"
WRANGLER_CONFIG="apps/sync-realtime-data/wrangler.jsonc"
RS_TABLE="race_running_style_model_predictions"
FP_TABLE="race_finish_position_model_predictions"
RS_KICK_URL="https://sync-realtime-data.kkk4oru.com/api/jobs"
RS_KICK_JOB_TYPE="plan-running-style-predictions"
FINISH_SCRIPT="$REPO_ROOT/scripts/launchd/finish-position-predict-daily.sh"

mkdir -p "$LOG_DIR"

# Single-writer lock for the guard itself. mkdir is atomic on macOS — flock is
# not shipped with macOS so we cannot use it portably here.
if ! mkdir "$GUARD_LOCK_DIR" 2>/dev/null; then
  printf '%s [race-prediction-guard] guard lock %s held; concurrent guard run, skipping\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$GUARD_LOCK_DIR" >> "$LOG_DIR/lock-skips.log"
  exit 0
fi
trap 'rmdir "$GUARD_LOCK_DIR" 2>/dev/null || true' EXIT

# Dated log capturing everything from here.
TODAY_DATE="$(date -u -v+9H +%Y%m%d)"
DATED_LOG="$LOG_DIR/${TODAY_DATE}.log"
exec > >(tee -a "$DATED_LOG") 2>&1

log() {
  printf '%s [race-prediction-guard] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

mask() {
  # Mask credentials in URLs of the form scheme://user:pass@host/...
  sed -E 's#(://)[^:@/]+:[^@]+@#\1***:***@#g'
}

# Determine the JST hour (00..23). FORCE_HOUR may override under DRY_RUN=1 only.
JST_HOUR="$(date -u -v+9H +%H)"
if [ "${DRY_RUN:-0}" = "1" ] && [ -n "${FORCE_HOUR:-}" ]; then
  JST_HOUR="$FORCE_HOUR"
  log "DRY_RUN with FORCE_HOUR=$JST_HOUR override"
fi

# Map the JST hour to the target date.
#   0-9   -> today
#   21-23 -> tomorrow
#   else  -> exit (not a scheduled window)
TODAY_ISO="${TODAY_DATE:0:4}-${TODAY_DATE:4:2}-${TODAY_DATE:6:2}"
TOMORROW_DATE="$(date -u -v+9H -v+1d +%Y%m%d)"
TOMORROW_ISO="${TOMORROW_DATE:0:4}-${TOMORROW_DATE:4:2}-${TOMORROW_DATE:6:2}"

case "$JST_HOUR" in
  0[0-9])
    TARGET_DATE="$TODAY_DATE"
    TARGET_DATE_ISO="$TODAY_ISO"
    TARGET_DAYS_AHEAD=0
    log "window=today JST_HOUR=$JST_HOUR TARGET_DATE_ISO=$TARGET_DATE_ISO"
    ;;
  21|22|23)
    TARGET_DATE="$TOMORROW_DATE"
    TARGET_DATE_ISO="$TOMORROW_ISO"
    TARGET_DAYS_AHEAD=1
    log "window=tomorrow JST_HOUR=$JST_HOUR TARGET_DATE_ISO=$TARGET_DATE_ISO"
    ;;
  *)
    log "outside guard window (JST_HOUR=$JST_HOUR) — exit 0"
    exit 0
    ;;
esac

# DRY_RUN-only override so the dry-run can be aimed at a date with known
# expected/actual values for verification. Ignored when DRY_RUN!=1.
if [ "${DRY_RUN:-0}" = "1" ] && [ -n "${FORCE_TARGET_DATE:-}" ]; then
  TARGET_DATE="$FORCE_TARGET_DATE"
  TARGET_DATE_ISO="${TARGET_DATE:0:4}-${TARGET_DATE:4:2}-${TARGET_DATE:6:2}"
  log "DRY_RUN with FORCE_TARGET_DATE=$TARGET_DATE override"
fi

# kaisai_nen / kaisai_tsukihi split for Neon WHERE clauses (TEXT in both DBs).
TARGET_NEN="${TARGET_DATE:0:4}"
TARGET_TSUKIHI="${TARGET_DATE:4:4}"

# Read NEON_DATABASE_URL once (used for both Neon counts).
if [ ! -f "$NEON_ENV_FILE" ]; then
  log "ERROR: $NEON_ENV_FILE not found — cannot query Neon"
  exit 1
fi
NEON_LINE="$(grep -E '^NEON_DATABASE_URL=' "$NEON_ENV_FILE" | head -1 || true)"
if [ -z "$NEON_LINE" ]; then
  log "ERROR: NEON_DATABASE_URL not set in $NEON_ENV_FILE"
  exit 1
fi
NEON_DATABASE_URL="${NEON_LINE#NEON_DATABASE_URL=}"
NEON_DATABASE_URL="${NEON_DATABASE_URL%\'}"
NEON_DATABASE_URL="${NEON_DATABASE_URL#\'}"
NEON_DATABASE_URL="${NEON_DATABASE_URL%\"}"
NEON_DATABASE_URL="${NEON_DATABASE_URL#\"}"
if [ -z "$NEON_DATABASE_URL" ]; then
  log "ERROR: NEON_DATABASE_URL parsed empty"
  exit 1
fi
export NEON_DATABASE_URL
log "NEON_DATABASE_URL=$(printf '%s' "$NEON_DATABASE_URL" | mask)"

# Query D1 for the expected race count for TARGET_DATE_ISO. wrangler is the
# established pattern in this repo (see apps/sync-realtime-data/scripts/*).
log "querying D1 expected race_key count for $TARGET_DATE_ISO ..."
D1_QUERY="SELECT COUNT(DISTINCT race_key) AS c FROM realtime_race_sources WHERE substr(race_start_at_jst, 1, 10) = '${TARGET_DATE_ISO}';"
D1_RESULT="$(bunx wrangler d1 execute "$D1_BINDING_NAME" --remote --config "$WRANGLER_CONFIG" --command "$D1_QUERY" --json 2>&1 || true)"
EXPECTED_COUNT="$(printf '%s' "$D1_RESULT" | jq -r '.[0].results[0].c // empty' 2>/dev/null || true)"
if [ -z "$EXPECTED_COUNT" ] || [ "$EXPECTED_COUNT" = "null" ]; then
  log "ERROR: failed to parse expected race count from D1 (result tail: $(printf '%s' "$D1_RESULT" | tail -c 400))"
  exit 1
fi
log "EXPECTED_COUNT=$EXPECTED_COUNT (distinct race_key in realtime_race_sources)"

if [ "$EXPECTED_COUNT" = "0" ]; then
  log "no races scheduled for $TARGET_DATE_ISO — nothing to guard, exit 0"
  exit 0
fi

# Query Neon for COUNT(DISTINCT (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango))
# in the named table for TARGET_DATE. Uses uv with a transient psycopg dep so we
# don't depend on a system psql. Returns "<integer>" or exits non-zero.
neon_count() {
  local table="$1"
  uv run --quiet --with 'psycopg[binary]' python - "$table" "$TARGET_NEN" "$TARGET_TSUKIHI" <<'PY'
import os
import sys

import psycopg

table, nen, tsukihi = sys.argv[1], sys.argv[2], sys.argv[3]
dsn = os.environ["NEON_DATABASE_URL"]
sql = (
    "SELECT COUNT(*) FROM ("
    "  SELECT DISTINCT kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango"
    f"  FROM {table}"
    "  WHERE kaisai_nen = %s AND kaisai_tsukihi = %s"
    ") AS races"
)
with psycopg.connect(dsn) as conn, conn.cursor() as cur:
    cur.execute(sql, (nen, tsukihi))
    row = cur.fetchone()
    print(row[0] if row else 0)
PY
}

# Read REALTIME_ADMIN_TOKEN once (only needed for the running-style kick).
read_admin_token() {
  if [ ! -f "$DEV_VARS_FILE" ]; then
    log "ERROR: $DEV_VARS_FILE not found — cannot kick running-style worker"
    return 1
  fi
  local line
  line="$(grep -E '^REALTIME_ADMIN_TOKEN=' "$DEV_VARS_FILE" | head -1 || true)"
  if [ -z "$line" ]; then
    log "ERROR: REALTIME_ADMIN_TOKEN not set in $DEV_VARS_FILE"
    return 1
  fi
  local token="${line#REALTIME_ADMIN_TOKEN=}"
  token="${token%\"}"
  token="${token#\"}"
  token="${token%\'}"
  token="${token#\'}"
  printf '%s' "$token"
}

# Mask token for logs: show only first 4 + last 4 chars.
mask_token() {
  local t="$1"
  local n="${#t}"
  if [ "$n" -le 8 ]; then
    printf '****'
    return
  fi
  printf '%s...%s' "${t:0:4}" "${t: -4}"
}

# --- running-style guard ---
log "checking running-style coverage in Neon ($RS_TABLE) for nen=$TARGET_NEN tsukihi=$TARGET_TSUKIHI ..."
RS_ACTUAL="$(neon_count "$RS_TABLE" || true)"
if ! printf '%s' "$RS_ACTUAL" | grep -Eq '^[0-9]+$'; then
  log "ERROR: failed to parse running-style count from Neon (got: $RS_ACTUAL)"
  exit 1
fi
log "running-style: actual=$RS_ACTUAL expected=$EXPECTED_COUNT"
if [ "$RS_ACTUAL" -lt "$EXPECTED_COUNT" ]; then
  log "running-style INCOMPLETE — kicking $RS_KICK_JOB_TYPE for date=$TARGET_DATE"
  if [ "${DRY_RUN:-0}" = "1" ]; then
    log "DRY_RUN: would POST $RS_KICK_URL body={\"type\":\"$RS_KICK_JOB_TYPE\",\"date\":\"$TARGET_DATE\"}"
  else
    RS_TOKEN="$(read_admin_token)" || exit 1
    log "POST $RS_KICK_URL token=$(mask_token "$RS_TOKEN")"
    HTTP_CODE="$(curl -fsS -o /tmp/rs-kick-response.json -w '%{http_code}' \
      -X POST "$RS_KICK_URL" \
      -H "Authorization: Bearer $RS_TOKEN" \
      -H 'Content-Type: application/json' \
      -d "{\"type\":\"$RS_KICK_JOB_TYPE\",\"date\":\"$TARGET_DATE\"}" 2>&1 || true)"
    log "running-style kick HTTP=$HTTP_CODE response=$(cat /tmp/rs-kick-response.json 2>/dev/null || echo '<no response>')"
  fi
else
  log "running-style COMPLETE — skip kick"
fi

# --- finish-position guard ---
log "checking finish-position coverage in Neon ($FP_TABLE) for nen=$TARGET_NEN tsukihi=$TARGET_TSUKIHI ..."
FP_ACTUAL="$(neon_count "$FP_TABLE" || true)"
if ! printf '%s' "$FP_ACTUAL" | grep -Eq '^[0-9]+$'; then
  log "ERROR: failed to parse finish-position count from Neon (got: $FP_ACTUAL)"
  exit 1
fi
log "finish-position: actual=$FP_ACTUAL expected=$EXPECTED_COUNT"
if [ "$FP_ACTUAL" -lt "$EXPECTED_COUNT" ]; then
  log "finish-position INCOMPLETE — preparing kick (RUN_DATE=$TARGET_DATE PREDICT_DAYS_AHEAD=$TARGET_DAYS_AHEAD)"
  if [ -d "$FINISH_LOCK_DIR" ]; then
    log "finish-position-predict lock $FINISH_LOCK_DIR held — another run in progress, skip kick"
  elif [ "${DRY_RUN:-0}" = "1" ]; then
    log "DRY_RUN: would exec RUN_DATE=$TARGET_DATE PREDICT_DAYS_AHEAD=$TARGET_DAYS_AHEAD RUN_DATE_MODE=auto bash $FINISH_SCRIPT"
  else
    log "exec RUN_DATE=$TARGET_DATE PREDICT_DAYS_AHEAD=$TARGET_DAYS_AHEAD bash $FINISH_SCRIPT"
    RUN_DATE="$TARGET_DATE" PREDICT_DAYS_AHEAD="$TARGET_DAYS_AHEAD" RUN_DATE_MODE=auto \
      bash "$FINISH_SCRIPT" || log "finish-position-predict-daily.sh exited non-zero (continuing)"
  fi
else
  log "finish-position COMPLETE — skip kick"
fi

log "guard done (TARGET_DATE=$TARGET_DATE expected=$EXPECTED_COUNT rs=$RS_ACTUAL fp=$FP_ACTUAL)"
exit 0
