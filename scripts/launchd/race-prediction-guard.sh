#!/usr/bin/env bash
# Hourly race-prediction completeness guard.
#
# Driven by the LaunchAgent at scripts/launchd/com.kkk4oru.race-prediction-guard.plist:
#   * JST 00:00 .. 09:00 (10 hourly fires) -> guard TODAY    JST
#   * JST 19:00 .. 20:00 (2  hourly fires) -> guard TODAY    JST (evening top-up)
#   * JST 21:00 .. 23:00 (3  hourly fires) -> guard TOMORROW JST + TODAY JST
# Total 15 fires/day. Re-runs are idempotent: Neon prediction tables are the
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
# When D1 has ZERO races for a target date, the guard kicks the worker
# `discover-urls` job (UPSERT-idempotent on the worker side) so the next
# hourly tick has rows to plan against. No prediction kick fires that hour.
#
# Locks:
#   /tmp/race-prediction-guard.lock  -- guard-level (single concurrent guard)
#   /tmp/finish-position-predict.lock -- shared with finish-position-predict-daily.sh
#                                        so JST 03:00 cron and the hourly kick
#                                        cannot race the same docker run.
#
# Manual / dry-run:
#   DRY_RUN=1 bash scripts/launchd/race-prediction-guard.sh
#   DRY_RUN=1 FORCE_HOUR=05 bash scripts/launchd/race-prediction-guard.sh     # today
#   DRY_RUN=1 FORCE_HOUR=19 bash scripts/launchd/race-prediction-guard.sh     # today (evening)
#   DRY_RUN=1 FORCE_HOUR=22 bash scripts/launchd/race-prediction-guard.sh     # today + tomorrow
#   DRY_RUN=1 FORCE_HOUR=22 FORCE_TARGET_DATE=20300101 bash ...               # exercise discover-urls path
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
JOBS_KICK_URL="https://sync-realtime-data.kkk4oru.com/api/jobs"
RS_KICK_JOB_TYPE="plan-running-style-predictions"
DISCOVER_JOB_TYPE="discover-urls"
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

TODAY_ISO="${TODAY_DATE:0:4}-${TODAY_DATE:4:2}-${TODAY_DATE:6:2}"
TOMORROW_DATE="$(date -u -v+9H -v+1d +%Y%m%d)"
TOMORROW_ISO="${TOMORROW_DATE:0:4}-${TOMORROW_DATE:4:2}-${TOMORROW_DATE:6:2}"

# Read NEON_DATABASE_URL once (used for all per-target Neon counts).
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

# Query Neon for COUNT(DISTINCT (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango))
# in the named table for the given (nen, tsukihi). Uses uv with a transient
# psycopg dep so we don't depend on a system psql.
neon_count() {
  local table="$1"
  local nen="$2"
  local tsukihi="$3"
  uv run --quiet --with 'psycopg[binary]' python - "$table" "$nen" "$tsukihi" <<'PY'
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

# Read REALTIME_ADMIN_TOKEN once (only needed for worker kicks).
read_admin_token() {
  if [ ! -f "$DEV_VARS_FILE" ]; then
    log "ERROR: $DEV_VARS_FILE not found — cannot kick worker"
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

# Kick a worker job via POST /api/jobs with the given JSON body.
# Args: $1 = log description, $2 = JSON body.
kick_worker_job() {
  local description="$1"
  local body="$2"
  local token
  token="$(read_admin_token)" || return 1
  log "POST $JOBS_KICK_URL ($description) token=$(mask_token "$token")"
  local safe_description
  safe_description="$(printf '%s' "$description" | tr -c 'A-Za-z0-9._-' '_')"
  local response_file="/tmp/${safe_description}-kick-response.json"
  local http_code
  http_code="$(curl -fsS -o "$response_file" -w '%{http_code}' \
    -X POST "$JOBS_KICK_URL" \
    -H "Authorization: Bearer $token" \
    -H 'Content-Type: application/json' \
    -d "$body" 2>&1 || true)"
  log "$description kick HTTP=$http_code response=$(cat "$response_file" 2>/dev/null || echo '<no response>')"
}

# Per-target guard. Args:
#   $1 target_date_yyyymmdd  e.g. 20260609
#   $2 target_date_iso       e.g. 2026-06-09
#   $3 days_ahead            0 (today) or 1 (tomorrow)
#   $4 label                 "today" or "tomorrow" — for log messages
guard_target() {
  local target_date="$1"
  local target_date_iso="$2"
  local days_ahead="$3"
  local label="$4"

  local target_nen="${target_date:0:4}"
  local target_tsukihi="${target_date:4:4}"

  log "=== guard_target label=$label target=$target_date_iso (days_ahead=$days_ahead) ==="

  # Query D1 for the expected race count for target_date_iso.
  log "querying D1 expected race_key count for $target_date_iso ..."
  local d1_query="SELECT COUNT(DISTINCT race_key) AS c FROM realtime_race_sources WHERE substr(race_start_at_jst, 1, 10) = '${target_date_iso}';"
  local d1_result
  d1_result="$(bunx wrangler d1 execute "$D1_BINDING_NAME" --remote --config "$WRANGLER_CONFIG" --command "$d1_query" --json 2>&1 || true)"
  local expected_count
  expected_count="$(printf '%s' "$d1_result" | jq -r '.[0].results[0].c // empty' 2>/dev/null || true)"
  if [ -z "$expected_count" ] || [ "$expected_count" = "null" ]; then
    log "ERROR: failed to parse expected race count for $label from D1 (result tail: $(printf '%s' "$d1_result" | tail -c 400))"
    return 1
  fi
  log "EXPECTED_COUNT[$label]=$expected_count (distinct race_key in realtime_race_sources)"

  if [ "$expected_count" = "0" ]; then
    log "no D1 races for $label ($target_date_iso) — kicking $DISCOVER_JOB_TYPE for date=$target_date"
    if [ "${DRY_RUN:-0}" = "1" ]; then
      log "DRY_RUN: would POST $JOBS_KICK_URL body={\"type\":\"$DISCOVER_JOB_TYPE\",\"date\":\"$target_date\"}"
    else
      kick_worker_job "discover-urls-$label" "{\"type\":\"$DISCOVER_JOB_TYPE\",\"date\":\"$target_date\"}"
    fi
    log "$label: discover-urls kicked — predictions will be evaluated on next hourly tick"
    return 0
  fi

  # --- running-style guard ---
  log "checking running-style coverage in Neon ($RS_TABLE) for nen=$target_nen tsukihi=$target_tsukihi ($label) ..."
  local rs_actual
  rs_actual="$(neon_count "$RS_TABLE" "$target_nen" "$target_tsukihi" || true)"
  if ! printf '%s' "$rs_actual" | grep -Eq '^[0-9]+$'; then
    log "ERROR: failed to parse running-style count for $label from Neon (got: $rs_actual)"
    return 1
  fi
  log "running-style[$label]: actual=$rs_actual expected=$expected_count"
  if [ "$rs_actual" -lt "$expected_count" ]; then
    log "running-style[$label] INCOMPLETE — kicking $RS_KICK_JOB_TYPE for date=$target_date"
    if [ "${DRY_RUN:-0}" = "1" ]; then
      log "DRY_RUN: would POST $JOBS_KICK_URL body={\"type\":\"$RS_KICK_JOB_TYPE\",\"date\":\"$target_date\"}"
    else
      kick_worker_job "running-style-$label" "{\"type\":\"$RS_KICK_JOB_TYPE\",\"date\":\"$target_date\"}"
    fi
  else
    log "running-style[$label] COMPLETE — skip kick"
  fi

  # --- finish-position guard ---
  log "checking finish-position coverage in Neon ($FP_TABLE) for nen=$target_nen tsukihi=$target_tsukihi ($label) ..."
  local fp_actual
  fp_actual="$(neon_count "$FP_TABLE" "$target_nen" "$target_tsukihi" || true)"
  if ! printf '%s' "$fp_actual" | grep -Eq '^[0-9]+$'; then
    log "ERROR: failed to parse finish-position count for $label from Neon (got: $fp_actual)"
    return 1
  fi
  log "finish-position[$label]: actual=$fp_actual expected=$expected_count"
  if [ "$fp_actual" -lt "$expected_count" ]; then
    log "finish-position[$label] INCOMPLETE — preparing kick (RUN_DATE=$target_date PREDICT_DAYS_AHEAD=$days_ahead)"
    if [ -d "$FINISH_LOCK_DIR" ]; then
      log "finish-position-predict lock $FINISH_LOCK_DIR held — another run in progress, skip kick"
    elif [ "${DRY_RUN:-0}" = "1" ]; then
      log "DRY_RUN: would exec RUN_DATE=$target_date PREDICT_DAYS_AHEAD=$days_ahead RUN_DATE_MODE=auto bash $FINISH_SCRIPT"
    else
      log "exec RUN_DATE=$target_date PREDICT_DAYS_AHEAD=$days_ahead bash $FINISH_SCRIPT"
      RUN_DATE="$target_date" PREDICT_DAYS_AHEAD="$days_ahead" RUN_DATE_MODE=auto \
        bash "$FINISH_SCRIPT" || log "finish-position-predict-daily.sh exited non-zero (continuing)"
    fi
  else
    log "finish-position[$label] COMPLETE — skip kick"
  fi

  log "guard_target done (label=$label target=$target_date_iso expected=$expected_count rs=$rs_actual fp=$fp_actual)"
}

# DRY_RUN-only override so the dry-run can be aimed at a date with known
# expected/actual values for verification. Ignored when DRY_RUN!=1.
# Replaces BOTH today and tomorrow targets with FORCE_TARGET_DATE / +1 so
# the hours 21-23 path can be exercised end-to-end against a deterministic
# date (e.g. 20300101 to exercise the empty-D1 discover-urls kick path).
if [ "${DRY_RUN:-0}" = "1" ] && [ -n "${FORCE_TARGET_DATE:-}" ]; then
  TODAY_DATE="$FORCE_TARGET_DATE"
  TODAY_ISO="${TODAY_DATE:0:4}-${TODAY_DATE:4:2}-${TODAY_DATE:6:2}"
  # Compute FORCE_TARGET_DATE + 1 day in pure-bash via date -j -f.
  TOMORROW_DATE="$(date -j -v+1d -f '%Y%m%d' "$TODAY_DATE" '+%Y%m%d')"
  TOMORROW_ISO="${TOMORROW_DATE:0:4}-${TOMORROW_DATE:4:2}-${TOMORROW_DATE:6:2}"
  log "DRY_RUN with FORCE_TARGET_DATE=$TODAY_DATE override (today=$TODAY_ISO tomorrow=$TOMORROW_ISO)"
fi

# Per-hour dispatch.
#   0-9   -> TODAY only
#   19-20 -> TODAY only (evening top-up)
#   21-23 -> TODAY + TOMORROW (pre-warm)
#   else  -> exit (not a scheduled window)
case "$JST_HOUR" in
  0[0-9])
    log "window=today JST_HOUR=$JST_HOUR (0-9 morning band)"
    guard_target "$TODAY_DATE" "$TODAY_ISO" 0 "today"
    ;;
  19|20)
    log "window=today JST_HOUR=$JST_HOUR (19-20 evening top-up band)"
    guard_target "$TODAY_DATE" "$TODAY_ISO" 0 "today"
    ;;
  21|22|23)
    log "window=today+tomorrow JST_HOUR=$JST_HOUR (21-23 pre-warm band)"
    guard_target "$TODAY_DATE" "$TODAY_ISO" 0 "today"
    guard_target "$TOMORROW_DATE" "$TOMORROW_ISO" 1 "tomorrow"
    ;;
  *)
    log "outside guard window (JST_HOUR=$JST_HOUR) — exit 0"
    exit 0
    ;;
esac

log "guard fully done (JST_HOUR=$JST_HOUR)"
exit 0
