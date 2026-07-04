#!/usr/bin/env bash
# Local/manual race-prediction diagnostic guard.
#
# Production feature generation, running-style prediction, and finish-position
# prediction are Cloudflare-side. This script is retained for local diagnostics
# around Cloudflare D1/Worker job state and optional manual backfills. It must
# not be treated as production scheduling or ordering authority.
#
# Optional local checks:
#   * running-style (脚質): Cloudflare Worker job (POST /api/jobs).
#   * finish-position (着順): skipped by default because Cloudflare
#     finish-position-cron owns production per-race prediction. Set
#     FINISH_POSITION_OFFLOADED_TO_CF=0 only for an explicit local Docker
#     backfill via scripts/launchd/finish-position-predict-daily.sh.
#
# CF-offload coverage-miss fallback order (2026-07-04, simplified same-day
# per team-lead confirmation of the trigger contract): when this guard
# detects finish-position coverage is INCOMPLETE during race hours while
# offloaded, it escalates cheapest/fastest first:
#   1. Cloudflare trigger: POST finish-position-cron's /run endpoint
#      (per-category, only for the categories that are actually short) —
#      this is the same production trigger surface sync-realtime-data uses,
#      just invoked from the Mac instead of from the running-style completion
#      event. Requires FINISH_POSITION_CRON_TRIGGER_TOKEN readable from
#      repo-root .env (Bearer auth; verified: bad token -> 401, valid token +
#      bad body -> 400, valid request -> 202 {"ok":true,"queued":N}).
#      Logs "cf-trigger-sent" and writes a small marker file
#      (/tmp/race-prediction-guard-cf-trigger/) recording that this date was
#      already CF-triggered, then returns — NO in-tick wait/verify: the next
#      ~20-min guard tick's own coverage check is the verifier.
#   2. Local docker (finish-position-predict-daily.sh): LAST resort only,
#      logged as "cf-trigger-failed->local" or "cf-already-tried->local" —
#      TRIGGER_TOKEN unavailable, the CF trigger POST itself failed (any
#      category non-2xx), or a PREVIOUS tick already sent a CF trigger for
#      this date (marker present) and coverage is STILL incomplete now.
#
# Reference data checked by this diagnostic:
#   * expected races: Cloudflare D1 sync-realtime-data.realtime_race_sources.
#   * prediction coverage: Neon race_running_style_model_predictions /
#     race_finish_position_model_predictions.
#
# Manual / dry-run examples:
#   DRY_RUN=1 bash scripts/launchd/race-prediction-guard.sh
#   DRY_RUN=1 FORCE_HOUR=05 bash scripts/launchd/race-prediction-guard.sh     # today
#   DRY_RUN=1 FORCE_HOUR=14 bash scripts/launchd/race-prediction-guard.sh     # race hours
#   DRY_RUN=1 FORCE_HOUR=19 bash scripts/launchd/race-prediction-guard.sh     # today (evening)
#   DRY_RUN=1 FORCE_HOUR=22 bash scripts/launchd/race-prediction-guard.sh     # today + tomorrow
#   DRY_RUN=1 FORCE_HOUR=22 FORCE_TARGET_DATE=20300101 bash ...               # exercise discover-urls path
#   DRY_RUN=1 FORCE_HOUR=05 FORCE_NO_CORNER_FEATURES=1 bash ...               # exercise corner-features build path
#   DRY_RUN=1 FORCE_HOUR=05 FORCE_VENUE_COUNTS=44:7,30:12 \
#     FORCE_EXPECTED_COUNT=42 FORCE_TARGET_DATE=20300101 bash ...             # exercise per-venue coverage check path
#   DRY_RUN=1 FORCE_HOUR=14 FORCE_EXPECTED_COUNT=12 FORCE_RS_ACTUAL=11 bash ... # exercise RS-before-FP order skip
#   DRY_RUN=1 FORCE_HOUR=14 FORCE_EXPECTED_COUNT=12 FORCE_RS_ACTUAL=12 FORCE_FP_ACTUAL=12 bash ... # exercise FP dry-run skip
#   DRY_RUN=1 FORCE_HOUR=14 FORCE_D1_FAIL=1 bash ...                         # exercise D1-unavailable path (race hours)
#   DRY_RUN=1 FORCE_HOUR=05 FORCE_D1_FAIL=1 bash ...                         # exercise D1-unavailable path (non-race hours)
#   DRY_RUN=1 FORCE_HOUR=14 FORCE_EXPECTED_COUNT=12 FORCE_RS_ACTUAL=12 FORCE_FP_ACTUAL=5 bash ...  # CF-offload coverage MISS, race hours -> tries CF trigger first
#   DRY_RUN=1 FORCE_HOUR=05 FORCE_EXPECTED_COUNT=12 FORCE_RS_ACTUAL=12 FORCE_FP_ACTUAL=5 bash ...  # CF-offload coverage MISS, non-race hours -> no fallback (next tick)
#   DRY_RUN=1 FORCE_HOUR=14 FORCE_EXPECTED_COUNT=12 FORCE_RS_ACTUAL=12 FORCE_FP_ACTUAL=12 bash ... # CF-offload coverage OK -> stays offloaded (no local kick)
#   DRY_RUN=1 FORCE_HOUR=14 FORCE_TARGET_DATE=20300101 FORCE_EXPECTED_COUNT=12 FORCE_RS_ACTUAL=12 FORCE_FP_ACTUAL=5 \
#     FORCE_FP_CATEGORY_EXPECTED=jra:5,nar:4,ban-ei:3 FORCE_FP_CATEGORY_ACTUAL=jra:5,nar:2,ban-ei:3 bash ...  # CF trigger sent for nar only (marker written, no local docker)
#   DRY_RUN=1 FORCE_HOUR=14 FORCE_TARGET_DATE=20300101 FORCE_EXPECTED_COUNT=12 FORCE_RS_ACTUAL=12 FORCE_FP_ACTUAL=5 \
#     FORCE_FP_CATEGORY_EXPECTED=jra:5,nar:4,ban-ei:3 FORCE_FP_CATEGORY_ACTUAL=jra:5,nar:2,ban-ei:3 FORCE_CF_TRIGGER_FAIL=1 bash ...  # CF trigger POST fails -> immediate local docker (last resort)
#   DRY_RUN=1 FORCE_HOUR=14 FORCE_TARGET_DATE=20300101 FORCE_EXPECTED_COUNT=12 FORCE_RS_ACTUAL=12 FORCE_FP_ACTUAL=5 FORCE_TRIGGER_TOKEN_MISSING=1 bash ...  # TRIGGER_TOKEN unavailable -> immediate local docker (last resort)
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
CORNER_FEATURES_TABLE="race_entry_corner_features"
JOBS_KICK_URL="https://sync-realtime-data.kkk4oru.com/api/jobs"
RS_KICK_JOB_TYPE="plan-running-style-predictions"
DISCOVER_JOB_TYPE="discover-urls"
FINISH_SCRIPT="$REPO_ROOT/scripts/launchd/finish-position-predict-daily.sh"
CORNER_FEATURES_BUILD_FILTER="pc-keiba-viewer"
CORNER_FEATURES_BUILD_SCRIPT="dev:build-corner-features"

# CF-offload coverage-miss fallback (see header). finish-position-cron has no
# custom domain route in wrangler.jsonc; this is its workers.dev URL
# (confirmed live via GET / health check returning {"cron":...,"ok":true}).
# The Worker-side secret is named TRIGGER_TOKEN; the repo-root .env mirrors it
# under FINISH_POSITION_CRON_TRIGGER_TOKEN for local scripts (same convention
# finish-position-predict-daily.sh already uses for R2 credentials in its
# pre-flight 10).
CF_TRIGGER_URL="https://finish-position-cron.kaoru.workers.dev/run"
ROOT_ENV_FILE="$REPO_ROOT/.env"
# Marker directory recording "a CF trigger was already sent for this date" so
# the NEXT guard tick (~20 min later during race hours) can tell "first miss,
# just triggered CF" apart from "already tried CF last tick, still incomplete
# -> escalate to local docker". No fixed wait/verify window — the next tick's
# own coverage re-check is the verifier, per team-lead's simplified design.
CF_TRIGGER_MARKER_DIR="/tmp/race-prediction-guard-cf-trigger"

# Per-venue coverage lower bounds. NAR major venues typically run 10-12 races
# per active day, JRA major venues typically run 12; if D1 shows fewer than
# these we treat the date as partially-discovered and re-kick discover-urls.
# Higher race counts are fine (some days have extra races).
# Today's incident: 大井 (44) ended up at 7 races because discover-urls D1
# write retries all failed — this guard would have caught that and re-kicked.
EXPECTED_NAR_RACES_PER_VENUE=10
EXPECTED_JRA_RACES_PER_VENUE=11

# Finish-position is Cloudflare-owned by default. Leave this at 1 for normal
# diagnostics so the local Docker runner cannot become production authority.
# Set FINISH_POSITION_OFFLOADED_TO_CF=0 only for an explicit manual backfill.
FINISH_POSITION_OFFLOADED_TO_CF="${FINISH_POSITION_OFFLOADED_TO_CF:-1}"

# NAR major venue keibajo_codes (門別/盛岡/水沢/浦和/船橋/大井/川崎/金沢/笠松/
# 名古屋/園田/姫路/高知/佐賀/帯広). Listed as a space-separated string so the
# Bash 3.2 shipped with macOS can iterate them without associative arrays.
NAR_MAJOR_VENUE_CODES="30 35 36 42 43 44 46 47 48 50 51 53 54 55 56 57 65 66"

# JRA major venue keibajo_codes (札幌/函館/福島/新潟/東京/中山/中京/京都/阪神/
# 小倉). All 10 official JRA tracks; not all run on a given day.
JRA_MAJOR_VENUE_CODES="01 02 03 04 05 06 07 08 09 10"

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

# Query Neon for COUNT(*) in race_entry_corner_features for the given
# (nen, tsukihi). Used to detect whether corner features for the target date
# have been built (a prerequisite for plan-running-style-predictions).
neon_corner_features_count() {
  local nen="$1"
  local tsukihi="$2"
  uv run --quiet --with 'psycopg[binary]' python - "$nen" "$tsukihi" <<'PY'
import os
import sys

import psycopg

nen, tsukihi = sys.argv[1], sys.argv[2]
dsn = os.environ["NEON_DATABASE_URL"]
sql = (
    "SELECT COUNT(*) FROM race_entry_corner_features"
    " WHERE kaisai_nen = %s AND kaisai_tsukihi = %s"
)
with psycopg.connect(dsn) as conn, conn.cursor() as cur:
    cur.execute(sql, (nen, tsukihi))
    row = cur.fetchone()
    print(row[0] if row else 0)
PY
}

# Check whether Neon race_entry_corner_features has rows for the target date.
# If not, run the build-corner-feature-table bun script for that single date
# across all sources (jra + nar + ban-ei). The bun script is idempotent —
# its INSERT uses `on conflict (source, kaisai_nen, kaisai_tsukihi,
# keibajo_code, race_bango, ketto_toroku_bango) do update set ...`, so a
# second run for the same date is a fast UPSERT, not a duplicate.
#
# Args:
#   $1 target_nen       e.g. 2026
#   $2 target_tsukihi   e.g. 0609
#   $3 target_date      e.g. 20260609
#   $4 label            "today" or "tomorrow"
# Returns:
#   0 — corner features present, or build succeeded (running-style may proceed)
#   1 — count parse failed or build failed (running-style kick must be skipped)
corner_features_check_and_build() {
  local target_nen="$1"
  local target_tsukihi="$2"
  local target_date="$3"
  local label="$4"

  log "corner-features check ($CORNER_FEATURES_TABLE) for nen=$target_nen tsukihi=$target_tsukihi ($label) ..."
  local cf_count
  if [ "${DRY_RUN:-0}" = "1" ] && [ "${FORCE_NO_CORNER_FEATURES:-0}" = "1" ]; then
    cf_count=0
    log "DRY_RUN: FORCE_NO_CORNER_FEATURES=1 — treating corner-features[$label] as 0 (skipping Neon query)"
  else
    cf_count="$(neon_corner_features_count "$target_nen" "$target_tsukihi" || true)"
    if ! printf '%s' "$cf_count" | grep -Eq '^[0-9]+$'; then
      log "ERROR: failed to parse corner-features count for $label from Neon (got: $cf_count)"
      return 1
    fi
    log "corner-features[$label]: actual=$cf_count"
  fi

  if [ "$cf_count" != "0" ]; then
    log "corner-features[$label] present — skip build"
    return 0
  fi

  log "corner-features[$label] absent — building for date=$target_date (source-scope=all)"
  if [ "${DRY_RUN:-0}" = "1" ]; then
    log "DRY_RUN: would exec DATABASE_URL_NEON=*** bun run --filter $CORNER_FEATURES_BUILD_FILTER $CORNER_FEATURES_BUILD_SCRIPT -- --target neon --source-scope all --from-date $target_date --to-date $target_date"
    log "corner-features build end (DRY_RUN — no rows written)"
    return 0
  fi

  log "corner-features build start (date=$target_date)"
  local build_status=0
  DATABASE_URL_NEON="$NEON_DATABASE_URL" \
    bun run --filter "$CORNER_FEATURES_BUILD_FILTER" "$CORNER_FEATURES_BUILD_SCRIPT" -- \
      --target neon --source-scope all --from-date "$target_date" --to-date "$target_date" \
    || build_status=$?
  if [ "$build_status" != "0" ]; then
    log "ERROR: corner-features build failed for $label (date=$target_date status=$build_status)"
    return 1
  fi
  log "corner-features build end (date=$target_date status=0)"
  return 0
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

# Read the finish-position-cron POST /run trigger token from the repo-root
# .env, key FINISH_POSITION_CRON_TRIGGER_TOKEN (confirmed the actual key name
# present in .env — NOT the bare "TRIGGER_TOKEN" the Worker-side secret is
# named; same file + convention finish-position-predict-daily.sh already uses
# for R2 credentials). Returns empty (not an error) when the file or key is
# missing — callers must treat empty as "CF trigger unavailable" and degrade
# to the local docker fallback rather than sending an empty/garbage
# Authorization header. DRY_RUN=1 FORCE_TRIGGER_TOKEN_MISSING=1 simulates a
# missing token without touching the real .env.
read_trigger_token() {
  if [ "${DRY_RUN:-0}" = "1" ] && [ "${FORCE_TRIGGER_TOKEN_MISSING:-0}" = "1" ]; then
    log "DRY_RUN: FORCE_TRIGGER_TOKEN_MISSING=1 — simulating missing TRIGGER_TOKEN" >&2
    printf ''
    return 0
  fi
  if [ ! -f "$ROOT_ENV_FILE" ]; then
    printf ''
    return 0
  fi
  local line
  line="$(grep -E '^FINISH_POSITION_CRON_TRIGGER_TOKEN=' "$ROOT_ENV_FILE" | head -1 || true)"
  if [ -z "$line" ]; then
    printf ''
    return 0
  fi
  local token="${line#FINISH_POSITION_CRON_TRIGGER_TOKEN=}"
  token="${token%\"}"
  token="${token#\"}"
  token="${token%\'}"
  token="${token#\'}"
  printf '%s' "$token"
}

# Path to the marker file that records when a CF trigger was last sent for a
# given (label, target_date), so the verify delay survives across separate
# guard invocations (each tick is a fresh process). One marker per label+date
# keeps today/tomorrow independent and never collides across dates.
cf_trigger_marker_path() {
  local label="$1"
  local target_date="$2"
  printf '%s/%s-%s.marker' "$CF_TRIGGER_MARKER_DIR" "$label" "$target_date"
}

# Query D1 for expected race counts broken down by category (jra / nar /
# ban-ei), using the SAME keibajo_code partition as the prediction container
# itself (apps/finish-position-predict-container/src/pipeline_runner.py):
# jra = keibajo_code 01-10, ban-ei = keibajo_code 83, nar = everything else.
# Emits one "<category> <count>" line per row to stdout; log lines go to
# stderr so they don't interleave with the data the caller captures via
# command substitution. When DRY_RUN=1 and FORCE_FP_CATEGORY_EXPECTED is set,
# the override is parsed instead of touching D1.
#
# Args: $1 target_date_iso   e.g. 2026-06-09
d1_expected_by_category() {
  local target_date_iso="$1"
  if [ "${DRY_RUN:-0}" = "1" ] && [ -n "${FORCE_FP_CATEGORY_EXPECTED:-}" ]; then
    log "DRY_RUN: FORCE_FP_CATEGORY_EXPECTED=$FORCE_FP_CATEGORY_EXPECTED override (skipping D1 query)" >&2
    printf '%s' "$FORCE_FP_CATEGORY_EXPECTED" | tr ',' '\n' | awk -F: 'NF==2 {print $1" "$2}'
    return 0
  fi
  local d1_query="SELECT CASE WHEN keibajo_code IN ('01','02','03','04','05','06','07','08','09','10') THEN 'jra' WHEN keibajo_code = '83' THEN 'ban-ei' ELSE 'nar' END AS category, COUNT(DISTINCT race_key) AS c FROM realtime_race_sources WHERE substr(race_start_at_jst, 1, 10) = '${target_date_iso}' GROUP BY category;"
  local d1_result
  d1_result="$(bunx wrangler d1 execute "$D1_BINDING_NAME" --remote --config "$WRANGLER_CONFIG" --command "$d1_query" --json 2>&1 || true)"
  printf '%s' "$d1_result" | jq -r '.[0].results[]? | "\(.category) \(.c)"' 2>/dev/null || true
}

# Query Neon for finish-position actual race counts broken down by category,
# using the identical keibajo_code partition as d1_expected_by_category. Emits
# one "<category> <count>" line per row. DRY_RUN=1 FORCE_FP_CATEGORY_ACTUAL
# overrides instead of touching Neon.
#
# Args: $1 table  $2 nen  $3 tsukihi
neon_count_by_category() {
  local table="$1"
  local nen="$2"
  local tsukihi="$3"
  if [ "${DRY_RUN:-0}" = "1" ] && [ -n "${FORCE_FP_CATEGORY_ACTUAL:-}" ]; then
    log "DRY_RUN: FORCE_FP_CATEGORY_ACTUAL=$FORCE_FP_CATEGORY_ACTUAL override (skipping Neon per-category query)" >&2
    printf '%s' "$FORCE_FP_CATEGORY_ACTUAL" | tr ',' '\n' | awk -F: 'NF==2 {print $1" "$2}'
    return 0
  fi
  uv run --quiet --with 'psycopg[binary]' python - "$table" "$nen" "$tsukihi" <<'PY'
import os
import sys

import psycopg

table, nen, tsukihi = sys.argv[1], sys.argv[2], sys.argv[3]
dsn = os.environ["NEON_DATABASE_URL"]
sql = (
    "SELECT category, COUNT(*) FROM ("
    "  SELECT DISTINCT kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,"
    "    CASE WHEN keibajo_code IN ('01','02','03','04','05','06','07','08','09','10') THEN 'jra'"
    "         WHEN keibajo_code = '83' THEN 'ban-ei'"
    "         ELSE 'nar' END AS category"
    f"  FROM {table}"
    "  WHERE kaisai_nen = %s AND kaisai_tsukihi = %s"
    ") AS races"
    " GROUP BY category"
)
with psycopg.connect(dsn) as conn, conn.cursor() as cur:
    cur.execute(sql, (nen, tsukihi))
    for row in cur.fetchall():
        print(f"{row[0]} {row[1]}")
PY
}

# Determine which categories are below their D1-expected count in Neon
# FP_TABLE for the given date. Emits one incomplete category per line to
# stdout (e.g. "nar"); emits nothing when D1 returned no per-category
# breakdown or every category with a nonzero expected count is covered.
#
# Args: $1 target_date_iso  $2 target_nen  $3 target_tsukihi
fp_incomplete_categories() {
  local target_date_iso="$1"
  local target_nen="$2"
  local target_tsukihi="$3"
  local expected_rows
  expected_rows="$(d1_expected_by_category "$target_date_iso")"
  if [ -z "$expected_rows" ]; then
    return 0
  fi
  local actual_rows
  actual_rows="$(neon_count_by_category "$FP_TABLE" "$target_nen" "$target_tsukihi" || true)"
  local category expected_c actual_c
  while read -r category expected_c; do
    if [ -z "$category" ]; then
      continue
    fi
    if ! printf '%s' "$expected_c" | grep -Eq '^[0-9]+$'; then
      continue
    fi
    actual_c="$(printf '%s\n' "$actual_rows" | awk -v c="$category" '$1==c {print $2}')"
    if ! printf '%s' "$actual_c" | grep -Eq '^[0-9]+$'; then
      actual_c=0
    fi
    if [ "$actual_c" -lt "$expected_c" ]; then
      printf '%s\n' "$category"
    fi
  done <<< "$expected_rows"
}

# POST a per-category "full" trigger to finish-position-cron's /run endpoint
# for each category in the comma-separated $1 (e.g. "nar,ban-ei"). This is the
# SAME endpoint + body shape sync-realtime-data uses after running-style
# completes (src/running-style-queue.ts triggerFinishPositionFullRun), just a
# whole-category message (no keibajoCode/raceBango) instead of a single-race
# focused message — per-race targeting would need an extra D1-vs-Neon race-
# list diff this guard does not compute today; per-category is the documented
# fallback tier when that diff isn't available.
#
# Returns 0 only when every POST succeeds (HTTP 2xx). Under DRY_RUN the real
# HTTP call is never made (same convention as kick_worker_job) —
# FORCE_CF_TRIGGER_FAIL=1 simulates every category failing so the local-
# docker-fallback branch can be exercised offline.
#
# Args: $1 comma-separated categories  $2 target_date (YYYYMMDD)  $3 token
cf_trigger_categories() {
  local categories_csv="$1"
  local target_date="$2"
  local token="$3"
  local all_ok=1
  local category
  while read -r category; do
    if [ -z "$category" ]; then
      continue
    fi
    local body="{\"runDate\":\"${target_date}\",\"category\":\"${category}\",\"mode\":\"full\",\"skipDedup\":true}"
    if [ "${DRY_RUN:-0}" = "1" ]; then
      if [ "${FORCE_CF_TRIGGER_FAIL:-0}" = "1" ]; then
        log "DRY_RUN: FORCE_CF_TRIGGER_FAIL=1 — simulating CF trigger POST failure for category=$category"
        all_ok=0
      else
        log "DRY_RUN: would POST $CF_TRIGGER_URL body=$body"
      fi
      continue
    fi
    local response_file="/tmp/cf-trigger-${category}-response.json"
    local http_code
    http_code="$(curl -fsS -o "$response_file" -w '%{http_code}' \
      -X POST "$CF_TRIGGER_URL" \
      -H "Authorization: Bearer $token" \
      -H 'Content-Type: application/json' \
      -d "$body" 2>&1 || true)"
    log "CF trigger category=$category HTTP=$http_code response=$(cat "$response_file" 2>/dev/null || echo '<no response>')"
    case "$http_code" in
      2??) ;;
      *) all_ok=0 ;;
    esac
  # printf '%s\n' (not '%s') guarantees a trailing newline so `read` doesn't
  # hit EOF-without-newline and silently skip the last category — process
  # substitution, unlike a here-string (<<<), does not auto-terminate.
  done < <(printf '%s\n' "$categories_csv" | tr ',' '\n')
  [ "$all_ok" = "1" ]
}

# Query D1 for per-venue race counts on the target date, emitting one
# `<keibajo_code> <count>` line per row to stdout. Returns 0 on success even
# when the result set is empty; the caller decides what to do with the rows.
# When DRY_RUN=1 and FORCE_VENUE_COUNTS is set, the override is parsed instead
# of touching D1 so the partial-coverage path can be exercised offline.
#
# Args:
#   $1 target_date_iso   e.g. 2026-06-09
query_d1_venue_counts() {
  local target_date_iso="$1"
  if [ "${DRY_RUN:-0}" = "1" ] && [ -n "${FORCE_VENUE_COUNTS:-}" ]; then
    # Log to stderr so it doesn't interleave with stdout rows the caller reads.
    log "DRY_RUN: FORCE_VENUE_COUNTS=$FORCE_VENUE_COUNTS override (target=$target_date_iso, skipping D1 query)" >&2
    printf '%s' "$FORCE_VENUE_COUNTS" | tr ',' '\n' | awk -F: 'NF==2 {print $1" "$2}'
    return 0
  fi
  local d1_query="SELECT keibajo_code, COUNT(*) AS c FROM realtime_race_sources WHERE substr(race_start_at_jst, 1, 10) = '${target_date_iso}' GROUP BY keibajo_code;"
  local d1_result
  d1_result="$(bunx wrangler d1 execute "$D1_BINDING_NAME" --remote --config "$WRANGLER_CONFIG" --command "$d1_query" --json 2>&1 || true)"
  printf '%s' "$d1_result" | jq -r '.[0].results[]? | "\(.keibajo_code) \(.c)"' 2>/dev/null || true
}

# Decide whether a (keibajo_code, count) pair is under the per-venue
# expected lower bound. Returns 0 (true) when the venue is "suspicious",
# 1 (false) when at-or-above the bound or the code is not in either major
# venue list (so we don't kick on minor / unscheduled venues).
#
# Args:
#   $1 keibajo_code   e.g. 44
#   $2 count          e.g. 7
is_venue_under_threshold() {
  local keibajo_code="$1"
  local count="$2"
  if [[ " $NAR_MAJOR_VENUE_CODES " == *" $keibajo_code "* ]]; then
    [ "$count" -lt "$EXPECTED_NAR_RACES_PER_VENUE" ] && return 0
    return 1
  fi
  if [[ " $JRA_MAJOR_VENUE_CODES " == *" $keibajo_code "* ]]; then
    [ "$count" -lt "$EXPECTED_JRA_RACES_PER_VENUE" ] && return 0
    return 1
  fi
  # Unknown / non-major venue: not under threshold (do not kick on these).
  return 1
}

# Per-venue coverage check. Compares each major-venue's D1 race count for the
# target date against the configured lower bound. If ANY major venue is under,
# log a structured WARN line and re-kick discover-urls.
#
# Today's incident was the canonical failure mode this catches: NAR 大井 (44)
# had only 7 races in D1 when it should have had 12, because the discover-urls
# cron + retries all failed with D1_ERROR / Idle connection closed and gave up.
#
# Args:
#   $1 target_date         e.g. 20260609
#   $2 target_date_iso     e.g. 2026-06-09
#   $3 label               "today" or "tomorrow"
# Returns:
#   0 — check passed OR re-kick was logged (caller proceeds either way)
check_venue_coverage() {
  local target_date="$1"
  local target_date_iso="$2"
  local label="$3"

  log "checking per-venue coverage in D1 for $target_date_iso ($label) ..."
  local rows
  rows="$(query_d1_venue_counts "$target_date_iso")"
  if [ -z "$rows" ]; then
    log "per-venue check[$label]: no rows returned from D1 (target=$target_date_iso) — skip"
    return 0
  fi

  local under_venues=""
  while read -r keibajo_code count; do
    if [ -z "$keibajo_code" ] || [ -z "$count" ]; then
      continue
    fi
    log "per-venue[$label] keibajo_code=$keibajo_code count=$count"
    if is_venue_under_threshold "$keibajo_code" "$count"; then
      under_venues="$under_venues $keibajo_code=$count"
    fi
  done <<< "$rows"

  if [ -z "$under_venues" ]; then
    log "per-venue coverage[$label] OK — all listed major venues meet thresholds"
    return 0
  fi

  log "WARN per-venue coverage[$label] INCOMPLETE — under-threshold venues:$under_venues"
  log "re-kicking $DISCOVER_JOB_TYPE for date=$target_date (per-venue partial-coverage recovery)"
  if [ "${DRY_RUN:-0}" = "1" ]; then
    log "DRY_RUN: would POST $JOBS_KICK_URL body={\"type\":\"$DISCOVER_JOB_TYPE\",\"date\":\"$target_date\"}"
  else
    kick_worker_job "discover-urls-coverage-$label" "{\"type\":\"$DISCOVER_JOB_TYPE\",\"date\":\"$target_date\"}"
  fi
  return 0
}

# Per-target guard. Args:
#   $1 target_date_yyyymmdd  e.g. 20260609
#   $2 target_date_iso       e.g. 2026-06-09
#   $3 days_ahead            0 (today) or 1 (tomorrow)
#   $4 label                 "today" or "tomorrow" — for log messages
#   $5 jst_hour              current JST hour (00..23) — controls freshness mode
guard_target() {
  local target_date="$1"
  local target_date_iso="$2"
  local days_ahead="$3"
  local label="$4"
  local jst_hour="$5"

  local target_nen="${target_date:0:4}"
  local target_tsukihi="${target_date:4:4}"

  log "=== guard_target label=$label target=$target_date_iso (days_ahead=$days_ahead) ==="

  # --- is_race_hours: calculated FIRST, before any D1 query ---
  # "race hours" = JST hour in [10, 20] inclusive.
  # Must be known before the D1 query so that D1 failure handling can branch
  # on whether we are in a freshness-critical window.
  local is_race_hours=0
  if [ "$jst_hour" -ge 10 ] && [ "$jst_hour" -le 20 ]; then
    is_race_hours=1
  fi

  # Query D1 for the expected race count for target_date_iso.
  # D1 is queried via bunx wrangler which requires CLOUDFLARE_API_TOKEN in a
  # non-interactive (launchd) environment. If the query fails (token missing /
  # refresh error / network) we enter the d1_unavailable path.
  log "querying D1 expected race_key count for $target_date_iso ..."
  local d1_query="SELECT COUNT(DISTINCT race_key) AS c FROM realtime_race_sources WHERE substr(race_start_at_jst, 1, 10) = '${target_date_iso}';"
  local d1_result
  # DRY_RUN-only hooks:
  #   FORCE_D1_FAIL=1 injects a fake error string so the D1-unavailable path
  #     can be exercised without touching real D1.
  #   FORCE_EXPECTED_COUNT=N bypasses D1 and supplies the parsed count so the
  #     downstream order gates can be exercised offline.
  if [ "${DRY_RUN:-0}" = "1" ] && [ "${FORCE_D1_FAIL:-0}" = "1" ]; then
    d1_result="In a non-interactive environment, it's necessary to set a CLOUDFLARE_API_TOKEN environment variable for wrangler to work"
    log "DRY_RUN: FORCE_D1_FAIL=1 — injecting fake D1 error response"
  elif [ "${DRY_RUN:-0}" = "1" ] && [ -n "${FORCE_EXPECTED_COUNT:-}" ]; then
    d1_result=""
    log "DRY_RUN: FORCE_EXPECTED_COUNT=$FORCE_EXPECTED_COUNT override (skipping D1 expected-count query)"
  else
    d1_result="$(bunx wrangler d1 execute "$D1_BINDING_NAME" --remote --config "$WRANGLER_CONFIG" --command "$d1_query" --json 2>&1 || true)"
  fi
  local expected_count
  if [ "${DRY_RUN:-0}" = "1" ] && [ -n "${FORCE_EXPECTED_COUNT:-}" ] && [ "${FORCE_D1_FAIL:-0}" != "1" ]; then
    expected_count="$FORCE_EXPECTED_COUNT"
  else
    expected_count="$(printf '%s' "$d1_result" | jq -r '.[0].results[0].c // empty' 2>/dev/null || true)"
  fi

  # --- D1 unavailability handling ---
  # If expected_count is empty/null (parse failure = D1 error), branch on
  # is_race_hours:
  #   is_race_hours=1: older behavior kicked finish-position for freshness, but
  #     that cannot prove running-style is complete. Order wins over freshness,
  #     so finish-position is skipped below when D1 is unavailable.
  #   is_race_hours=0: we cannot determine expected_count, so we cannot make
  #     safe progress on any check — abort as before.
  local d1_unavailable=0
  if [ -z "$expected_count" ] || [ "$expected_count" = "null" ]; then
    if [ "$is_race_hours" = "1" ]; then
      log "WARN: failed to parse expected race count for $label from D1 (result tail: $(printf '%s' "$d1_result" | tail -c 400))"
      log "WARN: D1 unavailable and is_race_hours=1 — skipping D1-dependent checks; finish-position will also be skipped to preserve feature -> running-style -> finish-position order"
      d1_unavailable=1
    else
      log "ERROR: failed to parse expected race count for $label from D1 (result tail: $(printf '%s' "$d1_result" | tail -c 400))"
      return 1
    fi
  fi

  if [ "$d1_unavailable" = "0" ]; then
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

    # --- per-venue coverage check (catches partial discover-urls failures) ---
    # Re-kicks discover-urls when any NAR/JRA major venue has fewer races than
    # the expected lower bound. Runs independently from running-style /
    # finish-position checks: even when the per-venue check re-kicks, we still
    # proceed with the downstream checks so any predictions we can compute now
    # still go through.
    check_venue_coverage "$target_date" "$target_date_iso" "$label"

    # --- corner-features prerequisite (needed before running-style) ---
    local corner_features_ok=1
    corner_features_check_and_build "$target_nen" "$target_tsukihi" "$target_date" "$label" \
      || corner_features_ok=0

    # --- running-style guard ---
    local rs_complete_for_finish=0
    local rs_kicked_this_tick=0
    if [ "$corner_features_ok" != "1" ]; then
      log "running-style[$label] SKIPPED — corner-features unavailable for $target_date"
    else
      log "checking running-style coverage in Neon ($RS_TABLE) for nen=$target_nen tsukihi=$target_tsukihi ($label) ..."
      local rs_actual
      if [ "${DRY_RUN:-0}" = "1" ] && [ -n "${FORCE_RS_ACTUAL:-}" ]; then
        rs_actual="$FORCE_RS_ACTUAL"
        log "DRY_RUN: FORCE_RS_ACTUAL=$FORCE_RS_ACTUAL override (skipping Neon running-style query)"
      else
        rs_actual="$(neon_count "$RS_TABLE" "$target_nen" "$target_tsukihi" || true)"
      fi
      if ! printf '%s' "$rs_actual" | grep -Eq '^[0-9]+$'; then
        log "ERROR: failed to parse running-style count for $label from Neon (got: $rs_actual)"
        return 1
      fi
      log "running-style[$label]: actual=$rs_actual expected=$expected_count"
      if [ "$rs_actual" -lt "$expected_count" ]; then
        log "running-style[$label] INCOMPLETE — kicking $RS_KICK_JOB_TYPE for date=$target_date"
        rs_kicked_this_tick=1
        if [ "${DRY_RUN:-0}" = "1" ]; then
          log "DRY_RUN: would POST $JOBS_KICK_URL body={\"type\":\"$RS_KICK_JOB_TYPE\",\"date\":\"$target_date\"}"
        else
          kick_worker_job "running-style-$label" "{\"type\":\"$RS_KICK_JOB_TYPE\",\"date\":\"$target_date\"}"
        fi
      else
        rs_complete_for_finish=1
        log "running-style[$label] COMPLETE — skip kick"
      fi
    fi
  else
    # d1_unavailable=1 (race hours only): D1-dependent checks skipped.
    local corner_features_ok=0
    local rs_complete_for_finish=0
    local rs_kicked_this_tick=0
    log "per-venue-coverage[$label] SKIPPED — D1 unavailable"
    log "corner-features[$label] SKIPPED — D1 unavailable"
    log "running-style[$label] SKIPPED — D1 unavailable"
  fi

  # --- finish-position guard ---
  #
  # Freshness-aware skip logic:
  #   During race hours (JST 10:00-20:00) bataiju (馬体重) for upcoming races
  #   is typically announced ~T-30..40 min before post, and odds continue to
  #   shift. We therefore re-kick the pipeline during race hours, even when
  #   fp_actual >= expected_count, after running-style completion has been
  #   confirmed, so predictions incorporate the latest bataiju/odds. The
  #   concurrent-run lock (FINISH_LOCK_DIR) is still checked — two docker runs
  #   never overlap.
  #
  #   Outside race hours (0-9, 21-23) the old "skip when complete" logic is
  #   preserved: no new race data is expected, so a re-run would be pure
  #   compute waste.
  #
  #   When D1 is unavailable (d1_unavailable=1), finish-position is skipped.
  #   The kick itself may not need D1, but this guard cannot prove the
  #   running-style prerequisite is complete without the expected race count.

  # CF cutover: when finish-position is offloaded to the Cloudflare Worker +
  # container per-race pipeline, the local docker kick is normally skipped —
  # this guard still ran every running-style / discover-urls / coverage duty
  # above. Fully reversible via the env var.
  #
  # Coverage fallback (2026-07-04): trusting the CF pipeline blindly let a
  # gated/pilot CF cron produce zero finish-position predictions for a full
  # day undetected. We now verify actual coverage in Neon (the same check the
  # non-offloaded branch below performs) before staying offloaded. Only when
  # coverage is INCOMPLETE and we are in race hours do we fall back to the
  # local docker kick; otherwise the previous offloaded-trust behavior holds.
  if [ "$FINISH_POSITION_OFFLOADED_TO_CF" = "1" ]; then
    if [ "$d1_unavailable" = "1" ]; then
      log "finish-position[$label] OFFLOADED to Cloudflare (FINISH_POSITION_OFFLOADED_TO_CF=1) — D1 unavailable, cannot verify CF coverage; keeping offloaded"
      log "guard_target done (label=$label target=$target_date_iso expected=D1_UNAVAILABLE rs=skipped fp=offloaded cf_ok=$corner_features_ok is_race_hours=$is_race_hours d1_unavailable=$d1_unavailable offload_fallback=0)"
      return 0
    fi

    if [ "$rs_complete_for_finish" != "1" ]; then
      log "finish-position[$label] OFFLOADED to Cloudflare — running-style not confirmed complete for expected=$expected_count, cannot verify/fallback; keeping offloaded"
      log "guard_target done (label=$label target=$target_date_iso expected=$expected_count rs=${rs_actual:-skipped} fp=offloaded cf_ok=$corner_features_ok is_race_hours=$is_race_hours d1_unavailable=$d1_unavailable offload_fallback=0)"
      return 0
    fi

    log "finish-position[$label] OFFLOADED to Cloudflare — verifying CF prediction coverage in Neon ($FP_TABLE) for nen=$target_nen tsukihi=$target_tsukihi ($label) ..."
    local fp_actual
    if [ "${DRY_RUN:-0}" = "1" ] && [ -n "${FORCE_FP_ACTUAL:-}" ]; then
      fp_actual="$FORCE_FP_ACTUAL"
      log "DRY_RUN: FORCE_FP_ACTUAL=$FORCE_FP_ACTUAL override (skipping Neon finish-position query)"
    else
      fp_actual="$(neon_count "$FP_TABLE" "$target_nen" "$target_tsukihi" || true)"
    fi
    if ! printf '%s' "$fp_actual" | grep -Eq '^[0-9]+$'; then
      log "ERROR: failed to parse finish-position count for $label from Neon (got: $fp_actual) — cannot verify CF coverage; keeping offloaded"
      log "guard_target done (label=$label target=$target_date_iso expected=$expected_count rs=${rs_actual:-skipped} fp=unknown cf_ok=$corner_features_ok is_race_hours=$is_race_hours d1_unavailable=$d1_unavailable offload_fallback=0)"
      return 0
    fi
    log "finish-position[$label] CF coverage check: actual=$fp_actual expected=$expected_count"

    if [ "$fp_actual" -ge "$expected_count" ]; then
      rm -f "$(cf_trigger_marker_path "$label" "$target_date")"
      log "finish-position[$label] OFFLOADED to Cloudflare — coverage OK, skip local docker kick"
      log "guard_target done (label=$label target=$target_date_iso expected=$expected_count rs=${rs_actual:-skipped} fp=$fp_actual cf_ok=$corner_features_ok is_race_hours=$is_race_hours d1_unavailable=$d1_unavailable offload_fallback=0)"
      return 0
    fi

    if [ "$is_race_hours" != "1" ]; then
      log "finish-position[$label] OFFLOADED to Cloudflare — coverage INCOMPLETE (actual=$fp_actual expected=$expected_count) but outside race hours — no local fallback, will re-check next tick"
      log "guard_target done (label=$label target=$target_date_iso expected=$expected_count rs=${rs_actual:-skipped} fp=$fp_actual cf_ok=$corner_features_ok is_race_hours=$is_race_hours d1_unavailable=$d1_unavailable offload_fallback=0)"
      return 0
    fi

    log "finish-position[$label] CF-offload coverage miss (actual=$fp_actual expected=$expected_count) — attempting Cloudflare trigger before local docker (see header fallback order)"

    local marker_path
    marker_path="$(cf_trigger_marker_path "$label" "$target_date")"
    local cf_trigger_status="not_attempted"
    if [ -f "$marker_path" ]; then
      local marker_epoch now_epoch elapsed
      marker_epoch="$(cat "$marker_path" 2>/dev/null || echo 0)"
      now_epoch="$(date -u +%s)"
      elapsed=$(( now_epoch - marker_epoch ))
      if [ "$elapsed" -lt "$CF_TRIGGER_VERIFY_DELAY_SECONDS" ]; then
        log "finish-position[$label] CF trigger already sent ${elapsed}s ago (< ${CF_TRIGGER_VERIFY_DELAY_SECONDS}s verify delay) — waiting, no action this tick"
        log "guard_target done (label=$label target=$target_date_iso expected=$expected_count rs=${rs_actual:-skipped} fp=$fp_actual cf_ok=$corner_features_ok is_race_hours=$is_race_hours d1_unavailable=$d1_unavailable offload_fallback=0 cf_trigger=waiting)"
        return 0
      fi
      log "finish-position[$label] CF trigger sent ${elapsed}s ago but coverage still incomplete after verify delay — escalating to local docker (last resort)"
      rm -f "$marker_path"
      cf_trigger_status="verify_delay_expired"
    else
      local trigger_token
      trigger_token="$(read_trigger_token)"
      if [ -z "$trigger_token" ]; then
        log "finish-position[$label] TRIGGER_TOKEN unset (checked $ROOT_ENV_FILE) — CF trigger unavailable, falling back to local docker kick"
        cf_trigger_status="token_unavailable"
      else
        local incomplete_categories
        incomplete_categories="$(fp_incomplete_categories "$target_date_iso" "$target_nen" "$target_tsukihi" | tr '\n' ',' | sed 's/,$//')"
        if [ -z "$incomplete_categories" ]; then
          log "WARN: finish-position[$label] could not determine incomplete categories from D1/Neon per-category query — falling back to local docker kick"
          cf_trigger_status="categories_unknown"
        elif cf_trigger_categories "$incomplete_categories" "$target_date" "$trigger_token"; then
          mkdir -p "$CF_TRIGGER_MARKER_DIR"
          date -u +%s > "$marker_path"
          log "finish-position[$label] CF trigger sent for categories=$incomplete_categories — skip local docker kick this tick, will verify after ${CF_TRIGGER_VERIFY_DELAY_SECONDS}s"
          log "guard_target done (label=$label target=$target_date_iso expected=$expected_count rs=${rs_actual:-skipped} fp=$fp_actual cf_ok=$corner_features_ok is_race_hours=$is_race_hours d1_unavailable=$d1_unavailable offload_fallback=0 cf_trigger=sent)"
          return 0
        else
          log "finish-position[$label] CF trigger POST failed for one or more categories=$incomplete_categories — falling back to local docker kick"
          cf_trigger_status="post_failed"
        fi
      fi
    fi

    if [ -d "$FINISH_LOCK_DIR" ]; then
      log "finish-position-predict lock $FINISH_LOCK_DIR held — another run in progress, skip fallback kick"
    elif [ "${DRY_RUN:-0}" = "1" ]; then
      log "DRY_RUN: would exec RUN_DATE=$target_date PREDICT_DAYS_AHEAD=$days_ahead RUN_DATE_MODE=auto bash $FINISH_SCRIPT (CF-offload fallback, last resort, reason=$cf_trigger_status)"
    else
      log "exec RUN_DATE=$target_date PREDICT_DAYS_AHEAD=$days_ahead bash $FINISH_SCRIPT (CF-offload fallback, last resort, reason=$cf_trigger_status)"
      RUN_DATE="$target_date" PREDICT_DAYS_AHEAD="$days_ahead" RUN_DATE_MODE=auto \
        bash "$FINISH_SCRIPT" || log "finish-position-predict-daily.sh exited non-zero (continuing)"
    fi
    log "guard_target done (label=$label target=$target_date_iso expected=$expected_count rs=${rs_actual:-skipped} fp=$fp_actual cf_ok=$corner_features_ok is_race_hours=$is_race_hours d1_unavailable=$d1_unavailable offload_fallback=1 cf_trigger=$cf_trigger_status)"
    return 0
  fi

  if [ "$d1_unavailable" = "1" ]; then
    log "finish-position[$label] SKIPPED — D1 unavailable, so running-style completion cannot be verified; order takes priority over race-hours freshness"
    log "guard_target done (label=$label target=$target_date_iso expected=D1_UNAVAILABLE rs=skipped fp=skipped cf_ok=$corner_features_ok is_race_hours=$is_race_hours d1_unavailable=$d1_unavailable)"
    return 0
  fi

  if [ "$rs_complete_for_finish" != "1" ]; then
    if [ "$rs_kicked_this_tick" = "1" ]; then
      log "finish-position[$label] SKIPPED — running-style was kicked asynchronously in this tick; next tick will re-check completion before docker"
    else
      log "finish-position[$label] SKIPPED — running-style is not confirmed complete for expected=$expected_count"
    fi
    log "guard_target done (label=$label target=$target_date_iso expected=$expected_count rs=${rs_actual:-skipped} fp=skipped cf_ok=$corner_features_ok is_race_hours=$is_race_hours d1_unavailable=$d1_unavailable)"
    return 0
  fi

  log "checking finish-position coverage in Neon ($FP_TABLE) for nen=$target_nen tsukihi=$target_tsukihi ($label) ..."
  local fp_actual
  if [ "${DRY_RUN:-0}" = "1" ] && [ -n "${FORCE_FP_ACTUAL:-}" ]; then
    fp_actual="$FORCE_FP_ACTUAL"
    log "DRY_RUN: FORCE_FP_ACTUAL=$FORCE_FP_ACTUAL override (skipping Neon finish-position query)"
  else
    fp_actual="$(neon_count "$FP_TABLE" "$target_nen" "$target_tsukihi" || true)"
  fi
  if ! printf '%s' "$fp_actual" | grep -Eq '^[0-9]+$'; then
    log "ERROR: failed to parse finish-position count for $label from Neon (got: $fp_actual)"
    return 1
  fi
  log "finish-position[$label]: actual=$fp_actual expected=${expected_count:-D1_UNAVAILABLE} is_race_hours=$is_race_hours d1_unavailable=$d1_unavailable"

  # Decide whether to kick:
  #   kick when incomplete OR when in race hours (freshness), but only after
  #   running-style has been confirmed complete above.
  local should_kick=0
  if [ "$fp_actual" -lt "$expected_count" ]; then
    log "finish-position[$label] INCOMPLETE — will kick (RUN_DATE=$target_date PREDICT_DAYS_AHEAD=$days_ahead)"
    should_kick=1
  elif [ "$is_race_hours" = "1" ]; then
    log "finish-position[$label] complete but race-hours freshness — will re-kick (RUN_DATE=$target_date PREDICT_DAYS_AHEAD=$days_ahead)"
    should_kick=1
  else
    log "finish-position[$label] COMPLETE (outside race hours) — skip kick"
  fi

  if [ "$should_kick" = "1" ]; then
    if [ -d "$FINISH_LOCK_DIR" ]; then
      log "finish-position-predict lock $FINISH_LOCK_DIR held — another run in progress, skip kick"
    elif [ "${DRY_RUN:-0}" = "1" ]; then
      log "DRY_RUN: would exec RUN_DATE=$target_date PREDICT_DAYS_AHEAD=$days_ahead RUN_DATE_MODE=auto bash $FINISH_SCRIPT"
    else
      log "exec RUN_DATE=$target_date PREDICT_DAYS_AHEAD=$days_ahead bash $FINISH_SCRIPT"
      RUN_DATE="$target_date" PREDICT_DAYS_AHEAD="$days_ahead" RUN_DATE_MODE=auto \
        bash "$FINISH_SCRIPT" || log "finish-position-predict-daily.sh exited non-zero (continuing)"
    fi
  fi

  log "guard_target done (label=$label target=$target_date_iso expected=${expected_count:-D1_UNAVAILABLE} rs=${rs_actual:-skipped} fp=$fp_actual cf_ok=$corner_features_ok is_race_hours=$is_race_hours d1_unavailable=$d1_unavailable)"
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
#   0-9   -> TODAY only (hourly; no races, skip-when-complete)
#   10-20 -> TODAY only (20-min cadence during race hours; freshness re-predict)
#   21-23 -> TODAY + TOMORROW (hourly pre-warm; skip-when-complete)
#   else  -> exit (not a scheduled window)
#
# JST_HOUR is passed to guard_target as $5 so the finish-position skip logic
# can distinguish race-hours (freshness mode) from non-race-hours.
case "$JST_HOUR" in
  0[0-9])
    log "window=today JST_HOUR=$JST_HOUR (0-9 morning band, hourly, skip-when-complete)"
    guard_target "$TODAY_DATE" "$TODAY_ISO" 0 "today" "$JST_HOUR"
    ;;
  1[0-9]|20)
    log "window=today JST_HOUR=$JST_HOUR (10-20 race-hours band, 20-min cadence, freshness re-predict)"
    guard_target "$TODAY_DATE" "$TODAY_ISO" 0 "today" "$JST_HOUR"
    ;;
  21|22|23)
    log "window=today+tomorrow JST_HOUR=$JST_HOUR (21-23 pre-warm band, skip-when-complete)"
    guard_target "$TODAY_DATE" "$TODAY_ISO" 0 "today" "$JST_HOUR"
    guard_target "$TOMORROW_DATE" "$TOMORROW_ISO" 1 "tomorrow" "$JST_HOUR"
    ;;
  *)
    log "outside guard window (JST_HOUR=$JST_HOUR) — exit 0"
    exit 0
    ;;
esac

log "guard fully done (JST_HOUR=$JST_HOUR)"
exit 0
