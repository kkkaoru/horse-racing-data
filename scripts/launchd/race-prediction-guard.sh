#!/usr/bin/env bash
# Race-prediction freshness guard.
#
# Driven by the LaunchAgent at scripts/launchd/com.kkk4oru.race-prediction-guard.plist:
#   * JST 00:00 .. 09:00 (10 hourly  fires) -> guard TODAY    JST
#   * JST 10:00 .. 20:00 (33 20-min  fires) -> guard TODAY    JST (race-hours band)
#   * JST 21:00 .. 23:00 ( 3 hourly  fires) -> guard TOMORROW JST + TODAY JST
# Total ~46 fires/day. During race hours (10-20) the guard always re-kicks the
# finish-position pipeline even when predictions already exist, so that fresh
# bataiju (announced ~T-30..40 min) and updated odds flow into each race before
# post. Outside race hours the skip-when-complete logic is preserved so we do
# not waste compute when no new data is expected.
#
# Freshness-aware re-prediction (PART 2 change):
#   During JST 10:00-20:00 ("race hours") the finish-position guard ignores
#   whether fp_actual >= expected_count and always kicks the pipeline, because
#   bataiju/odds land in D1 ~30-40 min before post and the prediction should
#   incorporate them. The concurrent-run lock (FINISH_LOCK_DIR) is still
#   respected — two docker runs never race each other.
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
# tick has rows to plan against. No prediction kick fires that hour.
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
#   DRY_RUN=1 FORCE_HOUR=14 bash scripts/launchd/race-prediction-guard.sh     # race hours (freshness mode)
#   DRY_RUN=1 FORCE_HOUR=19 bash scripts/launchd/race-prediction-guard.sh     # today (evening)
#   DRY_RUN=1 FORCE_HOUR=22 bash scripts/launchd/race-prediction-guard.sh     # today + tomorrow
#   DRY_RUN=1 FORCE_HOUR=22 FORCE_TARGET_DATE=20300101 bash ...               # exercise discover-urls path
#   DRY_RUN=1 FORCE_HOUR=05 FORCE_NO_CORNER_FEATURES=1 bash ...               # exercise corner-features build path
#   DRY_RUN=1 FORCE_HOUR=05 FORCE_VENUE_COUNTS=44:7,30:12 \
#     FORCE_EXPECTED_COUNT=42 FORCE_TARGET_DATE=20300101 bash ...             # exercise per-venue coverage check path
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
#   DRY_RUN=1 FORCE_HOUR=05 FORCE_NO_CORNER_FEATURES=1 bash ...               # exercise corner-features build path
#   DRY_RUN=1 FORCE_HOUR=05 FORCE_VENUE_COUNTS=44:7,30:12 \
#     FORCE_EXPECTED_COUNT=42 FORCE_TARGET_DATE=20300101 bash ...             # exercise per-venue coverage check path
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

# Per-venue coverage lower bounds. NAR major venues typically run 10-12 races
# per active day, JRA major venues typically run 12; if D1 shows fewer than
# these we treat the date as partially-discovered and re-kick discover-urls.
# Higher race counts are fine (some days have extra races).
# Today's incident: 大井 (44) ended up at 7 races because discover-urls D1
# write retries all failed — this guard would have caught that and re-kicked.
EXPECTED_NAR_RACES_PER_VENUE=10
EXPECTED_JRA_RACES_PER_VENUE=11

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
  cf_count="$(neon_corner_features_count "$target_nen" "$target_tsukihi" || true)"
  if ! printf '%s' "$cf_count" | grep -Eq '^[0-9]+$'; then
    log "ERROR: failed to parse corner-features count for $label from Neon (got: $cf_count)"
    return 1
  fi

  # DRY_RUN-only override that simulates a missing corner-features state.
  if [ "${DRY_RUN:-0}" = "1" ] && [ "${FORCE_NO_CORNER_FEATURES:-0}" = "1" ]; then
    log "corner-features[$label]: actual=$cf_count (FORCE_NO_CORNER_FEATURES=1 — treating as 0)"
    cf_count=0
  else
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
  # DRY_RUN-only override so the per-venue coverage path can be exercised
  # without touching a real D1. FORCE_EXPECTED_COUNT is applied AFTER the live
  # D1 query so any parse error is still surfaced above.
  if [ "${DRY_RUN:-0}" = "1" ] && [ -n "${FORCE_EXPECTED_COUNT:-}" ]; then
    log "DRY_RUN: FORCE_EXPECTED_COUNT=$FORCE_EXPECTED_COUNT override (was: $expected_count)"
    expected_count="$FORCE_EXPECTED_COUNT"
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
  if [ "$corner_features_ok" != "1" ]; then
    log "running-style[$label] SKIPPED — corner-features unavailable for $target_date"
  else
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
  fi

  # --- finish-position guard ---
  #
  # Freshness-aware skip logic:
  #   During race hours (JST 10:00-20:00) bataiju (馬体重) for upcoming races
  #   is typically announced ~T-30..40 min before post, and odds continue to
  #   shift. We therefore ALWAYS re-kick the pipeline during race hours, even
  #   when fp_actual >= expected_count, so predictions incorporate the latest
  #   bataiju/odds. The concurrent-run lock (FINISH_LOCK_DIR) is still checked
  #   — two docker runs never overlap.
  #
  #   Outside race hours (0-9, 21-23) the old "skip when complete" logic is
  #   preserved: no new race data is expected, so a re-run would be pure
  #   compute waste.
  #
  #   "race hours" = JST hour in [10, 20] inclusive.
  local is_race_hours=0
  if [ "$jst_hour" -ge 10 ] && [ "$jst_hour" -le 20 ]; then
    is_race_hours=1
  fi

  log "checking finish-position coverage in Neon ($FP_TABLE) for nen=$target_nen tsukihi=$target_tsukihi ($label) ..."
  local fp_actual
  fp_actual="$(neon_count "$FP_TABLE" "$target_nen" "$target_tsukihi" || true)"
  if ! printf '%s' "$fp_actual" | grep -Eq '^[0-9]+$'; then
    log "ERROR: failed to parse finish-position count for $label from Neon (got: $fp_actual)"
    return 1
  fi
  log "finish-position[$label]: actual=$fp_actual expected=$expected_count is_race_hours=$is_race_hours"

  # Decide whether to kick: kick when incomplete OR when in race hours
  # (freshness re-prediction for bataiju/odds).
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

  log "guard_target done (label=$label target=$target_date_iso expected=$expected_count rs=${rs_actual:-skipped} fp=$fp_actual cf_ok=$corner_features_ok is_race_hours=$is_race_hours)"
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
