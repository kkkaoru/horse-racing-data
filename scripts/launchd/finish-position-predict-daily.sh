#!/usr/bin/env bash
# Deprecated local finish-position-prediction Docker runner.
#
# Production feature generation, running-style prediction, and finish-position
# prediction are Cloudflare-side. This script is retained only for local/manual
# diagnostics and backfills; Mac launchd is not production authority.
#
# Idempotent UPSERT into race_finish_position_model_predictions — safe to re-run.
#
# Optional local invocation (dry-run / today's date):
#   bash scripts/launchd/finish-position-predict-daily.sh
#   DRY_RUN=1 FORCE_EXPECTED_COUNT=12 FORCE_RS_ACTUAL=12 bash scripts/launchd/finish-position-predict-daily.sh
#   DRY_RUN=1 FORCE_EXPECTED_COUNT=12 FORCE_RS_ACTUAL=11 bash scripts/launchd/finish-position-predict-daily.sh
#   DRY_RUN=1 FORCE_D1_FAIL_ATTEMPTS=3 bash ...                                                        # D1 expected-count retry exhaustion -> SKIP
#   DRY_RUN=1 FORCE_D1_FAIL_ATTEMPTS=1 FORCE_RS_ACTUAL=12 bash ...                                      # D1 expected-count retry succeeds on attempt 2
#   DRY_RUN=1 FORCE_EXPECTED_COUNT=12 FORCE_RS_ACTUAL=5 \
#     FORCE_RS_CATEGORY_EXPECTED=jra:5,nar:4,ban-ei:3 FORCE_RS_CATEGORY_ACTUAL=jra:5,nar:2,ban-ei:3 \
#     bash ...                                                                                          # RS retries exhaust -> per-category fallback (jra,ban-ei run, nar skipped)
#   DRY_RUN=1 FORCE_EXPECTED_COUNT=12 FORCE_RS_ACTUAL=5 \
#     FORCE_RS_CATEGORY_EXPECTED=jra:5,nar:4,ban-ei:3 FORCE_RS_CATEGORY_ACTUAL=jra:2,nar:2,ban-ei:1 \
#     bash ...                                                                                          # per-category fallback where every category is still incomplete -> SKIP
#
# RUN_DATE override:
#   The caller may set RUN_DATE=YYYYMMDD for local/manual diagnostics. When
#   unset, defaults to today JST. The container always interprets RUN_DATE as
#   JST YYYYMMDD.
#
# Resilience (2026-07-04):
#   * D1 expected-count query retries transiently-failing wrangler/D1 calls up
#     to D1_RETRY_MAX_ATTEMPTS times, D1_RETRY_INTERVAL_SECONDS apart, before
#     treating the date as SKIPPED. A transient D1 blip degrades to a skip
#     (self-heals next tick) rather than a hard error/page.
#   * running-style completeness retries up to RS_RETRY_MAX_ATTEMPTS times,
#     RS_RETRY_INTERVAL_SECONDS apart (a slow-materializing prewarm usually
#     finishes within ~30 min). If still incomplete after all retries, per-
#     category (jra / nar / ban-ei) coverage is checked independently and
#     PREDICT_CATEGORIES is narrowed to whichever categories ARE complete, so
#     one stuck category no longer blocks the others. Only SKIPs entirely when
#     every category is still incomplete (never runs blind).
#
# Lock coordination:
#   Holds /tmp/finish-position-predict.lock for the duration of the docker run
#   so local manual runs cannot overlap. The lock is a plain directory (mkdir
#   is atomic on macOS — flock is not shipped with macOS).
set -euo pipefail

# Resolve repo root from this script's location (scripts/launchd -> repo root).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

# Constants.
IMAGE_TAG="finish-position-predict-local:split2"
DOCKERFILE_PATH="apps/finish-position-predict-container/Dockerfile"
# Host is host.docker.internal (not 127.0.0.1) because local PG was migrated
# from Docker Compose (inside Colima VM) to Apple Container CLI (outside Colima
# VM) in commits ac8626f4 / 0fe46d1c / 8887fb52. With Apple-CLI PG bound on the
# Mac host, the Colima VM's 127.0.0.1 loopback no longer reaches it, so the
# predict container needs host.docker.internal to traverse Colima → Mac host.
# Caller may still override via the SOURCE_DATABASE_URL env (see pre-flight 5).
SOURCE_DATABASE_URL_DEFAULT="postgresql://horse_racing:horse_racing@host.docker.internal:15432/horse_racing"
NEON_ENV_FILE="apps/local-postgresql/.env.replica"
D1_BINDING_NAME="sync-realtime-data"
WRANGLER_CONFIG="apps/sync-realtime-data/wrangler.jsonc"
RS_TABLE="race_running_style_model_predictions"
LOG_DIR="/Users/kkk4oru/Library/Logs/finish-position-predict"
FAILURE_LOG="$LOG_DIR/failures.log"
LOCK_DIR="/tmp/finish-position-predict.lock"

# Retry tuning for the D1 expected-count query (transient wrangler/D1 errors)
# and the running-style completeness check (prewarm still materializing).
D1_RETRY_MAX_ATTEMPTS=3
D1_RETRY_INTERVAL_SECONDS=20
RS_RETRY_MAX_ATTEMPTS=6
RS_RETRY_INTERVAL_SECONDS=300

mkdir -p "$LOG_DIR"

# Single-writer lock for local/manual runs. mkdir is atomic on macOS
# (test-and-set in one syscall). If lock is held, exit 0 with a log; a
# concurrent Docker run would race the same UPSERT and waste local capacity.
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  printf '%s [finish-position-predict-daily] lock %s held; another run in progress, skipping\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$LOCK_DIR" >> "$LOG_DIR/lock-skips.log"
  exit 0
fi
trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT

# JST = UTC+9. `date -u +%Y%m%d -v+9H` adds 9 h to current UTC and formats as
# YYYYMMDD — that is "today in JST" regardless of the Mac's local timezone, so
# a misconfigured TZ still yields the correct run date. RUN_DATE env override
# is honored for explicit local backfills.
RUN_DATE="${RUN_DATE:-$(date -u -v+9H +%Y%m%d)}"
RUN_DATE_ISO="${RUN_DATE:0:4}-${RUN_DATE:4:2}-${RUN_DATE:6:2}"
DATED_LOG="$LOG_DIR/${RUN_DATE}.log"

# tee everything from here on to the dated log. This dated log is the per-run
# local record we can later grep for credentials etc.
exec > >(tee -a "$DATED_LOG") 2>&1

log() {
  printf '%s [finish-position-predict-daily] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

fail() {
  local msg="$1"
  log "ERROR: $msg"
  printf '%s RUN_DATE=%s status=error msg=%s\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$RUN_DATE" "$msg" >> "$FAILURE_LOG"
  # Best-effort desktop notification; never propagate its failure.
  osascript -e "display notification \"$msg\" with title \"finish-position local runner (RUN_DATE=$RUN_DATE)\"" \
    >/dev/null 2>&1 || true
  exit 1
}

mask() {
  # Mask credentials in any URL of the form scheme://user:pass@host/...
  # Used to redact NEON_DATABASE_URL when logging it.
  sed -E 's#(://)[^:@/]+:[^@]+@#\1***:***@#g'
}

# Query Neon for COUNT(DISTINCT race) in the named prediction table for the
# given (nen, tsukihi). This matches the local diagnostic race-level coverage
# check in race-prediction-guard.sh.
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

# Query D1 for expected race counts broken down by category (jra / nar /
# ban-ei), using the SAME keibajo_code partition as the prediction container
# itself (apps/finish-position-predict-container/src/pipeline_runner.py):
# jra = keibajo_code 01-10, ban-ei = keibajo_code 83, nar = everything else.
# Emits one "<category> <count>" line per row to stdout; log lines go to
# stderr so they don't interleave with the data the caller captures via
# command substitution. When DRY_RUN=1 and FORCE_RS_CATEGORY_EXPECTED is set,
# the override is parsed instead of touching D1.
d1_expected_by_category() {
  if [ "${DRY_RUN:-0}" = "1" ] && [ -n "${FORCE_RS_CATEGORY_EXPECTED:-}" ]; then
    log "DRY_RUN: FORCE_RS_CATEGORY_EXPECTED=$FORCE_RS_CATEGORY_EXPECTED override (skipping D1 query)" >&2
    printf '%s' "$FORCE_RS_CATEGORY_EXPECTED" | tr ',' '\n' | awk -F: 'NF==2 {print $1" "$2}'
    return 0
  fi
  local d1_query="SELECT CASE WHEN keibajo_code IN ('01','02','03','04','05','06','07','08','09','10') THEN 'jra' WHEN keibajo_code = '83' THEN 'ban-ei' ELSE 'nar' END AS category, COUNT(DISTINCT race_key) AS c FROM realtime_race_sources WHERE substr(race_start_at_jst, 1, 10) = '${RUN_DATE_ISO}' GROUP BY category;"
  local d1_result
  d1_result="$(bunx wrangler d1 execute "$D1_BINDING_NAME" --remote --config "$WRANGLER_CONFIG" --command "$d1_query" --json 2>&1 || true)"
  printf '%s' "$d1_result" | jq -r '.[0].results[]? | "\(.category) \(.c)"' 2>/dev/null || true
}

# Query Neon for running-style actual race counts broken down by category,
# using the identical keibajo_code partition as d1_expected_by_category.
# Emits one "<category> <count>" line per row.
neon_count_by_category() {
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

RUNNING_STYLE_PREFLIGHT_RESULT=""
RUNNING_STYLE_PREFLIGHT_REASON=""
# Set to "1" by running_style_percategory_fallback when it narrows
# PREDICT_CATEGORIES to a complete subset, so pre-flight 9 can log the reason
# accurately instead of mislabeling it a caller override.
RUNNING_STYLE_PARTIAL_CATEGORIES="0"

# Per-category running-style coverage fallback, used only after
# RS_RETRY_MAX_ATTEMPTS full-date retries are still incomplete. Compares each
# category's D1 expected count against its Neon actual count and narrows
# PREDICT_CATEGORIES to just the categories that ARE complete, so one stuck
# category (e.g. NAR) does not block others (e.g. JRA) that are ready.
#
# Sets RUNNING_STYLE_PREFLIGHT_RESULT/_REASON:
#   "ok"   — at least one category is complete; PREDICT_CATEGORIES narrowed
#            to that subset (RUNNING_STYLE_PARTIAL_CATEGORIES=1).
#   "skip" — every category is still incomplete (or D1 returned nothing) —
#            do not run blind.
# Args: $1 target_nen  $2 target_tsukihi
running_style_percategory_fallback() {
  local target_nen="$1"
  local target_tsukihi="$2"

  local expected_rows
  expected_rows="$(d1_expected_by_category)"
  if [ -z "$expected_rows" ]; then
    RUNNING_STYLE_PREFLIGHT_RESULT="skip"
    RUNNING_STYLE_PREFLIGHT_REASON="per-category fallback: D1 returned no per-category expected counts for RUN_DATE=$RUN_DATE"
    log "finish-position SKIPPED — $RUNNING_STYLE_PREFLIGHT_REASON"
    return 1
  fi

  local actual_rows
  if [ "${DRY_RUN:-0}" = "1" ] && [ -n "${FORCE_RS_CATEGORY_ACTUAL:-}" ]; then
    log "DRY_RUN: FORCE_RS_CATEGORY_ACTUAL=$FORCE_RS_CATEGORY_ACTUAL override (skipping Neon per-category query)"
    actual_rows="$(printf '%s' "$FORCE_RS_CATEGORY_ACTUAL" | tr ',' '\n' | awk -F: 'NF==2 {print $1" "$2}')"
  else
    actual_rows="$(neon_count_by_category "$RS_TABLE" "$target_nen" "$target_tsukihi" || true)"
  fi

  local complete_categories=""
  local incomplete_categories=""
  local category expected_c actual_c
  while read -r category expected_c; do
    if [ -z "$category" ]; then
      continue
    fi
    if ! printf '%s' "$expected_c" | grep -Eq '^[0-9]+$'; then
      log "WARN: running-style per-category[$category]: non-numeric expected count ($expected_c) — treating as incomplete"
      incomplete_categories="${incomplete_categories:+$incomplete_categories,}$category"
      continue
    fi
    actual_c="$(printf '%s\n' "$actual_rows" | awk -v c="$category" '$1==c {print $2}')"
    if ! printf '%s' "$actual_c" | grep -Eq '^[0-9]+$'; then
      actual_c=0
    fi
    log "running-style per-category[$category]: actual=$actual_c expected=$expected_c"
    if [ "$actual_c" -ge "$expected_c" ]; then
      complete_categories="${complete_categories:+$complete_categories,}$category"
    else
      incomplete_categories="${incomplete_categories:+$incomplete_categories,}$category"
    fi
  done <<< "$expected_rows"

  if [ -z "$complete_categories" ]; then
    RUNNING_STYLE_PREFLIGHT_RESULT="skip"
    RUNNING_STYLE_PREFLIGHT_REASON="per-category fallback: all categories still incomplete for RUN_DATE=$RUN_DATE (incomplete=$incomplete_categories)"
    log "finish-position SKIPPED — $RUNNING_STYLE_PREFLIGHT_REASON"
    return 1
  fi

  PREDICT_CATEGORIES="$complete_categories"
  RUNNING_STYLE_PARTIAL_CATEGORIES="1"
  RUNNING_STYLE_PREFLIGHT_RESULT="ok"
  RUNNING_STYLE_PREFLIGHT_REASON="per-category fallback: running complete subset=$complete_categories (skipping incomplete=${incomplete_categories:-none}) for RUN_DATE=$RUN_DATE"
  log "running-style preflight PARTIAL-OK — $RUNNING_STYLE_PREFLIGHT_REASON"
  return 0
}

running_style_preflight() {
  local target_nen="${RUN_DATE:0:4}"
  local target_tsukihi="${RUN_DATE:4:4}"
  local expected_count=""

  log "running-style preflight: checking D1 expected race count for $RUN_DATE_ISO before finish-position docker"
  if [ "${DRY_RUN:-0}" = "1" ] && [ -n "${FORCE_EXPECTED_COUNT:-}" ]; then
    expected_count="$FORCE_EXPECTED_COUNT"
    log "DRY_RUN: FORCE_EXPECTED_COUNT=$FORCE_EXPECTED_COUNT override (skipping D1 query)"
  else
    local d1_query="SELECT COUNT(DISTINCT race_key) AS c FROM realtime_race_sources WHERE substr(race_start_at_jst, 1, 10) = '${RUN_DATE_ISO}';"
    local d1_result=""
    local d1_attempt
    # Transient wrangler/D1 failures (fetch failed / token refresh) should not
    # abort the whole run outright — retry a few times before giving up.
    for d1_attempt in $(seq 1 "$D1_RETRY_MAX_ATTEMPTS"); do
      if [ "${DRY_RUN:-0}" = "1" ] && [ -n "${FORCE_D1_FAIL_ATTEMPTS:-}" ]; then
        if [ "$d1_attempt" -le "$FORCE_D1_FAIL_ATTEMPTS" ]; then
          d1_result="In a non-interactive environment, it's necessary to set a CLOUDFLARE_API_TOKEN environment variable for wrangler to work"
          log "DRY_RUN: FORCE_D1_FAIL_ATTEMPTS=$FORCE_D1_FAIL_ATTEMPTS — injecting fake D1 failure on attempt $d1_attempt/$D1_RETRY_MAX_ATTEMPTS"
        else
          d1_result='[{"results":[{"c":1}]}]'
          log "DRY_RUN: FORCE_D1_FAIL_ATTEMPTS=$FORCE_D1_FAIL_ATTEMPTS — injecting fake D1 success on attempt $d1_attempt/$D1_RETRY_MAX_ATTEMPTS"
        fi
      else
        d1_result="$(bunx wrangler d1 execute "$D1_BINDING_NAME" --remote --config "$WRANGLER_CONFIG" --command "$d1_query" --json 2>&1 || true)"
      fi
      expected_count="$(printf '%s' "$d1_result" | jq -r '.[0].results[0].c // empty' 2>/dev/null || true)"
      if [ -n "$expected_count" ] && [ "$expected_count" != "null" ]; then
        break
      fi
      log "WARN: running-style preflight: D1 expected-count attempt $d1_attempt/$D1_RETRY_MAX_ATTEMPTS failed (result tail: $(printf '%s' "$d1_result" | tail -c 400))"
      if [ "$d1_attempt" -lt "$D1_RETRY_MAX_ATTEMPTS" ]; then
        if [ "${DRY_RUN:-0}" = "1" ]; then
          log "DRY_RUN: would sleep ${D1_RETRY_INTERVAL_SECONDS}s before retry"
        else
          sleep "$D1_RETRY_INTERVAL_SECONDS"
        fi
      fi
    done

    if [ -z "$expected_count" ] || [ "$expected_count" = "null" ]; then
      # All attempts failed. This script has no per-venue D1 fallback (that
      # lives in race-prediction-guard.sh's query_d1_venue_counts /
      # check_venue_coverage) — do not run blind; SKIP rather than hard-fail
      # so a transient D1 outage self-heals next tick instead of paging.
      RUNNING_STYLE_PREFLIGHT_RESULT="skip"
      RUNNING_STYLE_PREFLIGHT_REASON="D1 expected race count unavailable after $D1_RETRY_MAX_ATTEMPTS attempts (last result tail: $(printf '%s' "$d1_result" | tail -c 400))"
      log "finish-position SKIPPED — $RUNNING_STYLE_PREFLIGHT_REASON"
      return 1
    fi
  fi

  if ! printf '%s' "$expected_count" | grep -Eq '^[0-9]+$'; then
    RUNNING_STYLE_PREFLIGHT_RESULT="error"
    RUNNING_STYLE_PREFLIGHT_REASON="non-numeric D1 expected race count: $expected_count"
    log "ERROR: running-style preflight: $RUNNING_STYLE_PREFLIGHT_REASON"
    return 1
  fi

  if [ "$expected_count" = "0" ]; then
    RUNNING_STYLE_PREFLIGHT_RESULT="skip"
    RUNNING_STYLE_PREFLIGHT_REASON="D1 expected race count is 0 for RUN_DATE=$RUN_DATE"
    log "finish-position SKIPPED — $RUNNING_STYLE_PREFLIGHT_REASON"
    return 1
  fi

  local rs_actual=""
  local rs_attempt
  for rs_attempt in $(seq 1 "$RS_RETRY_MAX_ATTEMPTS"); do
    if [ "${DRY_RUN:-0}" = "1" ] && [ -n "${FORCE_RS_ACTUAL:-}" ]; then
      rs_actual="$FORCE_RS_ACTUAL"
      log "DRY_RUN: FORCE_RS_ACTUAL=$FORCE_RS_ACTUAL override (attempt $rs_attempt/$RS_RETRY_MAX_ATTEMPTS, skipping Neon query)"
    else
      log "running-style preflight: checking Neon ($RS_TABLE) for nen=$target_nen tsukihi=$target_tsukihi (attempt $rs_attempt/$RS_RETRY_MAX_ATTEMPTS)"
      rs_actual="$(neon_count "$RS_TABLE" "$target_nen" "$target_tsukihi" || true)"
    fi

    if ! printf '%s' "$rs_actual" | grep -Eq '^[0-9]+$'; then
      RUNNING_STYLE_PREFLIGHT_RESULT="error"
      RUNNING_STYLE_PREFLIGHT_REASON="failed to parse running-style count from Neon (got: $rs_actual)"
      log "ERROR: running-style preflight: $RUNNING_STYLE_PREFLIGHT_REASON"
      return 1
    fi

    log "running-style preflight: actual=$rs_actual expected=$expected_count (attempt $rs_attempt/$RS_RETRY_MAX_ATTEMPTS)"
    if [ "$rs_actual" -ge "$expected_count" ]; then
      RUNNING_STYLE_PREFLIGHT_RESULT="ok"
      RUNNING_STYLE_PREFLIGHT_REASON="running-style complete for RUN_DATE=$RUN_DATE (actual=$rs_actual expected=$expected_count, attempt $rs_attempt/$RS_RETRY_MAX_ATTEMPTS)"
      log "running-style preflight OK — $RUNNING_STYLE_PREFLIGHT_REASON"
      return 0
    fi

    if [ "$rs_attempt" -lt "$RS_RETRY_MAX_ATTEMPTS" ]; then
      log "running-style preflight: incomplete (actual=$rs_actual expected=$expected_count) — retry $rs_attempt/$RS_RETRY_MAX_ATTEMPTS, waiting ${RS_RETRY_INTERVAL_SECONDS}s for prewarm to materialize"
      if [ "${DRY_RUN:-0}" = "1" ]; then
        log "DRY_RUN: would sleep ${RS_RETRY_INTERVAL_SECONDS}s before retry"
      else
        sleep "$RS_RETRY_INTERVAL_SECONDS"
      fi
    fi
  done

  # All RS_RETRY_MAX_ATTEMPTS attempts still incomplete for the full date.
  # Fall back to per-category coverage so a category that IS complete does
  # not keep waiting on a category that is stuck.
  log "running-style preflight: still incomplete after $RS_RETRY_MAX_ATTEMPTS attempts (actual=$rs_actual expected=$expected_count) — checking per-category coverage"
  running_style_percategory_fallback "$target_nen" "$target_tsukihi"
  return $?
}

log "RUN_DATE=$RUN_DATE RUN_DATE_ISO=$RUN_DATE_ISO REPO_ROOT=$REPO_ROOT"
if [ "${DRY_RUN:-0}" = "1" ]; then
  log "DRY_RUN=1 — docker/colima actions will be logged but not executed"
fi

# Pre-flight 1: read NEON_DATABASE_URL from .env.replica (single-quoted by
# convention). Strip surrounding quotes if present.
if [ ! -f "$NEON_ENV_FILE" ]; then
  fail "$NEON_ENV_FILE not found"
fi
NEON_LINE="$(grep -E '^NEON_DATABASE_URL=' "$NEON_ENV_FILE" | head -1 || true)"
if [ -z "$NEON_LINE" ]; then
  fail "NEON_DATABASE_URL not set in $NEON_ENV_FILE"
fi
NEON_DATABASE_URL="${NEON_LINE#NEON_DATABASE_URL=}"
NEON_DATABASE_URL="${NEON_DATABASE_URL%\'}"
NEON_DATABASE_URL="${NEON_DATABASE_URL#\'}"
NEON_DATABASE_URL="${NEON_DATABASE_URL%\"}"
NEON_DATABASE_URL="${NEON_DATABASE_URL#\"}"
if [ -z "$NEON_DATABASE_URL" ]; then
  fail "NEON_DATABASE_URL parsed empty from $NEON_ENV_FILE"
fi
export NEON_DATABASE_URL
log "NEON_DATABASE_URL=$(printf '%s' "$NEON_DATABASE_URL" | mask)"

# Pre-flight 2: local/manual safeguard. Production ordering is Cloudflare-side;
# this check only keeps this deprecated Docker runner from starting when the
# running-style inputs are visibly incomplete.
if ! running_style_preflight; then
  if [ "$RUNNING_STYLE_PREFLIGHT_RESULT" = "skip" ]; then
    log "SUCCESS SKIPPED RUN_DATE=$RUN_DATE reason=$RUNNING_STYLE_PREFLIGHT_REASON"
    exit 0
  fi
  fail "running-style preflight failed: $RUNNING_STYLE_PREFLIGHT_REASON"
fi

# Pre-flight 3: Colima must be running (docker daemon).
log "checking colima status..."
if [ "${DRY_RUN:-0}" = "1" ]; then
  log "DRY_RUN: would check colima status and start it if needed"
elif ! colima status >/dev/null 2>&1; then
  log "colima not running; attempting to start..."
  if ! colima start >/dev/null 2>&1; then
    fail "colima start failed; cannot reach docker"
  fi
  log "colima started"
fi

# Pre-flight 4: docker reachable.
if [ "${DRY_RUN:-0}" = "1" ]; then
  log "DRY_RUN: would check docker info"
elif ! docker info >/dev/null 2>&1; then
  fail "docker info failed (colima up but docker unreachable)"
fi

# Pre-flight 5: image exists locally; rebuild if not.
if [ "${DRY_RUN:-0}" = "1" ]; then
  log "DRY_RUN: would inspect image $IMAGE_TAG and build from $DOCKERFILE_PATH if missing"
elif ! docker image inspect "$IMAGE_TAG" >/dev/null 2>&1; then
  log "image $IMAGE_TAG missing; building from $DOCKERFILE_PATH..."
  if ! docker build -f "$DOCKERFILE_PATH" -t "$IMAGE_TAG" "$REPO_ROOT"; then
    fail "docker build $IMAGE_TAG failed"
  fi
  log "image $IMAGE_TAG built"
fi

# Pre-flight 6: DNS prewarm for the Neon host.
#
# Background (2026-06-28): NAR category failed with "[Errno -3] Temporary
# failure in name resolution" (EAI_AGAIN) inside the container while Ban-ei
# in the same docker run succeeded. The container retry path now treats
# EAI_AGAIN as transient (see apps/finish-position-predict-container/src/
# db_driver.py _TRANSIENT_ERROR_TOKENS), but a host-side prewarm gives the
# system resolver a fresh cache entry BEFORE docker starts — eliminating
# most EAI_AGAIN windows entirely for this local Docker runner.
#
# Mac scutil and the Colima VM share DNS via ``--network=host``, so resolving
# on the Mac side warms the path the container will use. Failure here is
# non-fatal — the in-container retry layer handles the residual case.
NEON_HOST="$(printf '%s' "$NEON_DATABASE_URL" \
  | sed -nE 's#^[a-zA-Z]+://[^@]*@([^/:?]+).*#\1#p')"
if [ -n "$NEON_HOST" ]; then
  log "DNS prewarm: resolving $NEON_HOST (best-effort, non-fatal)"
  # Two attempts with a 1 s gap. dscacheutil is the canonical macOS resolver
  # query — it warms the same DirectoryServices cache the docker bridge will
  # consult. nslookup would also work; dscacheutil is faster and exits 0/1.
  for prewarm_attempt in 1 2; do
    if /usr/bin/dscacheutil -q host -a name "$NEON_HOST" >/dev/null 2>&1; then
      log "DNS prewarm: $NEON_HOST resolved on attempt $prewarm_attempt"
      break
    fi
    if [ "$prewarm_attempt" = "1" ]; then
      log "DNS prewarm: attempt 1 failed, retrying in 1 s"
      sleep 1
    else
      log "WARN: DNS prewarm failed twice for $NEON_HOST (container retry will handle it)"
    fi
  done
else
  log "WARN: could not parse host from NEON_DATABASE_URL — skipping DNS prewarm"
fi

# Pre-flight 7: SOURCE_DATABASE_URL — env override > default local Colima PG.
SRC="${SOURCE_DATABASE_URL:-$SOURCE_DATABASE_URL_DEFAULT}"
log "SOURCE_DATABASE_URL=$(printf '%s' "$SRC" | mask)"

# Pre-flight 8: PREDICT_DAYS_AHEAD default 0 (today only). Allow caller override.
DAYS_AHEAD="${PREDICT_DAYS_AHEAD:-0}"

# Pre-flight 9: PREDICT_CATEGORIES — scope which categories the container
# runs.  Resolution order (highest priority wins):
#
#   0. running-style per-category fallback (running_style_percategory_fallback,
#      pre-flight 2): when running-style is still incomplete for the full
#      expected count after RS_RETRY_MAX_ATTEMPTS retries, only the categories
#      whose running-style coverage IS complete are run.
#      RUNNING_STYLE_PARTIAL_CATEGORIES=1 marks this case for logging below.
#   1. Explicit caller env override from a manual/local invocation: honoured as-is.
#   2. Time-based auto-scope for local early-morning runs:
#      jvd_se (JRA mirror) is not available until ~09:03 JST, so running JRA
#      before 09:00 always returns races=0 and wastes ~30 s. When the JST hour
#      is 00-08 AND no explicit override is set, automatically restrict to
#      nar,ban-ei.
#   3. Unset (empty string): the container's own default runs ALL categories.
#      This is the path for later local/manual runs.
#
# This local-only auto-scope avoids wasteful JRA races=0 runs before the mirror
# is ready. It is not production scheduling logic.
if [ -n "${PREDICT_CATEGORIES:-}" ]; then
  if [ "$RUNNING_STYLE_PARTIAL_CATEGORIES" = "1" ]; then
    log "PREDICT_CATEGORIES=$PREDICT_CATEGORIES (running-style per-category fallback subset)"
  else
    log "PREDICT_CATEGORIES=$PREDICT_CATEGORIES (caller override)"
  fi
else
  JST_HOUR_NOW="$(date -u -v+9H +%H)"
  if [ "$JST_HOUR_NOW" -le 8 ]; then
    PREDICT_CATEGORIES="nar,ban-ei"
    log "PREDICT_CATEGORIES=$PREDICT_CATEGORIES (auto-scoped: JST_HOUR=$JST_HOUR_NOW < 09 — JRA mirror not yet ready)"
  else
    PREDICT_CATEGORIES=""
    log "PREDICT_CATEGORIES=<all> (JST_HOUR=$JST_HOUR_NOW >= 09 — JRA mirror available)"
  fi
fi

# Pre-flight 10: optional R2 credentials so the container's add-pacestyle layer
# can read the per-day running-style Parquet directly from
# pc-keiba-features-archive instead of ATTACHing to Neon. Source the repo-root
# .env if it exists; un-quote single-quoted values. Falls back to PG when any
# of the three keys are unset / empty so RS_SOURCE=auto still works.
ROOT_ENV_FILE="$REPO_ROOT/.env"
if [ -f "$ROOT_ENV_FILE" ]; then
  # shellcheck disable=SC2046
  set -a
  # shellcheck source=/dev/null
  source "$ROOT_ENV_FILE"
  set +a
fi
R2_ACCOUNT_ID="${R2_ACCOUNT_ID:-}"
R2_ACCESS_KEY_ID="${R2_ACCESS_KEY_ID:-}"
R2_SECRET_ACCESS_KEY="${R2_SECRET_ACCESS_KEY:-}"
R2_BUCKET="${R2_BUCKET:-pc-keiba-features-archive}"
if [ -n "$R2_ACCOUNT_ID" ] && [ -n "$R2_ACCESS_KEY_ID" ] && [ -n "$R2_SECRET_ACCESS_KEY" ]; then
  RS_SOURCE="${RS_SOURCE:-auto}"
  log "R2 credentials detected — RS_SOURCE=$RS_SOURCE R2_BUCKET=$R2_BUCKET"
else
  RS_SOURCE="pg"
  log "R2 credentials missing — forcing RS_SOURCE=pg (Neon ATTACH fallback)"
fi

# Run the prediction container. --network=host keeps the Colima VM's networking
# stack shared with the container; the SOURCE_DATABASE_URL default targets
# host.docker.internal:15432 (the Mac host) because local PG now runs under
# Apple Container CLI on the host (post-migration commits ac8626f4 / 0fe46d1c /
# 8887fb52). --rm so the container is removed after exit.
log "starting docker run $IMAGE_TAG RUN_DATE=$RUN_DATE PREDICT_DAYS_AHEAD=$DAYS_AHEAD PREDICT_CATEGORIES=${PREDICT_CATEGORIES:-<all>}..."
set +e
if [ "${DRY_RUN:-0}" = "1" ]; then
  log "DRY_RUN: would docker run --rm --network=host -e RUN_DATE=$RUN_DATE -e RUN_DATE_ISO=$RUN_DATE_ISO -e PREDICT_DAYS_AHEAD=$DAYS_AHEAD -e PREDICT_CATEGORIES=${PREDICT_CATEGORIES:-<all>} $IMAGE_TAG"
  docker_exit=0
else
  docker run --rm --network=host \
    -e SOURCE_DATABASE_URL="$SRC" \
    -e NEON_DATABASE_URL="$NEON_DATABASE_URL" \
    -e RUN_DATE="$RUN_DATE" \
    -e RUN_DATE_ISO="$RUN_DATE_ISO" \
    -e PREDICT_DAYS_AHEAD="$DAYS_AHEAD" \
    -e MODELS_DIR=/models \
    -e NAR_TRANSFORMER_BLEND_ENABLED="${NAR_TRANSFORMER_BLEND_ENABLED:-1}" \
    -e RS_SOURCE="$RS_SOURCE" \
    -e R2_ACCOUNT_ID="$R2_ACCOUNT_ID" \
    -e R2_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID" \
    -e R2_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY" \
    -e R2_BUCKET="$R2_BUCKET" \
    -e PREDICT_SERVE_MODE="" \
    ${PREDICT_CATEGORIES:+-e PREDICT_CATEGORIES="$PREDICT_CATEGORIES"} \
    "$IMAGE_TAG"
  docker_exit=$?
fi
set -e
log "docker run exited with code=$docker_exit"

# Sanity check: did any credential leak into the dated log? If so, sanitize
# in-place (still emit a warning). Patterns: Neon role prefix "npg_", local
# "horse_racing:horse_racing", any "user:pass@" form.
if grep -E 'npg_[A-Za-z0-9]+|horse_racing:horse_racing|://[^:@/]+:[^@]+@' "$DATED_LOG" >/dev/null 2>&1; then
  log "WARN: credentials detected in $DATED_LOG; sanitizing in-place"
  tmp="$(mktemp)"
  sed -E \
    -e 's#(://)[^:@/]+:[^@]+@#\1***:***@#g' \
    -e 's#npg_[A-Za-z0-9]+#npg_***#g' \
    -e 's#horse_racing:horse_racing#***:***#g' \
    "$DATED_LOG" > "$tmp"
  mv "$tmp" "$DATED_LOG"
fi

if [ "$docker_exit" -ne 0 ]; then
  fail "docker run exited non-zero ($docker_exit) — see $DATED_LOG"
fi

log "SUCCESS RUN_DATE=$RUN_DATE"
exit 0
