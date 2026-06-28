#!/usr/bin/env bash
# pc-keiba pipeline daily health monitor.
#
# Driven by the LaunchAgent at scripts/launchd/com.kkk4oru.pc-keiba-health-monitor.plist
# (every 15 min during JST 09:00-22:59). Runs a battery of read-only checks
# against Cloudflare D1, the Cloudflare API (queue backlog), Neon Postgres,
# and the live viewer surface. Each check votes 0 (ok) / 1 (fail) and the
# script tracks consecutive failures per check in a small state file so we
# only fire a macOS notification once a check has failed 3 ticks in a row
# (= ~45 min). One-off flaps are logged but never page.
#
# Background:
#   On 2026-06-28 the fetch-results queue stopped processing at 15:42 JST
#   and was not noticed until ~7 h later (visually in the viewer). There
#   was no alert, no log threshold, no automation that paged the operator.
#   This script is the post-mortem control: each check below targets a
#   specific signal that would have caught that incident in <= 45 min.
#
# Run modes:
#   - Live  (default, used by launchd) — talks to real APIs, writes log /
#     state, may emit notification on 3rd consecutive failure of a check.
#   - Dry-run (HEALTH_DRY_RUN=1) — every external call is replaced by a
#     fixture file under scripts/launchd/health-monitor-fixtures/. State
#     is written to a per-run tmpdir so the test invocation cannot
#     clobber the real state file. Notifications are NEVER sent in
#     dry-run; instead the planned notification body is printed.
#
# Exit codes:
#   0 — all checks passed (or dry-run finished cleanly)
#   1 — at least one check failed this tick (still 0 from launchd's
#       perspective if the script ran to completion; launchd treats
#       non-zero as a hard error and may back off — we do NOT want
#       repeated check failures to trigger launchd backoff, so the
#       main path always exits 0 unless something hard-fails like
#       missing curl. The "failure" condition is tracked in state.json
#       and surfaced via the notification.)
#
# Silencer:
#   touch /tmp/pc-keiba-health-silenced
#   ...notifications will be skipped while that file exists. Logs still
#   run normally — only the macOS notification is suppressed. Useful when
#   the operator is on vacation or knowingly working through a known
#   outage and doesn't want repeated pages.
set -uo pipefail
# NOTE: we do NOT set -e here. The whole point of this script is to keep
# running through partial failures and aggregate them — a single failed
# curl must not abort the rest of the checks.

# ---------------------------------------------------------------------------
# Constants & paths.
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
FIXTURES_DIR="$SCRIPT_DIR/health-monitor-fixtures"

LOG_DIR="/Users/kkk4oru/Library/Logs/pc-keiba-health"
SILENCER_FILE="/tmp/pc-keiba-health-silenced"
LOCK_DIR="/tmp/pc-keiba-health-monitor.lock"

# In dry-run we re-point state/log dirs to a tmpdir so tests don't pollute
# the real state file (which would corrupt the consecutive-failure counter
# for the live monitor).
if [ "${HEALTH_DRY_RUN:-0}" = "1" ]; then
  HEALTH_TMP_ROOT="${HEALTH_TMP_ROOT:-$(mktemp -d -t pc-keiba-health-dryrun)}"
  LOG_DIR="$HEALTH_TMP_ROOT/log"
  STATE_FILE="$HEALTH_TMP_ROOT/state.json"
  LOCK_DIR="$HEALTH_TMP_ROOT/lock"
else
  STATE_FILE="$LOG_DIR/state.json"
fi

mkdir -p "$LOG_DIR"

# Per-day dated log. JST = UTC+9.
TODAY_JST="$(date -u -v+9H +%Y%m%d)"
DATED_LOG="$LOG_DIR/${TODAY_JST}.log"

# Failure-pattern thresholds (overridable for testing).
NOTIFY_AFTER_N_FAILURES="${NOTIFY_AFTER_N_FAILURES:-3}"

# Cloudflare config.
D1_BINDING_NAME="sync-realtime-data"
WRANGLER_CONFIG="apps/sync-realtime-data/wrangler.jsonc"
JOBS_QUEUE_NAME="sync-realtime-data-jobs"
JOBS_QUEUE_BACKLOG_THRESHOLD="${JOBS_QUEUE_BACKLOG_THRESHOLD:-200}"

# Viewer config.
VIEWER_BASE_URL_DEFAULT="https://pc-keiba-viewer.kkk4oru.com"

# Env file locations.
ROOT_ENV_FILE="$REPO_ROOT/.env"
VIEWER_ENV_FILE="$REPO_ROOT/apps/pc-keiba-viewer/.env.local"
NEON_ENV_FILE="$REPO_ROOT/apps/local-postgresql/.env.replica"

# ---------------------------------------------------------------------------
# Single-writer lock — a 15-min cadence can collide with a previous slow run
# (e.g. a 60s curl timeout). We skip rather than queue.
# ---------------------------------------------------------------------------
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  printf '%s [pc-keiba-health] lock %s held; previous run in progress, skipping\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$LOCK_DIR" >> "$LOG_DIR/lock-skips.log"
  exit 0
fi
trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT

# Tee everything from here on to the dated log.
exec > >(tee -a "$DATED_LOG") 2>&1

log() {
  # Print to stderr so callers that capture stdout (read_cloudflare_token,
  # fixture_load, etc.) aren't polluted by log lines. The script-level
  # `exec > >(tee -a "$DATED_LOG") 2>&1` above merges stderr back into stdout
  # for the tee, so the dated log still captures everything.
  printf '%s [pc-keiba-health] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >&2
}

mask() {
  # Mask credentials in URLs of the form scheme://user:pass@host/... and
  # bearer tokens.
  sed -E -e 's#(://)[^:@/]+:[^@]+@#\1***:***@#g' -e 's#(Bearer )[A-Za-z0-9._-]+#\1***#g'
}

mask_token() {
  local t="$1"
  local n="${#t}"
  if [ "$n" -le 8 ]; then
    printf '****'
    return
  fi
  printf '%s...%s' "${t:0:4}" "${t: -4}"
}

# ---------------------------------------------------------------------------
# State file — tiny JSON with per-check consecutive_failures counter.
# We avoid `jq -s` round-trips: just read/write a flat key=value bash assoc
# array and serialize it. macOS bash 3.2 lacks assoc arrays, so we use a
# pair of parallel arrays. The on-disk JSON is for human readability.
# ---------------------------------------------------------------------------
STATE_KEYS=()
STATE_VALS=()

load_state() {
  STATE_KEYS=()
  STATE_VALS=()
  if [ ! -f "$STATE_FILE" ]; then
    return 0
  fi
  # Parse {"check_name":N,...} into the parallel arrays via jq.
  if ! command -v jq >/dev/null 2>&1; then
    log "WARN: jq not on PATH — state file ignored, counters reset"
    return 0
  fi
  local line
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    local k="${line%%=*}"
    local v="${line#*=}"
    STATE_KEYS+=("$k")
    STATE_VALS+=("$v")
  done < <(jq -r 'to_entries | .[] | "\(.key)=\(.value)"' < "$STATE_FILE" 2>/dev/null || true)
}

state_get() {
  local key="$1"
  local i=0
  while [ "$i" -lt "${#STATE_KEYS[@]}" ]; do
    if [ "${STATE_KEYS[i]}" = "$key" ]; then
      printf '%s' "${STATE_VALS[i]}"
      return 0
    fi
    i=$((i + 1))
  done
  printf '0'
}

state_set() {
  local key="$1"
  local val="$2"
  local i=0
  while [ "$i" -lt "${#STATE_KEYS[@]}" ]; do
    if [ "${STATE_KEYS[i]}" = "$key" ]; then
      STATE_VALS[i]="$val"
      return 0
    fi
    i=$((i + 1))
  done
  STATE_KEYS+=("$key")
  STATE_VALS+=("$val")
}

persist_state() {
  # Build a JSON object {key:int,...} via jq from null.
  if ! command -v jq >/dev/null 2>&1; then
    log "WARN: jq not on PATH — state file NOT persisted"
    return 0
  fi
  local tmp
  tmp="$(mktemp -t pc-keiba-health-state)"
  printf '{}' > "$tmp"
  local i=0
  while [ "$i" -lt "${#STATE_KEYS[@]}" ]; do
    local k="${STATE_KEYS[i]}"
    local v="${STATE_VALS[i]}"
    local next
    next="$(jq --arg k "$k" --argjson v "$v" '. + {($k):$v}' < "$tmp")"
    printf '%s' "$next" > "$tmp"
    i=$((i + 1))
  done
  mv "$tmp" "$STATE_FILE"
}

# ---------------------------------------------------------------------------
# Notification.
# ---------------------------------------------------------------------------
send_notification() {
  local title="$1"
  local body="$2"
  if [ -f "$SILENCER_FILE" ]; then
    log "NOTIFY (silenced via $SILENCER_FILE): $title — $body"
    return 0
  fi
  if [ "${HEALTH_DRY_RUN:-0}" = "1" ]; then
    log "NOTIFY (DRY_RUN): $title — $body"
    return 0
  fi
  # Best-effort: never let osascript failure propagate.
  osascript -e "display notification \"$body\" with title \"$title\"" \
    >/dev/null 2>&1 || true
  log "NOTIFY sent: $title — $body"
}

# ---------------------------------------------------------------------------
# Env loaders (read-only, mask secrets in log).
# ---------------------------------------------------------------------------
read_cloudflare_token() {
  if [ ! -f "$ROOT_ENV_FILE" ]; then
    log "WARN: $ROOT_ENV_FILE missing — CF API checks will be skipped"
    return 1
  fi
  local line
  line="$(grep -E '^CLOUDFLARE_DEBUG_TOKEN=' "$ROOT_ENV_FILE" | head -1 || true)"
  if [ -z "$line" ]; then
    log "WARN: CLOUDFLARE_DEBUG_TOKEN missing in $ROOT_ENV_FILE — CF API checks will be skipped"
    return 1
  fi
  local t="${line#CLOUDFLARE_DEBUG_TOKEN=}"
  t="${t%\'}"; t="${t#\'}"; t="${t%\"}"; t="${t#\"}"
  if [ -z "$t" ]; then
    log "WARN: CLOUDFLARE_DEBUG_TOKEN parsed empty"
    return 1
  fi
  printf '%s' "$t"
}

read_cloudflare_account_id() {
  if [ ! -f "$ROOT_ENV_FILE" ]; then
    return 1
  fi
  local line
  line="$(grep -E '^CLOUDFLARE_ACCOUNT_ID=' "$ROOT_ENV_FILE" | head -1 || true)"
  if [ -z "$line" ]; then
    return 1
  fi
  local v="${line#CLOUDFLARE_ACCOUNT_ID=}"
  v="${v%\'}"; v="${v#\'}"; v="${v%\"}"; v="${v#\"}"
  if [ -z "$v" ]; then
    return 1
  fi
  printf '%s' "$v"
}

read_viewer_access_creds() {
  # Echoes "<id>|<secret>" on success, or returns non-zero.
  if [ ! -f "$VIEWER_ENV_FILE" ]; then
    log "WARN: $VIEWER_ENV_FILE missing — viewer checks will use unauthenticated requests"
    return 1
  fi
  local id_line secret_line
  id_line="$(grep -E '^PC_KEIBA_ACCESS_CLIENT_ID=' "$VIEWER_ENV_FILE" | head -1 || true)"
  secret_line="$(grep -E '^PC_KEIBA_ACCESS_CLIENT_SECRET=' "$VIEWER_ENV_FILE" | head -1 || true)"
  local id="${id_line#PC_KEIBA_ACCESS_CLIENT_ID=}"
  local secret="${secret_line#PC_KEIBA_ACCESS_CLIENT_SECRET=}"
  id="${id%\'}"; id="${id#\'}"; id="${id%\"}"; id="${id#\"}"
  secret="${secret%\'}"; secret="${secret#\'}"; secret="${secret%\"}"; secret="${secret#\"}"
  if [ -z "$id" ] || [ -z "$secret" ]; then
    return 1
  fi
  printf '%s|%s' "$id" "$secret"
}

read_neon_url() {
  if [ ! -f "$NEON_ENV_FILE" ]; then
    return 1
  fi
  local line
  line="$(grep -E '^NEON_DATABASE_URL=' "$NEON_ENV_FILE" | head -1 || true)"
  if [ -z "$line" ]; then
    return 1
  fi
  local v="${line#NEON_DATABASE_URL=}"
  v="${v%\'}"; v="${v#\'}"; v="${v%\"}"; v="${v#\"}"
  printf '%s' "$v"
}

# ---------------------------------------------------------------------------
# Low-level fetchers. Each call routes through a fixture file when dry-run.
# ---------------------------------------------------------------------------

# Args: fixture_basename
# In dry-run, HEALTH_DRY_RUN_FIXTURE_PREFIX may be set (e.g. to "fail__") so a
# test can drive deliberate failure paths by dropping fixtures named
# "fail__<name>.json" alongside the ok ones. When the prefixed file does not
# exist we fall back to the un-prefixed fixture (test only needed to flip one
# check).
fixture_load() {
  local name="$1"
  local prefix="${HEALTH_DRY_RUN_FIXTURE_PREFIX:-}"
  if [ -n "$prefix" ]; then
    local prefixed="$FIXTURES_DIR/${prefix}${name}.json"
    if [ -f "$prefixed" ]; then
      cat "$prefixed"
      return 0
    fi
  fi
  local path="$FIXTURES_DIR/${name}.json"
  if [ ! -f "$path" ]; then
    log "WARN: dry-run fixture $path missing — emitting empty body"
    printf '{}'
    return 0
  fi
  cat "$path"
}

# Args: d1_query, fixture_name
d1_execute() {
  local query="$1"
  local fixture="$2"
  if [ "${HEALTH_DRY_RUN:-0}" = "1" ]; then
    fixture_load "$fixture"
    return 0
  fi
  if ! command -v bunx >/dev/null 2>&1; then
    log "WARN: bunx not on PATH — D1 check unavailable"
    return 1
  fi
  bunx wrangler d1 execute "$D1_BINDING_NAME" --remote \
    --config "$WRANGLER_CONFIG" --command "$query" --json 2>&1 || true
}

# Args: url, [extra header args...], fixture_name (last argument)
http_get_json() {
  local all=("$@")
  local n=${#all[@]}
  local fixture_name="${all[$((n - 1))]}"
  if [ "${HEALTH_DRY_RUN:-0}" = "1" ]; then
    fixture_load "$fixture_name"
    return 0
  fi
  if ! command -v curl >/dev/null 2>&1; then
    log "ERROR: curl not on PATH — cannot make HTTP requests"
    return 1
  fi
  local args=("${all[@]:0:$((n - 1))}")
  curl -fsS --max-time 30 "${args[@]}" 2>&1 || true
}

# ---------------------------------------------------------------------------
# JST helpers.
# ---------------------------------------------------------------------------
jst_hour() { date -u -v+9H +%H; }
jst_now_epoch_minus() {
  # Args: minutes to subtract. Returns ISO 8601 in JST with offset.
  local mins="$1"
  date -u -v+9H -v"-${mins}M" +%Y-%m-%dT%H:%M:%S+09:00
}
jst_today_iso() { date -u -v+9H +%Y-%m-%d; }

# ---------------------------------------------------------------------------
# Checks. Each returns 0 if healthy, 1 if failing. Each logs the rationale.
# ---------------------------------------------------------------------------

# C1: fetch-results — must have an 'ok' row in fetch_logs within the last
# 30 min during 13:00-21:30 JST (the race-results window). Outside that
# window we skip the check (no expected traffic).
check_fetch_results_recent() {
  local h
  h="$(jst_hour)"
  if [ "${HEALTH_DRY_RUN:-0}" != "1" ] && { [ "$h" -lt 13 ] || [ "$h" -ge 22 ]; }; then
    log "[fetch-results-recent] skip — JST hour $h outside race-result window (13-21)"
    return 0
  fi
  log "[fetch-results-recent] checking..."
  local cutoff
  cutoff="$(jst_now_epoch_minus 30)"
  local q="SELECT COUNT(*) AS c FROM fetch_logs WHERE job_type='fetch-results' AND status='ok' AND created_at > '${cutoff}';"
  local out
  out="$(d1_execute "$q" "fetch_logs_results_ok")"
  local c
  c="$(as_int "$(printf '%s' "$out" | jq -r '.[0].results[0].c // 0' 2>/dev/null || printf '0')")"
  log "[fetch-results-recent] ok-rows-since-cutoff=$c cutoff=$cutoff"
  if [ "$c" -ge 1 ]; then
    return 0
  fi
  log "[fetch-results-recent] FAIL — no fetch-results ok rows in last 30 min"
  return 1
}

# C2: fetch-weights — same shape, race-window check shifted earlier
# because weights start landing T-60 min before post, so 11:00-21:00 JST.
check_fetch_weights_recent() {
  local h
  h="$(jst_hour)"
  if [ "${HEALTH_DRY_RUN:-0}" != "1" ] && { [ "$h" -lt 11 ] || [ "$h" -ge 22 ]; }; then
    log "[fetch-weights-recent] skip — JST hour $h outside weight-fetch window (11-21)"
    return 0
  fi
  log "[fetch-weights-recent] checking..."
  local cutoff
  cutoff="$(jst_now_epoch_minus 30)"
  local q="SELECT COUNT(*) AS c FROM fetch_logs WHERE job_type='fetch-weights' AND status='ok' AND created_at > '${cutoff}';"
  local out
  out="$(d1_execute "$q" "fetch_logs_weights_ok")"
  local c
  c="$(as_int "$(printf '%s' "$out" | jq -r '.[0].results[0].c // 0' 2>/dev/null || printf '0')")"
  log "[fetch-weights-recent] ok-rows-since-cutoff=$c cutoff=$cutoff"
  if [ "$c" -ge 1 ]; then
    return 0
  fi
  log "[fetch-weights-recent] FAIL — no fetch-weights ok rows in last 30 min"
  return 1
}

# C3: queue backlog — sync-realtime-data-jobs. Alert if > threshold for
# > 10 min. We approximate the "for > 10 min" gate by remembering the
# backlog value across ticks: if THIS tick's backlog is > threshold AND the
# previous backlog (from state.json) was ALSO > threshold, that's two
# consecutive 15-min ticks = ~15-30 min sustained.
check_queue_backlog() {
  log "[queue-backlog] checking..."
  local cf_token="" cf_account=""
  cf_token="$(read_cloudflare_token 2>/dev/null || true)"
  cf_account="$(read_cloudflare_account_id 2>/dev/null || true)"
  if [ "${HEALTH_DRY_RUN:-0}" != "1" ]; then
    if [ -z "$cf_token" ]; then
      log "[queue-backlog] skip — CF token missing in $ROOT_ENV_FILE"
      return 0
    fi
    if [ -z "$cf_account" ]; then
      log "[queue-backlog] skip — CLOUDFLARE_ACCOUNT_ID missing in $ROOT_ENV_FILE"
      return 0
    fi
  fi
  if [ -n "$cf_token" ]; then
    log "[queue-backlog] cf_token=$(mask_token "$cf_token") cf_account=$(mask_token "$cf_account")"
  fi
  local url="https://api.cloudflare.com/client/v4/accounts/${cf_account}/queues?name=${JOBS_QUEUE_NAME}"
  local body
  body="$(http_get_json "$url" -H "Authorization: Bearer $cf_token" -H "Content-Type: application/json" "queue_backlog")"
  # CF API returns {"result":[{"queue_id":"...","backlog_size":N,...}],...}
  local backlog
  backlog="$(printf '%s' "$body" | jq -r '.result[0].backlog_size // .result[0].producers_total_count // 0' 2>/dev/null || printf '0')"
  log "[queue-backlog] backlog=$backlog threshold=$JOBS_QUEUE_BACKLOG_THRESHOLD"
  local prev
  prev="$(state_get queue_backlog_last_value)"
  if [ -z "$prev" ] || ! printf '%s' "$prev" | grep -Eq '^[0-9]+$'; then
    prev=0
  fi
  if ! printf '%s' "$backlog" | grep -Eq '^[0-9]+$'; then
    log "[queue-backlog] WARN: non-numeric backlog '$backlog' — treating as 0"
    backlog=0
  fi
  state_set queue_backlog_last_value "$backlog"
  if [ "$backlog" -le "$JOBS_QUEUE_BACKLOG_THRESHOLD" ]; then
    return 0
  fi
  if [ "$prev" -le "$JOBS_QUEUE_BACKLOG_THRESHOLD" ]; then
    log "[queue-backlog] above threshold this tick but previous tick was below ($prev) — single-tick spike, not failing yet"
    return 0
  fi
  log "[queue-backlog] FAIL — backlog $backlog > $JOBS_QUEUE_BACKLOG_THRESHOLD for at least two consecutive ticks"
  return 1
}

# C4: today's race results coverage — count races whose start was > 30 min
# ago but which have NEVER been fetched (last_result_fetch_at IS NULL AND
# result_complete_at IS NULL). > 5 = at least 5 finished races we silently
# skipped — exactly the 2026-06-28 symptom.
check_today_results_coverage() {
  log "[today-results-coverage] checking..."
  local cutoff
  cutoff="$(jst_now_epoch_minus 30)"
  local today
  today="$(jst_today_iso)"
  local q="SELECT COUNT(*) AS c FROM realtime_race_sources WHERE substr(race_start_at_jst,1,10)='${today}' AND race_start_at_jst < '${cutoff}' AND last_result_fetch_at IS NULL AND result_complete_at IS NULL;"
  local out
  out="$(d1_execute "$q" "today_results_coverage")"
  local c
  c="$(as_int "$(printf '%s' "$out" | jq -r '.[0].results[0].c // 0' 2>/dev/null || printf '0')")"
  log "[today-results-coverage] unfetched-finished-races=$c"
  if [ "$c" -le 5 ]; then
    return 0
  fi
  log "[today-results-coverage] FAIL — $c finished races never had a result fetch attempted"
  return 1
}

# C5: viewer trends — hit the trends endpoint for a known sample race and
# assert the JSON contains a siblings[].finishPosition shape. We use a
# deterministic past-race route (2026-05-12 中山 R8, which is one of the
# in-repo test fixtures, so the response should always be populated even
# during a no-racing window).
check_today_trends_render() {
  log "[trends-render] checking..."
  local base="${PC_KEIBA_VIEWER_BASE_URL:-$VIEWER_BASE_URL_DEFAULT}"
  local creds
  local url="${base}/api/races/2026/05/12/06/08/trends"
  local body
  if creds="$(read_viewer_access_creds)"; then
    local id="${creds%%|*}"
    local secret="${creds##*|}"
    body="$(http_get_json "$url" -H "CF-Access-Client-Id: $id" -H "CF-Access-Client-Secret: $secret" "trends_sample")"
  else
    body="$(http_get_json "$url" "trends_sample")"
  fi
  local has_fp
  has_fp="$(printf '%s' "$body" | jq -r '[..|.finishPosition? // empty] | length' 2>/dev/null || printf '0')"
  log "[trends-render] finishPosition-count=$has_fp"
  if [ "$has_fp" -ge 1 ]; then
    return 0
  fi
  log "[trends-render] FAIL — trends response has no finishPosition keys (sample url=$url)"
  return 1
}

# C6: mojibake detector — fetch a small sample of training sections and
# look for U+FFFD (the replacement character that signals a charset
# mis-decode upstream). We hit the sections endpoint for the same sample
# triplet we use in C5 (avoids depending on today's racing schedule).
check_mojibake_in_responses() {
  log "[mojibake] checking..."
  local base="${PC_KEIBA_VIEWER_BASE_URL:-$VIEWER_BASE_URL_DEFAULT}"
  local creds id="" secret=""
  if creds="$(read_viewer_access_creds)"; then
    id="${creds%%|*}"
    secret="${creds##*|}"
  fi
  local samples=("2026/05/12/06/08" "2026/05/12/06/09" "2026/05/12/06/10")
  local hits=0
  local sample
  for sample in "${samples[@]}"; do
    local url="${base}/api/races/${sample}/sections/training"
    local body
    if [ -n "$id" ]; then
      body="$(http_get_json "$url" -H "CF-Access-Client-Id: $id" -H "CF-Access-Client-Secret: $secret" "training_sample")"
    else
      body="$(http_get_json "$url" "training_sample")"
    fi
    # U+FFFD is 0xEF 0xBF 0xBD in UTF-8. Use grep -F so it works without -P.
    if printf '%s' "$body" | grep -F $'\xef\xbf\xbd' >/dev/null 2>&1; then
      hits=$((hits + 1))
      log "[mojibake] U+FFFD detected in $url"
    fi
  done
  log "[mojibake] mojibake-samples=$hits / ${#samples[@]}"
  if [ "$hits" -eq 0 ]; then
    return 0
  fi
  log "[mojibake] FAIL — $hits of ${#samples[@]} training responses contain U+FFFD"
  return 1
}

# C7: weight freshness — for each race whose start was > 60 min ago today,
# we expect at least one horse_weight_snapshots row keyed to that race
# (fetched_at not null). Count races violating that. Alert if > 5.
check_weight_freshness() {
  log "[weight-freshness] checking..."
  local cutoff
  cutoff="$(jst_now_epoch_minus 60)"
  local today
  today="$(jst_today_iso)"
  # Races started 60+ min ago today that have no associated weight snapshot.
  # We use a NOT EXISTS subquery on horse_weight_snapshots keyed by race_key.
  local q="SELECT COUNT(*) AS c FROM realtime_race_sources rs WHERE substr(rs.race_start_at_jst,1,10)='${today}' AND rs.race_start_at_jst < '${cutoff}' AND NOT EXISTS (SELECT 1 FROM horse_weight_snapshots hws WHERE hws.race_key = rs.race_key);"
  local out
  out="$(d1_execute "$q" "weight_freshness")"
  local c
  c="$(as_int "$(printf '%s' "$out" | jq -r '.[0].results[0].c // 0' 2>/dev/null || printf '0')")"
  log "[weight-freshness] races-without-weight=$c"
  if [ "$c" -le 5 ]; then
    return 0
  fi
  log "[weight-freshness] FAIL — $c finished races (>60min) have no weight snapshot"
  return 1
}

# ---------------------------------------------------------------------------
# Failure-pattern dispatch.
#
# For each check name + return code:
#   ok  (0)  -> reset state.<check>_consecutive_failures to 0, log GREEN
#   fail(1) -> increment counter, log AMBER on 2, log RED + notify on 3+
# ---------------------------------------------------------------------------
record_check_result() {
  local name="$1"
  local rc="$2"
  local key="${name}_consecutive_failures"
  local prev
  prev="$(state_get "$key")"
  if [ -z "$prev" ] || ! printf '%s' "$prev" | grep -Eq '^[0-9]+$'; then
    prev=0
  fi
  if [ "$rc" -eq 0 ]; then
    if [ "$prev" -gt 0 ]; then
      log "[$name] RECOVERED (was at $prev consecutive failures) — counter reset"
    else
      log "[$name] OK"
    fi
    state_set "$key" 0
    return 0
  fi
  local next=$((prev + 1))
  state_set "$key" "$next"
  if [ "$next" -lt "$NOTIFY_AFTER_N_FAILURES" ]; then
    if [ "$next" -eq 1 ]; then
      log "[$name] FAIL (1) — first failure, log only"
    else
      log "[$name] FAIL ($next) — AMBER (notify at $NOTIFY_AFTER_N_FAILURES)"
    fi
    return 0
  fi
  log "[$name] FAIL ($next) — RED, sending notification"
  send_notification "pc-keiba health: $name" \
    "Check '$name' has failed $next consecutive times. See $DATED_LOG"
}

# ---------------------------------------------------------------------------
# Main.
# ---------------------------------------------------------------------------
# Coerce arbitrary input into a non-negative integer (used so a stray null /
# empty / non-numeric response from D1 or jq doesn't trip `[ "$c" -ge 1 ]`).
as_int() {
  local v="$1"
  if printf '%s' "$v" | grep -Eq '^[0-9]+$'; then
    printf '%s' "$v"
  else
    printf '0'
  fi
}

log "tick start (DRY_RUN=${HEALTH_DRY_RUN:-0} JST_HOUR=$(jst_hour) TODAY=$TODAY_JST)"
log "  STATE_FILE=$STATE_FILE LOG=$DATED_LOG"

load_state

# Eagerly validate the Neon env so an install-time misconfiguration is loud
# (rather than silently surfacing only when a future Neon-side check runs).
# The URL value itself is NOT used by the current 7 checks, but reading it
# now means a wrong path or missing key is logged on the very first tick.
NEON_DATABASE_URL_PARSED="$(read_neon_url 2>/dev/null || true)"
if [ -n "$NEON_DATABASE_URL_PARSED" ]; then
  log "NEON_DATABASE_URL parsed OK: $(printf '%s' "$NEON_DATABASE_URL_PARSED" | mask)"
else
  log "WARN: NEON_DATABASE_URL not parseable from $NEON_ENV_FILE (Neon-side checks would degrade)"
fi
unset NEON_DATABASE_URL_PARSED

# Sanity: warn if optional tools are missing but proceed.
for tool in jq curl bunx osascript; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    log "WARN: '$tool' not on PATH — checks that depend on it will degrade gracefully"
  fi
done

# Run each check; record state; sum failures. Each invocation is spelled
# out (rather than dispatched dynamically) so static analyzers can see
# every function is actually called.
ANY_FAIL=0

rc=0; check_fetch_results_recent    || rc=$?; record_check_result fetch_results_recent    "$rc"; [ "$rc" -ne 0 ] && ANY_FAIL=1
rc=0; check_fetch_weights_recent    || rc=$?; record_check_result fetch_weights_recent    "$rc"; [ "$rc" -ne 0 ] && ANY_FAIL=1
rc=0; check_queue_backlog           || rc=$?; record_check_result queue_backlog           "$rc"; [ "$rc" -ne 0 ] && ANY_FAIL=1
rc=0; check_today_results_coverage  || rc=$?; record_check_result today_results_coverage  "$rc"; [ "$rc" -ne 0 ] && ANY_FAIL=1
rc=0; check_today_trends_render     || rc=$?; record_check_result today_trends_render     "$rc"; [ "$rc" -ne 0 ] && ANY_FAIL=1
rc=0; check_mojibake_in_responses   || rc=$?; record_check_result mojibake_in_responses   "$rc"; [ "$rc" -ne 0 ] && ANY_FAIL=1
rc=0; check_weight_freshness        || rc=$?; record_check_result weight_freshness        "$rc"; [ "$rc" -ne 0 ] && ANY_FAIL=1
: "${ANY_FAIL}"  # mark ANY_FAIL as used downstream

persist_state
log "tick done — any-failure=$ANY_FAIL state=$STATE_FILE"
exit 0
