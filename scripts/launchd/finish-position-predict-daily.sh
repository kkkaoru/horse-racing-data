#!/usr/bin/env bash
# Daily finish-position-prediction docker pipeline wrapper.
#
# Driven by the LaunchAgent at scripts/launchd/com.kkk4oru.finish-position-predict.plist
# (JST 03:00 daily). Replaces the disabled Cloudflare Container cron (Cloudflare
# Containers reap batch instances at ~90-110 s; this workload needs ~10 min).
#
# Idempotent UPSERT into race_finish_position_model_predictions — safe to re-run.
#
# Manual invocation (dry-run / today's date):
#   launchctl kickstart -k gui/$(id -u)/com.kkk4oru.finish-position-predict
# or directly:
#   bash scripts/launchd/finish-position-predict-daily.sh
#
# RUN_DATE override:
#   The caller may set RUN_DATE=YYYYMMDD (e.g. for tomorrow's predictions from
#   the hourly race-prediction-guard launchd). When unset, defaults to today
#   JST. The container always interprets RUN_DATE as JST YYYYMMDD.
#
# Lock coordination:
#   Holds /tmp/finish-position-predict.lock for the duration of the docker run
#   so the JST 03:00 cron and the hourly race-prediction-guard (which can also
#   fire this script with PREDICT_DAYS_AHEAD=1 to cover tomorrow) cannot race.
#   The lock is a plain directory (mkdir is atomic on macOS — flock is not
#   shipped with macOS).
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
LOG_DIR="/Users/kkk4oru/Library/Logs/finish-position-predict"
FAILURE_LOG="$LOG_DIR/failures.log"
LOCK_DIR="/tmp/finish-position-predict.lock"

mkdir -p "$LOG_DIR"

# Single-writer lock shared with hourly race-prediction-guard. mkdir is atomic
# on macOS (test-and-set in one syscall). If lock is held, exit 0 with a log
# — a concurrent docker run would just race the same UPSERT and waste
# colima/docker capacity for ~10 min.
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  printf '%s [finish-position-predict-daily] lock %s held; another run in progress, skipping\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$LOCK_DIR" >> "$LOG_DIR/lock-skips.log"
  exit 0
fi
trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT

# JST = UTC+9. `date -u +%Y%m%d -v+9H` adds 9 h to current UTC and formats as
# YYYYMMDD — that is "today in JST" regardless of the Mac's local timezone, so
# a misconfigured TZ still yields the correct run date. RUN_DATE env override
# is honored (used by the race-prediction-guard to target tomorrow JST).
RUN_DATE="${RUN_DATE:-$(date -u -v+9H +%Y%m%d)}"
RUN_DATE_ISO="${RUN_DATE:0:4}-${RUN_DATE:4:2}-${RUN_DATE:6:2}"
DATED_LOG="$LOG_DIR/${RUN_DATE}.log"

# tee everything from here on to the dated log. The plist captures the raw
# stdout/stderr to its own files in $LOG_DIR; this dated log is the
# per-run record we can later grep for credentials etc.
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
  osascript -e "display notification \"$msg\" with title \"finish-position cron (RUN_DATE=$RUN_DATE)\"" \
    >/dev/null 2>&1 || true
  exit 1
}

mask() {
  # Mask credentials in any URL of the form scheme://user:pass@host/...
  # Used to redact NEON_DATABASE_URL when logging it.
  sed -E 's#(://)[^:@/]+:[^@]+@#\1***:***@#g'
}

log "RUN_DATE=$RUN_DATE RUN_DATE_ISO=$RUN_DATE_ISO REPO_ROOT=$REPO_ROOT"

# Pre-flight 1: Colima must be running (docker daemon).
log "checking colima status..."
if ! colima status >/dev/null 2>&1; then
  log "colima not running; attempting to start..."
  if ! colima start >/dev/null 2>&1; then
    fail "colima start failed; cannot reach docker"
  fi
  log "colima started"
fi

# Pre-flight 2: docker reachable.
if ! docker info >/dev/null 2>&1; then
  fail "docker info failed (colima up but docker unreachable)"
fi

# Pre-flight 3: image exists locally; rebuild if not.
if ! docker image inspect "$IMAGE_TAG" >/dev/null 2>&1; then
  log "image $IMAGE_TAG missing; building from $DOCKERFILE_PATH..."
  if ! docker build -f "$DOCKERFILE_PATH" -t "$IMAGE_TAG" "$REPO_ROOT"; then
    fail "docker build $IMAGE_TAG failed"
  fi
  log "image $IMAGE_TAG built"
fi

# Pre-flight 4: read NEON_DATABASE_URL from .env.replica (single-quoted by
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
log "NEON_DATABASE_URL=$(printf '%s' "$NEON_DATABASE_URL" | mask)"

# Pre-flight 4b: DNS prewarm for the Neon host.
#
# Background (2026-06-28): NAR category failed with "[Errno -3] Temporary
# failure in name resolution" (EAI_AGAIN) inside the container while Ban-ei
# in the same docker run succeeded. The container retry path now treats
# EAI_AGAIN as transient (see apps/finish-position-predict-container/src/
# db_driver.py _TRANSIENT_ERROR_TOKENS), but a host-side prewarm gives the
# system resolver a fresh cache entry BEFORE docker starts — eliminating
# most EAI_AGAIN windows entirely for the launchd cron.
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

# Pre-flight 5: SOURCE_DATABASE_URL — env override > default local Colima PG.
SRC="${SOURCE_DATABASE_URL:-$SOURCE_DATABASE_URL_DEFAULT}"
log "SOURCE_DATABASE_URL=$(printf '%s' "$SRC" | mask)"

# Pre-flight 6: PREDICT_DAYS_AHEAD default 0 (today only). Allow caller override.
DAYS_AHEAD="${PREDICT_DAYS_AHEAD:-0}"

# Pre-flight 6b: PREDICT_CATEGORIES — scope which categories the container
# runs.  Three-way resolution (highest priority wins):
#
#   1. Explicit caller env override (e.g. from the race-prediction-guard or a
#      manual invocation): honoured as-is.
#   2. Time-based auto-scope when called from the scheduled 03:00 JST slot:
#      jvd_se (JRA mirror) is not available until ~09:03 JST, so running JRA
#      at 03:00 always returns races=0 and wastes ~30 s.  When the JST hour is
#      00-08 (the 03:00 cron window, including catch-up fires after a sleep)
#      AND no explicit override is set, automatically restrict to nar,ban-ei.
#   3. Unset (empty string): the container's own default runs ALL categories.
#      This is the path for the 09:30 JST run and for any guard-kicked run.
#
# Phase-3 Fix #1: adding the 09:30 plist entry is the primary JRA odds fix;
# this auto-scope is the complementary "skip JRA at 03:00" optimisation that
# avoids wasteful races=0 runs without requiring a second plist.
if [ -n "${PREDICT_CATEGORIES:-}" ]; then
  # Explicit override from caller — use it unchanged.
  log "PREDICT_CATEGORIES=$PREDICT_CATEGORIES (caller override)"
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

# Pre-flight 7: optional R2 credentials so the container's add-pacestyle layer
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
docker run --rm --network=host \
  -e SOURCE_DATABASE_URL="$SRC" \
  -e NEON_DATABASE_URL="$NEON_DATABASE_URL" \
  -e RUN_DATE="$RUN_DATE" \
  -e RUN_DATE_ISO="$RUN_DATE_ISO" \
  -e PREDICT_DAYS_AHEAD="$DAYS_AHEAD" \
  -e MODELS_DIR=/models \
  -e RS_SOURCE="$RS_SOURCE" \
  -e R2_ACCOUNT_ID="$R2_ACCOUNT_ID" \
  -e R2_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID" \
  -e R2_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY" \
  -e R2_BUCKET="$R2_BUCKET" \
  -e PREDICT_SERVE_MODE="" \
  ${PREDICT_CATEGORIES:+-e PREDICT_CATEGORIES="$PREDICT_CATEGORIES"} \
  "$IMAGE_TAG"
docker_exit=$?
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
