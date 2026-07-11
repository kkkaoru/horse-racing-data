#!/usr/bin/env bash
# Daily MLflow production-usage sync + champion cell-eval + per-cell-run runner.
#
# WHAT THIS RUNS:
#   0. `backfill-finish-position --allow-missing-champion` to self-heal any
#      champion-alias drift left over from a prior model deploy, so step 1's
#      champion-lookup logic always sees fresh registry state. Best-effort
#      only: a failure here WARNs and does not block steps 1/2/3 (see the
#      `|| echo "WARNING: ..."` guard below).
#   1. `sync-production` over a "yesterday -> today" JST window, so a
#      genuinely-served prediction logged just before local midnight is not
#      missed by a same-day-only range. This step ALSO runs a startup
#      self-heal sweep by default (2026-07-11, see sync_production.py's own
#      module docstring's "SELF-HEAL" section): any run left stuck
#      status=RUNNING by a previously-interrupted invocation (killed
#      mid-loop, e.g. a Mac sleep/crash) for longer than
#      --stale-running-hours (default 6h) is force-terminated FINISHED
#      before this call's own date/category loop starts. No flag needed
#      here to opt in -- pass --no-repair-stale-running to disable it for a
#      one-off manual invocation if ever needed.
#   1b. `cf_serving_recorder` (a module, not a `cli` subcommand -- see that
#      module's own docstring) over the SAME "yesterday -> today" JST
#      window, for jra/nar/banei: records CF-container finish-position
#      serving PROCESSING (routing mix, coverage against the local race
#      calendar, late/missing/batch-burst/partial-write anomaly counts) into
#      `finish-position/cf-serving` -- distinct from step 1's accuracy-
#      focused production-usage sync. ONE cheap call per (date, category), a
#      handful of Neon/local-replica queries with no heavy joins, so this
#      command is also safe for a SEPARATE, more-frequent "monitoring
#      sibling" cron to re-run intraday during race hours (each call
#      REPLACES that day's snapshot in place -- see the module's own
#      idempotency docstring); this script's once-daily 22:30 JST
#      invocation is a baseline floor, not the only expected invocation.
#   2. `eval-champion-cells` for all three categories (default trailing
#      90-day window as of today), re-scoring each category's CURRENT
#      champion model at cell granularity against whatever genuinely-served
#      predictions now have finalized results. Logs ONE run per (category,
#      task), with the per-cell breakdown as a table INSIDE that run.
#   2b. `eval-champion-cells --window-days 60` for all three categories, the
#      same command as step 2 but with an explicit trailing 60-day window
#      instead of the 90-day default. This is a SEPARATE (category, task,
#      window_days, as_of_date) idempotency key (see champion_cell_eval.py's
#      cell_eval_key), so it logs its own run alongside step 2's 90-day run
#      rather than colliding with or replacing it -- both windows accumulate
#      daily, giving a shorter/fresher trailing-window view (e.g. for
#      spotting a recent serving-coverage gap sooner) next to the existing
#      longer one.
#   2c. `eval-champion-cells --window-days 3650` (REALIZED "all-period" view,
#      ~10y, comfortably covering the full genuinely-served production
#      history without an unbounded/open-ended window) for all three
#      categories, but ONLY on Sundays (TZ=Asia/Tokyo `%u` == 7) -- this is
#      a materially heavier query than steps 2/2b (it scans every
#      genuinely-served row ever logged, not just a trailing window, joining
#      finalized results per distinct date touched), so per the standing
#      Neon-cost-consciousness rule it runs weekly, not daily, mirroring the
#      "daily 60d + weekly all-period" cadence agreed for this cell-eval
#      recording. Same idempotency key mechanism as 2b (window_days=3650 is
#      simply a different cell_eval_key), so a catch-up fire after a missed
#      Sunday (Mac asleep, see IDEMPOTENCY section below) is harmless -- it
#      just logs that day's all-period run a bit late, same as any other
#      launchd catch-up.
#   3. `eval-cells` for all three categories (default trailing 90-day
#      window, default --min-races 20): scores EVERY model_version that
#      served enough volume this window -- champion, every cell-routed
#      variant, and any other version, not just the champion -- at cell
#      granularity. Unlike step 2's one-run-per-(category,task) table, this
#      logs one PERSISTENT, individually drillable MLflow run PER (category,
#      cell, model_version), appending one metric point per day this script
#      runs, so a single cell's accuracy trend over time renders as a real
#      line chart in the MLflow UI. See cell_eval_runs.py's own module
#      docstring for the full design (metric names, the cell_run_key
#      idempotency tag, the volume-guard cap on runs-per-invocation, ...).
#
# WHY 22:30 JST:
#   Same-day JRA/NAR/Ban-ei racing has finished by then, and results have
#   typically already mirrored into the local PostgreSQL replica that
#   sync-production / eval-champion-cells / eval-cells all read for
#   finalized-result joins (see docs/mlflow-tracking.md SS11/SS13). Running
#   before results land just means the day's production-usage rows get
#   logged without an evaluation yet -- sync-production's sync_eval_logged
#   tag stays unset and a later call (tomorrow's run, which re-covers
#   yesterday) fills it in once results are final. Nothing is lost by an
#   early or late fire.
#
# IDEMPOTENCY:
#   Every underlying command is safe to re-run. sync-production is
#   gated by per-(date, category, model_version) sync_base_logged /
#   sync_eval_logged tags, so re-logging the same range never duplicates
#   base tracking and only retries evaluation while it's still unfilled.
#   cf_serving_recorder is gated by a (date, category) cf_serving_key tag,
#   but UNLIKE the tag-gated commands below it does NOT skip re-work when
#   found -- every call REPLACES that run's metrics/table with today's
#   current state (see that module's own docstring on update-in-place
#   idempotency, the deliberate design choice for a value meant to be
#   re-read many times through race hours).
#   eval-champion-cells is gated by a (category, task, window_days,
#   as_of_date) cell_eval_key tag, so a same-day re-run is a cheap no-op
#   that reuses the existing run's summary instead of re-querying.
#   eval-cells is gated by a (category, cell, model_version, window_days)
#   cell_run_key tag PER RUN, with each metric point additionally deduped by
#   day (mirroring timelines.py's own step-dedup idiom) -- so a same-day
#   re-run finds every already-existing run and appends nothing new to any
#   of them. This means a launchd catch-up fire after the Mac was asleep at
#   22:30 (see launchd's StartCalendarInterval "fire on next wake" behavior)
#   is harmless -- it just repeats or extends today's already-safe work.
#
# NO SECRETS HERE:
#   Neither this script nor its paired plist
#   (com.horse-racing.mlflow-production-sync.plist) sets or reads any
#   credential. `mlflow_tracking.cli`'s own main() already calls
#   config.load_dotenv_local() (reads apps/mlflow/.env.local) and then
#   config.load_repo_root_env_fallback() (reads a narrow allow-listed subset
#   of the repo-root .env) before argparse even runs -- see that function's
#   own module docstring and docs/mlflow-tracking.md SS2.1. Those two loaders
#   exist specifically so this CLI resolves its backend URI / R2 settings
#   correctly under launchd, which -- unlike an interactive shell -- never
#   sources direnv's dotenv hook. This script therefore has nothing secret
#   to carry and carries nothing.
set -euo pipefail

# Resolve repo root from this script's own location: apps/mlflow/scripts/launchd
# is 4 levels under the repo root (apps -> mlflow -> scripts -> launchd).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

MLFLOW_DIR="$REPO_ROOT/apps/mlflow"
# launchd invokes this script with a minimal PATH (no interactive-shell
# Homebrew setup), so `uv` is addressed by its literal absolute path rather
# than relying on `command -v uv` -- same rationale as the reference
# finish-position wrapper's hardcoded /opt/homebrew/bin PATH entry.
UV_BIN="/opt/homebrew/bin/uv"

cd "$MLFLOW_DIR"

# JST = UTC+9. BSD `date` (macOS) with an explicit TZ gives today/yesterday
# in Asia/Tokyo regardless of the Mac's system timezone.
TODAY_JST="$(TZ=Asia/Tokyo date +%Y%m%d)"
YESTERDAY_JST="$(TZ=Asia/Tokyo date -v-1d +%Y%m%d)"

echo "=== mlflow-production-sync $(date -u +%Y-%m-%dT%H:%M:%SZ) (JST today=$TODAY_JST yesterday=$YESTERDAY_JST) ==="

echo "--- backfill-finish-position --allow-missing-champion ---"
"$UV_BIN" run python -m mlflow_tracking.cli backfill-finish-position --allow-missing-champion \
  || echo "WARNING: backfill-finish-position failed (continuing to sync-production)"

echo "--- sync-production --date-from $YESTERDAY_JST --date-to $TODAY_JST --categories jra,nar,banei ---"
"$UV_BIN" run python -m mlflow_tracking.cli sync-production \
  --date-from "$YESTERDAY_JST" --date-to "$TODAY_JST" --categories jra,nar,banei

echo "--- cf_serving_recorder --date-from $YESTERDAY_JST --date-to $TODAY_JST --categories jra,nar,banei ---"
"$UV_BIN" run python -m mlflow_tracking.cf_serving_recorder \
  --date-from "$YESTERDAY_JST" --date-to "$TODAY_JST" --categories jra,nar,banei

echo "--- eval-champion-cells --category jra,nar,banei ---"
"$UV_BIN" run python -m mlflow_tracking.cli eval-champion-cells --category jra,nar,banei

echo "--- eval-champion-cells --category jra,nar,banei --window-days 60 ---"
"$UV_BIN" run python -m mlflow_tracking.cli eval-champion-cells --category jra,nar,banei --window-days 60

# Weekly (Sunday, JST) REALIZED "all-period" cell-eval -- see step 2c's
# header comment above for why this is gated to once a week rather than run
# alongside steps 2/2b every day.
DOW_JST="$(TZ=Asia/Tokyo date +%u)"
if [ "$DOW_JST" = "7" ]; then
  echo "--- eval-champion-cells --category jra,nar,banei --window-days 3650 (Sunday all-period) ---"
  "$UV_BIN" run python -m mlflow_tracking.cli eval-champion-cells --category jra,nar,banei --window-days 3650
else
  echo "--- skipping all-period eval-champion-cells (DOW_JST=$DOW_JST, runs Sundays only) ---"
fi

echo "--- eval-cells --category jra,nar,banei ---"
"$UV_BIN" run python -m mlflow_tracking.cli eval-cells --category jra,nar,banei

echo "=== done ==="
