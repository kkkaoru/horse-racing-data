# Deprecated Mac launchd local/manual race-prediction tooling

> Production prediction scheduling is Cloudflare-only. These LaunchAgents are
> retained for historical reference and optional local/manual smoke operation;
> they must not be treated as production scheduler, authority, fallback, or
> ordering dependency.

Two deprecated LaunchAgents live here:

1. `com.kkk4oru.finish-position-predict` — **two** daily JST fires (not a
   single fire, despite older wording here): 03:00 (NAR + Ban-ei only, JRA
   mirror not yet available) and 09:30 (all categories, once JRA mirror +
   real advance odds are available). Both can run the legacy local Docker
   pipeline for finish-position predictions. This is not the production
   path. **DISABLED as of 2026-07-11 — see "Status" below.**
2. `com.kkk4oru.race-prediction-guard` — hourly completeness guard that
   compares D1 `realtime_race_sources` against Neon prediction tables and
   can exercise legacy local/manual recovery checks. **Still loaded**, but
   its local-Docker last-resort escalation is now off by default — see
   "Status" below.

See `apps/finish-position-predict-container/DEPLOY.md` for the architecture
backstory and the Cloudflare-Container cron-disable rationale.

## Status (2026-07-11) — Mac batch disabled, guard reduced to monitor + CF-retrigger

Production prediction generation must not run on this Mac (user directive,
2026-07-11 night, so that 2026-07-12's races are served by Cloudflare only).
This converges the actual runtime state to the CF-only policy documented in
`docs/finish-position-prediction-system.md` §1.1/§1.2 and §9 — a real local
Docker fallback had been running via both the direct launchd fires and
`race-prediction-guard.sh`'s escalation, and per the 2026-07-11
serving-latency-audit it had been load-bearing (a same-day 10:47 JST batch
run rescued 28/36 races that Cloudflare had not yet served).

- **`com.kkk4oru.finish-position-predict` — disabled.**
  `launchctl bootout gui/501/com.kkk4oru.finish-position-predict` was run
  (job was `state = not running` at the time — no in-flight run was killed),
  then the plist was moved (not deleted) from `~/Library/LaunchAgents/` to
  `~/Library/LaunchAgents.disabled-20260711/` so it cannot reload on next
  login/reboot. Neither the 03:00 nor the 09:30 fire will occur while
  disabled. **Re-enable (emergency rollback), one command:**
  ```sh
  cp ~/Library/LaunchAgents.disabled-20260711/com.kkk4oru.finish-position-predict.plist ~/Library/LaunchAgents/ && launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.kkk4oru.finish-position-predict.plist
  ```
- **`com.kkk4oru.race-prediction-guard` — still loaded, scope reduced.**
  The plist was left in place and loaded; the guard keeps doing its
  monitor duties (RS completeness checks, discover-urls re-kicks,
  corner-features prerequisite build, prewarm ticks) and keeps the
  Cloudflare-trigger escalation (`POST finish-position-cron.../run`, the
  same production trigger surface `sync-realtime-data` uses) as the sole
  fallback tier. Only the local-Docker "last resort" branch
  (`cf-trigger-failed->local` / `cf-already-tried->local`) is now gated by
  `GUARD_LOCAL_FALLBACK_ENABLED="${GUARD_LOCAL_FALLBACK_ENABLED:-0}"`
  (default off, logs `local fallback disabled by design 2026-07-11 — CF-only
architecture` and skips the exec instead of running
  `finish-position-predict-daily.sh`). Set `GUARD_LOCAL_FALLBACK_ENABLED=1`
  to restore the old behavior for an explicit emergency/manual run only —
  never as a standing default.
- **`com.kkkaoru.win5-overlay` (Sat/Sun 09:00 JST, lives in
  `apps/pc-keiba-viewer/scripts/`) is not a feature-data authority.** The
  production feature source is raw R2 Catalog data published only from local
  PostgreSQL. The plist was not installed under `~/Library/LaunchAgents/` as
  of 2026-07-11 and must not be introduced as a production dependency.

## Historical launchd rationale

This directory was created when a monolithic Cloudflare Container cron could be
idle-reaped before the DuckDB feature build completed. That rationale is
historical. Current production must remain Cloudflare-owned via Cron / Queue /
Worker / Container and `sync-realtime-data` coordination.

## Files

- `com.kkk4oru.finish-position-predict.plist` — LaunchAgent definition.
- `finish-position-predict-daily.sh` — wrapper script that runs the legacy
  local Docker pipeline (`finish-position-predict-local:split2`) once. Reads
  `NEON_DATABASE_URL` from `apps/local-postgresql/.env.replica`,
  defaults `SOURCE_DATABASE_URL` to `r2-catalog://pc-keiba`, and computes
  `RUN_DATE` as **today in JST** (`date -u -v+9H +%Y%m%d`). Catalog credentials
  are loaded from the ignored root `.env`. Missing Catalog or R2 object
  credentials stop the run; the wrapper does not fall back to local PG or Neon
  for heavy feature reads. Neon remains the prediction-result output only.

## Install

```sh
launchctl bootstrap gui/$(id -u) \
  /Users/kkk4oru/ghq/github.com/kkkaoru/horse-racing-data/scripts/launchd/com.kkk4oru.finish-position-predict.plist
```

(If you ever edit the plist, `bootout` first and `bootstrap` again — launchd
caches the loaded plist on disk.)

## Uninstall

```sh
launchctl bootout gui/$(id -u)/com.kkk4oru.finish-position-predict
```

## Status check

```sh
launchctl print gui/$(id -u)/com.kkk4oru.finish-position-predict \
  | grep -E 'state|last exit code|next firing'
```

Healthy output shows `state = waiting` and the next JST 03:00 timestamp.
`last exit code = 0` after a successful run.

## Manual fire (dry-run)

```sh
launchctl kickstart -k gui/$(id -u)/com.kkk4oru.finish-position-predict
```

Tail the logs:

```sh
tail -f /Users/kkk4oru/Library/Logs/finish-position-predict/*.log
```

The script runs for ~3-5 min (per the proven 06-03 / 06-04 local runs: ~514
preds in 3.5 min and ~525 preds in 3.6 min). It is idempotent — UPSERTs into
`race_finish_position_model_predictions` — so re-firing on the same day just
overwrites the predictions for that date.

## Logs

- `~/Library/Logs/finish-position-predict/stdout.log` — raw stdout (rotates
  manually if you care).
- `~/Library/Logs/finish-position-predict/stderr.log` — raw stderr.
- `~/Library/Logs/finish-position-predict/YYYYMMDD.log` — per-run dated log
  (the wrapper tees its output here). Credentials are masked.
- `~/Library/Logs/finish-position-predict/failures.log` — one-line failure
  records (`RUN_DATE`, status, message). Useful for grep after a sleepy week.

## Mac sleep behaviour

`StartCalendarInterval` is launchd's "fire at a wall-clock time" mechanism.
The relevant catch-up rule:

- If the Mac is **awake** at JST 03:00, the job fires on time.
- If the Mac is **asleep** at JST 03:00, launchd queues a missed-firing event
  and fires it on next wake (this is the documented "catch up on missed runs"
  behaviour for `StartCalendarInterval`).
- If the Mac is **off** at JST 03:00, the missed firing is **lost** — there
  is no persistent queue across power-off. Run the manual fire above when you
  power back on if you need a same-day prediction.
- `RunAtLoad = false` means the job will NOT fire when the LaunchAgent itself
  is loaded (e.g. after a reboot + login). Combined with the catch-up rule,
  this means: a reboot before JST 03:00 has no extra firing; a reboot after
  JST 03:00 fires once (catch-up) then waits for the next JST 03:00.

The wrapper's UPSERT is idempotent, so a delayed fire is harmless.

## Timezone caveat

`StartCalendarInterval` uses the system's local timezone (no `TZ` field
exists for launchd plists). This plist assumes the Mac is configured to
**Asia/Tokyo (JST)**. If you ever change the system timezone, edit the
`Hour` integer in the plist accordingly. The wrapper script computes
`RUN_DATE` from UTC+9 directly (`date -u -v+9H`), so the **run date** is
correct regardless of system TZ; only the **firing time** depends on it.

## Issue references

- Issue #114 (Cloudflare Container reap window — closed by this change).
- The Cloudflare Worker for `/run` + `/health` + D1 audit remains deployed
  (see `apps/finish-position-cron/wrangler.jsonc`) and can be used to trigger
  ad-hoc runs against the still-live Container code path (subject to the same
  ~90-110 s reap — only useful for very short test runs).

---

# `com.kkk4oru.race-prediction-guard` — hourly completeness guard

The second LaunchAgent (`com.kkk4oru.race-prediction-guard.plist` +
`race-prediction-guard.sh`) fires 15 times per JST day:

| Fires (Hour:00 JST) | Target dates guarded     | Notes                                                                                       |
| ------------------- | ------------------------ | ------------------------------------------------------------------------------------------- |
| 00, 01, 02, ... 09  | TODAY JST                | `PREDICT_DAYS_AHEAD=0`                                                                      |
| 19, 20              | TODAY JST                | Evening top-up for late NAR / Ban-ei rerun after lineup updates                             |
| 21, 22, 23          | TODAY JST + TOMORROW JST | Pre-warm + final today catch-up. TOMORROW uses `PREDICT_DAYS_AHEAD=1` + `RUN_DATE=tomorrow` |

Per tick:

1. Take a single-writer lock at `/tmp/race-prediction-guard.lock` (atomic
   `mkdir`); exit 0 if already held by another guard.
2. Resolve the target dates based on the JST hour (see table above). The hour
   is read live but `DRY_RUN=1` accepts a `FORCE_HOUR` override for testing.
   `DRY_RUN=1 FORCE_TARGET_DATE=YYYYMMDD` additionally overrides both the
   today and tomorrow dates so the empty-D1 discover-urls path can be
   exercised deterministically.
3. For each target date, run `guard_target`:
   - Query Cloudflare D1 `sync-realtime-data.realtime_race_sources` for
     `COUNT(DISTINCT race_key) WHERE substr(race_start_at_jst,1,10)='YYYY-MM-DD'`.
   - **If the D1 count is 0**, POST `{"type":"discover-urls","date":"YYYYMMDD"}`
     to `https://sync-realtime-data.kkk4oru.com/api/jobs` and stop processing
     that target — the worker upsert is naturally idempotent and the next
     hourly tick will see the freshly-discovered rows and proceed with
     predictions. (In DRY_RUN, only the planned POST is logged.)
   - **If the D1 count is > 0**, run a per-venue coverage check: GROUP BY
     `keibajo_code` on `realtime_race_sources` for the JST date and compare
     each row against the per-venue lower bounds.
     NAR major venues (`30 35 36 42 43 44 46 47 48 50 51 53 54 55 56 57 65 66`)
     use `EXPECTED_NAR_RACES_PER_VENUE` (10). Typical day is 10-12 races;
     under-10 is partial-coverage. This is the check that would have caught
     today's incident: 大井 (44) at 7 races was below the threshold and the
     guard would have re-kicked discover-urls.
     JRA major venues (`01 02 03 04 05 06 07 08 09 10`) use
     `EXPECTED_JRA_RACES_PER_VENUE` (11). JRA always runs 12 race cards, so
     anything below 11 is incomplete. Unknown / non-major `keibajo_code` rows
     do not trigger a re-kick.
     If ANY listed major venue is under threshold the guard logs a
     `WARN per-venue coverage[$label] INCOMPLETE` line and POSTs
     `discover-urls` again. The worker UPSERT is idempotent so re-discovery
     is cheap. The per-venue check runs independently from the running-style
     and finish-position checks below: even when it re-kicks, the downstream
     prediction checks still proceed so partial predictions still go through.
     The next hourly tick re-evaluates.
   - **Feature prerequisite.** Current production jobs read raw Iceberg tables
     through `pc-keiba-r2-catalog`. They do not query or rebuild Neon
     `race_entry_corner_features`. A Catalog read failure stops the job; the
     guard must not repair it from Neon, D1, or an older feature Parquet.
   - Else query Neon for
     `COUNT(DISTINCT (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango))`
     in each predictions table for that JST date. If `actual < expected`,
     kick:
     - **running-style (脚質):** `POST` to
       `https://sync-realtime-data.kkk4oru.com/api/jobs` with
       `{"type":"plan-running-style-predictions","date":"$TARGET_DATE"}` and the
       `REALTIME_ADMIN_TOKEN` from `apps/sync-realtime-data/.dev.vars`.
     - **finish-position (着順, deprecated local/manual path):** `RUN_DATE=$TARGET_DATE PREDICT_DAYS_AHEAD=$TARGET_DAYS_AHEAD bash scripts/launchd/finish-position-predict-daily.sh`.
       If `/tmp/finish-position-predict.lock` is already held (by the JST
       03:00 local fire or a previous guard fire), the kick is skipped this tick.

### Idempotency

Neon prediction tables are the source of truth. Once a previous kick has
filled the rows, the next tick re-evaluates, sees `actual >= expected`, and
exits without kicking. No state file is needed.

### Locks used

- `/tmp/race-prediction-guard.lock` — guard-level single-writer lock.
- `/tmp/finish-position-predict.lock` — **shared** with
  `finish-position-predict-daily.sh` so local/manual fires and any guard tick
  can't run two Docker pipelines concurrently.

### Install

```sh
cp scripts/launchd/com.kkk4oru.race-prediction-guard.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.kkk4oru.race-prediction-guard.plist
```

If you previously installed an older version of this plist (e.g. the
13-fire variant) you MUST `bootout` and `bootstrap` again so launchd
picks up the new `StartCalendarInterval` array:

```sh
launchctl bootout gui/$(id -u)/com.kkk4oru.race-prediction-guard 2>/dev/null || true
cp scripts/launchd/com.kkk4oru.race-prediction-guard.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.kkk4oru.race-prediction-guard.plist
```

(The companion `finish-position-predict-daily.sh` gained a lock at the top
but no behavior change for the existing 03:00 fire — you do **not** need to
re-bootstrap the existing `com.kkk4oru.finish-position-predict` plist.)

### Uninstall

```sh
launchctl bootout gui/$(id -u)/com.kkk4oru.race-prediction-guard
```

### Manual fire / dry-run

```sh
# Real fire (writes to Neon if incomplete)
launchctl kickstart -k gui/$(id -u)/com.kkk4oru.race-prediction-guard

# Dry-run that only prints planned kicks
DRY_RUN=1 bash scripts/launchd/race-prediction-guard.sh

# Dry-run forcing the "today only" morning band (hour 0..9)
DRY_RUN=1 FORCE_HOUR=05 bash scripts/launchd/race-prediction-guard.sh

# Dry-run forcing the "today only" evening top-up band (hour 19..20)
DRY_RUN=1 FORCE_HOUR=19 bash scripts/launchd/race-prediction-guard.sh

# Dry-run forcing the "today + tomorrow" pre-warm band (hour 21..23)
DRY_RUN=1 FORCE_HOUR=22 bash scripts/launchd/race-prediction-guard.sh

# Dry-run that exercises the empty-D1 discover-urls kick path
DRY_RUN=1 FORCE_HOUR=22 FORCE_TARGET_DATE=20300101 \
  bash scripts/launchd/race-prediction-guard.sh

# Dry-run that exercises the corner-features build path
DRY_RUN=1 FORCE_HOUR=05 FORCE_NO_CORNER_FEATURES=1 \
  bash scripts/launchd/race-prediction-guard.sh

# Dry-run that exercises the per-venue coverage check path.
# FORCE_VENUE_COUNTS=keibajo:count[,keibajo:count...] feeds synthetic D1
# venue counts; FORCE_EXPECTED_COUNT=N bypasses the EXPECTED_COUNT=0 early
# return so per-venue evaluation actually runs. The example below mirrors
# the 2026-06-09 incident: 大井 (44) at 7 races + JRA 札幌 (05) at 8 races.
DRY_RUN=1 FORCE_HOUR=05 FORCE_TARGET_DATE=20300101 \
  FORCE_EXPECTED_COUNT=42 FORCE_VENUE_COUNTS=44:7,30:12,36:11,05:8 \
  bash scripts/launchd/race-prediction-guard.sh
```

### Logs

- `~/Library/Logs/race-prediction-guard/stdout.log` / `stderr.log` — raw streams.
- `~/Library/Logs/race-prediction-guard/YYYYMMDD.log` — per-JST-day dated log
  (the wrapper tees its output here). Credentials are masked.
- `~/Library/Logs/race-prediction-guard/lock-skips.log` — one line per tick
  where the guard-lock was held (concurrent guard).

### Timezone caveat

`StartCalendarInterval` uses the system's local timezone (no `TZ` field
exists for launchd plists). This plist assumes the Mac is configured to
**Asia/Tokyo (JST)**. If you change the system timezone, edit each `Hour`
integer in the plist accordingly. The wrapper computes `TARGET_DATE` from
UTC+9 directly (`date -u -v+9H`), so the _which-day-to-guard_ logic is
robust to TZ drift; only the _when-to-fire_ depends on the system TZ.
