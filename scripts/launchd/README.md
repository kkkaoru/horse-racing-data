# Mac launchd cron for daily finish-position prediction

This LaunchAgent replaces the disabled Cloudflare Container cron. See
`apps/finish-position-predict-container/DEPLOY.md` for the architecture
backstory and the cron-disable rationale.

## Why launchd instead of Cloudflare Cron

Cloudflare Containers reap batch instances at ~90-110 s regardless of
`sleepAfter`, but the DuckDB feature build + per-category CatBoost / XGBoost
scoring needs ~10 min. The cron in
`apps/finish-position-cron/wrangler.jsonc` is therefore set to `triggers.crons
= []` (cron disabled) and the daily run is driven by the LaunchAgent below.
The Worker is still deployed for the `/run` on-demand HTTP endpoint, `/health`,
and the D1 audit table — those are unaffected.

## Files

- `com.kkk4oru.finish-position-predict.plist` — LaunchAgent definition.
- `finish-position-predict-daily.sh` — wrapper script that runs the proven
  local docker pipeline (`finish-position-predict-local:split2`) once. Reads
  `NEON_DATABASE_URL` from `apps/local-postgresql/.env.replica`,
  defaults `SOURCE_DATABASE_URL` to `postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing`,
  and computes `RUN_DATE` as **today in JST** (`date -u -v+9H +%Y%m%d`).

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
