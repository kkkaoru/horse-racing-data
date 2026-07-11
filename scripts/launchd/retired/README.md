# Retired launchd jobs — legacy D1 delete chunkers

These two LaunchAgents are retired as of **2026-07-11** (user approval, same
day). Their plist files here are the committed record; secret env values
(`PC_KEIBA_VIEWER_INTERNAL_TOKEN`, `REALTIME_ADMIN_TOKEN`) are redacted —
following the same "no secrets in tracked plists" convention as
`scripts/launchd/com.kkk4oru.finish-position-predict.plist`. Full plists
(with real secret values) were moved to
`~/Library/LaunchAgents.disabled-20260711/` on the local Mac, not deleted.

## What they did

1. `com.kkk4oru.horse-racing.delete-daily-race-entries` — daily 23:15 JST
   fire that ran `apps/sync-realtime-data/scripts/run-delete-daily-race-entries-chunk.ts`
   in chunks of 100 rows every 10s against the legacy D1 `daily_race_entries`
   table, migrating it off D1 after the R2 Parquet cutover.
2. `com.kkk4oru.horse-racing.delete-race-running-styles` — daily 01:15 JST
   fire that ran `apps/sync-realtime-data/scripts/run-delete-race-running-styles-chunk.ts`,
   the equivalent chunked delete for the legacy `race_running_styles` D1
   table.

Both were part of the Phase F full-period D1 cleanup approved on 2026-05-29
(see repo memory `feedback_phase_f_full_delete_2026_05_29`) — a one-time
migration off D1, not a recurring retention policy.

## Why retired

- The underlying migration completed weeks before retirement — both jobs
  had been observing `deleted=0` rows per run for a long time (nothing left
  to delete).
- Both jobs had been failing every run with HTTP 403 for ~5 weeks, broken by
  the 2026-07-04 token rotation (see repo memory
  `project_display_pipeline_incident_2026_07_04`), and were never repaired
  because there was no longer any data left to migrate.
- User approved retirement on 2026-07-11.

## Re-enable procedure (if ever needed)

1. Restore the real plist (with live token values) from
   `~/Library/LaunchAgents.disabled-20260711/<label>.plist` back to
   `~/Library/LaunchAgents/`. If that local backup is gone, recreate it from
   this directory's redacted copy and refill
   `PC_KEIBA_VIEWER_INTERNAL_TOKEN` / `REALTIME_ADMIN_TOKEN` from the current
   secret store.
2. Confirm the target D1 tables/scripts still exist — `daily_race_entries`
   and `race_running_styles` reads are banned repo-wide per
   `feedback_no_legacy_daily_race_entries_read`; re-enabling only makes
   sense if a new migration or cleanup pass is explicitly approved.
3. `launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/<label>.plist`
4. Verify with `launchctl list | grep <label>`.

Do not re-enable as a recurring retention job — the repo-wide rule is no
data deletion / retention crons (`feedback_no_data_delete`,
`feedback_no_odds_snapshot_delete`). Any future use of these scripts must be
a one-time, user-approved, bounded migration like the original Phase F pass.
