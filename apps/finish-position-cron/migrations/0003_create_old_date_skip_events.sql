-- finish_position_old_date_skip_events: one row per predict-queue message
-- whose runYmd was too old (see OLD_DATE_THRESHOLD_DAYS,
-- src/old-date-guard.ts) to dispatch to the Container. Past/historical
-- races do not need CF prediction generation at all -- that is Mac-local
-- backfill territory -- so a zombie queued message (retry, DLQ redrive, or
-- a stale manual trigger) carrying a runYmd far in the past is acked
-- without ever reaching the Container, instead of wasting a 10-27 minute
-- pipeline run on a race that has long since settled. Insert-only audit
-- (feedback_no_data_delete) -- never DELETE / TRUNCATE / DROP.
create table if not exists finish_position_old_date_skip_events (
  id integer primary key autoincrement,
  run_ymd text not null,
  category text not null,
  mode text not null,
  keibajo_code text,
  race_bango text,
  threshold_days integer not null,
  recorded_at text not null default (datetime('now'))
);

create index if not exists finish_position_old_date_skip_events_run_ymd_idx
  on finish_position_old_date_skip_events (run_ymd);
