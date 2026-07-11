-- finish_position_predict_dlq_events: one row per predict-queue message that
-- exhausted max_retries and landed in the finish-position-predict-dlq queue.
-- Before this table existed, the dead-letter queue had no consumer at all,
-- so a genuinely dead focused-full pipeline (or any other predict message)
-- left zero durable trace once it was dead-lettered. Insert-only audit
-- (feedback_no_data_delete) -- never DELETE / TRUNCATE / DROP.
create table if not exists finish_position_predict_dlq_events (
  id integer primary key autoincrement,
  run_ymd text not null,
  category text not null,
  mode text not null,
  keibajo_code text,
  race_bango text,
  redrive_count integer not null,
  redriven integer not null,
  recorded_at text not null default (datetime('now'))
);

create index if not exists finish_position_predict_dlq_events_run_ymd_idx
  on finish_position_predict_dlq_events (run_ymd);
