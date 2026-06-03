-- finish_position_cron_executions: one row per cron-triggered prediction run.
-- Insert-only audit (feedback_no_data_delete) — never DELETE / TRUNCATE / DROP.
create table if not exists finish_position_cron_executions (
  id integer primary key autoincrement,
  run_date text not null,
  status text not null,
  races_predicted integer not null,
  duration_ms integer not null,
  error text,
  recorded_at text not null default (datetime('now'))
);

create index if not exists finish_position_cron_executions_run_date_idx
  on finish_position_cron_executions (run_date);
