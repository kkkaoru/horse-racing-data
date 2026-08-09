-- Persist the failure that exhausted primary-queue max_retries onto
-- finish_position_predict_dlq_events, and keep a per-retry breadcrumb table
-- so the DLQ consumer can copy the last error even though Cloudflare Queues
-- message.retry() cannot mutate the original body.
-- ADD COLUMN / CREATE TABLE only -- never DELETE / TRUNCATE / DROP
-- (feedback_no_data_delete). Existing dlq_events rows stay; new columns are
-- nullable so historical rows remain valid.

alter table finish_position_predict_dlq_events add column error_name text;
alter table finish_position_predict_dlq_events add column error_message text;
alter table finish_position_predict_dlq_events add column error_stack text;
alter table finish_position_predict_dlq_events add column http_status integer;
alter table finish_position_predict_dlq_events add column http_body_excerpt text;
alter table finish_position_predict_dlq_events add column queue_attempts integer;

create table if not exists finish_position_predict_retry_errors (
  id integer primary key autoincrement,
  queue_message_id text,
  run_ymd text not null,
  category text not null,
  mode text not null,
  keibajo_code text,
  race_bango text,
  error_name text,
  error_message text,
  error_stack text,
  http_status integer,
  http_body_excerpt text,
  queue_attempts integer,
  recorded_at text not null default (datetime('now'))
);

create index if not exists finish_position_predict_retry_errors_message_id_idx
  on finish_position_predict_retry_errors (queue_message_id);

create index if not exists finish_position_predict_retry_errors_race_idx
  on finish_position_predict_retry_errors (run_ymd, category, mode, keibajo_code, race_bango);
