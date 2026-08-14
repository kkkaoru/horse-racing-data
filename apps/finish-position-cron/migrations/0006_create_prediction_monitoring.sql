-- Durable queue-delivery canary records. Enqueue and consume timestamps are
-- deliberately separate so producer success can never imply consumer health.
create table if not exists finish_position_delivery_canaries (
  id text primary key,
  enqueued_at text not null,
  consumed_at text,
  delivery_lag_ms integer
);

create index if not exists finish_position_delivery_canaries_enqueued_idx
  on finish_position_delivery_canaries (enqueued_at desc);

-- Per-message lifecycle for self-heal accounting. tracking_id is generated
-- before enqueue and follows the message through queue receipt and prediction
-- completion. Rows are append/upsert-only; no destructive cleanup is required.
create table if not exists finish_position_delivery_lifecycle (
  tracking_id text primary key,
  run_ymd text not null,
  category text not null,
  keibajo_code text not null,
  race_bango text not null,
  detected_at text not null,
  enqueued_at text,
  consumed_at text,
  prediction_completed_at text,
  notified_at text
);

create index if not exists finish_position_delivery_lifecycle_race_idx
  on finish_position_delivery_lifecycle
    (run_ymd, category, keibajo_code, race_bango);
