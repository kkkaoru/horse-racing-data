-- Per-delivery timing breadcrumbs.  Full messages are the pre-weight pass;
-- rescore messages are deliberately kept out of the timing calculations.
-- Existing lifecycle rows remain valid with NULL values in the new columns.
alter table finish_position_delivery_lifecycle add column mode text;
alter table finish_position_delivery_lifecycle add column generation_started_at text;
alter table finish_position_delivery_lifecycle add column generation_duration_ms integer;
alter table finish_position_delivery_lifecycle add column queue_to_generation_start_ms integer;
alter table finish_position_delivery_lifecycle add column kv_display_completed_at text;
alter table finish_position_delivery_lifecycle add column generation_to_display_ms integer;
alter table finish_position_delivery_lifecycle add column enqueue_to_display_ms integer;

create index if not exists finish_position_delivery_lifecycle_preweight_idx
  on finish_position_delivery_lifecycle
    (run_ymd, category, keibajo_code, race_bango, mode, enqueued_at);
