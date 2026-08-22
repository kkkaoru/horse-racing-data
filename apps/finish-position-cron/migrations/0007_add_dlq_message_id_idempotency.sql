-- Preserve the Cloudflare Queues message identity on DLQ audit rows so a
-- consumer retry cannot record the same delivery/redrive stage more than once.
-- Historical rows remain untouched with queue_message_id = NULL. The partial
-- unique index therefore protects new identified messages without deleting or
-- rewriting existing audit data.

alter table finish_position_predict_dlq_events add column queue_message_id text;

create unique index if not exists finish_position_predict_dlq_events_message_redrive_uidx
  on finish_position_predict_dlq_events (queue_message_id, redrive_count)
  where queue_message_id is not null;
