ALTER TABLE premium_paddock_notification_state
ADD COLUMN last_payload_fetched_at TEXT;

ALTER TABLE premium_paddock_notification_state
ADD COLUMN last_send_attempt_at TEXT;

ALTER TABLE premium_paddock_notification_state
ADD COLUMN skip_reason TEXT;

CREATE INDEX IF NOT EXISTS idx_premium_paddock_notification_state_payload_fetched_at
  ON premium_paddock_notification_state (last_payload_fetched_at);
