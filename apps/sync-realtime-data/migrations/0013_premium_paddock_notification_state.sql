CREATE TABLE IF NOT EXISTS premium_paddock_notification_state (
  race_key TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'idle',
  payload_signature TEXT,
  last_notified_at TEXT,
  message TEXT,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (race_key) REFERENCES realtime_race_sources (race_key) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_premium_paddock_notification_state_updated_at
  ON premium_paddock_notification_state (updated_at);
