CREATE TABLE IF NOT EXISTS premium_paddock_notification_events (
  race_key TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  payload_signature TEXT NOT NULL,
  status TEXT NOT NULL,
  skip_reason TEXT,
  message TEXT,
  sent_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (race_key, fetched_at),
  FOREIGN KEY (race_key) REFERENCES realtime_race_sources (race_key) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_premium_paddock_notification_events_status
  ON premium_paddock_notification_events (status, updated_at);
