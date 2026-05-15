CREATE TABLE IF NOT EXISTS premium_race_data_fetch_state (
  race_key TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'idle',
  message TEXT,
  last_queued_at TEXT,
  last_fetch_at TEXT,
  fetch_lock_until TEXT,
  retry_after TEXT,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (race_key) REFERENCES realtime_race_sources (race_key) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_premium_race_data_fetch_state_retry_after
  ON premium_race_data_fetch_state (retry_after);
