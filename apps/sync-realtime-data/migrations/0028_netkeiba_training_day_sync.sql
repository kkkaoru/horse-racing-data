CREATE TABLE IF NOT EXISTS netkeiba_training_day_sync_state (
  race_date TEXT PRIMARY KEY CHECK (length(race_date) = 8),
  status TEXT NOT NULL CHECK (status IN ('processing', 'catalog_pending', 'succeeded', 'failed')),
  catalog_run_id TEXT,
  workout_count INTEGER NOT NULL DEFAULT 0,
  error_message TEXT,
  attempted_at TEXT NOT NULL,
  completed_at TEXT
);
