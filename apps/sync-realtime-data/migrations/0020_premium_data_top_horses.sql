CREATE TABLE IF NOT EXISTS premium_data_top_horses (
  race_key TEXT NOT NULL,
  source_race_id TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  rank INTEGER NOT NULL,
  horse_number TEXT NOT NULL,
  horse_name TEXT,
  reasons_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (race_key, rank),
  FOREIGN KEY (race_key) REFERENCES realtime_race_sources (race_key) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_premium_data_top_horses_race_key
  ON premium_data_top_horses (race_key, rank);
