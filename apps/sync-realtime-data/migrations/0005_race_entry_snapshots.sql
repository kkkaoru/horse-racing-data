CREATE TABLE IF NOT EXISTS race_entry_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  race_key TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  horse_number TEXT NOT NULL,
  horse_name TEXT,
  jockey_name TEXT,
  status TEXT,
  FOREIGN KEY (race_key) REFERENCES nar_race_sources (race_key)
);

CREATE INDEX IF NOT EXISTS idx_race_entry_snapshots_race_time
  ON race_entry_snapshots (race_key, fetched_at);
