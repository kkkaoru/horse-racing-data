ALTER TABLE nar_race_sources ADD COLUMN last_result_fetch_at TEXT;
ALTER TABLE nar_race_sources ADD COLUMN last_result_queued_at TEXT;
ALTER TABLE nar_race_sources ADD COLUMN result_fetch_lock_until TEXT;

CREATE TABLE IF NOT EXISTS race_result_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  race_key TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  horse_number TEXT NOT NULL,
  horse_name TEXT,
  finish_position TEXT NOT NULL,
  time TEXT,
  FOREIGN KEY (race_key) REFERENCES nar_race_sources (race_key)
);

CREATE INDEX IF NOT EXISTS idx_race_result_snapshots_race_time
  ON race_result_snapshots (race_key, fetched_at);
