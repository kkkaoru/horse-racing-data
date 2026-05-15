CREATE TABLE IF NOT EXISTS jra_track_condition_fetch_state (
  kaisai_nen TEXT NOT NULL,
  kaisai_tsukihi TEXT NOT NULL,
  keibajo_code TEXT NOT NULL,
  last_queued_at TEXT,
  last_fetch_at TEXT,
  fetch_lock_until TEXT,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (kaisai_nen, kaisai_tsukihi, keibajo_code)
);

CREATE TABLE IF NOT EXISTS jra_track_condition_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  race_key TEXT NOT NULL,
  kaisai_nen TEXT NOT NULL,
  kaisai_tsukihi TEXT NOT NULL,
  keibajo_code TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  source_updated_at TEXT,
  weather TEXT,
  turf_condition TEXT,
  turf_measurement_date TEXT,
  turf_cushion_value TEXT,
  turf_cushion_measured_at TEXT,
  turf_moisture_measured_at TEXT,
  turf_moisture_final_furlong TEXT,
  turf_moisture_final_bend TEXT,
  turf_height_japanese_zoysia_grass TEXT,
  turf_height_perennial_ryegrass TEXT,
  turf_course_layout TEXT,
  turf_going TEXT,
  dirt_condition TEXT,
  dirt_measurement_date TEXT,
  dirt_moisture_measured_at TEXT,
  dirt_moisture_final_furlong TEXT,
  dirt_moisture_final_bend TEXT,
  FOREIGN KEY (race_key) REFERENCES realtime_race_sources (race_key)
);

CREATE INDEX IF NOT EXISTS idx_jra_track_condition_fetch_state_date
  ON jra_track_condition_fetch_state (kaisai_nen, kaisai_tsukihi, keibajo_code);

CREATE INDEX IF NOT EXISTS idx_jra_track_condition_snapshots_race_time
  ON jra_track_condition_snapshots (race_key, fetched_at);

CREATE INDEX IF NOT EXISTS idx_jra_track_condition_snapshots_date_venue
  ON jra_track_condition_snapshots (kaisai_nen, kaisai_tsukihi, keibajo_code, fetched_at);
