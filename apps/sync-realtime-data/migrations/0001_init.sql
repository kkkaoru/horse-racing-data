CREATE TABLE IF NOT EXISTS nar_race_sources (
  race_key TEXT PRIMARY KEY,
  source TEXT NOT NULL DEFAULT 'nar',
  kaisai_nen TEXT NOT NULL,
  kaisai_tsukihi TEXT NOT NULL,
  keibajo_code TEXT NOT NULL,
  race_bango TEXT NOT NULL,
  baba_code TEXT NOT NULL,
  race_start_at_jst TEXT NOT NULL,
  race_name TEXT,
  deba_url TEXT NOT NULL,
  odds_links_json TEXT NOT NULL DEFAULT '{}',
  discovered_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  last_odds_fetch_at TEXT,
  last_weight_fetch_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_nar_race_sources_date_start
  ON nar_race_sources (kaisai_nen, kaisai_tsukihi, race_start_at_jst);

CREATE TABLE IF NOT EXISTS odds_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  race_key TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  odds_type TEXT NOT NULL,
  combination TEXT NOT NULL,
  odds REAL,
  min_odds REAL,
  max_odds REAL,
  average_odds REAL,
  rank INTEGER,
  FOREIGN KEY (race_key) REFERENCES nar_race_sources (race_key)
);

CREATE INDEX IF NOT EXISTS idx_odds_snapshots_race_type_time
  ON odds_snapshots (race_key, odds_type, fetched_at);

CREATE INDEX IF NOT EXISTS idx_odds_snapshots_race_type_combination_time
  ON odds_snapshots (race_key, odds_type, combination, fetched_at);

CREATE TABLE IF NOT EXISTS horse_weight_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  race_key TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  horse_number TEXT NOT NULL,
  horse_name TEXT,
  weight INTEGER,
  change_sign TEXT,
  change_amount INTEGER,
  FOREIGN KEY (race_key) REFERENCES nar_race_sources (race_key)
);

CREATE INDEX IF NOT EXISTS idx_horse_weight_snapshots_race_time
  ON horse_weight_snapshots (race_key, fetched_at);

CREATE TABLE IF NOT EXISTS fetch_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  race_key TEXT,
  job_type TEXT NOT NULL,
  status TEXT NOT NULL,
  message TEXT,
  created_at TEXT NOT NULL
);
