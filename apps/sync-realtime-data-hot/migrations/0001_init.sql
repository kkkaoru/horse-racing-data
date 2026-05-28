-- sync-realtime-data-hot 0001 initial schema
-- odds_snapshots: high-frequency odds polling output (1.2M-3.6M rows/day)
-- odds_fetch_state: planner state, race_key PK
-- fetch_logs: audit log for fetch jobs

CREATE TABLE odds_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  race_key TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  odds_type TEXT NOT NULL,
  combination TEXT NOT NULL,
  odds REAL,
  min_odds REAL,
  max_odds REAL,
  average_odds REAL,
  rank INTEGER
);

CREATE INDEX idx_odds_snapshots_race_type_time
  ON odds_snapshots (race_key, odds_type, fetched_at);

CREATE INDEX idx_odds_snapshots_race_key_fetched_at
  ON odds_snapshots (race_key, fetched_at);

CREATE INDEX idx_odds_snapshots_race_type_time_rank_comb
  ON odds_snapshots (race_key, odds_type, fetched_at, rank, combination);

CREATE TABLE odds_fetch_state (
  race_key TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  race_start_at_jst TEXT NOT NULL,
  deba_url TEXT NOT NULL,
  odds_links_json TEXT NOT NULL DEFAULT '{}',
  last_odds_fetch_at TEXT,
  last_odds_queued_at TEXT,
  odds_fetch_lock_until TEXT,
  kaisai_nen TEXT NOT NULL,
  kaisai_tsukihi TEXT NOT NULL,
  keibajo_code TEXT NOT NULL,
  race_bango TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX idx_odds_fetch_state_date
  ON odds_fetch_state (kaisai_nen, kaisai_tsukihi, race_start_at_jst);

CREATE TABLE fetch_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  race_key TEXT,
  job_type TEXT NOT NULL,
  status TEXT NOT NULL,
  message TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX idx_fetch_logs_job_type_status_created_at
  ON fetch_logs (job_type, status, created_at DESC);

CREATE INDEX idx_fetch_logs_job_type_created_at
  ON fetch_logs (job_type, created_at DESC);
