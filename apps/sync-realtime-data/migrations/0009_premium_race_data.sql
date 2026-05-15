CREATE TABLE IF NOT EXISTS premium_race_links (
  race_key TEXT PRIMARY KEY,
  source_race_id TEXT NOT NULL,
  entry_url TEXT NOT NULL,
  discovered_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (race_key) REFERENCES realtime_race_sources (race_key) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_premium_race_links_source_race_id
  ON premium_race_links (source_race_id);

CREATE TABLE IF NOT EXISTS premium_training_reviews (
  race_key TEXT NOT NULL,
  source_race_id TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  horse_number TEXT NOT NULL,
  horse_name TEXT,
  training_date TEXT NOT NULL DEFAULT '',
  evaluation_text TEXT,
  evaluation_grade TEXT,
  comment_text TEXT,
  created_at TEXT NOT NULL,
  PRIMARY KEY (race_key, horse_number, training_date),
  FOREIGN KEY (race_key) REFERENCES realtime_race_sources (race_key) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_premium_training_reviews_race_key
  ON premium_training_reviews (race_key, horse_number, training_date);

CREATE TABLE IF NOT EXISTS premium_stable_comments (
  race_key TEXT NOT NULL,
  source_race_id TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  frame_number TEXT,
  horse_number TEXT NOT NULL,
  horse_name TEXT,
  comment_text TEXT NOT NULL,
  evaluation_text TEXT,
  created_at TEXT NOT NULL,
  PRIMARY KEY (race_key, horse_number),
  FOREIGN KEY (race_key) REFERENCES realtime_race_sources (race_key) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_premium_stable_comments_race_key
  ON premium_stable_comments (race_key, horse_number);

CREATE TABLE IF NOT EXISTS premium_paddock_bulletins (
  race_key TEXT NOT NULL,
  source_race_id TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  group_key TEXT NOT NULL,
  frame_number TEXT,
  horse_number TEXT NOT NULL,
  horse_name TEXT,
  evaluation_text TEXT,
  comment_text TEXT,
  created_at TEXT NOT NULL,
  PRIMARY KEY (race_key, group_key, horse_number),
  FOREIGN KEY (race_key) REFERENCES realtime_race_sources (race_key) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_premium_paddock_bulletins_race_key
  ON premium_paddock_bulletins (race_key, group_key, horse_number);

CREATE TABLE IF NOT EXISTS premium_paddock_fetch_state (
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

CREATE INDEX IF NOT EXISTS idx_premium_paddock_fetch_state_retry_after
  ON premium_paddock_fetch_state (retry_after);
