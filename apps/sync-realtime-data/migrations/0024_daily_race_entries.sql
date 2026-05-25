-- Migration: daily_race_entries
-- Per-day per-horse race entry rows mirrored to D1 by the
-- build-daily-features Worker cron. Data source: Neon nvd_se/jvd_se +
-- nvd_ra/jvd_ra (read), D1 (write). Indexes target the common access
-- patterns: lookup by race_key, list by race_date+source, and horse
-- history scans during inference.

CREATE TABLE IF NOT EXISTS daily_race_entries (
  race_key TEXT NOT NULL,
  race_date TEXT NOT NULL,
  source TEXT NOT NULL,
  kaisai_nen TEXT NOT NULL,
  kaisai_tsukihi TEXT NOT NULL,
  keibajo_code TEXT NOT NULL,
  race_bango TEXT NOT NULL,
  ketto_toroku_bango TEXT NOT NULL,
  umaban INTEGER,
  bamei TEXT,
  track_code TEXT,
  grade_code TEXT,
  kyoso_shubetsu_code TEXT,
  juryo_shubetsu_code TEXT,
  kyoso_joken_code TEXT,
  babajotai_code_shiba TEXT,
  babajotai_code_dirt TEXT,
  kyori INTEGER,
  shusso_tosu INTEGER,
  seibetsu_code TEXT,
  barei INTEGER,
  futan_juryo REAL,
  kishumei_ryakusho TEXT,
  chokyoshimei_ryakusho TEXT,
  banushimei TEXT,
  finish_position INTEGER,
  finish_norm REAL,
  tansho_ninkijun INTEGER,
  tansho_odds REAL,
  soha_time INTEGER,
  time_sa REAL,
  kohan_3f REAL,
  corner1_norm REAL,
  corner2_norm REAL,
  corner3_norm REAL,
  corner4_norm REAL,
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (race_key, ketto_toroku_bango)
);

CREATE INDEX IF NOT EXISTS idx_daily_race_entries_date_source
  ON daily_race_entries (race_date, source);

CREATE INDEX IF NOT EXISTS idx_daily_race_entries_horse_date
  ON daily_race_entries (ketto_toroku_bango, race_date DESC);

CREATE INDEX IF NOT EXISTS idx_daily_race_entries_lookup
  ON daily_race_entries (source, race_date, keibajo_code, race_bango);
