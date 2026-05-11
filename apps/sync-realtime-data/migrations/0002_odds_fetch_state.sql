ALTER TABLE nar_race_sources ADD COLUMN last_odds_queued_at TEXT;
ALTER TABLE nar_race_sources ADD COLUMN odds_fetch_lock_until TEXT;
