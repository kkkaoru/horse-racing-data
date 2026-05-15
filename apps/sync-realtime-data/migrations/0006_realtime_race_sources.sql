ALTER TABLE nar_race_sources RENAME TO realtime_race_sources;

DROP INDEX IF EXISTS idx_nar_race_sources_date_start;

CREATE INDEX IF NOT EXISTS idx_realtime_race_sources_date_start
  ON realtime_race_sources (kaisai_nen, kaisai_tsukihi, race_start_at_jst);
