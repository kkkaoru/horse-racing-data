-- Migration: 0025_daily_race_entries_trend_columns
-- Adds columns required by the race trend page (wakuban, race_name,
-- hasso_jikoku, raw corner positions, horse weight) so the trend API can
-- read everything from D1 once backfilled. Until backfill completes, the
-- pc-keiba-viewer Neon path remains the source of truth.

ALTER TABLE daily_race_entries ADD COLUMN wakuban TEXT;
ALTER TABLE daily_race_entries ADD COLUMN race_name TEXT;
ALTER TABLE daily_race_entries ADD COLUMN hasso_jikoku TEXT;
ALTER TABLE daily_race_entries ADD COLUMN corner_1 INTEGER;
ALTER TABLE daily_race_entries ADD COLUMN corner_2 INTEGER;
ALTER TABLE daily_race_entries ADD COLUMN corner_3 INTEGER;
ALTER TABLE daily_race_entries ADD COLUMN corner_4 INTEGER;
ALTER TABLE daily_race_entries ADD COLUMN bataiju INTEGER;
ALTER TABLE daily_race_entries ADD COLUMN zogen_fugo TEXT;
ALTER TABLE daily_race_entries ADD COLUMN zogen_sa INTEGER;

CREATE INDEX IF NOT EXISTS idx_daily_race_entries_source_date
  ON daily_race_entries (source, race_date DESC);
