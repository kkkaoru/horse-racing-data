-- Migration: 0027_weight_watchdog_index
--
-- The weight watchdog cron runs every minute and scans realtime_race_sources
-- with:
--
--   SELECT ... FROM realtime_race_sources
--   WHERE race_start_at_jst > ? AND race_start_at_jst < ?
--     AND (last_weight_fetch_at IS NULL OR last_weight_fetch_at < ?)
--   ORDER BY race_start_at_jst LIMIT 8
--
-- The only covering index so far is idx_realtime_race_sources_date_start
-- (kaisai_nen, kaisai_tsukihi, race_start_at_jst) from migration 0006. Because
-- the watchdog never constrains kaisai_nen / kaisai_tsukihi, SQLite cannot use
-- that index and falls back to a full table scan once a minute, which is the
-- dominant driver of D1 rows-read charges.
--
-- Leading the index with race_start_at_jst lets D1 satisfy the range predicate
-- plus the ORDER BY ... LIMIT straight from the index, and carrying
-- last_weight_fetch_at as the second column keeps the staleness filter inside
-- the index so the scan stops paging row bodies.

CREATE INDEX IF NOT EXISTS idx_rrs_weight_watchdog
  ON realtime_race_sources (race_start_at_jst, last_weight_fetch_at);
