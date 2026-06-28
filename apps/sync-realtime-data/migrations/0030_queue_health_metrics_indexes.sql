-- Migration: 0030_queue_health_metrics_indexes
--
-- D1 cost optimization (2026-06-28). The queue-health endpoint
-- (storage.getQueueHealthMetrics) runs two count(*) scans against
-- realtime_race_sources every time a viewer or operator polls
-- /queue-health:
--
--   RACES_QUEUED_NOT_FETCHED_TODAY_SQL:
--     SELECT count(*) FROM realtime_race_sources
--     WHERE (kaisai_nen || kaisai_tsukihi) = ?
--       AND last_result_queued_at IS NOT NULL
--       AND last_result_fetch_at IS NULL
--
--   RACES_STUCK_OVER_THIRTY_MIN_SQL:
--     SELECT count(*) FROM realtime_race_sources
--     WHERE last_result_fetch_at IS NOT NULL
--       AND last_result_fetch_at < ?
--       AND result_complete_at IS NULL
--
-- Neither query is covered by the existing indexes (0006 + 0022 + 0027) and
-- both fall back to a full table scan. realtime_race_sources currently sits
-- at ~50k+ rows (every JRA + NAR race for the rolling window), so each
-- /queue-health hit charges ~100k rows_read against D1. Two partial
-- indexes turn both into single-key range seeks.
--
-- Notes:
-- - `WHERE last_result_queued_at IS NOT NULL` is a partial-index filter that
--   keeps the index narrow (only races that have ever been enqueued — a
--   small fraction of the table). Same idea for the stuck-races index on
--   `result_complete_at IS NULL` (only races that have not yet completed).
-- - Indexes are additive — existing read paths and writers see no schema
--   change beyond a small per-row index maintenance cost on
--   markResultFetchQueued / completeResultFetch, which is dominated by the
--   row write itself.
-- - No data is moved or deleted. Compatible with the
--   `feedback_no_data_delete` rule.

CREATE INDEX IF NOT EXISTS idx_rrs_queued_not_fetched
  ON realtime_race_sources (kaisai_nen, kaisai_tsukihi, last_result_fetch_at)
  WHERE last_result_queued_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_rrs_stuck_open_results
  ON realtime_race_sources (last_result_fetch_at)
  WHERE result_complete_at IS NULL;
