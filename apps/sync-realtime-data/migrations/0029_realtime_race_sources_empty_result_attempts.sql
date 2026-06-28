-- Migration: 0029_realtime_race_sources_empty_result_attempts
--
-- Empty-result circuit breaker (paired with the 2026-06-28 queue concurrency
-- bump). The result-poll cron throws "race result rows are empty: ..." every
-- 2 minutes for races whose upstream HTML parses to zero result rows after
-- entries were published — observed in production today where one JRA race
-- looped 15 times in fetch_logs while NAR result jobs sat 12h behind the
-- backlog. Each empty re-queue clears the lock via failResultFetch and the
-- planner re-enqueues on the next cron tick, so the only natural backstop
-- was `RESULT_FETCH_GIVE_UP_HOURS = 24h` after race start.
--
-- The new column counts consecutive empty result attempts per race. When the
-- counter hits RESULT_FETCH_EMPTY_GIVEUP_COUNT (worker.ts) the race is
-- force-completed via `result_complete_at = now` so the planner stops re-
-- queueing it. A successful non-empty result resets the counter to 0 so a
-- transient empty followed by a real result does not trip the breaker.

ALTER TABLE realtime_race_sources ADD COLUMN empty_result_attempts INTEGER NOT NULL DEFAULT 0;
