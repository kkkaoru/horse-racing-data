-- Migration: 0034_realtime_race_sources_result_void
--
-- 2026-07-24 incident: 44:05 and 44:06 (Oi races 5 and 6) never produced
-- result rows -- keiba.go.jp's own RaceList page shows their odds/video/
-- results chartBtn links permanently disabled (confirmed hours after the
-- scheduled post time, unlike every other race that day), while
-- markEmptyResultGiveUp's existing empty-result circuit breaker force-
-- completed them via result_complete_at with zero rows in
-- race_result_snapshots. That state is indistinguishable downstream from
-- "results genuinely have not landed yet" -- the race-trend viewer rendered
-- these two races as a silent gap in the day's finish-position sequence
-- instead of a "race unavailable" indicator, and the result-fetch retry
-- loop only stopped once the 60-minute circuit breaker floor was reached.
--
-- This column records the moment a race was confirmed to have no result
-- (whether upstream-cancelled or otherwise never published) so callers can
-- render an explicit status instead of a blank gap, and so the result-fetch
-- planner can recognize an already-void race distinctly from one still
-- awaiting its normal completion.

alter table realtime_race_sources
  add column result_void_at text;
