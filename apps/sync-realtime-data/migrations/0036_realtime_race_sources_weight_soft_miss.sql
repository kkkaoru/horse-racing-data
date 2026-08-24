-- Migration: 0036_realtime_race_sources_weight_soft_miss
--
-- The weight watchdog normally backs off failed requests to avoid amplifying
-- upstream HTTP errors. A successfully fetched page whose weight table is
-- still empty/sparse is different: close to post time it is safe and useful
-- to check again on the next two-minute watchdog tick. This marker records
-- only that completed soft-miss outcome; the atomic reservation clears it
-- before the next request starts.

alter table realtime_race_sources
  add column last_weight_fetch_soft_miss_at text;
