-- Migration: 0033_realtime_race_sources_weight_fetch_attempt
--
-- 2026-07-03 incident: last_weight_fetch_at is only written on a SUCCESSFUL
-- weight capture (worker.ts fetchAndStoreWeights). A race whose weight page
-- 404s (transient keiba.go.jp rate-limiting under request-volume pressure,
-- not an actually-missing page) never gets last_weight_fetch_at set, so
-- findStaleWeightFetchRaces re-selects it as "due" on every subsequent
-- */2 cron tick indefinitely -- zero backoff, hours of repeat 404s against
-- the same race. This column records the last ATTEMPT regardless of outcome
-- so the watchdog can apply a real cooldown after a failure, independent of
-- the existing (unchanged) success-only staleness gate on last_weight_fetch_at.

alter table realtime_race_sources
  add column last_weight_fetch_attempt_at text;
