-- 0004: Drop dead index idx_odds_snapshots_race_type_time
-- (race_key, odds_type, fetched_at) is a strict leftmost-prefix of
-- idx_odds_snapshots_race_type_time_rank_comb
-- (race_key, odds_type, fetched_at, rank, combination). EXPLAIN QUERY PLAN
-- confirmed SQLite always picks the wider covering index for every existing
-- read on (race_key, odds_type) — the narrower one is dead weight: ~1.5 GB of
-- index pages with zero read benefit and a B-tree update on every INSERT.
-- Dropping it reduces write amplification on the 24,143,152-row odds_snapshots
-- table, the immediate fix for the 2026-06-22 13:01 JST D1 CPU exhaust
-- incident that put the per-minute polling cron into silent death.

DROP INDEX IF EXISTS idx_odds_snapshots_race_type_time;
