-- Migration: revised_hot_path_indexes
-- Second pass over D1 `insights` shows two query shapes still scanning
-- billions of rows per week even after 0022:
--   * fetch_logs lookups that filter only by job_type (no status) still
--     fell back to a full job-type partition scan.
--   * odds_snapshots reads keyed on `(race_key, fetched_at)` (latest
--     snapshot retrieval and `max(fetched_at)`) had to scan the entire
--     race_key partition because the existing index leads with odds_type.
-- Both indexes were verified with EXPLAIN QUERY PLAN to resolve as
-- (COVERING) INDEX seeks instead of partition scans.

-- fetch_logs (avg 16k–95k rows / call across ~200k+ calls per week).
-- Complements the (job_type, status, created_at DESC) index already in 0022
-- by also serving "select created_at ... where job_type = X order by
-- created_at desc limit ?" queries that don't constrain status.
CREATE INDEX IF NOT EXISTS idx_fetch_logs_job_type_created_at
  ON fetch_logs (job_type, created_at DESC);

-- odds_snapshots (avg 20k–43k rows / call across ~265k calls per week).
-- The CREATE INDEX previously hit SQLITE_NOMEM at 13M rows. Old data
-- (>5 days) was pruned via DELETE statements before this migration, after
-- which the index could be built in one shot. A retention cron in the
-- worker (see worker.ts scheduled handler) trims further history daily so
-- the index stays buildable on subsequent backfills.
CREATE INDEX IF NOT EXISTS idx_odds_snapshots_race_key_fetched_at
  ON odds_snapshots (race_key, fetched_at);
