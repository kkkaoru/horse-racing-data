-- Migration: hot_path_indexes
-- Address the top wasteful read queries surfaced by `wrangler d1 insights`.
-- Each index targets a specific query that scans ~hundreds of thousands of
-- rows to return a handful of values, draining D1 row-read budget and adding
-- latency to user requests.

-- fetch_logs is used by seedRealtimePlannerWatchdog on every minute cron
-- (38958 calls/day) to find the latest successful or queued plan entry.
-- Without an index the queries scan the entire fetch_logs table each time
-- (4.1 billion rows read in 24h). A composite index lets SQLite resolve the
-- ORDER BY ... LIMIT 1 with a single B-tree seek.
CREATE INDEX IF NOT EXISTS idx_fetch_logs_job_type_status_created_at
  ON fetch_logs (job_type, status, created_at DESC);

-- odds_snapshots would also benefit from (race_key, fetched_at), but the
-- table is too large (13M+ rows) for D1's CREATE INDEX sort buffer right now
-- (SQLITE_NOMEM). Backfill once row counts shrink or D1 supports chunked
-- index builds. The existing (race_key, odds_type, fetched_at) index is
-- accepted for now.

-- realtime_race_sources queries that filter by date and project keibajo_code
-- (distinct / count / venue navigation) cannot fully use the existing
-- (kaisai_nen, kaisai_tsukihi, race_start_at_jst) index because keibajo_code
-- is not covered. Adding the venue column as the third key turns the
-- queries into covering index scans.
CREATE INDEX IF NOT EXISTS idx_realtime_race_sources_date_venue
  ON realtime_race_sources (kaisai_nen, kaisai_tsukihi, keibajo_code);
