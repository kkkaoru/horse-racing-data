-- 0003: Add UNIQUE index on odds_snapshots so retried INSERTs (catch-up sweep,
-- F1 backfill scripts, queue retries) are idempotent. Without this, the
-- past-race finalSlot enqueue gate that allows a single re-fetch could double
-- up rows in the wide / 3rentan / 3renpuku tables whenever the queue
-- redelivered a message. The application layer pairs this with
-- `on conflict(race_key, fetched_at, odds_type, combination) do update set ...`
-- in `insertOddsSnapshot` and `bulkInsertOddsSnapshotRows`.

CREATE UNIQUE INDEX IF NOT EXISTS idx_odds_snapshots_race_fetched_type_combination_unique
  ON odds_snapshots (race_key, fetched_at, odds_type, combination);
