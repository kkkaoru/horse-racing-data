-- 0002: Add missing indexes for archive query and source-first lookups
-- These prevent full scans on odds_snapshots.fetched_at archive queries
-- and odds_fetch_state queries where the WHERE clause leads with `source`.

CREATE INDEX IF NOT EXISTS idx_odds_snapshots_fetched_at
  ON odds_snapshots (fetched_at ASC);

CREATE INDEX IF NOT EXISTS idx_odds_fetch_state_source_date_time
  ON odds_fetch_state (source, kaisai_nen, kaisai_tsukihi, race_start_at_jst);

CREATE INDEX IF NOT EXISTS idx_odds_fetch_state_source_date_keibajo
  ON odds_fetch_state (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_start_at_jst);
