-- 0006: R2/R2 Catalog cutover is canonical for realtime odds payloads.
-- Drop the legacy D1 odds table to remove storage cost and prevent accidental reuse.
DROP TABLE IF EXISTS odds_snapshots;
