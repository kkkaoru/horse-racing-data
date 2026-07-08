INSERT INTO odds_snapshots_hot_sink
SELECT
  race_key,
  source,
  kaisai_yyyymmdd,
  fetched_at,
  odds_type,
  combination,
  odds,
  min_odds,
  max_odds,
  average_odds,
  rank
FROM odds_snapshots_hot_stream
