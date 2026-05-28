-- Migration: 0026_realtime_payload_hot_path_indexes
--
-- Speeds up the buildRealtimePayload fan-out that the viewer detail page hits
-- every 30s for live odds. The hottest five reads are all keyed on race_key
-- and (in the snapshot tables) sorted by horse_number afterwards. Adding the
-- horse_number column to each (race_key, fetched_at) index turns the latest
-- snapshot reads into covering, index-only scans so D1 stops paging the row
-- bodies for sort.
--
-- For odds_snapshots, listOddsHistoryByType currently fetches every odds row
-- for the race and sorts in JS by (odds_type, fetched_at, coalesce(rank,
-- big_int), combination). Adding (race_key, odds_type, fetched_at, rank,
-- combination) lets D1 satisfy the sort + slice straight from the index for
-- the dominant code path. The existing indexes are kept because they cover
-- the writer side (insertOddsSnapshot) and other read patterns.

CREATE INDEX IF NOT EXISTS idx_race_entry_snapshots_race_time_horse
  ON race_entry_snapshots (race_key, fetched_at, horse_number);

CREATE INDEX IF NOT EXISTS idx_horse_weight_snapshots_race_time_horse
  ON horse_weight_snapshots (race_key, fetched_at, horse_number);

CREATE INDEX IF NOT EXISTS idx_race_result_snapshots_race_time_horse
  ON race_result_snapshots (race_key, fetched_at, horse_number);

CREATE INDEX IF NOT EXISTS idx_odds_snapshots_race_type_time_rank_comb
  ON odds_snapshots (race_key, odds_type, fetched_at, rank, combination);
