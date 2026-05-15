INSERT INTO realtime_race_sources (
  race_key,
  source,
  kaisai_nen,
  kaisai_tsukihi,
  keibajo_code,
  race_bango,
  baba_code,
  race_start_at_jst,
  race_name,
  deba_url,
  odds_links_json,
  discovered_at,
  updated_at,
  last_odds_fetch_at,
  last_weight_fetch_at,
  last_odds_queued_at,
  odds_fetch_lock_until,
  last_result_fetch_at,
  last_result_queued_at,
  result_fetch_lock_until,
  result_complete_at,
  result_expected_horse_count,
  result_saved_horse_count
)
SELECT
  'jra:' || substr(race_key, 5),
  source,
  kaisai_nen,
  kaisai_tsukihi,
  keibajo_code,
  race_bango,
  baba_code,
  race_start_at_jst,
  race_name,
  deba_url,
  odds_links_json,
  discovered_at,
  datetime('now'),
  last_odds_fetch_at,
  last_weight_fetch_at,
  NULL,
  NULL,
  last_result_fetch_at,
  NULL,
  NULL,
  result_complete_at,
  result_expected_horse_count,
  result_saved_horse_count
FROM realtime_race_sources
WHERE source = 'jra'
  AND race_key LIKE 'nar:%'
  AND NOT EXISTS (
    SELECT 1
    FROM realtime_race_sources existing
    WHERE existing.race_key = 'jra:' || substr(realtime_race_sources.race_key, 5)
  );

UPDATE odds_snapshots
SET race_key = 'jra:' || substr(race_key, 5)
WHERE race_key IN (
  SELECT race_key
  FROM realtime_race_sources
  WHERE source = 'jra'
    AND race_key LIKE 'nar:%'
);

UPDATE horse_weight_snapshots
SET race_key = 'jra:' || substr(race_key, 5)
WHERE race_key IN (
  SELECT race_key
  FROM realtime_race_sources
  WHERE source = 'jra'
    AND race_key LIKE 'nar:%'
);

UPDATE race_entry_snapshots
SET race_key = 'jra:' || substr(race_key, 5)
WHERE race_key IN (
  SELECT race_key
  FROM realtime_race_sources
  WHERE source = 'jra'
    AND race_key LIKE 'nar:%'
);

UPDATE race_result_snapshots
SET race_key = 'jra:' || substr(race_key, 5)
WHERE race_key IN (
  SELECT race_key
  FROM realtime_race_sources
  WHERE source = 'jra'
    AND race_key LIKE 'nar:%'
);

UPDATE jra_track_condition_snapshots
SET race_key = 'jra:' || substr(race_key, 5)
WHERE race_key IN (
  SELECT race_key
  FROM realtime_race_sources
  WHERE source = 'jra'
    AND race_key LIKE 'nar:%'
);

UPDATE fetch_logs
SET race_key = 'jra:' || substr(race_key, 5)
WHERE race_key IN (
  SELECT race_key
  FROM realtime_race_sources
  WHERE source = 'jra'
    AND race_key LIKE 'nar:%'
);

DELETE FROM realtime_race_sources
WHERE source = 'jra'
  AND race_key LIKE 'nar:%';
