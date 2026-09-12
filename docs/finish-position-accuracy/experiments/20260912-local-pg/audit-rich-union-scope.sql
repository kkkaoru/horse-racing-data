CREATE TEMP VIEW required AS
SELECT h.*
FROM read_parquet('/private/tmp/horse-nar-banei-0912/causal-features.parquet') h
JOIN read_parquet('docs/finish-position-accuracy/experiments/20260912-local-pg/ablation-v1/83/2026/no-speed/scope-races.parquet') s USING(race_id)
WHERE h.race_date < '20260101';
CREATE TEMP VIEW rich AS
SELECT concat_ws('-',source,race_date,keibajo_code,race_bango) AS race_id,
       ketto_toroku_bango AS horse_id, finish_position AS finish
FROM read_parquet('docs/finish-position-accuracy/experiments/20260912-local-pg/rich-history-v1/83/final/race_year=*/*.parquet')
WHERE race_date < '20260101'
UNION ALL
SELECT concat_ws('-',source,race_date,keibajo_code,race_bango) AS race_id,
       ketto_toroku_bango AS horse_id, finish_position AS finish
FROM read_parquet('docs/finish-position-accuracy/experiments/20260912-local-pg/rich-history-retired-v1/83/retired-base/race_year=*/*.parquet');
SELECT json_object(
  'required_rows', (SELECT count(*) FROM required),
  'required_labeled', (SELECT count(*) FROM required WHERE finish>0),
  'missing_rows', (SELECT count(*) FROM required q ANTI JOIN rich r USING(race_id,horse_id)),
  'missing_labeled', (SELECT count(*) FROM required q ANTI JOIN rich r USING(race_id,horse_id) WHERE q.finish>0),
  'label_differences', (SELECT count(*) FROM required q JOIN rich r USING(race_id,horse_id) WHERE q.finish IS DISTINCT FROM r.finish),
  'duplicate_keys', (SELECT count(*) FROM (SELECT race_id,horse_id FROM rich GROUP BY ALL HAVING count(*)>1)),
  'retired_extension_features', 'unavailable: 83-only native layer predicates; retain real base features and explicit null extension cells, never invent complete coverage'
);
