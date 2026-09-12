CREATE TEMP VIEW native_features AS
SELECT concat_ws('-',source,race_date,keibajo_code,race_bango) AS race_id,
       ketto_toroku_bango AS horse_id, race_date, finish_position AS finish
FROM read_parquet('docs/finish-position-accuracy/experiments/20260912-local-pg/production-baseline-features/83/final/race_year=*/*.parquet');
CREATE TEMP VIEW raw_features AS
SELECT race_id,horse_id,race_date,finish
FROM read_parquet('/private/tmp/horse-nar-banei-0912/causal-features-raw2026.parquet')
WHERE category='ban-ei' AND race_date BETWEEN '20260523' AND '20260907';
CREATE TEMP VIEW source_differences AS
SELECT coalesce(n.race_id,r.race_id) AS race_id,
       coalesce(n.race_date,r.race_date) AS race_date,
       n.horse_id IS NULL AS native_missing, r.horse_id IS NULL AS raw_missing,
       n.finish IS DISTINCT FROM r.finish AS label_differs
FROM native_features n FULL OUTER JOIN raw_features r USING(race_id,horse_id);
CREATE TEMP VIEW baseline AS
SELECT * FROM read_parquet('docs/finish-position-accuracy/experiments/20260912-local-pg/frozen-production-v1/83/predictions.parquet')
WHERE race_date <= '20260830';
CREATE TEMP VIEW candidate AS
SELECT * FROM read_parquet('docs/finish-position-accuracy/experiments/20260912-local-pg/ablation-v1/83/2026/speed/predictions.parquet')
WHERE race_date >= '20260523';
CREATE TEMP VIEW pair_differences AS
SELECT coalesce(b.race_id,c.race_id) AS race_id,
       b.horse_id IS NULL AS baseline_missing, c.horse_id IS NULL AS candidate_missing,
       b.finish IS DISTINCT FROM c.finish AS label_differs
FROM baseline b FULL OUTER JOIN candidate c USING(race_id,horse_id);
SELECT json_object(
  'native_rows', (SELECT count(*) FROM native_features),
  'native_labeled', (SELECT count(*) FROM native_features WHERE finish>0),
  'raw_rows', (SELECT count(*) FROM raw_features),
  'raw_labeled', (SELECT count(*) FROM raw_features WHERE finish>0),
  'source_native_missing', (SELECT count(*) FROM source_differences WHERE native_missing),
  'source_raw_missing', (SELECT count(*) FROM source_differences WHERE raw_missing),
  'source_label_differences', (SELECT count(*) FROM source_differences WHERE label_differs),
  'pair_baseline_rows', (SELECT count(*) FROM baseline),
  'pair_candidate_rows', (SELECT count(*) FROM candidate),
  'pair_mismatched_races', (SELECT list(DISTINCT race_id ORDER BY race_id) FROM pair_differences WHERE baseline_missing OR candidate_missing OR label_differs)
);
