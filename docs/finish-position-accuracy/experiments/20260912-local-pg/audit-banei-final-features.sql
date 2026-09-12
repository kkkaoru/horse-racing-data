CREATE TEMP VIEW features AS
SELECT * FROM read_parquet('docs/finish-position-accuracy/experiments/20260912-local-pg/production-baseline-features/83/final/race_year=*/*.parquet');
CREATE TEMP VIEW wanted AS
SELECT unnest(feature_names) AS name
FROM read_json_auto('apps/finish-position-predict-container/models/finish-position/ban-ei/banei-cb-v9-sim-2011/metadata.json');
SELECT json_object(
  'rows', (SELECT count(*) FROM features),
  'first_date', (SELECT min(race_date) FROM features),
  'last_date', (SELECT max(race_date) FROM features),
  'columns', (SELECT list(name) FROM pragma_table_info('features')),
  'missing_model_features', (SELECT list(name) FROM (SELECT name FROM wanted EXCEPT SELECT name FROM pragma_table_info('features')))
);
