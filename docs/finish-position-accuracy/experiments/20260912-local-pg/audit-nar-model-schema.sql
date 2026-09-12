SELECT json_object(
 'race_cell_columns', (SELECT to_json(list(DISTINCT name ORDER BY name)) FROM parquet_schema('docs/finish-position-accuracy/experiments/20260912-local-pg/race-cells-local-pg.parquet')),
 'missing_nar_model_features_from_base', (
    SELECT to_json(list(feature ORDER BY feature)) FROM (
      SELECT unnest(feature_names) AS feature FROM read_json_auto('apps/finish-position-predict-container/models/finish-position/nar/iter12-nar-xgb-hpo-v8-stage1-marketfree-184/metadata.json')
    ) model
    WHERE feature NOT IN (
      SELECT name FROM parquet_schema('docs/finish-position-accuracy/experiments/20260912-local-pg/rich-history-retired-v1/83/base/race_year=*/*.parquet')
    )
 ),
 'model_training_range', (SELECT json_extract(json,'$.train_date_range') FROM read_json_objects('apps/finish-position-predict-container/models/finish-position/nar/iter12-nar-xgb-hpo-v8-stage1-marketfree-184/metadata.json'))
);
