INSERT OR IGNORE INTO catalog_targets (
  table_name,
  provider,
  partition_field,
  enabled,
  created_at
) VALUES (
  'netkeiba_training_workouts',
  'jv',
  'kaisai_nen',
  1,
  '2026-09-05T00:00:00.000Z'
);
