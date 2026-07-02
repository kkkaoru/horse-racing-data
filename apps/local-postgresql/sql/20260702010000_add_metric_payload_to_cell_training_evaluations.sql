-- Store target-specific detailed metrics for cell training evaluations.
--
-- Shared scalar columns remain the adoption-compatible surface:
-- finish-position uses top/place metrics directly, while running-style maps
-- top1=accuracy, place2=top2_accuracy, place3=macro_f1. The JSONB payload keeps
-- target-native details such as running-style per-class scores and race-level
-- corner/finish metrics without changing the primary key.

begin;

alter table cell_training_evaluations
  add column if not exists metric_payload jsonb not null default '{}'::jsonb;

update cell_training_evaluations
set metric_payload = jsonb_build_object(
  'metric_schema_version', 'cell_training_evaluation_scalar_v1',
  'prediction_target', prediction_target,
  'feature_set_hash', feature_set_hash,
  'feature_count', feature_count,
  'cell', jsonb_build_object(
    'category', category,
    'surface', surface,
    'distance_band', distance_band,
    'class_label', class_label,
    'season', season,
    'venue', venue,
    'subgroup', subgroup
  ),
  'race_count', race_count,
  'metrics', jsonb_build_object(
    'ndcg_at_3', ndcg_at_3,
    'top1_accuracy', top1_accuracy,
    'place2_accuracy', place2_accuracy,
    'place3_accuracy', place3_accuracy,
    'place4_accuracy', place4_accuracy,
    'place5_accuracy', place5_accuracy,
    'place6_accuracy', place6_accuracy,
    'top3_box_accuracy', top3_box_accuracy
  ),
  'accuracy_vector', to_jsonb(accuracy_vector),
  'cell_vector', to_jsonb(cell_vector)
)
where metric_payload = '{}'::jsonb;

commit;
