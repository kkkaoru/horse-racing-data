-- Keep per-cell training candidates distinct by runtime identity.
--
-- A single feature_set_hash can be produced by different training methods or
-- running-style cell artifacts. Persist those identity fields as indexed columns
-- so per-cell best-method selection does not depend on JSONB scans and does not
-- overwrite candidates that share the same feature hash.

begin;

alter table cell_training_evaluations
  add column if not exists model_version text not null default '';

alter table cell_training_evaluations
  add column if not exists architecture text not null default '';

alter table cell_training_evaluations
  add column if not exists method text not null default '';

alter table cell_training_evaluations
  add column if not exists cell_model_key text not null default '';

alter table cell_training_evaluations
  add column if not exists cell_variant_id text not null default '';

update cell_training_evaluations
set
  model_version = coalesce(nullif(metric_payload->>'model_version', ''), nullif(metric_payload#>>'{extra,model_version}', ''), model_version, ''),
  architecture = coalesce(nullif(metric_payload->>'architecture', ''), nullif(metric_payload#>>'{extra,architecture}', ''), nullif(metric_payload->>'model_architecture', ''), architecture, ''),
  method = coalesce(nullif(metric_payload->>'method', ''), nullif(metric_payload->>'search_method', ''), nullif(metric_payload->>'exploration_method', ''), nullif(metric_payload#>>'{extra,method}', ''), nullif(metric_payload#>>'{extra,search_method}', ''), nullif(metric_payload#>>'{extra,exploration_method}', ''), method, ''),
  cell_model_key = coalesce(nullif(metric_payload->>'cell_model_key', ''), nullif(metric_payload->>'cellModelKey', ''), nullif(metric_payload#>>'{extra,cell_model_key}', ''), nullif(metric_payload#>>'{extra,cellModelKey}', ''), cell_model_key, ''),
  cell_variant_id = coalesce(nullif(metric_payload->>'cell_variant_id', ''), nullif(metric_payload->>'cellVariantId', ''), nullif(metric_payload#>>'{extra,cell_variant_id}', ''), nullif(metric_payload#>>'{extra,cellVariantId}', ''), cell_variant_id, '')
where model_version = ''
   or architecture = ''
   or method = ''
   or cell_model_key = ''
   or cell_variant_id = '';

do $$
declare
  pk_cols text[];
  desired_pk_cols text[] := array[
    'prediction_target', 'feature_set_hash', 'category', 'surface',
    'distance_band', 'class_label', 'season', 'venue', 'subgroup',
    'model_version', 'architecture', 'method', 'cell_model_key',
    'cell_variant_id'
  ];
begin
  select array_agg(a.attname order by u.ordinality)
    into pk_cols
  from pg_constraint c
  join unnest(c.conkey) with ordinality as u(attnum, ordinality) on true
  join pg_attribute a on a.attrelid = c.conrelid and a.attnum = u.attnum
  where c.conrelid = 'cell_training_evaluations'::regclass
    and c.contype = 'p';

  if pk_cols is distinct from desired_pk_cols then
    alter table cell_training_evaluations
      drop constraint cell_training_evaluations_pkey;

    alter table cell_training_evaluations
      add primary key (
        prediction_target, feature_set_hash, category, surface,
        distance_band, class_label, season, venue, subgroup,
        model_version, architecture, method, cell_model_key, cell_variant_id
      );
  end if;
end $$;

create index if not exists cell_training_evaluations_target_identity_idx
  on cell_training_evaluations (prediction_target, category, method, model_version);

create index if not exists cell_training_evaluations_target_cell_variant_idx
  on cell_training_evaluations (prediction_target, category, cell_model_key, cell_variant_id);

commit;
