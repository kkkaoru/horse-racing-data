-- Create the shared per-cell training evaluation table.
--
-- Viewer training code also bootstraps this table at runtime, but local
-- PostgreSQL migrations must be able to initialize a fresh database before any
-- learner has connected.

begin;

create table if not exists cell_training_evaluations (
  prediction_target text not null default 'finish_position',
  feature_set_hash text not null,
  category text not null,
  surface text not null,
  distance_band text not null,
  class_label text not null,
  season text not null,
  venue text not null,
  subgroup text not null default '',
  feature_count integer not null,
  race_count integer not null,
  ndcg_at_3 double precision not null,
  top1_accuracy double precision not null,
  place2_accuracy double precision not null,
  place3_accuracy double precision not null,
  place4_accuracy double precision not null,
  place5_accuracy double precision not null,
  place6_accuracy double precision not null,
  top3_box_accuracy double precision not null,
  accuracy_vector double precision[] not null,
  feature_names_array text[] not null,
  cell_vector text[] not null,
  metric_payload jsonb not null default '{}'::jsonb,
  model_version text not null default '',
  architecture text not null default '',
  method text not null default '',
  cell_model_key text not null default '',
  cell_variant_id text not null default '',
  evaluated_at timestamptz not null default now(),
  primary key (
    prediction_target,
    feature_set_hash,
    category,
    surface,
    distance_band,
    class_label,
    season,
    venue,
    subgroup,
    model_version,
    architecture,
    method,
    cell_model_key,
    cell_variant_id
  )
);

commit;
