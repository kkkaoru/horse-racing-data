-- Add prediction_target to cell_training_evaluations so finish-position and
-- running-style cell evaluations can coexist for the same feature/cell key.
--
-- Existing rows are finish-position evaluations. The default/backfill keeps
-- those rows on prediction_target='finish_position', then the legacy primary
-- key is replaced with a target-aware key. Idempotent: re-runs leave an already
-- migrated table unchanged and keep the target-aware indexes present.

begin;

alter table cell_training_evaluations
  add column if not exists prediction_target text;

alter table cell_training_evaluations
  alter column prediction_target set default 'finish_position';

update cell_training_evaluations
set prediction_target = 'finish_position'
where prediction_target is null;

alter table cell_training_evaluations
  alter column prediction_target set not null;

do $$
declare
  pk_cols text[];
begin
  select array_agg(a.attname order by u.ordinality)
  into pk_cols
  from pg_constraint c
  join unnest(c.conkey) with ordinality as u(attnum, ordinality) on true
  join pg_attribute a on a.attrelid = c.conrelid and a.attnum = u.attnum
  where c.conrelid = 'cell_training_evaluations'::regclass
    and c.contype = 'p';

  if pk_cols = array[
    'feature_set_hash',
    'category',
    'surface',
    'distance_band',
    'class_label',
    'season',
    'venue'
  ] then
    alter table cell_training_evaluations
      drop constraint cell_training_evaluations_pkey;

    alter table cell_training_evaluations
      add primary key (
        prediction_target,
        feature_set_hash,
        category,
        surface,
        distance_band,
        class_label,
        season,
        venue
      );
  end if;
end $$;

create index if not exists cell_training_evaluations_target_category_season_idx
  on cell_training_evaluations (prediction_target, category, season);

create index if not exists cell_training_evaluations_target_category_venue_idx
  on cell_training_evaluations (prediction_target, category, venue);

create index if not exists cell_training_evaluations_target_feature_hash_idx
  on cell_training_evaluations (prediction_target, feature_set_hash);

create index if not exists cell_training_evaluations_target_category_season_venue_idx
  on cell_training_evaluations (prediction_target, category, season, venue);

create index if not exists cell_training_evaluations_target_category_top1_idx
  on cell_training_evaluations (prediction_target, category, top1_accuracy desc);

commit;
