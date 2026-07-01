-- Preserve the full cell key for running-style and finish-position cell
-- evaluations. Existing rows predate subgroup persistence and are backfilled
-- with an empty subgroup so the migration is safe to rerun.

begin;

alter table cell_training_evaluations
  add column if not exists subgroup text not null default '';

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

  if pk_cols is distinct from array[
    'prediction_target',
    'feature_set_hash',
    'category',
    'surface',
    'distance_band',
    'class_label',
    'season',
    'venue',
    'subgroup'
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
        venue,
        subgroup
      );
  end if;
end $$;

create index if not exists cell_training_evaluations_target_category_subgroup_idx
  on cell_training_evaluations (prediction_target, category, subgroup);

commit;
