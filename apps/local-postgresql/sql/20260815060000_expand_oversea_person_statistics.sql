-- Full secondary-source person histories can contain historical rows where the
-- source publishes a distance but no surface. Preserve that absence as NULL.
-- Snapshot provenance is row-specific because JV and secondary-source
-- populations must never be merged into one denominator.

alter table oversea_person_race_history
  alter column venue drop not null,
  alter column horse_name drop not null,
  alter column surface drop not null,
  alter column distance_metres drop not null;

alter table oversea_person_race_history
  drop constraint if exists oversea_person_race_history_venue_check;

alter table oversea_person_race_history
  add constraint oversea_person_race_history_venue_check
  check (venue is null or btrim(venue) <> '');

alter table oversea_person_race_history
  drop constraint if exists oversea_person_race_history_horse_name_check;

alter table oversea_person_race_history
  add constraint oversea_person_race_history_horse_name_check
  check (horse_name is null or btrim(horse_name) <> '');

-- A historical source row can omit both the horse link and display name. Keep
-- that row without inventing an identity, while preserving idempotency at the
-- only remaining stable source boundary.
drop index if exists oversea_person_race_history_source_name_uidx;

create unique index oversea_person_race_history_source_name_uidx
  on oversea_person_race_history (
    source, person_kind, source_person_id, source_race_id, horse_name
  )
  where source_horse_id is null and horse_name is not null;

create unique index if not exists oversea_person_race_history_source_missing_horse_uidx
  on oversea_person_race_history (source, person_kind, source_person_id, source_race_id)
  where source_horse_id is null and horse_name is null;

alter table oversea_person_race_history
  drop constraint if exists oversea_person_race_history_surface_check;

alter table oversea_person_race_history
  add constraint oversea_person_race_history_surface_check
  check (surface is null or btrim(surface) <> '');

alter table oversea_person_race_history
  drop constraint if exists oversea_person_race_history_distance_metres_check;

alter table oversea_person_race_history
  add constraint oversea_person_race_history_distance_metres_check
  check (distance_metres is null or distance_metres > 0);

alter table oversea_person_win_rate_stats
  alter column years drop not null;

alter table oversea_person_win_rate_stats
  drop constraint if exists oversea_person_win_rate_stats_source;

alter table oversea_person_win_rate_stats
  add constraint oversea_person_win_rate_stats_source
  check (stats_source in ('jv', 'netkeiba'));

alter table oversea_person_win_rate_stats
  drop constraint if exists oversea_person_win_rate_stats_scope;

alter table oversea_person_win_rate_stats
  add constraint oversea_person_win_rate_stats_scope
  check (scope in ('all_venues_all_conditions', 'all_published_results'));

alter table oversea_person_win_rate_stats
  drop constraint if exists oversea_person_win_rate_stats_years;

alter table oversea_person_win_rate_stats
  add constraint oversea_person_win_rate_stats_years
  check (
    (stats_source = 'jv' and years = 10)
    or (stats_source = 'netkeiba' and years is null)
  );

alter table oversea_person_win_rate_stats
  add column if not exists source_person_id text,
  add column if not exists population_start date,
  add column if not exists population_end date,
  add column if not exists published_population_size integer,
  add column if not exists population_complete boolean not null default false;

alter table oversea_person_win_rate_stats
  drop constraint if exists oversea_person_win_rate_stats_source_person_check;

alter table oversea_person_win_rate_stats
  add constraint oversea_person_win_rate_stats_source_person_check
  check (source_person_id is null or btrim(source_person_id) <> '');

alter table oversea_person_win_rate_stats
  drop constraint if exists oversea_person_win_rate_stats_population_size_check;

alter table oversea_person_win_rate_stats
  add constraint oversea_person_win_rate_stats_population_size_check
  check (published_population_size is null or published_population_size >= starts);
