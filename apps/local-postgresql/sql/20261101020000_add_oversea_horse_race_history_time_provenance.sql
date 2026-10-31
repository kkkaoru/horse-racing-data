-- Which source published the race time and whether it is the source's own figure
-- ('measured', e.g. a winner's time) or derived from the winner's time and margin
-- ('estimated'). Closing-3F values are only ever stored as published.
alter table oversea_horse_race_history
  add column if not exists race_time_source text,
  add column if not exists race_time_kind text;

alter table oversea_horse_race_history
  drop constraint if exists oversea_horse_race_history_time_kind,
  add constraint oversea_horse_race_history_time_kind check (
    race_time_kind is null or race_time_kind in ('measured', 'estimated')
  );

-- Times filled before this column existed came from the archived netkeiba pages.
update oversea_horse_race_history
set race_time_source = 'netkeiba', race_time_kind = 'measured'
where race_time_text is not null and race_time_source is null;
