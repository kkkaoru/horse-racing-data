-- Race time and closing-3F time published on the archived horse pages. Nullable:
-- overseas pages often leave them blank, and rows are filled only where empty.
alter table oversea_horse_race_history
  add column if not exists race_time_text text,
  add column if not exists race_time_seconds numeric(7, 2),
  add column if not exists last_three_furlongs_seconds numeric(4, 1);

alter table oversea_horse_race_history
  drop constraint if exists oversea_horse_race_history_time_positive,
  add constraint oversea_horse_race_history_time_positive check (
    race_time_seconds is null or race_time_seconds > 0
  ),
  drop constraint if exists oversea_horse_race_history_last_three_positive,
  add constraint oversea_horse_race_history_last_three_positive check (
    last_three_furlongs_seconds is null or last_three_furlongs_seconds > 0
  );
