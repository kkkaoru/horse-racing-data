-- The secondary source can omit going for otherwise complete person results.
-- NULL preserves source absence without introducing a magic category string.

begin;

alter table oversea_person_race_history
  alter column going drop not null;

alter table oversea_person_race_history
  drop constraint if exists oversea_person_race_history_going_check;

alter table oversea_person_race_history
  add constraint oversea_person_race_history_going_check
  check (going is null or btrim(going) <> '');

commit;
