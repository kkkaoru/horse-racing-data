-- Source-native identities for the final ten 2026 Prix Jacques le Marois runners.
-- JRA-VAN horse IDs connect the canonical entries to imported histories.
-- netkeiba IDs come from the refreshed final racecard and horse profiles; the
-- earlier preliminary card had sixteen nominations and omitted numbers/riders.

begin;

insert into oversea_runner_source_id (
  race_source,
  kaisai_nen,
  kaisai_tsukihi,
  keibajo_code,
  race_bango,
  umaban,
  source,
  source_horse_id,
  source_jockey_id,
  source_trainer_id,
  source_owner_id,
  gate_number,
  source_url
)
values
  ('jra', '2026', '0816', 'A8', '04', '01', 'jra-van', 'H1021714', null, null, null, 6, 'https://world.jra-van.jp/db/horse/H1021714/'),
  ('jra', '2026', '0816', 'A8', '04', '02', 'jra-van', 'H1021966', null, null, null, 2, 'https://world.jra-van.jp/db/horse/H1021966/'),
  ('jra', '2026', '0816', 'A8', '04', '03', 'jra-van', 'H1019915', null, null, null, 5, 'https://world.jra-van.jp/db/horse/H1019915/'),
  ('jra', '2026', '0816', 'A8', '04', '04', 'jra-van', 'H1021505', null, null, null, 1, 'https://world.jra-van.jp/db/horse/H1021505/'),
  ('jra', '2026', '0816', 'A8', '04', '05', 'jra-van', 'H1020450', null, null, null, 8, 'https://world.jra-van.jp/db/horse/H1020450/'),
  ('jra', '2026', '0816', 'A8', '04', '06', 'jra-van', 'H1019555', null, null, null, 3, 'https://world.jra-van.jp/db/horse/H1019555/'),
  ('jra', '2026', '0816', 'A8', '04', '07', 'jra-van', 'H1021961', null, null, null, 7, 'https://world.jra-van.jp/db/horse/H1021961/'),
  ('jra', '2026', '0816', 'A8', '04', '08', 'jra-van', 'H1020430', null, null, null, 10, 'https://world.jra-van.jp/db/horse/H1020430/'),
  ('jra', '2026', '0816', 'A8', '04', '09', 'jra-van', 'H1021658', null, null, null, 9, 'https://world.jra-van.jp/db/horse/H1021658/'),
  ('jra', '2026', '0816', 'A8', '04', '10', 'jra-van', 'H1020328', null, null, null, 4, 'https://world.jra-van.jp/db/horse/H1020328/'),
  ('jra', '2026', '0816', 'A8', '04', '01', 'netkeiba', '000a02d639', 'a0583', 'a031e', 'a00b09', 6, 'https://db.netkeiba.com/horse/000a02d639/'),
  ('jra', '2026', '0816', 'A8', '04', '02', 'netkeiba', '000a029c22', '05621', '05764', 'a00768', 2, 'https://db.netkeiba.com/horse/000a029c22/'),
  ('jra', '2026', '0816', 'A8', '04', '03', 'netkeiba', '2021105724', '00666', '01162', '486800', 5, 'https://db.netkeiba.com/horse/2021105724/'),
  ('jra', '2026', '0816', 'A8', '04', '04', 'netkeiba', '2021105744', '05509', '01147', '486800', 1, 'https://db.netkeiba.com/horse/2021105744/'),
  ('jra', '2026', '0816', 'A8', '04', '05', 'netkeiba', '000a02d00c', '05626', '05665', 'a000ee', 8, 'https://db.netkeiba.com/horse/000a02d00c/'),
  ('jra', '2026', '0816', 'A8', '04', '06', 'netkeiba', '000a027210', '05464', 'a064b', 'a0006d', 3, 'https://db.netkeiba.com/horse/000a027210/'),
  ('jra', '2026', '0816', 'A8', '04', '07', 'netkeiba', '000a02d629', '05659', 'a0745', 'a004e6', 7, 'https://db.netkeiba.com/horse/000a02d629/'),
  ('jra', '2026', '0816', 'A8', '04', '08', 'netkeiba', '000a02ca97', '05504', '05701', 'a00762', 10, 'https://db.netkeiba.com/horse/000a02ca97/'),
  ('jra', '2026', '0816', 'A8', '04', '09', 'netkeiba', '000a02d63e', '05271', 'a0746', 'a00b0a', 9, 'https://db.netkeiba.com/horse/000a02d63e/'),
  ('jra', '2026', '0816', 'A8', '04', '10', 'netkeiba', '000a02ca51', '05366', '05518', 'a004ec', 4, 'https://db.netkeiba.com/horse/000a02ca51/')
on conflict (race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban, source)
do update set
  source_horse_id = excluded.source_horse_id,
  source_jockey_id = excluded.source_jockey_id,
  source_trainer_id = excluded.source_trainer_id,
  source_owner_id = excluded.source_owner_id,
  gate_number = excluded.gate_number,
  source_url = excluded.source_url,
  updated_at = now();

commit;
