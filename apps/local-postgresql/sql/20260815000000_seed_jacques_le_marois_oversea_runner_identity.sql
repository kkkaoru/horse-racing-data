-- Canonical JRA-VAN World identities for the 2026 Prix Jacques le Marois entries.
--
-- jvd_se uses the shared 0000000000 placeholder for eight overseas runners.
-- Keep JRA fixed-width keys untouched and attach external identity/profile data
-- only through the race-entry-scoped auxiliary table.

insert into oversea_runner_identity (
  race_source,
  kaisai_nen,
  kaisai_tsukihi,
  keibajo_code,
  race_bango,
  umaban,
  source,
  source_horse_id,
  horse_name_full,
  jockey_name_full,
  trainer_name_full,
  owner_name_full,
  source_url
)
values
  ('jra', '2026', '0816', 'A8', '04', '01', 'jra-van', 'H1021714', 'Zeus Olympios', 'C．リー', 'K．バーク', 'Exors Of The Late Sheikh Mohammed Obaid', 'https://world.jra-van.jp/db/horse/H1021714/'),
  ('jra', '2026', '0816', 'A8', '04', '02', 'jra-van', 'H1021966', 'Dreamliner', 'T．バシュロ', 'S．ワッテル', 'Patrick Hugh Betts', 'https://world.jra-van.jp/db/horse/H1021966/'),
  ('jra', '2026', '0816', 'A8', '04', '03', 'jra-van', 'H1019915', 'Sixpence', '武豊', '田中博康', 'キャロットファーム', 'https://world.jra-van.jp/db/horse/H1019915/'),
  ('jra', '2026', '0816', 'A8', '04', '04', 'jra-van', 'H1021505', 'Strauss', 'J．モレイラ', '武井亮', 'キャロットファーム', 'https://world.jra-van.jp/db/horse/H1021505/'),
  ('jra', '2026', '0816', 'A8', '04', '05', 'jra-van', 'H1020450', 'More Thunder', 'T．マーカンド', 'W．ハガス', 'Mr Saeed Suhail', 'https://world.jra-van.jp/db/horse/H1020450/'),
  ('jra', '2026', '0816', 'A8', '04', '06', 'jra-van', 'H1019555', 'No Lunch', 'M．ギュイヨン', 'C．フェルラン', 'ヴェルテメール・エ・フレール', 'https://world.jra-van.jp/db/horse/H1019555/'),
  ('jra', '2026', '0816', 'A8', '04', '07', 'jra-van', 'H1021961', 'Sir Tommy Cen', 'A．ルメートル', 'P．ヴァルディヴィエルソ', 'Yeguada Centurion Slu', 'https://world.jra-van.jp/db/horse/H1021961/'),
  ('jra', '2026', '0816', 'A8', '04', '08', 'jra-van', 'H1020430', 'Rayif', 'M．バルザローナ', 'F．グラファール', 'Aga Khan Studs SC', 'https://world.jra-van.jp/db/horse/H1020430/'),
  ('jra', '2026', '0816', 'A8', '04', '09', 'jra-van', 'H1021658', 'Thesecretadversary', 'C．スミヨン', 'J．スタック', 'Cayton Park Stud & Mrs John Magnier', 'https://world.jra-van.jp/db/horse/H1021658/'),
  ('jra', '2026', '0816', 'A8', '04', '10', 'jra-van', 'H1020328', 'Precise', 'R．ムーア', 'A．オブライエン', 'Mrs John Magnier, Michael B Tabor, Derrick Smith & Westerberg', 'https://world.jra-van.jp/db/horse/H1020328/')
on conflict (race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban)
do update set
  source = excluded.source,
  source_horse_id = excluded.source_horse_id,
  horse_name_full = excluded.horse_name_full,
  jockey_name_full = excluded.jockey_name_full,
  trainer_name_full = excluded.trainer_name_full,
  owner_name_full = excluded.owner_name_full,
  source_url = excluded.source_url,
  updated_at = now();
