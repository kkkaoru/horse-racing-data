-- Existing getSimilarRaceStats population, calculated before the target race:
-- 2016-08-16 through 2026-08-15, all JV/NAR venues and conditions.

insert into oversea_person_win_rate_stats (
  race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
  category, name, current_horse_numbers, stats_source, scope, years,
  calculated_through, starts, horse_count, win_count, quinella_count,
  show_count, win_rate, quinella_rate, show_rate
)
values
  ('jra', '2026', '0816', 'A8', '04', 'jockey', 'モレイラ', '4', 'jv', 'all_venues_all_conditions', 10, '2026-08-15', 831, 658, 235, 378, 468, 28.3, 45.5, 56.3),
  ('jra', '2026', '0816', 'A8', '04', 'jockey', 'スミヨン', '9', 'jv', 'all_venues_all_conditions', 10, '2026-08-15', 170, 151, 27, 46, 78, 15.9, 27.1, 45.9),
  ('jra', '2026', '0816', 'A8', '04', 'jockey', 'ムーア', '10', 'jv', 'all_venues_all_conditions', 10, '2026-08-15', 570, 464, 108, 191, 244, 18.9, 33.5, 42.8),
  ('jra', '2026', '0816', 'A8', '04', 'jockey', 'バシュロ', '2', 'jv', 'all_venues_all_conditions', 10, '2026-08-15', 118, 104, 13, 34, 48, 11.0, 28.8, 40.7),
  ('jra', '2026', '0816', 'A8', '04', 'jockey', '武豊', '3', 'jv', 'all_venues_all_conditions', 10, '2026-08-15', 6109, 2759, 873, 1677, 2347, 14.3, 27.5, 38.4),
  ('jra', '2026', '0816', 'A8', '04', 'jockey', 'マーカン', '5', 'jv', 'all_venues_all_conditions', 10, '2026-08-15', 548, 478, 73, 138, 195, 13.3, 25.2, 35.6),
  ('jra', '2026', '0816', 'A8', '04', 'jockey', 'バルザロ', '8', 'jv', 'all_venues_all_conditions', 10, '2026-08-15', 194, 170, 19, 45, 56, 9.8, 23.2, 28.9),
  ('jra', '2026', '0816', 'A8', '04', 'jockey', 'ルメート', '7', 'jv', 'all_venues_all_conditions', 10, '2026-08-15', 249, 210, 20, 43, 62, 8.0, 17.3, 24.9),
  ('jra', '2026', '0816', 'A8', '04', 'jockey', 'ギュイヨ', '6', 'jv', 'all_venues_all_conditions', 10, '2026-08-15', 46, 42, 7, 9, 11, 15.2, 19.6, 23.9),
  ('jra', '2026', '0816', 'A8', '04', 'owner', 'キャロットファーム', '3, 4', 'jv', 'all_venues_all_conditions', 10, '2026-08-15', 10453, 1027, 1410, 2601, 3596, 13.5, 24.9, 34.4),
  ('jra', '2026', '0816', 'A8', '04', 'trainer', 'グラファ', '8', 'jv', 'all_venues_all_conditions', 10, '2026-08-15', 30, 24, 3, 7, 11, 10.0, 23.3, 36.7),
  ('jra', '2026', '0816', 'A8', '04', 'trainer', 'ハガス', '5', 'jv', 'all_venues_all_conditions', 10, '2026-08-15', 30, 30, 5, 9, 11, 16.7, 30.0, 36.7),
  ('jra', '2026', '0816', 'A8', '04', 'trainer', '田中博康', '3', 'jv', 'all_venues_all_conditions', 10, '2026-08-15', 1853, 260, 267, 472, 648, 14.4, 25.5, 35.0),
  ('jra', '2026', '0816', 'A8', '04', 'trainer', '武井亮', '4', 'jv', 'all_venues_all_conditions', 10, '2026-08-15', 2782, 314, 239, 483, 721, 8.6, 17.4, 25.9),
  ('jra', '2026', '0816', 'A8', '04', 'trainer', 'オブライ', '10', 'jv', 'all_venues_all_conditions', 10, '2026-08-15', 197, 180, 14, 31, 43, 7.1, 15.7, 21.8)
on conflict (
  race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
  category, name
)
do update set
  current_horse_numbers = excluded.current_horse_numbers,
  stats_source = excluded.stats_source,
  scope = excluded.scope,
  years = excluded.years,
  calculated_through = excluded.calculated_through,
  starts = excluded.starts,
  horse_count = excluded.horse_count,
  win_count = excluded.win_count,
  quinella_count = excluded.quinella_count,
  show_count = excluded.show_count,
  win_rate = excluded.win_rate,
  quinella_rate = excluded.quinella_rate,
  show_rate = excluded.show_rate,
  updated_at = now();

update oversea_person_win_rate_stats
set calculated_at = '2026-08-15T09:33:00+09:00'
where race_source = 'jra'
  and kaisai_nen = '2026'
  and kaisai_tsukihi = '0816'
  and keibajo_code = 'A8'
  and race_bango = '04';
