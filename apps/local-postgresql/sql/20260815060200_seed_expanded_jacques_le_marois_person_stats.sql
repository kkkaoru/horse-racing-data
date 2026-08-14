-- Row-specific populations for A8/04. JV and netkeiba populations remain
-- separate and expose their source/scope instead of being added together.
-- Every netkeiba row is based on all pages published for that source person.

update oversea_person_win_rate_stats
set source_person_id = case name
    when 'モレイラ' then '05509'
    when 'スミヨン' then '05271'
    when 'ムーア' then '05366'
    when 'バシュロ' then '05621'
    when '武豊' then '00666'
    when 'マーカン' then '05626'
    when 'バルザロ' then '05504'
    when 'ルメート' then '05659'
    when 'ギュイヨ' then '05464'
  end,
  population_start = '2016-08-16',
  population_end = '2026-08-15',
  published_population_size = starts,
  population_complete = true,
  updated_at = now()
where race_source = 'jra'
  and kaisai_nen = '2026'
  and kaisai_tsukihi = '0816'
  and keibajo_code = 'A8'
  and race_bango = '04'
  and category = 'jockey';

insert into oversea_person_win_rate_stats (
  race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
  category, name, current_horse_numbers, stats_source, scope, years,
  minimum_starts, calculated_through, calculated_at, source_person_id,
  population_start, population_end, published_population_size,
  population_complete, starts, horse_count, win_count, quinella_count,
  show_count, win_rate, quinella_rate, show_rate
)
values
  ('jra', '2026', '0816', 'A8', '04', 'owner', 'キャロットファーム', '3, 4', 'jv', 'all_venues_all_conditions', 10, 20, '2026-08-15', '2026-08-15T08:15:00+09:00', '486800', '2016-08-16', '2026-08-15', 10453, true, 10453, 1027, 1410, 2601, 3596, 13.5, 24.9, 34.4),
  ('jra', '2026', '0816', 'A8', '04', 'owner', 'ヴェルテメール・エ・フレール', '6', 'netkeiba', 'all_published_results', null, 20, '2026-08-15', '2026-08-15T08:15:00+09:00', 'a0006d', '2000-11-04', '2026-04-26', 42, true, 42, 12, 15, 20, 26, 35.7, 47.6, 61.9),
  ('jra', '2026', '0816', 'A8', '04', 'owner', 'Mr Saeed Suhail', '5', 'netkeiba', 'all_published_results', null, 20, '2026-08-15', '2026-08-15T08:15:00+09:00', 'a000ee', '2015-10-21', '2022-06-18', 25, true, 25, 5, 12, 18, 19, 48.0, 72.0, 76.0),
  ('jra', '2026', '0816', 'A8', '04', 'trainer', '田中博康', '3', 'jv', 'all_venues_all_conditions', 10, 20, '2026-08-15', '2026-08-15T08:15:00+09:00', '01162', '2016-08-16', '2026-08-15', 1853, true, 1853, 260, 267, 472, 648, 14.4, 25.5, 35.0),
  ('jra', '2026', '0816', 'A8', '04', 'trainer', '武井亮', '4', 'jv', 'all_venues_all_conditions', 10, 20, '2026-08-15', '2026-08-15T08:15:00+09:00', '01147', '2016-08-16', '2026-08-15', 2782, true, 2782, 314, 239, 483, 721, 8.6, 17.4, 25.9),
  ('jra', '2026', '0816', 'A8', '04', 'trainer', 'オブライ', '10', 'netkeiba', 'all_published_results', null, 20, '2026-08-15', '2026-08-15T08:15:00+09:00', '05518', '2000-10-28', '2026-08-09', 2015, true, 2015, 414, 508, 817, 1052, 25.2, 40.5, 52.2),
  ('jra', '2026', '0816', 'A8', '04', 'trainer', 'ハガス', '5', 'netkeiba', 'all_published_results', null, 20, '2026-08-15', '2026-08-15T08:15:00+09:00', '05665', '2003-04-27', '2026-08-09', 464, true, 464, 131, 117, 197, 246, 25.2, 42.5, 53.0),
  ('jra', '2026', '0816', 'A8', '04', 'trainer', 'グラファ', '8', 'netkeiba', 'all_published_results', null, 20, '2026-08-15', '2026-08-15T08:15:00+09:00', '05701', '2015-03-19', '2026-08-09', 461, true, 461, 130, 118, 188, 262, 25.6, 40.8, 56.8),
  ('jra', '2026', '0816', 'A8', '04', 'trainer', 'ワッテル', '2', 'netkeiba', 'all_published_results', null, 20, '2026-08-15', '2026-08-15T08:15:00+09:00', '05764', '2021-10-01', '2026-06-20', 72, true, 72, 27, 12, 22, 31, 16.7, 30.6, 43.1),
  ('jra', '2026', '0816', 'A8', '04', 'trainer', 'K．バー', '1', 'netkeiba', 'all_published_results', null, 20, '2026-08-15', '2026-08-15T08:15:00+09:00', 'a031e', '2015-10-04', '2026-08-02', 360, true, 360, 119, 53, 93, 134, 14.7, 25.8, 37.2),
  ('jra', '2026', '0816', 'A8', '04', 'trainer', 'C．フェ', '6', 'netkeiba', 'all_published_results', null, 20, '2026-08-15', '2026-08-15T08:15:00+09:00', 'a064b', '2023-09-20', '2026-03-15', 26, true, 26, 12, 9, 16, 18, 34.6, 61.5, 69.2)
on conflict (
  race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
  category, name
)
do update set
  current_horse_numbers = excluded.current_horse_numbers,
  stats_source = excluded.stats_source,
  scope = excluded.scope,
  years = excluded.years,
  minimum_starts = excluded.minimum_starts,
  calculated_through = excluded.calculated_through,
  calculated_at = excluded.calculated_at,
  source_person_id = excluded.source_person_id,
  population_start = excluded.population_start,
  population_end = excluded.population_end,
  published_population_size = excluded.published_population_size,
  population_complete = excluded.population_complete,
  starts = excluded.starts,
  horse_count = excluded.horse_count,
  win_count = excluded.win_count,
  quinella_count = excluded.quinella_count,
  show_count = excluded.show_count,
  win_rate = excluded.win_rate,
  quinella_rate = excluded.quinella_rate,
  show_rate = excluded.show_rate,
  updated_at = now();
