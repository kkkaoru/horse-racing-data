-- Indexes for pc-keiba-viewer race detail analytics.
-- Run outside an explicit transaction because CONCURRENTLY cannot run in one.

create index concurrently if not exists jvd_ra_detail_stats_idx
  on public.jvd_ra (
    keibajo_code,
    kyoso_shubetsu_code,
    kyoso_kigo_code,
    kaisai_nen,
    kaisai_tsukihi,
    race_bango
  )
  include (
    kyori,
    track_code,
    juryo_shubetsu_code,
    kyoso_joken_code,
    kyoso_joken_meisho,
    kyosomei_hondai,
    kyosomei_fukudai,
    kyosomei_kakkonai,
    grade_code
  );

create index concurrently if not exists nvd_ra_detail_stats_idx
  on public.nvd_ra (
    keibajo_code,
    kyoso_shubetsu_code,
    kyoso_kigo_code,
    kaisai_nen,
    kaisai_tsukihi,
    race_bango
  )
  include (
    kyori,
    track_code,
    juryo_shubetsu_code,
    kyoso_joken_code,
    kyoso_joken_meisho,
    kyosomei_hondai,
    kyosomei_fukudai,
    kyosomei_kakkonai,
    grade_code
  );

create index concurrently if not exists jvd_ra_detail_stats_date_idx
  on public.jvd_ra (
    (kaisai_nen || kaisai_tsukihi),
    kyori,
    track_code,
    kyoso_shubetsu_code,
    kyoso_kigo_code,
    juryo_shubetsu_code,
    race_bango
  )
  include (
    keibajo_code,
    kyoso_joken_code,
    kyoso_joken_meisho,
    kyosomei_hondai,
    kyosomei_fukudai,
    kyosomei_kakkonai,
    grade_code
  );

create index concurrently if not exists nvd_ra_detail_stats_date_idx
  on public.nvd_ra (
    (kaisai_nen || kaisai_tsukihi),
    kyori,
    track_code,
    kyoso_shubetsu_code,
    kyoso_kigo_code,
    juryo_shubetsu_code,
    race_bango
  )
  include (
    keibajo_code,
    kyoso_joken_code,
    kyoso_joken_meisho,
    kyosomei_hondai,
    kyosomei_fukudai,
    kyosomei_kakkonai,
    grade_code
  );

create index concurrently if not exists jvd_ra_start_key_idx
  on public.jvd_ra ((kaisai_nen || kaisai_tsukihi || coalesce(nullif(hasso_jikoku, ''), '0000')))
  include (
    keibajo_code,
    race_bango,
    kyori,
    track_code,
    hasso_jikoku,
    shusso_tosu,
    kyosomei_hondai,
    kyosomei_fukudai,
    grade_code,
    kyoso_shubetsu_code,
    kyoso_kigo_code,
    juryo_shubetsu_code,
    kyoso_joken_code,
    kyoso_joken_meisho
  );

create index concurrently if not exists nvd_ra_start_key_idx
  on public.nvd_ra ((kaisai_nen || kaisai_tsukihi || coalesce(nullif(hasso_jikoku, ''), '0000')))
  include (
    keibajo_code,
    race_bango,
    kyori,
    track_code,
    hasso_jikoku,
    shusso_tosu,
    kyosomei_hondai,
    kyosomei_fukudai,
    grade_code,
    kyoso_shubetsu_code,
    kyoso_kigo_code,
    juryo_shubetsu_code,
    kyoso_joken_code,
    kyoso_joken_meisho
  );

create index concurrently if not exists nvd_ns_ketto_date_idx
  on public.nvd_ns (ketto_toroku_bango, kaisai_nen, kaisai_tsukihi);

create index concurrently if not exists jvd_se_horse_list_idx
  on public.jvd_se (ketto_toroku_bango, kaisai_nen desc, kaisai_tsukihi desc, race_bango desc)
  include (bamei, kakutei_chakujun, keibajo_code);

create index concurrently if not exists nvd_se_horse_list_idx
  on public.nvd_se (ketto_toroku_bango, kaisai_nen desc, kaisai_tsukihi desc, race_bango desc)
  include (bamei, kakutei_chakujun, keibajo_code);

create index concurrently if not exists jvd_se_horse_results_idx
  on public.jvd_se (ketto_toroku_bango, kaisai_nen desc, kaisai_tsukihi desc, race_bango desc)
  include (
    keibajo_code,
    wakuban,
    umaban,
    bamei,
    seibetsu_code,
    barei,
    futan_juryo,
    kishumei_ryakusho,
    chokyoshimei_ryakusho,
    banushimei,
    bataiju,
    zogen_fugo,
    zogen_sa,
    kakutei_chakujun,
    tansho_odds,
    tansho_ninkijun,
    soha_time,
    time_sa,
    kohan_3f
  );

create index concurrently if not exists nvd_se_horse_results_idx
  on public.nvd_se (ketto_toroku_bango, kaisai_nen desc, kaisai_tsukihi desc, race_bango desc)
  include (
    keibajo_code,
    wakuban,
    umaban,
    bamei,
    seibetsu_code,
    barei,
    futan_juryo,
    kishumei_ryakusho,
    chokyoshimei_ryakusho,
    banushimei,
    bataiju,
    zogen_fugo,
    zogen_sa,
    kakutei_chakujun,
    tansho_odds,
    tansho_ninkijun,
    soha_time,
    time_sa,
    kohan_3f
  );

create index concurrently if not exists jvd_um_bloodline_lookup_idx
  on public.jvd_um (ketto_toroku_bango)
  include (ketto_joho_01b, ketto_joho_03b, ketto_joho_05b);

create index concurrently if not exists nvd_um_bloodline_lookup_idx
  on public.nvd_um (ketto_toroku_bango)
  include (ketto_joho_01b, ketto_joho_03b, ketto_joho_05b);

create index concurrently if not exists nvd_nu_bloodline_lookup_idx
  on public.nvd_nu (ketto_toroku_bango)
  include (ketto_joho_01b, ketto_joho_03b, ketto_joho_05b);

create index concurrently if not exists jvd_um_sire_name_lookup_idx
  on public.jvd_um ((nullif(regexp_replace(ketto_joho_01b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')))
  include (ketto_toroku_bango, ketto_joho_03b, ketto_joho_05b);

create index concurrently if not exists jvd_um_sire_sire_name_lookup_idx
  on public.jvd_um ((nullif(regexp_replace(ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')))
  include (ketto_toroku_bango, ketto_joho_01b, ketto_joho_05b);

create index concurrently if not exists jvd_um_dam_sire_name_lookup_idx
  on public.jvd_um ((nullif(regexp_replace(ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')))
  include (ketto_toroku_bango, ketto_joho_01b, ketto_joho_03b);

create index concurrently if not exists jvd_um_bamei_bloodline_lookup_idx
  on public.jvd_um ((nullif(regexp_replace(bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')))
  include (ketto_toroku_bango, ketto_joho_01b, ketto_joho_03b, ketto_joho_05b);

create index concurrently if not exists nvd_um_sire_name_lookup_idx
  on public.nvd_um ((nullif(regexp_replace(ketto_joho_01b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')))
  include (ketto_toroku_bango, ketto_joho_03b, ketto_joho_05b);

create index concurrently if not exists nvd_um_sire_sire_name_lookup_idx
  on public.nvd_um ((nullif(regexp_replace(ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')))
  include (ketto_toroku_bango, ketto_joho_01b, ketto_joho_05b);

create index concurrently if not exists nvd_um_dam_sire_name_lookup_idx
  on public.nvd_um ((nullif(regexp_replace(ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')))
  include (ketto_toroku_bango, ketto_joho_01b, ketto_joho_03b);

create index concurrently if not exists nvd_um_bamei_bloodline_lookup_idx
  on public.nvd_um ((nullif(regexp_replace(bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')))
  include (ketto_toroku_bango, ketto_joho_01b, ketto_joho_03b, ketto_joho_05b);

create index concurrently if not exists nvd_nu_sire_name_lookup_idx
  on public.nvd_nu ((nullif(regexp_replace(ketto_joho_01b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')))
  include (ketto_toroku_bango, ketto_joho_03b, ketto_joho_05b);

create index concurrently if not exists nvd_nu_sire_sire_name_lookup_idx
  on public.nvd_nu ((nullif(regexp_replace(ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')))
  include (ketto_toroku_bango, ketto_joho_01b, ketto_joho_05b);

create index concurrently if not exists nvd_nu_dam_sire_name_lookup_idx
  on public.nvd_nu ((nullif(regexp_replace(ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')))
  include (ketto_toroku_bango, ketto_joho_01b, ketto_joho_03b);

create index concurrently if not exists nvd_nu_bamei_bloodline_lookup_idx
  on public.nvd_nu ((nullif(regexp_replace(bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')))
  include (ketto_toroku_bango, ketto_joho_01b, ketto_joho_03b, ketto_joho_05b);

create index concurrently if not exists jvd_se_race_stats_idx
  on public.jvd_se (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
  include (
    wakuban,
    umaban,
    ketto_toroku_bango,
    bamei,
    kakutei_chakujun,
    soha_time,
    kohan_3f,
    tansho_ninkijun,
    tansho_odds,
    kishumei_ryakusho,
    chokyoshimei_ryakusho,
    banushimei
  );

create index concurrently if not exists nvd_se_race_stats_idx
  on public.nvd_se (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
  include (
    wakuban,
    umaban,
    ketto_toroku_bango,
    bamei,
    kakutei_chakujun,
    soha_time,
    kohan_3f,
    tansho_ninkijun,
    tansho_odds,
    kishumei_ryakusho,
    chokyoshimei_ryakusho,
    banushimei
  );

create index concurrently if not exists jvd_hr_race_stats_idx
  on public.jvd_hr (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango);

create index concurrently if not exists nvd_hr_race_stats_idx
  on public.nvd_hr (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango);

-- Indexes for pc-keiba-viewer horse / jockey / trainer list pages.
-- These keep latest-list rendering fast while still showing all-time stats.

create index concurrently if not exists jvd_se_jockey_name_stats_idx
  on public.jvd_se ((coalesce(nullif(btrim(kishumei_ryakusho, ' 　'), ''), '-')))
  include (kakutei_chakujun);

create index concurrently if not exists nvd_se_jockey_name_stats_idx
  on public.nvd_se ((coalesce(nullif(btrim(kishumei_ryakusho, ' 　'), ''), '-')))
  include (kakutei_chakujun);

create index concurrently if not exists jvd_se_trainer_name_stats_idx
  on public.jvd_se ((coalesce(nullif(btrim(chokyoshimei_ryakusho, ' 　'), ''), '-')))
  include (kakutei_chakujun);

create index concurrently if not exists nvd_se_trainer_name_stats_idx
  on public.nvd_se ((coalesce(nullif(btrim(chokyoshimei_ryakusho, ' 　'), ''), '-')))
  include (kakutei_chakujun);

create index concurrently if not exists jvd_se_jockey_name_date_stats_idx
  on public.jvd_se ((coalesce(nullif(btrim(kishumei_ryakusho, ' 　'), ''), '-')), kaisai_nen, kaisai_tsukihi)
  include (kakutei_chakujun);

create index concurrently if not exists nvd_se_jockey_name_date_stats_idx
  on public.nvd_se ((coalesce(nullif(btrim(kishumei_ryakusho, ' 　'), ''), '-')), kaisai_nen, kaisai_tsukihi)
  include (kakutei_chakujun);

create index concurrently if not exists jvd_se_trainer_name_date_stats_idx
  on public.jvd_se ((coalesce(nullif(btrim(chokyoshimei_ryakusho, ' 　'), ''), '-')), kaisai_nen, kaisai_tsukihi)
  include (kakutei_chakujun);

create index concurrently if not exists nvd_se_trainer_name_date_stats_idx
  on public.nvd_se ((coalesce(nullif(btrim(chokyoshimei_ryakusho, ' 　'), ''), '-')), kaisai_nen, kaisai_tsukihi)
  include (kakutei_chakujun);

create index concurrently if not exists jvd_se_owner_name_stats_idx
  on public.jvd_se ((coalesce(nullif(btrim(banushimei, ' 　'), ''), '-')), kaisai_nen, kaisai_tsukihi)
  include (kakutei_chakujun);

create index concurrently if not exists nvd_se_owner_name_stats_idx
  on public.nvd_se ((coalesce(nullif(btrim(banushimei, ' 　'), ''), '-')), kaisai_nen, kaisai_tsukihi)
  include (kakutei_chakujun);
