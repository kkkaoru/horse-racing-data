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
