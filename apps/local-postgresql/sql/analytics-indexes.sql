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

create index concurrently if not exists nvd_ns_ketto_date_idx
  on public.nvd_ns (ketto_toroku_bango, kaisai_nen, kaisai_tsukihi);
