COPY (
  WITH seed_horses AS MATERIALIZED (
    SELECT DISTINCT ketto_toroku_bango
    FROM race_entry_corner_features
    WHERE source = 'nar' AND keibajo_code IN ('54', '55', '83')
      AND race_date >= '20000101' AND race_date < '20260912'
    UNION
    SELECT ketto_toroku_bango FROM nvd_se
    WHERE kaisai_nen = '2026' AND kaisai_tsukihi = '0912'
      AND keibajo_code IN ('54', '55', '83')
  ), history_races AS MATERIALIZED (
    SELECT DISTINCT h.source, h.kaisai_nen, h.kaisai_tsukihi, h.keibajo_code, h.race_bango
    FROM race_entry_corner_features h
    INNER JOIN seed_horses s USING (ketto_toroku_bango)
    WHERE h.race_date < '20260912'
  )
  SELECT h.source, h.race_date, h.kaisai_nen, h.kaisai_tsukihi,
    h.keibajo_code, h.race_bango, h.ketto_toroku_bango, h.umaban,
    h.track_code, h.kyori, h.shusso_tosu, h.grade_code,
    h.kyoso_shubetsu_code, h.kyoso_joken_code,
    h.babajotai_code_shiba, h.babajotai_code_dirt,
    h.seibetsu_code, h.barei, h.futan_juryo,
    h.kishumei_ryakusho, h.chokyoshimei_ryakusho,
    h.finish_position, h.soha_time, h.time_sa, h.kohan_3f,
    h.tansho_odds, h.tansho_ninkijun
  FROM race_entry_corner_features h
  INNER JOIN history_races r USING (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);
