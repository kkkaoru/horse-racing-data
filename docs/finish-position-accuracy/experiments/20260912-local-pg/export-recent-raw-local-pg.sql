COPY (
  SELECT 'nar' AS source, r.kaisai_nen||r.kaisai_tsukihi AS race_date,
    r.kaisai_nen,r.kaisai_tsukihi,r.keibajo_code,r.race_bango,
    s.ketto_toroku_bango,NULLIF(trim(s.umaban),'')::int AS umaban,
    r.track_code,NULLIF(trim(r.kyori),'')::int AS kyori,
    COALESCE(NULLIF(NULLIF(trim(r.shusso_tosu),''),'00')::int,
      count(*) OVER(PARTITION BY r.kaisai_nen,r.kaisai_tsukihi,r.keibajo_code,r.race_bango)) AS shusso_tosu,
    r.grade_code,r.kyoso_shubetsu_code,r.kyoso_joken_code,
    r.babajotai_code_shiba,r.babajotai_code_dirt,
    s.seibetsu_code,NULLIF(trim(s.barei),'')::int AS barei,
    NULL::numeric AS futan_juryo,
    s.kishumei_ryakusho,s.chokyoshimei_ryakusho,
    NULLIF(NULLIF(trim(s.kakutei_chakujun),''),'00')::int AS finish_position,
    NULLIF(NULLIF(trim(s.soha_time),''),'0000')::int AS soha_time,
    CASE WHEN trim(s.time_sa) ~ '^-?[0-9]+$' THEN trim(s.time_sa)::numeric/10 END AS time_sa,
    NULLIF(NULLIF(trim(s.kohan_3f),''),'000')::numeric/10 AS kohan_3f,
    NULLIF(NULLIF(trim(s.tansho_odds),''),'0000')::numeric/10 AS tansho_odds,
    NULLIF(NULLIF(trim(s.tansho_ninkijun),''),'00')::int AS tansho_ninkijun
  FROM nvd_ra r INNER JOIN nvd_se s
    USING(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)
  WHERE r.kaisai_nen='2026' AND r.kaisai_tsukihi<'0912'
) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);
