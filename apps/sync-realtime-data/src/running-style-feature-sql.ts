// Run with bun. PostgreSQL/Hyperdrive feature builder for one running-style
// race. This mirrors the 117-feature DuckDB pipeline at race granularity so it
// can be timed inside a Worker before switching cron/queue production writes.

import type { Pool } from "pg";

import {
  buildRunningStyleRaceKey,
  normalizeKeibajoCode,
  normalizeRaceBango,
  type RunningStyleRaceParams,
  type RunningStyleSource,
} from "./running-style-features";
import type { RaceHorseFeatureRow } from "./running-style-r2";

const RECENT_WINDOW_SIZE = 5;
const SAME_DISTANCE_TOLERANCE = 200;
const HISTORY_LOOKBACK_YEARS = 10;
const CONSECUTIVE_RACE_WINDOW_DAYS = 30;
const JOCKEY_RECENT_DAYS = 60;
const TRACK_BIAS_WINDOW_DAYS = 5;
const FRONT_CORNER_THRESHOLD = 0.33;
const RIVAL_DISTANCE_THRESHOLD = 0.3;
const MAX_FIELD_SIZE = 18;
const DISTANCE_BAND_METERS = 400;
const PEDIGREE_MIN_RACES = 5;
const PEDIGREE_COMPOSITE_DIVISOR = 3;
const TREND_MIN_RACES = 3;
const RUNNING_STYLE_SENKOU_THRESHOLD = 0.3;
const RUNNING_STYLE_SASHI_THRESHOLD = 0.7;
const RUNNING_STYLE_CLASS_NIGE = 0;
const RUNNING_STYLE_CLASS_SENKOU = 1;
const RUNNING_STYLE_CLASS_SASHI = 2;
const RUNNING_STYLE_CLASS_OIKOMI = 3;
const KYORI_BAND_SPRINT_MAX = 1300;
const KYORI_BAND_MILE_MAX = 1700;
const KYORI_BAND_INTERMEDIATE_MAX = 2200;
const KYORI_BAND_SPRINT = 0;
const KYORI_BAND_MILE = 1;
const KYORI_BAND_INTERMEDIATE = 2;
const KYORI_BAND_LONG = 3;
const SEASON_SPRING_MAX_MONTH = 5;
const SEASON_SUMMER_MAX_MONTH = 8;
const SEASON_AUTUMN_MAX_MONTH = 11;
const SEASON_SPRING = 0;
const SEASON_SUMMER = 1;
const SEASON_AUTUMN = 2;
const SEASON_WINTER = 3;
const NEWCOMER_RACE_JOKEN_CODE = "000";
const UMABAN_NORM_MIN_FIELD = 2;

const BATCH_SOURCE_WHITELIST = new Set<RunningStyleSource>(["jra", "nar"]);
const BATCH_DATE_PATTERN = /^\d{8}$/;
const BATCH_FEATURE_SCHEMA_VERSION_PATTERN = /^[A-Za-z0-9_.-]+$/;

const PEER_INPUT_COLUMNS = {
  career_win_rate: "careerWinRate",
  kohan3f_avg_5: "kohan3fAvg5",
  past_corner_1_norm_avg_5: "pastCorner1NormAvg5",
  past_first_3f_avg_5: "pastFirst3fAvg5",
  past_nige_rate_self: "pastNigeRate",
  past_oikomi_rate_self: "pastOikomiRate",
  past_sashi_rate_self: "pastSashiRate",
  past_senkou_rate_self: "pastSenkouRate",
  speed_index_avg_5: "speedIndexAvg5",
  speed_index_best_5: "speedIndexBest5",
} as const;

type SqlRow = Record<string, unknown>;
type PeerInputKey = (typeof PEER_INPUT_COLUMNS)[keyof typeof PEER_INPUT_COLUMNS];

const SAFE_BATAIJU_EXPR = (alias: string): string => `
  case
    when trim(coalesce(${alias}.bataiju::text, '')) ~ '^-?[0-9]+$'
      then trim(${alias}.bataiju::text)::int
    else null
  end
`;

const SAFE_ZENHAN_3F_EXPR = (alias: string): string => `
  case
    when trim(coalesce(${alias}.zenhan_3f::text, '')) ~ '^[0-9]+$'
      and nullif(trim(${alias}.zenhan_3f::text), '000') is not null
      then nullif(trim(${alias}.zenhan_3f::text), '000')::numeric / 10
    else null
  end
`;

const buildPerRaceCoreCtesSql = (): string => `
with params as (
  select
    $1::text as source,
    $2::text as kaisai_nen,
    $3::text as kaisai_tsukihi,
    lpad($4::text, 2, '0') as keibajo_code,
    lpad($5::text, 2, '0') as race_bango,
    ($2::text || $3::text) as race_date,
    to_date($2::text || $3::text, 'YYYYMMDD') as race_dt,
    to_char(to_date($2::text || $3::text, 'YYYYMMDD') - interval '${HISTORY_LOOKBACK_YEARS} years', 'YYYYMMDD') as history_start
),
rec as (
  select
    f.source,
    f.race_date,
    to_date(f.race_date, 'YYYYMMDD') as race_dt,
    f.kaisai_nen,
    f.kaisai_tsukihi,
    lpad(f.keibajo_code::text, 2, '0') as keibajo_code,
    lpad(f.race_bango::text, 2, '0') as race_bango,
    f.ketto_toroku_bango,
    f.umaban,
    f.bamei,
    f.kishumei_ryakusho,
    f.chokyoshimei_ryakusho,
    f.kyori,
    f.track_code,
    f.grade_code,
    f.kyoso_joken_code,
    f.shusso_tosu,
    f.finish_position,
    f.finish_norm,
    f.time_sa,
    f.kohan_3f,
    f.corner1_norm,
    f.corner3_norm,
    f.corner4_norm,
    f.babajotai_code_shiba,
    f.babajotai_code_dirt,
    f.tansho_ninkijun,
    f.tansho_odds
  from race_entry_corner_features f
  join params p on p.source = f.source
  where f.race_date between p.history_start and p.race_date
),
target as (
  select
    r.source,
    r.race_date,
    r.race_dt,
    r.kaisai_nen,
    r.kaisai_tsukihi,
    r.keibajo_code,
    r.race_bango,
    r.ketto_toroku_bango,
    r.umaban,
    r.bamei,
    case when r.source = 'jra' then 'jra' when r.keibajo_code = '83' then 'ban-ei' else 'nar' end as category,
    r.kyori,
    r.track_code,
    r.grade_code,
    coalesce(
      nullif(r.shusso_tosu, 0),
      count(*) over (
        partition by r.source, r.kaisai_nen, r.kaisai_tsukihi, r.keibajo_code, r.race_bango
      )::int
    ) as shusso_tosu,
    r.finish_position,
    r.finish_norm,
    r.kishumei_ryakusho,
    r.chokyoshimei_ryakusho,
    r.kyoso_joken_code,
    r.babajotai_code_shiba,
    r.babajotai_code_dirt,
    r.corner1_norm as target_corner_1_norm,
    r.corner3_norm as target_corner_3_norm,
    r.corner4_norm as target_corner_4_norm,
    case
      when r.corner1_norm is null then null
      when r.corner1_norm = 0 then ${RUNNING_STYLE_CLASS_NIGE}
      when r.corner1_norm <= ${RUNNING_STYLE_SENKOU_THRESHOLD} then ${RUNNING_STYLE_CLASS_SENKOU}
      when r.corner1_norm <= ${RUNNING_STYLE_SASHI_THRESHOLD} then ${RUNNING_STYLE_CLASS_SASHI}
      else ${RUNNING_STYLE_CLASS_OIKOMI}
    end as target_running_style_class,
    'v1' as feature_schema_version,
    cast(substr(r.race_date, 1, 4) as int) as race_year
  from rec r
  join params p
    on r.kaisai_nen = p.kaisai_nen
   and r.kaisai_tsukihi = p.kaisai_tsukihi
   and r.keibajo_code = p.keibajo_code
   and r.race_bango = p.race_bango
  where r.ketto_toroku_bango is not null
),
`;

const buildSharedFeatureCtesSql = (): string => `target_horses as (
  select distinct ketto_toroku_bango from target
),
se_lookup as (
  select 'jra' as source, se.kaisai_nen, se.kaisai_tsukihi,
         lpad(se.keibajo_code::text, 2, '0') as keibajo_code,
         lpad(se.race_bango::text, 2, '0') as race_bango,
         se.ketto_toroku_bango,
         ${SAFE_BATAIJU_EXPR("se")} as bataiju
  from jvd_se se
  join target_horses th on th.ketto_toroku_bango = se.ketto_toroku_bango
  cross join params p
  where se.kaisai_nen || se.kaisai_tsukihi between p.history_start and p.race_date
  union all
  select 'nar' as source, se.kaisai_nen, se.kaisai_tsukihi,
         lpad(se.keibajo_code::text, 2, '0') as keibajo_code,
         lpad(se.race_bango::text, 2, '0') as race_bango,
         se.ketto_toroku_bango,
         ${SAFE_BATAIJU_EXPR("se")} as bataiju
  from nvd_se se
  join target_horses th on th.ketto_toroku_bango = se.ketto_toroku_bango
  cross join params p
  where se.kaisai_nen || se.kaisai_tsukihi between p.history_start and p.race_date
),
ra_lookup as (
  select 'jra' as source, ra.kaisai_nen, ra.kaisai_tsukihi,
         lpad(ra.keibajo_code::text, 2, '0') as keibajo_code,
         lpad(ra.race_bango::text, 2, '0') as race_bango,
         ${SAFE_ZENHAN_3F_EXPR("ra")} as zenhan_3f
  from jvd_ra ra
  cross join params p
  where ra.kaisai_nen || ra.kaisai_tsukihi between p.history_start and p.race_date
  union all
  select 'nar' as source, ra.kaisai_nen, ra.kaisai_tsukihi,
         lpad(ra.keibajo_code::text, 2, '0') as keibajo_code,
         lpad(ra.race_bango::text, 2, '0') as race_bango,
         ${SAFE_ZENHAN_3F_EXPR("ra")} as zenhan_3f
  from nvd_ra ra
  cross join params p
  where ra.kaisai_nen || ra.kaisai_tsukihi between p.history_start and p.race_date
),
target_current_bataiju as (
  select t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango,
         t.ketto_toroku_bango, s.bataiju as current_bataiju
  from target t
  left join se_lookup s using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
),
horse_history_base as (
  select
    t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
    t.race_dt as target_race_dt,
    t.keibajo_code as target_keibajo,
    t.kyori as target_kyori,
    t.track_code as target_track_code,
    t.grade_code as target_grade_code,
    case t.kyoso_joken_code
      when '000' then 0 when '005' then 1 when '010' then 2 when '016' then 3
      when '701' then 4 when '703' then 5 when '999' then 6
      else null end as target_class_level,
    h.kaisai_nen as history_kaisai_nen,
    h.kaisai_tsukihi as history_kaisai_tsukihi,
    h.keibajo_code as history_keibajo,
    h.race_bango as history_race_bango,
    h.race_dt as history_race_dt,
    h.finish_position,
    h.finish_norm::double precision as finish_norm,
    h.time_sa::double precision as time_sa,
    h.kohan_3f::double precision as kohan_3f,
    h.corner1_norm::double precision as corner1_norm,
    h.corner3_norm::double precision as corner3_norm,
    h.corner4_norm::double precision as corner4_norm,
    hr.zenhan_3f::double precision as zenhan_3f,
    h.kyori as history_kyori,
    h.track_code as history_track_code,
    h.grade_code as history_grade_code,
    case h.kyoso_joken_code
      when '000' then 0 when '005' then 1 when '010' then 2 when '016' then 3
      when '701' then 4 when '703' then 5 when '999' then 6
      else null end as history_class_level,
    hs.bataiju as history_bataiju,
    row_number() over (
      partition by t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango
      order by h.race_date desc
    ) as recent_rank
  from target t
  join rec h
    on h.source = t.source
   and h.ketto_toroku_bango = t.ketto_toroku_bango
   and h.race_date < t.race_date
   and h.race_dt >= t.race_dt - interval '${HISTORY_LOOKBACK_YEARS} years'
  left join se_lookup hs
    on hs.source = h.source
   and hs.kaisai_nen = h.kaisai_nen
   and hs.kaisai_tsukihi = h.kaisai_tsukihi
   and hs.keibajo_code = h.keibajo_code
   and hs.race_bango = h.race_bango
   and hs.ketto_toroku_bango = h.ketto_toroku_bango
  left join ra_lookup hr
    on hr.source = h.source
   and hr.kaisai_nen = h.kaisai_nen
   and hr.kaisai_tsukihi = h.kaisai_tsukihi
   and hr.keibajo_code = h.keibajo_code
   and hr.race_bango = h.race_bango
  where h.finish_position is not null
),
horse_career as (
  select
    source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
    avg(time_sa) filter (where recent_rank <= ${RECENT_WINDOW_SIZE}) as speed_index_avg_5,
    min(time_sa) filter (where recent_rank <= ${RECENT_WINDOW_SIZE}) as speed_index_best_5,
    avg(kohan_3f) filter (where recent_rank <= ${RECENT_WINDOW_SIZE}) as kohan3f_avg_5,
    avg(zenhan_3f) filter (where recent_rank <= ${RECENT_WINDOW_SIZE}) as past_first_3f_avg_5,
    avg(corner4_norm) filter (where recent_rank <= ${RECENT_WINDOW_SIZE}) as corner_pass_avg_5,
    avg(case when finish_position = 1 then 1 else 0 end) as career_win_rate,
    avg(case when finish_position between 1 and 3 then 1 else 0 end) as career_place_rate,
    count(*) filter (where finish_position = 1) as career_top1_count,
    avg(case when finish_position = 1 then 1 else 0 end) filter (where history_keibajo = target_keibajo) as same_keibajo_win_rate,
    avg(case when finish_position = 1 then 1 else 0 end) filter (where abs(history_kyori - target_kyori) <= ${SAME_DISTANCE_TOLERANCE}) as same_distance_win_rate,
    avg(case when finish_position = 1 then 1 else 0 end) filter (where left(coalesce(history_track_code, ''), 1) = left(coalesce(target_track_code, ''), 1)) as same_track_win_rate,
    avg(case when finish_position = 1 then 1 else 0 end) filter (where coalesce(history_grade_code, '') = coalesce(target_grade_code, '')) as same_grade_win_rate,
    max(target_race_dt) - max(history_race_dt) filter (where recent_rank = 1) as days_since_last_race,
    count(*) filter (where target_race_dt - history_race_dt <= ${CONSECUTIVE_RACE_WINDOW_DAYS}) as consecutive_race_count
  from horse_history_base
  group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
),
jockey_history as (
  select
    t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
    t.race_dt as target_race_dt,
    t.keibajo_code as target_keibajo,
    t.kyori as target_kyori,
    t.track_code as target_track_code,
    t.grade_code as target_grade_code,
    t.ketto_toroku_bango as target_horse,
    h.finish_position,
    h.corner1_norm::double precision as corner1_norm,
    h.race_dt as history_race_dt,
    h.keibajo_code as history_keibajo,
    h.kyori as history_kyori,
    h.track_code as history_track_code,
    h.grade_code as history_grade_code,
    h.ketto_toroku_bango as history_horse
  from target t
  join rec h
    on h.source = t.source
   and h.kishumei_ryakusho = t.kishumei_ryakusho
   and h.race_date < t.race_date
   and h.race_dt >= t.race_dt - interval '${HISTORY_LOOKBACK_YEARS} years'
  where h.finish_position is not null and t.kishumei_ryakusho is not null
),
jockey_career as (
  select
    source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
    avg(case when finish_position = 1 then 1 else 0 end) as jockey_career_win_rate,
    avg(case when finish_position = 1 then 1 else 0 end) filter (where history_race_dt >= target_race_dt - ${JOCKEY_RECENT_DAYS}) as jockey_recent_win_rate,
    avg(case when finish_position = 1 then 1 else 0 end) filter (where history_keibajo = target_keibajo) as jockey_keibajo_win_rate,
    avg(case when finish_position = 1 then 1 else 0 end) filter (where abs(history_kyori - target_kyori) <= ${SAME_DISTANCE_TOLERANCE}) as jockey_distance_win_rate,
    avg(case when finish_position = 1 then 1 else 0 end) filter (where left(coalesce(history_track_code, ''), 1) = left(coalesce(target_track_code, ''), 1)) as jockey_track_win_rate,
    avg(case when finish_position = 1 then 1 else 0 end) filter (where coalesce(history_grade_code, '') = coalesce(target_grade_code, '')) as jockey_grade_win_rate,
    count(*) filter (where history_horse = target_horse) as jockey_horse_pair_count,
    avg(case when finish_position = 1 then 1 else 0 end) filter (where history_horse = target_horse) as jockey_horse_pair_win_rate,
    avg(case when corner1_norm = 0 then 1.0 when corner1_norm is null then null else 0.0 end) filter (where history_horse = target_horse) as jockey_horse_pair_nige_rate,
    avg(case when corner1_norm = 0 then 1.0 when corner1_norm is null then null else 0.0 end) as jockey_nige_rate,
    avg(case when corner1_norm is null then null when corner1_norm > 0 and corner1_norm <= ${RUNNING_STYLE_SENKOU_THRESHOLD} then 1.0 else 0.0 end) as jockey_senkou_rate,
    avg(case when corner1_norm is null then null when corner1_norm > ${RUNNING_STYLE_SENKOU_THRESHOLD} and corner1_norm <= ${RUNNING_STYLE_SASHI_THRESHOLD} then 1.0 else 0.0 end) as jockey_sashi_rate,
    avg(case when corner1_norm is null then null when corner1_norm > ${RUNNING_STYLE_SASHI_THRESHOLD} then 1.0 else 0.0 end) as jockey_oikomi_rate,
    avg(corner1_norm) as jockey_corner_1_norm_avg,
    avg(corner1_norm) filter (where history_horse = target_horse) as jockey_horse_corner_1_norm_avg,
    avg(corner1_norm) filter (where history_race_dt >= target_race_dt - ${JOCKEY_RECENT_DAYS}) as jockey_recent_corner_1_norm_avg_90d,
    avg(case when corner1_norm = 0 then 1.0 when corner1_norm is null then null else 0.0 end)
      filter (where history_race_dt >= target_race_dt - ${JOCKEY_RECENT_DAYS}) as jockey_recent_nige_rate_90d
  from jockey_history
  group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
),
trainer_history as (
  select
    t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
    t.race_dt as target_race_dt,
    t.keibajo_code as target_keibajo,
    t.kyori as target_kyori,
    t.track_code as target_track_code,
    t.grade_code as target_grade_code,
    t.ketto_toroku_bango as target_horse,
    h.finish_position,
    h.corner1_norm::double precision as corner1_norm,
    h.race_dt as history_race_dt,
    h.keibajo_code as history_keibajo,
    h.kyori as history_kyori,
    h.track_code as history_track_code,
    h.grade_code as history_grade_code,
    h.ketto_toroku_bango as history_horse
  from target t
  join rec h
    on h.source = t.source
   and h.chokyoshimei_ryakusho = t.chokyoshimei_ryakusho
   and h.race_date < t.race_date
   and h.race_dt >= t.race_dt - interval '${HISTORY_LOOKBACK_YEARS} years'
  where h.finish_position is not null and t.chokyoshimei_ryakusho is not null
),
trainer_career as (
  select
    source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
    avg(case when finish_position = 1 then 1 else 0 end) as trainer_career_win_rate,
    avg(case when finish_position = 1 then 1 else 0 end) filter (where history_keibajo = target_keibajo) as trainer_keibajo_win_rate,
    avg(case when finish_position = 1 then 1 else 0 end) filter (where abs(history_kyori - target_kyori) <= ${SAME_DISTANCE_TOLERANCE}) as trainer_distance_win_rate,
    avg(case when finish_position = 1 then 1 else 0 end) filter (where history_horse = target_horse) as trainer_horse_win_rate,
    avg(case when corner1_norm = 0 then 1.0 when corner1_norm is null then null else 0.0 end) filter (where history_horse = target_horse) as trainer_horse_pair_nige_rate,
    avg(case when corner1_norm = 0 then 1.0 when corner1_norm is null then null else 0.0 end) as trainer_nige_rate,
    avg(case when corner1_norm is null then null when corner1_norm > 0 and corner1_norm <= ${RUNNING_STYLE_SENKOU_THRESHOLD} then 1.0 else 0.0 end) as trainer_senkou_rate,
    avg(case when corner1_norm is null then null when corner1_norm > ${RUNNING_STYLE_SENKOU_THRESHOLD} and corner1_norm <= ${RUNNING_STYLE_SASHI_THRESHOLD} then 1.0 else 0.0 end) as trainer_sashi_rate,
    avg(case when corner1_norm is null then null when corner1_norm > ${RUNNING_STYLE_SASHI_THRESHOLD} then 1.0 else 0.0 end) as trainer_oikomi_rate,
    avg(corner1_norm) as trainer_corner_1_norm_avg
  from trainer_history
  group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
),
target_months as (
  select distinct cast(kaisai_nen as int) * 100 + cast(substr(kaisai_tsukihi, 1, 2) as int) as stats_year_month
  from target
),
pedigree_rec_um as (
  select
    r.source,
    r.race_date,
    cast(substr(r.race_date, 1, 4) as int) * 100 + cast(substr(r.race_date, 5, 2) as int) as race_year_month,
    r.ketto_toroku_bango,
    r.kyori,
    r.track_code,
    r.finish_position,
    r.finish_norm,
    r.keibajo_code,
    coalesce(j_um.ketto_joho_01b, n_um.ketto_joho_01b) as ketto_joho_01b,
    coalesce(j_um.ketto_joho_05b, n_um.ketto_joho_05b) as ketto_joho_05b,
    r.corner1_norm::double precision as corner1_norm
  from rec r
  left join jvd_um j_um on r.source = 'jra' and j_um.ketto_toroku_bango = r.ketto_toroku_bango
  left join nvd_um n_um on r.source = 'nar' and n_um.ketto_toroku_bango = r.ketto_toroku_bango
  where not (r.source = 'nar' and r.keibajo_code = '83')
),
sire_distance_monthly as (
  select race_year_month, ketto_joho_01b as sire, cast(coalesce(kyori, 0) as int) / ${DISTANCE_BAND_METERS} as kyori_band,
    sum(case when finish_position = 1 then 1 else 0 end) as win_count,
    sum(finish_norm) as finish_norm_sum,
    count(finish_norm) as finish_norm_count,
    count(*) as race_count
  from pedigree_rec_um
  where finish_position is not null and ketto_joho_01b is not null and trim(ketto_joho_01b) <> ''
  group by 1, 2, 3
),
sire_distance_stats as (
  select tm.stats_year_month, m.sire, m.kyori_band,
    sum(m.win_count)::double precision / nullif(sum(m.race_count), 0) as sire_distance_win_rate_val,
    sum(m.finish_norm_sum)::double precision / nullif(sum(m.finish_norm_count), 0) as sire_avg_finish_at_distance_val,
    sum(m.race_count) as race_count
  from target_months tm
  join sire_distance_monthly m on m.race_year_month < tm.stats_year_month
  group by tm.stats_year_month, m.sire, m.kyori_band
),
sire_track_monthly as (
  select race_year_month, ketto_joho_01b as sire, left(coalesce(track_code, ''), 1) as surface,
    sum(case when finish_position = 1 then 1 else 0 end) as win_count,
    count(*) as race_count
  from pedigree_rec_um
  where finish_position is not null and ketto_joho_01b is not null and trim(ketto_joho_01b) <> ''
  group by 1, 2, 3
),
sire_track_stats as (
  select tm.stats_year_month, m.sire, m.surface,
    sum(m.win_count)::double precision / nullif(sum(m.race_count), 0) as sire_track_win_rate_val,
    sum(m.race_count) as race_count
  from target_months tm
  join sire_track_monthly m on m.race_year_month < tm.stats_year_month
  group by tm.stats_year_month, m.sire, m.surface
),
damsire_distance_monthly as (
  select race_year_month, ketto_joho_05b as damsire, cast(coalesce(kyori, 0) as int) / ${DISTANCE_BAND_METERS} as kyori_band,
    sum(case when finish_position = 1 then 1 else 0 end) as win_count,
    count(*) as race_count
  from pedigree_rec_um
  where finish_position is not null and ketto_joho_05b is not null and trim(ketto_joho_05b) <> ''
  group by 1, 2, 3
),
damsire_distance_stats as (
  select tm.stats_year_month, m.damsire, m.kyori_band,
    sum(m.win_count)::double precision / nullif(sum(m.race_count), 0) as dam_sire_distance_win_rate_val,
    sum(m.race_count) as race_count
  from target_months tm
  join damsire_distance_monthly m on m.race_year_month < tm.stats_year_month
  group by tm.stats_year_month, m.damsire, m.kyori_band
),
damsire_track_monthly as (
  select race_year_month, ketto_joho_05b as damsire, left(coalesce(track_code, ''), 1) as surface,
    sum(finish_norm) as finish_norm_sum,
    count(finish_norm) as finish_norm_count,
    count(*) as race_count
  from pedigree_rec_um
  where finish_position is not null and ketto_joho_05b is not null and trim(ketto_joho_05b) <> ''
  group by 1, 2, 3
),
damsire_track_stats as (
  select tm.stats_year_month, m.damsire, m.surface,
    sum(m.finish_norm_sum)::double precision / nullif(sum(m.finish_norm_count), 0) as damsire_avg_finish_at_track_val,
    sum(m.race_count) as race_count
  from target_months tm
  join damsire_track_monthly m on m.race_year_month < tm.stats_year_month
  group by tm.stats_year_month, m.damsire, m.surface
),
sire_running_style_monthly as (
  select race_year_month, ketto_joho_01b as sire, 0 as rs_bucket,
    sum(case when corner1_norm = 0 then 1 else 0 end) as nige_count,
    sum(case when corner1_norm > 0 and corner1_norm <= ${RUNNING_STYLE_SENKOU_THRESHOLD} then 1 else 0 end) as senkou_count,
    sum(case when corner1_norm > ${RUNNING_STYLE_SENKOU_THRESHOLD} and corner1_norm <= ${RUNNING_STYLE_SASHI_THRESHOLD} then 1 else 0 end) as sashi_count,
    sum(case when corner1_norm > ${RUNNING_STYLE_SASHI_THRESHOLD} then 1 else 0 end) as oikomi_count,
    sum(corner1_norm) as corner1_norm_sum,
    count(corner1_norm) as corner1_norm_count,
    count(*) as race_count
  from pedigree_rec_um
  where finish_position is not null and ketto_joho_01b is not null and trim(ketto_joho_01b) <> ''
  group by 1, 2, 3
),
sire_running_style_stats as (
  select tm.stats_year_month, m.sire, m.rs_bucket,
    sum(m.nige_count)::double precision / nullif(sum(m.race_count), 0) as sire_nige_rate_val,
    sum(m.senkou_count)::double precision / nullif(sum(m.race_count), 0) as sire_senkou_rate_val,
    sum(m.sashi_count)::double precision / nullif(sum(m.race_count), 0) as sire_sashi_rate_val,
    sum(m.oikomi_count)::double precision / nullif(sum(m.race_count), 0) as sire_oikomi_rate_val,
    sum(m.corner1_norm_sum)::double precision / nullif(sum(m.corner1_norm_count), 0) as sire_corner_1_norm_avg_val,
    sum(m.race_count) as race_count
  from target_months tm
  join sire_running_style_monthly m on m.race_year_month < tm.stats_year_month
  group by tm.stats_year_month, m.sire, m.rs_bucket
),
target_pedigree as (
  select
    t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
    cast(coalesce(t.kyori, 0) as int) / ${DISTANCE_BAND_METERS} as kyori_band,
    left(coalesce(t.track_code, ''), 1) as surface,
    0 as rs_bucket,
    coalesce(j_um.ketto_joho_01b, n_um.ketto_joho_01b) as target_sire,
    coalesce(j_um.ketto_joho_05b, n_um.ketto_joho_05b) as target_damsire
  from target t
  left join jvd_um j_um on t.source = 'jra' and j_um.ketto_toroku_bango = t.ketto_toroku_bango
  left join nvd_um n_um on t.source = 'nar' and n_um.ketto_toroku_bango = t.ketto_toroku_bango
),
race_horses as (
  select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
         speed_index_avg_5, speed_index_best_5, same_distance_win_rate
  from horse_career
),
race_field_aggregates as (
  select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
         avg(speed_index_avg_5) as race_avg_speed,
         count(*) filter (where same_distance_win_rate > ${RIVAL_DISTANCE_THRESHOLD}) as race_strong_count
  from race_horses
  group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
),
race_top3_speed as (
  select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
         avg(speed_index_best_5) as race_top_speed
  from (
    select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
           speed_index_best_5,
           row_number() over (
             partition by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
             order by speed_index_best_5 asc nulls last
           ) as rk
    from race_horses
    where speed_index_best_5 is not null
  ) ranked
  where rk <= 3
  group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
),
track_bias as (
  select t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
    avg(case when h.finish_position = 1 and h.umaban * 2 <= h.shusso_tosu + 1 then 1 else 0 end) as track_bias_inside,
    avg(case when h.finish_position = 1 and h.corner1_norm::double precision <= ${FRONT_CORNER_THRESHOLD} then 1 else 0 end) as track_bias_front
  from target t
  left join rec h
    on h.source = t.source
   and h.keibajo_code = t.keibajo_code
   and h.race_date < t.race_date
   and h.race_dt >= t.race_dt - ${TRACK_BIAS_WINDOW_DAYS}
   and h.finish_position is not null
  group by t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango
),
weight_agg as (
  select b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango, b.ketto_toroku_bango,
    max(tcb.current_bataiju) as current_bataiju_kept,
    avg(b.history_bataiju) filter (where b.recent_rank <= ${RECENT_WINDOW_SIZE}) as weight_avg_5
  from horse_history_base b
  left join target_current_bataiju tcb using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
  group by b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango, b.ketto_toroku_bango
),
recent_form as (
  select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
    max(finish_norm) filter (where recent_rank = 1) as last_race_finish_norm,
    max(time_sa) filter (where recent_rank = 1) as last_race_margin_to_winner,
    max(corner3_norm) filter (where recent_rank = 1) as last_race_corner_pass_norm,
    max(target_class_level) filter (where recent_rank = 1)
      - max(history_class_level) filter (where recent_rank = 1) as last_race_class_diff,
    max(history_kyori) filter (where recent_rank = 1)
      - max(target_kyori) filter (where recent_rank = 1) as last_race_distance_diff,
    case when count(*) filter (where recent_rank <= ${RECENT_WINDOW_SIZE}) >= ${TREND_MIN_RACES}
         then regr_slope(finish_norm, recent_rank::double precision) filter (where recent_rank <= ${RECENT_WINDOW_SIZE})
         else null end as finish_trend_5,
    avg(finish_norm) filter (where recent_rank <= 3) as last_3_avg_finish_norm
  from horse_history_base
  group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
),
legacy_horse_avg as (
  select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
    avg(finish_norm) as avg_finish,
    avg(finish_norm) filter (where recent_rank <= ${RECENT_WINDOW_SIZE}) as recent_finish
  from horse_history_base
  group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
),
legacy_target as (
  select t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
    rec.tansho_ninkijun::int as ninkijun,
    rec.tansho_odds::double precision as odds_value,
    rec.shusso_tosu::int as runner_count
  from target t
  join rec on rec.source = t.source and rec.kaisai_nen = t.kaisai_nen
    and rec.kaisai_tsukihi = t.kaisai_tsukihi and rec.keibajo_code = t.keibajo_code
    and rec.race_bango = t.race_bango and rec.ketto_toroku_bango = t.ketto_toroku_bango
),
legacy_features as (
  select t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
    lha.avg_finish,
    lha.recent_finish,
    case when t.runner_count > 1 and t.ninkijun is not null
         then greatest(0::double precision, least(1::double precision, (t.ninkijun - 1)::double precision / nullif(t.runner_count - 1, 0)))
         else null end as popularity_score,
    case when t.odds_value is not null and t.odds_value > 0
         then greatest(0::double precision, least(1::double precision, ln(greatest(t.odds_value, 1::double precision)) / ln(300::double precision)))
         else null end as odds_score
  from legacy_target t
  left join legacy_horse_avg lha using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
),
horse_running_style_history as (
  select b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango, b.ketto_toroku_bango,
    avg(b.corner1_norm) filter (where b.recent_rank <= ${RECENT_WINDOW_SIZE}) as past_corner_1_norm_avg_5,
    avg(b.corner1_norm) filter (where b.recent_rank <= 3) as past_corner_1_norm_avg_3,
    avg(b.corner1_norm) filter (where b.recent_rank <= 10) as past_corner_1_norm_avg_10,
    avg(b.corner4_norm - b.corner1_norm) filter (where b.recent_rank <= ${RECENT_WINDOW_SIZE}) as past_corner_progression_avg_5,
    stddev_samp(b.corner1_norm) filter (where b.recent_rank <= ${RECENT_WINDOW_SIZE}) as past_corner_1_norm_std_5,
    min(b.corner1_norm) filter (where b.recent_rank <= ${RECENT_WINDOW_SIZE}) as past_corner_1_norm_best_5,
    max(b.corner1_norm) filter (where b.recent_rank <= ${RECENT_WINDOW_SIZE}) as past_corner_1_norm_worst_5,
    avg(case when b.corner1_norm = 0 then 1.0 when b.corner1_norm is null then null else 0.0 end) as past_nige_rate_self,
    avg(case when b.corner1_norm is null then null when b.corner1_norm > 0 and b.corner1_norm <= ${RUNNING_STYLE_SENKOU_THRESHOLD} then 1.0 else 0.0 end) as past_senkou_rate_self,
    avg(case when b.corner1_norm is null then null when b.corner1_norm > ${RUNNING_STYLE_SENKOU_THRESHOLD} and b.corner1_norm <= ${RUNNING_STYLE_SASHI_THRESHOLD} then 1.0 else 0.0 end) as past_sashi_rate_self,
    avg(case when b.corner1_norm is null then null when b.corner1_norm > ${RUNNING_STYLE_SASHI_THRESHOLD} then 1.0 else 0.0 end) as past_oikomi_rate_self,
    case when count(*) filter (where b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm is not null) > 0
      then (count(*) filter (where b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm = 0))::double precision
        / count(*) filter (where b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm is not null)
      else null end as past_nige_rate_self_recent_5,
    case when count(*) filter (where b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm is not null) > 0
      then (count(*) filter (where b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm > 0 and b.corner1_norm <= ${RUNNING_STYLE_SENKOU_THRESHOLD}))::double precision
        / count(*) filter (where b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm is not null)
      else null end as past_senkou_rate_self_recent_5,
    case when count(*) filter (where b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm is not null) > 0
      then (count(*) filter (where b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm > ${RUNNING_STYLE_SENKOU_THRESHOLD} and b.corner1_norm <= ${RUNNING_STYLE_SASHI_THRESHOLD}))::double precision
        / count(*) filter (where b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm is not null)
      else null end as past_sashi_rate_self_recent_5,
    case when count(*) filter (where b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm is not null) > 0
      then (count(*) filter (where b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm > ${RUNNING_STYLE_SASHI_THRESHOLD}))::double precision
        / count(*) filter (where b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm is not null)
      else null end as past_oikomi_rate_self_recent_5,
    case when count(*) filter (where b.recent_rank <= 3 and b.corner1_norm is not null) > 0
      then (count(*) filter (where b.recent_rank <= 3 and b.corner1_norm = 0))::double precision
        / count(*) filter (where b.recent_rank <= 3 and b.corner1_norm is not null)
      else null end as past_nige_rate_self_recent_3,
    case when count(*) filter (where b.recent_rank <= 3 and b.corner1_norm is not null) > 0
      then (count(*) filter (where b.recent_rank <= 3 and b.corner1_norm > 0 and b.corner1_norm <= ${RUNNING_STYLE_SENKOU_THRESHOLD}))::double precision
        / count(*) filter (where b.recent_rank <= 3 and b.corner1_norm is not null)
      else null end as past_senkou_rate_self_recent_3,
    case when count(*) filter (where b.recent_rank <= 3 and b.corner1_norm is not null) > 0
      then (count(*) filter (where b.recent_rank <= 3 and b.corner1_norm > ${RUNNING_STYLE_SENKOU_THRESHOLD} and b.corner1_norm <= ${RUNNING_STYLE_SASHI_THRESHOLD}))::double precision
        / count(*) filter (where b.recent_rank <= 3 and b.corner1_norm is not null)
      else null end as past_sashi_rate_self_recent_3,
    case when count(*) filter (where b.recent_rank <= 3 and b.corner1_norm is not null) > 0
      then (count(*) filter (where b.recent_rank <= 3 and b.corner1_norm > ${RUNNING_STYLE_SASHI_THRESHOLD}))::double precision
        / count(*) filter (where b.recent_rank <= 3 and b.corner1_norm is not null)
      else null end as past_oikomi_rate_self_recent_3,
    max(b.corner1_norm) filter (where b.recent_rank = 1) as last_race_corner_1_norm,
    max(b.corner4_norm - b.corner1_norm) filter (where b.recent_rank = 1) as last_race_corner_progression,
    avg(b.corner1_norm) filter (where abs(b.history_kyori - b.target_kyori) <= ${SAME_DISTANCE_TOLERANCE}) as horse_distance_corner_1_norm_avg,
    avg(b.corner1_norm) filter (where left(coalesce(b.history_track_code, ''), 1) = left(coalesce(b.target_track_code, ''), 1)) as horse_track_corner_1_norm_avg,
    avg(b.corner1_norm) filter (where b.history_keibajo = b.target_keibajo) as horse_keibajo_corner_1_norm_avg,
    avg(b.corner1_norm) filter (where coalesce(b.history_grade_code, '') = coalesce(b.target_grade_code, '')) as horse_grade_corner_1_norm_avg,
    avg(case when b.finish_position = 1 then 1.0 else 0.0 end) filter (where b.corner1_norm = 0) as past_nige_win_rate_self,
    avg(case when b.finish_position = 1 then 1.0 else 0.0 end) filter (where b.corner1_norm > 0 and b.corner1_norm <= ${RUNNING_STYLE_SENKOU_THRESHOLD}) as past_senkou_win_rate_self,
    avg(case when b.finish_position = 1 then 1.0 else 0.0 end) filter (where b.corner1_norm > ${RUNNING_STYLE_SENKOU_THRESHOLD} and b.corner1_norm <= ${RUNNING_STYLE_SASHI_THRESHOLD}) as past_sashi_win_rate_self,
    avg(case when b.finish_position = 1 then 1.0 else 0.0 end) filter (where b.corner1_norm > ${RUNNING_STYLE_SASHI_THRESHOLD}) as past_oikomi_win_rate_self,
    percentile_cont(0.75) within group (order by b.corner1_norm) filter (where b.recent_rank <= ${RECENT_WINDOW_SIZE})
      - percentile_cont(0.25) within group (order by b.corner1_norm) filter (where b.recent_rank <= ${RECENT_WINDOW_SIZE}) as past_corner_1_norm_iqr_5,
    (count(*) filter (where b.finish_position = 1 and trim(coalesce(b.history_grade_code, '')) in ('A', 'B', 'C')))::bigint as top1_count_in_grade_races,
    (count(*) filter (where b.finish_position between 1 and 3 and trim(coalesce(b.history_grade_code, '')) in ('A', 'B', 'C')))::bigint as place_count_in_grade_races,
    (count(*) filter (where trim(coalesce(b.history_grade_code, '')) = 'A'))::bigint as experience_in_g1_race,
    (count(*) filter (where b.finish_position = 1 and b.recent_rank <= ${RECENT_WINDOW_SIZE}))::bigint as recent_win_count_5,
    (count(*) filter (where b.finish_position between 1 and 3 and b.recent_rank <= ${RECENT_WINDOW_SIZE}))::bigint as recent_top3_count_5,
    avg(b.kohan_3f) filter (where b.recent_rank <= 3) as last_3_avg_kohan_3f,
    greatest(
      count(*) filter (where b.corner1_norm = 0 and b.recent_rank <= ${RECENT_WINDOW_SIZE}),
      count(*) filter (where b.corner1_norm > 0 and b.corner1_norm <= ${RUNNING_STYLE_SENKOU_THRESHOLD} and b.recent_rank <= ${RECENT_WINDOW_SIZE}),
      count(*) filter (where b.corner1_norm > ${RUNNING_STYLE_SENKOU_THRESHOLD} and b.corner1_norm <= ${RUNNING_STYLE_SASHI_THRESHOLD} and b.recent_rank <= ${RECENT_WINDOW_SIZE}),
      count(*) filter (where b.corner1_norm > ${RUNNING_STYLE_SASHI_THRESHOLD} and b.recent_rank <= ${RECENT_WINDOW_SIZE})
    )::double precision / nullif(count(*) filter (where b.corner1_norm is not null and b.recent_rank <= ${RECENT_WINDOW_SIZE}), 0)
      as past_dominant_label_consistency_5
  from horse_history_base b
  group by b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango, b.ketto_toroku_bango
),
weather_lookup as (
  select t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
    coalesce(jr.tenko_code, nr.tenko_code) as tenko_code
  from target t
  left join jvd_ra jr on t.source = 'jra' and jr.kaisai_nen = t.kaisai_nen and jr.kaisai_tsukihi = t.kaisai_tsukihi
    and lpad(jr.keibajo_code::text, 2, '0') = t.keibajo_code and lpad(jr.race_bango::text, 2, '0') = t.race_bango
  left join nvd_ra nr on t.source = 'nar' and nr.kaisai_nen = t.kaisai_nen and nr.kaisai_tsukihi = t.kaisai_tsukihi
    and lpad(nr.keibajo_code::text, 2, '0') = t.keibajo_code and lpad(nr.race_bango::text, 2, '0') = t.race_bango
),
base_features as (
  select
    t.source, t.race_date, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango,
    t.ketto_toroku_bango, t.umaban, t.bamei, t.category, t.kyori, t.track_code, t.grade_code, t.shusso_tosu,
    t.finish_position, t.finish_norm,
    t.target_corner_1_norm, t.target_corner_3_norm, t.target_corner_4_norm, t.target_running_style_class,
    hc.speed_index_avg_5, hc.speed_index_best_5, hc.kohan3f_avg_5, hc.past_first_3f_avg_5, hc.corner_pass_avg_5,
    hc.career_win_rate, hc.career_place_rate, hc.career_top1_count,
    hc.same_keibajo_win_rate, hc.same_distance_win_rate, hc.same_track_win_rate, hc.same_grade_win_rate,
    wa.weight_avg_5,
    wa.current_bataiju_kept::double precision - wa.weight_avg_5 as weight_diff_from_avg,
    hc.days_since_last_race, hc.consecutive_race_count,
    jc.jockey_career_win_rate, jc.jockey_recent_win_rate, jc.jockey_keibajo_win_rate,
    jc.jockey_distance_win_rate, jc.jockey_track_win_rate, jc.jockey_grade_win_rate,
    jc.jockey_horse_pair_count, jc.jockey_horse_pair_win_rate, jc.jockey_horse_pair_nige_rate,
    jc.jockey_nige_rate, jc.jockey_senkou_rate, jc.jockey_sashi_rate, jc.jockey_oikomi_rate,
    jc.jockey_corner_1_norm_avg, jc.jockey_horse_corner_1_norm_avg,
    jc.jockey_recent_corner_1_norm_avg_90d, jc.jockey_recent_nige_rate_90d,
    tc.trainer_career_win_rate, tc.trainer_keibajo_win_rate, tc.trainer_distance_win_rate, tc.trainer_horse_win_rate,
    tc.trainer_horse_pair_nige_rate,
    tc.trainer_nige_rate, tc.trainer_senkou_rate, tc.trainer_sashi_rate, tc.trainer_oikomi_rate,
    tc.trainer_corner_1_norm_avg,
    case when sds.race_count >= ${PEDIGREE_MIN_RACES} then sds.sire_distance_win_rate_val else null end as sire_distance_win_rate,
    case when sts.race_count >= ${PEDIGREE_MIN_RACES} then sts.sire_track_win_rate_val else null end as sire_track_win_rate,
    case when dsd.race_count >= ${PEDIGREE_MIN_RACES} then dsd.dam_sire_distance_win_rate_val else null end as dam_sire_distance_win_rate,
    case when sds.race_count >= ${PEDIGREE_MIN_RACES} then sds.sire_avg_finish_at_distance_val else null end as sire_avg_finish_at_distance,
    case when dst.race_count >= ${PEDIGREE_MIN_RACES} then dst.damsire_avg_finish_at_track_val else null end as damsire_avg_finish_at_track,
    case when srs.race_count >= ${PEDIGREE_MIN_RACES} then srs.sire_nige_rate_val else null end as sire_nige_rate,
    case when srs.race_count >= ${PEDIGREE_MIN_RACES} then srs.sire_senkou_rate_val else null end as sire_senkou_rate,
    case when srs.race_count >= ${PEDIGREE_MIN_RACES} then srs.sire_sashi_rate_val else null end as sire_sashi_rate,
    case when srs.race_count >= ${PEDIGREE_MIN_RACES} then srs.sire_oikomi_rate_val else null end as sire_oikomi_rate,
    case when srs.race_count >= ${PEDIGREE_MIN_RACES} then srs.sire_corner_1_norm_avg_val else null end as sire_corner_1_norm_avg,
    (
      coalesce(sds.sire_distance_win_rate_val, 0) +
      coalesce(dsd.dam_sire_distance_win_rate_val, 0) +
      coalesce(sts.sire_track_win_rate_val, 0)
    ) / ${PEDIGREE_COMPOSITE_DIVISOR}::double precision as pedigree_score_for_race,
    rfa.race_avg_speed as field_strength_avg_speed,
    rts.race_top_speed as field_strength_top3_speed,
    greatest(0, rfa.race_strong_count - case when hc.same_distance_win_rate > ${RIVAL_DISTANCE_THRESHOLD} then 1 else 0 end) as rival_count_at_distance,
    tb.track_bias_inside,
    tb.track_bias_front,
    case wl.tenko_code
      when '1' then 0::double precision when '2' then 0.3::double precision
      when '3' then 0.7::double precision when '4' then 0.7::double precision
      when '5' then 1.0::double precision when '6' then 1.0::double precision
      else null end as weather_normalized,
    case
      when left(coalesce(t.track_code, ''), 1) = '1' then
        case t.babajotai_code_shiba when '1' then 0::double precision when '2' then 0.3::double precision when '3' then 0.6::double precision when '4' then 1.0::double precision else null end
      else
        case t.babajotai_code_dirt when '1' then 0::double precision when '2' then 0.3::double precision when '3' then 0.6::double precision when '4' then 1.0::double precision else null end
    end as track_condition_normalized,
    least(1::double precision, greatest(0::double precision, coalesce(t.shusso_tosu, 0)::double precision / ${MAX_FIELD_SIZE})) as field_size_normalized,
    case when trim(coalesce(t.grade_code, '')) in ('A', 'B', 'C', 'D', 'G', 'H') then 1 else 0 end::int as is_grade_race,
    rf.last_race_finish_norm, rf.last_race_margin_to_winner, rf.last_race_corner_pass_norm,
    rf.last_race_class_diff, rf.last_race_distance_diff, rf.finish_trend_5, rf.last_3_avg_finish_norm,
    lf.avg_finish, lf.recent_finish, lf.popularity_score, lf.odds_score,
    rsh.past_corner_1_norm_avg_5,
    rsh.past_corner_1_norm_avg_3,
    rsh.past_corner_1_norm_avg_10,
    rsh.past_corner_progression_avg_5,
    rsh.past_corner_1_norm_std_5,
    rsh.past_corner_1_norm_best_5,
    rsh.past_corner_1_norm_worst_5,
    rsh.past_nige_rate_self,
    rsh.past_senkou_rate_self,
    rsh.past_sashi_rate_self,
    rsh.past_oikomi_rate_self,
    rsh.past_nige_rate_self_recent_5,
    rsh.past_senkou_rate_self_recent_5,
    rsh.past_sashi_rate_self_recent_5,
    rsh.past_oikomi_rate_self_recent_5,
    rsh.past_nige_rate_self_recent_3,
    rsh.past_senkou_rate_self_recent_3,
    rsh.past_sashi_rate_self_recent_3,
    rsh.past_oikomi_rate_self_recent_3,
    rsh.last_race_corner_1_norm,
    rsh.last_race_corner_progression,
    rsh.horse_distance_corner_1_norm_avg,
    rsh.horse_track_corner_1_norm_avg,
    rsh.horse_keibajo_corner_1_norm_avg,
    rsh.horse_grade_corner_1_norm_avg,
    rsh.past_nige_win_rate_self,
    rsh.past_senkou_win_rate_self,
    rsh.past_sashi_win_rate_self,
    rsh.past_oikomi_win_rate_self,
    rsh.past_corner_1_norm_iqr_5,
    rsh.top1_count_in_grade_races,
    rsh.place_count_in_grade_races,
    rsh.experience_in_g1_race,
    rsh.recent_win_count_5,
    rsh.recent_top3_count_5,
    rsh.past_dominant_label_consistency_5,
    rsh.last_3_avg_kohan_3f,
    case
      when t.shusso_tosu is null or t.shusso_tosu < ${UMABAN_NORM_MIN_FIELD} then null
      when t.umaban is null then null
      else least(1.0, greatest(0.0, (t.umaban::double precision - 1) / (t.shusso_tosu::double precision - 1)))
    end as umaban_norm,
    case when trim(coalesce(t.kyoso_joken_code, '')) = '${NEWCOMER_RACE_JOKEN_CODE}' then 1 else 0 end as is_newcomer_race,
    case
      when t.kyori is null then null
      when t.kyori <= ${KYORI_BAND_SPRINT_MAX} then ${KYORI_BAND_SPRINT}
      when t.kyori <= ${KYORI_BAND_MILE_MAX} then ${KYORI_BAND_MILE}
      when t.kyori <= ${KYORI_BAND_INTERMEDIATE_MAX} then ${KYORI_BAND_INTERMEDIATE}
      else ${KYORI_BAND_LONG}
    end as kyori_band,
    case
      when t.kaisai_tsukihi is null or length(t.kaisai_tsukihi) < 2 then null
      when cast(substr(t.kaisai_tsukihi, 1, 2) as int) < 3 then ${SEASON_WINTER}
      when cast(substr(t.kaisai_tsukihi, 1, 2) as int) <= ${SEASON_SPRING_MAX_MONTH} then ${SEASON_SPRING}
      when cast(substr(t.kaisai_tsukihi, 1, 2) as int) <= ${SEASON_SUMMER_MAX_MONTH} then ${SEASON_SUMMER}
      when cast(substr(t.kaisai_tsukihi, 1, 2) as int) <= ${SEASON_AUTUMN_MAX_MONTH} then ${SEASON_AUTUMN}
      else ${SEASON_WINTER}
    end as season_band,
    t.feature_schema_version,
    t.race_year,
    t.source || ':' || t.kaisai_nen || ':' || t.kaisai_tsukihi || ':' || t.keibajo_code || ':' || t.race_bango as race_id
  from target t
  left join horse_career hc using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
  left join jockey_career jc using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
  left join trainer_career tc using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
  left join target_pedigree tp using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
  left join sire_distance_stats sds on sds.sire = tp.target_sire and sds.kyori_band = tp.kyori_band and sds.stats_year_month = cast(t.kaisai_nen as int) * 100 + cast(substr(t.kaisai_tsukihi, 1, 2) as int)
  left join sire_track_stats sts on sts.sire = tp.target_sire and sts.surface = tp.surface and sts.stats_year_month = cast(t.kaisai_nen as int) * 100 + cast(substr(t.kaisai_tsukihi, 1, 2) as int)
  left join damsire_distance_stats dsd on dsd.damsire = tp.target_damsire and dsd.kyori_band = tp.kyori_band and dsd.stats_year_month = cast(t.kaisai_nen as int) * 100 + cast(substr(t.kaisai_tsukihi, 1, 2) as int)
  left join damsire_track_stats dst on dst.damsire = tp.target_damsire and dst.surface = tp.surface and dst.stats_year_month = cast(t.kaisai_nen as int) * 100 + cast(substr(t.kaisai_tsukihi, 1, 2) as int)
  left join sire_running_style_stats srs on srs.sire = tp.target_sire and srs.rs_bucket = tp.rs_bucket and srs.stats_year_month = cast(t.kaisai_nen as int) * 100 + cast(substr(t.kaisai_tsukihi, 1, 2) as int)
  left join race_field_aggregates rfa using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
  left join race_top3_speed rts using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
  left join track_bias tb using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
  left join weight_agg wa using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
  left join recent_form rf using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
  left join legacy_features lf using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
  left join weather_lookup wl using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
  left join horse_running_style_history rsh using (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
),
final_features as (
  select
    b.*,
    rank() over race_by_speed_avg_asc as speed_index_avg_5_rank_in_race,
    rank() over race_by_speed_best_asc as speed_index_best_5_rank_in_race,
    rank() over race_by_jockey_recent_desc as jockey_recent_win_rate_rank_in_race,
    rank() over race_by_trainer_career_desc as trainer_career_win_rate_rank_in_race,
    rank() over race_by_pedigree_desc as pedigree_score_for_race_rank_in_race,
    rank() over race_by_same_distance_desc as same_distance_win_rate_rank_in_race,
    rank() over race_by_past_nige_recent_5_desc as field_nige_pressure_rank,
    b.speed_index_avg_5 - avg(b.speed_index_avg_5) over race_partition as speed_index_avg_5_diff_from_race_avg,
    b.jockey_recent_win_rate - avg(b.jockey_recent_win_rate) over race_partition as jockey_recent_win_rate_diff_from_race_avg,
    b.pedigree_score_for_race - avg(b.pedigree_score_for_race) over race_partition as pedigree_score_diff_from_race_avg
  from base_features b
  window
    race_partition as (partition by b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango),
    race_by_speed_avg_asc as (partition by b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango order by b.speed_index_avg_5 asc nulls last),
    race_by_speed_best_asc as (partition by b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango order by b.speed_index_best_5 asc nulls last),
    race_by_jockey_recent_desc as (partition by b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango order by b.jockey_recent_win_rate desc nulls last),
    race_by_trainer_career_desc as (partition by b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango order by b.trainer_career_win_rate desc nulls last),
    race_by_pedigree_desc as (partition by b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango order by b.pedigree_score_for_race desc nulls last),
    race_by_same_distance_desc as (partition by b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango order by b.same_distance_win_rate desc nulls last),
    race_by_past_nige_recent_5_desc as (partition by b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango order by b.past_nige_rate_self_recent_5 desc nulls last)
)
select * from final_features order by umaban
`;

export const buildRunningStylePostgresFeatureSql = (): string =>
  buildPerRaceCoreCtesSql() + buildSharedFeatureCtesSql();

export interface BuildRunningStyleBatchFeatureSqlArgs {
  featureSchemaVersion: string;
  fromDate: string;
  source: RunningStyleSource;
  /**
   * When true, narrows the `nige` target class so a horse only counts as nige
   * when it leads at BOTH the 1st AND 2nd corner (corner1_norm = 0 AND
   * corner2_norm = 0). When false / omitted, retains the existing lax
   * definition that only checks corner1_norm = 0. Strict mode is opt-in for
   * new model training; the production worker continues to call the lax
   * (default) path so existing features parquet is not affected.
   */
  strictNigeTarget?: boolean;
  toDate: string;
}

const assertBatchSource = (source: RunningStyleSource): RunningStyleSource => {
  if (!BATCH_SOURCE_WHITELIST.has(source)) {
    throw new Error(`invalid batch source: ${source}`);
  }
  return source;
};

const assertBatchDate = (label: string, value: string): string => {
  if (!BATCH_DATE_PATTERN.test(value)) {
    throw new Error(`invalid batch ${label}: expected YYYYMMDD (8 digits), got ${value}`);
  }
  return value;
};

const assertBatchFeatureSchemaVersion = (value: string): string => {
  if (!BATCH_FEATURE_SCHEMA_VERSION_PATTERN.test(value)) {
    throw new Error(`invalid batch featureSchemaVersion: expected [A-Za-z0-9_.-]+, got ${value}`);
  }
  return value;
};

const buildBatchCoreCtesSql = (args: BuildRunningStyleBatchFeatureSqlArgs): string => {
  const source = assertBatchSource(args.source);
  const fromDate = assertBatchDate("fromDate", args.fromDate);
  const toDate = assertBatchDate("toDate", args.toDate);
  const featureSchemaVersion = assertBatchFeatureSchemaVersion(args.featureSchemaVersion);
  if (fromDate > toDate) {
    throw new Error(`invalid batch date range: fromDate (${fromDate}) > toDate (${toDate})`);
  }
  return `
with params as (
  select
    '${source}'::text as source,
    '${fromDate}'::text as race_date_min,
    '${toDate}'::text as race_date,
    to_char(to_date('${fromDate}', 'YYYYMMDD') - interval '${HISTORY_LOOKBACK_YEARS} years', 'YYYYMMDD') as history_start
),
rec as (
  select
    f.source,
    f.race_date,
    to_date(f.race_date, 'YYYYMMDD') as race_dt,
    f.kaisai_nen,
    f.kaisai_tsukihi,
    lpad(f.keibajo_code::text, 2, '0') as keibajo_code,
    lpad(f.race_bango::text, 2, '0') as race_bango,
    f.ketto_toroku_bango,
    f.umaban,
    f.bamei,
    f.kishumei_ryakusho,
    f.chokyoshimei_ryakusho,
    f.kyori,
    f.track_code,
    f.grade_code,
    f.kyoso_joken_code,
    f.shusso_tosu,
    f.finish_position,
    f.finish_norm,
    f.time_sa,
    f.kohan_3f,
    f.corner1_norm,
    f.corner3_norm,
    f.corner4_norm,
    f.babajotai_code_shiba,
    f.babajotai_code_dirt,
    f.tansho_ninkijun,
    f.tansho_odds
  from race_entry_corner_features f
  join params p on p.source = f.source
  where f.race_date between p.history_start and p.race_date
),
target as (
  select
    r.source,
    r.race_date,
    r.race_dt,
    r.kaisai_nen,
    r.kaisai_tsukihi,
    r.keibajo_code,
    r.race_bango,
    r.ketto_toroku_bango,
    r.umaban,
    r.bamei,
    case when r.source = 'jra' then 'jra' when r.keibajo_code = '83' then 'ban-ei' else 'nar' end as category,
    r.kyori,
    r.track_code,
    r.grade_code,
    coalesce(
      nullif(r.shusso_tosu, 0),
      count(*) over (
        partition by r.source, r.kaisai_nen, r.kaisai_tsukihi, r.keibajo_code, r.race_bango
      )::int
    ) as shusso_tosu,
    r.finish_position,
    r.finish_norm,
    r.kishumei_ryakusho,
    r.chokyoshimei_ryakusho,
    r.kyoso_joken_code,
    r.babajotai_code_shiba,
    r.babajotai_code_dirt,
    r.corner1_norm as target_corner_1_norm,
    r.corner3_norm as target_corner_3_norm,
    r.corner4_norm as target_corner_4_norm,
    case
      when r.corner1_norm is null then null
      when r.corner1_norm = 0 then ${RUNNING_STYLE_CLASS_NIGE}
      when r.corner1_norm <= ${RUNNING_STYLE_SENKOU_THRESHOLD} then ${RUNNING_STYLE_CLASS_SENKOU}
      when r.corner1_norm <= ${RUNNING_STYLE_SASHI_THRESHOLD} then ${RUNNING_STYLE_CLASS_SASHI}
      else ${RUNNING_STYLE_CLASS_OIKOMI}
    end as target_running_style_class,
    '${featureSchemaVersion}' as feature_schema_version,
    cast(substr(r.race_date, 1, 4) as int) as race_year
  from rec r
  cross join params p
  where r.ketto_toroku_bango is not null
    and r.race_date between p.race_date_min and p.race_date
),
`;
};

// CTE names that should be materialized (forced to physically materialize, not
// inline) when generating the batch SQL. Heavy joins / multi-referenced CTEs
// benefit because PG inlines small CTEs by default which causes repeated
// evaluation against the same 10-year window. MATERIALIZED forces the planner
// to compute once and reuse, simplifying the plan tree dramatically for batch
// runs that already process millions of rows. Per-race SQL keeps the default
// (inlineable) since the planner has small enough inputs to optimize freely.
const BATCH_MATERIALIZED_CTES: ReadonlyArray<string> = [
  "rec",
  "target",
  "target_horses",
  "se_lookup",
  "ra_lookup",
  "horse_history_base",
  "jockey_history",
  "trainer_history",
  "pedigree_rec_um",
  "target_months",
];

const applyBatchMaterializedHints = (sql: string): string =>
  BATCH_MATERIALIZED_CTES.reduce(
    (acc, cteName) => acc.replace(`${cteName} as (`, `${cteName} as materialized (`),
    sql,
  );

// Markers used to inject corner2_norm / target_corner_2_norm into the batch
// rec & target CTEs when strict nige target derivation is requested. They
// match the unique 4-space-indented projections inside `buildBatchCoreCtesSql`
// and the unique 6-space-indented case-arm inside the target CTE so a single
// `replace()` call mutates exactly one site per marker. Any future refactor
// that changes those line shapes must update the markers here in lockstep.
const BATCH_REC_CORNER1_MARKER = "    f.corner1_norm,\n";
const BATCH_REC_CORNER1_WITH_CORNER2 = "    f.corner1_norm,\n    f.corner2_norm,\n";
const BATCH_TARGET_CORNER1_PROPAGATION_MARKER = "    r.corner1_norm as target_corner_1_norm,\n";
const BATCH_TARGET_CORNER1_PROPAGATION_WITH_CORNER2 =
  "    r.corner1_norm as target_corner_1_norm,\n    r.corner2_norm as target_corner_2_norm,\n";
const BATCH_TARGET_NIGE_LAX_CASE_ARM = `when r.corner1_norm = 0 then ${RUNNING_STYLE_CLASS_NIGE}`;
const BATCH_TARGET_NIGE_STRICT_CASE_ARM = `when r.corner1_norm = 0 and r.corner2_norm = 0 then ${RUNNING_STYLE_CLASS_NIGE}`;

const applyBatchStrictNigeTargetTransform = (sql: string): string =>
  sql
    .replace(BATCH_REC_CORNER1_MARKER, BATCH_REC_CORNER1_WITH_CORNER2)
    .replace(BATCH_TARGET_CORNER1_PROPAGATION_MARKER, BATCH_TARGET_CORNER1_PROPAGATION_WITH_CORNER2)
    .replace(BATCH_TARGET_NIGE_LAX_CASE_ARM, BATCH_TARGET_NIGE_STRICT_CASE_ARM);

export const buildRunningStyleBatchFeatureSql = (
  args: BuildRunningStyleBatchFeatureSqlArgs,
): string => {
  // Default (lax) path stays byte-identical to the pre-strict snapshot so the
  // production worker / parquet pipeline are not affected. Strict mode is an
  // opt-in transform that adds corner2_norm to the rec & target CTEs and
  // narrows the nige case-arm to require leading at both 1st and 2nd corner.
  const lax = applyBatchMaterializedHints(
    buildBatchCoreCtesSql(args) + buildSharedFeatureCtesSql(),
  );
  if (args.strictNigeTarget !== true) return lax;
  return applyBatchStrictNigeTargetTransform(lax);
};

const toStringOrNull = (value: unknown): string | null => {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") {
    const text = value.trim();
    return text.length === 0 ? null : text;
  }
  if (typeof value === "number" || typeof value === "bigint" || typeof value === "boolean") {
    // String(number/bigint/boolean) is never empty after trim, so no length guard needed.
    return String(value).trim();
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  return null;
};

const toRequiredString = (value: unknown, fallback = ""): string => {
  const text = toStringOrNull(value);
  return text ?? fallback;
};

const toNumberOrNull = (value: unknown): number | null => {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "boolean") return value ? 1 : 0;
  if (value instanceof Date) return null;
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};

const rowToFeaturePayload = (
  row: SqlRow,
  featureNames: ReadonlyArray<string>,
): RaceHorseFeatureRow => {
  const perHorseFeatures: Record<string, number | null> = {};
  featureNames.forEach((name) => {
    perHorseFeatures[name] = toNumberOrNull(row[name]);
  });
  const peerInputs = {} as Record<PeerInputKey, number | null>;
  Object.entries(PEER_INPUT_COLUMNS).forEach(([featureName, peerName]) => {
    peerInputs[peerName] = perHorseFeatures[featureName] ?? null;
  });
  const source = toRequiredString(row.source);
  const kaisaiNen = toRequiredString(row.kaisai_nen);
  const kaisaiTsukihi = toRequiredString(row.kaisai_tsukihi);
  const keibajoCode = normalizeKeibajoCode(toRequiredString(row.keibajo_code));
  const raceBango = normalizeRaceBango(toRequiredString(row.race_bango));
  return {
    bamei: toStringOrNull(row.bamei),
    category: toRequiredString(row.category, source),
    kaisaiNen,
    kaisaiTsukihi,
    keibajoCode,
    kettoTorokuBango: toRequiredString(row.ketto_toroku_bango),
    peerInputs: peerInputs as RaceHorseFeatureRow["peerInputs"],
    perHorseFeatures,
    raceBango,
    raceKey: `${source}:${kaisaiNen}${kaisaiTsukihi}:${keibajoCode}:${raceBango}`,
    source,
    umaban: toNumberOrNull(row.umaban) ?? 0,
  };
};

export interface PostgresFeatureBuildSummary {
  elapsedMs: number;
  rows: ReadonlyArray<RaceHorseFeatureRow>;
  sqlRows: number;
}

export const buildRunningStyleFeaturesForRaceFromPostgres = async (
  pool: Pool,
  params: RunningStyleRaceParams,
  featureNames: ReadonlyArray<string>,
): Promise<PostgresFeatureBuildSummary> => {
  const started = performance.now();
  const result = await pool.query<SqlRow>(buildRunningStylePostgresFeatureSql(), [
    params.source,
    params.kaisaiNen,
    params.kaisaiTsukihi,
    normalizeKeibajoCode(params.keibajoCode),
    normalizeRaceBango(params.raceBango),
  ]);
  const rows = result.rows.map((row) => rowToFeaturePayload(row, featureNames));
  const raceKey = buildRunningStyleRaceKey(params);
  if (rows.some((row) => row.raceKey !== raceKey)) {
    throw new Error(`unexpected race key in PostgreSQL feature result for ${raceKey}`);
  }
  return {
    elapsedMs: Math.round(performance.now() - started),
    rows,
    sqlRows: result.rowCount ?? result.rows.length,
  };
};

export interface DailyTargetRow {
  babajotai_code_dirt: string | null;
  babajotai_code_shiba: string | null;
  bamei: string | null;
  chokyoshimei_ryakusho: string | null;
  grade_code: string | null;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  ketto_toroku_bango: string;
  kishumei_ryakusho: string | null;
  kyori: number | null;
  kyoso_joken_code: string | null;
  race_bango: string;
  race_date: string;
  shusso_tosu: number | null;
  source: "jra" | "nar";
  track_code: string | null;
  umaban: number | null;
}

const D1_TARGET_CTE_SQL = `target as (
  select
    j.source,
    j.race_date,
    to_date(j.race_date, 'YYYYMMDD') as race_dt,
    j.kaisai_nen,
    j.kaisai_tsukihi,
    lpad(j.keibajo_code, 2, '0') as keibajo_code,
    lpad(j.race_bango, 2, '0') as race_bango,
    j.ketto_toroku_bango,
    j.umaban,
    j.bamei,
    case when j.source = 'jra' then 'jra' when lpad(j.keibajo_code, 2, '0') = '83' then 'ban-ei' else 'nar' end as category,
    j.kyori,
    j.track_code,
    j.grade_code,
    coalesce(
      nullif(j.shusso_tosu, 0),
      count(*) over (partition by j.source, j.kaisai_nen, j.kaisai_tsukihi, j.keibajo_code, j.race_bango)::int
    ) as shusso_tosu,
    null::int as finish_position,
    null::numeric as finish_norm,
    j.kishumei_ryakusho,
    j.chokyoshimei_ryakusho,
    j.kyoso_joken_code,
    j.babajotai_code_shiba,
    j.babajotai_code_dirt,
    null::numeric as target_corner_1_norm,
    null::numeric as target_corner_3_norm,
    null::numeric as target_corner_4_norm,
    null::int as target_running_style_class,
    'v1' as feature_schema_version,
    cast(substr(j.race_date, 1, 4) as int) as race_year
  from jsonb_to_recordset($6::jsonb) as j(
    source text,
    race_date text,
    kaisai_nen text,
    kaisai_tsukihi text,
    keibajo_code text,
    race_bango text,
    ketto_toroku_bango text,
    umaban int,
    bamei text,
    kyori int,
    track_code text,
    grade_code text,
    shusso_tosu int,
    kyoso_joken_code text,
    babajotai_code_shiba text,
    babajotai_code_dirt text,
    kishumei_ryakusho text,
    chokyoshimei_ryakusho text
  )
  where j.ketto_toroku_bango is not null
)`;

const REC_TARGET_CTE_MARKER =
  /target as \(\s*select\s+r\.source,[\s\S]*?where r\.ketto_toroku_bango is not null\s*\)/;

export const buildRunningStylePostgresFeatureSqlWithD1Target = (): string => {
  // REC_TARGET_CTE_MARKER is matched against a hardcoded SQL constant in the
  // same module, so the marker always matches. replace() would silently no-op
  // if it ever stopped matching, surfacing the regression via downstream SQL
  // failures rather than this synchronous throw.
  const baseSql = buildRunningStylePostgresFeatureSql();
  return baseSql.replace(REC_TARGET_CTE_MARKER, D1_TARGET_CTE_SQL);
};

export const buildRunningStyleFeaturesForRaceFromD1Target = async (
  pool: Pool,
  params: RunningStyleRaceParams,
  featureNames: ReadonlyArray<string>,
  targetRows: ReadonlyArray<DailyTargetRow>,
): Promise<PostgresFeatureBuildSummary> => {
  if (targetRows.length === 0) {
    throw new Error(`no D1 target rows provided for race ${buildRunningStyleRaceKey(params)}`);
  }
  const started = performance.now();
  const result = await pool.query<SqlRow>(buildRunningStylePostgresFeatureSqlWithD1Target(), [
    params.source,
    params.kaisaiNen,
    params.kaisaiTsukihi,
    normalizeKeibajoCode(params.keibajoCode),
    normalizeRaceBango(params.raceBango),
    JSON.stringify(targetRows),
  ]);
  const rows = result.rows.map((row) => rowToFeaturePayload(row, featureNames));
  const raceKey = buildRunningStyleRaceKey(params);
  if (rows.some((row) => row.raceKey !== raceKey)) {
    throw new Error(`unexpected race key in PostgreSQL feature result for ${raceKey}`);
  }
  return {
    elapsedMs: Math.round(performance.now() - started),
    rows,
    sqlRows: result.rowCount ?? result.rows.length,
  };
};
