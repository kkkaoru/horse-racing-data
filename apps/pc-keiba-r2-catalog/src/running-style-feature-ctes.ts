const RECENT_WINDOW_SIZE = 5;
const SAME_DISTANCE_TOLERANCE = 200;
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

const raceKeyJoin = (left: string, right: string): string =>
  `${right}.source = ${left}.source
    and ${right}.kaisai_nen = ${left}.kaisai_nen
    and ${right}.kaisai_tsukihi = ${left}.kaisai_tsukihi
    and ${right}.keibajo_code = ${left}.keibajo_code
    and ${right}.race_bango = ${left}.race_bango`;

const entryKeyJoin = (left: string, right: string): string =>
  `${raceKeyJoin(left, right)}
    and ${right}.ketto_toroku_bango = ${left}.ketto_toroku_bango`;

export const runningStyleFeatureCtesSql = (
  masterTable: string,
): string => `target_current_bataiju as (
  select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
         ketto_toroku_bango, bataiju as current_bataiju
  from target
),
horse_history_unranked as (
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
    h.finish_norm * 1.0 as finish_norm,
    h.time_sa * 1.0 as time_sa,
    h.kohan_3f * 1.0 as kohan_3f,
    h.corner1_norm * 1.0 as corner1_norm,
    h.corner2_norm * 1.0 as corner2_norm,
    h.corner3_norm * 1.0 as corner3_norm,
    h.corner4_norm * 1.0 as corner4_norm,
    h.zenhan_3f as zenhan_3f,
    h.kyori as history_kyori,
    h.track_code as history_track_code,
    h.grade_code as history_grade_code,
    case h.kyoso_joken_code
      when '000' then 0 when '005' then 1 when '010' then 2 when '016' then 3
      when '701' then 4 when '703' then 5 when '999' then 6
      else null end as history_class_level,
    h.bataiju as history_bataiju
  from target t
  join rec h
    on h.source = t.source
   and h.ketto_toroku_bango = t.ketto_toroku_bango
   and h.race_date < t.race_date
  where h.finish_position is not null
),
horse_history_base as (
  select
    h.source, h.kaisai_nen, h.kaisai_tsukihi, h.keibajo_code, h.race_bango,
    h.ketto_toroku_bango, h.target_race_dt, h.target_keibajo, h.target_kyori,
    h.target_track_code, h.target_grade_code, h.target_class_level,
    h.history_kaisai_nen, h.history_kaisai_tsukihi, h.history_keibajo,
    h.history_race_bango, h.history_race_dt, h.finish_position, h.finish_norm,
    h.time_sa, h.kohan_3f, h.corner1_norm, h.corner2_norm, h.corner3_norm,
    h.corner4_norm, h.zenhan_3f, h.history_kyori, h.history_track_code,
    h.history_grade_code, h.history_class_level, h.history_bataiju,
    count(newer.history_race_dt) + 1 as recent_rank
  from horse_history_unranked h
  left join horse_history_unranked newer
    on ${entryKeyJoin("h", "newer")}
   and newer.history_race_dt > h.history_race_dt
  group by
    h.source, h.kaisai_nen, h.kaisai_tsukihi, h.keibajo_code, h.race_bango,
    h.ketto_toroku_bango, h.target_race_dt, h.target_keibajo, h.target_kyori,
    h.target_track_code, h.target_grade_code, h.target_class_level,
    h.history_kaisai_nen, h.history_kaisai_tsukihi, h.history_keibajo,
    h.history_race_bango, h.history_race_dt, h.finish_position, h.finish_norm,
    h.time_sa, h.kohan_3f, h.corner1_norm, h.corner2_norm, h.corner3_norm,
    h.corner4_norm, h.zenhan_3f, h.history_kyori, h.history_track_code,
    h.history_grade_code, h.history_class_level, h.history_bataiju
),
horse_career as (
  select
    source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
    avg(case when recent_rank <= ${RECENT_WINDOW_SIZE} then time_sa else null end) as speed_index_avg_5,
    min(case when recent_rank <= ${RECENT_WINDOW_SIZE} then time_sa else null end) as speed_index_best_5,
    avg(case when recent_rank <= ${RECENT_WINDOW_SIZE} then kohan_3f else null end) as kohan3f_avg_5,
    avg(case when recent_rank <= ${RECENT_WINDOW_SIZE} then zenhan_3f else null end) as past_first_3f_avg_5,
    avg(case when recent_rank <= ${RECENT_WINDOW_SIZE} then corner4_norm else null end) as corner_pass_avg_5,
    avg(case when finish_position = 1 then 1 else 0 end) as career_win_rate,
    avg(case when finish_position between 1 and 3 then 1 else 0 end) as career_place_rate,
    count(case when finish_position = 1 then 1 else null end) as career_top1_count,
    avg(case when history_keibajo = target_keibajo and finish_position = 1 then 1 when history_keibajo = target_keibajo then 0 else null end) as same_keibajo_win_rate,
    avg(case when abs(history_kyori - target_kyori) <= ${SAME_DISTANCE_TOLERANCE} and finish_position = 1 then 1 when abs(history_kyori - target_kyori) <= ${SAME_DISTANCE_TOLERANCE} then 0 else null end) as same_distance_win_rate,
    avg(case when left(coalesce(history_track_code, ''), 1) = left(coalesce(target_track_code, ''), 1) and finish_position = 1 then 1 when left(coalesce(history_track_code, ''), 1) = left(coalesce(target_track_code, ''), 1) then 0 else null end) as same_track_win_rate,
    avg(case when coalesce(history_grade_code, '') = coalesce(target_grade_code, '') and finish_position = 1 then 1 when coalesce(history_grade_code, '') = coalesce(target_grade_code, '') then 0 else null end) as same_grade_win_rate,
    max(target_race_dt) - max(case when recent_rank = 1 then history_race_dt else null end) as days_since_last_race,
    count(case when target_race_dt - history_race_dt <= ${CONSECUTIVE_RACE_WINDOW_DAYS} then 1 else null end) as consecutive_race_count
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
    h.corner1_norm * 1.0 as corner1_norm,
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
  where h.finish_position is not null and t.kishumei_ryakusho is not null
),
jockey_career as (
  select
    source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
    avg(case when finish_position = 1 then 1 else 0 end) as jockey_career_win_rate,
    avg(case when history_race_dt >= target_race_dt - ${JOCKEY_RECENT_DAYS} and finish_position = 1 then 1 when history_race_dt >= target_race_dt - ${JOCKEY_RECENT_DAYS} then 0 else null end) as jockey_recent_win_rate,
    avg(case when history_keibajo = target_keibajo and finish_position = 1 then 1 when history_keibajo = target_keibajo then 0 else null end) as jockey_keibajo_win_rate,
    avg(case when abs(history_kyori - target_kyori) <= ${SAME_DISTANCE_TOLERANCE} and finish_position = 1 then 1 when abs(history_kyori - target_kyori) <= ${SAME_DISTANCE_TOLERANCE} then 0 else null end) as jockey_distance_win_rate,
    avg(case when left(coalesce(history_track_code, ''), 1) = left(coalesce(target_track_code, ''), 1) and finish_position = 1 then 1 when left(coalesce(history_track_code, ''), 1) = left(coalesce(target_track_code, ''), 1) then 0 else null end) as jockey_track_win_rate,
    avg(case when coalesce(history_grade_code, '') = coalesce(target_grade_code, '') and finish_position = 1 then 1 when coalesce(history_grade_code, '') = coalesce(target_grade_code, '') then 0 else null end) as jockey_grade_win_rate,
    count(case when history_horse = target_horse then 1 else null end) as jockey_horse_pair_count,
    avg(case when history_horse = target_horse and finish_position = 1 then 1 when history_horse = target_horse then 0 else null end) as jockey_horse_pair_win_rate,
    avg(case when history_horse = target_horse and corner1_norm = 0 then 1.0 when history_horse = target_horse and corner1_norm is not null then 0.0 else null end) as jockey_horse_pair_nige_rate,
    avg(case when corner1_norm = 0 then 1.0 when corner1_norm is null then null else 0.0 end) as jockey_nige_rate,
    avg(case when corner1_norm is null then null when corner1_norm > 0 and corner1_norm <= ${RUNNING_STYLE_SENKOU_THRESHOLD} then 1.0 else 0.0 end) as jockey_senkou_rate,
    avg(case when corner1_norm is null then null when corner1_norm > ${RUNNING_STYLE_SENKOU_THRESHOLD} and corner1_norm <= ${RUNNING_STYLE_SASHI_THRESHOLD} then 1.0 else 0.0 end) as jockey_sashi_rate,
    avg(case when corner1_norm is null then null when corner1_norm > ${RUNNING_STYLE_SASHI_THRESHOLD} then 1.0 else 0.0 end) as jockey_oikomi_rate,
    avg(corner1_norm) as jockey_corner_1_norm_avg,
    avg(case when history_horse = target_horse then corner1_norm else null end) as jockey_horse_corner_1_norm_avg,
    avg(case when history_race_dt >= target_race_dt - ${JOCKEY_RECENT_DAYS} then corner1_norm else null end) as jockey_recent_corner_1_norm_avg_90d,
    avg(case when history_race_dt >= target_race_dt - ${JOCKEY_RECENT_DAYS} and corner1_norm = 0 then 1.0 when history_race_dt >= target_race_dt - ${JOCKEY_RECENT_DAYS} and corner1_norm is not null then 0.0 else null end) as jockey_recent_nige_rate_90d
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
    h.corner1_norm * 1.0 as corner1_norm,
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
  where h.finish_position is not null and t.chokyoshimei_ryakusho is not null
),
trainer_career as (
  select
    source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
    avg(case when finish_position = 1 then 1 else 0 end) as trainer_career_win_rate,
    avg(case when history_keibajo = target_keibajo and finish_position = 1 then 1 when history_keibajo = target_keibajo then 0 else null end) as trainer_keibajo_win_rate,
    avg(case when abs(history_kyori - target_kyori) <= ${SAME_DISTANCE_TOLERANCE} and finish_position = 1 then 1 when abs(history_kyori - target_kyori) <= ${SAME_DISTANCE_TOLERANCE} then 0 else null end) as trainer_distance_win_rate,
    avg(case when history_horse = target_horse and finish_position = 1 then 1 when history_horse = target_horse then 0 else null end) as trainer_horse_win_rate,
    avg(case when history_horse = target_horse and corner1_norm = 0 then 1.0 when history_horse = target_horse and corner1_norm is not null then 0.0 else null end) as trainer_horse_pair_nige_rate,
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
    um.ketto_joho_01b as ketto_joho_01b,
    um.ketto_joho_05b as ketto_joho_05b,
    r.corner1_norm * 1.0 as corner1_norm
  from rec r
  left join ${masterTable} um on um.ketto_toroku_bango = r.ketto_toroku_bango
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
    sum(m.win_count) * 1.0 / nullif(sum(m.race_count), 0) as sire_distance_win_rate_val,
    sum(m.finish_norm_sum) * 1.0 / nullif(sum(m.finish_norm_count), 0) as sire_avg_finish_at_distance_val,
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
    sum(m.win_count) * 1.0 / nullif(sum(m.race_count), 0) as sire_track_win_rate_val,
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
    sum(m.win_count) * 1.0 / nullif(sum(m.race_count), 0) as dam_sire_distance_win_rate_val,
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
    sum(m.finish_norm_sum) * 1.0 / nullif(sum(m.finish_norm_count), 0) as damsire_avg_finish_at_track_val,
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
    sum(m.nige_count) * 1.0 / nullif(sum(m.race_count), 0) as sire_nige_rate_val,
    sum(m.senkou_count) * 1.0 / nullif(sum(m.race_count), 0) as sire_senkou_rate_val,
    sum(m.sashi_count) * 1.0 / nullif(sum(m.race_count), 0) as sire_sashi_rate_val,
    sum(m.oikomi_count) * 1.0 / nullif(sum(m.race_count), 0) as sire_oikomi_rate_val,
    sum(m.corner1_norm_sum) * 1.0 / nullif(sum(m.corner1_norm_count), 0) as sire_corner_1_norm_avg_val,
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
    um.ketto_joho_01b as target_sire,
    um.ketto_joho_05b as target_damsire
  from target t
  left join ${masterTable} um on um.ketto_toroku_bango = t.ketto_toroku_bango
),
race_horses as (
  select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
         ketto_toroku_bango, speed_index_avg_5, speed_index_best_5, same_distance_win_rate
  from horse_career
),
race_field_aggregates as (
  select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
         avg(speed_index_avg_5) as race_avg_speed,
         count(case when same_distance_win_rate > ${RIVAL_DISTANCE_THRESHOLD} then 1 else null end) as race_strong_count
  from race_horses
  group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
),
race_speed_ranked as (
  select
    candidate.source, candidate.kaisai_nen, candidate.kaisai_tsukihi,
    candidate.keibajo_code, candidate.race_bango, candidate.ketto_toroku_bango,
    candidate.speed_index_best_5,
    count(faster.ketto_toroku_bango) + 1 as rk
  from race_horses candidate
  left join race_horses faster
    on ${raceKeyJoin("candidate", "faster")}
   and (
     faster.speed_index_best_5 < candidate.speed_index_best_5
     or (
       faster.speed_index_best_5 = candidate.speed_index_best_5
       and faster.ketto_toroku_bango < candidate.ketto_toroku_bango
     )
   )
  where candidate.speed_index_best_5 is not null
  group by
    candidate.source, candidate.kaisai_nen, candidate.kaisai_tsukihi,
    candidate.keibajo_code, candidate.race_bango, candidate.ketto_toroku_bango,
    candidate.speed_index_best_5
),
race_top3_speed as (
  select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
         avg(speed_index_best_5) as race_top_speed
  from race_speed_ranked
  where rk <= 3
  group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
),
track_bias as (
  select t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
    avg(case when h.finish_position = 1 and h.umaban * 2 <= h.shusso_tosu + 1 then 1 else 0 end) as track_bias_inside,
    avg(case when h.finish_position = 1 and h.corner1_norm * 1.0 <= ${FRONT_CORNER_THRESHOLD} then 1 else 0 end) as track_bias_front
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
    avg(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.history_bataiju else null end) as weight_avg_5
  from horse_history_base b
  left join target_current_bataiju tcb on ${entryKeyJoin("b", "tcb")}
  group by b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango, b.ketto_toroku_bango
),
recent_form as (
  select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
    max(case when recent_rank = 1 then finish_norm else null end) as last_race_finish_norm,
    max(case when recent_rank = 1 then time_sa else null end) as last_race_margin_to_winner,
    max(case when recent_rank = 1 then corner3_norm else null end) as last_race_corner_pass_norm,
    max(case when recent_rank = 1 then target_class_level else null end)
      - max(case when recent_rank = 1 then history_class_level else null end) as last_race_class_diff,
    max(case when recent_rank = 1 then history_kyori else null end)
      - max(case when recent_rank = 1 then target_kyori else null end) as last_race_distance_diff,
    case when count(case when recent_rank <= ${RECENT_WINDOW_SIZE} then 1 else null end) >= ${TREND_MIN_RACES}
         then regr_slope(case when recent_rank <= ${RECENT_WINDOW_SIZE} then finish_norm else null end, case when recent_rank <= ${RECENT_WINDOW_SIZE} then recent_rank * 1.0 else null end)
         else null end as finish_trend_5,
    avg(case when recent_rank <= 3 then finish_norm else null end) as last_3_avg_finish_norm
  from horse_history_base
  group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
),
legacy_horse_avg as (
  select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
    avg(finish_norm) as avg_finish,
    avg(case when recent_rank <= ${RECENT_WINDOW_SIZE} then finish_norm else null end) as recent_finish
  from horse_history_base
  group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
),
legacy_target as (
  select t.source, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango, t.ketto_toroku_bango,
    rec.tansho_ninkijun as ninkijun,
    rec.tansho_odds * 1.0 as odds_value,
    rec.shusso_tosu as runner_count
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
         then greatest(0 * 1.0, least(1 * 1.0, (t.ninkijun - 1) * 1.0 / nullif(t.runner_count - 1, 0)))
         else null end as popularity_score,
    case when t.odds_value is not null and t.odds_value > 0
         then greatest(0 * 1.0, least(1 * 1.0, ln(greatest(t.odds_value, 1 * 1.0)) / ln(300 * 1.0)))
         else null end as odds_score
  from legacy_target t
  left join legacy_horse_avg lha on ${entryKeyJoin("t", "lha")}
),
horse_running_style_history as (
  select b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango, b.ketto_toroku_bango,
    avg(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner1_norm else null end) as past_corner_1_norm_avg_5,
    avg(case when b.recent_rank <= 3 then b.corner1_norm else null end) as past_corner_1_norm_avg_3,
    avg(case when b.recent_rank <= 10 then b.corner1_norm else null end) as past_corner_1_norm_avg_10,
    avg(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner2_norm else null end) as past_corner_2_norm_avg_5,
    avg(case when b.recent_rank <= 3 then b.corner2_norm else null end) as past_corner_2_norm_avg_3,
    avg(case when b.recent_rank <= 10 then b.corner2_norm else null end) as past_corner_2_norm_avg_10,
    avg(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner3_norm else null end) as past_corner_3_norm_avg_5,
    avg(case when b.recent_rank <= 3 then b.corner3_norm else null end) as past_corner_3_norm_avg_3,
    avg(case when b.recent_rank <= 10 then b.corner3_norm else null end) as past_corner_3_norm_avg_10,
    avg(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner4_norm else null end) as past_corner_4_norm_avg_5,
    avg(case when b.recent_rank <= 3 then b.corner4_norm else null end) as past_corner_4_norm_avg_3,
    avg(case when b.recent_rank <= 10 then b.corner4_norm else null end) as past_corner_4_norm_avg_10,
    avg(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner4_norm - b.corner1_norm else null end) as past_corner_progression_avg_5,
    stddev_samp(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner1_norm else null end) as past_corner_1_norm_std_5,
    stddev_samp(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner2_norm else null end) as past_corner_2_norm_std_5,
    stddev_samp(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner3_norm else null end) as past_corner_3_norm_std_5,
    stddev_samp(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner4_norm else null end) as past_corner_4_norm_std_5,
    min(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner1_norm else null end) as past_corner_1_norm_best_5,
    min(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner2_norm else null end) as past_corner_2_norm_best_5,
    min(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner3_norm else null end) as past_corner_3_norm_best_5,
    min(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner4_norm else null end) as past_corner_4_norm_best_5,
    max(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner1_norm else null end) as past_corner_1_norm_worst_5,
    max(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner2_norm else null end) as past_corner_2_norm_worst_5,
    max(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner3_norm else null end) as past_corner_3_norm_worst_5,
    max(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner4_norm else null end) as past_corner_4_norm_worst_5,
    avg(case when b.corner1_norm = 0 then 1.0 when b.corner1_norm is null then null else 0.0 end) as past_nige_rate_self,
    avg(case when b.corner1_norm is null then null when b.corner1_norm > 0 and b.corner1_norm <= ${RUNNING_STYLE_SENKOU_THRESHOLD} then 1.0 else 0.0 end) as past_senkou_rate_self,
    avg(case when b.corner1_norm is null then null when b.corner1_norm > ${RUNNING_STYLE_SENKOU_THRESHOLD} and b.corner1_norm <= ${RUNNING_STYLE_SASHI_THRESHOLD} then 1.0 else 0.0 end) as past_sashi_rate_self,
    avg(case when b.corner1_norm is null then null when b.corner1_norm > ${RUNNING_STYLE_SASHI_THRESHOLD} then 1.0 else 0.0 end) as past_oikomi_rate_self,
    case when count(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm is not null then 1 else null end) > 0
      then (count(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm = 0 then 1 else null end)) * 1.0
        / count(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm is not null then 1 else null end)
      else null end as past_nige_rate_self_recent_5,
    case when count(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm is not null then 1 else null end) > 0
      then (count(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm > 0 and b.corner1_norm <= ${RUNNING_STYLE_SENKOU_THRESHOLD} then 1 else null end)) * 1.0
        / count(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm is not null then 1 else null end)
      else null end as past_senkou_rate_self_recent_5,
    case when count(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm is not null then 1 else null end) > 0
      then (count(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm > ${RUNNING_STYLE_SENKOU_THRESHOLD} and b.corner1_norm <= ${RUNNING_STYLE_SASHI_THRESHOLD} then 1 else null end)) * 1.0
        / count(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm is not null then 1 else null end)
      else null end as past_sashi_rate_self_recent_5,
    case when count(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm is not null then 1 else null end) > 0
      then (count(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm > ${RUNNING_STYLE_SASHI_THRESHOLD} then 1 else null end)) * 1.0
        / count(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} and b.corner1_norm is not null then 1 else null end)
      else null end as past_oikomi_rate_self_recent_5,
    case when count(case when b.recent_rank <= 3 and b.corner1_norm is not null then 1 else null end) > 0
      then (count(case when b.recent_rank <= 3 and b.corner1_norm = 0 then 1 else null end)) * 1.0
        / count(case when b.recent_rank <= 3 and b.corner1_norm is not null then 1 else null end)
      else null end as past_nige_rate_self_recent_3,
    case when count(case when b.recent_rank <= 3 and b.corner1_norm is not null then 1 else null end) > 0
      then (count(case when b.recent_rank <= 3 and b.corner1_norm > 0 and b.corner1_norm <= ${RUNNING_STYLE_SENKOU_THRESHOLD} then 1 else null end)) * 1.0
        / count(case when b.recent_rank <= 3 and b.corner1_norm is not null then 1 else null end)
      else null end as past_senkou_rate_self_recent_3,
    case when count(case when b.recent_rank <= 3 and b.corner1_norm is not null then 1 else null end) > 0
      then (count(case when b.recent_rank <= 3 and b.corner1_norm > ${RUNNING_STYLE_SENKOU_THRESHOLD} and b.corner1_norm <= ${RUNNING_STYLE_SASHI_THRESHOLD} then 1 else null end)) * 1.0
        / count(case when b.recent_rank <= 3 and b.corner1_norm is not null then 1 else null end)
      else null end as past_sashi_rate_self_recent_3,
    case when count(case when b.recent_rank <= 3 and b.corner1_norm is not null then 1 else null end) > 0
      then (count(case when b.recent_rank <= 3 and b.corner1_norm > ${RUNNING_STYLE_SASHI_THRESHOLD} then 1 else null end)) * 1.0
        / count(case when b.recent_rank <= 3 and b.corner1_norm is not null then 1 else null end)
      else null end as past_oikomi_rate_self_recent_3,
    max(case when b.recent_rank = 1 then b.corner1_norm else null end) as last_race_corner_1_norm,
    max(case when b.recent_rank = 1 then b.corner2_norm else null end) as last_race_corner_2_norm,
    max(case when b.recent_rank = 1 then b.corner3_norm else null end) as last_race_corner_3_norm,
    max(case when b.recent_rank = 1 then b.corner4_norm else null end) as last_race_corner_4_norm,
    max(case when b.recent_rank = 1 then b.corner4_norm - b.corner1_norm else null end) as last_race_corner_progression,
    avg(case when abs(b.history_kyori - b.target_kyori) <= ${SAME_DISTANCE_TOLERANCE} then b.corner1_norm else null end) as horse_distance_corner_1_norm_avg,
    avg(case when left(coalesce(b.history_track_code, ''), 1) = left(coalesce(b.target_track_code, ''), 1) then b.corner1_norm else null end) as horse_track_corner_1_norm_avg,
    avg(case when b.history_keibajo = b.target_keibajo then b.corner1_norm else null end) as horse_keibajo_corner_1_norm_avg,
    avg(case when coalesce(b.history_grade_code, '') = coalesce(b.target_grade_code, '') then b.corner1_norm else null end) as horse_grade_corner_1_norm_avg,
    avg(case when b.corner1_norm = 0 and b.finish_position = 1 then 1.0 when b.corner1_norm = 0 then 0.0 else null end) as past_nige_win_rate_self,
    avg(case when b.corner1_norm > 0 and b.corner1_norm <= ${RUNNING_STYLE_SENKOU_THRESHOLD} and b.finish_position = 1 then 1.0 when b.corner1_norm > 0 and b.corner1_norm <= ${RUNNING_STYLE_SENKOU_THRESHOLD} then 0.0 else null end) as past_senkou_win_rate_self,
    avg(case when b.corner1_norm > ${RUNNING_STYLE_SENKOU_THRESHOLD} and b.corner1_norm <= ${RUNNING_STYLE_SASHI_THRESHOLD} and b.finish_position = 1 then 1.0 when b.corner1_norm > ${RUNNING_STYLE_SENKOU_THRESHOLD} and b.corner1_norm <= ${RUNNING_STYLE_SASHI_THRESHOLD} then 0.0 else null end) as past_sashi_win_rate_self,
    avg(case when b.corner1_norm > ${RUNNING_STYLE_SASHI_THRESHOLD} and b.finish_position = 1 then 1.0 when b.corner1_norm > ${RUNNING_STYLE_SASHI_THRESHOLD} then 0.0 else null end) as past_oikomi_win_rate_self,
    approx_percentile_cont(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner1_norm else null end, 0.75)
      - approx_percentile_cont(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner1_norm else null end, 0.25) as past_corner_1_norm_iqr_5,
    approx_percentile_cont(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner2_norm else null end, 0.75)
      - approx_percentile_cont(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner2_norm else null end, 0.25) as past_corner_2_norm_iqr_5,
    approx_percentile_cont(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner3_norm else null end, 0.75)
      - approx_percentile_cont(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner3_norm else null end, 0.25) as past_corner_3_norm_iqr_5,
    approx_percentile_cont(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner4_norm else null end, 0.75)
      - approx_percentile_cont(case when b.recent_rank <= ${RECENT_WINDOW_SIZE} then b.corner4_norm else null end, 0.25) as past_corner_4_norm_iqr_5,
    (count(case when b.finish_position = 1 and trim(coalesce(b.history_grade_code, '')) in ('A', 'B', 'C') then 1 else null end)) as top1_count_in_grade_races,
    (count(case when b.finish_position between 1 and 3 and trim(coalesce(b.history_grade_code, '')) in ('A', 'B', 'C') then 1 else null end)) as place_count_in_grade_races,
    (count(case when trim(coalesce(b.history_grade_code, '')) = 'A' then 1 else null end)) as experience_in_g1_race,
    (count(case when b.finish_position = 1 and b.recent_rank <= ${RECENT_WINDOW_SIZE} then 1 else null end)) as recent_win_count_5,
    (count(case when b.finish_position between 1 and 3 and b.recent_rank <= ${RECENT_WINDOW_SIZE} then 1 else null end)) as recent_top3_count_5,
    avg(case when b.recent_rank <= 3 then b.kohan_3f else null end) as last_3_avg_kohan_3f,
    greatest(
      count(case when b.corner1_norm = 0 and b.recent_rank <= ${RECENT_WINDOW_SIZE} then 1 else null end),
      count(case when b.corner1_norm > 0 and b.corner1_norm <= ${RUNNING_STYLE_SENKOU_THRESHOLD} and b.recent_rank <= ${RECENT_WINDOW_SIZE} then 1 else null end),
      count(case when b.corner1_norm > ${RUNNING_STYLE_SENKOU_THRESHOLD} and b.corner1_norm <= ${RUNNING_STYLE_SASHI_THRESHOLD} and b.recent_rank <= ${RECENT_WINDOW_SIZE} then 1 else null end),
      count(case when b.corner1_norm > ${RUNNING_STYLE_SASHI_THRESHOLD} and b.recent_rank <= ${RECENT_WINDOW_SIZE} then 1 else null end)
    ) * 1.0 / nullif(count(case when b.corner1_norm is not null and b.recent_rank <= ${RECENT_WINDOW_SIZE} then 1 else null end), 0)
      as past_dominant_label_consistency_5
  from horse_history_base b
  group by b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango, b.ketto_toroku_bango
),
weather_lookup as (
  select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
         ketto_toroku_bango, tenko_code
  from target
),
base_features as (
  select
    t.source, t.race_date, t.kaisai_nen, t.kaisai_tsukihi, t.keibajo_code, t.race_bango,
    t.ketto_toroku_bango, t.umaban, t.bamei, t.category, t.kyori, t.track_code, t.grade_code, t.shusso_tosu,
    t.kyoso_joken_code, t.nar_subclass,
    t.finish_position, t.finish_norm,
    t.target_corner_1_norm, t.target_corner_2_norm, t.target_corner_3_norm, t.target_corner_4_norm, t.target_running_style_class,
    hc.speed_index_avg_5, hc.speed_index_best_5, hc.kohan3f_avg_5, hc.past_first_3f_avg_5, hc.corner_pass_avg_5,
    hc.career_win_rate, hc.career_place_rate, hc.career_top1_count,
    hc.same_keibajo_win_rate, hc.same_distance_win_rate, hc.same_track_win_rate, hc.same_grade_win_rate,
    wa.weight_avg_5,
    wa.current_bataiju_kept * 1.0 - wa.weight_avg_5 as weight_diff_from_avg,
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
    ) / ${PEDIGREE_COMPOSITE_DIVISOR} * 1.0 as pedigree_score_for_race,
    rfa.race_avg_speed as field_strength_avg_speed,
    rts.race_top_speed as field_strength_top3_speed,
    greatest(0, rfa.race_strong_count - case when hc.same_distance_win_rate > ${RIVAL_DISTANCE_THRESHOLD} then 1 else 0 end) as rival_count_at_distance,
    tb.track_bias_inside,
    tb.track_bias_front,
    case wl.tenko_code
      when '1' then 0 * 1.0 when '2' then 0.3 * 1.0
      when '3' then 0.7 * 1.0 when '4' then 0.7 * 1.0
      when '5' then 1.0 * 1.0 when '6' then 1.0 * 1.0
      else null end as weather_normalized,
    case
      when left(coalesce(t.track_code, ''), 1) = '1' and t.babajotai_code_shiba = '1' then 0 * 1.0
      when left(coalesce(t.track_code, ''), 1) = '1' and t.babajotai_code_shiba = '2' then 0.3 * 1.0
      when left(coalesce(t.track_code, ''), 1) = '1' and t.babajotai_code_shiba = '3' then 0.6 * 1.0
      when left(coalesce(t.track_code, ''), 1) = '1' and t.babajotai_code_shiba = '4' then 1.0 * 1.0
      when left(coalesce(t.track_code, ''), 1) <> '1' and t.babajotai_code_dirt = '1' then 0 * 1.0
      when left(coalesce(t.track_code, ''), 1) <> '1' and t.babajotai_code_dirt = '2' then 0.3 * 1.0
      when left(coalesce(t.track_code, ''), 1) <> '1' and t.babajotai_code_dirt = '3' then 0.6 * 1.0
      when left(coalesce(t.track_code, ''), 1) <> '1' and t.babajotai_code_dirt = '4' then 1.0 * 1.0
      else null
    end as track_condition_normalized,
    least(1 * 1.0, greatest(0 * 1.0, coalesce(t.shusso_tosu, 0) * 1.0 / ${MAX_FIELD_SIZE})) as field_size_normalized,
    case when trim(coalesce(t.grade_code, '')) in ('A', 'B', 'C', 'D', 'G', 'H') then 1 else 0 end as is_grade_race,
    rf.last_race_finish_norm, rf.last_race_margin_to_winner, rf.last_race_corner_pass_norm,
    rf.last_race_class_diff, rf.last_race_distance_diff, rf.finish_trend_5, rf.last_3_avg_finish_norm,
    lf.avg_finish, lf.recent_finish, lf.popularity_score, lf.odds_score,
    rsh.past_corner_1_norm_avg_5,
    rsh.past_corner_1_norm_avg_3,
    rsh.past_corner_1_norm_avg_10,
    rsh.past_corner_2_norm_avg_5,
    rsh.past_corner_2_norm_avg_3,
    rsh.past_corner_2_norm_avg_10,
    rsh.past_corner_3_norm_avg_5,
    rsh.past_corner_3_norm_avg_3,
    rsh.past_corner_3_norm_avg_10,
    rsh.past_corner_4_norm_avg_5,
    rsh.past_corner_4_norm_avg_3,
    rsh.past_corner_4_norm_avg_10,
    rsh.past_corner_progression_avg_5,
    rsh.past_corner_1_norm_std_5,
    rsh.past_corner_2_norm_std_5,
    rsh.past_corner_3_norm_std_5,
    rsh.past_corner_4_norm_std_5,
    rsh.past_corner_1_norm_best_5,
    rsh.past_corner_2_norm_best_5,
    rsh.past_corner_3_norm_best_5,
    rsh.past_corner_4_norm_best_5,
    rsh.past_corner_1_norm_worst_5,
    rsh.past_corner_2_norm_worst_5,
    rsh.past_corner_3_norm_worst_5,
    rsh.past_corner_4_norm_worst_5,
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
    rsh.last_race_corner_2_norm,
    rsh.last_race_corner_3_norm,
    rsh.last_race_corner_4_norm,
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
    rsh.past_corner_2_norm_iqr_5,
    rsh.past_corner_3_norm_iqr_5,
    rsh.past_corner_4_norm_iqr_5,
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
      else least(1.0, greatest(0.0, (t.umaban * 1.0 - 1) / (t.shusso_tosu * 1.0 - 1)))
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
    concat(t.source, ':', t.kaisai_nen, ':', t.kaisai_tsukihi, ':', t.keibajo_code, ':', t.race_bango) as race_id
  from target t
  left join horse_career hc on ${entryKeyJoin("t", "hc")}
  left join jockey_career jc on ${entryKeyJoin("t", "jc")}
  left join trainer_career tc on ${entryKeyJoin("t", "tc")}
  left join target_pedigree tp on ${entryKeyJoin("t", "tp")}
  left join sire_distance_stats sds on sds.sire = tp.target_sire and sds.kyori_band = tp.kyori_band and sds.stats_year_month = cast(t.kaisai_nen as int) * 100 + cast(substr(t.kaisai_tsukihi, 1, 2) as int)
  left join sire_track_stats sts on sts.sire = tp.target_sire and sts.surface = tp.surface and sts.stats_year_month = cast(t.kaisai_nen as int) * 100 + cast(substr(t.kaisai_tsukihi, 1, 2) as int)
  left join damsire_distance_stats dsd on dsd.damsire = tp.target_damsire and dsd.kyori_band = tp.kyori_band and dsd.stats_year_month = cast(t.kaisai_nen as int) * 100 + cast(substr(t.kaisai_tsukihi, 1, 2) as int)
  left join damsire_track_stats dst on dst.damsire = tp.target_damsire and dst.surface = tp.surface and dst.stats_year_month = cast(t.kaisai_nen as int) * 100 + cast(substr(t.kaisai_tsukihi, 1, 2) as int)
  left join sire_running_style_stats srs on srs.sire = tp.target_sire and srs.rs_bucket = tp.rs_bucket and srs.stats_year_month = cast(t.kaisai_nen as int) * 100 + cast(substr(t.kaisai_tsukihi, 1, 2) as int)
  left join race_field_aggregates rfa on ${raceKeyJoin("t", "rfa")}
  left join race_top3_speed rts on ${raceKeyJoin("t", "rts")}
  left join track_bias tb on ${entryKeyJoin("t", "tb")}
  left join weight_agg wa on ${entryKeyJoin("t", "wa")}
  left join recent_form rf on ${entryKeyJoin("t", "rf")}
  left join legacy_features lf on ${entryKeyJoin("t", "lf")}
  left join weather_lookup wl on ${entryKeyJoin("t", "wl")}
  left join horse_running_style_history rsh on ${entryKeyJoin("t", "rsh")}
),
base_feature_race_aggregates as (
  select
    source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
    avg(speed_index_avg_5) as speed_index_avg_5_race_avg,
    avg(jockey_recent_win_rate) as jockey_recent_win_rate_race_avg,
    avg(pedigree_score_for_race) as pedigree_score_for_race_avg
  from base_features
  group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
),
base_feature_ranks as (
  select
    candidate.source, candidate.kaisai_nen, candidate.kaisai_tsukihi,
    candidate.keibajo_code, candidate.race_bango, candidate.ketto_toroku_bango,
    count(case when
      (candidate.speed_index_avg_5 is null and peer.speed_index_avg_5 is not null)
      or (candidate.speed_index_avg_5 is not null and peer.speed_index_avg_5 < candidate.speed_index_avg_5)
      then 1 else null end) + 1 as speed_index_avg_5_rank_in_race,
    count(case when
      (candidate.speed_index_best_5 is null and peer.speed_index_best_5 is not null)
      or (candidate.speed_index_best_5 is not null and peer.speed_index_best_5 < candidate.speed_index_best_5)
      then 1 else null end) + 1 as speed_index_best_5_rank_in_race,
    count(case when
      (candidate.jockey_recent_win_rate is null and peer.jockey_recent_win_rate is not null)
      or (candidate.jockey_recent_win_rate is not null and peer.jockey_recent_win_rate > candidate.jockey_recent_win_rate)
      then 1 else null end) + 1 as jockey_recent_win_rate_rank_in_race,
    count(case when
      (candidate.trainer_career_win_rate is null and peer.trainer_career_win_rate is not null)
      or (candidate.trainer_career_win_rate is not null and peer.trainer_career_win_rate > candidate.trainer_career_win_rate)
      then 1 else null end) + 1 as trainer_career_win_rate_rank_in_race,
    count(case when
      (candidate.pedigree_score_for_race is null and peer.pedigree_score_for_race is not null)
      or (candidate.pedigree_score_for_race is not null and peer.pedigree_score_for_race > candidate.pedigree_score_for_race)
      then 1 else null end) + 1 as pedigree_score_for_race_rank_in_race,
    count(case when
      (candidate.same_distance_win_rate is null and peer.same_distance_win_rate is not null)
      or (candidate.same_distance_win_rate is not null and peer.same_distance_win_rate > candidate.same_distance_win_rate)
      then 1 else null end) + 1 as same_distance_win_rate_rank_in_race,
    count(case when
      (candidate.past_nige_rate_self_recent_5 is null and peer.past_nige_rate_self_recent_5 is not null)
      or (candidate.past_nige_rate_self_recent_5 is not null and peer.past_nige_rate_self_recent_5 > candidate.past_nige_rate_self_recent_5)
      then 1 else null end) + 1 as field_nige_pressure_rank
  from base_features candidate
  left join base_features peer on ${raceKeyJoin("candidate", "peer")}
  group by
    candidate.source, candidate.kaisai_nen, candidate.kaisai_tsukihi,
    candidate.keibajo_code, candidate.race_bango, candidate.ketto_toroku_bango,
    candidate.speed_index_avg_5, candidate.speed_index_best_5,
    candidate.jockey_recent_win_rate, candidate.trainer_career_win_rate,
    candidate.pedigree_score_for_race, candidate.same_distance_win_rate,
    candidate.past_nige_rate_self_recent_5
),
final_features as (
  select
    b.*,
    ranks.speed_index_avg_5_rank_in_race,
    ranks.speed_index_best_5_rank_in_race,
    ranks.jockey_recent_win_rate_rank_in_race,
    ranks.trainer_career_win_rate_rank_in_race,
    ranks.pedigree_score_for_race_rank_in_race,
    ranks.same_distance_win_rate_rank_in_race,
    ranks.field_nige_pressure_rank,
    b.speed_index_avg_5 - aggregates.speed_index_avg_5_race_avg as speed_index_avg_5_diff_from_race_avg,
    b.jockey_recent_win_rate - aggregates.jockey_recent_win_rate_race_avg as jockey_recent_win_rate_diff_from_race_avg,
    b.pedigree_score_for_race - aggregates.pedigree_score_for_race_avg as pedigree_score_diff_from_race_avg
  from base_features b
  left join base_feature_ranks ranks on ${entryKeyJoin("b", "ranks")}
  left join base_feature_race_aggregates aggregates on ${raceKeyJoin("b", "aggregates")}
)
select * from final_features order by umaban limit 18
`;
