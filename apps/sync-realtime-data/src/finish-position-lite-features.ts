// Run with bun. Builds the 19-feature lite finish-position feature set for
// a single race by reading entries + past races out of race_entry_corner_features.
// Mirrors the column names the lite LightGBM model expects (parquet style):
// kyori, shusso_tosu, umaban, umaban_norm, popularity_score,
// career_win_rate, career_place_rate, past_corner_1_norm_avg_5,
// past_nige_rate_self/senkou/sashi/oikomi, jockey_career_win_rate,
// jockey_nige_rate, jockey_senkou_rate, rs_p_nige/senkou/sashi/oikomi.
// rs_p_* are left null here (Worker can fold in a follow-up pass).

import type { Pool } from "pg";

export interface LiteHorseFeatures {
  kettoTorokuBango: string;
  umaban: number;
  bamei: string | null;
  features: Record<string, number | null>;
}

export interface LoadFeaturesParams {
  source: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

interface EntryRow {
  ketto_toroku_bango: string;
  umaban: number;
  bamei: string | null;
  kishumei_ryakusho: string | null;
  race_date: string;
  kyori: number | null;
  shusso_tosu: number | null;
  tansho_ninkijun: number | null;
}

interface HorseAggregateRow {
  ketto_toroku_bango: string;
  career_win_rate: string | null;
  career_place_rate: string | null;
  past_corner_1_norm_avg_5: string | null;
  past_nige_rate_self: string | null;
  past_senkou_rate_self: string | null;
  past_sashi_rate_self: string | null;
  past_oikomi_rate_self: string | null;
}

interface JockeyAggregateRow {
  kishumei_ryakusho: string;
  jockey_career_win_rate: string | null;
  jockey_nige_rate: string | null;
  jockey_senkou_rate: string | null;
}

const ENTRY_QUERY = `
  select
    ketto_toroku_bango, umaban, bamei, kishumei_ryakusho, race_date,
    kyori, shusso_tosu, tansho_ninkijun
  from race_entry_corner_features
  where source = $1 and kaisai_nen = $2 and kaisai_tsukihi = $3
    and keibajo_code = $4 and race_bango = $5
  order by umaban
`;

const HORSE_AGGREGATE_QUERY = `
  with past as (
    select
      ketto_toroku_bango,
      race_date,
      finish_position,
      corner1_norm,
      case
        when corner1_norm = 0 then 0
        when corner1_norm <= 0.3 then 1
        when corner1_norm <= 0.7 then 2
        when corner1_norm is not null then 3
        else null
      end as style_class
    from race_entry_corner_features
    where source = $1
      and race_date < $2
      and ketto_toroku_bango = any($3::text[])
  ),
  last5 as (
    select ketto_toroku_bango, corner1_norm,
      row_number() over (partition by ketto_toroku_bango order by race_date desc) as rn
    from past
    where corner1_norm is not null
  ),
  last5_avg as (
    select ketto_toroku_bango, avg(corner1_norm) as past_corner_1_norm_avg_5
    from last5 where rn <= 5
    group by ketto_toroku_bango
  )
  select
    p.ketto_toroku_bango,
    (avg(case when p.finish_position = 1 then 1.0 else 0.0 end) filter (where p.finish_position is not null))::text as career_win_rate,
    (avg(case when p.finish_position <= 3 then 1.0 else 0.0 end) filter (where p.finish_position is not null))::text as career_place_rate,
    coalesce((select past_corner_1_norm_avg_5::text from last5_avg l where l.ketto_toroku_bango = p.ketto_toroku_bango), null) as past_corner_1_norm_avg_5,
    (avg(case when p.style_class = 0 then 1.0 else 0.0 end) filter (where p.style_class is not null))::text as past_nige_rate_self,
    (avg(case when p.style_class = 1 then 1.0 else 0.0 end) filter (where p.style_class is not null))::text as past_senkou_rate_self,
    (avg(case when p.style_class = 2 then 1.0 else 0.0 end) filter (where p.style_class is not null))::text as past_sashi_rate_self,
    (avg(case when p.style_class = 3 then 1.0 else 0.0 end) filter (where p.style_class is not null))::text as past_oikomi_rate_self
  from past p
  group by p.ketto_toroku_bango
`;

const JOCKEY_AGGREGATE_QUERY = `
  with past as (
    select
      kishumei_ryakusho,
      finish_position,
      case
        when corner1_norm = 0 then 0
        when corner1_norm <= 0.3 then 1
        when corner1_norm <= 0.7 then 2
        when corner1_norm is not null then 3
        else null
      end as style_class
    from race_entry_corner_features
    where source = $1
      and race_date < $2
      and kishumei_ryakusho = any($3::text[])
  )
  select
    kishumei_ryakusho,
    (avg(case when finish_position = 1 then 1.0 else 0.0 end) filter (where finish_position is not null))::text as jockey_career_win_rate,
    (avg(case when style_class = 0 then 1.0 else 0.0 end) filter (where style_class is not null))::text as jockey_nige_rate,
    (avg(case when style_class = 1 then 1.0 else 0.0 end) filter (where style_class is not null))::text as jockey_senkou_rate
  from past
  group by kishumei_ryakusho
`;

const numericOrNull = (value: string | null | undefined): number | null => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const buildHorseLookup = (
  rows: ReadonlyArray<HorseAggregateRow>,
): Map<string, HorseAggregateRow> => {
  const lookup = new Map<string, HorseAggregateRow>();
  rows.forEach((row) => lookup.set(row.ketto_toroku_bango, row));
  return lookup;
};

const buildJockeyLookup = (
  rows: ReadonlyArray<JockeyAggregateRow>,
): Map<string, JockeyAggregateRow> => {
  const lookup = new Map<string, JockeyAggregateRow>();
  rows.forEach((row) => lookup.set(row.kishumei_ryakusho, row));
  return lookup;
};

const buildPopularityScore = (
  ninkijun: number | null,
  shussoTosu: number | null,
): number | null => {
  if (ninkijun === null || shussoTosu === null || shussoTosu <= 1) return null;
  const score = (ninkijun - 1) / (shussoTosu - 1);
  return Math.max(0, Math.min(1, score));
};

const buildUmabanNorm = (umaban: number, shussoTosu: number | null): number | null => {
  if (shussoTosu === null || shussoTosu <= 1) return null;
  return (umaban - 1) / (shussoTosu - 1);
};

const mergeFeatures = (
  entry: EntryRow,
  horseAgg: HorseAggregateRow | undefined,
  jockeyAgg: JockeyAggregateRow | undefined,
): LiteHorseFeatures => ({
  bamei: entry.bamei,
  features: {
    career_place_rate: numericOrNull(horseAgg?.career_place_rate ?? null),
    career_win_rate: numericOrNull(horseAgg?.career_win_rate ?? null),
    jockey_career_win_rate: numericOrNull(jockeyAgg?.jockey_career_win_rate ?? null),
    jockey_nige_rate: numericOrNull(jockeyAgg?.jockey_nige_rate ?? null),
    jockey_senkou_rate: numericOrNull(jockeyAgg?.jockey_senkou_rate ?? null),
    kyori: entry.kyori,
    past_corner_1_norm_avg_5: numericOrNull(horseAgg?.past_corner_1_norm_avg_5 ?? null),
    past_nige_rate_self: numericOrNull(horseAgg?.past_nige_rate_self ?? null),
    past_oikomi_rate_self: numericOrNull(horseAgg?.past_oikomi_rate_self ?? null),
    past_sashi_rate_self: numericOrNull(horseAgg?.past_sashi_rate_self ?? null),
    past_senkou_rate_self: numericOrNull(horseAgg?.past_senkou_rate_self ?? null),
    popularity_score: buildPopularityScore(entry.tansho_ninkijun, entry.shusso_tosu),
    rs_p_nige: null,
    rs_p_oikomi: null,
    rs_p_sashi: null,
    rs_p_senkou: null,
    shusso_tosu: entry.shusso_tosu,
    umaban: entry.umaban,
    umaban_norm: buildUmabanNorm(entry.umaban, entry.shusso_tosu),
  },
  kettoTorokuBango: entry.ketto_toroku_bango,
  umaban: entry.umaban,
});

export const loadLiteFeaturesForRace = async (
  pool: Pool,
  params: LoadFeaturesParams,
): Promise<LiteHorseFeatures[]> => {
  const entries = await pool.query<EntryRow>(ENTRY_QUERY, [
    params.source,
    params.kaisaiNen,
    params.kaisaiTsukihi,
    params.keibajoCode,
    params.raceBango,
  ]);
  if (entries.rows.length === 0) return [];
  const raceDate = entries.rows[0]?.race_date ?? `${params.kaisaiNen}${params.kaisaiTsukihi}`;
  const horseIds = entries.rows.map((row) => row.ketto_toroku_bango);
  const jockeyNames = entries.rows
    .map((row) => row.kishumei_ryakusho)
    .filter((value): value is string => typeof value === "string" && value.length > 0);
  const [horseAggs, jockeyAggs] = await Promise.all([
    pool.query<HorseAggregateRow>(HORSE_AGGREGATE_QUERY, [params.source, raceDate, horseIds]),
    jockeyNames.length === 0
      ? Promise.resolve({ rows: [] as JockeyAggregateRow[] })
      : pool.query<JockeyAggregateRow>(JOCKEY_AGGREGATE_QUERY, [params.source, raceDate, jockeyNames]),
  ]);
  const horseLookup = buildHorseLookup(horseAggs.rows);
  const jockeyLookup = buildJockeyLookup(jockeyAggs.rows);
  return entries.rows.map((entry) =>
    mergeFeatures(
      entry,
      horseLookup.get(entry.ketto_toroku_bango),
      entry.kishumei_ryakusho === null ? undefined : jockeyLookup.get(entry.kishumei_ryakusho),
    ),
  );
};
