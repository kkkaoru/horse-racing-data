// Run with bun. Builds the per-race running-style feature JSONL payload
// inside the Worker from race_entry_corner_features via Hyperdrive.

import type { Pool } from "pg";

import type { RaceHorseFeatureRow } from "./running-style-r2";

export type RunningStyleSource = "jra" | "nar";

export interface RunningStyleRaceParams {
  source: RunningStyleSource;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

const RACE_BANGO_PAD_WIDTH = 2;
const KEIBAJO_CODE_PAD_WIDTH = 2;

const PEER_INPUT_COLUMNS = {
  career_win_rate: "careerWinRate",
  kohan_3f_avg_5: "kohan3fAvg5",
  past_corner_1_norm_avg_5: "pastCorner1NormAvg5",
  past_first_3f_avg_5: "pastFirst3fAvg5",
  past_nige_rate_self: "pastNigeRate",
  past_oikomi_rate_self: "pastOikomiRate",
  past_sashi_rate_self: "pastSashiRate",
  past_senkou_rate_self: "pastSenkouRate",
  speed_index_avg_5: "speedIndexAvg5",
  speed_index_best_5: "speedIndexBest5",
} as const;

const EXCLUDED_FROM_PER_HORSE = new Set([
  "bamei",
  "category",
  "feature_schema_version",
  "finish_norm",
  "finish_position",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "ketto_toroku_bango",
  "race_bango",
  "race_date",
  "race_id",
  "race_year",
  "source",
  "target_corner_1_norm",
  "target_corner_3_norm",
  "target_corner_4_norm",
  "target_running_style_class",
  "umaban",
]);

type FeatureRow = Record<string, unknown>;
type PeerInputKey = (typeof PEER_INPUT_COLUMNS)[keyof typeof PEER_INPUT_COLUMNS];

const FEATURE_ROW_QUERY = `
  select *
  from race_entry_corner_features
  where source = $1
    and kaisai_nen = $2
    and kaisai_tsukihi = $3
    and lpad(keibajo_code::text, 2, '0') = $4
    and lpad(race_bango::text, 2, '0') = $5
  order by umaban
`;

interface RawEntryRow {
  source: RunningStyleSource;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  ketto_toroku_bango: string;
  umaban: string | number | null;
  bamei: string | null;
  kishumei_ryakusho: string | null;
  kyori: string | number | null;
  shusso_tosu: string | number | null;
  tansho_ninkijun: string | number | null;
}

interface HorseAggregateRow {
  ketto_toroku_bango: string;
  career_win_rate: string | null;
  career_place_rate: string | null;
  past_corner_1_norm_avg_5: string | null;
  past_corner_1_norm_avg_3: string | null;
  past_corner_1_norm_std_5: string | null;
  past_corner_1_norm_best_5: string | null;
  corner_pass_avg_5: string | null;
  speed_index_avg_5: string | null;
  speed_index_best_5: string | null;
  kohan3f_avg_5: string | null;
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
  jockey_sashi_rate: string | null;
  jockey_oikomi_rate: string | null;
}

const HORSE_AGGREGATE_QUERY = `
  with past as (
    select
      ketto_toroku_bango,
      race_date,
      finish_position,
      corner1_norm,
      corner4_norm,
      time_sa,
      kohan_3f,
      case
        when corner1_norm = 0 then 0
        when corner1_norm <= 0.3 then 1
        when corner1_norm <= 0.7 then 2
        when corner1_norm is not null then 3
        else null
      end as style_class,
      row_number() over (
        partition by ketto_toroku_bango
        order by race_date desc
      ) as rn
    from race_entry_corner_features
    where source = $1
      and race_date < $2
      and ketto_toroku_bango = any($3::text[])
  )
  select
    ketto_toroku_bango,
    (avg(case when finish_position = 1 then 1.0 else 0.0 end)
      filter (where finish_position is not null))::text as career_win_rate,
    (avg(case when finish_position <= 3 then 1.0 else 0.0 end)
      filter (where finish_position is not null))::text as career_place_rate,
    (avg(corner1_norm) filter (where rn <= 5 and corner1_norm is not null))::text as past_corner_1_norm_avg_5,
    (avg(corner1_norm) filter (where rn <= 3 and corner1_norm is not null))::text as past_corner_1_norm_avg_3,
    (stddev_samp(corner1_norm) filter (where rn <= 5 and corner1_norm is not null))::text as past_corner_1_norm_std_5,
    (min(corner1_norm) filter (where rn <= 5 and corner1_norm is not null))::text as past_corner_1_norm_best_5,
    (avg(corner4_norm) filter (where rn <= 5 and corner4_norm is not null))::text as corner_pass_avg_5,
    (avg(time_sa) filter (where rn <= 5 and time_sa is not null))::text as speed_index_avg_5,
    (min(time_sa) filter (where rn <= 5 and time_sa is not null))::text as speed_index_best_5,
    (avg(kohan_3f) filter (where rn <= 5 and kohan_3f is not null))::text as kohan3f_avg_5,
    (avg(case when style_class = 0 then 1.0 else 0.0 end)
      filter (where style_class is not null))::text as past_nige_rate_self,
    (avg(case when style_class = 1 then 1.0 else 0.0 end)
      filter (where style_class is not null))::text as past_senkou_rate_self,
    (avg(case when style_class = 2 then 1.0 else 0.0 end)
      filter (where style_class is not null))::text as past_sashi_rate_self,
    (avg(case when style_class = 3 then 1.0 else 0.0 end)
      filter (where style_class is not null))::text as past_oikomi_rate_self
  from past
  group by ketto_toroku_bango
`;

const JOCKEY_AGGREGATE_QUERY = `
  with past as (
    select
      kishumei_ryakusho,
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
      and kishumei_ryakusho = any($3::text[])
  )
  select
    kishumei_ryakusho,
    (avg(case when finish_position = 1 then 1.0 else 0.0 end)
      filter (where finish_position is not null))::text as jockey_career_win_rate,
    (avg(case when style_class = 0 then 1.0 else 0.0 end)
      filter (where style_class is not null))::text as jockey_nige_rate,
    (avg(case when style_class = 1 then 1.0 else 0.0 end)
      filter (where style_class is not null))::text as jockey_senkou_rate,
    (avg(case when style_class = 2 then 1.0 else 0.0 end)
      filter (where style_class is not null))::text as jockey_sashi_rate,
    (avg(case when style_class = 3 then 1.0 else 0.0 end)
      filter (where style_class is not null))::text as jockey_oikomi_rate
  from past
  group by kishumei_ryakusho
`;

export const normalizeKeibajoCode = (value: string): string =>
  value.padStart(KEIBAJO_CODE_PAD_WIDTH, "0");

export const normalizeRaceBango = (value: string): string =>
  value.padStart(RACE_BANGO_PAD_WIDTH, "0");

export const buildRunningStyleRaceKey = (params: RunningStyleRaceParams): string =>
  `${params.source}:${params.kaisaiNen}${params.kaisaiTsukihi}:${normalizeKeibajoCode(params.keibajoCode)}:${normalizeRaceBango(params.raceBango)}`;

export const buildRunningStyleModelKey = (source: RunningStyleSource): string =>
  `running-style/models/${source}/latest.json`;

export const buildRunningStyleFeaturesKey = (params: RunningStyleRaceParams): string =>
  `running-style/features/${params.source}/${params.kaisaiNen}${params.kaisaiTsukihi}/${buildRunningStyleRaceKey(params)}.jsonl`;

const toStringOrNull = (value: unknown): string | null => {
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
  }
  if (value === null || value === undefined) return null;
  if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
    return String(value);
  }
  return null;
};

const requireString = (row: FeatureRow, key: string): string => {
  const value = toStringOrNull(row[key]);
  if (value === null) throw new Error(`race_entry_corner_features row missing ${key}`);
  return value;
};

const toNumberOrNull = (value: unknown): number | null => {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "boolean") return value ? 1 : 0;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};

const requireNumber = (row: FeatureRow, key: string): number => {
  const value = toNumberOrNull(row[key]);
  if (value === null) throw new Error(`race_entry_corner_features row missing ${key}`);
  return value;
};

const buildRawEntryQuery = (source: RunningStyleSource): string => {
  const raceTable = source === "nar" ? "nvd_ra" : "jvd_ra";
  const entryTable = source === "nar" ? "nvd_se" : "jvd_se";
  return `
    select
      '${source}' as source,
      ra.kaisai_nen,
      ra.kaisai_tsukihi,
      ra.keibajo_code,
      ra.race_bango,
      se.ketto_toroku_bango,
      se.umaban,
      se.bamei,
      se.kishumei_ryakusho,
      ra.kyori,
      ra.shusso_tosu,
      se.tansho_ninkijun
    from ${entryTable} se
    join ${raceTable} ra
      on ra.kaisai_nen = se.kaisai_nen
      and ra.kaisai_tsukihi = se.kaisai_tsukihi
      and ra.keibajo_code = se.keibajo_code
      and ra.race_bango = se.race_bango
    where se.kaisai_nen = $1
      and se.kaisai_tsukihi = $2
      and lpad(se.keibajo_code::text, 2, '0') = $3
      and lpad(se.race_bango::text, 2, '0') = $4
    order by nullif(se.umaban, '')::int
  `;
};

const normalizeRaceDate = (value: unknown, fallback: RunningStyleRaceParams): string => {
  if (value instanceof Date) {
    const month = String(value.getUTCMonth() + 1).padStart(2, "0");
    const day = String(value.getUTCDate()).padStart(2, "0");
    return `${value.getUTCFullYear()}${month}${day}`;
  }
  const text = toStringOrNull(value);
  if (text === null) return `${fallback.kaisaiNen}${fallback.kaisaiTsukihi}`;
  return text.replaceAll("-", "");
};

const isPerHorseFeatureColumn = (column: string): boolean => {
  if (EXCLUDED_FROM_PER_HORSE.has(column)) return false;
  if (column.startsWith("field_")) return false;
  if (column.startsWith("self_")) return false;
  return true;
};

const extractPerHorseFeatures = (row: FeatureRow): Record<string, number | null> => {
  const features: Record<string, number | null> = {};
  Object.keys(row).forEach((column) => {
    if (!isPerHorseFeatureColumn(column)) return;
    features[column] = toNumberOrNull(row[column]);
  });
  return features;
};

const extractPeerInputs = (row: FeatureRow): RaceHorseFeatureRow["peerInputs"] => {
  const peerInputs = {} as Record<PeerInputKey, number | null>;
  Object.entries(PEER_INPUT_COLUMNS).forEach(([sourceColumn, targetKey]) => {
    peerInputs[targetKey] = toNumberOrNull(row[sourceColumn]);
  });
  return peerInputs as RaceHorseFeatureRow["peerInputs"];
};

const toFeaturePayload = (
  row: FeatureRow,
  fallback: RunningStyleRaceParams,
): RaceHorseFeatureRow => {
  const source = requireString(row, "source") as RunningStyleSource;
  const kaisaiNen = requireString(row, "kaisai_nen");
  const kaisaiTsukihi = requireString(row, "kaisai_tsukihi");
  const keibajoCode = normalizeKeibajoCode(requireString(row, "keibajo_code"));
  const raceBango = normalizeRaceBango(requireString(row, "race_bango"));
  const raceDate = normalizeRaceDate(row.race_date, fallback);
  return {
    bamei: toStringOrNull(row.bamei),
    category: toStringOrNull(row.category) ?? source,
    kaisaiNen,
    kaisaiTsukihi,
    keibajoCode,
    kettoTorokuBango: requireString(row, "ketto_toroku_bango"),
    peerInputs: extractPeerInputs(row),
    perHorseFeatures: extractPerHorseFeatures(row),
    raceBango,
    raceKey: `${source}:${raceDate}:${keibajoCode}:${raceBango}`,
    source,
    umaban: requireNumber(row, "umaban"),
  };
};

const buildLookup = <T>(rows: ReadonlyArray<T>, getKey: (row: T) => string): Map<string, T> => {
  const lookup = new Map<string, T>();
  rows.forEach((row) => lookup.set(getKey(row), row));
  return lookup;
};

const buildPopularityScore = (
  ninkijun: number | null,
  shussoTosu: number | null,
): number | null => {
  if (ninkijun === null || shussoTosu === null || shussoTosu <= 1) return null;
  return Math.max(0, Math.min(1, (ninkijun - 1) / (shussoTosu - 1)));
};

const buildUmabanNorm = (umaban: number, shussoTosu: number | null): number | null => {
  if (shussoTosu === null || shussoTosu <= 1) return null;
  return (umaban - 1) / (shussoTosu - 1);
};

const buildFallbackFeatureRow = (
  entry: RawEntryRow,
  horseAgg: HorseAggregateRow | undefined,
  jockeyAgg: JockeyAggregateRow | undefined,
): RaceHorseFeatureRow => {
  const kaisaiNen = entry.kaisai_nen;
  const kaisaiTsukihi = entry.kaisai_tsukihi;
  const keibajoCode = normalizeKeibajoCode(entry.keibajo_code);
  const raceBango = normalizeRaceBango(entry.race_bango);
  const umaban = toNumberOrNull(entry.umaban) ?? 0;
  const shussoTosu = toNumberOrNull(entry.shusso_tosu);
  const featureMap: Record<string, number | null> = {
    career_place_rate: toNumberOrNull(horseAgg?.career_place_rate),
    career_win_rate: toNumberOrNull(horseAgg?.career_win_rate),
    corner_pass_avg_5: toNumberOrNull(horseAgg?.corner_pass_avg_5),
    jockey_career_win_rate: toNumberOrNull(jockeyAgg?.jockey_career_win_rate),
    jockey_nige_rate: toNumberOrNull(jockeyAgg?.jockey_nige_rate),
    jockey_oikomi_rate: toNumberOrNull(jockeyAgg?.jockey_oikomi_rate),
    jockey_sashi_rate: toNumberOrNull(jockeyAgg?.jockey_sashi_rate),
    jockey_senkou_rate: toNumberOrNull(jockeyAgg?.jockey_senkou_rate),
    kohan3f_avg_5: toNumberOrNull(horseAgg?.kohan3f_avg_5),
    kyori: toNumberOrNull(entry.kyori),
    past_corner_1_norm_avg_3: toNumberOrNull(horseAgg?.past_corner_1_norm_avg_3),
    past_corner_1_norm_avg_5: toNumberOrNull(horseAgg?.past_corner_1_norm_avg_5),
    past_corner_1_norm_best_5: toNumberOrNull(horseAgg?.past_corner_1_norm_best_5),
    past_corner_1_norm_std_5: toNumberOrNull(horseAgg?.past_corner_1_norm_std_5),
    past_first_3f_avg_5: null,
    past_nige_rate_self: toNumberOrNull(horseAgg?.past_nige_rate_self),
    past_oikomi_rate_self: toNumberOrNull(horseAgg?.past_oikomi_rate_self),
    past_sashi_rate_self: toNumberOrNull(horseAgg?.past_sashi_rate_self),
    past_senkou_rate_self: toNumberOrNull(horseAgg?.past_senkou_rate_self),
    popularity_score: buildPopularityScore(toNumberOrNull(entry.tansho_ninkijun), shussoTosu),
    shusso_tosu: shussoTosu,
    speed_index_avg_5: toNumberOrNull(horseAgg?.speed_index_avg_5),
    speed_index_best_5: toNumberOrNull(horseAgg?.speed_index_best_5),
    umaban,
    umaban_norm: buildUmabanNorm(umaban, shussoTosu),
  };
  const feature = (key: string): number | null => featureMap[key] ?? null;
  return {
    bamei: entry.bamei,
    category: entry.source,
    kaisaiNen,
    kaisaiTsukihi,
    keibajoCode,
    kettoTorokuBango: entry.ketto_toroku_bango,
    peerInputs: {
      careerWinRate: feature("career_win_rate"),
      kohan3fAvg5: feature("kohan3f_avg_5"),
      pastCorner1NormAvg5: feature("past_corner_1_norm_avg_5"),
      pastFirst3fAvg5: feature("past_first_3f_avg_5"),
      pastNigeRate: feature("past_nige_rate_self"),
      pastOikomiRate: feature("past_oikomi_rate_self"),
      pastSashiRate: feature("past_sashi_rate_self"),
      pastSenkouRate: feature("past_senkou_rate_self"),
      speedIndexAvg5: feature("speed_index_avg_5"),
      speedIndexBest5: feature("speed_index_best_5"),
    },
    perHorseFeatures: featureMap,
    raceBango,
    raceKey: `${entry.source}:${kaisaiNen}${kaisaiTsukihi}:${keibajoCode}:${raceBango}`,
    source: entry.source,
    umaban,
  };
};

const loadFallbackRunningStyleFeaturesForRace = async (
  pool: Pool,
  params: RunningStyleRaceParams,
): Promise<ReadonlyArray<RaceHorseFeatureRow>> => {
  const entries = await pool.query<RawEntryRow>(buildRawEntryQuery(params.source), [
    params.kaisaiNen,
    params.kaisaiTsukihi,
    normalizeKeibajoCode(params.keibajoCode),
    normalizeRaceBango(params.raceBango),
  ]);
  if (entries.rows.length === 0) return [];
  const horseIds = entries.rows.map((row) => row.ketto_toroku_bango);
  const jockeyNames = entries.rows
    .map((row) => row.kishumei_ryakusho)
    .filter((value): value is string => typeof value === "string" && value.length > 0);
  const raceDate = `${params.kaisaiNen}${params.kaisaiTsukihi}`;
  const [horseAggs, jockeyAggs] = await Promise.all([
    pool.query<HorseAggregateRow>(HORSE_AGGREGATE_QUERY, [params.source, raceDate, horseIds]),
    jockeyNames.length === 0
      ? Promise.resolve({ rows: [] as JockeyAggregateRow[] })
      : pool.query<JockeyAggregateRow>(JOCKEY_AGGREGATE_QUERY, [
          params.source,
          raceDate,
          jockeyNames,
        ]),
  ]);
  const horseLookup = buildLookup(horseAggs.rows, (row) => row.ketto_toroku_bango);
  const jockeyLookup = buildLookup(jockeyAggs.rows, (row) => row.kishumei_ryakusho);
  return entries.rows.map((entry) =>
    buildFallbackFeatureRow(
      entry,
      horseLookup.get(entry.ketto_toroku_bango),
      entry.kishumei_ryakusho === null ? undefined : jockeyLookup.get(entry.kishumei_ryakusho),
    ),
  );
};

export const loadRunningStyleFeaturesForRace = async (
  pool: Pool,
  params: RunningStyleRaceParams,
): Promise<ReadonlyArray<RaceHorseFeatureRow>> => {
  const result = await pool.query<FeatureRow>(FEATURE_ROW_QUERY, [
    params.source,
    params.kaisaiNen,
    params.kaisaiTsukihi,
    normalizeKeibajoCode(params.keibajoCode),
    normalizeRaceBango(params.raceBango),
  ]);
  if (result.rows.length > 0) {
    return result.rows.map((row) => toFeaturePayload(row, params));
  }
  return loadFallbackRunningStyleFeaturesForRace(pool, params);
};

export const serializeRunningStyleFeaturesJsonl = (
  rows: ReadonlyArray<RaceHorseFeatureRow>,
): string => `${rows.map((row) => JSON.stringify(row)).join("\n")}\n`;

export const writeRunningStyleFeaturesToR2 = async (
  bucket: R2Bucket,
  key: string,
  rows: ReadonlyArray<RaceHorseFeatureRow>,
): Promise<void> => {
  await bucket.put(key, serializeRunningStyleFeaturesJsonl(rows), {
    httpMetadata: { contentType: "application/x-ndjson; charset=utf-8" },
  });
};
