// Run with bun. Reads the shared finish-position day-base before rebuilding
// the same early-binding running-style inputs from Catalog/PostgreSQL.

import { parquetReadObjects } from "hyparquet";

import { buildRunningStyleRaceKey, type RunningStyleRaceParams } from "./running-style-features";
import type { RaceHorseFeatureRow } from "./running-style-r2";

const DAY_BASE_PREFIX = "feat-daybase/catalog-v1";
const DAY_BASE_FILE = "features.parquet";
const MAX_CACHED_DAY_BASES = 4;
const WATERMARK_METADATA_KEYS = [
  "max-data-sakusei-nengappi",
  "row-count",
  "rs-predicted-at-max",
  "rs-row-count",
] as const;

const PEER_INPUT_FEATURES = {
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

interface CachedDayBase {
  etag: string;
  rowsByRace: ReadonlyMap<string, ReadonlyArray<RaceHorseFeatureRow>>;
}

const dayBaseCache = new Map<string, CachedDayBase>();

const toNumberOrNull = (value: unknown): number | null => {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "boolean") return value ? 1 : 0;
  if (typeof value !== "string") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const toStringOrNull = (value: unknown): string | null => {
  if (typeof value !== "string" || value.length === 0) return null;
  return value;
};

const normalizedCode = (value: unknown): string => String(value).padStart(2, "0");

const hasFreshnessMetadata = (object: R2Object): boolean => {
  const metadata = object.customMetadata;
  if (metadata === undefined) return false;
  return WATERMARK_METADATA_KEYS.every((key) => {
    const value = metadata[key];
    return typeof value === "string" && value.length > 0;
  });
};

export const buildFinishPositionDayBaseKey = (params: RunningStyleRaceParams): string =>
  `${DAY_BASE_PREFIX}/${params.source}/${params.kaisaiNen}${params.kaisaiTsukihi}/${DAY_BASE_FILE}`;

const toFeatureRow = (
  raw: Record<string, unknown>,
  featureNames: ReadonlyArray<string>,
): RaceHorseFeatureRow | null => {
  const source = raw.source;
  if (source !== "jra" && source !== "nar") return null;
  const kaisaiNen = String(raw.kaisai_nen);
  const kaisaiTsukihi = String(raw.kaisai_tsukihi).padStart(4, "0");
  const keibajoCode = normalizedCode(raw.keibajo_code);
  const raceBango = normalizedCode(raw.race_bango);
  const kettoTorokuBango = String(raw.ketto_toroku_bango);
  const umaban = toNumberOrNull(raw.umaban);
  if (kaisaiNen.length !== 4 || kaisaiTsukihi.length !== 4 || kettoTorokuBango.length === 0)
    return null;
  if (umaban === null) return null;

  const perHorseFeatures: Record<string, number | null> = {};
  for (const name of featureNames) {
    if (!Object.hasOwn(raw, name)) return null;
    perHorseFeatures[name] = toNumberOrNull(raw[name]);
  }
  const peerInputs = {} as RaceHorseFeatureRow["peerInputs"];
  Object.entries(PEER_INPUT_FEATURES).forEach(([featureName, peerName]) => {
    peerInputs[peerName] = perHorseFeatures[featureName] ?? null;
  });
  return {
    bamei: toStringOrNull(raw.bamei),
    category: typeof raw.category === "string" ? raw.category : source,
    kaisaiNen,
    kaisaiTsukihi,
    keibajoCode,
    kettoTorokuBango,
    kyori: toNumberOrNull(raw.kyori),
    kyosoJokenCode: toStringOrNull(raw.kyoso_joken_code),
    gradeCode: toStringOrNull(raw.grade_code),
    narSubClass: toStringOrNull(raw.nar_subclass),
    peerInputs,
    perHorseFeatures,
    raceBango,
    raceKey: `${source}:${kaisaiNen}${kaisaiTsukihi}:${keibajoCode}:${raceBango}`,
    shussoTosu: toNumberOrNull(raw.shusso_tosu),
    source,
    trackCode: toStringOrNull(raw.track_code),
    umaban,
  };
};

const decodeDayBase = async (
  bytes: ArrayBuffer,
  featureNames: ReadonlyArray<string>,
): Promise<ReadonlyMap<string, ReadonlyArray<RaceHorseFeatureRow>>> => {
  const mutable = new Map<string, RaceHorseFeatureRow[]>();
  const decoded = await parquetReadObjects({ file: bytes });
  for (const raw of decoded) {
    const row = toFeatureRow(raw, featureNames);
    if (row === null) continue;
    const existing = mutable.get(row.raceKey);
    if (existing === undefined) mutable.set(row.raceKey, [row]);
    else existing.push(row);
  }
  return mutable;
};

const rememberDayBase = (key: string, value: CachedDayBase): void => {
  dayBaseCache.delete(key);
  dayBaseCache.set(key, value);
  if (dayBaseCache.size <= MAX_CACHED_DAY_BASES) return;
  const oldest = dayBaseCache.keys().next().value;
  if (typeof oldest === "string") dayBaseCache.delete(oldest);
};

export const loadRunningStyleFeaturesFromFinishPositionDayBase = async (params: {
  bucket: R2Bucket | undefined;
  featureNames: ReadonlyArray<string>;
  race: RunningStyleRaceParams;
}): Promise<ReadonlyArray<RaceHorseFeatureRow> | null> => {
  if (params.bucket === undefined) return null;
  const key = buildFinishPositionDayBaseKey(params.race);
  const cacheKey = `${key}\u0000${params.featureNames.join("\u0000")}`;
  const head = await params.bucket.head(key);
  if (head === null || !hasFreshnessMetadata(head)) return null;
  const cached = dayBaseCache.get(cacheKey);
  if (cached !== undefined && cached.etag === head.etag) {
    return cached.rowsByRace.get(buildRunningStyleRaceKey(params.race)) ?? null;
  }
  const object = await params.bucket.get(key);
  if (object === null || !hasFreshnessMetadata(object)) return null;
  const rowsByRace = await decodeDayBase(await object.arrayBuffer(), params.featureNames);
  rememberDayBase(cacheKey, { etag: object.etag, rowsByRace });
  return rowsByRace.get(buildRunningStyleRaceKey(params.race)) ?? null;
};

export const clearFinishPositionDayBaseCache = (): void => {
  dayBaseCache.clear();
};
