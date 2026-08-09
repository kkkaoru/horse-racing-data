// Run with bun. Pure key / window / TTL helpers for serving running-style and
// finish-position predictions from DETAIL_SECTION_CACHE_KV + Cache API.
//
// Window: yesterday / today / tomorrow in JST. Outside that range the viewer
// must fall back to Neon / D1 / production API and must not read or write
// these prediction keys.
//
// Freshness vs suppression:
// - Cache API (colo-local) is short, especially on race day, so a weight
//   rescore overwrite is visible at the edge within seconds.
// - KV (global) is long enough that a score written on race day still serves
//   during the following "yesterday" window after JST midnight. Stale today
//   scores are defeated by overwrite (or prediction-cache-bust delete), not
//   by waiting for KV expiry.
//
// Key shapes are human-readable so finish-position-cron / sync-realtime-data
// can overwrite the same entries after generation or weight rescore:
//   pred:fp:v1:{YYYYMMDD}:{keibajo}:{raceBango}
//   pred:rs:v1:{source}:{YYYYMMDD}:{keibajo}:{raceBango}

import {
  isRunningStyleLabel,
  requireNumber,
  requireString,
  stringOrNull,
  type RaceRunningStyleRow,
} from "../db/corner-running-style-parsers";
import type { RaceSource } from "./codes";
import type {
  FinishPositionConfidenceTier,
  FinishPositionModelPredictionFeature,
} from "./race-types";

export type PredictionCacheKind = "fp" | "rs";
export type PredictionCacheWindow = "yesterday" | "today" | "tomorrow" | "outside";

export interface PredictionCacheRaceId {
  keibajoCode: string;
  mmdd: string;
  raceBango: string;
  year: string;
}

export interface RunningStylePredictionCacheRaceId extends PredictionCacheRaceId {
  source: RaceSource;
}

export const PREDICTION_KV_VERSION = "v1";
export const PREDICTION_KV_KEY_PREFIX = "pred";
export const PREDICTION_KV_KIND_FINISH = "fp";
export const PREDICTION_KV_KIND_RUNNING_STYLE = "rs";
export const PREDICTION_KV_CACHE_URL_BASE = "https://pc-keiba-viewer.local/prediction-kv/";
export const PREDICTION_CACHE_BUST_INTERNAL_PATH = "/api/internal/prediction-cache-bust";

const YMD_PATTERN = /^\d{8}$/u;
const YEAR_WIDTH = 4;
const MONTH_START = 4;
const MONTH_END = 6;
const DAY_START = 6;
const DAY_END = 8;
const KEIBAJO_CODE_WIDTH = 2;
const RACE_BANGO_WIDTH = 2;
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const TODAY_DIFF_DAYS = 0;
const TOMORROW_DIFF_DAYS = 1;
const YESTERDAY_DIFF_DAYS = -1;

const TODAY_KV_TTL_SECONDS = 36 * 60 * 60;
const TOMORROW_KV_TTL_SECONDS = 24 * 60 * 60;
const YESTERDAY_KV_TTL_SECONDS = 24 * 60 * 60;
const TODAY_CACHE_API_TTL_SECONDS = 30;
const TOMORROW_CACHE_API_TTL_SECONDS = 5 * 60;
const YESTERDAY_CACHE_API_TTL_SECONDS = 10 * 60;

const FINISH_KEY_PATTERN = /^pred:fp:v1:(\d{8}):(\d{2}):(\d{2})$/u;
const RUNNING_STYLE_KEY_PATTERN = /^pred:rs:v1:(jra|nar):(\d{8}):(\d{2}):(\d{2})$/u;

const padRacePart = (value: string, width: number): string => value.padStart(width, "0");

const isRaceSource = (value: string): value is RaceSource => value === "jra" || value === "nar";

export const toJstYmd = (date: Date): string =>
  new Intl.DateTimeFormat("en-CA", {
    day: "2-digit",
    month: "2-digit",
    timeZone: "Asia/Tokyo",
    year: "numeric",
  })
    .format(date)
    .replaceAll("-", "");

export const buildRaceYmd = (year: string, month: string, day: string): string =>
  `${year}${padRacePart(month, 2)}${padRacePart(day, 2)}`;

const ymdToJstMidnightMs = (ymd: string): number => {
  const year = ymd.slice(0, YEAR_WIDTH);
  const month = ymd.slice(MONTH_START, MONTH_END);
  const day = ymd.slice(DAY_START, DAY_END);
  return Date.parse(`${year}-${month}-${day}T00:00:00+09:00`);
};

export const resolvePredictionCacheWindow = (
  raceYmd: string,
  nowMs = Date.now(),
): PredictionCacheWindow => {
  if (!YMD_PATTERN.test(raceYmd)) return "outside";
  const raceMs = ymdToJstMidnightMs(raceYmd);
  if (!Number.isFinite(raceMs)) return "outside";
  const todayMs = ymdToJstMidnightMs(toJstYmd(new Date(nowMs)));
  const diffDays = Math.round((raceMs - todayMs) / MS_PER_DAY);
  if (diffDays === TODAY_DIFF_DAYS) return "today";
  if (diffDays === TOMORROW_DIFF_DAYS) return "tomorrow";
  if (diffDays === YESTERDAY_DIFF_DAYS) return "yesterday";
  return "outside";
};

export const isPredictionCacheEligibleYmd = (raceYmd: string, nowMs = Date.now()): boolean =>
  resolvePredictionCacheWindow(raceYmd, nowMs) !== "outside";

export const getPredictionKvTtlSeconds = (window: PredictionCacheWindow): number => {
  if (window === "today") return TODAY_KV_TTL_SECONDS;
  if (window === "tomorrow") return TOMORROW_KV_TTL_SECONDS;
  if (window === "yesterday") return YESTERDAY_KV_TTL_SECONDS;
  return 0;
};

export const getPredictionCacheApiTtlSeconds = (window: PredictionCacheWindow): number => {
  if (window === "today") return TODAY_CACHE_API_TTL_SECONDS;
  if (window === "tomorrow") return TOMORROW_CACHE_API_TTL_SECONDS;
  if (window === "yesterday") return YESTERDAY_CACHE_API_TTL_SECONDS;
  return 0;
};

export const buildFinishPositionPredictionKvKey = (race: PredictionCacheRaceId): string =>
  [
    PREDICTION_KV_KEY_PREFIX,
    PREDICTION_KV_KIND_FINISH,
    PREDICTION_KV_VERSION,
    `${race.year}${padRacePart(race.mmdd, 4)}`,
    padRacePart(race.keibajoCode, KEIBAJO_CODE_WIDTH),
    padRacePart(race.raceBango, RACE_BANGO_WIDTH),
  ].join(":");

export const buildRunningStylePredictionKvKey = (race: RunningStylePredictionCacheRaceId): string =>
  [
    PREDICTION_KV_KEY_PREFIX,
    PREDICTION_KV_KIND_RUNNING_STYLE,
    PREDICTION_KV_VERSION,
    race.source,
    `${race.year}${padRacePart(race.mmdd, 4)}`,
    padRacePart(race.keibajoCode, KEIBAJO_CODE_WIDTH),
    padRacePart(race.raceBango, RACE_BANGO_WIDTH),
  ].join(":");

export const buildRunningStylePredictionCacheKeyFromRace = ({
  kaisaiNen,
  kaisaiTsukihi,
  keibajoCode,
  raceBango,
  source,
}: {
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  source: string;
}): string | null => {
  if (!isRaceSource(source)) return null;
  return buildRunningStylePredictionKvKey({
    keibajoCode,
    mmdd: kaisaiTsukihi,
    raceBango,
    source,
    year: kaisaiNen,
  });
};

export const parseFinishPositionPredictionKvKey = (
  cacheKey: string,
): PredictionCacheRaceId | null => {
  const match = FINISH_KEY_PATTERN.exec(cacheKey);
  if (!match) return null;
  const ymd = match[1];
  return {
    keibajoCode: match[2]!,
    mmdd: ymd!.slice(MONTH_START, DAY_END),
    raceBango: match[3]!,
    year: ymd!.slice(0, YEAR_WIDTH),
  };
};

export const parseRunningStylePredictionKvKey = (
  cacheKey: string,
): RunningStylePredictionCacheRaceId | null => {
  const match = RUNNING_STYLE_KEY_PATTERN.exec(cacheKey);
  if (!match) return null;
  const ymd = match[2];
  const sourceMatch = match[1];
  if (!sourceMatch || !isRaceSource(sourceMatch) || !ymd) return null;
  return {
    keibajoCode: match[3]!,
    mmdd: ymd.slice(MONTH_START, DAY_END),
    raceBango: match[4]!,
    source: sourceMatch,
    year: ymd.slice(0, YEAR_WIDTH),
  };
};

export const createPredictionKvCacheRequest = (cacheKey: string): Request =>
  new Request(`${PREDICTION_KV_CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`);

export const buildPredictionCacheBustKeys = (race: RunningStylePredictionCacheRaceId): string[] => [
  buildFinishPositionPredictionKvKey(race),
  buildRunningStylePredictionKvKey(race),
];

export const raceYmdFromPredictionRaceId = (race: PredictionCacheRaceId): string =>
  `${race.year}${padRacePart(race.mmdd, 4)}`;

// ---------------------------------------------------------------------------
// Payload parse/validate helpers. The canonical KV body for the `pred:fp` and
// `pred:rs` keys is a JSON array of the exact rows the viewer serves
// (FinishPositionModelPredictionFeature[] and RaceRunningStyleRow[]). Parsing
// is strict-but-non-throwing: any malformed value returns null so the caller
// falls back to the original DB / production-API source instead of serving a
// partial or corrupt prediction. `pred:fp` also tolerates an `{features:[]}`
// envelope so a producer that wraps the array still round-trips.
// ---------------------------------------------------------------------------

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const parseCachedRunningStyleRow = (raw: unknown): RaceRunningStyleRow => {
  if (!isRecord(raw)) {
    throw new Error("cached running-style row is not an object");
  }
  const predictedLabel = raw.predictedLabel;
  if (typeof predictedLabel !== "string" || !isRunningStyleLabel(predictedLabel)) {
    throw new Error("cached running-style row has an invalid predictedLabel");
  }
  return {
    bamei: stringOrNull(raw.bamei),
    category: requireString(raw.category, "category"),
    horseNumber: requireNumber(raw.horseNumber, "horseNumber"),
    kaisaiNen: requireString(raw.kaisaiNen, "kaisaiNen"),
    kettoTorokuBango: requireString(raw.kettoTorokuBango, "kettoTorokuBango"),
    modelVersion: requireString(raw.modelVersion, "modelVersion"),
    p_nige: requireNumber(raw.p_nige, "p_nige"),
    p_oikomi: requireNumber(raw.p_oikomi, "p_oikomi"),
    p_sashi: requireNumber(raw.p_sashi, "p_sashi"),
    p_senkou: requireNumber(raw.p_senkou, "p_senkou"),
    predictedAt: requireString(raw.predictedAt, "predictedAt"),
    predictedLabel,
    raceKey: requireString(raw.raceKey, "raceKey"),
  };
};

export const parseCachedRunningStyleRows = (payload: unknown): RaceRunningStyleRow[] | null => {
  if (!Array.isArray(payload)) {
    return null;
  }
  try {
    const rows = payload.map(parseCachedRunningStyleRow);
    return rows.length > 0 ? rows : null;
  } catch {
    return null;
  }
};

export const parsePredictionRunningStyleText = (text: string): RaceRunningStyleRow[] | null => {
  try {
    const parsed: unknown = JSON.parse(text);
    return parseCachedRunningStyleRows(parsed);
  } catch {
    return null;
  }
};

const toNullableNumber = (value: unknown): number | null =>
  typeof value === "number" ? value : null;

const toConfidenceTier = (value: unknown): FinishPositionConfidenceTier | null =>
  value === "low" || value === "mid" || value === "high" ? value : null;

const getFinishFeaturesArray = (parsed: unknown): unknown[] | null => {
  if (Array.isArray(parsed)) {
    return parsed;
  }
  if (!isRecord(parsed)) {
    return null;
  }
  const features = parsed.features;
  return Array.isArray(features) ? features : null;
};

export const parsePredictionFinishPositionFeatures = (
  text: string,
): FinishPositionModelPredictionFeature[] | null => {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return null;
  }
  const array = getFinishFeaturesArray(parsed);
  if (array === null) {
    return null;
  }
  const features: FinishPositionModelPredictionFeature[] = [];
  for (const raw of array) {
    if (!isRecord(raw)) {
      return null;
    }
    const horseNumber = raw.horseNumber;
    const modelVersion = raw.modelVersion;
    if (typeof horseNumber !== "string" || typeof modelVersion !== "string") {
      return null;
    }
    features.push({
      confidenceTier: toConfidenceTier(raw.confidenceTier),
      horseNumber,
      modelVersion,
      predictedFinishNorm: toNullableNumber(raw.predictedFinishNorm),
      predictedScoreStddev: toNullableNumber(raw.predictedScoreStddev),
      showProbability: toNullableNumber(raw.showProbability),
      winProbability: toNullableNumber(raw.winProbability),
    });
  }
  return features.length > 0 ? features : null;
};
