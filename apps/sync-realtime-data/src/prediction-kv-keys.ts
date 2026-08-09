// Run with bun. Viewer-compatible pred:rs key, window, and TTL helpers.
// Must stay byte-compatible with apps/pc-keiba-viewer/src/lib/prediction-kv-cache.ts.

export type PredictionCacheWindow = "yesterday" | "today" | "tomorrow" | "outside";
export type PredictionCacheSource = "jra" | "nar";

export interface RunningStylePredictionCacheRaceId {
  keibajoCode: string;
  mmdd: string;
  raceBango: string;
  source: PredictionCacheSource;
  year: string;
}

export const PREDICTION_KV_VERSION = "v1";
export const PREDICTION_KV_KEY_PREFIX = "pred";
export const PREDICTION_KV_KIND_RUNNING_STYLE = "rs";

const YMD_PATTERN = /^\d{8}$/u;
const YEAR_WIDTH = 4;
const MONTH_START = 4;
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

const padRacePart = (value: string, width: number): string => value.padStart(width, "0");

export const toJstYmd = (date: Date): string =>
  new Intl.DateTimeFormat("en-CA", {
    day: "2-digit",
    month: "2-digit",
    timeZone: "Asia/Tokyo",
    year: "numeric",
  })
    .format(date)
    .replaceAll("-", "");

const ymdToJstMidnightMs = (ymd: string): number => {
  const year = ymd.slice(0, YEAR_WIDTH);
  const month = ymd.slice(MONTH_START, MONTH_START + 2);
  const day = ymd.slice(MONTH_START + 2, DAY_END);
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

export const getPredictionKvTtlSeconds = (window: PredictionCacheWindow): number => {
  if (window === "today") return TODAY_KV_TTL_SECONDS;
  if (window === "tomorrow") return TOMORROW_KV_TTL_SECONDS;
  if (window === "yesterday") return YESTERDAY_KV_TTL_SECONDS;
  return 0;
};

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

export const raceYmdFromRunningStyle = (kaisaiNen: string, kaisaiTsukihi: string): string =>
  `${kaisaiNen}${padRacePart(kaisaiTsukihi, 4)}`;
