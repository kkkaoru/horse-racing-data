import { buildRaceKey } from "../db/corner-running-style-parsers";

export const DEFAULT_RUNNING_STYLE_CACHE_ORIGIN = "https://pc-keiba-viewer.kkk4oru.com";
export const RUNNING_STYLE_CACHE_VERSION = "v3";
// 4-colon canonical race_key — matches `${source}:${YYYY}:${MMDD}:${keibajo}:${race_bango}`.
// keibajoCode and raceBango are allowed 1-2 digits so unpadded callsite values still
// parse, but `buildRaceKey` callers are expected to pass already-padded values per the
// running-style-cache contract.
export const RUNNING_STYLE_RACE_KEY_PATTERN = /^(jra|nar):(\d{4}):(\d{4}):(\d{1,2}):(\d{1,2})$/u;

export interface RunningStyleCacheRace {
  source: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

export { buildRaceKey };

const normalizeKeibajoCode = (value: string): string => value.padStart(2, "0");
const normalizeRaceBango = (value: string): string => value.padStart(2, "0");

export const parseRunningStyleRaceKey = (raceKey: string): RunningStyleCacheRace | null => {
  const match = raceKey.match(RUNNING_STYLE_RACE_KEY_PATTERN);
  if (!match) {
    return null;
  }
  return {
    kaisaiNen: match[2]!,
    kaisaiTsukihi: match[3]!,
    keibajoCode: match[4]!,
    raceBango: match[5]!,
    source: match[1]!,
  };
};

export const buildRunningStyleCacheRequest = (
  race: RunningStyleCacheRace,
  cacheOrigin = DEFAULT_RUNNING_STYLE_CACHE_ORIGIN,
): Request => {
  const url = new URL(
    `/api/races/${race.kaisaiNen}/${race.kaisaiTsukihi.slice(
      0,
      2,
    )}/${race.kaisaiTsukihi.slice(2, 4)}/${normalizeKeibajoCode(
      race.keibajoCode,
    )}/${normalizeRaceBango(race.raceBango)}/running-styles`,
    cacheOrigin,
  );
  url.searchParams.set("source", race.source);
  url.searchParams.set("__runningStyleCache", RUNNING_STYLE_CACHE_VERSION);
  return new Request(url.toString());
};

const getRaceDayExpiresAtMs = (race: Pick<RunningStyleCacheRace, "kaisaiNen" | "kaisaiTsukihi">) =>
  Date.parse(
    `${race.kaisaiNen}-${race.kaisaiTsukihi.slice(0, 2)}-${race.kaisaiTsukihi.slice(
      2,
      4,
    )}T23:59:59+09:00`,
  );

export const getRunningStyleCacheTtlSeconds = (
  race: Pick<RunningStyleCacheRace, "kaisaiNen" | "kaisaiTsukihi">,
  nowMs = Date.now(),
): number => {
  const expiresAt = getRaceDayExpiresAtMs(race);
  if (!Number.isFinite(expiresAt)) return 0;
  return Math.max(0, Math.floor((expiresAt - nowMs) / 1000));
};

export const parseRaceDayFromRunningStyleRaceKey = (
  raceKey: string,
): { kaisaiNen: string; kaisaiTsukihi: string } | undefined => {
  const race = parseRunningStyleRaceKey(raceKey);
  if (!race) {
    return undefined;
  }
  return {
    kaisaiNen: race.kaisaiNen,
    kaisaiTsukihi: race.kaisaiTsukihi,
  };
};

export const buildProductionRunningStylesPath = (race: RunningStyleCacheRace): string => {
  const month = String(Number(race.kaisaiTsukihi.slice(0, 2)));
  const day = String(Number(race.kaisaiTsukihi.slice(2, 4)));
  return `/api/races/${race.kaisaiNen}/${month}/${day}/${normalizeKeibajoCode(
    race.keibajoCode,
  )}/${normalizeRaceBango(race.raceBango)}/running-styles?source=${encodeURIComponent(race.source)}`;
};
