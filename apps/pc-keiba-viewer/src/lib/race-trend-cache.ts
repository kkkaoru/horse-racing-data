import type { RaceSource } from "./codes";

// v7 bumped 2026-05-29 for the Phase E features-worker cutover: the
// underlying d1-trend cache prefixes moved to v5 once
// getRaceTrendD1StarterRows stopped reading legacy daily_race_entries,
// so the outer trend payload also has to invalidate pre-cutover entries.
export const RACE_TREND_CACHE_VERSION = "v7";

export const RACE_TREND_CACHE_WARM_PARAM = "__trendCacheWarm";

export const RACE_TREND_CACHE_REFRESH_PARAM = "__trendCacheRefresh";

export const RACE_TREND_CACHE_PRE_START_SECONDS = 20 * 60;

export const RACE_TREND_CACHE_AFTER_START_SECONDS = 6 * 60 * 60;

export const RACE_TREND_CACHE_WARM_VARIANT_COUNT = 1;

export interface RaceTrendCacheOptions {
  frameEndYmd: string;
  frameStartYmd: string;
  includeRealtimeResults: boolean;
  jockeyEndYmd: string;
  jockeyStartYmd: string;
  source: RaceSource;
}

export interface RaceTrendCacheWarmMessage {
  day: string;
  kind: "race-trend";
  keibajoCode: string;
  month: string;
  options: RaceTrendCacheOptions;
  raceNumber: string;
  source: RaceSource;
  year: string;
}

const booleanKey = (value: boolean): string => (value ? "1" : "0");

export const addDaysToYmd = (ymd: string, days: number): string => {
  const date = new Date(
    Date.UTC(Number(ymd.slice(0, 4)), Number(ymd.slice(4, 6)) - 1, Number(ymd.slice(6, 8))),
  );
  date.setUTCDate(date.getUTCDate() + days);
  return [
    String(date.getUTCFullYear()).padStart(4, "0"),
    String(date.getUTCMonth() + 1).padStart(2, "0"),
    String(date.getUTCDate()).padStart(2, "0"),
  ].join("");
};

export const buildDefaultRaceTrendCacheOptions = (
  source: RaceSource,
  targetYmd: string,
): RaceTrendCacheOptions => {
  const defaultStartYmd = addDaysToYmd(targetYmd, source === "jra" ? -1 : -3);
  return {
    frameEndYmd: targetYmd,
    frameStartYmd: defaultStartYmd,
    includeRealtimeResults: true,
    jockeyEndYmd: targetYmd,
    jockeyStartYmd: defaultStartYmd,
    source,
  };
};

export const buildRaceTrendCacheWarmOptions = (
  source: RaceSource,
  targetYmd: string,
): RaceTrendCacheOptions[] => [buildDefaultRaceTrendCacheOptions(source, targetYmd)];

export interface RaceTrendCacheKeyInput {
  keibajoCode: string;
  options: RaceTrendCacheOptions;
  raceBango: string;
}

export const buildRaceTrendCacheKey = ({
  keibajoCode,
  options,
  raceBango,
}: RaceTrendCacheKeyInput): string =>
  [
    "race-trend",
    RACE_TREND_CACHE_VERSION,
    options.source,
    keibajoCode,
    raceBango,
    options.jockeyStartYmd,
    options.jockeyEndYmd,
    options.frameStartYmd,
    options.frameEndYmd,
    booleanKey(options.includeRealtimeResults),
  ].join(":");

export const buildRaceTrendApiPath = ({
  day,
  keibajoCode,
  month,
  options,
  raceNumber,
  year,
}: RaceTrendCacheWarmMessage): string => {
  const params = new URLSearchParams({
    source: options.source,
    jockeyStart: options.jockeyStartYmd,
    jockeyEnd: options.jockeyEndYmd,
    frameStart: options.frameStartYmd,
    frameEnd: options.frameEndYmd,
    includeRealtimeResults: String(options.includeRealtimeResults),
    [RACE_TREND_CACHE_WARM_PARAM]: "1",
  });
  return `/api/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/trends?${params.toString()}`;
};

const parseStartTimeParts = (hassoJikoku: string | null | undefined): string | null => {
  const normalizedTime = hassoJikoku?.trim().padStart(4, "0");
  return normalizedTime && /^\d{4}$/u.test(normalizedTime) ? normalizedTime : null;
};

export const getRaceStartTimeMs = (race: {
  hassoJikoku?: string | null;
  kaisaiNen: string;
  kaisaiTsukihi: string;
}): number | null => {
  const normalizedTime = parseStartTimeParts(race.hassoJikoku);
  if (!normalizedTime) {
    return null;
  }
  const startTime = Date.parse(
    `${race.kaisaiNen}-${race.kaisaiTsukihi.slice(0, 2)}-${race.kaisaiTsukihi.slice(
      2,
      4,
    )}T${normalizedTime.slice(0, 2)}:${normalizedTime.slice(2, 4)}:00+09:00`,
  );
  return Number.isFinite(startTime) ? startTime : null;
};

export const getRaceTrendCacheTtlSeconds = (
  race: {
    hassoJikoku?: string | null;
    kaisaiNen: string;
    kaisaiTsukihi: string;
  },
  afterStartSeconds = RACE_TREND_CACHE_AFTER_START_SECONDS,
  nowMs = Date.now(),
): number => {
  const raceStartTime = getRaceStartTimeMs(race);
  if (raceStartTime === null) {
    return 0;
  }
  const expiresAt = raceStartTime + afterStartSeconds * 1000;
  return Math.max(0, Math.floor((expiresAt - nowMs) / 1000));
};

const compareRaceNumber = (left: string, right: string): number => {
  const leftNumber = Number(left);
  const rightNumber = Number(right);
  if (Number.isFinite(leftNumber) && Number.isFinite(rightNumber)) {
    return leftNumber - rightNumber;
  }
  return left.localeCompare(right, "ja", { numeric: true });
};

export const isRaceBeforeTargetRace = (
  candidate: {
    hassoJikoku?: string | null;
    kaisaiNen: string;
    kaisaiTsukihi: string;
    keibajoCode: string;
    raceBango: string;
    source: RaceSource;
  },
  target: {
    hassoJikoku?: string | null;
    kaisaiNen: string;
    kaisaiTsukihi: string;
    keibajoCode: string;
    raceBango: string;
    source: RaceSource;
  },
): boolean => {
  if (candidate.source !== target.source) {
    return false;
  }
  const candidateYmd = `${candidate.kaisaiNen}${candidate.kaisaiTsukihi}`;
  const targetYmd = `${target.kaisaiNen}${target.kaisaiTsukihi}`;
  if (candidateYmd !== targetYmd) {
    return candidateYmd < targetYmd;
  }

  const candidateStart = getRaceStartTimeMs(candidate);
  const targetStart = getRaceStartTimeMs(target);
  if (candidateStart !== null && targetStart !== null && candidateStart !== targetStart) {
    return candidateStart < targetStart;
  }

  if (candidate.keibajoCode !== target.keibajoCode) {
    return false;
  }
  return compareRaceNumber(candidate.raceBango, target.raceBango) < 0;
};
