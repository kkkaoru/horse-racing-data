import type { RealtimeRacePayload } from "horse-racing-realtime/types";
import { NextResponse } from "next/server";

import {
  getRaceRunningStylesByRaceKeysWithCache,
  getRaceRunningStylesWithCache,
} from "../../../../../../../../../lib/running-style-cache.server";
import {
  getRaceDetail,
  getRaceRunners,
  getRacesByDateWithoutJockeyNames,
  getRaceSourceByRoute,
} from "../../../../../../../../../db/queries";
import {
  getRaceTrendD1StarterRows,
  getRaceTrendDailyStarterRows,
} from "../../../../../../../../../db/d1-trend-queries.server";
import type { RaceSource } from "../../../../../../../../../lib/codes";
import { fetchWithRetry } from "../../../../../../../../../lib/fetch-with-retry";
import {
  RACE_TREND_CACHE_REFRESH_PARAM,
  RACE_TREND_CACHE_WARM_PARAM,
  type RaceTrendCacheOptions,
} from "../../../../../../../../../lib/race-trend-cache";
import {
  buildRaceTrendCacheKeyForRequest,
  getCachedRaceTrendResponse,
  putRaceTrendCache,
} from "../../../../../../../../../lib/race-trend-cache.server";
import { notifyRaceTrendRoom } from "../../../../../../../../../lib/race-trend-room.server";
import { starterKey, starterRaceKey } from "../../../../../../../../../lib/race-trend-aggregate";
import type {
  RaceDetail,
  RaceListItem,
  RaceTrendRawPayload,
  RaceTrendRunningStyle,
  RaceTrendStarterRow,
  Runner,
} from "../../../../../../../../../lib/race-types";

interface RouteContext {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    year: string;
  }>;
}

const REALTIME_API_BASE_URL =
  process.env.NEXT_PUBLIC_REALTIME_DATA_API_BASE_URL ?? "https://sync-realtime-data.kkk4oru.com";

const isRaceSource = (value: string | null): value is RaceSource =>
  value === "jra" || value === "nar";

const normalizeText = (value: string | null | undefined): string | null => {
  const trimmed = value?.trim();
  return trimmed ? trimmed : null;
};

const normalizeNumberText = (value: string | null | undefined): string | null => {
  const normalized = normalizeText(value);
  if (!normalized) return null;
  return normalized.replace(/^0+(?=\d)/, "");
};

const formatRealtimeInteger = (value: number | null | undefined): string | null =>
  typeof value === "number" && Number.isFinite(value) && value > 0
    ? String(Math.trunc(value))
    : null;

const formatRealtimeWinOdds = (value: number | null | undefined): string | null =>
  typeof value === "number" && Number.isFinite(value) && value > 0
    ? String(Math.round(value * 10))
    : null;

const isRealtimeRacePayload = (value: unknown): value is RealtimeRacePayload =>
  typeof value === "object" && value !== null && "raceResults" in value;

const toYmd = (year: string, monthDay: string): string => `${year}${monthDay}`;

const parseDateInput = (value: string | null, fallbackYmd: string): string => {
  const compact = value?.replaceAll("-", "").trim();
  return compact && /^\d{8}$/.test(compact) ? compact : fallbackYmd;
};

const addDays = (ymd: string, days: number): string => {
  const date = new Date(
    Date.UTC(Number(ymd.slice(0, 4)), Number(ymd.slice(4, 6)) - 1, Number(ymd.slice(6, 8))),
  );
  date.setUTCDate(date.getUTCDate() + days);
  const year = String(date.getUTCFullYear()).padStart(4, "0");
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}${month}${day}`;
};

const enumerateDates = (startYmd: string, endYmd: string): string[] => {
  const dates: string[] = [];
  const collect = (ymd: string): void => {
    if (ymd > endYmd) return;
    dates.push(ymd);
    collect(addDays(ymd, 1));
  };
  collect(startYmd);
  return dates;
};

const isYmdInRange = (ymd: string, startYmd: string, endYmd: string): boolean =>
  ymd >= startYmd && ymd <= endYmd;

const mapLimit = async <T, U>(
  values: T[],
  limit: number,
  mapper: (value: T) => Promise<U>,
): Promise<U[]> => {
  const entries = values.map((value, index) => ({ index, value }));
  const results: U[] = [];
  const indexState = { value: 0 };
  const runNext = (): Promise<void> => {
    const currentIndex = indexState.value;
    indexState.value = currentIndex + 1;
    const entry = entries[currentIndex];
    if (!entry) return Promise.resolve();
    return mapper(entry.value).then((result) => {
      results[entry.index] = result;
      return runNext();
    });
  };
  await Promise.all(Array.from({ length: Math.min(limit, entries.length) }, runNext));
  return results;
};

const fetchRealtimePayload = async (race: RaceListItem): Promise<RealtimeRacePayload | null> => {
  const month = race.kaisaiTsukihi.slice(0, 2);
  const day = race.kaisaiTsukihi.slice(2, 4);
  const url = `${REALTIME_API_BASE_URL}/api/${race.source}/races/${race.kaisaiNen}/${month}/${day}/${race.keibajoCode}/${race.raceBango}/realtime`;
  try {
    const response = await fetchWithRetry(url, { cache: "no-store" }, { attempts: 1 });
    if (!response.ok) return null;
    const body: unknown = await response.json();
    return isRealtimeRacePayload(body) ? body : null;
  } catch {
    return null;
  }
};

const buildRealtimeStarterRows = async (race: RaceListItem): Promise<RaceTrendStarterRow[]> => {
  const payload = await fetchRealtimePayload(race);
  const resultHorses = payload?.raceResults?.horses ?? [];
  if (resultHorses.length === 0) return [];
  const runners = await getRaceRunners(
    race.source,
    race.kaisaiNen,
    race.kaisaiTsukihi.slice(0, 2),
    race.kaisaiTsukihi.slice(2, 4),
    race.keibajoCode,
    race.raceBango,
  );
  const runnerByHorseNumber = new Map(
    runners.map((runner) => [normalizeNumberText(runner.umaban), runner]),
  );
  const entryByHorseNumber = new Map(
    (payload?.raceEntries?.horses ?? []).map((entry) => [
      normalizeNumberText(entry.horseNumber),
      entry,
    ]),
  );
  const latestTanshoByHorseNumber = new Map(
    (payload?.odds?.latest.tansho ?? []).map((entry) => [
      normalizeNumberText(entry.combination),
      entry,
    ]),
  );
  const weightByHorseNumber = new Map(
    (payload?.horseWeights?.horses ?? []).map((entry) => [
      normalizeNumberText(entry.horseNumber),
      entry,
    ]),
  );

  return resultHorses.flatMap((resultHorse) => {
    const finishPosition = Number(resultHorse.finishPosition.replace(/[^\d]/g, ""));
    if (!Number.isFinite(finishPosition) || finishPosition <= 0) return [];
    const horseNumber = normalizeNumberText(resultHorse.horseNumber);
    const runner = runnerByHorseNumber.get(horseNumber);
    const entry = entryByHorseNumber.get(horseNumber);
    const latestTansho = latestTanshoByHorseNumber.get(horseNumber);
    const weight = weightByHorseNumber.get(horseNumber);
    return [
      {
        source: race.source,
        kaisaiNen: race.kaisaiNen,
        kaisaiTsukihi: race.kaisaiTsukihi,
        keibajoCode: race.keibajoCode,
        raceBango: race.raceBango,
        hassoJikoku: race.hassoJikoku,
        raceName:
          normalizeText(race.kyosomeiHondai) ?? normalizeText(race.kyosomeiFukudai) ?? "一般競走",
        runnerCount: String(resultHorses.length),
        wakuban: normalizeNumberText(runner?.wakuban),
        umaban: horseNumber,
        bamei:
          normalizeText(resultHorse.horseName) ??
          normalizeText(runner?.bamei) ??
          normalizeText(entry?.horseName),
        jockeyName: normalizeText(entry?.jockeyName) ?? normalizeText(runner?.kishumeiRyakusho),
        tanshoOdds: formatRealtimeWinOdds(latestTansho?.odds) ?? normalizeText(runner?.tanshoOdds),
        tanshoPopularity:
          formatRealtimeInteger(latestTansho?.rank) ?? normalizeText(runner?.tanshoNinkijun),
        finishPosition,
        sohaTime: normalizeText(resultHorse.time),
        corner1: null,
        corner2: null,
        corner3: null,
        corner4: null,
        bataiju: typeof weight?.weight === "number" ? String(weight.weight) : null,
        zogenFugo: weight?.changeSign ?? null,
        zogenSa: typeof weight?.changeAmount === "number" ? String(weight.changeAmount) : null,
      } satisfies RaceTrendStarterRow,
    ];
  });
};

type RaceTrendBuildOptions = RaceTrendCacheOptions;

const REALTIME_TREND_LOOKBACK_DAYS = 1;

const buildRealtimeRowsForTrend = async (
  race: RaceDetail,
  options: RaceTrendBuildOptions,
  historicalRows: RaceTrendStarterRow[],
): Promise<RaceTrendStarterRow[]> => {
  // The realtime fetcher only fills in data that D1 / Neon haven't absorbed
  // yet — typically today's still-running races. Anything older than a day
  // is reliably in daily_race_entries, so we clamp the realtime scan to the
  // target race date ± REALTIME_TREND_LOOKBACK_DAYS regardless of how wide
  // the trend window itself is. Without this clamp a 30-day trend window
  // would dispatch a realtime fetch per race per day (1500+ for NAR), which
  // pegged the dev server for ~40 minutes before D1 caching kicked in.
  const trendMinYmd =
    [options.jockeyStartYmd, options.frameStartYmd].toSorted()[0] ?? options.jockeyStartYmd;
  const trendMaxYmd =
    [options.jockeyEndYmd, options.frameEndYmd].toSorted().at(-1) ?? options.jockeyEndYmd;
  const targetYmd = toYmd(race.kaisaiNen, race.kaisaiTsukihi);
  const realtimeStartYmd = (() => {
    const clamped = addDays(targetYmd, -REALTIME_TREND_LOOKBACK_DAYS);
    return clamped < trendMinYmd ? trendMinYmd : clamped;
  })();
  const realtimeEndYmd = targetYmd < trendMaxYmd ? targetYmd : trendMaxYmd;
  if (realtimeEndYmd < realtimeStartYmd) return [];
  const dateRaces = (
    await Promise.all(
      enumerateDates(realtimeStartYmd, realtimeEndYmd).map((ymd) =>
        getRacesByDateWithoutJockeyNames(ymd.slice(0, 4), ymd.slice(4, 6), ymd.slice(6, 8)),
      ),
    )
  ).flat();
  const historicalRaceKeys = new Set(historicalRows.map(starterRaceKey));
  const candidateRaces = dateRaces.filter((candidate) => {
    if (candidate.source !== race.source) return false;
    if (historicalRaceKeys.has(starterRaceKey(candidate))) return false;
    const ymd = toYmd(candidate.kaisaiNen, candidate.kaisaiTsukihi);
    return isYmdInRange(ymd, realtimeStartYmd, realtimeEndYmd);
  });
  return (await mapLimit(candidateRaces, 6, buildRealtimeStarterRows)).flat();
};

const mergeStarterRows = (
  dailyRows: RaceTrendStarterRow[],
  snapshotRows: RaceTrendStarterRow[],
  realtimeRows: RaceTrendStarterRow[],
): RaceTrendStarterRow[] => {
  const merged = new Map<string, RaceTrendStarterRow>();
  for (const row of dailyRows) merged.set(starterKey(row), row);
  for (const row of snapshotRows) {
    const key = starterKey(row);
    const existing = merged.get(key);
    merged.set(key, existing ? mergeRowPair(existing, row) : row);
  }
  for (const row of realtimeRows) {
    const key = starterKey(row);
    const existing = merged.get(key);
    merged.set(key, existing ? mergeRowPair(existing, row) : row);
  }
  return Array.from(merged.values());
};

const pickNonEmpty = <T>(...values: Array<T | null | undefined>): T | null => {
  for (const value of values) {
    if (value !== null && value !== undefined && value !== "") return value;
  }
  return null;
};

const mergeRowPair = (a: RaceTrendStarterRow, b: RaceTrendStarterRow): RaceTrendStarterRow => ({
  ...a,
  raceName: pickNonEmpty(a.raceName, b.raceName),
  hassoJikoku: pickNonEmpty(a.hassoJikoku, b.hassoJikoku),
  runnerCount: pickNonEmpty(a.runnerCount, b.runnerCount),
  wakuban: pickNonEmpty(a.wakuban, b.wakuban),
  bamei: pickNonEmpty(a.bamei, b.bamei),
  jockeyName: pickNonEmpty(a.jockeyName, b.jockeyName),
  tanshoOdds: pickNonEmpty(a.tanshoOdds, b.tanshoOdds),
  tanshoPopularity: pickNonEmpty(a.tanshoPopularity, b.tanshoPopularity),
  finishPosition: a.finishPosition > 0 ? a.finishPosition : b.finishPosition,
  sohaTime: pickNonEmpty(a.sohaTime, b.sohaTime),
  corner1: pickNonEmpty(a.corner1, b.corner1),
  corner2: pickNonEmpty(a.corner2, b.corner2),
  corner3: pickNonEmpty(a.corner3, b.corner3),
  corner4: pickNonEmpty(a.corner4, b.corner4),
  bataiju: pickNonEmpty(a.bataiju, b.bataiju),
  zogenFugo: pickNonEmpty(a.zogenFugo, b.zogenFugo),
  zogenSa: pickNonEmpty(a.zogenSa, b.zogenSa),
});

const toCurrentRunningStyles = (
  rows: ReadonlyArray<{ horseNumber: number; predictedLabel: RaceTrendRunningStyle }>,
): RaceTrendRawPayload["currentRunningStyles"] =>
  rows.map((row) => ({
    horseNumber: String(row.horseNumber),
    predictedLabel: row.predictedLabel,
  }));

const toHistoricalRunningStyles = (
  rows: ReadonlyArray<{
    raceKey: string;
    horseNumber: number;
    predictedLabel: RaceTrendRunningStyle;
  }>,
): RaceTrendRawPayload["historicalRunningStyles"] =>
  rows.map((row) => ({
    raceKey: row.raceKey,
    horseNumber: String(row.horseNumber),
    predictedLabel: row.predictedLabel,
  }));

const buildRaceTrendRawPayload = async (
  race: RaceDetail,
  runners: Runner[],
  options: RaceTrendBuildOptions,
): Promise<RaceTrendRawPayload> => {
  const currentRunningStylesPromise = getRaceRunningStylesWithCache({
    kaisaiNen: race.kaisaiNen,
    kaisaiTsukihi: race.kaisaiTsukihi,
    keibajoCode: race.keibajoCode,
    raceBango: race.raceBango,
    source: race.source,
  }).catch(() => []);
  const historicalDailyPromise = getRaceTrendDailyStarterRows({
    source: options.source,
    startYmd: options.jockeyStartYmd,
    endYmd: options.jockeyEndYmd,
  });
  const historicalSnapshotPromise = getRaceTrendD1StarterRows({
    source: options.source,
    startYmd: options.jockeyStartYmd,
    endYmd: options.jockeyEndYmd,
  });
  const [dailyRows, snapshotRows] = await Promise.all([
    historicalDailyPromise,
    historicalSnapshotPromise,
  ]);
  const historicalRows = mergeStarterRows(dailyRows, snapshotRows, []);
  const realtimeRows = options.includeRealtimeResults
    ? await buildRealtimeRowsForTrend(race, options, historicalRows)
    : [];
  const starterRows = mergeStarterRows(dailyRows, snapshotRows, realtimeRows);
  const currentRunningStyles = await currentRunningStylesPromise;
  const historicalRunningStyleRows = await getRaceRunningStylesByRaceKeysWithCache(
    Array.from(new Set(starterRows.map(starterRaceKey))),
  ).catch(() => []);
  return {
    raceContext: {
      keibajoCode: race.keibajoCode,
      raceBango: race.raceBango,
      source: race.source,
    },
    runners: runners.map((runner) => ({
      frameNumber: runner.wakuban,
      horseNumber: runner.umaban,
      jockeyName: runner.kishumeiRyakusho,
    })),
    starterRows,
    currentRunningStyles: toCurrentRunningStyles(currentRunningStyles),
    historicalRunningStyles: toHistoricalRunningStyles(historicalRunningStyleRows),
  };
};

export const dynamic = "force-dynamic";

export async function GET(request: Request, context: RouteContext) {
  const { day, keibajoCode, month, raceNumber, year } = await context.params;
  const searchParams = new URL(request.url).searchParams;
  const sourceParam = searchParams.get("source");
  const source = isRaceSource(sourceParam)
    ? sourceParam
    : await getRaceSourceByRoute(year, month, day, keibajoCode, raceNumber);

  if (!source) {
    return NextResponse.json({ error: "race source not found" }, { status: 404 });
  }

  const race = await getRaceDetail(source, year, month, day, keibajoCode, raceNumber);
  if (!race) {
    return NextResponse.json({ error: "race not found" }, { status: 404 });
  }

  const targetYmd = `${year}${month}${day}`;
  const defaultStartYmd = addDays(targetYmd, source === "jra" ? -1 : -3);
  const options: RaceTrendBuildOptions = {
    source,
    jockeyStartYmd: parseDateInput(searchParams.get("jockeyStart"), defaultStartYmd),
    jockeyEndYmd: parseDateInput(searchParams.get("jockeyEnd"), targetYmd),
    frameStartYmd: parseDateInput(searchParams.get("frameStart"), defaultStartYmd),
    frameEndYmd: parseDateInput(searchParams.get("frameEnd"), targetYmd),
    includeRealtimeResults: searchParams.get("includeRealtimeResults") !== "false",
  };
  const cacheKey = buildRaceTrendCacheKeyForRequest({
    day,
    keibajoCode,
    month,
    options,
    raceNumber,
    year,
  });
  const isCacheWarmRequest = searchParams.get(RACE_TREND_CACHE_WARM_PARAM) === "1";
  const isCacheRefreshRequest = searchParams.get(RACE_TREND_CACHE_REFRESH_PARAM) === "1";
  if (!isCacheWarmRequest && !isCacheRefreshRequest) {
    const cachedResponse = await getCachedRaceTrendResponse(cacheKey);
    if (cachedResponse) {
      return cachedResponse;
    }
  }
  const runners = await getRaceRunners(source, year, month, day, keibajoCode, raceNumber);
  const payload = await buildRaceTrendRawPayload(race, runners, options);
  const body = JSON.stringify(payload);
  await putRaceTrendCache({ body, cacheKey, race });
  await notifyRaceTrendRoom(
    { day, keibajoCode, month, raceNumber, source, year },
    { cacheKey },
  ).catch(() => false);

  return new Response(body, {
    headers: {
      "Cache-Control": "public, max-age=60",
      "Content-Type": "application/json; charset=utf-8",
      "X-Race-Trend-Cache": isCacheWarmRequest
        ? "MISS-STORED-WARM"
        : isCacheRefreshRequest
          ? "MISS-STORED-REFRESH"
          : "MISS-STORED",
    },
  });
}
