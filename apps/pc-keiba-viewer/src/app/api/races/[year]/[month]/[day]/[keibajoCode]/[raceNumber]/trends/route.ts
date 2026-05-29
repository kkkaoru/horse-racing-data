import { NextResponse } from "next/server";

import {
  buildPast14WindowForTarget,
  getRaceTrendPast14StarterRows,
  getRaceTrendRunningStylesFromD1,
  getRaceTrendTodayRunningStylesFromD1,
  getRaceTrendTodayStarterRows,
} from "../../../../../../../../../db/d1-trend-queries.server";
import {
  getRaceDetail,
  getRaceRunners,
  getRaceSourceByRoute,
} from "../../../../../../../../../db/queries";
import type { RaceSource } from "../../../../../../../../../lib/codes";
import {
  fetchProductionApi,
  useProductionApiProxy,
} from "../../../../../../../../../lib/production-api-proxy.server";
import { starterKey, starterRaceKey } from "../../../../../../../../../lib/race-trend-aggregate";
import {
  RACE_TREND_CACHE_REFRESH_PARAM,
  RACE_TREND_CACHE_WARM_PARAM,
  RACE_TREND_PAST14_LOOKBACK_DAYS,
  type RaceTrendCacheOptions,
} from "../../../../../../../../../lib/race-trend-cache";
import {
  buildRaceTrendCacheKeyForRequest,
  getCachedRaceTrendResponse,
  putRaceTrendCache,
} from "../../../../../../../../../lib/race-trend-cache.server";
import { notifyRaceTrendRoom } from "../../../../../../../../../lib/race-trend-room.server";
import type {
  RaceDetail,
  RaceTrendRawPayload,
  RaceTrendRunningStyle,
  RaceTrendStarterRow,
  Runner,
} from "../../../../../../../../../lib/race-types";
import { getRaceRunningStylesWithCache } from "../../../../../../../../../lib/running-style-cache.server";

interface RouteContext {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    year: string;
  }>;
}

const isRaceSource = (value: string | null): value is RaceSource =>
  value === "jra" || value === "nar";

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

type RaceTrendBuildOptions = RaceTrendCacheOptions;

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

const mergeHistoricalRunningStyles = (
  cached: ReadonlyArray<{
    raceKey: string;
    horseNumber: number;
    predictedLabel: RaceTrendRunningStyle;
  }>,
  direct: RaceTrendRawPayload["historicalRunningStyles"],
): RaceTrendRawPayload["historicalRunningStyles"] => {
  const merged = new Map<string, RaceTrendRawPayload["historicalRunningStyles"][number]>();
  for (const row of toHistoricalRunningStyles(cached)) {
    merged.set(`${row.raceKey}:${row.horseNumber}`, row);
  }
  for (const row of direct) {
    const key = `${row.raceKey}:${row.horseNumber}`;
    if (!merged.has(key)) merged.set(key, row);
  }
  return Array.from(merged.values());
};

// Reject degenerate trend payloads from the cache write path. The
// payload is only useful for client-side aggregation when both starter
// rows and the matching running-style history are populated — if
// either side came back empty it almost always means D1 was
// momentarily saturated, and pinning that for the cache TTL hides the
// recovered data the moment D1 catches up.
export const isCacheableTrendPayload = (payload: RaceTrendRawPayload): boolean =>
  payload.starterRows.length > 0 && payload.historicalRunningStyles.length > 0;

const compareRaceBango = (left: string, right: string): number => {
  const leftNumber = Number(left);
  const rightNumber = Number(right);
  if (Number.isFinite(leftNumber) && Number.isFinite(rightNumber)) {
    return leftNumber - rightNumber;
  }
  return left.localeCompare(right, "ja", { numeric: true });
};

// Today cache returns every completed starter row for the day across all
// venues so multiple races can share one upstream D1 round trip. The
// route narrows it back down to the current race's siblings — same
// source, same date, same venue, and a strictly smaller raceBango than
// the target — so the trend section reflects only races already over by
// the time the user lands on the page.
export const filterTodaySiblingRows = (
  rows: ReadonlyArray<RaceTrendStarterRow>,
  target: {
    keibajoCode: string;
    raceBango: string;
    source: RaceSource;
    targetYmd: string;
  },
): RaceTrendStarterRow[] =>
  rows.filter((row) => {
    if (row.source !== target.source) return false;
    if (`${row.kaisaiNen}${row.kaisaiTsukihi}` !== target.targetYmd) return false;
    if (row.keibajoCode !== target.keibajoCode) return false;
    return compareRaceBango(row.raceBango, target.raceBango) < 0;
  });

const buildRaceTrendRawPayload = async (
  race: RaceDetail,
  runners: Runner[],
  options: RaceTrendBuildOptions,
): Promise<RaceTrendRawPayload> => {
  const targetYmd = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
  const past14Window = buildPast14WindowForTarget(targetYmd);
  const currentRunningStylesPromise = getRaceRunningStylesWithCache({
    kaisaiNen: race.kaisaiNen,
    kaisaiTsukihi: race.kaisaiTsukihi,
    keibajoCode: race.keibajoCode,
    raceBango: race.raceBango,
    source: race.source,
  }).catch(() => []);
  const past14Promise = getRaceTrendPast14StarterRows({
    endYmd: past14Window.endYmd,
    keibajoCode: race.keibajoCode,
    raceBango: race.raceBango,
    source: options.source,
    startYmd: past14Window.startYmd,
  });
  const todayPromise = getRaceTrendTodayStarterRows({
    source: options.source,
    targetYmd,
  });
  // Realtime scraping is no longer triggered from this route: the trend
  // payload is built solely from D1's race_*_snapshots (today) and the
  // features-worker past-14 aggregate (R2 Parquet), which already get
  // refreshed by the sync-realtime-data worker on race finish (see
  // viewer-trend-cache-bust). Calling sync-realtime-data's per-race
  // /realtime here used to fan out ~100 concurrent scrapes per
  // (source, ymd) and saturate the upstream worker.
  const [past14Rows, todayRows] = await Promise.all([past14Promise, todayPromise]);
  const todaySiblingRows = filterTodaySiblingRows(todayRows, {
    keibajoCode: race.keibajoCode,
    raceBango: race.raceBango,
    source: race.source,
    targetYmd,
  });
  const starterRows = mergeStarterRows(past14Rows, todaySiblingRows, []);
  const currentRunningStyles = await currentRunningStylesPromise;
  const past14RaceKeys = Array.from(new Set(past14Rows.map(starterRaceKey)));
  const todayRaceKeys = Array.from(new Set(todaySiblingRows.map(starterRaceKey)));
  // Split running-style fetches: past-14 keys go through the KV-backed
  // helper (stable history, safe to cache cross-colo), while today's
  // sibling keys bypass KV because new inferences land throughout the
  // race day and a KV mirror would pin them out of date.
  const [past14RunningStyles, todayRunningStyles] = await Promise.all([
    getRaceTrendRunningStylesFromD1(past14RaceKeys),
    getRaceTrendTodayRunningStylesFromD1(todayRaceKeys),
  ]);
  const mergedHistoricalRunningStyles = mergeHistoricalRunningStyles(
    [],
    [...past14RunningStyles, ...todayRunningStyles],
  );
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
    historicalRunningStyles: mergedHistoricalRunningStyles,
  };
};

const proxyToProduction = async (
  path: string,
  searchParams: URLSearchParams,
): Promise<Response> => {
  const upstreamUrl = `${path}?${searchParams.toString()}`;
  const upstream = await fetchProductionApi(upstreamUrl);
  const body = await upstream.text();
  return new Response(body, {
    headers: {
      "Cache-Control": upstream.headers.get("Cache-Control") ?? "public, max-age=60",
      "Content-Type": upstream.headers.get("Content-Type") ?? "application/json; charset=utf-8",
      "X-Race-Trend-Cache": upstream.headers.get("X-Race-Trend-Cache") ?? "PROXIED-PRODUCTION",
    },
    status: upstream.status,
  });
};

export const dynamic = "force-dynamic";

export async function GET(request: Request, context: RouteContext) {
  const { day, keibajoCode, month, raceNumber, year } = await context.params;
  const searchParams = new URL(request.url).searchParams;
  // In dev (PC_KEIBA_ACCESS_CLIENT_ID / SECRET set, NODE_ENV=development) the
  // worker has no D1 binding so D1-backed reads silently return empty rows.
  // Proxy straight to the production trend endpoint so the dev UI shows the
  // same data the live site does, without needing a local D1 mirror.
  if (useProductionApiProxy()) {
    return proxyToProduction(
      `/api/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/trends`,
      searchParams,
    );
  }
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
  const defaultStartYmd = addDays(targetYmd, -RACE_TREND_PAST14_LOOKBACK_DAYS);
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
  const hasUsableData = isCacheableTrendPayload(payload);
  if (hasUsableData) {
    await putRaceTrendCache({ body, cacheKey, race });
    await notifyRaceTrendRoom(
      { day, keibajoCode, month, raceNumber, source, year },
      { cacheKey },
    ).catch(() => false);
  }

  return new Response(body, {
    headers: {
      "Cache-Control": hasUsableData ? "public, max-age=60" : "no-store",
      "Content-Type": "application/json; charset=utf-8",
      "X-Race-Trend-Cache": isCacheWarmRequest
        ? "MISS-STORED-WARM"
        : isCacheRefreshRequest
          ? "MISS-STORED-REFRESH"
          : hasUsableData
            ? "MISS-STORED"
            : "MISS-EMPTY-SKIPPED",
    },
  });
}
