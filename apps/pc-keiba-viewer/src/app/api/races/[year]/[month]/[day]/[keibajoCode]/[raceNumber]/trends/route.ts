import { NextResponse } from "next/server";

import {
  getRaceRunningStylesByRaceKeysWithCache,
  getRaceRunningStylesWithCache,
} from "../../../../../../../../../lib/running-style-cache.server";
import {
  getRaceDetail,
  getRaceRunners,
  getRaceSourceByRoute,
} from "../../../../../../../../../db/queries";
import {
  getRaceTrendD1StarterRows,
  getRaceTrendDailyStarterRows,
  getRaceTrendRunningStylesFromD1,
} from "../../../../../../../../../db/d1-trend-queries.server";
import type { RaceSource } from "../../../../../../../../../lib/codes";
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
import {
  fetchProductionApi,
  useProductionApiProxy,
} from "../../../../../../../../../lib/production-api-proxy.server";
import { notifyRaceTrendRoom } from "../../../../../../../../../lib/race-trend-room.server";
import { starterKey, starterRaceKey } from "../../../../../../../../../lib/race-trend-aggregate";
import type {
  RaceDetail,
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
  // Realtime scraping is no longer triggered from this route: the trend
  // payload is built solely from D1's daily_race_entries + race_*_snapshots,
  // which already get refreshed by the sync-realtime-data worker on race
  // finish (see viewer-trend-cache-bust). Calling sync-realtime-data's
  // per-race /realtime here used to fan out ~100 concurrent scrapes per
  // (source, ymd) and saturate the upstream worker so the detail-page
  // /realtime endpoint started timing out, dropping live odds binding.
  const starterRows = mergeStarterRows(dailyRows, snapshotRows, []);
  const currentRunningStyles = await currentRunningStylesPromise;
  const historicalRaceKeys = Array.from(new Set(starterRows.map(starterRaceKey)));
  const [cachedHistoricalRunningStyles, directHistoricalRunningStyles] = await Promise.all([
    getRaceRunningStylesByRaceKeysWithCache(historicalRaceKeys).catch(() => []),
    getRaceTrendRunningStylesFromD1(historicalRaceKeys),
  ]);
  const mergedHistoricalRunningStyles = mergeHistoricalRunningStyles(
    cachedHistoricalRunningStyles,
    directHistoricalRunningStyles,
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
