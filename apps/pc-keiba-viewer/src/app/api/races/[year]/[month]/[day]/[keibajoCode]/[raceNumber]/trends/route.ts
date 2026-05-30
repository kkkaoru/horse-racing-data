import type { RaceTrendStarterRow } from "horse-racing-realtime/race-trend-daily-track-types";
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
import { safeGetCloudflareEnv } from "../../../../../../../../../lib/cloudflare-context.server";
import type { RaceSource } from "../../../../../../../../../lib/codes";
import {
  fetchProductionApi,
  useProductionApiProxy,
} from "../../../../../../../../../lib/production-api-proxy.server";
import {
  filterTodaySiblingRows,
  mergeStarterRows,
  starterRaceKey,
} from "../../../../../../../../../lib/race-trend-aggregate";
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
import {
  fetchRaceTrendDailyTrack,
  type RaceTrendDailyTrackFetchResult,
} from "../../../../../../../../../lib/race-trend-daily-track-client.server";
import { notifyRaceTrendRoom } from "../../../../../../../../../lib/race-trend-room.server";
import type {
  RaceDetail,
  RaceTrendRawPayload,
  RaceTrendRunningStyle,
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

export type RaceTrendSourceHeaderValue = "do-hit" | "do-miss-fallback" | "do-error-fallback";

interface DoFallbackArgs {
  fallbackRows: RaceTrendStarterRow[];
  result: RaceTrendDailyTrackFetchResult;
}

const pickSiblingRowsFromDoResult = (
  result: RaceTrendDailyTrackFetchResult,
): RaceTrendStarterRow[] => result.rows.flatMap((row) => row.starterRows);

const DO_ERROR_HEADER: RaceTrendSourceHeaderValue = "do-error-fallback";
const DO_MISS_HEADER: RaceTrendSourceHeaderValue = "do-miss-fallback";

export const pickTodaySiblingRowsAndSource = ({
  fallbackRows,
  result,
}: DoFallbackArgs): {
  rows: RaceTrendStarterRow[];
  sourceHeader: RaceTrendSourceHeaderValue;
} => {
  if (result.status === "hit") {
    return { rows: pickSiblingRowsFromDoResult(result), sourceHeader: "do-hit" };
  }
  return {
    rows: fallbackRows,
    sourceHeader: result.status === "miss" ? DO_MISS_HEADER : DO_ERROR_HEADER,
  };
};

const DO_ERROR_RESULT: RaceTrendDailyTrackFetchResult = { rows: [], status: "error" };

const safePast14Promise = (
  promise: Promise<RaceTrendStarterRow[]>,
): Promise<RaceTrendStarterRow[]> => promise.catch(() => []);

const safeDoResultPromise = (
  promise: Promise<RaceTrendDailyTrackFetchResult>,
): Promise<RaceTrendDailyTrackFetchResult> => promise.catch(() => DO_ERROR_RESULT);

const safeLegacyTodayPromise = (
  promise: Promise<RaceTrendStarterRow[]>,
): Promise<RaceTrendStarterRow[]> => promise.catch(() => []);

const toCurrentRunningStyles = (
  rows: ReadonlyArray<{ horseNumber: number; predictedLabel: RaceTrendRunningStyle }>,
): RaceTrendRawPayload["currentRunningStyles"] =>
  rows.map((row) => ({
    horseNumber: String(row.horseNumber),
    predictedLabel: row.predictedLabel,
  }));

const dedupeHistoricalRunningStyles = (
  direct: RaceTrendRawPayload["historicalRunningStyles"],
): RaceTrendRawPayload["historicalRunningStyles"] => {
  const merged = new Map<string, RaceTrendRawPayload["historicalRunningStyles"][number]>();
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

interface BuildRaceTrendRawPayloadResult {
  payload: RaceTrendRawPayload;
  sourceHeader: RaceTrendSourceHeaderValue;
}

const buildRaceTrendRawPayload = async (
  race: RaceDetail,
  runners: Runner[],
  options: RaceTrendBuildOptions,
): Promise<BuildRaceTrendRawPayloadResult> => {
  const targetYmd = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
  const past14Window = buildPast14WindowForTarget(targetYmd);
  const currentRunningStylesPromise = getRaceRunningStylesWithCache({
    kaisaiNen: race.kaisaiNen,
    kaisaiTsukihi: race.kaisaiTsukihi,
    keibajoCode: race.keibajoCode,
    raceBango: race.raceBango,
    source: race.source,
  }).catch(() => []);
  // Each upstream is wrapped in `.catch(() => fallback)` so a single
  // rejected branch cannot black out the whole trend payload. This
  // matches the existing `currentRunningStylesPromise.catch(() => [])`
  // pattern above.
  const past14Promise = safePast14Promise(
    getRaceTrendPast14StarterRows({
      endYmd: past14Window.endYmd,
      keibajoCode: race.keibajoCode,
      raceBango: race.raceBango,
      source: options.source,
      startYmd: past14Window.startYmd,
    }),
  );
  // DO-primary path: sync-realtime-data's RaceTrendDailyTrackDO maintains
  // a per-(source, ymd, keibajoCode) daily aggregate refreshed by the
  // 5 min poller. When the DO has the answer ready we skip the legacy
  // D1-backed today-cache entirely. The legacy helper stays around as a
  // fallback for DO miss / error so a single missing DO entry can't
  // black out the trend section.
  //
  // We await DO before deciding whether to fire the legacy fetch: hitting
  // both in parallel wasted a D1 round-trip every time DO won (= the hot
  // path now that the DO is populated by the 5 min poller). On DO miss /
  // error we still fall back to legacy, paying one extra D1 read of
  // sequential latency only in that minority case.
  const env = await safeGetCloudflareEnv();
  const doResultPromise = safeDoResultPromise(
    fetchRaceTrendDailyTrack(env, {
      beforeRaceBango: race.raceBango,
      keibajoCode: race.keibajoCode,
      source: race.source,
      targetYmd,
    }),
  );
  const doResult = await doResultPromise;
  const legacyTodayRows =
    doResult.status === "hit"
      ? []
      : await safeLegacyTodayPromise(
          getRaceTrendTodayStarterRows({
            keibajoCode: race.keibajoCode,
            source: options.source,
            targetYmd,
          }),
        );
  const past14Rows = await past14Promise;
  const legacyTodaySiblingRows = filterTodaySiblingRows(legacyTodayRows, {
    keibajoCode: race.keibajoCode,
    raceBango: race.raceBango,
    source: race.source,
    targetYmd,
  });
  const { rows: rawTodaySiblingRows, sourceHeader } = pickTodaySiblingRowsAndSource({
    fallbackRows: legacyTodaySiblingRows,
    result: doResult,
  });
  // Defense-in-depth: re-apply the sibling filter so DO state with
  // stale-day or other-venue rows (the DO is partitioned per
  // (source, ymd, keibajoCode) but the flattened payload still carries
  // raw `RaceTrendStarterRow` records we should re-narrow before merge).
  const todaySiblingRows = filterTodaySiblingRows(rawTodaySiblingRows, {
    keibajoCode: race.keibajoCode,
    raceBango: race.raceBango,
    source: race.source,
    targetYmd,
  });
  const starterRows = mergeStarterRows(past14Rows, todaySiblingRows);
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
  const mergedHistoricalRunningStyles = dedupeHistoricalRunningStyles([
    ...past14RunningStyles,
    ...todayRunningStyles,
  ]);
  return {
    payload: {
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
    },
    sourceHeader,
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
  const { payload, sourceHeader } = await buildRaceTrendRawPayload(race, runners, options);
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
      "X-Race-Trend-Source": sourceHeader,
    },
  });
}
