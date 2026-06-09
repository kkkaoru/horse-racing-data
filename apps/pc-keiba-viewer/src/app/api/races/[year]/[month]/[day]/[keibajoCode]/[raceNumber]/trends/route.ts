import { NextResponse } from "next/server";

import {
  getRaceDetail,
  getRaceRunners,
  getRaceSourceByRoute,
} from "../../../../../../../../../db/queries";
import { safeGetCloudflareExecutionContext } from "../../../../../../../../../lib/cloudflare-context.server";
import type { RaceSource } from "../../../../../../../../../lib/codes";
import {
  fetchProductionApi,
  useProductionApiProxy,
} from "../../../../../../../../../lib/production-api-proxy.server";
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
  buildRaceTrendRawPayloadForRace,
  isCacheableTrendPayload,
  pickTodaySiblingRowsAndSource,
  type RaceTrendSourceHeaderValue,
} from "../../../../../../../../../lib/race-trend-payload.server";
import { notifyRaceTrendRoomIfChanged } from "../../../../../../../../../lib/race-trend-room.server";

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

// Re-export the helpers extracted into race-trend-payload.server so the
// route-level test surface stays unchanged.
export { isCacheableTrendPayload, pickTodaySiblingRowsAndSource };
export type { RaceTrendSourceHeaderValue };

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
  const { payload, sourceHeader } = await buildRaceTrendRawPayloadForRace({
    options,
    race,
    runners,
  });
  const body = JSON.stringify(payload);
  const hasUsableData = isCacheableTrendPayload(payload);
  if (hasUsableData) {
    // Defer the cache write (Cache API + KV) so a KV 429 / write failure
    // cannot turn the trend response into a 500. The KV-backed putter is
    // the most frequent 429 victim under load; the client otherwise sees
    // `fetchWithRetry` exhaust its budget and the trend section stays
    // skeleton, mirroring the recent-results route fix.
    const persistCachePut = putRaceTrendCache({ body, cacheKey, race }).catch(() => undefined);
    const cloudflareCtx = await safeGetCloudflareExecutionContext();
    if (cloudflareCtx) {
      cloudflareCtx.waitUntil(persistCachePut);
    }
    await notifyRaceTrendRoomIfChanged({
      body,
      event: { cacheKey },
      params: { day, keibajoCode, month, raceNumber, source, year },
    }).catch(() => false);
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
