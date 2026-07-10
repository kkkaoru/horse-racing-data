// Run with bun.
import { getWeatherFromKv, KV_WEATHER_TTL_SECONDS, putWeatherToKv } from "./weather-kv";
import {
  backfillVenueWeatherCatalogOnly,
  backfillVenueWeatherPayloads,
  backfillVenueWeatherRuntimeOnly,
  readWeatherByDate,
} from "./weather-r2-store";
import type { Env } from "./types";

const RACE_DATE_PARAM = "race_date";
const WEATHER_PATH = "/weather";
const PING_PATH = "/ping";
const BACKFILL_PATH = "/api/internal/backfill-r2-catalog";
const RACE_DATE_PATTERN = /^\d{8}$/;
const BAD_REQUEST_STATUS = 400;
const UNAUTHORIZED_STATUS = 401;
const SOURCE_KV = "kv";
const SOURCE_R2 = "r2";
const DEFAULT_BODY = "venue-weather";
const OK_BODY = "ok";
const INVALID_RACE_DATE_BODY = "invalid race_date";
const UNAUTHORIZED_BODY = "unauthorized";
const RESPONSE_CACHE_TTL_SECONDS = 60;
const INTERNAL_TOKEN_HEADER = "x-venue-weather-internal-token";

interface BackfillRequestBody {
  catalogOnly?: boolean;
  fetchedAt?: string;
  payloads?: unknown;
  runtimeOnly?: boolean;
}

const toIsoDate = (yyyymmdd: string): string =>
  `${yyyymmdd.slice(0, 4)}-${yyyymmdd.slice(4, 6)}-${yyyymmdd.slice(6, 8)}`;

const getDefaultCache = (): Cache | null => {
  if (typeof caches === "undefined") return null;
  return caches.default ?? null;
};

const buildResponseCacheRequest = (raceDate: string): Request =>
  new Request(`https://venue-weather.local/weather/${raceDate}`);

const handleWeatherRoute = async (env: Env, raceDate: string): Promise<Response> => {
  const cache = getDefaultCache();
  const cacheKey = buildResponseCacheRequest(raceDate);
  if (cache !== null) {
    const cachedResponse = await cache.match(cacheKey);
    if (cachedResponse !== undefined) return cachedResponse;
  }
  const cached = await getWeatherFromKv(env.WEATHER_KV, raceDate);
  if (cached !== null) return Response.json({ rows: cached, source: SOURCE_KV });
  const rows = await readWeatherByDate(env.WEATHER_ARCHIVE, raceDate);
  if (rows.length === 0) return Response.json({ rows: [], source: SOURCE_R2 });
  await putWeatherToKv({ kv: env.WEATHER_KV, raceDate, rows, ttlSeconds: KV_WEATHER_TTL_SECONDS });
  const response = Response.json({ rows, source: SOURCE_R2 });
  if (cache !== null) {
    await cache.put(
      cacheKey,
      new Response(response.clone().body, {
        headers: {
          "Cache-Control": `public, max-age=${RESPONSE_CACHE_TTL_SECONDS}`,
          "Content-Type": "application/json",
        },
      }),
    );
  }
  return response;
};

const isAuthorized = (request: Request, env: Env): boolean => {
  const token = env.VENUE_WEATHER_INTERNAL_TOKEN;
  return Boolean(token) && request.headers.get(INTERNAL_TOKEN_HEADER) === token;
};

const handleBackfillRoute = async (request: Request, env: Env): Promise<Response> => {
  if (!isAuthorized(request, env)) {
    return new Response(UNAUTHORIZED_BODY, { status: UNAUTHORIZED_STATUS });
  }
  const body = (await request.json()) as BackfillRequestBody;
  if (!Array.isArray(body.payloads)) {
    return new Response("invalid payloads", { status: BAD_REQUEST_STATUS });
  }
  const catalogOnly = body.catalogOnly === true;
  const runtimeOnly = body.runtimeOnly === true;
  if (catalogOnly && runtimeOnly) {
    return new Response("invalid backfill mode", { status: BAD_REQUEST_STATUS });
  }
  const fetchedAt = body.fetchedAt ?? new Date().toISOString();
  const payloads = body.payloads as Parameters<typeof backfillVenueWeatherPayloads>[2];
  const rows = catalogOnly
    ? await backfillVenueWeatherCatalogOnly(
        env.WEATHER_ARCHIVE,
        env.WEATHER_CATALOG_STREAM,
        payloads,
        fetchedAt,
      )
    : runtimeOnly
      ? await backfillVenueWeatherRuntimeOnly(env.WEATHER_ARCHIVE, payloads)
      : await backfillVenueWeatherPayloads(
          env.WEATHER_ARCHIVE,
          env.WEATHER_CATALOG_STREAM,
          payloads,
        );
  const raceDates = new Set(
    body.payloads
      .map((payload) =>
        typeof payload === "object" && payload !== null && "raceDate" in payload
          ? String(payload.raceDate)
          : null,
      )
      .filter((raceDate): raceDate is string => raceDate !== null),
  );
  await Promise.all([...raceDates].map((raceDate) => env.WEATHER_KV.delete(`weather:${raceDate}`)));
  return Response.json({ payloads: body.payloads.length, rows });
};

export const handleWeatherFetch = async (request: Request, env: Env): Promise<Response> => {
  const url = new URL(request.url);
  if (url.pathname === PING_PATH) return new Response(OK_BODY);
  if (request.method === "POST" && url.pathname === BACKFILL_PATH) {
    return handleBackfillRoute(request, env);
  }
  if (url.pathname !== WEATHER_PATH) return new Response(DEFAULT_BODY);
  const raceDate = url.searchParams.get(RACE_DATE_PARAM);
  if (raceDate === null || !RACE_DATE_PATTERN.test(raceDate)) {
    return new Response(INVALID_RACE_DATE_BODY, { status: BAD_REQUEST_STATUS });
  }
  return handleWeatherRoute(env, toIsoDate(raceDate));
};
