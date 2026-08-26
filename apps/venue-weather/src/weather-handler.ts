// Run with bun.
import { getWeatherFromKv, KV_WEATHER_TTL_SECONDS, putWeatherToKv } from "./weather-kv";
import {
  backfillVenueWeatherCatalogOnly,
  backfillVenueWeatherPayloads,
  backfillVenueWeatherRuntimeOnly,
  readWeatherByDate,
} from "./weather-r2-store";
import { readWeatherV2ByDate, type WeatherV2Row } from "./weather-v2-store";
import type { Env, WeatherCacheRow } from "./types";

const RACE_DATE_PARAM = "race_date";
const WEATHER_PATH = "/weather";
const PING_PATH = "/ping";
const BACKFILL_PATH = "/api/internal/backfill-r2-catalog";
const V2_BACKFILL_PATH = "/api/internal/backfill-r2-catalog-v2";
const RACE_DATE_PATTERN = /^\d{8}$/;
const BAD_REQUEST_STATUS = 400;
const UNAUTHORIZED_STATUS = 401;
const SOURCE_KV = "kv";
const SOURCE_R2 = "r2";
const DEFAULT_BODY = "venue-weather";
const OK_BODY = "ok";
const INVALID_RACE_DATE_BODY = "invalid race_date";
const UNAUTHORIZED_BODY = "unauthorized";
const INTERNAL_TOKEN_HEADER = "x-venue-weather-internal-token";
const V2_BACKFILL_TOKEN_HEADER = "x-venue-weather-v2-backfill-token";
const V2_BACKFILL_MAX_EVENTS = 500;

interface WeatherResponseRow extends WeatherCacheRow {
  weather_code: number | null;
  relative_humidity?: number;
  dew_point?: number;
  wet_bulb_temperature?: number;
  shortwave_radiation?: number;
}

interface BackfillRequestBody {
  catalogOnly?: boolean;
  fetchedAt?: string;
  payloads?: unknown;
  runtimeOnly?: boolean;
}

const toIsoDate = (yyyymmdd: string): string =>
  `${yyyymmdd.slice(0, 4)}-${yyyymmdd.slice(4, 6)}-${yyyymmdd.slice(6, 8)}`;

const v2Key = (row: WeatherV2Row): string => `${row.keibajo_code}:${String(row.weather_hour)}`;

const hasCompleteV2Metrics = (row: WeatherV2Row): boolean =>
  typeof row.relative_humidity === "number" &&
  typeof row.dew_point === "number" &&
  typeof row.wet_bulb_temperature === "number" &&
  typeof row.shortwave_radiation === "number";

const toResponseRows = (rows: WeatherCacheRow[], v2Rows: WeatherV2Row[]): WeatherResponseRow[] => {
  const v2ByKey = new Map(v2Rows.map((row) => [v2Key(row), row]));
  const complete =
    rows.length === v2Rows.length &&
    v2Rows.every(hasCompleteV2Metrics) &&
    rows.every((row) => v2ByKey.has(`${row.keibajo_code}:${String(row.weather_hour)}`));
  return rows.map((row) => {
    const base = { ...row, weather_code: row.weather_type };
    if (!complete) return base;
    const v2 = v2ByKey.get(`${row.keibajo_code}:${String(row.weather_hour)}`);
    if (v2 === undefined) return base;
    return {
      ...base,
      relative_humidity: v2.relative_humidity,
      dew_point: v2.dew_point,
      wet_bulb_temperature: v2.wet_bulb_temperature,
      shortwave_radiation: v2.shortwave_radiation,
    };
  });
};

const handleWeatherRoute = async (env: Env, raceDate: string): Promise<Response> => {
  const cached = await getWeatherFromKv(env.WEATHER_KV, raceDate);
  if (cached !== null) {
    const v2Rows = await readWeatherV2ByDate(env.WEATHER_ARCHIVE, raceDate);
    return Response.json({ rows: toResponseRows(cached, v2Rows), source: SOURCE_KV });
  }
  const rows = await readWeatherByDate(env.WEATHER_ARCHIVE, raceDate);
  if (rows.length === 0) return Response.json({ rows: [], source: SOURCE_R2 });
  await putWeatherToKv({ kv: env.WEATHER_KV, raceDate, rows, ttlSeconds: KV_WEATHER_TTL_SECONDS });
  const v2Rows = await readWeatherV2ByDate(env.WEATHER_ARCHIVE, raceDate);
  return Response.json({ rows: toResponseRows(rows, v2Rows), source: SOURCE_R2 });
};

const isAuthorized = (request: Request, env: Env): boolean => {
  const token = env.VENUE_WEATHER_INTERNAL_TOKEN;
  return Boolean(token) && request.headers.get(INTERNAL_TOKEN_HEADER) === token;
};

const handleV2BackfillRoute = async (request: Request, env: Env): Promise<Response> => {
  const token = env.VENUE_WEATHER_V2_BACKFILL_TOKEN;
  if (token === undefined || request.headers.get(V2_BACKFILL_TOKEN_HEADER) !== token) {
    return new Response(UNAUTHORIZED_BODY, { status: UNAUTHORIZED_STATUS });
  }
  const events: unknown = await request.json();
  if (!Array.isArray(events) || events.length === 0 || events.length > V2_BACKFILL_MAX_EVENTS) {
    return new Response("invalid v2 events", { status: BAD_REQUEST_STATUS });
  }
  const stream = env.WEATHER_CATALOG_STREAM_V2;
  if (stream === undefined) {
    return new Response("v2 stream unavailable", { status: 503 });
  }
  await stream.send(events);
  return Response.json({ rows: events.length });
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
  if (request.method === "POST" && url.pathname === V2_BACKFILL_PATH) {
    return handleV2BackfillRoute(request, env);
  }
  if (url.pathname !== WEATHER_PATH) return new Response(DEFAULT_BODY);
  const raceDate = url.searchParams.get(RACE_DATE_PARAM);
  if (raceDate === null || !RACE_DATE_PATTERN.test(raceDate)) {
    return new Response(INVALID_RACE_DATE_BODY, { status: BAD_REQUEST_STATUS });
  }
  return handleWeatherRoute(env, toIsoDate(raceDate));
};
