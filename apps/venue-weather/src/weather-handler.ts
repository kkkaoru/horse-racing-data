// Run with bun.
import { getWeatherFromKv, KV_WEATHER_TTL_SECONDS, putWeatherToKv } from "./weather-kv";
import { readWeatherByDate } from "./weather-d1-reader";
import type { Env } from "./types";

const RACE_DATE_PARAM = "race_date";
const WEATHER_PATH = "/weather";
const PING_PATH = "/ping";
const RACE_DATE_PATTERN = /^\d{8}$/;
const BAD_REQUEST_STATUS = 400;
const SOURCE_KV = "kv";
const SOURCE_D1 = "d1";
const DEFAULT_BODY = "venue-weather";
const OK_BODY = "ok";
const INVALID_RACE_DATE_BODY = "invalid race_date";

const toIsoDate = (yyyymmdd: string): string =>
  `${yyyymmdd.slice(0, 4)}-${yyyymmdd.slice(4, 6)}-${yyyymmdd.slice(6, 8)}`;

const handleWeatherRoute = async (env: Env, raceDate: string): Promise<Response> => {
  const cached = await getWeatherFromKv(env.WEATHER_KV, raceDate);
  if (cached !== null) return Response.json({ rows: cached, source: SOURCE_KV });
  const rows = await readWeatherByDate(env.WEATHER_DB, raceDate);
  if (rows.length === 0) return Response.json({ rows: [], source: SOURCE_D1 });
  await putWeatherToKv({ kv: env.WEATHER_KV, raceDate, rows, ttlSeconds: KV_WEATHER_TTL_SECONDS });
  return Response.json({ rows, source: SOURCE_D1 });
};

export const handleWeatherFetch = async (request: Request, env: Env): Promise<Response> => {
  const url = new URL(request.url);
  if (url.pathname === PING_PATH) return new Response(OK_BODY);
  if (url.pathname !== WEATHER_PATH) return new Response(DEFAULT_BODY);
  const raceDate = url.searchParams.get(RACE_DATE_PARAM);
  if (raceDate === null || !RACE_DATE_PATTERN.test(raceDate)) {
    return new Response(INVALID_RACE_DATE_BODY, { status: BAD_REQUEST_STATUS });
  }
  return handleWeatherRoute(env, toIsoDate(raceDate));
};
