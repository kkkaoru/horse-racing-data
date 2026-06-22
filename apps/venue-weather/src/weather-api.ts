// Run with bun.
import type { VenueCoord, WeatherRow, WeatherType } from "./types";

// Constants at top
const OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive";
const OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast";
const HOURLY_VARS = "weather_code,temperature_2m,precipitation,wind_speed_10m,wind_gusts_10m";
const TIMEZONE = "Asia/Tokyo";
const CACHE_URL_BASE = "https://venue-weather.local/open-meteo/";
const FORECAST_CACHE_TTL_SECONDS = 1800;
const ACTUAL_CACHE_TTL_SECONDS = 86400;
const ARCHIVE_LAG_DAYS = 5;

const CACHE_TTL_BY_TYPE: Record<WeatherType, number> = {
  forecast: FORECAST_CACHE_TTL_SECONDS,
  actual: ACTUAL_CACHE_TTL_SECONDS,
};

interface OpenMeteoHourly {
  time: string[];
  weather_code: (number | null)[];
  temperature_2m: (number | null)[];
  precipitation: (number | null)[];
  wind_speed_10m: (number | null)[];
  wind_gusts_10m: (number | null)[];
}

interface OpenMeteoResponse {
  hourly?: OpenMeteoHourly;
}

export interface BuildUrlParams {
  venue: VenueCoord;
  raceDate: string;
  weatherType: WeatherType;
}

// isArchiveDate: true when raceDate is old enough for archive API
export const isArchiveDate = (raceDate: string): boolean => {
  const cutoff = new Date();
  cutoff.setUTCDate(cutoff.getUTCDate() - ARCHIVE_LAG_DAYS);
  return new Date(raceDate) < cutoff;
};

export const buildWeatherUrl = ({ venue, raceDate, weatherType }: BuildUrlParams): string => {
  const useArchive = weatherType === "actual" && isArchiveDate(raceDate);
  const base = useArchive ? OPEN_METEO_ARCHIVE_URL : OPEN_METEO_FORECAST_URL;
  const params = new URLSearchParams({
    latitude: String(venue.lat),
    longitude: String(venue.lon),
    hourly: HOURLY_VARS,
    timezone: TIMEZONE,
    start_date: raceDate,
    end_date: raceDate,
  });
  return `${base}?${params.toString()}`;
};

const getDefaultCache = (): Cache | null => {
  if (typeof caches === "undefined") return null;
  return caches.default ?? null;
};

const buildCacheRequest = (url: string): Request =>
  new Request(`${CACHE_URL_BASE}${encodeURIComponent(url)}`);

export const fetchWithCache = async (url: string, ttlSeconds: number): Promise<string> => {
  const cache = getDefaultCache();
  const cacheKey = buildCacheRequest(url);
  if (cache !== null) {
    const cached = await cache.match(cacheKey);
    if (cached !== undefined) return cached.text();
  }
  const response = await fetch(url);
  if (!response.ok) throw new Error(`HTTP ${response.status}: ${url}`);
  const text = await response.text();
  if (cache !== null) {
    await cache.put(
      cacheKey,
      new Response(text, {
        headers: { "Cache-Control": `public, max-age=${ttlSeconds}` },
      }),
    );
  }
  return text;
};

export const parseWeatherResponse = (raw: string): WeatherRow[] => {
  const parsed: OpenMeteoResponse = JSON.parse(raw);
  const { hourly } = parsed;
  if (!hourly?.time) return [];
  return hourly.time.map((t, i) => ({
    date: t.slice(0, 10),
    hour: parseInt(t.slice(11, 13), 10),
    weatherCode: hourly.weather_code[i] ?? null,
    temperature: hourly.temperature_2m[i] ?? null,
    precipitation: hourly.precipitation[i] ?? null,
    windSpeed: hourly.wind_speed_10m[i] ?? null,
    windGusts: hourly.wind_gusts_10m[i] ?? null,
  }));
};

export const fetchVenueWeather = async (params: BuildUrlParams): Promise<WeatherRow[]> => {
  const ttl = CACHE_TTL_BY_TYPE[params.weatherType];
  const url = buildWeatherUrl(params);
  const raw = await fetchWithCache(url, ttl);
  return parseWeatherResponse(raw);
};
