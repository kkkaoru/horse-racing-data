// Run with bun.
import type { WeatherCacheRow } from "./types";

interface PutWeatherToKvParams {
  kv: KVNamespace;
  raceDate: string;
  rows: WeatherCacheRow[];
  ttlSeconds: number;
}

const KEY_PREFIX = "weather:";

export const KV_WEATHER_TTL_SECONDS: number = 3600;

export const buildWeatherKey = (raceDate: string): string => `${KEY_PREFIX}${raceDate}`;

export const getWeatherFromKv = async (
  kv: KVNamespace,
  raceDate: string,
): Promise<WeatherCacheRow[] | null> => {
  const raw = await kv.get(buildWeatherKey(raceDate));
  if (raw === null) return null;
  const parsed: WeatherCacheRow[] = JSON.parse(raw);
  return parsed;
};

export const putWeatherToKv = async ({
  kv,
  raceDate,
  rows,
  ttlSeconds,
}: PutWeatherToKvParams): Promise<void> => {
  await kv.put(buildWeatherKey(raceDate), JSON.stringify(rows), { expirationTtl: ttlSeconds });
};

export const deleteWeatherFromKv = async (kv: KVNamespace, raceDate: string): Promise<void> => {
  await kv.delete(buildWeatherKey(raceDate));
};
