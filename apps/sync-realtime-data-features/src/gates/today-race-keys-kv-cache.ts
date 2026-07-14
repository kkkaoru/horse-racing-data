// Run with bun. KV cache for catalog race-key results.
// Replaces repeated catalog requests with one cache-miss request per source/date.
// After the local-PG raw-table partition is published to Iceberg, the
// (today, tomorrow) race-key sets are immutable for the rest of the day, so a
// 30-min TTL is safe for the features build path.

import type { TodayRaceKey, TodayRaceKeySource } from "../scheduled-race-list";

export interface TodayRaceKeysKvStore {
  get(key: string): Promise<string | null>;
  put(key: string, value: string, options: { expirationTtl: number }): Promise<void>;
}

export interface TodayRaceKeysKvEnv {
  FEATURES_KV: TodayRaceKeysKvStore;
  FEATURES_TODAY_RACE_KEYS_KV_TTL_SECONDS?: string;
}

const TODAY_RACE_KEYS_KV_KEY_PREFIX = "race-keys:catalog-v1";
const DEFAULT_TODAY_RACE_KEYS_KV_TTL_SECONDS = 1800;

const buildTodayRaceKeysCacheKey = (source: TodayRaceKeySource, yyyymmdd: string): string =>
  `${TODAY_RACE_KEYS_KV_KEY_PREFIX}:${source}:${yyyymmdd}`;

const resolveTtlSeconds = (env: TodayRaceKeysKvEnv): number => {
  const raw = env.FEATURES_TODAY_RACE_KEYS_KV_TTL_SECONDS;
  if (!raw) {
    return DEFAULT_TODAY_RACE_KEYS_KV_TTL_SECONDS;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_TODAY_RACE_KEYS_KV_TTL_SECONDS;
};

export const getTodayRaceKeysFromKv = async (
  env: TodayRaceKeysKvEnv,
  source: TodayRaceKeySource,
  yyyymmdd: string,
): Promise<TodayRaceKey[] | null> => {
  const json = await env.FEATURES_KV.get(buildTodayRaceKeysCacheKey(source, yyyymmdd));
  return json ? (JSON.parse(json) as TodayRaceKey[]) : null;
};

export const putTodayRaceKeysToKv = async (
  env: TodayRaceKeysKvEnv,
  source: TodayRaceKeySource,
  yyyymmdd: string,
  keys: TodayRaceKey[],
): Promise<void> => {
  await env.FEATURES_KV.put(buildTodayRaceKeysCacheKey(source, yyyymmdd), JSON.stringify(keys), {
    expirationTtl: resolveTtlSeconds(env),
  });
};
