// Run with bun. KV cache for listTodayRaceKeysFromHyperdrive results.
// Replaces the previous behaviour of issuing one Hyperdrive UNION query per
// cron tick (180+ calls/day) with one cache-miss query per source/date.
// After the JST 08:30 local-PG -> Neon sync completes, the (today, tomorrow)
// race-key sets are immutable for the rest of the day, so a 30-min TTL is
// safe for the features build path (the odds hot-path is unaffected).

import type { Env } from "../types";
import type { TodayRaceKey, TodayRaceKeySource } from "../scheduled-race-list";

const TODAY_RACE_KEYS_KV_KEY_PREFIX = "race-keys:v1";
const DEFAULT_TODAY_RACE_KEYS_KV_TTL_SECONDS = 1800;

const buildTodayRaceKeysCacheKey = (source: TodayRaceKeySource, yyyymmdd: string): string =>
  `${TODAY_RACE_KEYS_KV_KEY_PREFIX}:${source}:${yyyymmdd}`;

const resolveTtlSeconds = (env: Env): number => {
  const raw = env.FEATURES_TODAY_RACE_KEYS_KV_TTL_SECONDS;
  if (!raw) {
    return DEFAULT_TODAY_RACE_KEYS_KV_TTL_SECONDS;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_TODAY_RACE_KEYS_KV_TTL_SECONDS;
};

export const getTodayRaceKeysFromKv = async (
  env: Env,
  source: TodayRaceKeySource,
  yyyymmdd: string,
): Promise<TodayRaceKey[] | null> => {
  const json = await env.FEATURES_KV.get(buildTodayRaceKeysCacheKey(source, yyyymmdd));
  return json ? (JSON.parse(json) as TodayRaceKey[]) : null;
};

export const putTodayRaceKeysToKv = async (
  env: Env,
  source: TodayRaceKeySource,
  yyyymmdd: string,
  keys: TodayRaceKey[],
): Promise<void> => {
  await env.FEATURES_KV.put(buildTodayRaceKeysCacheKey(source, yyyymmdd), JSON.stringify(keys), {
    expirationTtl: resolveTtlSeconds(env),
  });
};
