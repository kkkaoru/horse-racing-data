// Run with bun. Per-day race list KV cache. Used to avoid hitting REALTIME_OLD on every tick.

import type { Env } from "../types";

const RACE_LIST_KV_KEY_PREFIX = "features:race-list:v1";
const DEFAULT_RACE_LIST_KV_TTL_SECONDS = 21_600;

export interface FeaturesRaceListEntry {
  raceKey: string;
  source: "jra" | "nar";
  raceStartAtJst: string;
  lastBuiltAt: string | null;
}

const buildRaceListKey = (source: "jra" | "nar", yyyymmdd: string): string =>
  `${RACE_LIST_KV_KEY_PREFIX}:${source}:${yyyymmdd}`;

const resolveTtlSeconds = (env: Env): number => {
  const raw = env.FEATURES_RACE_LIST_KV_TTL_SECONDS;
  if (!raw) {
    return DEFAULT_RACE_LIST_KV_TTL_SECONDS;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_RACE_LIST_KV_TTL_SECONDS;
};

export const getRaceListFromKv = async (
  env: Env,
  source: "jra" | "nar",
  yyyymmdd: string,
): Promise<FeaturesRaceListEntry[] | null> => {
  const json = await env.FEATURES_KV.get(buildRaceListKey(source, yyyymmdd));
  return json ? (JSON.parse(json) as FeaturesRaceListEntry[]) : null;
};

export const putRaceListToKv = async (
  env: Env,
  source: "jra" | "nar",
  yyyymmdd: string,
  list: FeaturesRaceListEntry[],
): Promise<void> => {
  await env.FEATURES_KV.put(buildRaceListKey(source, yyyymmdd), JSON.stringify(list), {
    expirationTtl: resolveTtlSeconds(env),
  });
};

export const invalidateRaceListInKv = async (
  env: Env,
  source: "jra" | "nar",
  yyyymmdd: string,
): Promise<void> => {
  await env.FEATURES_KV.delete(buildRaceListKey(source, yyyymmdd));
};
