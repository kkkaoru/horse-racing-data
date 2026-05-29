// Run with bun. Gate: mirror latest per-race features in KV to avoid R2 GET.

import type { DailyRaceEntryRow, Env } from "../types";

const LATEST_KV_KEY_PREFIX = "features:latest";
const DEFAULT_LATEST_KV_TTL_SECONDS = 600;

const buildLatestKvKey = (raceKey: string): string => `${LATEST_KV_KEY_PREFIX}:${raceKey}`;

const resolveTtlSeconds = (env: Env): number => {
  const raw = env.FEATURES_LATEST_KV_TTL_SECONDS;
  if (!raw) {
    return DEFAULT_LATEST_KV_TTL_SECONDS;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_LATEST_KV_TTL_SECONDS;
};

export const readLatestFeaturesFromKv = async (
  env: Env,
  raceKey: string,
): Promise<DailyRaceEntryRow[] | null> => {
  const json = await env.FEATURES_KV.get(buildLatestKvKey(raceKey));
  return json ? (JSON.parse(json) as DailyRaceEntryRow[]) : null;
};

export const writeLatestFeaturesToKv = async (
  env: Env,
  raceKey: string,
  rows: DailyRaceEntryRow[],
): Promise<void> => {
  await env.FEATURES_KV.put(buildLatestKvKey(raceKey), JSON.stringify(rows), {
    expirationTtl: resolveTtlSeconds(env),
  });
};
