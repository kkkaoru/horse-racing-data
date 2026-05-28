import type { Env, OddsData, OddsType } from "../types";

const LATEST_KV_KEY_PREFIX = "odds:latest";
const DEFAULT_LATEST_KV_TTL_SECONDS = 600;
const DEFAULT_STALE_MIRROR_SECONDS = 90;
const MS_PER_SECOND = 1000;

export interface LatestOddsMirrorPayload {
  fetchedAt: string;
  latest: Partial<Record<OddsType, OddsData[]>>;
}

export interface ReadLatestOddsFromKvOptions {
  now: Date;
  allowStale: boolean;
}

const buildLatestKvKey = (raceKey: string): string => `${LATEST_KV_KEY_PREFIX}:${raceKey}`;

const resolveLatestKvTtl = (env: Env): number => {
  const raw = env.ODDS_LATEST_KV_TTL_SECONDS;
  if (!raw) {
    return DEFAULT_LATEST_KV_TTL_SECONDS;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_LATEST_KV_TTL_SECONDS;
};

const resolveStaleSeconds = (env: Env): number => {
  const raw = env.ODDS_STALE_MIRROR_SECONDS;
  if (!raw) {
    return DEFAULT_STALE_MIRROR_SECONDS;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_STALE_MIRROR_SECONDS;
};

export const readLatestOddsFromKv = async (
  env: Env,
  raceKey: string,
  options: ReadLatestOddsFromKvOptions,
): Promise<LatestOddsMirrorPayload | null> => {
  const json = await env.ODDS_HOT_KV.get(buildLatestKvKey(raceKey));
  if (!json) {
    return null;
  }
  const payload = JSON.parse(json) as LatestOddsMirrorPayload;
  if (options.allowStale) {
    return payload;
  }
  const fetchedAtMs = new Date(payload.fetchedAt).getTime();
  const staleThresholdMs = resolveStaleSeconds(env) * MS_PER_SECOND;
  if (options.now.getTime() - fetchedAtMs > staleThresholdMs) {
    return null;
  }
  return payload;
};

export const writeLatestOddsToKv = async (
  env: Env,
  raceKey: string,
  payload: LatestOddsMirrorPayload,
): Promise<void> => {
  await env.ODDS_HOT_KV.put(buildLatestKvKey(raceKey), JSON.stringify(payload), {
    expirationTtl: resolveLatestKvTtl(env),
  });
};
