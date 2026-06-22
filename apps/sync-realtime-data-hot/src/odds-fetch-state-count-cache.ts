// Run with: bun run --filter sync-realtime-data-hot test
// KV-backed cache for the per-day `count(*) from odds_fetch_state` lookup.
//
// `runPopulateGate` (worker.ts) reads this count on every per-minute planner
// tick that misses the `expected-race-count:stable:` short-circuit. In the
// 10-min window between stable-flag expiries (and on race-day mornings before
// the count first equals the expected total) this fires once per minute. The
// row count for a given JST day only ever increases via the populate cron
// (every ~5h) and the multi-day populate at 20:55/23:00 JST, so a 10-min
// staleness window is safe — `runPopulateGate` re-runs populate when the
// cached count is short, and stale-low only delays writing the stable flag by
// at most one TTL.

import { countOddsFetchStateForDate } from "./storage";
import type { Env } from "./types";

const KV_KEY_PREFIX = "odds-fetch-state-count:";
const KV_TTL_SECONDS = 600;

const buildKvKey = (yyyymmdd: string): string => `${KV_KEY_PREFIX}${yyyymmdd}`;

const parseCachedValue = (raw: string | null): number | null => {
  if (raw === null) {
    return null;
  }
  const parsed = Number.parseInt(raw, 10);
  return Number.isNaN(parsed) ? null : parsed;
};

const readCached = async (env: Env, yyyymmdd: string): Promise<number | null> => {
  try {
    return parseCachedValue(await env.ODDS_HOT_KV.get(buildKvKey(yyyymmdd)));
  } catch {
    // KV read failure must never break the planner — fall through to D1.
    return null;
  }
};

const writeCached = async (env: Env, yyyymmdd: string, count: number): Promise<void> => {
  try {
    await env.ODDS_HOT_KV.put(buildKvKey(yyyymmdd), count.toString(), {
      expirationTtl: KV_TTL_SECONDS,
    });
  } catch {
    // KV write failure must never break the planner — the next tick will retry.
  }
};

export const getCachedOddsFetchStateCount = async (env: Env, yyyymmdd: string): Promise<number> => {
  const cached = await readCached(env, yyyymmdd);
  if (cached !== null) {
    return cached;
  }
  const fresh = await countOddsFetchStateForDate(
    env.REALTIME_HOT_DB,
    yyyymmdd.slice(0, 4),
    yyyymmdd.slice(4, 8),
  );
  await writeCached(env, yyyymmdd, fresh);
  return fresh;
};

// Drop the cached count after a populate run so the next planner tick re-reads
// D1 and observes the fresh row total. Without this, a stale-low value would
// keep re-triggering populate every minute for up to one full TTL.
export const invalidateOddsFetchStateCount = async (env: Env, yyyymmdd: string): Promise<void> => {
  try {
    await env.ODDS_HOT_KV.delete(buildKvKey(yyyymmdd));
  } catch {
    // KV delete failure is non-fatal — the cache entry will expire on its own.
  }
};
