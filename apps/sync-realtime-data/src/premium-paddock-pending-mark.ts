// run with: bun run test
// KV-backed marker that suppresses D1 writes for the "pending placeholder"
// outcome of the premium paddock fetch. During the T-120 window each race
// polls upstream every 15s. If the upstream HTML is the "paddock not yet
// published" placeholder, we mark the race in KV for PENDING_MARK_TTL_SECONDS
// so subsequent fetches within that bucket can short-circuit before issuing
// 3 D1 writes (fetch_state + notification_event + notification_state).
//
// Freshness invariants:
// - Only the "pending" verdict is cached. auth_required / unavailable /
//   empty / ok all bypass the cache (read path) and always run D1 writes.
// - On cache miss the worker still issues a real upstream fetch, so a newly
//   published paddock is detected within at most PENDING_MARK_TTL_SECONDS.
// - On cache hit the worker skips both upstream fetch and D1 writes, leaving
//   the previously-written pending row intact.
import type { Env } from "./types";

const PENDING_MARK_KV_PREFIX = "paddock:pending-mark:";
const PENDING_MARK_TTL_SECONDS = 60;
const PENDING_MARK_VALUE = "1";

const getKv = (env: Env): KVNamespace | null => env.DETAIL_SECTION_CACHE_KV ?? null;

const buildPendingMarkKey = (raceKey: string): string => `${PENDING_MARK_KV_PREFIX}${raceKey}`;

export const readPremiumPaddockPendingMark = async (
  env: Env,
  raceKey: string,
): Promise<boolean> => {
  const kv = getKv(env);
  if (!kv) {
    return false;
  }
  const value = await kv.get(buildPendingMarkKey(raceKey));
  return value !== null;
};

export const writePremiumPaddockPendingMark = async (env: Env, raceKey: string): Promise<void> => {
  const kv = getKv(env);
  if (!kv) {
    return;
  }
  await kv.put(buildPendingMarkKey(raceKey), PENDING_MARK_VALUE, {
    expirationTtl: PENDING_MARK_TTL_SECONDS,
  });
};

export const clearPremiumPaddockPendingMark = async (env: Env, raceKey: string): Promise<void> => {
  const kv = getKv(env);
  if (!kv) {
    return;
  }
  await kv.delete(buildPendingMarkKey(raceKey));
};
