import type { Env } from "../types";

const ENQUEUE_LOCK_KEY_PREFIX = "odds:enqueue-lock";
const FINAL_WINDOW_MINUTES_BEFORE = 5;
const FINAL_WINDOW_MINUTES_AFTER = 3;
const HIGH_FREQ_WINDOW_MINUTES_BEFORE = 30;
const LOCK_TTL_FINAL_SECONDS = 0;
const LOCK_TTL_HIGH_FREQ_SECONDS = 20;
const LOCK_TTL_DEFAULT_SECONDS = 60;

const buildEnqueueLockKey = (raceKey: string): string => `${ENQUEUE_LOCK_KEY_PREFIX}:${raceKey}`;

export const calculateEnqueueLockTtlSeconds = (raceStart: Date, now: Date): number => {
  const minutesUntilRace = (raceStart.getTime() - now.getTime()) / 60_000;
  if (
    minutesUntilRace <= FINAL_WINDOW_MINUTES_BEFORE &&
    minutesUntilRace >= -FINAL_WINDOW_MINUTES_AFTER
  ) {
    return LOCK_TTL_FINAL_SECONDS;
  }
  if (
    minutesUntilRace > FINAL_WINDOW_MINUTES_BEFORE &&
    minutesUntilRace <= HIGH_FREQ_WINDOW_MINUTES_BEFORE
  ) {
    return LOCK_TTL_HIGH_FREQ_SECONDS;
  }
  return LOCK_TTL_DEFAULT_SECONDS;
};

export const isEnqueueLocked = async (env: Env, raceKey: string): Promise<boolean> => {
  const locked = await env.ODDS_HOT_KV.get(buildEnqueueLockKey(raceKey));
  return locked !== null;
};

export const acquireEnqueueLock = async (
  env: Env,
  raceKey: string,
  ttlSeconds: number,
): Promise<void> => {
  if (ttlSeconds <= 0) {
    return;
  }
  await env.ODDS_HOT_KV.put(buildEnqueueLockKey(raceKey), "1", {
    expirationTtl: ttlSeconds,
  });
};
