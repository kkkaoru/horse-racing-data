import type { Env } from "../types";

const ENQUEUE_LOCK_KEY_PREFIX = "odds:enqueue-lock";
const FINAL_WINDOW_MINUTES_BEFORE = 10;
const PAST_RACE_GRACE_MINUTES_AFTER = 2;
const HIGH_FREQ_WINDOW_MINUTES_BEFORE = 60;
const LOCK_TTL_SKIP_PAST_RACE = 0;
const LOCK_TTL_FINAL_SECONDS = 60;
const LOCK_TTL_HIGH_FREQ_SECONDS = 600;
const LOCK_TTL_DEFAULT_SECONDS = 3600;
const SECONDS_PER_MINUTE = 60;

const buildEnqueueLockKey = (raceKey: string): string => `${ENQUEUE_LOCK_KEY_PREFIX}:${raceKey}`;

export const calculateEnqueueLockTtlSeconds = (raceStart: Date, now: Date): number => {
  const minutesUntilRace = (raceStart.getTime() - now.getTime()) / 60_000;
  // Past races beyond the grace window: signal "skip, do not enqueue" via ttl 0.
  if (minutesUntilRace < -PAST_RACE_GRACE_MINUTES_AFTER) {
    return LOCK_TTL_SKIP_PAST_RACE;
  }
  if (minutesUntilRace <= FINAL_WINDOW_MINUTES_BEFORE) {
    return LOCK_TTL_FINAL_SECONDS;
  }
  if (minutesUntilRace <= HIGH_FREQ_WINDOW_MINUTES_BEFORE) {
    const secondsUntilFinal = Math.ceil(
      (minutesUntilRace - FINAL_WINDOW_MINUTES_BEFORE) * SECONDS_PER_MINUTE,
    );
    return Math.min(LOCK_TTL_HIGH_FREQ_SECONDS, secondsUntilFinal);
  }
  const secondsUntilHighFreq = Math.ceil(
    (minutesUntilRace - HIGH_FREQ_WINDOW_MINUTES_BEFORE) * SECONDS_PER_MINUTE,
  );
  return Math.min(LOCK_TTL_DEFAULT_SECONDS, secondsUntilHighFreq);
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
