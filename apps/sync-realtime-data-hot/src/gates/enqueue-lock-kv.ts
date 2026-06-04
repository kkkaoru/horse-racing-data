import { getOddsFetchIntervalMinutes } from "../time";
import type { Env } from "../types";

const ENQUEUE_LOCK_KEY_PREFIX = "odds:enqueue-lock";
const PAST_RACE_GRACE_MINUTES_AFTER = 2;
// Catch-up window (task F2 C): allow a single final-slot enqueue for races
// that ran in the last 60 minutes but never had their finalSlot fetched.
// Beyond that, the race result is in and odds are no longer changing.
const CATCH_UP_WINDOW_MINUTES_AFTER = 60;
const LOCK_TTL_SKIP_PAST_RACE = 0;
// Upper cap (60min cadence). Lower clamp at KV minimum (60s) matches the
// 1min cadence window and keeps any Cloudflare KV expirationTtl >= 60.
const LOCK_TTL_DEFAULT_SECONDS = 3600;
const LOCK_TTL_CATCH_UP_SECONDS = 300;
// Cloudflare KV requires expirationTtl >= 60. Also matches the 1-min
// cadence interval, so callers in the final window naturally land here.
const LOCK_TTL_KV_MINIMUM_SECONDS = 60;
const SECONDS_PER_MINUTE = 60;
const MS_PER_MINUTE = 60_000;

interface EnqueueLockTtlInput {
  raceStart: Date;
  now: Date;
  // Catch-up gate: `undefined` means "do not consider catch-up" (preserves
  // legacy `calculateEnqueueLockTtlSeconds(raceStart, now)` past-race=0
  // behavior). Pass an explicit `string | null` to opt in.
  lastOddsFetchAt?: string | null | undefined;
  allowCatchUp?: boolean;
}

const buildEnqueueLockKey = (raceKey: string): string => `${ENQUEUE_LOCK_KEY_PREFIX}:${raceKey}`;

// The catch-up final slot is `raceStart + PAST_RACE_GRACE_MINUTES_AFTER`,
// matching `resolveOddsSlotAt` / `getOddsFetchSlotAt`. If the row's
// `last_odds_fetch_at` is null or earlier than that target, the final
// closing odds were never persisted and we let one more enqueue through.
const wasFinalSlotCaptured = (raceStart: Date, lastOddsFetchAt: string | null): boolean => {
  if (!lastOddsFetchAt) {
    return false;
  }
  const captured = new Date(lastOddsFetchAt).getTime();
  if (Number.isNaN(captured)) {
    return false;
  }
  const finalSlotMs = raceStart.getTime() + PAST_RACE_GRACE_MINUTES_AFTER * MS_PER_MINUTE;
  return captured >= finalSlotMs;
};

const resolveCatchUpTtl = (
  minutesUntilRace: number,
  raceStart: Date,
  lastOddsFetchAt: string | null,
): number => {
  if (
    minutesUntilRace >= -CATCH_UP_WINDOW_MINUTES_AFTER &&
    !wasFinalSlotCaptured(raceStart, lastOddsFetchAt)
  ) {
    return LOCK_TTL_CATCH_UP_SECONDS;
  }
  return LOCK_TTL_SKIP_PAST_RACE;
};

// Lock TTL mirrors the planner cadence interval so a planner tick can
// always fire on schedule: 60min cadence → 3600s lock, 5min → 300s,
// 1min → 60s. Anything outside the interval table (final +2 slot or
// post-grace) falls to the KV minimum so a stale lock cannot block the
// next opportunity.
const resolveCadenceLockSeconds = (minutesUntilRace: number): number => {
  const intervalMinutes = getOddsFetchIntervalMinutes(minutesUntilRace);
  if (intervalMinutes === null) {
    return LOCK_TTL_KV_MINIMUM_SECONDS;
  }
  const cadenceSeconds = intervalMinutes * SECONDS_PER_MINUTE;
  return Math.max(LOCK_TTL_KV_MINIMUM_SECONDS, Math.min(LOCK_TTL_DEFAULT_SECONDS, cadenceSeconds));
};

export const calculateEnqueueLockTtlSeconds = (raceStart: Date, now: Date): number =>
  calculateEnqueueLockTtlSecondsFromInput({ allowCatchUp: false, now, raceStart });

export const calculateEnqueueLockTtlSecondsFromInput = (input: EnqueueLockTtlInput): number => {
  const lastOddsFetchAt = input.lastOddsFetchAt ?? null;
  const allowCatchUp = input.allowCatchUp ?? false;
  const minutesUntilRace = (input.raceStart.getTime() - input.now.getTime()) / MS_PER_MINUTE;
  // Past races beyond the grace window: one chance to catch the final slot
  // when `last_odds_fetch_at` proves it was never recorded — but only if
  // the caller opted into catch-up. Legacy 2-arg callers keep ttl=0.
  if (minutesUntilRace < -PAST_RACE_GRACE_MINUTES_AFTER) {
    if (!allowCatchUp) {
      return LOCK_TTL_SKIP_PAST_RACE;
    }
    return resolveCatchUpTtl(minutesUntilRace, input.raceStart, lastOddsFetchAt);
  }
  return resolveCadenceLockSeconds(minutesUntilRace);
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
