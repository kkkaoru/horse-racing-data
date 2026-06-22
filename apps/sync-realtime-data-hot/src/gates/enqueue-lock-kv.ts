import { getOddsFetchIntervalMinutes } from "../time";
import type { Env } from "../types";

const ENQUEUE_LOCK_KEY_PREFIX = "odds:enqueue-lock";
const PAST_RACE_GRACE_MINUTES_AFTER = 2;
// catch-up window for late venue self-discovery; covers ~6h after race start
const CATCH_UP_WINDOW_MINUTES_AFTER = 360;
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
// Cadence boundaries (in minutes-until-race). When a long-cadence lock would
// span past one of these, we cap the TTL so the next planner tick can
// re-enqueue immediately under the new cadence. This is what unblocks the
// "1 venue starves next race" bug (task F1): at T-64 the hourly cadence
// otherwise locks for 60min and blocks the 5min/1min tiers entirely.
const CADENCE_BOUNDARIES_MINUTES = [60, 15, 1] satisfies readonly number[];

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

// Minutes remaining inside the current cadence tier. We pick the largest
// cadence boundary that is `<= minutesUntilRace` — that boundary is the
// lower edge of the current tier. The gap is `minutesUntilRace - boundary`,
// i.e. the time until we drop into the next (finer) tier. For
// `minutesUntilRace=64` the current tier is hourly (lower edge T-60), gap
// = 4min — so the lock is capped at 4min and the 5-min tier can take over
// immediately at T-60. Returns `null` when no boundary is `<=` the input
// (sub-1min final slot) — caller falls through to KV minimum.
const minutesRemainingInCurrentCadenceTier = (minutesUntilRace: number): number | null => {
  const lowerEdge = CADENCE_BOUNDARIES_MINUTES.find((boundary) => boundary <= minutesUntilRace);
  return lowerEdge === undefined ? null : minutesUntilRace - lowerEdge;
};

// Lock TTL mirrors the planner cadence interval so a planner tick can
// always fire on schedule: 60min cadence → 3600s lock, 5min → 300s,
// 1min → 60s. Anything outside the interval table (final +2 slot or
// post-grace) falls to the KV minimum so a stale lock cannot block the
// next opportunity. We additionally cap the TTL at the gap to the next
// cadence boundary so a single hourly-tier lock cannot block the 5min /
// 1min tiers that follow it (task F1).
const resolveCadenceLockSeconds = (minutesUntilRace: number): number => {
  const intervalMinutes = getOddsFetchIntervalMinutes(minutesUntilRace);
  if (intervalMinutes === null) {
    return LOCK_TTL_KV_MINIMUM_SECONDS;
  }
  const cadenceSeconds = intervalMinutes * SECONDS_PER_MINUTE;
  const tierGapMinutes = minutesRemainingInCurrentCadenceTier(minutesUntilRace);
  const tierCapSeconds =
    tierGapMinutes === null ? cadenceSeconds : tierGapMinutes * SECONDS_PER_MINUTE;
  const cappedSeconds = Math.min(cadenceSeconds, tierCapSeconds);
  return Math.max(LOCK_TTL_KV_MINIMUM_SECONDS, Math.min(LOCK_TTL_DEFAULT_SECONDS, cappedSeconds));
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

// Drop the enqueue lock so the next planner tick can re-enqueue this race
// immediately (task K1-B). Used by the consumer on retryable scrape errors
// (transient JRA browser failure, network blip) where holding the lock for
// the full cadence interval would skip the next opportunity to fetch odds.
// Non-retryable failures (missing binding, missing state) leave the lock in
// place so the planner does not spin against a known-broken race row.
export const releaseEnqueueLock = async (env: Env, raceKey: string): Promise<void> => {
  await env.ODDS_HOT_KV.delete(buildEnqueueLockKey(raceKey));
};
