// Run with bun.
import { expect, it, vi } from "vitest";

import {
  acquireEnqueueLock,
  calculateEnqueueLockTtlSeconds,
  calculateEnqueueLockTtlSecondsFromInput,
  isEnqueueLocked,
} from "./enqueue-lock-kv";
import type { Env } from "../types";

interface KvMockHandle {
  get: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
}

const buildEnv = (): Env => {
  const kv = {
    delete: vi.fn(async () => undefined),
    get: vi.fn(async () => null),
    put: vi.fn(async () => undefined),
  } as unknown as KVNamespace;
  return { ODDS_HOT_KV: kv } as Env;
};

it("cadence-60-when-3-min-before (1min cadence interior)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:57:00+09:00"),
    ),
  ).toBe(60);
});

it("cadence-60-at-race-start (fractional final slot, null interval)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T10:00:00+09:00"),
    ),
  ).toBe(60);
});

it("cadence-60-during-past-grace-window (-1 min after race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T10:01:00+09:00"),
    ),
  ).toBe(60);
});

it("cadence-60-at-past-race-grace-boundary (2 min after race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T10:02:00+09:00"),
    ),
  ).toBe(60);
});

it("cadence-60-when-9-min-before (1min cadence interior)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:51:00+09:00"),
    ),
  ).toBe(60);
});

it("cadence-60-when-10-min-before (1min cadence interior)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:50:00+09:00"),
    ),
  ).toBe(60);
});

it("cadence-0-when-past-grace-no-catch-up (3 min after race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T10:03:00+09:00"),
    ),
  ).toBe(0);
});

it("cadence-60-when-11-min-before (1min cadence interior)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:49:00+09:00"),
    ),
  ).toBe(60);
});

it("cadence-60-when-15-min-before (boundary into 5min cadence, gap to T-1 boundary is zero)", () => {
  // T-15 is the lower edge of the 5-min tier. Any lock here would block
  // T-14 (first 1-min slot). Cap to KV minimum (60s) so the 1-min cadence
  // can take over immediately.
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:45:00+09:00"),
    ),
  ).toBe(60);
});

it("cadence-240-when-19-min-before (5min cadence interior, capped at gap to T-15)", () => {
  // gap to T-15 = 4min = 240s, cadence = 300s. min(300, 240) = 240s.
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:41:00+09:00"),
    ),
  ).toBe(240);
});

it("cadence-300-when-30-min-before (5min cadence interior)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:30:00+09:00"),
    ),
  ).toBe(300);
});

it("cadence-300-when-45-min-before (5min cadence interior)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:15:00+09:00"),
    ),
  ).toBe(300);
});

it("cadence-300-when-59-min-before (5min cadence upper)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:01:00+09:00"),
    ),
  ).toBe(300);
});

it("cadence-60-when-60-min-before (boundary T-60, capped to KV minimum at tier lower edge)", () => {
  // T-60 is the lower edge of the hourly tier; any longer lock would
  // block the immediately-following 5-min tier slot at T-59. Cap to KV
  // minimum so the 5-min cadence can take over at the next planner tick.
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:00:00+09:00"),
    ),
  ).toBe(60);
});

it("cadence-60-when-61-min-before (hourly cadence near T-60 boundary, capped to KV minimum)", () => {
  // gap to T-60 = 1min = 60s, KV minimum = 60s.
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T08:59:00+09:00"),
    ),
  ).toBe(60);
});

it("cadence-1800-when-90-min-before (hourly cadence interior, capped at gap to T-60)", () => {
  // gap to T-60 = 30min = 1800s, hourly cadence = 3600s, min = 1800s.
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T08:30:00+09:00"),
    ),
  ).toBe(1800);
});

it("cadence-3600-when-120-min-before (gap to T-60 is exactly the hourly cadence)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T08:00:00+09:00"),
    ),
  ).toBe(3600);
});

it("cadence-3600-when-200-min-before (hourly cadence interior, far before race)", () => {
  // gap to T-60 = 140min = 8400s, but hourly cadence caps at 3600s.
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T06:40:00+09:00"),
    ),
  ).toBe(3600);
});

it("cadence-240-when-64-min-before (regression test for 5R 03 starvation bug)", () => {
  // 06-06 incident: at T-64 the legacy 3600s lock spanned past T-15 and T-1
  // entirely, blocking 5min and 1min cadence enqueues for that race while
  // the previous race in the same venue (T-30) kept being fetched.
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T08:56:00+09:00"),
    ),
  ).toBe(240);
});

it("cadence-60-when-16-min-before (5min cadence near T-15 boundary, capped to KV minimum)", () => {
  // T-16: gap to T-15 = 1min = 60s, cadence = 300s. min(300, 60) = 60.
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:44:00+09:00"),
    ),
  ).toBe(60);
});

it("cadence-60-when-5-min-before (1min cadence, gap to T-1 of 4min larger than cadence)", () => {
  // T-5 is in the 1-min tier (1 <= T < 15). cadence = 60s, gap = 4min =
  // 240s, min(60, 240) = 60. Tier-cap doesn't kick in here.
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:55:00+09:00"),
    ),
  ).toBe(60);
});

it("cadence-0-well-after-race (4 min after)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T10:04:00+09:00"),
    ),
  ).toBe(0);
});

it("cadence-0-well-after-race (5 min after)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T10:05:00+09:00"),
    ),
  ).toBe(0);
});

it("cadence-0-far-past-race (1 hour after)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T11:00:00+09:00"),
    ),
  ).toBe(0);
});

it("cadence-60-when-fractional-final-slot (0.5 min before race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:59:30+09:00"),
    ),
  ).toBe(60);
});

it("isEnqueueLocked returns true when KV value present", async () => {
  const env = buildEnv();
  const getMock = env.ODDS_HOT_KV.get as unknown as KvMockHandle["get"];
  getMock.mockResolvedValueOnce("1");
  expect(await isEnqueueLocked(env, "nar:20260528:42:01")).toBe(true);
});

it("isEnqueueLocked returns false when KV value absent", async () => {
  const env = buildEnv();
  expect(await isEnqueueLocked(env, "nar:20260528:42:01")).toBe(false);
});

it("acquireEnqueueLock skips PUT when ttlSeconds is zero", async () => {
  const env = buildEnv();
  await acquireEnqueueLock(env, "nar:20260528:42:01", 0);
  expect(env.ODDS_HOT_KV.put).not.toHaveBeenCalled();
});

it("acquireEnqueueLock writes KV entry when ttlSeconds positive", async () => {
  const env = buildEnv();
  await acquireEnqueueLock(env, "nar:20260528:42:01", 60);
  expect(env.ODDS_HOT_KV.put).toHaveBeenCalledWith("odds:enqueue-lock:nar:20260528:42:01", "1", {
    expirationTtl: 60,
  });
});

it("calculateEnqueueLockTtlSecondsFromInput returns 300 catch-up TTL for past race within 60min when lastOddsFetchAt is null", () => {
  expect(
    calculateEnqueueLockTtlSecondsFromInput({
      allowCatchUp: true,
      lastOddsFetchAt: null,
      now: new Date("2026-05-28T10:30:00+09:00"),
      raceStart: new Date("2026-05-28T10:00:00+09:00"),
    }),
  ).toBe(300);
});

it("calculateEnqueueLockTtlSecondsFromInput returns 0 for past race when lastOddsFetchAt is at or after final slot", () => {
  expect(
    calculateEnqueueLockTtlSecondsFromInput({
      allowCatchUp: true,
      lastOddsFetchAt: "2026-05-28T10:02:00+09:00",
      now: new Date("2026-05-28T10:30:00+09:00"),
      raceStart: new Date("2026-05-28T10:00:00+09:00"),
    }),
  ).toBe(0);
});

it("calculateEnqueueLockTtlSecondsFromInput returns 300 catch-up TTL when lastOddsFetchAt is before final slot", () => {
  expect(
    calculateEnqueueLockTtlSecondsFromInput({
      allowCatchUp: true,
      lastOddsFetchAt: "2026-05-28T09:55:00+09:00",
      now: new Date("2026-05-28T10:30:00+09:00"),
      raceStart: new Date("2026-05-28T10:00:00+09:00"),
    }),
  ).toBe(300);
});

it("calculateEnqueueLockTtlSecondsFromInput returns 0 once past 60-minute catch-up window expires", () => {
  expect(
    calculateEnqueueLockTtlSecondsFromInput({
      allowCatchUp: true,
      lastOddsFetchAt: null,
      now: new Date("2026-05-28T11:30:00+09:00"),
      raceStart: new Date("2026-05-28T10:00:00+09:00"),
    }),
  ).toBe(0);
});

it("calculateEnqueueLockTtlSecondsFromInput returns 0 when allowCatchUp is false (legacy behavior)", () => {
  expect(
    calculateEnqueueLockTtlSecondsFromInput({
      allowCatchUp: false,
      lastOddsFetchAt: null,
      now: new Date("2026-05-28T10:30:00+09:00"),
      raceStart: new Date("2026-05-28T10:00:00+09:00"),
    }),
  ).toBe(0);
});

it("calculateEnqueueLockTtlSecondsFromInput defaults allowCatchUp to false when omitted", () => {
  expect(
    calculateEnqueueLockTtlSecondsFromInput({
      now: new Date("2026-05-28T10:30:00+09:00"),
      raceStart: new Date("2026-05-28T10:00:00+09:00"),
    }),
  ).toBe(0);
});

it("calculateEnqueueLockTtlSecondsFromInput ignores unparseable lastOddsFetchAt and treats it as a missed final slot", () => {
  expect(
    calculateEnqueueLockTtlSecondsFromInput({
      allowCatchUp: true,
      lastOddsFetchAt: "not-a-date",
      now: new Date("2026-05-28T10:30:00+09:00"),
      raceStart: new Date("2026-05-28T10:00:00+09:00"),
    }),
  ).toBe(300);
});

it("calculateEnqueueLockTtlSecondsFromInput returns 60 at T-60 with allowCatchUp true (tier lower edge)", () => {
  // T-60 is at the lower edge of the hourly tier; gap = 0 → KV minimum.
  // Tier-cap applies independently of allowCatchUp.
  expect(
    calculateEnqueueLockTtlSecondsFromInput({
      allowCatchUp: true,
      lastOddsFetchAt: null,
      now: new Date("2026-05-28T09:00:00+09:00"),
      raceStart: new Date("2026-05-28T10:00:00+09:00"),
    }),
  ).toBe(60);
});

it("calculateEnqueueLockTtlSecondsFromInput returns 60 in 1min cadence even when allowCatchUp is true (5min before race)", () => {
  // T-5: gap to T-1 = 4min = 240s, cadence = 60s. min(60, 240) = 60.
  expect(
    calculateEnqueueLockTtlSecondsFromInput({
      allowCatchUp: true,
      lastOddsFetchAt: null,
      now: new Date("2026-05-28T09:55:00+09:00"),
      raceStart: new Date("2026-05-28T10:00:00+09:00"),
    }),
  ).toBe(60);
});

it("calculateEnqueueLockTtlSecondsFromInput returns 3600 hourly cadence even when allowCatchUp is true (120min before race)", () => {
  // T-120: gap to T-60 = 60min = 3600s, cadence = 3600s. min = 3600.
  expect(
    calculateEnqueueLockTtlSecondsFromInput({
      allowCatchUp: true,
      lastOddsFetchAt: null,
      now: new Date("2026-05-28T08:00:00+09:00"),
      raceStart: new Date("2026-05-28T10:00:00+09:00"),
    }),
  ).toBe(3600);
});

it("cadence-60-at-1min-boundary (1 min before race, lower edge of 1min cadence)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:59:00+09:00"),
    ),
  ).toBe(60);
});

it("cadence-60-just-below-1min-boundary (0.99 min before race, null interval)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:59:00.600+09:00"),
    ),
  ).toBe(60);
});
