// run with: bun run --filter sync-realtime-data test
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import {
  clearDailyPgCache,
  getCachedDailyPgRows,
  getDailyPgCacheSize,
  invalidateDailyPgCache,
  setCachedDailyPgRows,
} from "./daily-pg-cache";

beforeEach(() => {
  clearDailyPgCache();
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
  delete process.env.SYNC_REALTIME_DAILY_PG_CACHE_TTL_MS;
});

test("returns null when key is not cached", () => {
  expect(getCachedDailyPgRows({ source: "jra", targetDate: "20260628" })).toBe(null);
});

test("returns rows after set within TTL", () => {
  setCachedDailyPgRows({ source: "jra", targetDate: "20260628" }, [{ id: 1 }, { id: 2 }]);
  expect(getCachedDailyPgRows({ source: "jra", targetDate: "20260628" })).toStrictEqual([
    { id: 1 },
    { id: 2 },
  ]);
});

test("scopes by source so jra and nar do not collide", () => {
  setCachedDailyPgRows({ source: "jra", targetDate: "20260628" }, [{ id: 1 }]);
  setCachedDailyPgRows({ source: "nar", targetDate: "20260628" }, [{ id: 99 }]);
  expect(getCachedDailyPgRows({ source: "nar", targetDate: "20260628" })).toStrictEqual([
    { id: 99 },
  ]);
});

test("scopes by targetDate so different dates do not collide", () => {
  setCachedDailyPgRows({ source: "jra", targetDate: "20260628" }, [{ id: 1 }]);
  setCachedDailyPgRows({ source: "jra", targetDate: "20260629" }, [{ id: 2 }]);
  expect(getCachedDailyPgRows({ source: "jra", targetDate: "20260629" })).toStrictEqual([
    { id: 2 },
  ]);
});

test("expires entries after the default TTL elapses", () => {
  const baseDate = new Date("2026-06-28T00:00:00Z");
  vi.setSystemTime(baseDate);
  setCachedDailyPgRows({ source: "jra", targetDate: "20260628" }, [{ id: 1 }]);
  vi.setSystemTime(new Date(baseDate.getTime() + 60 * 60 * 1000 + 1));
  expect(getCachedDailyPgRows({ source: "jra", targetDate: "20260628" })).toBe(null);
});

test("uses SYNC_REALTIME_DAILY_PG_CACHE_TTL_MS when finite and positive", () => {
  process.env.SYNC_REALTIME_DAILY_PG_CACHE_TTL_MS = "1000";
  const baseDate = new Date("2026-06-28T00:00:00Z");
  vi.setSystemTime(baseDate);
  setCachedDailyPgRows({ source: "jra", targetDate: "20260628" }, [{ id: 1 }]);
  vi.setSystemTime(new Date(baseDate.getTime() + 999));
  expect(getCachedDailyPgRows({ source: "jra", targetDate: "20260628" })).toStrictEqual([
    { id: 1 },
  ]);
  vi.setSystemTime(new Date(baseDate.getTime() + 1001));
  expect(getCachedDailyPgRows({ source: "jra", targetDate: "20260628" })).toBe(null);
});

test("falls back to default TTL when env override is not finite", () => {
  process.env.SYNC_REALTIME_DAILY_PG_CACHE_TTL_MS = "not-a-number";
  const baseDate = new Date("2026-06-28T00:00:00Z");
  vi.setSystemTime(baseDate);
  setCachedDailyPgRows({ source: "jra", targetDate: "20260628" }, [{ id: 1 }]);
  vi.setSystemTime(new Date(baseDate.getTime() + 60 * 60 * 1000 - 1));
  expect(getCachedDailyPgRows({ source: "jra", targetDate: "20260628" })).toStrictEqual([
    { id: 1 },
  ]);
});

test("falls back to default TTL when env override is zero or negative", () => {
  process.env.SYNC_REALTIME_DAILY_PG_CACHE_TTL_MS = "0";
  const baseDate = new Date("2026-06-28T00:00:00Z");
  vi.setSystemTime(baseDate);
  setCachedDailyPgRows({ source: "nar", targetDate: "20260628" }, [{ id: 5 }]);
  vi.setSystemTime(new Date(baseDate.getTime() + 60 * 60 * 1000 - 1));
  expect(getCachedDailyPgRows({ source: "nar", targetDate: "20260628" })).toStrictEqual([
    { id: 5 },
  ]);
});

test("invalidateDailyPgCache removes only the targeted entry", () => {
  setCachedDailyPgRows({ source: "jra", targetDate: "20260628" }, [{ id: 1 }]);
  setCachedDailyPgRows({ source: "nar", targetDate: "20260628" }, [{ id: 2 }]);
  invalidateDailyPgCache({ source: "jra", targetDate: "20260628" });
  expect(getCachedDailyPgRows({ source: "jra", targetDate: "20260628" })).toBe(null);
  expect(getCachedDailyPgRows({ source: "nar", targetDate: "20260628" })).toStrictEqual([
    { id: 2 },
  ]);
});

test("clearDailyPgCache empties the entire store", () => {
  setCachedDailyPgRows({ source: "jra", targetDate: "20260628" }, [{ id: 1 }]);
  setCachedDailyPgRows({ source: "nar", targetDate: "20260628" }, [{ id: 2 }]);
  clearDailyPgCache();
  expect(getDailyPgCacheSize()).toBe(0);
});

test("overwrites prior entry on repeated set for the same key", () => {
  setCachedDailyPgRows({ source: "jra", targetDate: "20260628" }, [{ id: 1 }]);
  setCachedDailyPgRows({ source: "jra", targetDate: "20260628" }, [{ id: 2 }, { id: 3 }]);
  expect(getCachedDailyPgRows({ source: "jra", targetDate: "20260628" })).toStrictEqual([
    { id: 2 },
    { id: 3 },
  ]);
});

test("getDailyPgCacheSize reports the number of distinct entries", () => {
  expect(getDailyPgCacheSize()).toBe(0);
  setCachedDailyPgRows({ source: "jra", targetDate: "20260628" }, [{ id: 1 }]);
  setCachedDailyPgRows({ source: "nar", targetDate: "20260628" }, [{ id: 2 }]);
  expect(getDailyPgCacheSize()).toBe(2);
});

test("expired entry is purged on read so size decreases", () => {
  const baseDate = new Date("2026-06-28T00:00:00Z");
  vi.setSystemTime(baseDate);
  setCachedDailyPgRows({ source: "jra", targetDate: "20260628" }, [{ id: 1 }]);
  vi.setSystemTime(new Date(baseDate.getTime() + 60 * 60 * 1000 + 1));
  getCachedDailyPgRows({ source: "jra", targetDate: "20260628" });
  expect(getDailyPgCacheSize()).toBe(0);
});
