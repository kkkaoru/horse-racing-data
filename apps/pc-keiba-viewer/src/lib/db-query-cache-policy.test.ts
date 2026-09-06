// Run with bun; tests are executed by Vitest.
import { expect, test } from "vitest";

import { getDbQueryCachePolicy } from "./db-query-cache-policy";

test("bounds day-page cache lifetime and separates it from the old hour-long cache", () => {
  expect(
    getDbQueryCachePolicy({
      keyParts: ["getRacesByDate", "2026", "09", "06"],
      nowMs: 59999,
      ttlSeconds: 3600,
    }),
  ).toStrictEqual({
    keyParts: ["getRacesByDate", "2026", "09", "06", "race-list-minute-v1", 0],
    ttlSeconds: 60,
  });
});

test("uses a new edge and KV key at the next minute for newly synchronized races", () => {
  expect(
    getDbQueryCachePolicy({
      keyParts: ["getRacesByDate", "2026", "09", "06"],
      nowMs: 60000,
      ttlSeconds: 3600,
    }),
  ).toStrictEqual({
    keyParts: ["getRacesByDate", "2026", "09", "06", "race-list-minute-v1", 1],
    ttlSeconds: 60,
  });
});

test("also refreshes the lightweight list without increasing a shorter configured TTL", () => {
  expect(
    getDbQueryCachePolicy({
      keyParts: ["getRacesByDateWithoutJockeyNames", "2026", "09", "06"],
      nowMs: 60001,
      ttlSeconds: 20,
    }),
  ).toStrictEqual({
    keyParts: ["getRacesByDateWithoutJockeyNames", "2026", "09", "06", "race-list-minute-v1", 1],
    ttlSeconds: 20,
  });
});

test("preserves expensive detail query cache keys and lifetime", () => {
  expect(
    getDbQueryCachePolicy({
      keyParts: ["getRaceRunners", "2026", "09", "06", "01", "01"],
      nowMs: 60000,
      ttlSeconds: 3600,
    }),
  ).toStrictEqual({
    keyParts: ["getRaceRunners", "2026", "09", "06", "01", "01"],
    ttlSeconds: 3600,
  });
});
