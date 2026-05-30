// Run with: bun run --filter pc-keiba-viewer test src/lib/race-trend-live-backoff.test.ts

import { expect, test } from "vitest";

import {
  computeRaceTrendLiveBackoffMs,
  RACE_TREND_LIVE_BACKOFF_BASE_MS,
  RACE_TREND_LIVE_BACKOFF_CAP_MS,
} from "./race-trend-live-backoff";

test("computeRaceTrendLiveBackoffMs returns base delay for the first attempt (attempt=0)", () => {
  expect(computeRaceTrendLiveBackoffMs(0)).toBe(1000);
});

test("computeRaceTrendLiveBackoffMs doubles the delay for each subsequent attempt", () => {
  expect(computeRaceTrendLiveBackoffMs(1)).toBe(2000);
});

test("computeRaceTrendLiveBackoffMs doubles the delay at the second retry attempt", () => {
  expect(computeRaceTrendLiveBackoffMs(2)).toBe(4000);
});

test("computeRaceTrendLiveBackoffMs caps at 30_000ms when the exponential exceeds the cap", () => {
  expect(computeRaceTrendLiveBackoffMs(20)).toBe(30000);
});

test("computeRaceTrendLiveBackoffMs treats a negative attempt as the base delay", () => {
  expect(computeRaceTrendLiveBackoffMs(-1)).toBe(1000);
});

test("computeRaceTrendLiveBackoffMs treats NaN as the base delay", () => {
  expect(computeRaceTrendLiveBackoffMs(Number.NaN)).toBe(1000);
});

test("computeRaceTrendLiveBackoffMs treats positive Infinity as the cap delay", () => {
  expect(computeRaceTrendLiveBackoffMs(Number.POSITIVE_INFINITY)).toBe(30000);
});

test("computeRaceTrendLiveBackoffMs floors fractional attempt counts", () => {
  expect(computeRaceTrendLiveBackoffMs(1.9)).toBe(2000);
});

test("RACE_TREND_LIVE_BACKOFF_BASE_MS exposes the base delay constant", () => {
  expect(RACE_TREND_LIVE_BACKOFF_BASE_MS).toBe(1000);
});

test("RACE_TREND_LIVE_BACKOFF_CAP_MS exposes the cap delay constant", () => {
  expect(RACE_TREND_LIVE_BACKOFF_CAP_MS).toBe(30000);
});
