// Run with: bun run --filter pc-keiba-viewer test src/lib/race-trend-live-backoff.test.ts

import { expect, test } from "vitest";

import {
  computeRaceTrendLiveBackoffMs,
  isRaceTrendLiveReconnectExhausted,
  RACE_TREND_LIVE_BACKOFF_BASE_MS,
  RACE_TREND_LIVE_BACKOFF_CAP_MS,
  RACE_TREND_LIVE_MAX_RECONNECT_ATTEMPTS,
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

test("RACE_TREND_LIVE_MAX_RECONNECT_ATTEMPTS exposes the circuit breaker cap", () => {
  expect(RACE_TREND_LIVE_MAX_RECONNECT_ATTEMPTS).toBe(5);
});

test("isRaceTrendLiveReconnectExhausted returns false when the attempt count is below the cap", () => {
  expect(isRaceTrendLiveReconnectExhausted(0)).toBe(false);
});

test("isRaceTrendLiveReconnectExhausted returns false when the attempt count is one below the cap", () => {
  expect(isRaceTrendLiveReconnectExhausted(4)).toBe(false);
});

test("isRaceTrendLiveReconnectExhausted returns true when the attempt count reaches the cap", () => {
  expect(isRaceTrendLiveReconnectExhausted(5)).toBe(true);
});

test("isRaceTrendLiveReconnectExhausted returns true when the attempt count exceeds the cap", () => {
  expect(isRaceTrendLiveReconnectExhausted(10)).toBe(true);
});

test("isRaceTrendLiveReconnectExhausted floors fractional attempt counts to compare with the cap", () => {
  expect(isRaceTrendLiveReconnectExhausted(4.9)).toBe(false);
});

test("isRaceTrendLiveReconnectExhausted returns false for negative attempt counts", () => {
  expect(isRaceTrendLiveReconnectExhausted(-1)).toBe(false);
});

test("isRaceTrendLiveReconnectExhausted returns false for NaN attempt counts", () => {
  expect(isRaceTrendLiveReconnectExhausted(Number.NaN)).toBe(false);
});

test("isRaceTrendLiveReconnectExhausted returns true for positive Infinity attempt counts", () => {
  expect(isRaceTrendLiveReconnectExhausted(Number.POSITIVE_INFINITY)).toBe(true);
});

// F7 BUG-1 SSR-safety: the backoff and circuit-breaker helpers are pure and
// must produce identical results whether or not a browser-only global is
// present. The render path on the server cannot rely on `window`, so any
// helper indirectly referenced from a useMemo/useState initializer also has
// to behave identically with `window === undefined`.

test("computeRaceTrendLiveBackoffMs returns the same delay whether window is defined or undefined", () => {
  const csrResult = computeRaceTrendLiveBackoffMs(3);
  const stash = { window: globalThis.window };
  Reflect.set(globalThis, "window", undefined);
  const ssrResult = computeRaceTrendLiveBackoffMs(3);
  Reflect.set(globalThis, "window", stash.window);
  expect(ssrResult).toBe(csrResult);
});

test("isRaceTrendLiveReconnectExhausted returns the same verdict whether window is defined or undefined", () => {
  const csrResult = isRaceTrendLiveReconnectExhausted(5);
  const stash = { window: globalThis.window };
  Reflect.set(globalThis, "window", undefined);
  const ssrResult = isRaceTrendLiveReconnectExhausted(5);
  Reflect.set(globalThis, "window", stash.window);
  expect(ssrResult).toBe(csrResult);
});
