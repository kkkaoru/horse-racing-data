// Run with bun.
import { expect, test } from "vitest";
import {
  PREDICTION_CACHE_BUST_INTERNAL_PATH,
  buildFinishPositionPredictionKvKey,
  buildRunningStylePredictionKvKey,
  getPredictionKvTtlSeconds,
  isPredictionCacheEligibleYmd,
  raceYmdFromParts,
  resolvePredictionCacheWindow,
  toJstYmd,
} from "./prediction-kv-keys";

const NOON_JST_MS = Date.parse("2026-08-09T12:00:00+09:00");

test("toJstYmd formats a UTC instant in Asia/Tokyo", () => {
  expect(toJstYmd(new Date("2026-08-08T16:00:00.000Z"))).toBe("20260809");
  expect(toJstYmd(new Date("2026-08-08T14:59:59.000Z"))).toBe("20260808");
});

test("resolvePredictionCacheWindow classifies the 3-day JST window", () => {
  expect(resolvePredictionCacheWindow("20260808", NOON_JST_MS)).toBe("yesterday");
  expect(resolvePredictionCacheWindow("20260809", NOON_JST_MS)).toBe("today");
  expect(resolvePredictionCacheWindow("20260810", NOON_JST_MS)).toBe("tomorrow");
  expect(resolvePredictionCacheWindow("20260807", NOON_JST_MS)).toBe("outside");
  expect(resolvePredictionCacheWindow("20260811", NOON_JST_MS)).toBe("outside");
  expect(resolvePredictionCacheWindow("2026-08-09", NOON_JST_MS)).toBe("outside");
  expect(resolvePredictionCacheWindow("20261399", NOON_JST_MS)).toBe("outside");
});

test("isPredictionCacheEligibleYmd is true only inside the 3-day window", () => {
  expect(isPredictionCacheEligibleYmd("20260808", NOON_JST_MS)).toBe(true);
  expect(isPredictionCacheEligibleYmd("20260809", NOON_JST_MS)).toBe(true);
  expect(isPredictionCacheEligibleYmd("20260810", NOON_JST_MS)).toBe(true);
  expect(isPredictionCacheEligibleYmd("20260811", NOON_JST_MS)).toBe(false);
});

test("getPredictionKvTtlSeconds matches viewer today/adjacent/outside TTLs", () => {
  expect(getPredictionKvTtlSeconds("today")).toBe(129600);
  expect(getPredictionKvTtlSeconds("tomorrow")).toBe(86400);
  expect(getPredictionKvTtlSeconds("yesterday")).toBe(86400);
  expect(getPredictionKvTtlSeconds("outside")).toBe(0);
});

test("buildFinishPositionPredictionKvKey zero-pads venue and race number", () => {
  expect(
    buildFinishPositionPredictionKvKey({
      keibajoCode: "5",
      mmdd: "809",
      raceBango: "1",
      year: "2026",
    }),
  ).toBe("pred:fp:v1:20260809:05:01");
});

test("buildRunningStylePredictionKvKey includes source", () => {
  expect(
    buildRunningStylePredictionKvKey({
      keibajoCode: "83",
      mmdd: "0809",
      raceBango: "12",
      source: "nar",
      year: "2026",
    }),
  ).toBe("pred:rs:v1:nar:20260809:83:12");
});

test("raceYmdFromParts zero-pads mmdd", () => {
  expect(raceYmdFromParts("2026", "809")).toBe("20260809");
});

test("PREDICTION_CACHE_BUST_INTERNAL_PATH matches the viewer internal route", () => {
  expect(PREDICTION_CACHE_BUST_INTERNAL_PATH).toBe("/api/internal/prediction-cache-bust");
});
