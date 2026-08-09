// Run with bun.
import { expect, it } from "vitest";
import {
  buildRunningStylePredictionKvKey,
  getPredictionKvTtlSeconds,
  raceYmdFromRunningStyle,
  resolvePredictionCacheWindow,
  toJstYmd,
} from "./prediction-kv-keys";

const NOON_JST_MS = Date.parse("2026-08-09T12:00:00+09:00");

it("toJstYmd formats a UTC instant in Asia/Tokyo", () => {
  expect(toJstYmd(new Date("2026-08-08T16:00:00.000Z"))).toBe("20260809");
  expect(toJstYmd(new Date("2026-08-08T14:59:59.000Z"))).toBe("20260808");
});

it("resolvePredictionCacheWindow classifies the 3-day JST window", () => {
  expect(resolvePredictionCacheWindow("20260808", NOON_JST_MS)).toBe("yesterday");
  expect(resolvePredictionCacheWindow("20260809", NOON_JST_MS)).toBe("today");
  expect(resolvePredictionCacheWindow("20260810", NOON_JST_MS)).toBe("tomorrow");
  expect(resolvePredictionCacheWindow("20260807", NOON_JST_MS)).toBe("outside");
  expect(resolvePredictionCacheWindow("2026-08-09", NOON_JST_MS)).toBe("outside");
  expect(resolvePredictionCacheWindow("20261399", NOON_JST_MS)).toBe("outside");
});

it("getPredictionKvTtlSeconds matches viewer today/adjacent/outside TTLs", () => {
  expect(getPredictionKvTtlSeconds("today")).toBe(129600);
  expect(getPredictionKvTtlSeconds("tomorrow")).toBe(86400);
  expect(getPredictionKvTtlSeconds("yesterday")).toBe(86400);
  expect(getPredictionKvTtlSeconds("outside")).toBe(0);
});

it("buildRunningStylePredictionKvKey zero-pads venue and race number", () => {
  expect(
    buildRunningStylePredictionKvKey({
      keibajoCode: "5",
      mmdd: "809",
      raceBango: "1",
      source: "jra",
      year: "2026",
    }),
  ).toBe("pred:rs:v1:jra:20260809:05:01");
});

it("raceYmdFromRunningStyle zero-pads mmdd", () => {
  expect(raceYmdFromRunningStyle("2026", "809")).toBe("20260809");
});
