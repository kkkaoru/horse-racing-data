// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import {
  WIN_RATE_HEATMAP_CACHE_NAMESPACE,
  WIN_RATE_HEATMAP_CACHE_TTL_SECONDS,
  WIN_RATE_HEATMAP_CACHE_URL_BASE,
  buildWinRateHeatmapCacheKey,
  createWinRateHeatmapCacheRequest,
  isWinRateHeatmapSectionPayload,
  serializeWinRateHeatmapCacheQuery,
} from "./win-rate-heatmap-cache";

it("uses heatmap cache namespace v10", () => {
  expect(WIN_RATE_HEATMAP_CACHE_NAMESPACE).toBe("pc-keiba-viewer:win-rate-heatmap:v10");
});

it("uses a 36 hour heatmap cache TTL", () => {
  expect(WIN_RATE_HEATMAP_CACHE_TTL_SECONDS).toBe(129600);
});

it("builds a per-race heatmap cache key with a default query token", () => {
  expect(
    buildWinRateHeatmapCacheKey({
      day: "21",
      keibajoCode: "05",
      month: "8",
      query: "",
      raceNumber: "1",
      year: "2026",
    }),
  ).toBe(`${WIN_RATE_HEATMAP_CACHE_NAMESPACE}:2026:08:21:05:01:default`);
});

it("keeps an explicit query fingerprint on the heatmap cache key", () => {
  expect(
    buildWinRateHeatmapCacheKey({
      day: "21",
      keibajoCode: "50",
      month: "08",
      query: "statsVenue=1",
      raceNumber: "12",
      year: "2026",
    }),
  ).toBe(`${WIN_RATE_HEATMAP_CACHE_NAMESPACE}:2026:08:21:50:12:statsVenue=1`);
});

it("serializes empty search params as the default heatmap query token", () => {
  expect(serializeWinRateHeatmapCacheQuery(new URLSearchParams())).toBe("default");
});

it("sorts heatmap query params so cache keys stay stable", () => {
  expect(serializeWinRateHeatmapCacheQuery(new URLSearchParams("b=2&a=1"))).toBe("a=1&b=2");
  expect(serializeWinRateHeatmapCacheQuery(new URLSearchParams("a=2&a=1"))).toBe("a=1&a=2");
});

it("builds a Cache API request URL under the heatmap namespace", () => {
  expect(createWinRateHeatmapCacheRequest("heatmap-key").url).toBe(
    `${WIN_RATE_HEATMAP_CACHE_URL_BASE}heatmap-key`,
  );
});

it("accepts a complete win-rate heatmap section payload", () => {
  expect(
    isWinRateHeatmapSectionPayload({
      bloodlineRows: [],
      carriedWeightClassStats: [],
      frameStats: [],
      horseResults: [],
      runners: [],
      similarRows: [],
      type: "win-rate-heatmap",
      weightClassStats: [],
    }),
  ).toBe(true);
});

it("rejects incomplete or mistyped heatmap payloads", () => {
  expect(isWinRateHeatmapSectionPayload(null)).toBe(false);
  expect(isWinRateHeatmapSectionPayload("win-rate-heatmap")).toBe(false);
  expect(isWinRateHeatmapSectionPayload({ type: "condition" })).toBe(false);
  expect(
    isWinRateHeatmapSectionPayload({
      bloodlineRows: [],
      carriedWeightClassStats: [],
      frameStats: [],
      horseResults: [],
      runners: [],
      similarRows: [],
      type: "win-rate-heatmap",
    }),
  ).toBe(false);
});
