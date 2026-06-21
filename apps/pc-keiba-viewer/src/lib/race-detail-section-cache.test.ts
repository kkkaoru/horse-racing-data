import { describe, expect, it } from "vitest";

import {
  buildDetailSectionApiPath,
  buildDetailSectionCacheKey,
  DEFAULT_RACE_DETAIL_CACHE_WARM_SECTIONS,
  DETAIL_SECTION_CACHEABLE_SECTIONS,
  getJstDateParts,
  getTomorrowJstDateParts,
  isDefaultDetailSectionCacheRequest,
  isDetailSectionCacheableSection,
  parseIsoDateParts,
  stripDetailSectionCacheWarmParams,
} from "./race-detail-section-cache";

describe("race detail section cache helpers", () => {
  const warmMessage = {
    day: "23",
    keibajoCode: "44",
    month: "5",
    raceNumber: "12",
    section: "pace-prediction" as const,
    source: "nar" as const,
    year: "2026",
  };

  it("builds cache keys and API paths for detail sections", () => {
    expect(buildDetailSectionCacheKey(warmMessage)).toBe(
      "race-detail-section:v2:2026:5:23:44:12:pace-prediction:default",
    );
    expect(buildDetailSectionApiPath(warmMessage)).toBe(
      "/api/races/2026/5/23/44/12/sections/pace-prediction",
    );
    expect(buildDetailSectionCacheKey({ ...warmMessage, section: "premium-data-top" })).toContain(
      ":v2:",
    );
  });

  it("validates cacheable sections and default warm targets", () => {
    expect(isDetailSectionCacheableSection("results")).toBe(true);
    expect(isDetailSectionCacheableSection("unknown")).toBe(false);
    expect(DETAIL_SECTION_CACHEABLE_SECTIONS).toContain("pace-prediction");
    expect(DEFAULT_RACE_DETAIL_CACHE_WARM_SECTIONS).toContain("condition");
  });

  it("detects default cacheable requests without extra query params", () => {
    const params = new URLSearchParams("__cacheWarm=1");
    expect(stripDetailSectionCacheWarmParams(params).toString()).toBe("");
    expect(isDefaultDetailSectionCacheRequest("results", new URLSearchParams())).toBe(true);
    expect(isDefaultDetailSectionCacheRequest("results", new URLSearchParams("foo=1"))).toBe(false);
    expect(isDefaultDetailSectionCacheRequest("unknown", new URLSearchParams())).toBe(false);
  });

  it("parses JST date parts and ISO dates", () => {
    expect(getJstDateParts(new Date("2026-05-23T12:00:00+09:00"))).toEqual({
      day: "23",
      month: "05",
      year: "2026",
    });
    expect(getTomorrowJstDateParts(new Date("2026-05-23T12:00:00+09:00"))).toEqual({
      day: "24",
      month: "05",
      year: "2026",
    });
    expect(parseIsoDateParts("2026-05-23")).toEqual({
      day: "23",
      month: "05",
      year: "2026",
    });
    expect(parseIsoDateParts("invalid")).toBeNull();
  });
});

it("strips __predictionRefresh from search params alongside __cacheWarm", () => {
  const params = new URLSearchParams("__predictionRefresh=1&__cacheWarm=1");
  expect(stripDetailSectionCacheWarmParams(params).toString()).toBe("");
});

it("keeps unrelated params while stripping __predictionRefresh", () => {
  const params = new URLSearchParams("foo=bar&__predictionRefresh=1");
  expect(stripDetailSectionCacheWarmParams(params).toString()).toBe("foo=bar");
});
