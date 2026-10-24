// Run with bun.
import { describe, expect, it } from "vitest";

import {
  buildDetailSectionApiPath,
  buildDetailSectionCacheKey,
  DEFAULT_RACE_DETAIL_CACHE_WARM_SECTIONS,
  DETAIL_SECTION_CACHEABLE_SECTIONS,
  getLegacyCacheKey,
  getJstDateParts,
  getTomorrowJstDateParts,
  isDefaultDetailSectionCacheRequest,
  isDetailSectionCacheableSection,
  parseIsoDateParts,
  isGenerationalCacheKey,
  stripDetailSectionCacheWarmParams,
  withCacheGeneration,
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
      "race-detail-section:v4:2026:5:23:44:12:pace-prediction:default",
    );
    expect(buildDetailSectionApiPath(warmMessage)).toBe(
      "/api/races/2026/5/23/44/12/sections/pace-prediction",
    );
    expect(buildDetailSectionCacheKey({ ...warmMessage, section: "premium-data-top" })).toContain(
      ":v2:",
    );
    expect(buildDetailSectionCacheKey({ ...warmMessage, section: "training" })).toBe(
      "race-detail-section:v6:2026:5:23:44:12:training:default",
    );
  });

  it("uses new overseas history keys without invalidating domestic sections", () => {
    expect(
      buildDetailSectionCacheKey({
        ...warmMessage,
        keibajoCode: "A8",
        section: "results",
      }),
    ).toBe("race-detail-section:v8:2026:5:23:A8:12:results:default");
    expect(
      buildDetailSectionCacheKey({
        ...warmMessage,
        keibajoCode: "A8",
        section: "time-score",
      }),
    ).toBe("race-detail-section:v12-overseas-readers:2026:5:23:A8:12:time-score:default");
    expect(
      buildDetailSectionCacheKey({
        ...warmMessage,
        keibajoCode: "A8",
        section: "bloodline",
      }),
    ).toBe("race-detail-section:v8:2026:5:23:A8:12:bloodline:default");
    expect(
      buildDetailSectionCacheKey({
        ...warmMessage,
        keibajoCode: "A8",
        section: "similar",
      }),
    ).toBe("race-detail-section:v12-overseas-readers:2026:5:23:A8:12:similar:default");
    expect(
      buildDetailSectionCacheKey({
        ...warmMessage,
        keibajoCode: "44",
        section: "bloodline",
      }),
    ).toBe("race-detail-section:v10:2026:5:23:44:12:bloodline:default");
    expect(
      buildDetailSectionCacheKey({
        ...warmMessage,
        keibajoCode: "44",
        section: "overall-score",
      }),
    ).toBe("race-detail-section:v11:2026:5:23:44:12:overall-score:default");
    expect(
      buildDetailSectionCacheKey({
        ...warmMessage,
        keibajoCode: "44",
        section: "similar",
      }),
    ).toBe("race-detail-section:v10:2026:5:23:44:12:similar:default");
    expect(
      buildDetailSectionCacheKey({
        ...warmMessage,
        keibajoCode: "44",
        section: "time-score",
      }),
    ).toBe("race-detail-section:v11:2026:5:23:44:12:time-score:default");
    expect(
      buildDetailSectionCacheKey({
        ...warmMessage,
        keibajoCode: "A8",
        section: "condition",
      }),
    ).toBe("race-detail-section:v19:2026:5:23:A8:12:condition:default");
    expect(
      getLegacyCacheKey("race-detail-section:v19:2026:08:29:04:08:condition:default"),
    ).toBeNull();
    expect(
      getLegacyCacheKey("race-detail-section:v6:2026:08:29:04:08:training:default"),
    ).toBeNull();
  });

  it("isolates domestic and overseas overall scores without stale fallback", () => {
    expect(
      buildDetailSectionCacheKey({ ...warmMessage, keibajoCode: "05", section: "overall-score" }),
    ).toBe("race-detail-section:v11:2026:5:23:05:12:overall-score:default");
    expect(
      buildDetailSectionCacheKey({ ...warmMessage, keibajoCode: "A8", section: "overall-score" }),
    ).toBe("race-detail-section:v11:2026:5:23:A8:12:overall-score:default");
    expect(
      getLegacyCacheKey("race-detail-section:v11:2026:5:23:A8:12:overall-score:default"),
    ).toBeNull();
  });

  it("does not read old numerical generations on a condition cache miss", () => {
    expect(
      buildDetailSectionCacheKey({ ...warmMessage, keibajoCode: "05", section: "condition" }),
    ).toBe("race-detail-section:v19:2026:5:23:05:12:condition:default");
    expect(
      getLegacyCacheKey("race-detail-section:v19:2026:5:23:05:12:condition:default"),
    ).toBeNull();
    expect(
      getLegacyCacheKey("race-detail-section:v17:2026:5:23:05:12:condition:default"),
    ).toBeNull();
  });

  it("isolates JRA and Ban-ei time-score caches without stale fallback", () => {
    expect(
      buildDetailSectionCacheKey({ ...warmMessage, keibajoCode: "05", section: "time-score" }),
    ).toBe("race-detail-section:v11:2026:5:23:05:12:time-score:default");
    expect(
      buildDetailSectionCacheKey({ ...warmMessage, keibajoCode: "83", section: "time-score" }),
    ).toBe("race-detail-section:v11:2026:5:23:83:12:time-score:default");
    expect(
      getLegacyCacheKey("race-detail-section:v11:2026:5:23:83:12:time-score:default"),
    ).toBeNull();
  });

  it("validates cacheable sections and default warm targets", () => {
    expect(isDetailSectionCacheableSection("results")).toBe(true);
    expect(isDetailSectionCacheableSection("unknown")).toBe(false);
    expect(isDetailSectionCacheableSection("win-rate-heatmap")).toBe(false);
    expect([...DETAIL_SECTION_CACHEABLE_SECTIONS]).toStrictEqual([
      "ability",
      "bloodline",
      "condition",
      "premium-data-top",
      "overall-score",
      "pace-prediction",
      "results",
      "similar",
      "time-score",
      "training",
    ]);
    expect([...DEFAULT_RACE_DETAIL_CACHE_WARM_SECTIONS]).toStrictEqual([
      "time-score",
      "results",
      "training",
      "condition",
      "similar",
      "bloodline",
      "overall-score",
    ]);
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

it("invalidates only the exact Arc results cache without reading partial fallback", () => {
  const input = {
    year: "2026",
    month: "10",
    day: "04",
    keibajoCode: "A8",
    raceNumber: "05",
    section: "results",
  } satisfies Parameters<typeof buildDetailSectionCacheKey>[0];
  expect(buildDetailSectionCacheKey(input)).toBe(
    "race-detail-section:v9-arc-complete-results:2026:10:04:A8:05:results:default",
  );
  expect(buildDetailSectionCacheKey({ ...input, raceNumber: "06" })).toBe(
    "race-detail-section:v8:2026:10:04:A8:06:results:default",
  );
  expect(buildDetailSectionCacheKey({ ...input, section: "pace-prediction" })).toBe(
    "race-detail-section:v4:2026:10:04:A8:05:pace-prediction:default",
  );
});

it("withCacheGeneration appends the race generation as the last key segment", () => {
  expect(
    withCacheGeneration("race-detail-section:v4:2026:5:23:44:12:results:default", "1761300000000"),
  ).toBe("race-detail-section:v4:2026:5:23:44:12:results:default:g1761300000000");
});

it("getLegacyCacheKey maps a never-busted generation to the pre-generation key", () => {
  expect(getLegacyCacheKey("race-detail-section:v4:2026:5:23:44:12:results:default:g0")).toBe(
    "race-detail-section:v4:2026:5:23:44:12:results:default",
  );
});

it("getLegacyCacheKey returns null once the race has been busted", () => {
  expect(
    getLegacyCacheKey("race-detail-section:v4:2026:5:23:44:12:results:default:g1761300000000"),
  ).toBeNull();
});

it("getLegacyCacheKey returns null for a generation ending in zero", () => {
  expect(getLegacyCacheKey("race-detail-ssr:v4:jra:2026:05:23:05:12:g1761300000010")).toBeNull();
});

it("isGenerationalCacheKey tells generational keys from legacy keys", () => {
  expect(isGenerationalCacheKey("race-detail-section:v4:2026:5:23:44:12:results:default:g0")).toBe(
    true,
  );
  expect(isGenerationalCacheKey("race-detail-section:v4:2026:5:23:44:12:results:default")).toBe(
    false,
  );
});
