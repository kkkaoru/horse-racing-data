// Run with bun (vitest).
import { describe, expect, it } from "vitest";

import {
  RACE_TREND_CACHE_WARM_VARIANT_COUNT,
  addDaysToYmd,
  buildDefaultRaceTrendCacheOptions,
  buildRaceTrendApiPath,
  buildRaceTrendCacheKey,
  buildRaceTrendCacheWarmOptions,
  buildRaceTrendPast14CacheKey,
  buildRaceTrendTodayCacheKey,
  getRaceStartTimeMs,
  getRaceTrendCacheTtlSeconds,
  isRaceBeforeTargetRace,
} from "./race-trend-cache";

const targetRace = {
  hassoJikoku: "2050",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0520",
  keibajoCode: "44",
  raceBango: "12",
  source: "nar" as const,
};

describe("race trend cache helpers", () => {
  it("allows only races before the target race on the same venue", () => {
    expect(
      isRaceBeforeTargetRace({ ...targetRace, hassoJikoku: "2015", raceBango: "11" }, targetRace),
    ).toBe(true);
    expect(isRaceBeforeTargetRace(targetRace, targetRace)).toBe(false);
    expect(
      isRaceBeforeTargetRace({ ...targetRace, hassoJikoku: "2120", raceBango: "13" }, targetRace),
    ).toBe(false);
  });

  it("uses start time to compare same-day races across venues", () => {
    expect(
      isRaceBeforeTargetRace(
        { ...targetRace, hassoJikoku: "2030", keibajoCode: "45", raceBango: "10" },
        targetRace,
      ),
    ).toBe(true);
    expect(
      isRaceBeforeTargetRace(
        { ...targetRace, hassoJikoku: "2100", keibajoCode: "45", raceBango: "10" },
        targetRace,
      ),
    ).toBe(false);
  });

  it("builds default trend options for an NAR target with a past-14-day window", () => {
    expect(buildDefaultRaceTrendCacheOptions("nar", "20260520")).toStrictEqual({
      frameEndYmd: "20260520",
      frameStartYmd: "20260506",
      includeRealtimeResults: true,
      jockeyEndYmd: "20260520",
      jockeyStartYmd: "20260506",
      source: "nar",
    });
  });

  it("builds default trend options for a JRA target with the same past-14-day window", () => {
    expect(buildDefaultRaceTrendCacheOptions("jra", "20260520")).toStrictEqual({
      frameEndYmd: "20260520",
      frameStartYmd: "20260506",
      includeRealtimeResults: true,
      jockeyEndYmd: "20260520",
      jockeyStartYmd: "20260506",
      source: "jra",
    });
  });

  it("returns a single warm variant", () => {
    const variants = buildRaceTrendCacheWarmOptions("nar", "20260520");
    expect(variants).toHaveLength(RACE_TREND_CACHE_WARM_VARIANT_COUNT);
    expect(variants[0]).toStrictEqual(buildDefaultRaceTrendCacheOptions("nar", "20260520"));
  });

  it("expires trend cache after the configured post-time window", () => {
    const startTime = getRaceStartTimeMs(targetRace);
    expect(startTime).not.toBeNull();
    expect(getRaceTrendCacheTtlSeconds(targetRace, 600, (startTime ?? 0) - 60_000)).toBe(660);
    expect(getRaceTrendCacheTtlSeconds(targetRace, 600, (startTime ?? 0) + 601_000)).toBe(0);
    expect(getRaceTrendCacheTtlSeconds({ ...targetRace, hassoJikoku: null })).toBe(0);
    expect(getRaceStartTimeMs({ ...targetRace, hassoJikoku: "bad" })).toBeNull();
  });

  it("builds the outer race-trend cache key under the v8 namespace", () => {
    const options = buildDefaultRaceTrendCacheOptions("jra", "20260520");
    expect(options.frameStartYmd).toBe("20260506");
    expect(addDaysToYmd("20260520", -14)).toBe("20260506");
    const cacheKey = buildRaceTrendCacheKey({
      keibajoCode: "05",
      options,
      raceBango: "11",
    });
    expect(cacheKey).toStrictEqual("race-trend:v8:jra:05:11:20260506:20260520:20260506:20260520:1");
  });

  it("builds the warm API path under the trends endpoint", () => {
    const options = buildDefaultRaceTrendCacheOptions("jra", "20260520");
    const path = buildRaceTrendApiPath({
      day: "20",
      kind: "race-trend",
      keibajoCode: "05",
      month: "05",
      options,
      raceNumber: "11",
      source: "jra",
      year: "2026",
    });
    const [pathPart] = path.split("?");
    expect(pathPart).toStrictEqual("/api/races/2026/05/20/05/11/trends");
  });

  it("builds the past-14 cache key under the race-trend-past14:v8 namespace for JRA", () => {
    expect(
      buildRaceTrendPast14CacheKey({
        endYmd: "20260519",
        keibajoCode: "05",
        raceBango: "11",
        source: "jra",
        startYmd: "20260506",
      }),
    ).toStrictEqual("race-trend-past14:v8:jra:05:11:20260506:20260519");
  });

  it("builds the past-14 cache key under the race-trend-past14:v8 namespace for NAR", () => {
    expect(
      buildRaceTrendPast14CacheKey({
        endYmd: "20260527",
        keibajoCode: "50",
        raceBango: "07",
        source: "nar",
        startYmd: "20260514",
      }),
    ).toStrictEqual("race-trend-past14:v8:nar:50:07:20260514:20260527");
  });

  it("builds the today cache key under the race-trend-today:v8 namespace for JRA", () => {
    expect(buildRaceTrendTodayCacheKey({ source: "jra", targetYmd: "20260520" })).toStrictEqual(
      "race-trend-today:v8:jra:20260520",
    );
  });

  it("builds the today cache key under the race-trend-today:v8 namespace for NAR", () => {
    expect(buildRaceTrendTodayCacheKey({ source: "nar", targetYmd: "20260528" })).toStrictEqual(
      "race-trend-today:v8:nar:20260528",
    );
  });

  it("compares races across dates and non-numeric race numbers", () => {
    expect(isRaceBeforeTargetRace({ ...targetRace, source: "jra" }, targetRace)).toBe(false);
    expect(isRaceBeforeTargetRace({ ...targetRace, kaisaiTsukihi: "0519" }, targetRace)).toBe(true);
    expect(
      isRaceBeforeTargetRace(
        { ...targetRace, raceBango: "A", keibajoCode: "44" },
        { ...targetRace, raceBango: "B", keibajoCode: "44" },
      ),
    ).toBe(true);
    expect(
      isRaceBeforeTargetRace(
        { ...targetRace, keibajoCode: "45", raceBango: "10" },
        { ...targetRace, keibajoCode: "44", raceBango: "12" },
      ),
    ).toBe(false);
  });
});
