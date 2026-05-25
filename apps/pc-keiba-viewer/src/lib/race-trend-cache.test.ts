// Run with bun (vitest).
import { describe, expect, it } from "vitest";

import {
  RACE_TREND_CACHE_WARM_VARIANT_COUNT,
  addDaysToYmd,
  buildDefaultRaceTrendCacheOptions,
  buildRaceTrendApiPath,
  buildRaceTrendCacheKey,
  buildRaceTrendCacheWarmOptions,
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
      isRaceBeforeTargetRace(
        { ...targetRace, hassoJikoku: "2015", raceBango: "11" },
        targetRace,
      ),
    ).toBe(true);
    expect(isRaceBeforeTargetRace(targetRace, targetRace)).toBe(false);
    expect(
      isRaceBeforeTargetRace(
        { ...targetRace, hassoJikoku: "2120", raceBango: "13" },
        targetRace,
      ),
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

  it("builds default realtime trend options for the target race date", () => {
    expect(buildDefaultRaceTrendCacheOptions("nar", "20260520")).toStrictEqual({
      frameEndYmd: "20260520",
      frameStartYmd: "20260517",
      includeRealtimeResults: true,
      jockeyEndYmd: "20260520",
      jockeyStartYmd: "20260517",
      source: "nar",
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

  it("builds cache keys and warm API paths", () => {
    const options = buildDefaultRaceTrendCacheOptions("jra", "20260520");
    expect(options.frameStartYmd).toBe("20260519");
    expect(addDaysToYmd("20260520", -3)).toBe("20260517");
    const cacheKey = buildRaceTrendCacheKey({ options });
    expect(cacheKey).toStrictEqual("race-trend:v2:jra:20260519:20260520:20260519:20260520:1");
    expect(
      buildRaceTrendApiPath({
        day: "20",
        kind: "race-trend",
        keibajoCode: "05",
        month: "05",
        options,
        raceNumber: "11",
        source: "jra",
        year: "2026",
      }),
    ).toContain("/api/races/2026/05/20/05/11/trends?");
  });

  it("compares races across dates and non-numeric race numbers", () => {
    expect(isRaceBeforeTargetRace({ ...targetRace, source: "jra" }, targetRace)).toBe(false);
    expect(
      isRaceBeforeTargetRace({ ...targetRace, kaisaiTsukihi: "0519" }, targetRace),
    ).toBe(true);
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
