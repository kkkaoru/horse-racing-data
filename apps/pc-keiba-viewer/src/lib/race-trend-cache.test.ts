import { describe, expect, it } from "vitest";

import {
  buildDefaultRaceTrendCacheOptions,
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
        {
          ...targetRace,
          hassoJikoku: "2015",
          raceBango: "11",
        },
        targetRace,
      ),
    ).toBe(true);
    expect(isRaceBeforeTargetRace(targetRace, targetRace)).toBe(false);
    expect(
      isRaceBeforeTargetRace(
        {
          ...targetRace,
          hassoJikoku: "2120",
          raceBango: "13",
        },
        targetRace,
      ),
    ).toBe(false);
  });

  it("uses start time to compare same-day races across venues", () => {
    expect(
      isRaceBeforeTargetRace(
        {
          ...targetRace,
          hassoJikoku: "2030",
          keibajoCode: "45",
          raceBango: "10",
        },
        targetRace,
      ),
    ).toBe(true);
    expect(
      isRaceBeforeTargetRace(
        {
          ...targetRace,
          hassoJikoku: "2100",
          keibajoCode: "45",
          raceBango: "10",
        },
        targetRace,
      ),
    ).toBe(false);
  });

  it("builds default realtime trend options for the target race date", () => {
    expect(buildDefaultRaceTrendCacheOptions("nar", "20260520")).toMatchObject({
      frameEndYmd: "20260520",
      frameStartYmd: "20260517",
      includeRealtimeResults: true,
      jockeyEndYmd: "20260520",
      jockeySameVenue: true,
      jockeyStartYmd: "20260517",
      runningStyleIgnoreRaceNumber: true,
      source: "nar",
    });
  });

  it("expires trend cache after the configured post-time window", () => {
    const startTime = getRaceStartTimeMs(targetRace);
    expect(startTime).not.toBeNull();
    expect(getRaceTrendCacheTtlSeconds(targetRace, 600, startTime! - 60_000)).toBe(660);
    expect(getRaceTrendCacheTtlSeconds(targetRace, 600, startTime! + 601_000)).toBe(0);
  });
});
