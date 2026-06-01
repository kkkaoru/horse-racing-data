import { describe, expect, it } from "vitest";

import {
  buildProductionRunningStylesPath,
  buildRaceKey,
  buildRunningStyleCacheRequest,
  getRunningStyleCacheTtlSeconds,
  parseRaceDayFromRunningStyleRaceKey,
  parseRunningStyleRaceKey,
} from "./running-style-cache";

const sampleRace = {
  kaisaiNen: "2026",
  kaisaiTsukihi: "0523",
  keibajoCode: "04",
  raceBango: "03",
  source: "jra",
};

describe("running style cache helpers", () => {
  it("parses a running-style race key", () => {
    const raceKey = buildRaceKey(sampleRace);
    expect(raceKey).toBe("jra:2026:0523:04:03");
    expect(parseRunningStyleRaceKey(raceKey)).toEqual({
      kaisaiNen: "2026",
      kaisaiTsukihi: "0523",
      keibajoCode: "04",
      raceBango: "03",
      source: "jra",
    });
    expect(parseRunningStyleRaceKey("invalid")).toBeNull();
    expect(parseRaceDayFromRunningStyleRaceKey(raceKey)).toEqual({
      kaisaiNen: "2026",
      kaisaiTsukihi: "0523",
    });
    expect(parseRaceDayFromRunningStyleRaceKey("invalid")).toBeUndefined();
  });

  it("builds the URL cache request used by sync-realtime-data prewarm", () => {
    const request = buildRunningStyleCacheRequest(sampleRace, "https://viewer.example.com");
    const url = new URL(request.url);
    expect(url.origin).toBe("https://viewer.example.com");
    expect(url.pathname).toBe("/api/races/2026/05/23/04/03/running-styles");
    expect(url.searchParams.get("source")).toBe("jra");
    expect(url.searchParams.get("__runningStyleCache")).toBe("v3");
  });

  it("builds production proxy paths without leading zeros in month/day", () => {
    expect(buildProductionRunningStylesPath(sampleRace)).toBe(
      "/api/races/2026/5/23/04/03/running-styles?source=jra",
    );
  });

  it("expires running-style URL cache at end of race day", () => {
    const beforeEnd = Date.parse("2026-05-23T20:00:00+09:00");
    const afterEnd = Date.parse("2026-05-24T00:00:00+09:00");
    expect(getRunningStyleCacheTtlSeconds(sampleRace, beforeEnd)).toBeGreaterThan(0);
    expect(getRunningStyleCacheTtlSeconds(sampleRace, afterEnd)).toBe(0);
    expect(
      getRunningStyleCacheTtlSeconds({ kaisaiNen: "bad", kaisaiTsukihi: "0000" }, beforeEnd),
    ).toBe(0);
  });
});
