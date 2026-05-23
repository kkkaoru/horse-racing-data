import { describe, expect, it } from "vitest";

import {
  buildD1QueryCacheKey,
  createD1QueryCacheRequest,
  resolveD1QueryCacheTtlSeconds,
} from "../lib/d1-query-cache";

describe("d1 query cache helpers", () => {
  it("builds stable cache keys regardless of object key order", () => {
    const left = buildD1QueryCacheKey("running-style-race", [
      "getRaceRunningStylesFromD1",
      "nar:20260523:44:12",
    ]);
    const right = buildD1QueryCacheKey("running-style-race", [
      "getRaceRunningStylesFromD1",
      "nar:20260523:44:12",
    ]);
    expect(left).toBe(right);
    expect(left).toMatch(/^[0-9a-f]{8}$/u);
    expect(buildD1QueryCacheKey("running-style-races", [{ z: 1, a: 2 }])).toBe(
      buildD1QueryCacheKey("running-style-races", [{ a: 2, z: 1 }]),
    );
  });

  it("expires running-style caches at end of race day in JST", () => {
    const raceDay = { kaisaiNen: "2026", kaisaiTsukihi: "0523" };
    const beforeEnd = Date.parse("2026-05-23T20:00:00+09:00");
    const afterEnd = Date.parse("2026-05-24T00:00:00+09:00");
    expect(resolveD1QueryCacheTtlSeconds("running-style-race", raceDay, beforeEnd)).toBeGreaterThan(
      0,
    );
    expect(resolveD1QueryCacheTtlSeconds("running-style-races", raceDay, beforeEnd)).toBeGreaterThan(
      0,
    );
    expect(resolveD1QueryCacheTtlSeconds("running-style-race", raceDay, afterEnd)).toBe(0);
  });

  it("uses default TTL for non race-day profiles", () => {
    expect(resolveD1QueryCacheTtlSeconds("horse-running-style-history")).toBe(60 * 60);
    expect(resolveD1QueryCacheTtlSeconds("realtime-short")).toBe(60);
    expect(resolveD1QueryCacheTtlSeconds("running-style-race")).toBe(6 * 60 * 60);
  });

  it("returns zero TTL for invalid race-day timestamps", () => {
    expect(
      resolveD1QueryCacheTtlSeconds(
        "running-style-race",
        { kaisaiNen: "bad", kaisaiTsukihi: "0000" },
      ),
    ).toBe(0);
  });
});
