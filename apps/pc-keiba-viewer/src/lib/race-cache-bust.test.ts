// Run with bun: `bun run --filter pc-keiba-viewer test`
import { expect, it } from "vitest";

import {
  RACE_CACHE_BUST_INTERNAL_PATH,
  buildRaceCacheBustKeys,
  buildRaceCacheGenerationKey,
  buildRaceCacheWarmMarkerKey,
  buildRaceDetailSsrCacheKey,
  parseRaceCacheBustRequest,
  parseRaceKey,
} from "./race-cache-bust";

it("RACE_CACHE_BUST_INTERNAL_PATH points at /api/internal/race-cache-bust", () => {
  expect(RACE_CACHE_BUST_INTERNAL_PATH).toBe("/api/internal/race-cache-bust");
});

it("parseRaceCacheBustRequest accepts a well-formed JRA body", () => {
  expect(
    parseRaceCacheBustRequest({
      keibajoCode: "05",
      mmdd: "0628",
      raceBango: "11",
      source: "jra",
      year: "2026",
    }),
  ).toStrictEqual({
    keibajoCode: "05",
    mmdd: "0628",
    raceBango: "11",
    source: "jra",
    year: "2026",
  });
});

it("parseRaceCacheBustRequest accepts an overseas alphanumeric venue", () => {
  expect(
    parseRaceCacheBustRequest({
      keibajoCode: "A8",
      mmdd: "0816",
      raceBango: "04",
      source: "jra",
      year: "2026",
    }),
  ).toStrictEqual({
    keibajoCode: "A8",
    mmdd: "0816",
    raceBango: "04",
    source: "jra",
    year: "2026",
  });
});

it("parseRaceCacheBustRequest validates the narrow training scope", () => {
  const body = {
    keibajoCode: "06",
    mmdd: "0926",
    raceBango: "03",
    source: "jra",
    year: "2026",
  };
  expect(parseRaceCacheBustRequest({ ...body, scope: "training" })).toStrictEqual({
    ...body,
    scope: "training",
  });
  expect(parseRaceCacheBustRequest({ ...body, scope: "unknown" })).toBeNull();
  expect(parseRaceCacheBustRequest({ ...body, scope: null })).toBeNull();
});

it("parseRaceCacheBustRequest rejects null", () => {
  expect(parseRaceCacheBustRequest(null)).toBeNull();
});

it("parseRaceCacheBustRequest rejects a non-object primitive", () => {
  expect(parseRaceCacheBustRequest("string")).toBeNull();
});

it("parseRaceCacheBustRequest rejects an unknown source", () => {
  expect(
    parseRaceCacheBustRequest({
      keibajoCode: "05",
      mmdd: "0628",
      raceBango: "11",
      source: "world",
      year: "2026",
    }),
  ).toBeNull();
});

it("parseRaceCacheBustRequest rejects a non-4-digit year", () => {
  expect(
    parseRaceCacheBustRequest({
      keibajoCode: "05",
      mmdd: "0628",
      raceBango: "11",
      source: "jra",
      year: "26",
    }),
  ).toBeNull();
});

it("parseRaceCacheBustRequest rejects a non-4-digit mmdd", () => {
  expect(
    parseRaceCacheBustRequest({
      keibajoCode: "05",
      mmdd: "628",
      raceBango: "11",
      source: "jra",
      year: "2026",
    }),
  ).toBeNull();
});

it("parseRaceCacheBustRequest rejects a non-2-digit keibajoCode", () => {
  expect(
    parseRaceCacheBustRequest({
      keibajoCode: "5",
      mmdd: "0628",
      raceBango: "11",
      source: "jra",
      year: "2026",
    }),
  ).toBeNull();
});

it("parseRaceCacheBustRequest rejects a non-2-digit raceBango", () => {
  expect(
    parseRaceCacheBustRequest({
      keibajoCode: "05",
      mmdd: "0628",
      raceBango: "1",
      source: "jra",
      year: "2026",
    }),
  ).toBeNull();
});

it("buildRaceCacheGenerationKey produces a per-race key without the source", () => {
  expect(
    buildRaceCacheGenerationKey({
      keibajoCode: "05",
      mmdd: "0628",
      raceBango: "11",
      year: "2026",
    }),
  ).toBe("race-cache:gen:v2:2026:0628:05:11");
});

it("buildRaceCacheBustKeys includes the generation key shape", () => {
  expect(
    buildRaceCacheBustKeys({
      keibajoCode: "50",
      mmdd: "0529",
      raceBango: "07",
      source: "nar",
      year: "2026",
    }).generationKey,
  ).toBe("race-cache:gen:v2:2026:0529:50:07");
});

it("buildRaceCacheBustKeys lists only the training-scope section keys", () => {
  expect(
    buildRaceCacheBustKeys({
      keibajoCode: "06",
      mmdd: "0926",
      raceBango: "03",
      scope: "training",
      source: "jra",
      year: "2026",
    }).trainingSectionKeys,
  ).toStrictEqual([
    "race-detail-section:v6:2026:09:26:06:03:training:default",
    "race-detail-section:v2:2026:09:26:06:03:premium-data-top:default",
  ]);
});

it("buildRaceCacheWarmMarkerKey embeds the generation key", () => {
  expect(
    buildRaceCacheWarmMarkerKey("race-trend", {
      keibajoCode: "50",
      mmdd: "0529",
      raceBango: "07",
      year: "2026",
    }),
  ).toBe("race-cache:warm:v1:race-trend:race-cache:gen:v2:2026:0529:50:07");
});

it("buildRaceDetailSsrCacheKey keeps the source segment", () => {
  expect(
    buildRaceDetailSsrCacheKey({
      day: "28",
      keibajoCode: "05",
      month: "06",
      raceNumber: "11",
      source: "jra",
      year: "2026",
    }),
  ).toBe("race-detail-ssr:v4:jra:2026:06:28:05:11");
});

it("parseRaceKey accepts the JRA raceKey shape", () => {
  expect(parseRaceKey("jra:20260628:05:11")).toStrictEqual({
    keibajoCode: "05",
    mmdd: "0628",
    raceBango: "11",
    source: "jra",
    year: "2026",
  });
});

it("parseRaceKey accepts an overseas JRA raceKey shape", () => {
  expect(parseRaceKey("jra:20260816:A8:04")).toStrictEqual({
    keibajoCode: "A8",
    mmdd: "0816",
    raceBango: "04",
    source: "jra",
    year: "2026",
  });
});

it("parseRaceKey accepts the NAR raceKey shape", () => {
  expect(parseRaceKey("nar:20260529:50:07")).toStrictEqual({
    keibajoCode: "50",
    mmdd: "0529",
    raceBango: "07",
    source: "nar",
    year: "2026",
  });
});

it("parseRaceKey rejects a malformed raceKey", () => {
  expect(parseRaceKey("not-a-race-key")).toBeNull();
});

it("parseRaceKey rejects a raceKey with an unknown source", () => {
  expect(parseRaceKey("world:20260529:50:07")).toBeNull();
});
