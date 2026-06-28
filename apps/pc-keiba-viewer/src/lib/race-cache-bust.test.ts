// Run with bun: `bun run --filter pc-keiba-viewer test`
import { expect, it } from "vitest";

import {
  RACE_CACHE_BUST_INTERNAL_PATH,
  buildRaceCacheBustKeys,
  buildRaceCacheGenerationKey,
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

it("buildRaceCacheGenerationKey produces a per-race prefixed key", () => {
  expect(
    buildRaceCacheGenerationKey({
      keibajoCode: "05",
      mmdd: "0628",
      raceBango: "11",
      source: "jra",
      year: "2026",
    }),
  ).toBe("race-cache:gen:jra:2026:0628:05:11");
});

it("buildRaceCacheBustKeys generates main + stale entries for every cacheable section", () => {
  const keys = buildRaceCacheBustKeys({
    keibajoCode: "05",
    mmdd: "0628",
    raceBango: "11",
    source: "jra",
    year: "2026",
  });
  expect(keys.mainKeys).toStrictEqual([
    "race-detail-section:v2:2026:06:28:05:11:ability:default",
    "race-detail-section:v2:2026:06:28:05:11:bloodline:default",
    "race-detail-section:v2:2026:06:28:05:11:condition:default",
    "race-detail-section:v2:2026:06:28:05:11:premium-data-top:default",
    "race-detail-section:v2:2026:06:28:05:11:overall-score:default",
    "race-detail-section:v2:2026:06:28:05:11:pace-prediction:default",
    "race-detail-section:v2:2026:06:28:05:11:results:default",
    "race-detail-section:v2:2026:06:28:05:11:similar:default",
    "race-detail-section:v2:2026:06:28:05:11:time-score:default",
    "race-detail-section:v2:2026:06:28:05:11:training:default",
  ]);
});

it("buildRaceCacheBustKeys staleKeys prepend the stale: prefix to each main key", () => {
  const keys = buildRaceCacheBustKeys({
    keibajoCode: "05",
    mmdd: "0628",
    raceBango: "11",
    source: "jra",
    year: "2026",
  });
  expect(keys.staleKeys).toStrictEqual([
    "stale:race-detail-section:v2:2026:06:28:05:11:ability:default",
    "stale:race-detail-section:v2:2026:06:28:05:11:bloodline:default",
    "stale:race-detail-section:v2:2026:06:28:05:11:condition:default",
    "stale:race-detail-section:v2:2026:06:28:05:11:premium-data-top:default",
    "stale:race-detail-section:v2:2026:06:28:05:11:overall-score:default",
    "stale:race-detail-section:v2:2026:06:28:05:11:pace-prediction:default",
    "stale:race-detail-section:v2:2026:06:28:05:11:results:default",
    "stale:race-detail-section:v2:2026:06:28:05:11:similar:default",
    "stale:race-detail-section:v2:2026:06:28:05:11:time-score:default",
    "stale:race-detail-section:v2:2026:06:28:05:11:training:default",
  ]);
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
  ).toBe("race-cache:gen:nar:2026:0529:50:07");
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
