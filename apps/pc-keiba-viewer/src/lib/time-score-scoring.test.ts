// Run with bun.
import { expect, it } from "vitest";

import {
  ageBandDays,
  compactDateToEpochMs,
  dayGap,
  distanceScore,
  normaliseHorseName,
  normaliseHorseNumber,
  parseDigitsOnly,
  parseHorseNumberSort,
  parseRaceTimeTenths,
  recencyWeight,
} from "./time-score-scoring";

it("normalises horse numbers and names the way the SQL does", () => {
  expect(normaliseHorseNumber("01")).toBe("1");
  expect(normaliseHorseNumber("018")).toBe("18");
  expect(normaliseHorseNumber("00")).toBe("0");
  expect(normaliseHorseNumber("")).toBe("0");
  expect(normaliseHorseNumber(null)).toBe("0");
  expect(parseHorseNumberSort("07")).toBe(7);
  expect(parseHorseNumberSort("aa")).toBeNull();
  expect(parseHorseNumberSort(null)).toBeNull();
  expect(normaliseHorseName("　バビット　")).toBe("バビット");
  expect(normaliseHorseName("  ")).toBe("-");
  expect(normaliseHorseName(null)).toBe("-");
});

it("encodes race times as tenths with the SQL guards", () => {
  expect(parseRaceTimeTenths("1234")).toBe(834);
  expect(parseRaceTimeTenths("1100")).toBe(700);
  expect(parseRaceTimeTenths("034")).toBe(34);
  expect(parseRaceTimeTenths("0000")).toBeNull();
  expect(parseRaceTimeTenths("9999")).toBeNull();
  expect(parseRaceTimeTenths("12345")).toBeNull();
  expect(parseRaceTimeTenths("12a4")).toBeNull();
  expect(parseRaceTimeTenths("")).toBeNull();
  expect(parseRaceTimeTenths(null)).toBeNull();
});

it("parses digits-only numerics", () => {
  expect(parseDigitsOnly("375")).toBe(375);
  expect(parseDigitsOnly("51２")).toBe(51);
  expect(parseDigitsOnly("000")).toBe(0);
  expect(parseDigitsOnly("abc")).toBeNull();
  expect(parseDigitsOnly(null)).toBeNull();
});

it("selects the recency band by age", () => {
  expect(ageBandDays(null)).toBe(365);
  expect(ageBandDays(3)).toBe(180);
  expect(ageBandDays(4)).toBe(270);
  expect(ageBandDays(5)).toBe(365);
});

it("computes date gaps and recency weights", () => {
  expect(compactDateToEpochMs("20231112")).toBe(Date.UTC(2023, 10, 12));
  expect(compactDateToEpochMs("20230230")).toBeNull();
  expect(compactDateToEpochMs("2023111")).toBeNull();
  expect(dayGap("20260920", "20231112")).toBe(1043);
  expect(dayGap("20260920", "20260920")).toBe(0);
  expect(dayGap("20260920", null)).toBeNull();
  expect(recencyWeight(0, 365)).toBe(1);
  expect(recencyWeight(365, 365)).toBe(0.5);
  expect(recencyWeight(-5, 365)).toBe(1);
  expect(recencyWeight(null, 365)).toBe(1);
});

it("scores distances with the SQL fallbacks", () => {
  expect(distanceScore(1200, 1200)).toBe(1);
  expect(distanceScore(1200, 1600)).toBe(0.5);
  expect(distanceScore(1600, 1200)).toBeCloseTo(1 / 3, 6);
  expect(distanceScore(null, 1600)).toBe(0.5);
  expect(distanceScore(1600, null)).toBe(0.5);
  expect(distanceScore(100, 1200)).toBeCloseTo(0, 5);
});
