// Run with bun test apps/sync-realtime-data/test/running-style-features.test.ts
import { expect, test } from "vitest";

import {
  buildRealtimeRaceKeyFromRunningStyle,
  buildRunningStyleRaceKey,
  normalizeKeibajoCode,
  normalizeRaceBango,
  parseRunningStyleRaceKey,
} from "../src/running-style-features";

test("buildRunningStyleRaceKey concatenates date without colon", () => {
  expect(
    buildRunningStyleRaceKey({
      kaisaiNen: "2026",
      kaisaiTsukihi: "0524",
      keibajoCode: "4",
      raceBango: "1",
      source: "jra",
    }),
  ).toBe("jra:20260524:04:01");
});

test("buildRealtimeRaceKeyFromRunningStyle keeps date colon for realtime tables", () => {
  expect(
    buildRealtimeRaceKeyFromRunningStyle({
      kaisaiNen: "2026",
      kaisaiTsukihi: "0524",
      keibajoCode: "4",
      raceBango: "1",
      source: "jra",
    }),
  ).toBe("jra:2026:0524:04:01");
});

test("buildRealtimeRaceKeyFromRunningStyle pads keibajo and raceBango for nar", () => {
  expect(
    buildRealtimeRaceKeyFromRunningStyle({
      kaisaiNen: "2026",
      kaisaiTsukihi: "0524",
      keibajoCode: "83",
      raceBango: "12",
      source: "nar",
    }),
  ).toBe("nar:2026:0524:83:12");
});

test("normalizeKeibajoCode pads to two digits", () => {
  expect(normalizeKeibajoCode("4")).toBe("04");
});

test("normalizeRaceBango pads to two digits", () => {
  expect(normalizeRaceBango("1")).toBe("01");
});

test("parseRunningStyleRaceKey round-trips a jra key", () => {
  const parsed = parseRunningStyleRaceKey("jra:20260524:04:01");
  expect(parsed).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0524",
    keibajoCode: "04",
    raceBango: "01",
    raceKey: "jra:20260524:04:01",
    source: "jra",
  });
});

test("parseRunningStyleRaceKey returns null for malformed key", () => {
  expect(parseRunningStyleRaceKey("jra:2026:0524:04:01")).toBe(null);
});
