// Run with bun. `bun run --filter pc-keiba-viewer test`
import type { RaceTrendStarterRow } from "horse-racing-realtime/race-trend-daily-track-types";
import { expect, it } from "vitest";

import {
  buildTodaySiblingRunnerLookup,
  mergeTanshoOddsEnrichment,
  mergeTodaySiblingRunnerData,
  type TanshoOddsEnrichmentEntry,
  type TodaySiblingRunnerEntry,
} from "./today-sibling-runner-merge";

const buildRow = (overrides: Partial<RaceTrendStarterRow> = {}): RaceTrendStarterRow => ({
  bamei: "テストホース",
  bataiju: null,
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  finishPosition: 0,
  hassoJikoku: null,
  jockeyName: "騎手",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0607",
  keibajoCode: "05",
  raceBango: "01",
  raceName: null,
  runnerCount: null,
  sohaTime: null,
  source: "jra",
  tanshoOdds: null,
  tanshoPopularity: null,
  umaban: "01",
  wakuban: null,
  zogenFugo: null,
  zogenSa: null,
  ...overrides,
});

it("buildTodaySiblingRunnerLookup keys entries by raceBango and parsed umaban", () => {
  const lookup = buildTodaySiblingRunnerLookup([
    { raceBango: "01", umaban: "01", wakuban: "1", chokyoshiName: "調教師A" },
    { raceBango: "01", umaban: "02", wakuban: "1", chokyoshiName: "調教師B" },
  ]);
  expect(lookup.get("01:1")).toStrictEqual({
    raceBango: "01",
    umaban: "01",
    wakuban: "1",
    chokyoshiName: "調教師A",
  });
  expect(lookup.get("01:2")).toStrictEqual({
    raceBango: "01",
    umaban: "02",
    wakuban: "1",
    chokyoshiName: "調教師B",
  });
});

it("buildTodaySiblingRunnerLookup keeps the last entry when normalized keys collide", () => {
  const lookup = buildTodaySiblingRunnerLookup([
    { raceBango: "01", umaban: "01", wakuban: "1", chokyoshiName: "古い" },
    { raceBango: "01", umaban: "1", wakuban: "2", chokyoshiName: "新しい" },
  ]);
  expect(lookup.get("01:1")).toStrictEqual({
    raceBango: "01",
    umaban: "1",
    wakuban: "2",
    chokyoshiName: "新しい",
  });
});

it("mergeTodaySiblingRunnerData fills in missing wakuban and chokyoshiName", () => {
  const row = buildRow({ raceBango: "01", umaban: "05", wakuban: null });
  const entries: TodaySiblingRunnerEntry[] = [
    { raceBango: "01", umaban: "05", wakuban: "3", chokyoshiName: "調教師X" },
  ];
  const merged = mergeTodaySiblingRunnerData([row], entries);
  expect(merged[0]?.wakuban).toBe("3");
  expect(merged[0]?.chokyoshiName).toBe("調教師X");
});

it("mergeTodaySiblingRunnerData preserves an existing non-empty wakuban", () => {
  const row = buildRow({ raceBango: "01", umaban: "05", wakuban: "8" });
  const entries: TodaySiblingRunnerEntry[] = [
    { raceBango: "01", umaban: "05", wakuban: "3", chokyoshiName: "調教師X" },
  ];
  const merged = mergeTodaySiblingRunnerData([row], entries);
  expect(merged[0]?.wakuban).toBe("8");
  expect(merged[0]?.chokyoshiName).toBe("調教師X");
});

it("mergeTodaySiblingRunnerData preserves an existing non-empty chokyoshiName", () => {
  const row = buildRow({
    raceBango: "01",
    umaban: "05",
    chokyoshiName: "既存",
  });
  const entries: TodaySiblingRunnerEntry[] = [
    { raceBango: "01", umaban: "05", wakuban: "3", chokyoshiName: "新規" },
  ];
  const merged = mergeTodaySiblingRunnerData([row], entries);
  expect(merged[0]?.chokyoshiName).toBe("既存");
});

it("mergeTodaySiblingRunnerData treats empty-string wakuban as missing", () => {
  const row = buildRow({ raceBango: "01", umaban: "05", wakuban: "" });
  const entries: TodaySiblingRunnerEntry[] = [
    { raceBango: "01", umaban: "05", wakuban: "3", chokyoshiName: "調教師X" },
  ];
  const merged = mergeTodaySiblingRunnerData([row], entries);
  expect(merged[0]?.wakuban).toBe("3");
});

it("mergeTodaySiblingRunnerData treats empty-string chokyoshiName as missing", () => {
  const row = buildRow({
    raceBango: "01",
    umaban: "05",
    chokyoshiName: "",
  });
  const entries: TodaySiblingRunnerEntry[] = [
    { raceBango: "01", umaban: "05", wakuban: "3", chokyoshiName: "新規" },
  ];
  const merged = mergeTodaySiblingRunnerData([row], entries);
  expect(merged[0]?.chokyoshiName).toBe("新規");
});

it("mergeTodaySiblingRunnerData passes rows without an umaban through unchanged", () => {
  const row = buildRow({ raceBango: "01", umaban: null, wakuban: null });
  const entries: TodaySiblingRunnerEntry[] = [
    { raceBango: "01", umaban: "05", wakuban: "3", chokyoshiName: "調教師X" },
  ];
  const merged = mergeTodaySiblingRunnerData([row], entries);
  expect(merged[0]?.wakuban).toBeNull();
  expect(merged[0]?.chokyoshiName).toBeUndefined();
});

it("mergeTodaySiblingRunnerData passes rows with an empty umaban through unchanged", () => {
  const row = buildRow({ raceBango: "01", umaban: "", wakuban: null });
  const entries: TodaySiblingRunnerEntry[] = [
    { raceBango: "01", umaban: "", wakuban: "3", chokyoshiName: "調教師X" },
  ];
  const merged = mergeTodaySiblingRunnerData([row], entries);
  expect(merged[0]?.wakuban).toBeNull();
  expect(merged[0]?.chokyoshiName).toBeUndefined();
});

it("mergeTodaySiblingRunnerData passes rows without a matching entry through unchanged", () => {
  const row = buildRow({ raceBango: "01", umaban: "07", wakuban: null });
  const entries: TodaySiblingRunnerEntry[] = [
    { raceBango: "01", umaban: "05", wakuban: "3", chokyoshiName: "調教師X" },
  ];
  const merged = mergeTodaySiblingRunnerData([row], entries);
  expect(merged[0]?.wakuban).toBeNull();
  expect(merged[0]?.chokyoshiName).toBeUndefined();
});

it("mergeTodaySiblingRunnerData returns a copy of the original rows when entries are empty", () => {
  const row = buildRow({
    raceBango: "01",
    umaban: "05",
    wakuban: "2",
    bamei: "コピーテスト",
  });
  const merged = mergeTodaySiblingRunnerData([row], []);
  expect(merged).toHaveLength(1);
  expect(merged[0]?.wakuban).toBe("2");
  expect(merged[0]?.bamei).toBe("コピーテスト");
});

it("mergeTodaySiblingRunnerData merges unpadded row umaban against zero-padded entry umaban", () => {
  const row = buildRow({ raceBango: "01", umaban: "5", wakuban: null });
  const entries: TodaySiblingRunnerEntry[] = [
    { raceBango: "01", umaban: "05", wakuban: "3", chokyoshiName: "調教師Z" },
  ];
  const merged = mergeTodaySiblingRunnerData([row], entries);
  expect(merged[0]?.wakuban).toBe("3");
  expect(merged[0]?.chokyoshiName).toBe("調教師Z");
});

it("mergeTodaySiblingRunnerData merges row umaban with surrounding whitespace against zero-padded entry", () => {
  const row = buildRow({ raceBango: "01", umaban: " 7 ", wakuban: null });
  const entries: TodaySiblingRunnerEntry[] = [
    { raceBango: "01", umaban: "07", wakuban: "4", chokyoshiName: "調教師W" },
  ];
  const merged = mergeTodaySiblingRunnerData([row], entries);
  expect(merged[0]?.wakuban).toBe("4");
  expect(merged[0]?.chokyoshiName).toBe("調教師W");
});

it("mergeTodaySiblingRunnerData falls back to the raw umaban when not numeric", () => {
  const row = buildRow({ raceBango: "01", umaban: "abc", wakuban: null });
  const entries: TodaySiblingRunnerEntry[] = [
    { raceBango: "01", umaban: "abc", wakuban: "9", chokyoshiName: "調教師Q" },
  ];
  const merged = mergeTodaySiblingRunnerData([row], entries);
  expect(merged[0]?.wakuban).toBe("9");
  expect(merged[0]?.chokyoshiName).toBe("調教師Q");
});

it("mergeTodaySiblingRunnerData supports multiple rows across races", () => {
  const rows: RaceTrendStarterRow[] = [
    buildRow({ raceBango: "01", umaban: "01", wakuban: null }),
    buildRow({ raceBango: "02", umaban: "03", wakuban: null }),
  ];
  const entries: TodaySiblingRunnerEntry[] = [
    { raceBango: "01", umaban: "01", wakuban: "1", chokyoshiName: "A" },
    { raceBango: "02", umaban: "03", wakuban: "2", chokyoshiName: "B" },
  ];
  const merged = mergeTodaySiblingRunnerData(rows, entries);
  expect(merged[0]?.wakuban).toBe("1");
  expect(merged[0]?.chokyoshiName).toBe("A");
  expect(merged[1]?.wakuban).toBe("2");
  expect(merged[1]?.chokyoshiName).toBe("B");
});

it("mergeTanshoOddsEnrichment returns rows unchanged when entries is empty", () => {
  const row = buildRow({ raceBango: "01", umaban: "01" });
  const merged = mergeTanshoOddsEnrichment([row], []);
  expect(merged).toStrictEqual([row]);
});

it("mergeTanshoOddsEnrichment populates tanshoOdds + tanshoPopularity by raceKey + umaban", () => {
  const row = buildRow({ raceBango: "01", umaban: "01" });
  const entries: TanshoOddsEnrichmentEntry[] = [
    {
      raceKey: "jra:2026:0607:05:01",
      tanshoOddsTenth: 42,
      tanshoPopularity: 3,
      umaban: "1",
    },
  ];
  const merged = mergeTanshoOddsEnrichment([row], entries);
  expect(merged[0]?.tanshoOdds).toBe("0042");
  expect(merged[0]?.tanshoPopularity).toBe("03");
});

it("mergeTanshoOddsEnrichment leaves rows untouched when the umaban does not match", () => {
  const row = buildRow({ raceBango: "01", umaban: "07" });
  const entries: TanshoOddsEnrichmentEntry[] = [
    {
      raceKey: "jra:2026:0607:05:01",
      tanshoOddsTenth: 42,
      tanshoPopularity: 3,
      umaban: "1",
    },
  ];
  const merged = mergeTanshoOddsEnrichment([row], entries);
  expect(merged[0]?.tanshoOdds).toBe(null);
  expect(merged[0]?.tanshoPopularity).toBe(null);
});

it("mergeTanshoOddsEnrichment preserves an already-populated tanshoOdds", () => {
  const row = buildRow({ raceBango: "01", tanshoOdds: "0099", umaban: "01" });
  const entries: TanshoOddsEnrichmentEntry[] = [
    {
      raceKey: "jra:2026:0607:05:01",
      tanshoOddsTenth: 42,
      tanshoPopularity: 3,
      umaban: "1",
    },
  ];
  const merged = mergeTanshoOddsEnrichment([row], entries);
  expect(merged[0]?.tanshoOdds).toBe("0099");
  expect(merged[0]?.tanshoPopularity).toBe("03");
});

it("mergeTanshoOddsEnrichment treats null tanshoOddsTenth as a no-op for that field", () => {
  const row = buildRow({ raceBango: "01", umaban: "01" });
  const entries: TanshoOddsEnrichmentEntry[] = [
    {
      raceKey: "jra:2026:0607:05:01",
      tanshoOddsTenth: null,
      tanshoPopularity: 5,
      umaban: "1",
    },
  ];
  const merged = mergeTanshoOddsEnrichment([row], entries);
  expect(merged[0]?.tanshoOdds).toBe(null);
  expect(merged[0]?.tanshoPopularity).toBe("05");
});

it("mergeTanshoOddsEnrichment skips rows whose umaban is null or empty", () => {
  const row = buildRow({ raceBango: "01", umaban: null });
  const entries: TanshoOddsEnrichmentEntry[] = [
    {
      raceKey: "jra:2026:0607:05:01",
      tanshoOddsTenth: 42,
      tanshoPopularity: 3,
      umaban: "1",
    },
  ];
  const merged = mergeTanshoOddsEnrichment([row], entries);
  expect(merged[0]?.tanshoOdds).toBe(null);
  expect(merged[0]?.tanshoPopularity).toBe(null);
});
