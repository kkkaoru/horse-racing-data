// Run with: bun run --filter sync-realtime-data-features test
import { expect, it } from "vitest";

import {
  aggregateRaceTrendRows,
  buildRaceKey,
  dailyRowToStarterRow,
  normalizeNumberText,
  normalizeText,
  parseCornerPosition,
  parseStoredInteger,
  parseStoredPopularity,
  parseStoredWinOdds,
  runningStyleFromCorners,
} from "./aggregate";
import type { DailyRaceEntryRow } from "../types";

const baseRow: DailyRaceEntryRow = {
  babajotai_code_dirt: null,
  babajotai_code_shiba: null,
  bamei: null,
  banushimei: null,
  barei: null,
  bataiju: null,
  chokyoshimei_ryakusho: null,
  corner1_norm: null,
  corner2_norm: null,
  corner3_norm: null,
  corner4_norm: null,
  corner_1: null,
  corner_2: null,
  corner_3: null,
  corner_4: null,
  finish_norm: null,
  finish_position: null,
  futan_juryo: null,
  grade_code: null,
  hasso_jikoku: null,
  juryo_shubetsu_code: null,
  kaisai_nen: "2026",
  kaisai_tsukihi: "0529",
  keibajo_code: "30",
  ketto_toroku_bango: "kt",
  kishumei_ryakusho: null,
  kohan_3f: null,
  kyori: null,
  kyoso_joken_code: null,
  kyoso_shubetsu_code: null,
  race_bango: "08",
  race_date: "20260529",
  race_name: null,
  seibetsu_code: null,
  shusso_tosu: null,
  soha_time: null,
  source: "nar",
  tansho_ninkijun: null,
  tansho_odds: null,
  time_sa: null,
  track_code: null,
  umaban: null,
  wakuban: null,
  zogen_fugo: null,
  zogen_sa: null,
};

it("normalizeText returns null for empty string", () => {
  expect(normalizeText("")).toBeNull();
});

it("normalizeText returns null for whitespace", () => {
  expect(normalizeText("   ")).toBeNull();
});

it("normalizeText returns null for undefined", () => {
  expect(normalizeText(undefined)).toBeNull();
});

it("normalizeText trims surrounding whitespace", () => {
  expect(normalizeText("  山田  ")).toBe("山田");
});

it("normalizeNumberText strips leading zeros", () => {
  expect(normalizeNumberText("007")).toBe("7");
});

it("normalizeNumberText returns null for empty input", () => {
  expect(normalizeNumberText(null)).toBeNull();
});

it("normalizeNumberText keeps zero-only token as a single 0", () => {
  expect(normalizeNumberText("0")).toBe("0");
});

it("parseStoredInteger returns null when value equals empty marker", () => {
  expect(parseStoredInteger("00", "00")).toBeNull();
});

it("parseStoredInteger returns the integer when valid", () => {
  expect(parseStoredInteger("12", "00")).toBe(12);
});

it("parseStoredInteger returns null for non-positive integer", () => {
  expect(parseStoredInteger("abc", "00")).toBeNull();
});

it("parseStoredInteger returns null for null input", () => {
  expect(parseStoredInteger(null, "00")).toBeNull();
});

it("parseStoredPopularity treats 00 as empty", () => {
  expect(parseStoredPopularity("00")).toBeNull();
});

it("parseStoredPopularity parses 03 as 3", () => {
  expect(parseStoredPopularity("03")).toBe(3);
});

it("parseStoredWinOdds returns null when input is 0000", () => {
  expect(parseStoredWinOdds("0000")).toBeNull();
});

it("parseStoredWinOdds divides parsed value by 10", () => {
  expect(parseStoredWinOdds("0123")).toBe(12.3);
});

it("parseCornerPosition returns null for 00", () => {
  expect(parseCornerPosition("00")).toBeNull();
});

it("parseCornerPosition parses 04 as 4", () => {
  expect(parseCornerPosition("04")).toBe(4);
});

it("runningStyleFromCorners returns null when every corner is missing", () => {
  expect(
    runningStyleFromCorners({
      corner1: null,
      corner2: null,
      corner3: null,
      corner4: null,
      runnerCount: null,
    }),
  ).toBeNull();
});

it("runningStyleFromCorners returns nige when leading the first corner", () => {
  expect(
    runningStyleFromCorners({
      corner1: "01",
      corner2: "01",
      corner3: "01",
      corner4: "01",
      runnerCount: "12",
    }),
  ).toBe("nige");
});

it("runningStyleFromCorners returns senkou when ratio is low", () => {
  expect(
    runningStyleFromCorners({
      corner1: "02",
      corner2: null,
      corner3: null,
      corner4: null,
      runnerCount: "10",
    }),
  ).toBe("senkou");
});

it("runningStyleFromCorners returns sashi when ratio is middle", () => {
  expect(
    runningStyleFromCorners({
      corner1: "05",
      corner2: null,
      corner3: null,
      corner4: null,
      runnerCount: "10",
    }),
  ).toBe("sashi");
});

it("runningStyleFromCorners returns oikomi when ratio is high", () => {
  expect(
    runningStyleFromCorners({
      corner1: "09",
      corner2: null,
      corner3: null,
      corner4: null,
      runnerCount: "10",
    }),
  ).toBe("oikomi");
});

it("runningStyleFromCorners falls back to corner table when runnerCount missing senkou", () => {
  expect(
    runningStyleFromCorners({
      corner1: "03",
      corner2: null,
      corner3: null,
      corner4: null,
      runnerCount: null,
    }),
  ).toBe("senkou");
});

it("runningStyleFromCorners falls back to corner table when runnerCount missing sashi", () => {
  expect(
    runningStyleFromCorners({
      corner1: "07",
      corner2: null,
      corner3: null,
      corner4: null,
      runnerCount: null,
    }),
  ).toBe("sashi");
});

it("runningStyleFromCorners falls back to corner table when runnerCount missing oikomi", () => {
  expect(
    runningStyleFromCorners({
      corner1: "10",
      corner2: null,
      corner3: null,
      corner4: null,
      runnerCount: null,
    }),
  ).toBe("oikomi");
});

it("buildRaceKey zero-pads keibajoCode and raceBango", () => {
  expect(
    buildRaceKey({
      source: "nar",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "5",
      raceBango: "8",
    }),
  ).toBe("nar:20260529:05:08");
});

it("dailyRowToStarterRow maps DailyRaceEntryRow fields to viewer StarterRow shape", () => {
  expect(
    dailyRowToStarterRow({
      ...baseRow,
      bamei: "ホースA",
      bataiju: 452,
      chokyoshimei_ryakusho: "山田調教師",
      corner_1: 3,
      corner_2: 4,
      finish_position: 2,
      hasso_jikoku: "1400",
      kishumei_ryakusho: "山田",
      race_name: "test race",
      shusso_tosu: 12,
      soha_time: 1234,
      tansho_ninkijun: 3,
      tansho_odds: 5.4,
      umaban: 7,
      wakuban: "4",
      zogen_fugo: "+",
      zogen_sa: 2,
    }),
  ).toStrictEqual({
    bamei: "ホースA",
    bataiju: "452",
    chokyoshiName: "山田調教師",
    corner1: "3",
    corner2: "4",
    corner3: null,
    corner4: null,
    finishPosition: 2,
    hassoJikoku: "1400",
    jockeyName: "山田",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
    raceName: "test race",
    runnerCount: "12",
    sohaTime: "1234",
    source: "nar",
    tanshoOdds: "0054",
    tanshoPopularity: "03",
    umaban: "7",
    wakuban: "4",
    zogenFugo: "+",
    zogenSa: "2",
  });
});

it("dailyRowToStarterRow normalises finish_position null to 0", () => {
  expect(dailyRowToStarterRow(baseRow).finishPosition).toBe(0);
});

it("dailyRowToStarterRow returns null chokyoshiName when source is null", () => {
  expect(dailyRowToStarterRow(baseRow).chokyoshiName).toBeNull();
});

it("dailyRowToStarterRow keeps null odds and popularity when source is null", () => {
  const result = dailyRowToStarterRow(baseRow);
  expect(result.tanshoOdds).toBeNull();
  expect(result.tanshoPopularity).toBeNull();
});

it("aggregateRaceTrendRows returns empty buckets for empty input", () => {
  expect(aggregateRaceTrendRows([])).toStrictEqual({
    byJockey: {},
    byWaku: {},
    raceCount: 0,
    starterCount: 0,
    starterRows: [],
  });
});

it("aggregateRaceTrendRows counts wins / shows / quinellas per jockey", () => {
  const result = aggregateRaceTrendRows([
    { ...baseRow, finish_position: 1, kishumei_ryakusho: "山田", wakuban: "1" },
    { ...baseRow, finish_position: 2, kishumei_ryakusho: "山田", race_bango: "09", wakuban: "2" },
    { ...baseRow, finish_position: 4, kishumei_ryakusho: "佐藤", race_bango: "10", wakuban: "1" },
  ]);
  expect(result.byJockey).toStrictEqual({
    佐藤: { quinellas: 0, runs: 1, shows: 0, wins: 0 },
    山田: { quinellas: 2, runs: 2, shows: 2, wins: 1 },
  });
});

it("aggregateRaceTrendRows counts wins / shows / quinellas per waku", () => {
  const result = aggregateRaceTrendRows([
    { ...baseRow, finish_position: 1, wakuban: "1" },
    { ...baseRow, finish_position: 3, race_bango: "09", wakuban: "1" },
    { ...baseRow, finish_position: 5, race_bango: "10", wakuban: "2" },
  ]);
  expect(result.byWaku).toStrictEqual({
    1: { quinellas: 1, runs: 2, shows: 2, wins: 1 },
    2: { quinellas: 0, runs: 1, shows: 0, wins: 0 },
  });
});

it("aggregateRaceTrendRows skips finish_position null rows from buckets but counts starters", () => {
  const result = aggregateRaceTrendRows([
    { ...baseRow, finish_position: null, kishumei_ryakusho: "山田", wakuban: "1" },
    { ...baseRow, finish_position: 1, kishumei_ryakusho: "山田", race_bango: "09", wakuban: "1" },
  ]);
  expect(result.byJockey).toStrictEqual({
    山田: { quinellas: 1, runs: 1, shows: 1, wins: 1 },
  });
  expect(result.byWaku).toStrictEqual({
    1: { quinellas: 1, runs: 1, shows: 1, wins: 1 },
  });
  expect(result.starterCount).toBe(2);
});

it("aggregateRaceTrendRows counts distinct race keys", () => {
  const result = aggregateRaceTrendRows([
    { ...baseRow, race_bango: "08" },
    { ...baseRow, race_bango: "09" },
    { ...baseRow, race_bango: "09" },
  ]);
  expect(result.raceCount).toBe(2);
});

it("aggregateRaceTrendRows ignores rows without jockey name when bucketing", () => {
  const result = aggregateRaceTrendRows([
    { ...baseRow, finish_position: 1, kishumei_ryakusho: "  ", wakuban: "1" },
  ]);
  expect(result.byJockey).toStrictEqual({});
  expect(result.byWaku).toStrictEqual({
    1: { quinellas: 1, runs: 1, shows: 1, wins: 1 },
  });
});

it("aggregateRaceTrendRows ignores rows without wakuban when bucketing", () => {
  const result = aggregateRaceTrendRows([
    { ...baseRow, finish_position: 2, kishumei_ryakusho: "山田", wakuban: null },
  ]);
  expect(result.byWaku).toStrictEqual({});
  expect(result.byJockey).toStrictEqual({
    山田: { quinellas: 1, runs: 1, shows: 1, wins: 0 },
  });
});

it("aggregateRaceTrendRows builds starterRows in input order", () => {
  const result = aggregateRaceTrendRows([
    { ...baseRow, race_bango: "08" },
    { ...baseRow, race_bango: "09" },
  ]);
  expect(result.starterRows.length).toBe(2);
  expect(result.starterRows[0]!.raceBango).toBe("08");
  expect(result.starterRows[1]!.raceBango).toBe("09");
});
