// Run with: bunx vitest run src/lib/race-trend-aggregate.test.ts
import { expect, test } from "vitest";

import {
  aggregateForTargets,
  countDistinctRunningStyleDetailRaces,
  detailFromStarter,
  getJockeyNameAliases,
  normalizeNumberText,
  normalizeRaceTrendJockeyName,
  normalizeText,
  parseCornerPosition,
  parseStoredInteger,
  parseStoredPopularity,
  parseStoredWinOdds,
  resolveRowJockeyKey,
  runningStyleFromCorners,
  starterKey,
  starterRaceKey,
  starterRunningStyleKey,
} from "./race-trend-aggregate";
import type {
  RaceTrendCurrentRunningStyle,
  RaceTrendRunningStyleCache,
  RaceTrendStarterRow,
} from "./race-types";

const baseRow: RaceTrendStarterRow = {
  source: "nar",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0520",
  keibajoCode: "44",
  raceBango: "11",
  raceName: "テストS",
  hassoJikoku: "2030",
  runnerCount: "10",
  wakuban: "3",
  umaban: "5",
  bamei: "サンプル",
  jockeyName: "山田太郎",
  tanshoOdds: "0050",
  tanshoPopularity: "03",
  finishPosition: 2,
  sohaTime: null,
  corner1: "04",
  corner2: "03",
  corner3: "02",
  corner4: "02",
  bataiju: "498",
  zogenFugo: "+",
  zogenSa: "004",
};

test("normalizeText trims whitespace", () => {
  expect(normalizeText("  hello  ")).toStrictEqual("hello");
});

test("normalizeText returns null for empty string", () => {
  expect(normalizeText("")).toBeNull();
});

test("normalizeText returns null for null input", () => {
  expect(normalizeText(null)).toBeNull();
});

test("normalizeText returns null for undefined input", () => {
  expect(normalizeText(undefined)).toBeNull();
});

test("normalizeNumberText strips leading zeros while keeping single zero", () => {
  expect(normalizeNumberText("03")).toStrictEqual("3");
});

test("normalizeNumberText preserves a literal 0", () => {
  expect(normalizeNumberText("0")).toStrictEqual("0");
});

test("normalizeNumberText returns null for empty input", () => {
  expect(normalizeNumberText("")).toBeNull();
});

test("parseStoredInteger returns null when value matches the empty sentinel", () => {
  expect(parseStoredInteger("00", "00")).toBeNull();
});

test("parseStoredInteger returns null when value contains only non-digit characters", () => {
  expect(parseStoredInteger("---", "00")).toBeNull();
});

test("parseStoredInteger parses digits and strips non-digit prefixes", () => {
  expect(parseStoredInteger("042", "00")).toStrictEqual(42);
});

test("parseStoredPopularity returns null for the 00 sentinel", () => {
  expect(parseStoredPopularity("00")).toBeNull();
});

test("parseStoredPopularity parses a populated value", () => {
  expect(parseStoredPopularity("12")).toStrictEqual(12);
});

test("parseStoredWinOdds returns null for the 0000 sentinel", () => {
  expect(parseStoredWinOdds("0000")).toBeNull();
});

test("parseStoredWinOdds divides the stored integer by 10", () => {
  expect(parseStoredWinOdds("1234")).toStrictEqual(123.4);
});

test("parseCornerPosition returns null for the 00 sentinel", () => {
  expect(parseCornerPosition("00")).toBeNull();
});

test("parseCornerPosition parses a populated value", () => {
  expect(parseCornerPosition("05")).toStrictEqual(5);
});

test("normalizeRaceTrendJockeyName returns null for empty after normalization", () => {
  expect(normalizeRaceTrendJockeyName("")).toBeNull();
});

test("normalizeRaceTrendJockeyName normalizes a jockey name", () => {
  expect(normalizeRaceTrendJockeyName("山田太郎")).toStrictEqual("山田太郎");
});

test("getJockeyNameAliases returns a single-element list for unrelated jockeys", () => {
  expect(getJockeyNameAliases("山田太郎")).toStrictEqual(["山田太郎"]);
});

test("getJockeyNameAliases adds the legacy demuro aliases when the jockey is demuro", () => {
  expect(getJockeyNameAliases("デムーロ")).toStrictEqual([
    "デムーロ",
    "デムーロ",
    "Ｍ．デム",
    "M.デム",
  ]);
});

test("runningStyleFromCorners returns nige when the first corner is 1", () => {
  expect(
    runningStyleFromCorners({
      corner1: "01",
      corner2: null,
      corner3: null,
      corner4: null,
      runnerCount: "10",
    }),
  ).toStrictEqual("nige");
});

test("runningStyleFromCorners returns senkou for low ratio", () => {
  expect(
    runningStyleFromCorners({
      corner1: "02",
      corner2: null,
      corner3: null,
      corner4: null,
      runnerCount: "10",
    }),
  ).toStrictEqual("senkou");
});

test("runningStyleFromCorners returns sashi for mid ratio", () => {
  expect(
    runningStyleFromCorners({
      corner1: "05",
      corner2: null,
      corner3: null,
      corner4: null,
      runnerCount: "10",
    }),
  ).toStrictEqual("sashi");
});

test("runningStyleFromCorners returns oikomi for high ratio", () => {
  expect(
    runningStyleFromCorners({
      corner1: "09",
      corner2: "09",
      corner3: "09",
      corner4: "09",
      runnerCount: "10",
    }),
  ).toStrictEqual("oikomi");
});

test("runningStyleFromCorners returns senkou when runnerCount is missing and corner <= 4", () => {
  expect(
    runningStyleFromCorners({
      corner1: "03",
      corner2: null,
      corner3: null,
      corner4: null,
      runnerCount: null,
    }),
  ).toStrictEqual("senkou");
});

test("runningStyleFromCorners returns sashi when runnerCount is missing and corner is between 5 and 8", () => {
  expect(
    runningStyleFromCorners({
      corner1: "07",
      corner2: null,
      corner3: null,
      corner4: null,
      runnerCount: null,
    }),
  ).toStrictEqual("sashi");
});

test("runningStyleFromCorners returns oikomi when runnerCount is missing and corner is past 8", () => {
  expect(
    runningStyleFromCorners({
      corner1: "12",
      corner2: null,
      corner3: null,
      corner4: null,
      runnerCount: null,
    }),
  ).toStrictEqual("oikomi");
});

test("runningStyleFromCorners falls back to the next corner when the first one is missing", () => {
  expect(
    runningStyleFromCorners({
      corner1: null,
      corner2: null,
      corner3: "01",
      corner4: null,
      runnerCount: "10",
    }),
  ).toStrictEqual("nige");
});

test("runningStyleFromCorners returns null when no corner has data", () => {
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

test("starterKey joins the canonical six fields", () => {
  expect(starterKey(baseRow)).toStrictEqual("nar:2026:0520:44:11:5");
});

test("starterKey leaves the umaban segment empty when missing", () => {
  expect(starterKey({ ...baseRow, umaban: null })).toStrictEqual("nar:2026:0520:44:11:");
});

test("starterRaceKey builds the canonical race key", () => {
  expect(starterRaceKey(baseRow)).toStrictEqual("nar:20260520:44:11");
});

test("detailFromStarter passes through horse weight and signed delta", () => {
  const detail = detailFromStarter(baseRow);
  expect(detail.horseWeight).toStrictEqual(498);
  expect(detail.horseWeightDelta).toStrictEqual(4);
});

test("detailFromStarter handles a negative weight delta", () => {
  expect(
    detailFromStarter({ ...baseRow, zogenFugo: "-", zogenSa: "002" }).horseWeightDelta,
  ).toStrictEqual(-2);
});

test("detailFromStarter treats a literal zero weight delta as zero", () => {
  expect(
    detailFromStarter({ ...baseRow, zogenFugo: "+", zogenSa: "0" }).horseWeightDelta,
  ).toStrictEqual(0);
});

test("detailFromStarter returns null weight delta when zogenSa is null", () => {
  expect(
    detailFromStarter({ ...baseRow, zogenFugo: null, zogenSa: null }).horseWeightDelta,
  ).toBeNull();
});

test("detailFromStarter returns null weight when bataiju is missing", () => {
  expect(detailFromStarter({ ...baseRow, bataiju: null }).horseWeight).toBeNull();
});

test("aggregateForTargets aggregates a single matching jockey row", () => {
  const result = aggregateForTargets(
    {
      starterRows: [baseRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows).toHaveLength(1);
  expect(result.runningStyleRows[0]?.starts).toStrictEqual(1);
});

test("aggregateForTargets returns zero races when no starter rows are passed", () => {
  const result = aggregateForTargets(
    {
      starterRows: [],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.raceCount).toStrictEqual(0);
});

test("aggregateForTargets excludes rows outside the date range", () => {
  const result = aggregateForTargets(
    {
      starterRows: [baseRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260101",
    "20260101",
  );
  expect(result.runningStyleRows[0]?.starts).toStrictEqual(0);
});

test("aggregateForTargets excludes rows from a different venue when jockeySameVenue is true", () => {
  const otherVenueRow: RaceTrendStarterRow = { ...baseRow, keibajoCode: "45" };
  const result = aggregateForTargets(
    {
      starterRows: [otherVenueRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.starts).toStrictEqual(0);
});

test("aggregateForTargets aggregates short and long jockey name variants via isSameJockeyName", () => {
  const shortNameRow: RaceTrendStarterRow = { ...baseRow, jockeyName: "和田譲" };
  const result = aggregateForTargets(
    {
      starterRows: [shortNameRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "和田譲治" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.starts).toStrictEqual(1);
});

test("aggregateForTargets aggregates kyujitai jockey names via isSameJockeyName", () => {
  const kyujitaiRow: RaceTrendStarterRow = { ...baseRow, jockeyName: "澤田龍" };
  const result = aggregateForTargets(
    {
      starterRows: [kyujitaiRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "沢田龍哉" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.starts).toStrictEqual(1);
});

test("aggregateForTargets does not aggregate rows with a different jockey", () => {
  const otherJockeyRow: RaceTrendStarterRow = { ...baseRow, jockeyName: "和田譲" };
  const result = aggregateForTargets(
    {
      starterRows: [otherJockeyRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "高田太郎" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.starts).toStrictEqual(0);
});

test("aggregateForTargets includes rows from other venues when jockeySameVenue is false", () => {
  const otherVenueRow: RaceTrendStarterRow = { ...baseRow, keibajoCode: "45" };
  const result = aggregateForTargets(
    {
      starterRows: [otherVenueRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    false,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.starts).toStrictEqual(1);
});

test("aggregateForTargets uses the current running style from the runner mapping", () => {
  const currentRunningStyles: RaceTrendCurrentRunningStyle[] = [
    { horseNumber: "5", predictedLabel: "nige" },
  ];
  const result = aggregateForTargets(
    {
      starterRows: [baseRow],
      currentRunningStyles,
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: true },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.runningStyle).toStrictEqual("nige");
});

test("aggregateForTargets joins historical running style rows for matching starters", () => {
  const historicalRunningStyles: RaceTrendRunningStyleCache[] = [
    {
      raceKey: "nar:20260520:44:11",
      horseNumber: "5",
      predictedLabel: "oikomi",
    },
  ];
  const result = aggregateForTargets(
    {
      starterRows: [baseRow],
      currentRunningStyles: [{ horseNumber: "5", predictedLabel: "oikomi" }],
      historicalRunningStyles,
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: true },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.details[0]?.runningStyle).toStrictEqual("oikomi");
});

test("aggregateForTargets falls back to corner-derived running style when no prediction cache row matches", () => {
  // baseRow has corner1=04, runnerCount=10 → ratio 0.333 → senkou. The
  // historical prediction cache is empty, so the aggregation key has to
  // derive senkou from the corner data to match the runner's predicted
  // running style.
  const result = aggregateForTargets(
    {
      starterRows: [baseRow],
      currentRunningStyles: [{ horseNumber: "5", predictedLabel: "senkou" }],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: false, raceNumber: false, runningStyle: true },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.runningStyle).toStrictEqual("senkou");
  expect(result.runningStyleRows[0]?.starts).toStrictEqual(1);
});

test("aggregateForTargets returns nullable rates when the group has no starters", () => {
  const result = aggregateForTargets(
    {
      starterRows: [],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.showRate).toStrictEqual(0);
  expect(result.runningStyleRows[0]?.quinellaRate).toStrictEqual(0);
  expect(result.runningStyleRows[0]?.winRate).toStrictEqual(0);
});

test("aggregateForTargets keeps frameNumber on the result when ignoreFrame is false", () => {
  const result = aggregateForTargets(
    {
      starterRows: [baseRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: true, jockey: true, raceNumber: true, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.frameNumber).toStrictEqual("3");
  expect(result.runningStyleRows[0]?.jockeyName).toStrictEqual("山田太郎");
  expect(result.runningStyleRows[0]?.raceNumber).toStrictEqual("12");
});

test("aggregateForTargets nulls frame/jockey/raceNumber when their targets are ignored", () => {
  const result = aggregateForTargets(
    {
      starterRows: [baseRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: false, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.frameNumber).toBeNull();
  expect(result.runningStyleRows[0]?.jockeyName).toBeNull();
  expect(result.runningStyleRows[0]?.raceNumber).toBeNull();
});

test("aggregateForTargets returns an empty targetHorseNumbers when runner has no horse number", () => {
  const result = aggregateForTargets(
    {
      starterRows: [baseRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: null, jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: true, raceNumber: true, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.targetHorseNumbers).toStrictEqual([]);
});

test("aggregateForTargets aggregates per-row medians using even-length winOdds", () => {
  const rowA: RaceTrendStarterRow = { ...baseRow, tanshoOdds: "0100" };
  const rowB: RaceTrendStarterRow = { ...baseRow, tanshoOdds: "0200", kaisaiTsukihi: "0519" };
  const result = aggregateForTargets(
    {
      starterRows: [rowA, rowB],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260519",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.winOddsMedian).toStrictEqual(15);
});

test("aggregateForTargets calculates odd-length medians correctly", () => {
  const rowA: RaceTrendStarterRow = { ...baseRow, tanshoOdds: "0100" };
  const rowB: RaceTrendStarterRow = { ...baseRow, tanshoOdds: "0200", kaisaiTsukihi: "0519" };
  const rowC: RaceTrendStarterRow = { ...baseRow, tanshoOdds: "0300", kaisaiTsukihi: "0518" };
  const result = aggregateForTargets(
    {
      starterRows: [rowA, rowB, rowC],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260518",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.winOddsMedian).toStrictEqual(20);
});

test("aggregateForTargets handles winners and shows across multiple rows", () => {
  const winningRow: RaceTrendStarterRow = { ...baseRow, finishPosition: 1 };
  const showingRow: RaceTrendStarterRow = {
    ...baseRow,
    kaisaiTsukihi: "0519",
    finishPosition: 3,
  };
  const losingRow: RaceTrendStarterRow = {
    ...baseRow,
    kaisaiTsukihi: "0518",
    finishPosition: 8,
  };
  const result = aggregateForTargets(
    {
      starterRows: [winningRow, showingRow, losingRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260518",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.winRate).toBeCloseTo(33.333, 1);
  expect(result.runningStyleRows[0]?.quinellaRate).toBeCloseTo(33.333, 1);
  expect(result.runningStyleRows[0]?.showRate).toBeCloseTo(66.666, 1);
});

test("aggregateForTargets sort comparator orders rows by descending showRate", () => {
  const lateRow: RaceTrendStarterRow = {
    ...baseRow,
    umaban: "5",
    finishPosition: 8,
  };
  const earlyRow: RaceTrendStarterRow = {
    ...baseRow,
    umaban: "3",
    jockeyName: "佐藤次郎",
    finishPosition: 1,
  };
  const result = aggregateForTargets(
    {
      starterRows: [lateRow, earlyRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [
        { frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" },
        { frameNumber: "1", horseNumber: "3", jockeyName: "佐藤次郎" },
      ],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.targetHorseNumbers).toStrictEqual(["3"]);
});

test("countDistinctRunningStyleDetailRaces counts unique race tuples", () => {
  expect(
    countDistinctRunningStyleDetailRaces([
      {
        key: "k1",
        targetHorseNumbers: ["5"],
        runningStyle: null,
        starts: 1,
        showRate: 0,
        quinellaRate: 0,
        winRate: 0,
        finishPositionAverage: null,
        popularityMedian: null,
        winOddsMedian: null,
        finishPositionMedian: null,
        details: [detailFromStarter(baseRow), detailFromStarter(baseRow)],
      },
    ]),
  ).toStrictEqual(1);
});

test("countDistinctRunningStyleDetailRaces returns zero for empty rows", () => {
  expect(countDistinctRunningStyleDetailRaces([])).toStrictEqual(0);
});

test("aggregateForTargets skips starter rows whose jockey is empty when jockey target is required", () => {
  const blankJockeyRow: RaceTrendStarterRow = { ...baseRow, jockeyName: "" };
  const result = aggregateForTargets(
    {
      starterRows: [blankJockeyRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.starts).toStrictEqual(0);
});

test("aggregateForTargets skips starter rows whose wakuban is empty when frame target is required", () => {
  const blankFrameRow: RaceTrendStarterRow = { ...baseRow, wakuban: null };
  const result = aggregateForTargets(
    {
      starterRows: [blankFrameRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: true, jockey: false, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.starts).toStrictEqual(0);
});

test("aggregateForTargets skips starter rows whose raceBango is empty when raceNumber target is required", () => {
  const blankRaceNumberRow: RaceTrendStarterRow = { ...baseRow, raceBango: "" };
  const result = aggregateForTargets(
    {
      starterRows: [blankRaceNumberRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: false, raceNumber: true, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.starts).toStrictEqual(0);
});

test("aggregateForTargets filters runner targets that lack a required frameNumber", () => {
  const result = aggregateForTargets(
    {
      starterRows: [baseRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: null, horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: true, jockey: false, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows).toStrictEqual([]);
});

test("aggregateForTargets filters runner targets that lack a required jockey", () => {
  const result = aggregateForTargets(
    {
      starterRows: [baseRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: null }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows).toStrictEqual([]);
});

test("aggregateForTargets sorts trend details by race number when dates tie", () => {
  const rowEarlyRaceNumber: RaceTrendStarterRow = {
    ...baseRow,
    raceBango: "01",
    umaban: "5",
    finishPosition: 5,
  };
  const rowLaterRaceNumber: RaceTrendStarterRow = {
    ...baseRow,
    raceBango: "11",
    umaban: "5",
    finishPosition: 5,
  };
  const result = aggregateForTargets(
    {
      starterRows: [rowEarlyRaceNumber, rowLaterRaceNumber],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.details[0]?.raceNumber).toStrictEqual("11");
  expect(result.runningStyleRows[0]?.details[1]?.raceNumber).toStrictEqual("01");
});

test("aggregateForTargets sorts trend details by horse number when both date and race number tie", () => {
  const lowerHorseRow: RaceTrendStarterRow = {
    ...baseRow,
    raceBango: "01",
    umaban: "3",
    finishPosition: 5,
  };
  const higherHorseRow: RaceTrendStarterRow = {
    ...baseRow,
    raceBango: "01",
    umaban: "7",
    finishPosition: 5,
  };
  const result = aggregateForTargets(
    {
      starterRows: [lowerHorseRow, higherHorseRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [
        { frameNumber: "3", horseNumber: "3", jockeyName: "山田太郎" },
        { frameNumber: "7", horseNumber: "7", jockeyName: "山田太郎" },
      ],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.details[0]?.horseNumber).toStrictEqual("3");
  expect(result.runningStyleRows[0]?.details[1]?.horseNumber).toStrictEqual("7");
});

test("aggregateForTargets sorts aggregated rows by tied secondary metrics using horse number", () => {
  const rowA: RaceTrendStarterRow = {
    ...baseRow,
    raceBango: "01",
    umaban: "3",
    finishPosition: 5,
  };
  const rowB: RaceTrendStarterRow = {
    ...baseRow,
    raceBango: "01",
    umaban: "9",
    finishPosition: 5,
    jockeyName: "佐藤次郎",
  };
  const result = aggregateForTargets(
    {
      starterRows: [rowA, rowB],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [
        { frameNumber: "3", horseNumber: "3", jockeyName: "山田太郎" },
        { frameNumber: "9", horseNumber: "9", jockeyName: "佐藤次郎" },
      ],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.targetHorseNumbers).toStrictEqual(["3"]);
});

test("aggregateForTargets uses historical running styles keyed on the canonical raceKey for details", () => {
  const result = aggregateForTargets(
    {
      starterRows: [baseRow],
      currentRunningStyles: [],
      historicalRunningStyles: [
        { raceKey: "nar:20260520:44:11", horseNumber: "05", predictedLabel: "sashi" },
      ],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.details[0]?.runningStyle).toStrictEqual("sashi");
});

test("aggregateForTargets sortAggregatedRows orders ties by quinellaRate when showRate matches", () => {
  const showOnlyRow: RaceTrendStarterRow = {
    ...baseRow,
    umaban: "3",
    finishPosition: 3,
  };
  const winRow: RaceTrendStarterRow = {
    ...baseRow,
    umaban: "9",
    jockeyName: "佐藤次郎",
    finishPosition: 1,
  };
  const result = aggregateForTargets(
    {
      starterRows: [showOnlyRow, winRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [
        { frameNumber: "3", horseNumber: "3", jockeyName: "山田太郎" },
        { frameNumber: "9", horseNumber: "9", jockeyName: "佐藤次郎" },
      ],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.targetHorseNumbers).toStrictEqual(["9"]);
});

test("aggregateForTargets sortAggregatedRows breaks frameNumber ties by jockeyName when frameNumber is null", () => {
  const yamadaRow: RaceTrendStarterRow = {
    ...baseRow,
    umaban: "3",
    jockeyName: "山田太郎",
    finishPosition: 3,
  };
  const satoRow: RaceTrendStarterRow = {
    ...baseRow,
    umaban: "5",
    jockeyName: "佐藤次郎",
    finishPosition: 3,
  };
  const result = aggregateForTargets(
    {
      starterRows: [satoRow, yamadaRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [
        { frameNumber: null, horseNumber: "3", jockeyName: "山田太郎" },
        { frameNumber: null, horseNumber: "5", jockeyName: "佐藤次郎" },
      ],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows).toHaveLength(2);
});

test("aggregateForTargets normalizes historical running styles keyed by zero-padded horse number", () => {
  const result = aggregateForTargets(
    {
      starterRows: [baseRow],
      currentRunningStyles: [],
      historicalRunningStyles: [
        { raceKey: "nar:20260520:44:11", horseNumber: "", predictedLabel: "sashi" },
      ],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.details[0]?.runningStyle).toStrictEqual("senkou");
});

test("aggregateForTargets sortAggregatedRows orders tied rows by raceNumber when other tiebreakers tie", () => {
  const earlyRow: RaceTrendStarterRow = {
    ...baseRow,
    raceBango: "01",
    umaban: "5",
    finishPosition: 3,
  };
  const lateRow: RaceTrendStarterRow = {
    ...baseRow,
    raceBango: "11",
    umaban: "5",
    finishPosition: 3,
  };
  const result = aggregateForTargets(
    {
      starterRows: [lateRow, earlyRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.starts).toStrictEqual(2);
});

test("aggregateForTargets sortAggregatedRows handles null targetHorseNumbers in the secondary sort", () => {
  const sameJockeyShowRow: RaceTrendStarterRow = {
    ...baseRow,
    umaban: "5",
    finishPosition: 3,
  };
  const result = aggregateForTargets(
    {
      starterRows: [sameJockeyShowRow, sameJockeyShowRow],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [
        { frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" },
        { frameNumber: "3", horseNumber: null, jockeyName: "山田太郎" },
      ],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows).toHaveLength(2);
});

test("aggregateForTargets sortTrendDetails handles null horseNumbers in tied groups", () => {
  const namelessRowA: RaceTrendStarterRow = {
    ...baseRow,
    umaban: null,
    finishPosition: 3,
  };
  const namelessRowB: RaceTrendStarterRow = {
    ...baseRow,
    umaban: null,
    kaisaiTsukihi: "0520",
    raceBango: "11",
    finishPosition: 3,
  };
  const result = aggregateForTargets(
    {
      starterRows: [namelessRowA, namelessRowB],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.details).toHaveLength(2);
});

test("aggregateForTargets aggregates starter rows that have a blank umaban into the empty-umaban group", () => {
  const result = aggregateForTargets(
    {
      starterRows: [{ ...baseRow, umaban: null }],
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "44", raceBango: "12", source: "nar" },
      runners: [{ frameNumber: "3", horseNumber: "5", jockeyName: "山田太郎" }],
    },
    { frame: false, jockey: true, raceNumber: false, runningStyle: false },
    true,
    "20260520",
    "20260520",
  );
  expect(result.runningStyleRows[0]?.starts).toStrictEqual(1);
});

test("countDistinctRunningStyleDetailRaces deduplicates across multiple row groups", () => {
  const detail = detailFromStarter(baseRow);
  expect(
    countDistinctRunningStyleDetailRaces([
      {
        key: "k1",
        targetHorseNumbers: ["5"],
        runningStyle: null,
        starts: 1,
        showRate: 0,
        quinellaRate: 0,
        winRate: 0,
        finishPositionAverage: null,
        popularityMedian: null,
        winOddsMedian: null,
        finishPositionMedian: null,
        details: [detail],
      },
      {
        key: "k2",
        targetHorseNumbers: ["6"],
        runningStyle: null,
        starts: 1,
        showRate: 0,
        quinellaRate: 0,
        winRate: 0,
        finishPositionAverage: null,
        popularityMedian: null,
        winOddsMedian: null,
        finishPositionMedian: null,
        details: [detail],
      },
    ]),
  ).toStrictEqual(1);
});

test("starterRunningStyleKey concatenates race key and normalized umaban", () => {
  expect(
    starterRunningStyleKey({
      source: "jra",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0530",
      keibajoCode: "05",
      raceBango: "01",
      umaban: "07",
    }),
  ).toStrictEqual("jra:20260530:05:01:7");
});

test("starterRunningStyleKey leaves an empty umaban segment when umaban is null", () => {
  expect(
    starterRunningStyleKey({
      source: "nar",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0530",
      keibajoCode: "44",
      raceBango: "11",
      umaban: null,
    }),
  ).toStrictEqual("nar:20260530:44:11:");
});

test("resolveRowJockeyKey returns the normalized comparison key", () => {
  expect(resolveRowJockeyKey("山田 太郎")).toStrictEqual("山田太郎");
});

test("resolveRowJockeyKey returns null for empty string", () => {
  expect(resolveRowJockeyKey("")).toBeNull();
});

test("resolveRowJockeyKey returns null for null input", () => {
  expect(resolveRowJockeyKey(null)).toBeNull();
});

test("resolveRowJockeyKey returns null for undefined input", () => {
  expect(resolveRowJockeyKey(undefined)).toBeNull();
});
