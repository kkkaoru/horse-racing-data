// Run with bun (vitest).
import { describe, expect, it } from "vitest";

import {
  aggregateForTargets,
  countDistinctRunningStyleDetailRaces,
  detailFromStarter,
  normalizeNumberText,
  normalizeText,
  parseStoredPopularity,
  parseStoredWinOdds,
  runningStyleFromCorners,
  starterKey,
  starterRaceKey,
} from "./race-trend-aggregate";
import type { RaceTrendStarterRow } from "./race-types";

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

describe("normalize helpers", () => {
  it("trims whitespace and returns null for empty input", () => {
    expect(normalizeText("  hello  ")).toStrictEqual("hello");
    expect(normalizeText("")).toBeNull();
    expect(normalizeText(null)).toBeNull();
  });

  it("strips leading zeros while keeping a single digit zero meaningful", () => {
    expect(normalizeNumberText("03")).toStrictEqual("3");
    expect(normalizeNumberText("0")).toStrictEqual("0");
    expect(normalizeNumberText("")).toBeNull();
  });

  it("parses stored popularity and odds values from zero-padded strings", () => {
    expect(parseStoredPopularity("03")).toStrictEqual(3);
    expect(parseStoredPopularity("00")).toBeNull();
    expect(parseStoredWinOdds("0050")).toStrictEqual(5);
    expect(parseStoredWinOdds("0000")).toBeNull();
  });
});

describe("running style classification", () => {
  it("returns nige for corner 1", () => {
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

  it("returns oikomi for last group", () => {
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

  it("returns null when no corner data available", () => {
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
});

describe("starter keys", () => {
  it("builds deterministic keys for a row", () => {
    expect(starterKey(baseRow)).toStrictEqual("nar:2026:0520:44:11:5");
    expect(starterRaceKey(baseRow)).toStrictEqual("nar:20260520:44:11");
  });
});

describe("detailFromStarter", () => {
  it("includes horse weight and signed delta", () => {
    const detail = detailFromStarter(baseRow);
    expect(detail.horseWeight).toStrictEqual(498);
    expect(detail.horseWeightDelta).toStrictEqual(4);
    expect(detail.popularity).toStrictEqual(3);
    expect(detail.winOdds).toStrictEqual(5);
    expect(detail.date).toStrictEqual("2026-05-20");
  });

  it("handles negative weight delta", () => {
    const negativeRow: RaceTrendStarterRow = {
      ...baseRow,
      zogenFugo: "-",
      zogenSa: "002",
    };
    expect(detailFromStarter(negativeRow).horseWeightDelta).toStrictEqual(-2);
  });

  it("returns null weight when bataiju is missing", () => {
    const missingRow: RaceTrendStarterRow = {
      ...baseRow,
      bataiju: null,
      zogenFugo: null,
      zogenSa: null,
    };
    expect(detailFromStarter(missingRow).horseWeight).toBeNull();
    expect(detailFromStarter(missingRow).horseWeightDelta).toBeNull();
  });
});

describe("aggregateForTargets", () => {
  it("aggregates rows by current runner using jockey trend", () => {
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
    expect(result.runningStyleRows[0]?.details).toHaveLength(1);
  });

  it("returns 0 races when no rows match", () => {
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
});

describe("countDistinctRunningStyleDetailRaces", () => {
  it("counts unique race tuples across rows", () => {
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
});
