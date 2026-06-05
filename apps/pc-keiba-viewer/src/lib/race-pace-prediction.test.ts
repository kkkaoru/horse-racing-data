import { describe, expect, it } from "vitest";

import {
  applyRunningStyleSortToRacePaceRows,
  buildRacePacePredictionRowsFromResults,
  DEFAULT_RACE_PACE_PREDICTION_MODEL,
  isCornerPacePredictionSupported,
} from "./race-pace-prediction";
import type { HorseRaceResult, RacePacePredictionRow, Runner } from "./race-types";

const runner = (overrides: Partial<Runner>): Runner => ({
  barei: "4",
  bamei: "テストホース",
  banushimei: "テスト馬主",
  bataiju: "480",
  chokyoshimeiRyakusho: "田中",
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  damSireName: null,
  futanJuryo: "560",
  kakuteiChakujun: null,
  kettoTorokuBango: "2020000001",
  kishumeiRyakusho: "山田",
  kohan3f: null,
  seibetsuCode: "1",
  sireName: null,
  sireSireName: null,
  sohaTime: null,
  tanshoNinkijun: "1",
  tanshoOdds: "1.2",
  timeSa: null,
  umaban: "01",
  wakuban: "1",
  zogenFugo: null,
  zogenSa: null,
  ...overrides,
});

const result = (overrides: Partial<HorseRaceResult>): HorseRaceResult => ({
  babajotaiCodeDirt: "1",
  babajotaiCodeShiba: null,
  banushimei: "テスト馬主",
  barei: "4",
  bamei: "テストホース",
  bataiju: "480",
  chokyoshimeiRyakusho: "田中",
  corner1: "01",
  corner2: "02",
  corner3: "02",
  corner4: "03",
  currentBarei: "4",
  currentJockey: "山田",
  currentSeibetsuCode: "1",
  currentUmaban: "01",
  futanJuryo: "560",
  gradeCode: null,
  hassoJikoku: "1200",
  juryoShubetsuCode: null,
  kakuteiChakujun: "1",
  kaisaiNen: "2025",
  kaisaiTsukihi: "1201",
  keibajoCode: "45",
  kettoTorokuBango: "2020000001",
  kishumeiRyakusho: "山田",
  kohan3f: "360",
  kyori: "1200",
  kyosoJokenCode: null,
  kyosoJokenMeisho: null,
  kyosoKigoCode: null,
  kyosoShubetsuCode: null,
  kyosomeiFukudai: null,
  kyosomeiHondai: "テストレース",
  kyosomeiKakkonai: null,
  raceBango: "01",
  seibetsuCode: "1",
  shussoTosu: "16",
  sohaTime: "1123",
  tanshoNinkijun: "1",
  tanshoOdds: "1.2",
  tenkoCode: "1",
  timeSa: null,
  trackCode: "24",
  umaban: "01",
  wakuban: "1",
  zogenFugo: null,
  zogenSa: null,
  ...overrides,
});

describe("race pace prediction", () => {
  it("disables corner prediction for banei and Niigata 1000m races", () => {
    expect(
      isCornerPacePredictionSupported({
        distance: "1200",
        keibajoCode: "83",
        source: "nar",
      }),
    ).toBe(false);
    expect(
      isCornerPacePredictionSupported({
        distance: "1000",
        keibajoCode: "04",
        source: "jra",
      }),
    ).toBe(false);
    expect(
      isCornerPacePredictionSupported({
        distance: "1200",
        keibajoCode: "04",
        source: "jra",
      }),
    ).toBe(true);
  });

  it("recalculates rows from filtered race results with date and distance weighting", () => {
    const rows = buildRacePacePredictionRowsFromResults({
      currentDistance: "1200",
      currentRaceDate: "20260512",
      results: [
        result({ corner1: "02", corner2: "02", corner3: "03", corner4: "03" }),
        result({
          corner1: "04",
          corner2: "04",
          corner3: "04",
          corner4: "04",
          kaisaiTsukihi: "0101",
          kishumeiRyakusho: "別騎手",
          kyori: "1800",
        }),
        result({
          bamei: "外枠ホース",
          corner1: "06",
          corner2: "06",
          corner3: "05",
          corner4: "05",
          currentUmaban: "02",
          kettoTorokuBango: "2020000002",
          umaban: "02",
        }),
      ],
      runners: [
        runner({ bamei: "テストホース", umaban: "01" }),
        runner({ bamei: "外枠ホース", kettoTorokuBango: "2020000002", umaban: "02" }),
      ],
    });

    expect(rows).toHaveLength(2);
    expect(rows[0]?.horseNumber).toBe("1");
    expect(rows[0]?.predictedCorners).not.toBe("-");
    expect(rows[0]?.confidence).toBeGreaterThan(0);
    expect(rows[0]?.details.map((detail) => detail.label)).toEqual([
      "馬自身の通過傾向",
      "騎手との組み合わせ",
      "調教師の傾向",
      "似た出走条件の近傍馬",
      "LightGBMモデル予測",
    ]);
  });

  it("ignores blank horse numbers and empty corner values", () => {
    const rows = buildRacePacePredictionRowsFromResults({
      currentDistance: null,
      currentRaceDate: "20260512",
      results: [
        result({ corner1: "00", corner2: null, corner3: "0", corner4: "-", currentUmaban: "01" }),
        result({ corner1: "01", currentUmaban: null }),
      ],
      runners: [runner({ umaban: "01" })],
    });

    expect(rows).toHaveLength(1);
    expect(rows[0]?.predictedCorners).toBe("-");
    expect(rows[0]?.confidence).toBe(0.13);
    expect(rows[0]?.corner1).toBeNull();
  });

  it("normalizes past corner ranks by runner count before comparing current runners", () => {
    const rows = buildRacePacePredictionRowsFromResults({
      currentDistance: "1200",
      currentRaceDate: "20260512",
      currentRunnerCount: 18,
      model: {
        ...DEFAULT_RACE_PACE_PREDICTION_MODEL,
        jraPopularityPriorFloorWeight: 0,
      },
      results: [
        result({
          corner1: "04",
          currentUmaban: "01",
          shussoTosu: "08",
        }),
        result({
          corner1: "04",
          currentUmaban: "01",
          shussoTosu: "08",
        }),
        result({
          corner1: "04",
          currentUmaban: "01",
          shussoTosu: "08",
        }),
        result({
          corner1: "04",
          currentUmaban: "01",
          shussoTosu: "08",
        }),
      ],
      runners: [runner({ umaban: "01" })],
    });

    expect(rows[0]?.corner1).toBeCloseTo(8.29, 2);
  });

  it("adjusts race pace weights by race source, class, and horse history amount", () => {
    const rows = buildRacePacePredictionRowsFromResults({
      currentConditionCode: "000",
      currentConditionName: "C1",
      currentDistance: null,
      currentRaceDate: "20260512",
      currentSource: "nar",
      results: Array.from({ length: 6 }, (_, index) =>
        result({
          corner1: String(index + 1).padStart(2, "0"),
          currentUmaban: "01",
          kaisaiTsukihi: `01${String(index + 1).padStart(2, "0")}`,
          kyori: null,
        }),
      ),
      runners: [runner({ umaban: "01" })],
    });

    expect(rows[0]?.details[0]?.weight).toBe(0.74);
    expect(rows[0]?.details[1]?.weight).toBe(0.13);
    expect(rows[0]?.corner1).not.toBeNull();
  });

  it("uses model and similarity features as supplemental corner predictions", () => {
    const rows = buildRacePacePredictionRowsFromResults({
      currentDistance: "1200",
      currentRaceDate: "20260512",
      currentSource: "jra",
      currentTrackCode: "23",
      model: {
        ...DEFAULT_RACE_PACE_PREDICTION_MODEL,
        jraModelPredictionWeight: 0.2,
        jraPopularityPriorFloorWeight: 0,
        jraSimilarityWeight: 0.2,
      },
      modelPredictionFeatures: [
        {
          corner1: 3,
          corner2: 3,
          corner3: 4,
          corner4: 4,
          horseNumber: "01",
          modelVersion: "test-model",
        },
      ],
      results: [
        result({
          corner1: "02",
          corner2: "02",
          corner3: "03",
          corner4: "03",
          trackCode: "24",
        }),
      ],
      runners: [runner({ umaban: "01" })],
      similarityFeatures: [
        {
          corner1: 2,
          corner2: 2,
          corner3: 3,
          corner4: 3,
          horseNumber: "01",
          neighborCount: 40,
          similarityScore: 0.9,
        },
      ],
    });

    expect(rows[0]?.details.map((detail) => detail.label)).toContain("LightGBMモデル予測");
    expect(rows[0]?.corner1).toBeGreaterThan(1);
    expect(rows[0]?.corner1).toBeLessThan(3);
  });

  it("adjusts JRA weights for graded races and young or maiden races", () => {
    const gradedRows = buildRacePacePredictionRowsFromResults({
      currentDistance: "1600",
      currentGradeCode: "A",
      currentRaceDate: "20260512",
      currentSource: "jra",
      results: [result({ corner1: "02", kyori: "1600" })],
      runners: [runner({ umaban: "01" })],
    });
    const maidenRows = buildRacePacePredictionRowsFromResults({
      currentConditionName: "3歳未勝利",
      currentDistance: "1600",
      currentRaceAgeCode: "11",
      currentRaceDate: "20260512",
      currentSource: "jra",
      results: [result({ corner1: "02", kyori: "1600" })],
      runners: [runner({ umaban: "01" })],
    });

    expect(gradedRows[0]?.details[0]?.weight).toBe(0.64);
    expect(gradedRows[0]?.details[1]?.weight).toBe(0.19);
    expect(maidenRows[0]?.details[0]?.weight).toBe(0.52);
    expect(maidenRows[0]?.details[1]?.weight).toBe(0.25);
  });

  it("adjusts NAR weights for non-class races and graded races", () => {
    const nonClassRows = buildRacePacePredictionRowsFromResults({
      currentConditionCode: "999",
      currentConditionName: "特別競走",
      currentDistance: "1400",
      currentRaceDate: "20260512",
      currentSource: "nar",
      results: [result({ corner1: "02", kyori: "1400" })],
      runners: [runner({ umaban: "01" })],
    });
    const gradedRows = buildRacePacePredictionRowsFromResults({
      currentConditionCode: "999",
      currentConditionName: "重賞",
      currentDistance: "1400",
      currentGradeCode: "A",
      currentRaceDate: "20260512",
      currentSource: "nar",
      results: [result({ corner1: "02", kyori: "1400" })],
      runners: [runner({ umaban: "01" })],
    });

    expect(nonClassRows[0]?.details[0]?.weight).toBeCloseTo(0.58);
    expect(nonClassRows[0]?.details[1]?.weight).toBeCloseTo(0.21);
    expect(gradedRows[0]?.details[0]?.weight).toBe(0.64);
    expect(gradedRows[0]?.details[2]?.weight).toBeCloseTo(0.17);
  });

  it("handles malformed historical dates and different track groups", () => {
    const rows = buildRacePacePredictionRowsFromResults({
      currentDistance: "1200",
      currentRaceDate: "20260512",
      currentSource: "jra",
      currentTrackCode: "11",
      model: {
        ...DEFAULT_RACE_PACE_PREDICTION_MODEL,
        jraPopularityPriorFloorWeight: 0,
      },
      results: [
        result({
          corner1: "02",
          currentUmaban: "01",
          kaisaiNen: "不正",
          kaisaiTsukihi: "日付",
          trackCode: "24",
        }),
        result({
          corner1: "03",
          currentUmaban: "02",
          trackCode: "24",
        }),
      ],
      runners: [
        runner({ tanshoNinkijun: "00", umaban: "01" }),
        runner({ kettoTorokuBango: "2020000002", tanshoNinkijun: "00", umaban: "02" }),
      ],
    });

    expect(rows.some((row) => row.horseNumber === "1" && row.predictedCorners === "-")).toBe(true);
    expect(rows.find((row) => row.horseNumber === "2")?.corner1).not.toBeNull();
  });
});

const buildRow = (overrides: Partial<RacePacePredictionRow> = {}): RacePacePredictionRow => ({
  confidence: 0,
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  details: [],
  horseName: "",
  horseNumber: "1",
  predictedCorners: "-",
  ...overrides,
});

describe("applyRunningStyleSortToRacePaceRows", () => {
  it("returns rows untouched when no running-style probabilities are provided", () => {
    const rows = [
      buildRow({ horseNumber: "1", corner1: 3 }),
      buildRow({ horseNumber: "2", corner1: 5 }),
    ];
    const sorted = applyRunningStyleSortToRacePaceRows(rows, []);
    expect(sorted).toStrictEqual(rows);
  });

  it("ranks horses by running-style label tier (nige first, oikomi last)", () => {
    const rows = [
      buildRow({ horseNumber: "1", corner1: 5, corner2: 5, corner3: 5, corner4: 5 }),
      buildRow({ horseNumber: "2", corner1: 5, corner2: 5, corner3: 5, corner4: 5 }),
      buildRow({ horseNumber: "3", corner1: 5, corner2: 5, corner3: 5, corner4: 5 }),
    ];
    const sorted = applyRunningStyleSortToRacePaceRows(rows, [
      {
        pNige: 0.9,
        pOikomi: 0.05,
        pSashi: 0.025,
        pSenkou: 0.025,
        predictedLabel: "nige",
        umaban: 1,
      },
      {
        pNige: 0.05,
        pOikomi: 0.9,
        pSashi: 0.025,
        pSenkou: 0.025,
        predictedLabel: "oikomi",
        umaban: 2,
      },
      {
        pNige: 0.05,
        pOikomi: 0.05,
        pSashi: 0.45,
        pSenkou: 0.45,
        predictedLabel: "sashi",
        umaban: 3,
      },
    ]);
    expect(sorted.map((row) => row.horseNumber)).toStrictEqual(["1", "3", "2"]);
  });

  it("keeps a nige-labelled horse ahead of a senkou-labelled horse even if scores are similar", () => {
    const rows = [
      buildRow({ horseNumber: "1", corner1: 5, corner2: 5, corner3: 5, corner4: 5 }),
      buildRow({ horseNumber: "2", corner1: 5, corner2: 5, corner3: 5, corner4: 5 }),
    ];
    const sorted = applyRunningStyleSortToRacePaceRows(rows, [
      { pNige: 0.35, pOikomi: 0.1, pSashi: 0.25, pSenkou: 0.3, predictedLabel: "nige", umaban: 1 },
      {
        pNige: 0.3,
        pOikomi: 0.05,
        pSashi: 0.25,
        pSenkou: 0.4,
        predictedLabel: "senkou",
        umaban: 2,
      },
    ]);
    expect(sorted.map((row) => row.horseNumber)).toStrictEqual(["1", "2"]);
  });

  it("blends running-style rank into corner 1 most strongly and corner 4 least", () => {
    const rows = [
      buildRow({ horseNumber: "1", corner1: 9, corner2: 9, corner3: 9, corner4: 9 }),
      buildRow({ horseNumber: "2", corner1: 1, corner2: 1, corner3: 1, corner4: 1 }),
    ];
    const sorted = applyRunningStyleSortToRacePaceRows(rows, [
      {
        pNige: 0.95,
        pOikomi: 0.02,
        pSashi: 0.02,
        pSenkou: 0.01,
        predictedLabel: "nige",
        umaban: 1,
      },
      {
        pNige: 0.0,
        pOikomi: 0.9,
        pSashi: 0.05,
        pSenkou: 0.05,
        predictedLabel: "oikomi",
        umaban: 2,
      },
    ]);
    const nigeHorse = sorted.find((row) => row.horseNumber === "1");
    expect(nigeHorse?.corner1).toBe(3);
    expect(nigeHorse?.corner4).toBe(7.4);
  });

  it("falls back to running-style rank when history corner value is null", () => {
    const rows = [
      buildRow({ horseNumber: "1", corner1: null, corner2: null, corner3: null, corner4: null }),
    ];
    const sorted = applyRunningStyleSortToRacePaceRows(rows, [
      { pNige: 0.9, pOikomi: 0.02, pSashi: 0.04, pSenkou: 0.04, predictedLabel: "nige", umaban: 1 },
    ]);
    expect(sorted[0]?.corner1).toBe(1);
    expect(sorted[0]?.corner4).toBe(1);
  });

  it("preserves rows that have no matching probability entry", () => {
    const rows = [
      buildRow({ horseNumber: "1", corner1: 4 }),
      buildRow({ horseNumber: "2", corner1: 7 }),
    ];
    const sorted = applyRunningStyleSortToRacePaceRows(rows, [
      {
        pNige: 0.95,
        pOikomi: 0.02,
        pSashi: 0.02,
        pSenkou: 0.01,
        predictedLabel: "nige",
        umaban: 1,
      },
    ]);
    expect(sorted.find((row) => row.horseNumber === "2")?.corner1).toBe(7);
  });

  it("does not produce flat predictedCorners when running-style is applied", () => {
    const rows = [buildRow({ horseNumber: "1", corner1: 1, corner2: 3, corner3: 5, corner4: 7 })];
    const sorted = applyRunningStyleSortToRacePaceRows(rows, [
      { pNige: 0.0, pOikomi: 0.8, pSashi: 0.1, pSenkou: 0.1, predictedLabel: "oikomi", umaban: 1 },
    ]);
    const corners = sorted[0]?.predictedCorners.split("-").map(Number);
    expect(corners?.[0] === corners?.[3]).toBe(false);
  });
});
