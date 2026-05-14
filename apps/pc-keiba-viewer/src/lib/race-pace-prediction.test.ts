import { describe, expect, it } from "vitest";

import {
  buildRacePacePredictionRowsFromResults,
  DEFAULT_RACE_PACE_PREDICTION_MODEL,
  isCornerPacePredictionSupported,
} from "./race-pace-prediction";
import type { HorseRaceResult, Runner } from "./race-types";

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
  futanJuryo: "560",
  kakuteiChakujun: null,
  kettoTorokuBango: "2020000001",
  kishumeiRyakusho: "山田",
  kohan3f: null,
  seibetsuCode: "1",
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
});
