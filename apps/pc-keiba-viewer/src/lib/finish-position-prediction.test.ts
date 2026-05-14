import { describe, expect, it } from "vitest";

import { buildFinishPredictionRowsFromResults } from "./finish-position-prediction";
import type { HorseRaceResult, Runner } from "./race-types";

const runner = (overrides: Partial<Runner>): Runner => ({
  bamei: "テストホース",
  barei: "4",
  banushimei: null,
  bataiju: null,
  chokyoshimeiRyakusho: "調教師",
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  futanJuryo: null,
  kakuteiChakujun: null,
  kettoTorokuBango: null,
  kishumeiRyakusho: "騎手",
  kohan3f: null,
  seibetsuCode: "1",
  sohaTime: null,
  tanshoNinkijun: "01",
  tanshoOdds: "0020",
  timeSa: null,
  umaban: "01",
  wakuban: null,
  zogenFugo: null,
  zogenSa: null,
  ...overrides,
});

const result = (overrides: Partial<HorseRaceResult>): HorseRaceResult => ({
  babajotaiCodeDirt: null,
  babajotaiCodeShiba: null,
  bamei: "テストホース",
  barei: "4",
  banushimei: null,
  bataiju: null,
  chokyoshimeiRyakusho: "調教師",
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  currentBarei: "4",
  currentJockey: "騎手",
  currentSeibetsuCode: "1",
  currentUmaban: "01",
  futanJuryo: null,
  gradeCode: null,
  hassoJikoku: null,
  kaisaiNen: "2025",
  kaisaiTsukihi: "0514",
  kakuteiChakujun: "01",
  keibajoCode: "05",
  kettoTorokuBango: "1",
  kishumeiRyakusho: "騎手",
  kohan3f: null,
  kyori: "1600",
  kyosoJokenCode: null,
  kyosoJokenMeisho: null,
  kyosoKigoCode: null,
  kyosoShubetsuCode: null,
  kyosomeiFukudai: null,
  kyosomeiHondai: null,
  kyosomeiKakkonai: null,
  raceBango: "01",
  seibetsuCode: "1",
  shussoTosu: "10",
  sohaTime: null,
  tanshoNinkijun: "01",
  tanshoOdds: "0020",
  tenkoCode: null,
  timeSa: null,
  trackCode: "17",
  umaban: "01",
  wakuban: null,
  zogenFugo: null,
  zogenSa: null,
  juryoShubetsuCode: null,
  ...overrides,
});

describe("buildFinishPredictionRowsFromResults", () => {
  it("orders runners by past finish, popularity, and odds", () => {
    const rows = buildFinishPredictionRowsFromResults({
      currentDistance: "1600",
      currentKeibajoCode: "05",
      currentRaceDate: "20260514",
      currentSource: "jra",
      currentTrackCode: "17",
      results: [
        result({ currentUmaban: "01", kakuteiChakujun: "01", shussoTosu: "10" }),
        result({ currentUmaban: "02", kakuteiChakujun: "09", shussoTosu: "10" }),
      ],
      runners: [
        runner({ bamei: "先着馬", tanshoNinkijun: "01", tanshoOdds: "0020", umaban: "01" }),
        runner({ bamei: "後着馬", tanshoNinkijun: "08", tanshoOdds: "0120", umaban: "02" }),
      ],
    });

    expect(rows.map((row) => row.horseNumber)).toEqual(["1", "2"]);
    expect(rows[0]?.predictedRank).toBe(1);
    expect(rows[0]?.score).toBeGreaterThan(rows[1]?.score ?? 0);
    expect(rows[0]?.storedPopularity).toBe(1);
    expect(rows[0]?.storedOdds).toBe(2);
  });

  it("supports ban-ei races without corner-specific requirements", () => {
    const rows = buildFinishPredictionRowsFromResults({
      currentDistance: "200",
      currentKeibajoCode: "83",
      currentRaceDate: "20260514",
      currentSource: "nar",
      currentTrackCode: null,
      results: [result({ currentUmaban: "01", kakuteiChakujun: "02", kyori: "200" })],
      runners: [runner({ umaban: "01" })],
    });

    expect(rows).toHaveLength(1);
    expect(rows[0]?.predictedRank).toBe(1);
  });

  it("uses similarity and model predictions when available", () => {
    const rows = buildFinishPredictionRowsFromResults({
      currentDistance: "1400",
      currentKeibajoCode: "35",
      currentRaceDate: "20260514",
      currentSource: "nar",
      currentTrackCode: "24",
      modelPredictionFeatures: [
        {
          horseNumber: "01",
          modelVersion: "test-model",
          predictedFinishNorm: 0.1,
          showProbability: 0.8,
          winProbability: 0.4,
        },
      ],
      results: [
        result({
          currentUmaban: null,
          kakuteiChakujun: "01",
          keibajoCode: "35",
          kyori: "1400",
          trackCode: "24",
        }),
      ],
      runners: [runner({ bamei: "モデル馬", tanshoNinkijun: "00", tanshoOdds: "0000" })],
      similarityFeatures: [
        {
          averageFinishPosition: 1,
          horseNumber: "01",
          neighborCount: 12,
          showRate: 0.75,
          similarityScore: 0.9,
          winRate: 0.25,
        },
      ],
    });

    expect(rows[0]?.details.some((detail) => detail.reason.includes("test-model"))).toBe(true);
    expect(rows[0]?.winProbability).toBeGreaterThan(0);
  });

  it("falls back to a neutral score when no usable data exists", () => {
    const rows = buildFinishPredictionRowsFromResults({
      currentDistance: null,
      currentKeibajoCode: "05",
      currentRaceDate: "20260514",
      currentSource: "jra",
      results: [],
      runners: [runner({ tanshoNinkijun: "00", tanshoOdds: "0000", umaban: "03" })],
    });

    expect(rows[0]?.horseNumber).toBe("3");
    expect(rows[0]?.score).toBe(0.5);
  });
});
