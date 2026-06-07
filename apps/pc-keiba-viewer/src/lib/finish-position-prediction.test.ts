import { describe, expect, it } from "vitest";

import {
  buildFinishPredictionMarketOverrides,
  buildFinishPredictionRowsFromResults,
  type FinishPredictionBuildInputs,
} from "./finish-position-prediction";
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
  damSireName: null,
  futanJuryo: null,
  kakuteiChakujun: null,
  kettoTorokuBango: null,
  kishumeiRyakusho: "騎手",
  kohan3f: null,
  seibetsuCode: "1",
  sireName: null,
  sireSireName: null,
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

  it("boosts jockeys who already won at the same venue on the same day", () => {
    const rows = buildFinishPredictionRowsFromResults({
      currentDistance: "1400",
      currentKeibajoCode: "45",
      currentRaceDate: "20260514",
      currentSource: "nar",
      currentTrackCode: "24",
      results: [],
      runners: [
        runner({
          bamei: "当日勝利騎手",
          kishumeiRyakusho: "山田太郎",
          tanshoNinkijun: "03",
          tanshoOdds: "0060",
          umaban: "01",
        }),
        runner({
          bamei: "通常騎手",
          kishumeiRyakusho: "佐藤次郎",
          tanshoNinkijun: "03",
          tanshoOdds: "0060",
          umaban: "02",
        }),
      ],
      sameDayVenueJockeyWins: [
        {
          jockeyName: "山田太郎",
          latestRaceNumber: "05",
          winCount: 1,
        },
      ],
    });

    expect(rows.map((row) => row.horseName)).toEqual(["当日勝利騎手", "通常騎手"]);
    expect(rows[0]?.details.some((detail) => detail.label === "同日同場の騎手勝利")).toBe(true);
  });

  it("adjusts same-day jockey weight by grade, condition, distance, and history amount", () => {
    const sprintClassRows = buildFinishPredictionRowsFromResults({
      currentDistance: "1200",
      currentKeibajoCode: "35",
      currentKyosoJokenCode: "000",
      currentKyosoJokenMeisho: "C2",
      currentRaceDate: "20260514",
      currentSource: "nar",
      currentTrackCode: "24",
      results: [],
      runners: [runner({ kishumeiRyakusho: "山田太郎", tanshoNinkijun: "00", tanshoOdds: "0000" })],
      sameDayVenueJockeyWins: [{ jockeyName: "山田太郎", latestRaceNumber: "03", winCount: 1 }],
    });
    const gradedLongRows = buildFinishPredictionRowsFromResults({
      currentDistance: "2100",
      currentGradeCode: "B",
      currentKeibajoCode: "45",
      currentRaceDate: "20260514",
      currentSource: "nar",
      currentTrackCode: "24",
      results: Array.from({ length: 6 }, (_, index) =>
        result({
          currentUmaban: "01",
          kakuteiChakujun: "03",
          kaisaiTsukihi: `050${index + 1}`,
          kyori: "2100",
          trackCode: "24",
        }),
      ),
      runners: [runner({ kishumeiRyakusho: "山田太郎", tanshoNinkijun: "00", tanshoOdds: "0000" })],
      sameDayVenueJockeyWins: [{ jockeyName: "山田太郎", latestRaceNumber: "03", winCount: 1 }],
    });

    const sprintWeight = sprintClassRows[0]?.details.find(
      (detail) => detail.label === "同日同場の騎手勝利",
    )?.weight;
    const gradedLongWeight = gradedLongRows[0]?.details.find(
      (detail) => detail.label === "同日同場の騎手勝利",
    )?.weight;

    expect(sprintWeight).toBeGreaterThan(gradedLongWeight ?? 0);
  });

  it("applies refined market weights to NAR non-graded non-sprint races", () => {
    const middleRows = buildFinishPredictionRowsFromResults({
      currentDistance: "1600",
      currentGradeCode: null,
      currentKeibajoCode: "35",
      currentRaceDate: "20260514",
      currentSource: "nar",
      currentTrackCode: "24",
      results: [
        result({ currentUmaban: "01", kakuteiChakujun: "03", keibajoCode: "35", kyori: "1600" }),
        result({ currentUmaban: "01", kakuteiChakujun: "05", keibajoCode: "35", kyori: "1600" }),
      ],
      runners: [runner({ tanshoNinkijun: "02", tanshoOdds: "0040" })],
    });
    const sprintRows = buildFinishPredictionRowsFromResults({
      currentDistance: "1200",
      currentGradeCode: null,
      currentKeibajoCode: "35",
      currentRaceDate: "20260514",
      currentSource: "nar",
      currentTrackCode: "24",
      results: [
        result({ currentUmaban: "01", kakuteiChakujun: "03", keibajoCode: "35", kyori: "1200" }),
        result({ currentUmaban: "01", kakuteiChakujun: "05", keibajoCode: "35", kyori: "1200" }),
      ],
      runners: [runner({ tanshoNinkijun: "02", tanshoOdds: "0040" })],
    });

    const middleOddsWeight = middleRows[0]?.details.find(
      (detail) => detail.label === "単勝",
    )?.weight;
    const sprintOddsWeight = sprintRows[0]?.details.find(
      (detail) => detail.label === "単勝",
    )?.weight;

    expect(middleOddsWeight).toBeGreaterThan(sprintOddsWeight ?? 0);
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

  it("uses LightGBM, LSTM, and Transformer model predictions as an ensemble", () => {
    const rows = buildFinishPredictionRowsFromResults({
      currentDistance: "1600",
      currentKeibajoCode: "05",
      currentRaceDate: "20260514",
      currentSource: "jra",
      modelPredictionFeatures: [
        {
          horseNumber: "01",
          modelVersion: "finish-lightgbm-10y",
          predictedFinishNorm: 0.1,
          showProbability: 0.8,
          winProbability: 0.5,
        },
        {
          horseNumber: "01",
          modelVersion: "finish-lstm-10y",
          predictedFinishNorm: 0.2,
          showProbability: 0.7,
          winProbability: 0.4,
        },
        {
          horseNumber: "01",
          modelVersion: "finish-transformer-10y",
          predictedFinishNorm: 0.15,
          showProbability: 0.75,
          winProbability: 0.45,
        },
      ],
      results: [],
      runners: [runner({ bamei: "モデル統合馬", tanshoNinkijun: "00", tanshoOdds: "0000" })],
    });

    const labels = rows[0]?.details.map((detail) => detail.label) ?? [];
    expect(labels).toContain("LightGBMモデル");
    expect(labels).toContain("LSTMモデル");
    expect(labels).toContain("Transformerモデル");
  });

  it("keeps generic model labels and ignores null model values", () => {
    const rows = buildFinishPredictionRowsFromResults({
      currentDistance: "1600",
      currentKeibajoCode: "05",
      currentRaceDate: "20260514",
      currentSource: "jra",
      modelPredictionFeatures: [
        {
          horseNumber: "01",
          modelVersion: "finish-generic-10y",
          predictedFinishNorm: 0.2,
          showProbability: null,
          winProbability: null,
        },
        {
          horseNumber: "01",
          modelVersion: "finish-lightgbm-null",
          predictedFinishNorm: null,
          showProbability: null,
          winProbability: null,
        },
      ],
      results: [],
      runners: [runner({ bamei: "汎用モデル馬", tanshoNinkijun: "00", tanshoOdds: "0000" })],
    });

    expect(rows[0]?.details.some((detail) => detail.label === "モデル")).toBe(true);
    expect(rows[0]?.details.some((detail) => detail.reason.includes("finish-lightgbm-null"))).toBe(
      false,
    );
  });

  it("keeps base history weights for moderate history and skips blank model horse numbers", () => {
    const rows = buildFinishPredictionRowsFromResults({
      currentDistance: "1600",
      currentKeibajoCode: "05",
      currentRaceDate: "20260514",
      currentSource: "jra",
      modelPredictionFeatures: [
        {
          horseNumber: "",
          modelVersion: "finish-lightgbm-blank",
          predictedFinishNorm: 0.01,
          showProbability: 0.9,
          winProbability: 0.9,
        },
      ],
      results: Array.from({ length: 3 }, (_, index) =>
        result({
          currentUmaban: "01",
          kakuteiChakujun: "03",
          kaisaiTsukihi: `050${index + 1}`,
        }),
      ),
      runners: [runner({ bamei: "中程度履歴馬", tanshoNinkijun: "00", tanshoOdds: "0000" })],
    });

    expect(rows[0]?.details.some((detail) => detail.reason.includes("finish-lightgbm-blank"))).toBe(
      false,
    );
    expect(rows[0]?.details.find((detail) => detail.label === "競走成績")?.weight).toBeGreaterThan(
      0,
    );
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

  it("boosts modelWeight by 4x and zeroes odds and popularity weights for JRA new-horse maiden races", () => {
    const rows = buildFinishPredictionRowsFromResults({
      currentDistance: "1600",
      currentKeibajoCode: "09",
      currentKyosoJokenCode: "701",
      currentRaceDate: "20260607",
      currentSource: "jra",
      modelPredictionFeatures: [
        {
          horseNumber: "01",
          modelVersion: "iter14-jra-generic",
          predictedFinishNorm: 0.1,
          showProbability: 0.6,
          winProbability: 0.4,
        },
      ],
      results: [],
      runners: [runner({ tanshoNinkijun: "01", tanshoOdds: "0020", umaban: "01" })],
    });

    const modelWeight = rows[0]?.details.find((detail) => detail.label === "モデル")?.weight;
    const popularityWeight = rows[0]?.details.find((detail) => detail.label === "人気")?.weight;
    const oddsWeight = rows[0]?.details.find((detail) => detail.label === "単勝")?.weight;

    expect(modelWeight).toBe(0.32);
    expect(popularityWeight).toBe(0);
    expect(oddsWeight).toBe(0);
  });

  it("forces oddsWeight and popularityWeight to exactly zero for NAR new-horse maiden races with no prior results", () => {
    const rows = buildFinishPredictionRowsFromResults({
      currentDistance: "1600",
      currentKeibajoCode: "35",
      currentKyosoJokenCode: "701",
      currentRaceDate: "20260607",
      currentSource: "nar",
      currentTrackCode: "24",
      modelPredictionFeatures: [
        {
          horseNumber: "01",
          modelVersion: "iter12-nar-generic",
          predictedFinishNorm: 0.1,
          showProbability: 0.6,
          winProbability: 0.4,
        },
      ],
      results: [],
      runners: [runner({ tanshoNinkijun: "01", tanshoOdds: "0020", umaban: "01" })],
    });

    const modelWeight = rows[0]?.details.find((detail) => detail.label === "モデル")?.weight;
    const popularityWeight = rows[0]?.details.find((detail) => detail.label === "人気")?.weight;
    const oddsWeight = rows[0]?.details.find((detail) => detail.label === "単勝")?.weight;

    expect(modelWeight).toBe(0.24);
    expect(popularityWeight).toBe(0);
    expect(oddsWeight).toBe(0);
  });

  it("applies odds correction by default for non-maiden race", () => {
    const rows = buildFinishPredictionRowsFromResults({
      currentDistance: "1600",
      currentKeibajoCode: "05",
      currentKyosoJokenCode: "703",
      currentRaceDate: "20260607",
      currentSource: "jra",
      results: [],
      runners: [runner({ tanshoNinkijun: "01", tanshoOdds: "0020", umaban: "01" })],
    });

    const oddsWeight = rows[0]?.details.find((detail) => detail.label === "単勝")?.weight;

    expect(oddsWeight).toBe(0.15);
  });

  it("skips odds correction by default for new-horse maiden race", () => {
    const rows = buildFinishPredictionRowsFromResults({
      currentDistance: "1600",
      currentKeibajoCode: "05",
      currentKyosoJokenCode: "701",
      currentRaceDate: "20260607",
      currentSource: "jra",
      results: [],
      runners: [runner({ tanshoNinkijun: "01", tanshoOdds: "0020", umaban: "01" })],
    });

    const oddsWeight = rows[0]?.details.find((detail) => detail.label === "単勝")?.weight;

    expect(oddsWeight).toBe(0);
  });

  it("odds correction default is overridable by explicit oddsCorrectionEnabled flag", () => {
    const maidenWithOddsOn = buildFinishPredictionRowsFromResults({
      currentDistance: "1600",
      currentKeibajoCode: "05",
      currentKyosoJokenCode: "701",
      currentRaceDate: "20260607",
      currentSource: "jra",
      oddsCorrectionEnabled: true,
      results: [],
      runners: [runner({ tanshoNinkijun: "01", tanshoOdds: "0020", umaban: "01" })],
    });
    const nonMaidenWithOddsOff = buildFinishPredictionRowsFromResults({
      currentDistance: "1600",
      currentKeibajoCode: "05",
      currentKyosoJokenCode: "703",
      currentRaceDate: "20260607",
      currentSource: "jra",
      oddsCorrectionEnabled: false,
      results: [],
      runners: [runner({ tanshoNinkijun: "01", tanshoOdds: "0020", umaban: "01" })],
    });

    const maidenOddsWeight = maidenWithOddsOn[0]?.details.find(
      (detail) => detail.label === "単勝",
    )?.weight;
    const maidenPopularityWeight = maidenWithOddsOn[0]?.details.find(
      (detail) => detail.label === "人気",
    )?.weight;
    const nonMaidenOddsWeight = nonMaidenWithOddsOff[0]?.details.find(
      (detail) => detail.label === "単勝",
    )?.weight;
    const nonMaidenPopularityWeight = nonMaidenWithOddsOff[0]?.details.find(
      (detail) => detail.label === "人気",
    )?.weight;

    expect(maidenOddsWeight).toBe(0.15);
    expect(maidenPopularityWeight).toBe(0.04);
    expect(nonMaidenOddsWeight).toBe(0);
    expect(nonMaidenPopularityWeight).toBe(0);
  });

  it("toggling odds correction OFF zeroes popularity for non-maiden race", () => {
    const rows = buildFinishPredictionRowsFromResults({
      currentDistance: "1600",
      currentKeibajoCode: "05",
      currentKyosoJokenCode: "703",
      currentRaceDate: "20260607",
      currentSource: "jra",
      oddsCorrectionEnabled: false,
      results: [],
      runners: [runner({ tanshoNinkijun: "01", tanshoOdds: "0020", umaban: "01" })],
    });

    const oddsWeight = rows[0]?.details.find((detail) => detail.label === "単勝")?.weight;
    const popularityWeight = rows[0]?.details.find((detail) => detail.label === "人気")?.weight;

    expect(oddsWeight).toBe(0);
    expect(popularityWeight).toBe(0);
  });

  it("toggling odds correction ON enables popularity for new-horse maiden race", () => {
    const rows = buildFinishPredictionRowsFromResults({
      currentDistance: "1600",
      currentKeibajoCode: "05",
      currentKyosoJokenCode: "701",
      currentRaceDate: "20260607",
      currentSource: "jra",
      oddsCorrectionEnabled: true,
      results: [],
      runners: [runner({ tanshoNinkijun: "01", tanshoOdds: "0020", umaban: "01" })],
    });

    const oddsWeight = rows[0]?.details.find((detail) => detail.label === "単勝")?.weight;
    const popularityWeight = rows[0]?.details.find((detail) => detail.label === "人気")?.weight;

    expect(oddsWeight).toBe(0.15);
    expect(popularityWeight).toBe(0.04);
  });

  it("keeps base modelWeight when kyosoJokenCode is not the new-horse maiden code", () => {
    const rows = buildFinishPredictionRowsFromResults({
      currentDistance: "1600",
      currentKeibajoCode: "09",
      currentKyosoJokenCode: "703",
      currentRaceDate: "20260607",
      currentSource: "jra",
      modelPredictionFeatures: [
        {
          horseNumber: "01",
          modelVersion: "iter14-jra-generic",
          predictedFinishNorm: 0.1,
          showProbability: 0.6,
          winProbability: 0.4,
        },
      ],
      results: [],
      runners: [runner({ tanshoNinkijun: "01", tanshoOdds: "0020", umaban: "01" })],
    });

    const modelWeight = rows[0]?.details.find((detail) => detail.label === "モデル")?.weight;
    const popularityWeight = rows[0]?.details.find((detail) => detail.label === "人気")?.weight;

    expect(modelWeight).toBe(0.08);
    expect(popularityWeight).toBe(0.04);
  });

  it("doubles oddsWeight for JRA runners with no prior results", () => {
    const rows = buildFinishPredictionRowsFromResults({
      currentDistance: "1600",
      currentKeibajoCode: "05",
      currentRaceDate: "20260607",
      currentSource: "jra",
      results: [],
      runners: [runner({ tanshoNinkijun: "01", tanshoOdds: "0020", umaban: "01" })],
    });

    const oddsWeight = rows[0]?.details.find((detail) => detail.label === "単勝")?.weight;

    expect(oddsWeight).toBe(0.15);
  });

  it("doubles oddsWeight for NAR runners with no prior results", () => {
    const rows = buildFinishPredictionRowsFromResults({
      currentDistance: "1600",
      currentKeibajoCode: "35",
      currentRaceDate: "20260607",
      currentSource: "nar",
      currentTrackCode: "24",
      results: [],
      runners: [runner({ tanshoNinkijun: "01", tanshoOdds: "0020", umaban: "01" })],
    });

    const oddsWeight = rows[0]?.details.find((detail) => detail.label === "単勝")?.weight;

    expect(oddsWeight).toBe(0.13);
  });

  it("keeps oddsWeight unchanged for ban-ei runners with no prior results", () => {
    const rows = buildFinishPredictionRowsFromResults({
      currentDistance: "200",
      currentKeibajoCode: "83",
      currentRaceDate: "20260607",
      currentSource: "nar",
      currentTrackCode: null,
      results: [],
      runners: [runner({ tanshoNinkijun: "01", tanshoOdds: "0020", umaban: "01" })],
    });

    const oddsWeight = rows[0]?.details.find((detail) => detail.label === "単勝")?.weight;

    expect(oddsWeight).toBe(0.12);
  });

  it("re-ranks rows when realtime market overrides change popularity and odds", () => {
    const inputs: FinishPredictionBuildInputs = {
      currentDistance: "1600",
      currentKeibajoCode: "05",
      currentRaceDate: "20260523",
      currentSource: "jra",
      results: [],
      runners: [
        runner({ umaban: "01", tanshoNinkijun: "05", tanshoOdds: "0100" }),
        runner({ umaban: "02", bamei: "二号", tanshoNinkijun: "01", tanshoOdds: "0015" }),
      ],
    };
    const storedRows = buildFinishPredictionRowsFromResults({ ...inputs });
    const dynamicRows = buildFinishPredictionRowsFromResults({
      ...inputs,
      marketOverrides: new Map([
        ["1", { odds: 12, popularity: 1 }],
        ["2", { odds: 80, popularity: 10 }],
      ]),
    });
    expect(storedRows[0]?.horseNumber).toBe("2");
    expect(dynamicRows[0]?.horseNumber).toBe("1");
    expect(dynamicRows[0]?.storedPopularity).toBe(1);
    expect(dynamicRows[0]?.storedOdds).toBe(12);
  });
});

describe("buildFinishPredictionMarketOverrides", () => {
  it("strips leading zeros from the horse number key", () => {
    const overrides = buildFinishPredictionMarketOverrides([
      { combination: "07", odds: 4.2, rank: 1 },
    ]);
    expect(overrides.get("7")).toStrictEqual({ odds: 4.2, popularity: 1 });
  });

  it("preserves a combination of all zeros as the literal key", () => {
    const overrides = buildFinishPredictionMarketOverrides([
      { combination: "00", odds: 12, rank: 5 },
    ]);
    expect(overrides.get("00")).toStrictEqual({ odds: 12, popularity: 5 });
  });

  it("defaults odds and popularity to null when the row omits them", () => {
    const overrides = buildFinishPredictionMarketOverrides([{ combination: "3" }]);
    expect(overrides.get("3")).toStrictEqual({ odds: null, popularity: null });
  });
});
