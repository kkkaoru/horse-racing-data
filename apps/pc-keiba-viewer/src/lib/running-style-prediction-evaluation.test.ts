// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import { scaleRunningStyleEvaluationFromCM } from "./running-style-prediction-evaluation";

it("scaleRunningStyleEvaluationFromCM returns empty metrics when predictionCount is 0", () => {
  const result = scaleRunningStyleEvaluationFromCM({
    cm: [
      [0, 0, 0, 0],
      [0, 0, 0, 0],
      [0, 0, 0, 0],
      [0, 0, 0, 0],
    ],
    logLossCountByClass: { nige: 0, oikomi: 0, sashi: 0, senkou: 0 },
    logLossSumByClass: { nige: 0, oikomi: 0, sashi: 0, senkou: 0 },
    predictionCount: 0,
    raceCount: 0,
    top2HitCount: 0,
  });
  expect(result).toStrictEqual({
    accuracy: 0,
    accuracyCI: { lower: 0, upper: 0 },
    confusionMatrix: [
      [0, 0, 0, 0],
      [0, 0, 0, 0],
      [0, 0, 0, 0],
      [0, 0, 0, 0],
    ],
    corner1PairScore: { pairCount: 0, score: null },
    corner3PairScore: { pairCount: 0, score: null },
    corner4PairScore: { pairCount: 0, score: null },
    finishPairScore: { pairCount: 0, score: null },
    macroF1: null,
    overallLogLoss: null,
    perClass: {
      nige: { accuracy: null, f1: null, precision: null, recall: null, support: 0 },
      oikomi: { accuracy: null, f1: null, precision: null, recall: null, support: 0 },
      sashi: { accuracy: null, f1: null, precision: null, recall: null, support: 0 },
      senkou: { accuracy: null, f1: null, precision: null, recall: null, support: 0 },
    },
    perClassLogLoss: { nige: null, oikomi: null, sashi: null, senkou: null },
    predictionCount: 0,
    qwk: 0,
    raceCount: 0,
    smallSampleWarning: true,
    top2Accuracy: 0,
    weightedF1: null,
  });
});

it("scaleRunningStyleEvaluationFromCM combines all derived metrics on the happy path", () => {
  const result = scaleRunningStyleEvaluationFromCM({
    cm: [
      [10, 0, 0, 0],
      [0, 10, 0, 0],
      [0, 0, 10, 0],
      [0, 0, 0, 10],
    ],
    logLossCountByClass: { nige: 10, oikomi: 10, sashi: 10, senkou: 10 },
    logLossSumByClass: { nige: 5, oikomi: 8, sashi: 6, senkou: 4 },
    predictionCount: 40,
    raceCount: 5,
    top2HitCount: 38,
    corner1PairScoreSum: 18,
    corner1PairScoreCount: 20,
    corner3PairScoreSum: 17,
    corner3PairScoreCount: 20,
    corner4PairScoreSum: 16,
    corner4PairScoreCount: 20,
    finishPairScoreSum: 15,
    finishPairScoreCount: 20,
  });
  expect(result.accuracy).toBe(1);
  expect(result.confusionMatrix).toStrictEqual([
    [10, 0, 0, 0],
    [0, 10, 0, 0],
    [0, 0, 10, 0],
    [0, 0, 0, 10],
  ]);
  expect(result.macroF1).toBe(1);
  expect(result.weightedF1).toBe(1);
  expect(result.qwk).toBe(1);
  expect(result.perClass).toStrictEqual({
    nige: { accuracy: 1, f1: 1, precision: 1, recall: 1, support: 10 },
    oikomi: { accuracy: 1, f1: 1, precision: 1, recall: 1, support: 10 },
    sashi: { accuracy: 1, f1: 1, precision: 1, recall: 1, support: 10 },
    senkou: { accuracy: 1, f1: 1, precision: 1, recall: 1, support: 10 },
  });
  expect(result.corner1PairScore).toStrictEqual({ pairCount: 20, score: 0.9 });
  expect(result.corner3PairScore).toStrictEqual({ pairCount: 20, score: 0.85 });
  expect(result.corner4PairScore).toStrictEqual({ pairCount: 20, score: 0.8 });
  expect(result.finishPairScore).toStrictEqual({ pairCount: 20, score: 0.75 });
  expect(result.perClassLogLoss).toStrictEqual({
    nige: 0.5,
    oikomi: 0.8,
    sashi: 0.6,
    senkou: 0.4,
  });
  expect(result.overallLogLoss).toBe(0.575);
  expect(result.top2Accuracy).toBe(0.95);
  expect(result.predictionCount).toBe(40);
  expect(result.raceCount).toBe(5);
  expect(result.smallSampleWarning).toBe(false);
});

it("scaleRunningStyleEvaluationFromCM keeps Wilson CI lower<=center<=upper on aggregated trace and trials", () => {
  const result = scaleRunningStyleEvaluationFromCM({
    cm: [
      [70, 5, 3, 2],
      [4, 60, 6, 0],
      [3, 4, 55, 8],
      [1, 2, 5, 72],
    ],
    logLossCountByClass: { nige: 80, oikomi: 80, sashi: 70, senkou: 70 },
    logLossSumByClass: { nige: 32, oikomi: 40, sashi: 28, senkou: 35 },
    predictionCount: 300,
    raceCount: 25,
    top2HitCount: 285,
  });
  expect(result.accuracyCI.lower < result.accuracy).toBe(true);
  expect(result.accuracyCI.upper > result.accuracy).toBe(true);
  expect(result.accuracyCI.lower >= 0).toBe(true);
  expect(result.accuracyCI.upper <= 1).toBe(true);
});

it("scaleRunningStyleEvaluationFromCM yields null F1 when per-class support is below threshold", () => {
  const result = scaleRunningStyleEvaluationFromCM({
    cm: [
      [10, 0, 0, 0],
      [0, 0, 1, 0],
      [0, 0, 0, 1],
      [0, 0, 0, 0],
    ],
    logLossCountByClass: { nige: 10, oikomi: 0, sashi: 1, senkou: 1 },
    logLossSumByClass: { nige: 1, oikomi: 0, sashi: 0.1, senkou: 0.1 },
    predictionCount: 12,
    raceCount: 2,
    top2HitCount: 10,
  });
  expect(result.perClass.nige.f1).toBe(1);
  expect(result.perClass.senkou.f1).toBe(null);
  expect(result.perClass.sashi.f1).toBe(null);
  expect(result.perClass.oikomi.f1).toBe(null);
});

it("scaleRunningStyleEvaluationFromCM marks small sample warning when predictionCount is 15", () => {
  const result = scaleRunningStyleEvaluationFromCM({
    cm: [
      [5, 0, 0, 0],
      [0, 5, 0, 0],
      [0, 0, 3, 0],
      [0, 0, 0, 2],
    ],
    logLossCountByClass: { nige: 5, oikomi: 2, sashi: 3, senkou: 5 },
    logLossSumByClass: { nige: 0.5, oikomi: 0.4, sashi: 0.3, senkou: 0.5 },
    predictionCount: 15,
    raceCount: 2,
    top2HitCount: 14,
  });
  expect(result.smallSampleWarning).toBe(true);
});

it("scaleRunningStyleEvaluationFromCM marks small sample warning false when predictionCount is 30", () => {
  const result = scaleRunningStyleEvaluationFromCM({
    cm: [
      [10, 0, 0, 0],
      [0, 8, 0, 0],
      [0, 0, 6, 0],
      [0, 0, 0, 6],
    ],
    logLossCountByClass: { nige: 10, oikomi: 6, sashi: 6, senkou: 8 },
    logLossSumByClass: { nige: 1, oikomi: 0.6, sashi: 0.6, senkou: 0.8 },
    predictionCount: 30,
    raceCount: 5,
    top2HitCount: 28,
  });
  expect(result.smallSampleWarning).toBe(false);
});

it("scaleRunningStyleEvaluationFromCM computes top-2 accuracy as hitCount divided by total", () => {
  const result = scaleRunningStyleEvaluationFromCM({
    cm: [
      [9, 1, 0, 0],
      [1, 8, 1, 0],
      [0, 1, 7, 2],
      [0, 0, 1, 9],
    ],
    logLossCountByClass: { nige: 10, oikomi: 10, sashi: 10, senkou: 10 },
    logLossSumByClass: { nige: 1, oikomi: 1.2, sashi: 1.5, senkou: 1.1 },
    predictionCount: 40,
    raceCount: 5,
    top2HitCount: 30,
  });
  expect(result.top2Accuracy).toBe(0.75);
});

it("scaleRunningStyleEvaluationFromCM derives per-class log loss averages on mixed counts", () => {
  const result = scaleRunningStyleEvaluationFromCM({
    cm: [
      [4, 1, 0, 0],
      [1, 3, 0, 0],
      [0, 0, 2, 0],
      [0, 0, 0, 0],
    ],
    logLossCountByClass: { nige: 5, oikomi: 0, sashi: 2, senkou: 4 },
    logLossSumByClass: { nige: 2.5, oikomi: 0, sashi: 1, senkou: 2 },
    predictionCount: 11,
    raceCount: 1,
    top2HitCount: 10,
  });
  expect(result.perClassLogLoss).toStrictEqual({
    nige: 0.5,
    oikomi: null,
    sashi: 0.5,
    senkou: 0.5,
  });
  expect(result.overallLogLoss).toBe(0.5);
});
