// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import {
  buildRunningStyleBucketFilter,
  deriveAccuracy,
  deriveLogLoss,
  deriveMacroF1,
  derivePerClassMetrics,
  deriveQuadraticWeightedKappa,
  deriveTop2Accuracy,
  deriveWeightedF1,
  deriveWilsonScoreCI,
  getRunningStyleDimensionFlags,
  isSmallSample,
  RUNNING_STYLE_BUCKET_PERIOD_PARAM_NAME,
  RUNNING_STYLE_CLASSES,
  RUNNING_STYLE_PREDICTION_PARAM_NAMES,
} from "./running-style-prediction-dimensions";

it("derivePerClassMetrics returns perfect precision/recall/F1 for identity-shape CM", () => {
  const metrics = derivePerClassMetrics([
    [10, 0, 0, 0],
    [0, 10, 0, 0],
    [0, 0, 10, 0],
    [0, 0, 0, 10],
  ]);
  expect(metrics).toStrictEqual({
    nige: { accuracy: 1, precision: 1, recall: 1, f1: 1, support: 10 },
    senkou: { accuracy: 1, precision: 1, recall: 1, f1: 1, support: 10 },
    sashi: { accuracy: 1, precision: 1, recall: 1, f1: 1, support: 10 },
    oikomi: { accuracy: 1, precision: 1, recall: 1, f1: 1, support: 10 },
  });
});

it("derivePerClassMetrics returns null precision/recall/F1 for an all-zero CM", () => {
  const metrics = derivePerClassMetrics([
    [0, 0, 0, 0],
    [0, 0, 0, 0],
    [0, 0, 0, 0],
    [0, 0, 0, 0],
  ]);
  expect(metrics).toStrictEqual({
    nige: { accuracy: null, precision: null, recall: null, f1: null, support: 0 },
    senkou: { accuracy: null, precision: null, recall: null, f1: null, support: 0 },
    sashi: { accuracy: null, precision: null, recall: null, f1: null, support: 0 },
    oikomi: { accuracy: null, precision: null, recall: null, f1: null, support: 0 },
  });
});

it("derivePerClassMetrics suppresses F1 when class support is below MIN_SUPPORT_FOR_F1", () => {
  const metrics = derivePerClassMetrics([
    [10, 0, 0, 0],
    [0, 0, 5, 0],
    [0, 0, 0, 3],
    [0, 0, 0, 0],
  ]);
  expect(metrics.nige).toStrictEqual({
    accuracy: 1,
    precision: 1,
    recall: 1,
    f1: 1,
    support: 10,
  });
  expect(metrics.senkou).toStrictEqual({
    accuracy: 0,
    precision: null,
    recall: 0,
    f1: null,
    support: 5,
  });
  expect(metrics.sashi).toStrictEqual({
    accuracy: 0,
    precision: 0,
    recall: 0,
    f1: null,
    support: 3,
  });
  expect(metrics.oikomi).toStrictEqual({
    accuracy: null,
    precision: 0,
    recall: null,
    f1: null,
    support: 0,
  });
});

it("derivePerClassMetrics returns nige precision 0.5 with mixed predictions", () => {
  const metrics = derivePerClassMetrics([
    [5, 5, 0, 0],
    [5, 5, 0, 0],
    [0, 0, 5, 5],
    [0, 0, 5, 5],
  ]);
  expect(metrics.nige).toStrictEqual({
    accuracy: 0.5,
    precision: 0.5,
    recall: 0.5,
    f1: 0.5,
    support: 10,
  });
});

it("derivePerClassMetrics returns null F1 when precision and recall are both zero at qualifying support", () => {
  const metrics = derivePerClassMetrics([
    [0, 5, 0, 0],
    [5, 5, 0, 0],
    [0, 0, 10, 0],
    [0, 0, 0, 10],
  ]);
  expect(metrics.nige).toStrictEqual({
    accuracy: 0,
    precision: 0,
    recall: 0,
    f1: null,
    support: 5,
  });
});

it("deriveAccuracy returns 1 for a CM whose mass is on the diagonal", () => {
  expect(
    deriveAccuracy([
      [4, 0, 0, 0],
      [0, 4, 0, 0],
      [0, 0, 4, 0],
      [0, 0, 0, 4],
    ]),
  ).toBe(1);
});

it("deriveAccuracy returns 0 for an empty CM", () => {
  expect(
    deriveAccuracy([
      [0, 0, 0, 0],
      [0, 0, 0, 0],
      [0, 0, 0, 0],
      [0, 0, 0, 0],
    ]),
  ).toBe(0);
});

it("deriveAccuracy returns 0.5 for a CM where half the mass is on the diagonal", () => {
  expect(
    deriveAccuracy([
      [5, 5, 0, 0],
      [5, 5, 0, 0],
      [0, 0, 5, 5],
      [0, 0, 5, 5],
    ]),
  ).toBe(0.5);
});

it("deriveMacroF1 returns 1 for a perfectly classified CM", () => {
  const perClass = derivePerClassMetrics([
    [10, 0, 0, 0],
    [0, 10, 0, 0],
    [0, 0, 10, 0],
    [0, 0, 0, 10],
  ]);
  expect(deriveMacroF1(perClass)).toBe(1);
});

it("derivePerClassMetrics exposes per-actual-class accuracy for each running style", () => {
  const perClass = derivePerClassMetrics([
    [8, 2, 0, 0],
    [1, 9, 0, 0],
    [0, 1, 9, 0],
    [0, 0, 2, 8],
  ]);
  expect(perClass.nige.accuracy).toBeCloseTo(8 / 10, 12);
  expect(perClass.senkou.accuracy).toBeCloseTo(9 / 10, 12);
  expect(perClass.sashi.accuracy).toBeCloseTo(9 / 10, 12);
  expect(perClass.oikomi.accuracy).toBeCloseTo(8 / 10, 12);
});

it("deriveMacroF1 averages all four classes and treats missing low-support F1 as zero", () => {
  const perClass = derivePerClassMetrics([
    [10, 0, 0, 0],
    [0, 10, 0, 0],
    [0, 0, 0, 3],
    [0, 0, 0, 0],
  ]);
  expect(deriveMacroF1(perClass)).toBe(0.5);
});

it("deriveMacroF1 returns 0 when all classes rely on zero_division fallback", () => {
  const perClass = derivePerClassMetrics([
    [0, 0, 0, 0],
    [0, 0, 0, 0],
    [0, 0, 0, 0],
    [0, 0, 0, 0],
  ]);
  expect(deriveMacroF1(perClass)).toBe(0);
});

it("deriveMacroF1 treats precision=0 and recall=0 as zero instead of dropping the class", () => {
  const perClass = derivePerClassMetrics([
    [0, 5, 0, 0],
    [5, 5, 0, 0],
    [0, 0, 10, 0],
    [0, 0, 0, 10],
  ]);
  expect(deriveMacroF1(perClass)).toBe(0.625);
});

it("deriveWeightedF1 weights F1 by support across qualified classes", () => {
  const perClass = derivePerClassMetrics([
    [10, 0, 0, 0],
    [5, 5, 0, 0],
    [0, 0, 10, 0],
    [0, 0, 0, 20],
  ]);
  expect(deriveWeightedF1(perClass)).toBeCloseTo(0.8933333333333333, 12);
});

it("deriveWeightedF1 returns null when no qualified class contributes support", () => {
  const perClass = derivePerClassMetrics([
    [1, 0, 0, 0],
    [0, 1, 0, 0],
    [0, 0, 1, 0],
    [0, 0, 0, 1],
  ]);
  expect(deriveWeightedF1(perClass)).toBe(null);
});

it("deriveQuadraticWeightedKappa returns 1 for a perfectly aligned CM", () => {
  expect(
    deriveQuadraticWeightedKappa([
      [10, 0, 0, 0],
      [0, 10, 0, 0],
      [0, 0, 10, 0],
      [0, 0, 0, 10],
    ]),
  ).toBe(1);
});

it("deriveQuadraticWeightedKappa returns a strongly negative score for nige-oikomi swap", () => {
  expect(
    deriveQuadraticWeightedKappa([
      [0, 0, 0, 10],
      [0, 0, 10, 0],
      [0, 10, 0, 0],
      [10, 0, 0, 0],
    ]),
  ).toBe(-1);
});

it("deriveQuadraticWeightedKappa penalises adjacent senkou-sashi swap less than far swaps", () => {
  const adjacentSwap = deriveQuadraticWeightedKappa([
    [10, 0, 0, 0],
    [0, 0, 10, 0],
    [0, 10, 0, 0],
    [0, 0, 0, 10],
  ]);
  const farSwap = deriveQuadraticWeightedKappa([
    [10, 0, 0, 0],
    [0, 0, 0, 10],
    [0, 0, 10, 0],
    [0, 10, 0, 0],
  ]);
  expect(adjacentSwap > farSwap).toBe(true);
});

it("deriveQuadraticWeightedKappa returns 0 for chance-level uniform predictions", () => {
  expect(
    deriveQuadraticWeightedKappa([
      [3, 3, 3, 3],
      [3, 3, 3, 3],
      [3, 3, 3, 3],
      [3, 3, 3, 3],
    ]),
  ).toBe(0);
});

it("deriveQuadraticWeightedKappa returns 0 for an empty CM", () => {
  expect(
    deriveQuadraticWeightedKappa([
      [0, 0, 0, 0],
      [0, 0, 0, 0],
      [0, 0, 0, 0],
      [0, 0, 0, 0],
    ]),
  ).toBe(0);
});

it("deriveQuadraticWeightedKappa returns 0 when predictions collapse into a single class column", () => {
  expect(
    deriveQuadraticWeightedKappa([
      [4, 0, 0, 0],
      [4, 0, 0, 0],
      [4, 0, 0, 0],
      [4, 0, 0, 0],
    ]),
  ).toBe(0);
});

it("deriveQuadraticWeightedKappa returns 0 when the entire CM mass falls on a single diagonal cell", () => {
  expect(
    deriveQuadraticWeightedKappa([
      [10, 0, 0, 0],
      [0, 0, 0, 0],
      [0, 0, 0, 0],
      [0, 0, 0, 0],
    ]),
  ).toBe(0);
});

it("deriveWilsonScoreCI returns a balanced 95% interval around 0.5 for 50/100", () => {
  const ci = deriveWilsonScoreCI({ successes: 50, trials: 100, confidence: 0.95 });
  expect(ci.lower).toBeCloseTo(0.4038, 4);
  expect(ci.upper).toBeCloseTo(0.5962, 4);
});

it("deriveWilsonScoreCI clamps the lower bound at 0 for zero successes", () => {
  const ci = deriveWilsonScoreCI({ successes: 0, trials: 10, confidence: 0.95 });
  expect(ci.lower).toBe(0);
  expect(ci.upper > 0).toBe(true);
});

it("deriveWilsonScoreCI clamps the upper bound at 1 for full successes", () => {
  const ci = deriveWilsonScoreCI({ successes: 10, trials: 10, confidence: 0.95 });
  expect(ci.upper).toBe(1);
  expect(ci.lower > 0).toBe(true);
});

it("deriveWilsonScoreCI returns a zero-width interval at the origin for trials=0", () => {
  expect(deriveWilsonScoreCI({ successes: 0, trials: 0, confidence: 0.95 })).toStrictEqual({
    lower: 0,
    upper: 0,
  });
});

it("deriveLogLoss returns per-class averages and the overall average across all classes", () => {
  const result = deriveLogLoss({
    sumByClass: { nige: 10, senkou: 20, sashi: 0, oikomi: 30 },
    countByClass: { nige: 5, senkou: 10, sashi: 0, oikomi: 15 },
  });
  expect(result).toStrictEqual({
    perClass: { nige: 2, senkou: 2, sashi: null, oikomi: 2 },
    overall: 2,
  });
});

it("deriveLogLoss returns null per-class and overall for zero-count inputs", () => {
  const result = deriveLogLoss({
    sumByClass: { nige: 0, senkou: 0, sashi: 0, oikomi: 0 },
    countByClass: { nige: 0, senkou: 0, sashi: 0, oikomi: 0 },
  });
  expect(result).toStrictEqual({
    perClass: { nige: null, senkou: null, sashi: null, oikomi: null },
    overall: null,
  });
});

it("deriveTop2Accuracy returns hit-count over total", () => {
  expect(deriveTop2Accuracy({ hitCount: 75, total: 100 })).toBe(0.75);
});

it("deriveTop2Accuracy returns 0 when total is 0", () => {
  expect(deriveTop2Accuracy({ hitCount: 0, total: 0 })).toBe(0);
});

it("isSmallSample keeps numeric input compatible with the prediction-count threshold", () => {
  expect(isSmallSample(15)).toBe(true);
});

it("isSmallSample flags 29 as small", () => {
  expect(isSmallSample(29)).toBe(true);
});

it("isSmallSample does not flag 30 as small", () => {
  expect(isSmallSample(30)).toBe(false);
});

it("isSmallSample does not flag 100 as small", () => {
  expect(isSmallSample(100)).toBe(false);
});

it("isSmallSample flags an object input when raceCount is below the race threshold", () => {
  const perClass = derivePerClassMetrics([
    [10, 0, 0, 0],
    [0, 10, 0, 0],
    [0, 0, 10, 0],
    [0, 0, 0, 10],
  ]);
  expect(isSmallSample({ raceCount: 4, predictionCount: 40, perClass })).toBe(true);
});

it("isSmallSample flags an object input when predictionCount is below the prediction threshold", () => {
  const perClass = derivePerClassMetrics([
    [10, 0, 0, 0],
    [0, 10, 0, 0],
    [0, 0, 10, 0],
    [0, 0, 0, 10],
  ]);
  expect(isSmallSample({ raceCount: 5, predictionCount: 29, perClass })).toBe(true);
});

it("isSmallSample flags an object input when any class support is below the F1 support threshold", () => {
  const perClass = derivePerClassMetrics([
    [10, 0, 0, 0],
    [0, 10, 0, 0],
    [0, 0, 10, 0],
    [0, 0, 0, 4],
  ]);
  expect(isSmallSample({ raceCount: 5, predictionCount: 34, perClass })).toBe(true);
});

it("isSmallSample does not flag an object input when all sample dimensions meet thresholds", () => {
  const perClass = derivePerClassMetrics([
    [10, 0, 0, 0],
    [0, 10, 0, 0],
    [0, 0, 10, 0],
    [0, 0, 0, 10],
  ]);
  expect(isSmallSample({ raceCount: 5, predictionCount: 40, perClass })).toBe(false);
});

it("getRunningStyleDimensionFlags returns all dims ON by default for NAR without grade", () => {
  const flags = getRunningStyleDimensionFlags({
    query: {},
    source: "nar",
    gradeCode: null,
    isBanEi: false,
  });
  expect(flags).toStrictEqual({
    keibajo: true,
    distance: true,
    kyosoShubetsu: true,
    kyosoJoken: false,
    condition: true,
    track: true,
    grade: false,
    raceName: false,
  });
});

it("getRunningStyleDimensionFlags returns the keibajo flag OFF when its query param is 0", () => {
  const flags = getRunningStyleDimensionFlags({
    query: { runningStyleKeibajo: "0" },
    source: "nar",
    gradeCode: null,
    isBanEi: false,
  });
  expect(flags.keibajo).toBe(false);
});

it("getRunningStyleDimensionFlags forces kyosoJoken OFF for NAR even when its query param is on", () => {
  const flags = getRunningStyleDimensionFlags({
    query: { runningStyleJoken: "1" },
    source: "nar",
    gradeCode: null,
    isBanEi: false,
  });
  expect(flags.kyosoJoken).toBe(false);
});

it("getRunningStyleDimensionFlags forces condition and grade OFF for JRA", () => {
  const flags = getRunningStyleDimensionFlags({
    query: {},
    source: "jra",
    gradeCode: "A",
    isBanEi: false,
  });
  expect(flags.condition).toBe(false);
  expect(flags.grade).toBe(false);
});

it("getRunningStyleDimensionFlags forces grade OFF when gradeCode is null for NAR", () => {
  const flags = getRunningStyleDimensionFlags({
    query: {},
    source: "nar",
    gradeCode: null,
    isBanEi: false,
  });
  expect(flags.grade).toBe(false);
});

it("getRunningStyleDimensionFlags forces grade OFF when gradeCode is empty for NAR", () => {
  const flags = getRunningStyleDimensionFlags({
    query: {},
    source: "nar",
    gradeCode: "",
    isBanEi: false,
  });
  expect(flags.grade).toBe(false);
});

it("getRunningStyleDimensionFlags forces raceName OFF for a non-graded NAR race", () => {
  const flags = getRunningStyleDimensionFlags({
    query: {},
    source: "nar",
    gradeCode: "C",
    isBanEi: false,
  });
  expect(flags.raceName).toBe(false);
});

it("getRunningStyleDimensionFlags keeps raceName ON for NAR when gradeCode is A", () => {
  const flags = getRunningStyleDimensionFlags({
    query: {},
    source: "nar",
    gradeCode: "A",
    isBanEi: false,
  });
  expect(flags.raceName).toBe(true);
});

it("getRunningStyleDimensionFlags keeps raceName ON for NAR when gradeCode is F", () => {
  const flags = getRunningStyleDimensionFlags({
    query: {},
    source: "nar",
    gradeCode: "F",
    isBanEi: false,
  });
  expect(flags.raceName).toBe(true);
});

it("getRunningStyleDimensionFlags zeroes every flag when the race is ban-ei", () => {
  const flags = getRunningStyleDimensionFlags({
    query: { runningStyleKeibajo: "1", runningStyleDistance: "1" },
    source: "nar",
    gradeCode: "A",
    isBanEi: true,
  });
  expect(flags).toStrictEqual({
    keibajo: false,
    distance: false,
    kyosoShubetsu: false,
    kyosoJoken: false,
    condition: false,
    track: false,
    grade: false,
    raceName: false,
  });
});

it("getRunningStyleDimensionFlags turns the distance flag OFF when its query param is 0", () => {
  const flags = getRunningStyleDimensionFlags({
    query: { runningStyleDistance: "0" },
    source: "nar",
    gradeCode: null,
    isBanEi: false,
  });
  expect(flags.distance).toBe(false);
});

it("getRunningStyleDimensionFlags turns the kyosoShubetsu flag OFF when its query param is 0", () => {
  const flags = getRunningStyleDimensionFlags({
    query: { runningStyleShubetsu: "0" },
    source: "nar",
    gradeCode: null,
    isBanEi: false,
  });
  expect(flags.kyosoShubetsu).toBe(false);
});

it("getRunningStyleDimensionFlags turns the track flag OFF when its query param is 0", () => {
  const flags = getRunningStyleDimensionFlags({
    query: { runningStyleTrack: "0" },
    source: "nar",
    gradeCode: null,
    isBanEi: false,
  });
  expect(flags.track).toBe(false);
});

it("getRunningStyleDimensionFlags turns the condition flag OFF for NAR when its query param is 0", () => {
  const flags = getRunningStyleDimensionFlags({
    query: { runningStyleCondition: "0" },
    source: "nar",
    gradeCode: null,
    isBanEi: false,
  });
  expect(flags.condition).toBe(false);
});

it("getRunningStyleDimensionFlags turns the grade flag OFF when its query param is 0 for a graded NAR race", () => {
  const flags = getRunningStyleDimensionFlags({
    query: { runningStyleGrade: "0" },
    source: "nar",
    gradeCode: "A",
    isBanEi: false,
  });
  expect(flags.grade).toBe(false);
});

it("getRunningStyleDimensionFlags turns raceName OFF when its query param is 0 for a graded NAR race", () => {
  const flags = getRunningStyleDimensionFlags({
    query: { runningStyleRaceName: "0" },
    source: "nar",
    gradeCode: "A",
    isBanEi: false,
  });
  expect(flags.raceName).toBe(false);
});

it("getRunningStyleDimensionFlags reads the first element when the query value is an array", () => {
  const flags = getRunningStyleDimensionFlags({
    query: { runningStyleKeibajo: ["0", "1"] },
    source: "nar",
    gradeCode: null,
    isBanEi: false,
  });
  expect(flags.keibajo).toBe(false);
});

it("buildRunningStyleBucketFilter builds a JRA filter with conditionKey null and category jra", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "jra",
      keibajoCode: "05",
      kyori: 2400,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: "005",
      kyosoJokenMeisho: "3歳未勝利",
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: true,
      condition: false,
      track: true,
      grade: false,
      raceName: false,
    },
  });
  expect(filter).toStrictEqual({
    category: "jra",
    source: "jra",
    keibajoCode: "05",
    kyori: 2400,
    kyosoShubetsuCode: "11",
    kyosoJokenCode: "005",
    conditionKey: null,
    trackCode: "10",
    gradeCode: null,
    raceName: null,
    enabled: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: true,
      condition: false,
      track: true,
      grade: false,
      raceName: false,
    },
    period: "all",
  });
});

it("buildRunningStyleBucketFilter resolves the NAR conditionKey from a trimmed kyosoJokenMeisho", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "nar",
      keibajoCode: "30",
      kyori: 1600,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: "  B3  ",
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: true,
      track: true,
      grade: false,
      raceName: false,
    },
  });
  expect(filter.category).toBe("nar");
  expect(filter.conditionKey).toBe("B3");
});

it("buildRunningStyleBucketFilter returns conditionKey null when kyosoJokenMeisho is null for NAR", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "nar",
      keibajoCode: "30",
      kyori: 1600,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: null,
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: true,
      track: true,
      grade: false,
      raceName: false,
    },
  });
  expect(filter.conditionKey).toBe(null);
});

it("buildRunningStyleBucketFilter returns conditionKey null when kyosoJokenMeisho is whitespace-only for NAR", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "nar",
      keibajoCode: "30",
      kyori: 1600,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: "   ",
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: true,
      track: true,
      grade: false,
      raceName: false,
    },
  });
  expect(filter.conditionKey).toBe(null);
});

it("buildRunningStyleBucketFilter clears conditionKey when the condition flag is OFF for a NAR race", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "nar",
      keibajoCode: "30",
      kyori: 1600,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: "B3",
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: true,
      grade: false,
      raceName: false,
    },
  });
  expect(filter.conditionKey).toBe(null);
});

it("buildRunningStyleBucketFilter resolves a trimmed race name for gradeCode A", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "jra",
      keibajoCode: "05",
      kyori: 2500,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: null,
      trackCode: "10",
      gradeCode: "A",
      kyosomeiHondai: "  有馬記念  ",
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: true,
      grade: false,
      raceName: true,
    },
  });
  expect(filter.raceName).toBe("有馬記念");
});

it("buildRunningStyleBucketFilter strips trailing U+3000 padding from kyosomeiHondai", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "jra",
      keibajoCode: "05",
      kyori: 2500,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: null,
      trackCode: "10",
      gradeCode: "A",
      kyosomeiHondai: "有馬記念　　　　　　　　　　",
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: true,
      grade: false,
      raceName: true,
    },
  });
  expect(filter.raceName).toBe("有馬記念");
});

it("buildRunningStyleBucketFilter strips mixed U+3000 and ASCII whitespace padding around kyosomeiHondai", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "nar",
      keibajoCode: "30",
      kyori: 2000,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: "G1",
      trackCode: null,
      gradeCode: "F",
      kyosomeiHondai: " 　 東京大賞典 　 ",
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: false,
      grade: false,
      raceName: true,
    },
  });
  expect(filter.raceName).toBe("東京大賞典");
});

it("buildRunningStyleBucketFilter returns null raceName when kyosomeiHondai is only U+3000 padding", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "jra",
      keibajoCode: "05",
      kyori: 2500,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: null,
      trackCode: "10",
      gradeCode: "A",
      kyosomeiHondai: "　　　　　",
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: true,
      grade: false,
      raceName: true,
    },
  });
  expect(filter.raceName).toBe(null);
});

it("buildRunningStyleBucketFilter clears the race name when gradeCode is C", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "jra",
      keibajoCode: "05",
      kyori: 1600,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: null,
      trackCode: "10",
      gradeCode: "C",
      kyosomeiHondai: "Some Race",
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: true,
      grade: false,
      raceName: true,
    },
  });
  expect(filter.raceName).toBe(null);
});

it("buildRunningStyleBucketFilter returns null raceName when kyosomeiHondai is null for gradeCode A", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "jra",
      keibajoCode: "05",
      kyori: 2500,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: null,
      trackCode: "10",
      gradeCode: "A",
      kyosomeiHondai: null,
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: true,
      grade: false,
      raceName: true,
    },
  });
  expect(filter.raceName).toBe(null);
});

it("buildRunningStyleBucketFilter returns null raceName when kyosomeiHondai is whitespace-only for gradeCode F", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "nar",
      keibajoCode: "30",
      kyori: 2000,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: "G1",
      trackCode: null,
      gradeCode: "F",
      kyosomeiHondai: "   ",
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: false,
      grade: false,
      raceName: true,
    },
  });
  expect(filter.raceName).toBe(null);
});

it("buildRunningStyleBucketFilter clears raceName when the raceName flag is OFF even for gradeCode A", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "jra",
      keibajoCode: "05",
      kyori: 2500,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: null,
      trackCode: "10",
      gradeCode: "A",
      kyosomeiHondai: "有馬記念",
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: true,
      grade: false,
      raceName: false,
    },
  });
  expect(filter.raceName).toBe(null);
});

it("buildRunningStyleBucketFilter clears trackCode and gradeCode when their flags are OFF", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "nar",
      keibajoCode: "30",
      kyori: 1800,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: "G1",
      trackCode: "10",
      gradeCode: "A",
      kyosomeiHondai: "東京大賞典",
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: false,
      grade: false,
      raceName: false,
    },
  });
  expect(filter.trackCode).toBe(null);
  expect(filter.gradeCode).toBe(null);
});

it("buildRunningStyleBucketFilter keeps gradeCode and a trimmed raceName when both flags are ON", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "nar",
      keibajoCode: "30",
      kyori: 1800,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: "G1",
      trackCode: null,
      gradeCode: "A",
      kyosomeiHondai: "東京大賞典",
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: false,
      grade: true,
      raceName: true,
    },
  });
  expect(filter.gradeCode).toBe("A");
  expect(filter.raceName).toBe("東京大賞典");
});

it("buildRunningStyleBucketFilter clears kyosoJokenCode when its flag is OFF for a JRA race", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "jra",
      keibajoCode: "05",
      kyori: 2400,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: "005",
      kyosoJokenMeisho: "3歳未勝利",
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: true,
      grade: false,
      raceName: false,
    },
  });
  expect(filter.kyosoJokenCode).toBe(null);
});

it("RUNNING_STYLE_CLASSES exports the four ordered class names", () => {
  expect(RUNNING_STYLE_CLASSES).toStrictEqual(["nige", "senkou", "sashi", "oikomi"]);
});

it("RUNNING_STYLE_PREDICTION_PARAM_NAMES exports the expected URL param mapping", () => {
  expect(RUNNING_STYLE_PREDICTION_PARAM_NAMES).toStrictEqual({
    keibajo: "runningStyleKeibajo",
    distance: "runningStyleDistance",
    kyosoShubetsu: "runningStyleShubetsu",
    kyosoJoken: "runningStyleJoken",
    condition: "runningStyleCondition",
    track: "runningStyleTrack",
    grade: "runningStyleGrade",
    raceName: "runningStyleRaceName",
  });
});

it("RUNNING_STYLE_BUCKET_PERIOD_PARAM_NAME exposes the rs_period URL param name", () => {
  expect(RUNNING_STYLE_BUCKET_PERIOD_PARAM_NAME).toBe("rs_period");
});

it("buildRunningStyleBucketFilter defaults the period to all when query is omitted", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "jra",
      keibajoCode: "05",
      kyori: 2400,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: null,
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: true,
      grade: false,
      raceName: false,
    },
  });
  expect(filter.period).toBe("all");
});

it("buildRunningStyleBucketFilter defaults the period to all when rs_period is missing", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "jra",
      keibajoCode: "05",
      kyori: 2400,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: null,
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: true,
      grade: false,
      raceName: false,
    },
    query: {},
  });
  expect(filter.period).toBe("all");
});

it("buildRunningStyleBucketFilter sets the period to oos-only when rs_period equals oos-only", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "jra",
      keibajoCode: "05",
      kyori: 2400,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: null,
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: true,
      grade: false,
      raceName: false,
    },
    query: { rs_period: "oos-only" },
  });
  expect(filter.period).toBe("oos-only");
});

it("buildRunningStyleBucketFilter keeps the period as all when rs_period is an unknown string", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "jra",
      keibajoCode: "05",
      kyori: 2400,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: null,
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: true,
      grade: false,
      raceName: false,
    },
    query: { rs_period: "bogus" },
  });
  expect(filter.period).toBe("all");
});

it("buildRunningStyleBucketFilter keeps the period as all when rs_period is explicitly all", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "jra",
      keibajoCode: "05",
      kyori: 2400,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: null,
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: true,
      grade: false,
      raceName: false,
    },
    query: { rs_period: "all" },
  });
  expect(filter.period).toBe("all");
});

it("buildRunningStyleBucketFilter reads the first array element when rs_period is supplied as an array", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "jra",
      keibajoCode: "05",
      kyori: 2400,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: null,
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: true,
      grade: false,
      raceName: false,
    },
    query: { rs_period: ["oos-only", "all"] },
  });
  expect(filter.period).toBe("oos-only");
});

it("buildRunningStyleBucketFilter falls back to all when rs_period is undefined inside query", () => {
  const filter = buildRunningStyleBucketFilter({
    race: {
      source: "jra",
      keibajoCode: "05",
      kyori: 2400,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: null,
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
    },
    flags: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: true,
      grade: false,
      raceName: false,
    },
    query: { rs_period: undefined },
  });
  expect(filter.period).toBe("all");
});
