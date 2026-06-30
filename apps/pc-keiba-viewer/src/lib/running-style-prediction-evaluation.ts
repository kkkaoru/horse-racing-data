// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

import {
  type ConfusionMatrix,
  deriveAccuracy,
  deriveLogLoss,
  deriveMacroF1,
  derivePerClassMetrics,
  deriveQuadraticWeightedKappa,
  deriveTop2Accuracy,
  deriveWeightedF1,
  deriveWilsonScoreCI,
  isSmallSample,
  type RunningStyleBucketMetrics,
  type RunningStyleClass,
} from "./running-style-prediction-dimensions";

export interface RawRunningStyleBucketAggregate {
  raceCount: number;
  predictionCount: number;
  cm: ConfusionMatrix;
  logLossSumByClass: Record<RunningStyleClass, number>;
  logLossCountByClass: Record<RunningStyleClass, number>;
  top2HitCount: number;
}

const WILSON_CONFIDENCE_95 = 0.95;

const ZERO_PER_CLASS_LOG_LOSS: Record<RunningStyleClass, number | null> = {
  nige: null,
  oikomi: null,
  sashi: null,
  senkou: null,
};

const ZERO_CONFUSION_MATRIX: ConfusionMatrix = [
  [0, 0, 0, 0],
  [0, 0, 0, 0],
  [0, 0, 0, 0],
  [0, 0, 0, 0],
];

const ZERO_PER_CLASS_METRIC = {
  f1: null,
  precision: null,
  recall: null,
  support: 0,
} satisfies RunningStyleBucketMetrics["perClass"]["nige"];

const ZERO_PER_CLASS: RunningStyleBucketMetrics["perClass"] = {
  nige: ZERO_PER_CLASS_METRIC,
  oikomi: ZERO_PER_CLASS_METRIC,
  sashi: ZERO_PER_CLASS_METRIC,
  senkou: ZERO_PER_CLASS_METRIC,
};

const traceOfConfusionMatrix = (cm: ConfusionMatrix): number =>
  cm[0][0] + cm[1][1] + cm[2][2] + cm[3][3];

const buildEmptyMetrics = (): RunningStyleBucketMetrics => ({
  accuracy: 0,
  accuracyCI: { lower: 0, upper: 0 },
  confusionMatrix: ZERO_CONFUSION_MATRIX,
  macroF1: null,
  overallLogLoss: null,
  perClass: ZERO_PER_CLASS,
  perClassLogLoss: ZERO_PER_CLASS_LOG_LOSS,
  predictionCount: 0,
  qwk: 0,
  raceCount: 0,
  smallSampleWarning: true,
  top2Accuracy: 0,
  weightedF1: null,
});

export const scaleRunningStyleEvaluationFromCM = (
  rawAgg: RawRunningStyleBucketAggregate,
): RunningStyleBucketMetrics => {
  const { cm, logLossCountByClass, logLossSumByClass, predictionCount, raceCount, top2HitCount } =
    rawAgg;
  if (predictionCount === 0) {
    return buildEmptyMetrics();
  }
  const perClass = derivePerClassMetrics(cm);
  const accuracy = deriveAccuracy(cm);
  const macroF1 = deriveMacroF1(perClass);
  const weightedF1 = deriveWeightedF1(perClass);
  const qwk = deriveQuadraticWeightedKappa(cm);
  const accuracyCI = deriveWilsonScoreCI({
    confidence: WILSON_CONFIDENCE_95,
    successes: traceOfConfusionMatrix(cm),
    trials: predictionCount,
  });
  const logLoss = deriveLogLoss({
    countByClass: logLossCountByClass,
    sumByClass: logLossSumByClass,
  });
  const top2Accuracy = deriveTop2Accuracy({ hitCount: top2HitCount, total: predictionCount });
  const smallSampleWarning = isSmallSample({ raceCount, predictionCount, perClass });
  return {
    accuracy,
    accuracyCI,
    confusionMatrix: cm,
    macroF1,
    overallLogLoss: logLoss.overall,
    perClass,
    perClassLogLoss: logLoss.perClass,
    predictionCount,
    qwk,
    raceCount,
    smallSampleWarning,
    top2Accuracy,
    weightedF1,
  };
};
