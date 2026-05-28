// Run with bun. Race-level post-processing for running-style softmax outputs.

import {
  probsToRunningStyleMap,
  resolveRunningStyleLabels,
  type RunningStyleClassLabel,
  type RunningStylePrediction,
} from "./running-style-lightgbm-tree";

const NIGE_CLASS_INDEX = 0;
const DEFAULT_MIN_NIGE_PROBABILITY = 0.18;

const normalizeProbabilities = (probabilities: Float64Array): Float64Array => {
  const sum = probabilities.reduce((total, value) => total + value, 0);
  if (sum <= 0) return probabilities;
  return Float64Array.from(probabilities, (value) => value / sum);
};

const predictionFromProbabilities = (
  probabilities: Float64Array,
  labels: readonly RunningStyleClassLabel[],
): RunningStylePrediction => {
  let predictedClass = 0;
  probabilities.forEach((value, index) => {
    if (value > probabilities[predictedClass]!) predictedClass = index;
  });
  return {
    predictedClass,
    predictedLabel: labels[predictedClass]!,
    probabilities: probsToRunningStyleMap(probabilities, labels),
  };
};

export const applyRaceLevelNigeConstraint = (
  probabilities: Float64Array,
  labels: readonly RunningStyleClassLabel[],
  options?: { minNigeProbability?: number },
): RunningStylePrediction => {
  const minNigeProbability = options?.minNigeProbability ?? DEFAULT_MIN_NIGE_PROBABILITY;
  const adjusted = Float64Array.from(probabilities);
  const nigeProbability = adjusted[NIGE_CLASS_INDEX]!;
  if (nigeProbability < minNigeProbability) {
    adjusted[NIGE_CLASS_INDEX] = 0;
    return predictionFromProbabilities(normalizeProbabilities(adjusted), labels);
  }
  return predictionFromProbabilities(normalizeProbabilities(adjusted), labels);
};

export const applyRaceLevelNigeConstraintForRace = (
  raceProbabilities: ReadonlyArray<Float64Array>,
  classLabels: readonly string[],
  numClass: number,
  options?: { disableNigeCap?: boolean; minNigeProbability?: number },
): RunningStylePrediction[] => {
  const labels = resolveRunningStyleLabels(classLabels, numClass);
  if (options?.disableNigeCap === true) {
    return raceProbabilities.map((probabilities) =>
      predictionFromProbabilities(normalizeProbabilities(Float64Array.from(probabilities)), labels),
    );
  }
  if (raceProbabilities.length <= 1) {
    return raceProbabilities.map((probabilities) =>
      applyRaceLevelNigeConstraint(probabilities, labels, options),
    );
  }

  const minNigeProbability = options?.minNigeProbability ?? DEFAULT_MIN_NIGE_PROBABILITY;
  let topIndex = 0;
  raceProbabilities.forEach((probabilities, index) => {
    if (probabilities[NIGE_CLASS_INDEX]! > raceProbabilities[topIndex]![NIGE_CLASS_INDEX]!) {
      topIndex = index;
    }
  });
  const topNigeProbability = raceProbabilities[topIndex]![NIGE_CLASS_INDEX]!;

  return raceProbabilities.map((probabilities, index) => {
    const adjusted = Float64Array.from(probabilities);
    if (topNigeProbability < minNigeProbability) {
      adjusted[NIGE_CLASS_INDEX] = 0;
    } else if (index !== topIndex) {
      adjusted[NIGE_CLASS_INDEX] = 0;
    }
    return predictionFromProbabilities(normalizeProbabilities(adjusted), labels);
  });
};
