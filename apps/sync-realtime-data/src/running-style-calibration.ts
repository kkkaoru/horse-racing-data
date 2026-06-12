// Run with bun.
import type { RunningStyleClassLabel, RunningStylePrediction } from "./running-style-lightgbm-tree";

export interface CalibrationKnots {
  x: readonly number[];
  y: readonly number[];
}

export interface RunningStyleCalibrationTable {
  category: string;
  fit_year: number;
  classes: readonly string[];
  calibrators: Record<RunningStyleClassLabel, CalibrationKnots>;
}

interface CalibratorShape {
  category: unknown;
  fit_year: unknown;
  classes: unknown;
  calibrators: unknown;
}

const RUNNING_STYLE_LABELS: readonly RunningStyleClassLabel[] = [
  "nige",
  "senkou",
  "sashi",
  "oikomi",
];

const UNIFORM_PROB = 0.25;

export const linearInterp = (
  xKnots: readonly number[],
  yKnots: readonly number[],
  value: number,
): number => {
  if (xKnots.length === 1) return yKnots[0]!;
  const lo = xKnots[0]!;
  const hi = xKnots[xKnots.length - 1]!;
  if (value <= lo) return yKnots[0]!;
  if (value >= hi) return yKnots[yKnots.length - 1]!;
  const segIdx = xKnots.findIndex(
    (x, i) => i < xKnots.length - 1 && value >= x && value <= xKnots[i + 1]!,
  );
  if (segIdx === -1) return yKnots[yKnots.length - 1]!;
  const x0 = xKnots[segIdx]!;
  const x1 = xKnots[segIdx + 1]!;
  const y0 = yKnots[segIdx]!;
  const y1 = yKnots[segIdx + 1]!;
  return y0 + ((y1 - y0) * (value - x0)) / (x1 - x0);
};

const calibrateProbs = (
  probs: Record<RunningStyleClassLabel, number>,
  calibrators: Record<RunningStyleClassLabel, CalibrationKnots>,
): Record<RunningStyleClassLabel, number> =>
  RUNNING_STYLE_LABELS.reduce<Record<RunningStyleClassLabel, number>>(
    (acc, label) => ({
      ...acc,
      [label]: linearInterp(calibrators[label].x, calibrators[label].y, probs[label]),
    }),
    { nige: 0, senkou: 0, sashi: 0, oikomi: 0 },
  );

const sumProbs = (probs: Record<RunningStyleClassLabel, number>): number =>
  RUNNING_STYLE_LABELS.reduce((sum, label) => sum + probs[label], 0);

const uniformProbs = (): Record<RunningStyleClassLabel, number> => ({
  nige: UNIFORM_PROB,
  oikomi: UNIFORM_PROB,
  sashi: UNIFORM_PROB,
  senkou: UNIFORM_PROB,
});

const normalizeProbs = (
  probs: Record<RunningStyleClassLabel, number>,
): Record<RunningStyleClassLabel, number> => {
  const total = sumProbs(probs);
  if (total === 0) return uniformProbs();
  return RUNNING_STYLE_LABELS.reduce<Record<RunningStyleClassLabel, number>>(
    (acc, label) => ({ ...acc, [label]: probs[label] / total }),
    { nige: 0, senkou: 0, sashi: 0, oikomi: 0 },
  );
};

const argmaxLabel = (probs: Record<RunningStyleClassLabel, number>): RunningStyleClassLabel =>
  RUNNING_STYLE_LABELS.reduce((best, label) => (probs[label] > probs[best] ? label : best));

export const applyRunningStyleCalibration = (
  prediction: RunningStylePrediction,
  calibrators: RunningStyleCalibrationTable,
): RunningStylePrediction => {
  const calibrated = calibrateProbs(prediction.probabilities, calibrators.calibrators);
  const normalized = normalizeProbs(calibrated);
  const predictedLabel = argmaxLabel(normalized);
  const predictedClass = RUNNING_STYLE_LABELS.indexOf(predictedLabel);
  return { probabilities: normalized, predictedClass, predictedLabel };
};

export const buildCalibrationR2Key = (source: "jra" | "nar"): string =>
  `running-style/models/${source}/calibrators.json`;

const isCalibratorShape = (data: unknown): data is CalibratorShape =>
  data !== null &&
  typeof data === "object" &&
  "category" in data &&
  "fit_year" in data &&
  "classes" in data &&
  "calibrators" in data;

const isValidCalibrators = (
  cal: unknown,
): cal is Record<RunningStyleClassLabel, CalibrationKnots> => {
  if (cal === null || typeof cal !== "object") return false;
  const calObj: object = cal;
  return RUNNING_STYLE_LABELS.every((label) => label in calObj);
};

export const loadCalibratorsFromR2 = async (
  bucket: R2Bucket,
  key: string,
): Promise<RunningStyleCalibrationTable> => {
  const obj = await bucket.get(key);
  if (obj === null) throw new Error(`Calibration table not found in R2: ${key}`);
  const text = await obj.text();
  const data: unknown = JSON.parse(text);
  if (!isCalibratorShape(data)) {
    throw new Error("Invalid calibration table: missing required keys");
  }
  if (!isValidCalibrators(data.calibrators)) {
    throw new Error("Invalid calibration table: missing calibrator for one or more classes");
  }
  return {
    calibrators: data.calibrators,
    category: String(data.category),
    classes: Array.isArray(data.classes) ? data.classes : [],
    fit_year: Number(data.fit_year),
  };
};
