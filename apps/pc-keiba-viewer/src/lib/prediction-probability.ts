// Run with bun. Model probabilities are optional; never derive them from ranks.

export type PredictionProbabilityStatus = "available" | "partial" | "not_provided";

export interface PredictionProbabilityAvailability {
  show: PredictionProbabilityStatus;
  win: PredictionProbabilityStatus;
}

interface PredictionProbabilities {
  showProbability: number | null;
  winProbability: number | null;
}

export const parsePredictionProbability = (value: unknown): number | null => {
  if (typeof value !== "number" && typeof value !== "string") return null;
  if (typeof value === "string" && value.trim().length === 0) return null;
  const probability: number = Number(value);
  return Number.isFinite(probability) && probability >= 0 && probability <= 1 ? probability : null;
};

const getPredictionProbabilityStatus = (
  values: readonly (number | null)[],
): PredictionProbabilityStatus => {
  const providedCount: number = values.filter((value) => value !== null).length;
  if (providedCount === 0) return "not_provided";
  return providedCount === values.length ? "available" : "partial";
};

export const getPredictionProbabilityAvailability = (
  predictions: readonly PredictionProbabilities[],
): PredictionProbabilityAvailability => ({
  show: getPredictionProbabilityStatus(predictions.map((item) => item.showProbability)),
  win: getPredictionProbabilityStatus(predictions.map((item) => item.winProbability)),
});
