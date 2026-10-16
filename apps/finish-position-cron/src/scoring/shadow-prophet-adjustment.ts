// Run with bun. Shared read-only post-model adjustment; no routing or writes.
import type { FeatureEntry } from "./feature-projection";

export interface ShadowProphetConfig {
  enabled?: string;
  weight?: string;
}
export interface ShadowScore {
  kettoTorokuBango: string;
  predictedScore: number;
}
export interface ProphetPolicyDecision {
  enabled: boolean;
  weight: number;
}
export interface ShadowProphetInput<T extends ShadowScore> {
  category: string;
  config?: ShadowProphetConfig;
  decision: ProphetPolicyDecision;
  entries: ReadonlyArray<FeatureEntry>;
  predictions: ReadonlyArray<T>;
}

const MIN_RUNNERS = 2;
const MIN_COVERAGE = 0.5;
const MIN_STDDEV = 1e-12;
const MAX_WEIGHT = 1;
const DISABLED_VALUES: ReadonlySet<string> = new Set(["0", "false", "off", "disabled"]);

const finiteNumber = (value: unknown): number | null => {
  if (typeof value !== "string" && typeof value !== "number") return null;
  if (typeof value === "string" && value.trim() === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
};
const populationStddev = (values: ReadonlyArray<number>, mean: number): number =>
  Math.sqrt(values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / values.length);

const configuredWeight = <T extends ShadowScore>(input: ShadowProphetInput<T>): number | null => {
  const enabled = input.config?.enabled?.trim();
  if (
    !input.decision.enabled ||
    (enabled &&
      (DISABLED_VALUES.has(enabled.toLowerCase()) ||
        !enabled.split(",").some((category) => category.trim() === input.category)))
  )
    return null;
  const override = input.config?.weight?.trim();
  const weight = override ? Number(override) : input.decision.weight;
  return Number.isFinite(weight) && weight > 0 && weight <= MAX_WEIGHT ? weight : null;
};

export const adjustShadowProphetScores = <T extends ShadowScore>(
  input: ShadowProphetInput<T>,
): T[] => {
  const { entries, predictions } = input;
  const weight = configuredWeight(input);
  if (weight === null || predictions.length === 0 || entries.length !== predictions.length) {
    return [...predictions];
  }
  const trends = new Map<string, number>();
  entries.forEach((entry) => {
    const coverage = finiteNumber(entry.prophet_entity_coverage);
    const trend = finiteNumber(entry.prophet_entity_performance_mean);
    const horseId = entry.ketto_toroku_bango;
    if (
      coverage !== null &&
      coverage >= 1 &&
      trend !== null &&
      (typeof horseId === "string" || typeof horseId === "number") &&
      String(horseId).trim() !== ""
    ) {
      trends.set(String(horseId).trim(), trend);
    }
  });
  if (trends.size < Math.max(MIN_RUNNERS, Math.ceil(predictions.length * MIN_COVERAGE))) {
    return [...predictions];
  }
  const available = [...trends.values()];
  const trendMean = available.reduce((sum, trend) => sum + trend, 0) / available.length;
  const aligned = predictions.map(
    (prediction) => trends.get(prediction.kettoTorokuBango) ?? trendMean,
  );
  const scores = predictions.map((prediction) => prediction.predictedScore);
  if (!scores.every(Number.isFinite)) return [...predictions];
  const scoreMean = scores.reduce((sum, score) => sum + score, 0) / scores.length;
  const scoreStd = populationStddev(scores, scoreMean);
  const trendStd = populationStddev(aligned, trendMean);
  if (scoreStd <= MIN_STDDEV || trendStd <= MIN_STDDEV) return [...predictions];
  return predictions.map((prediction, index) => ({
    ...prediction,
    predictedScore:
      prediction.predictedScore + (weight * scoreStd * (aligned[index]! - trendMean)) / trendStd,
  }));
};
