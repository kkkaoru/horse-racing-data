// Run with bun. Pure validation for generation-bound prediction cache warming.

interface PredictionGenerationFeature {
  predictionGeneratedAt?: string | null;
}

export const normalizeExpectedPredictionGeneratedAt = (value: string): string | null => {
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) ? new Date(timestamp).toISOString() : null;
};

export const arePredictionFeaturesFreshForGeneration = (
  features: readonly PredictionGenerationFeature[],
  expectedPredictionGeneratedAt: string,
): boolean => {
  const expectedTimestamp = Date.parse(expectedPredictionGeneratedAt);
  return (
    features.length > 0 &&
    features.every((feature) => {
      const generatedTimestamp = Date.parse(feature.predictionGeneratedAt ?? "");
      return Number.isFinite(generatedTimestamp) && generatedTimestamp >= expectedTimestamp;
    })
  );
};

export const havePredictionFeaturesSingleGeneration = (
  features: readonly PredictionGenerationFeature[],
): boolean => {
  const generationTimestamp = Date.parse(features[0]?.predictionGeneratedAt ?? "");
  return (
    features.length > 0 &&
    Number.isFinite(generationTimestamp) &&
    features.every(
      (feature) => Date.parse(feature.predictionGeneratedAt ?? "") === generationTimestamp,
    )
  );
};
