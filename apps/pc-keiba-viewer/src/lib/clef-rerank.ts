// Run with bun (bunx vitest). Applies the finish-position-clef-rerank Worker result to the served
// finish prediction so the regular 着順予測 table shows the reranked order.
import type { FinishPositionModelPredictionFeature } from "./race-types";

export interface ClefRerankRaceAddress {
  year: string;
  month: string;
  day: string;
  keibajoCode: string;
  raceNumber: string;
}

interface RerankRank {
  horseNumber: number;
  rank: number;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isRank = (value: unknown): value is RerankRank =>
  isRecord(value) && typeof value.horseNumber === "number" && typeof value.rank === "number";

export const clefRerankResultKey = (race: {
  year: string;
  mmdd: string;
  keibajoCode: string;
  raceBango: string;
}): string => `clefrr:v1:result:${race.year}${race.mmdd}:${race.keibajoCode}:${race.raceBango}`;

const parseText = (text: string | null): unknown => {
  try {
    return text === null ? null : JSON.parse(text);
  } catch {
    return null;
  }
};

// The rerank only applies to the exact served generation it was computed from, so a newer
// prediction (for example after the horse-weight rescore) is shown as served until re-ranked.
export const applyClefRerank = (
  features: FinishPositionModelPredictionFeature[],
  rerankText: string | null,
): FinishPositionModelPredictionFeature[] => {
  const rerank = parseText(rerankText);
  const generation = features[0]?.predictionGeneratedAt;
  if (
    !isRecord(rerank) ||
    rerank.baseGeneratedAt !== generation ||
    !Array.isArray(rerank.predictions)
  )
    return features;
  const ranks = rerank.predictions.filter(isRank);
  const field = Math.max(ranks.length - 1, 1);
  const norms = new Map(ranks.map((row) => [String(row.horseNumber), (row.rank - 1) / field]));
  return features.map((feature) => {
    const norm = norms.get(String(Number(feature.horseNumber)));
    return norm === undefined ? feature : { ...feature, predictedFinishNorm: norm };
  });
};
