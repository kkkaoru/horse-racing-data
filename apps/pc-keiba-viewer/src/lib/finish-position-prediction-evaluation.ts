import type { RaceSource } from "./codes";
import { isBanEiKeibajoCode } from "./runner-format";

export type FinishPredictionEvaluationCategory = "ban-ei" | "jra" | "nar";

export interface FinishPredictionEvaluationMetrics extends Record<string, unknown> {
  category: FinishPredictionEvaluationCategory;
  categoryLabel: string;
  fromDate: string;
  pairScore: number;
  place1Accuracy: number;
  place2Accuracy: number;
  place3Accuracy: number;
  raceCount: number;
  target: "local";
  toDate: string;
  top1Accuracy: number;
  top3BoxAccuracy: number;
  top3ExactOrderAccuracy: number;
  top3PlaceRelation: number;
  top3WinnerCapture: number;
  top5WinnerCapture: number;
}

export const FINISH_POSITION_PREDICTION_EVALUATIONS: Record<
  FinishPredictionEvaluationCategory,
  FinishPredictionEvaluationMetrics
> = {
  // Fallback constants reflect the v7-lineage walk-forward 21y rollups
  // (model_prediction_evaluations where model_version like '%-v7-lineage-wf-21y',
  // post JRA de-contamination). They are only rendered when the DB-driven
  // model_prediction_evaluations path is unavailable, and are stored as 0-100
  // percentages to match detail-section-data's `* 100` scaling of DB fractions.
  "ban-ei": {
    category: "ban-ei",
    categoryLabel: "ばんえい競馬",
    fromDate: "20080101",
    pairScore: 67.853,
    place1Accuracy: 33.647,
    place2Accuracy: 19.606,
    place3Accuracy: 15.681,
    raceCount: 31771,
    target: "local",
    toDate: "20261231",
    top1Accuracy: 33.647,
    top3BoxAccuracy: 11.561,
    top3ExactOrderAccuracy: 2.823,
    top3PlaceRelation: 56.781,
    top3WinnerCapture: 68.704,
    top5WinnerCapture: 86.242,
  },
  jra: {
    category: "jra",
    categoryLabel: "中央競馬",
    fromDate: "20070101",
    pairScore: 74.668,
    place1Accuracy: 40.139,
    place2Accuracy: 21.73,
    place3Accuracy: 16.203,
    raceCount: 66964,
    target: "local",
    toDate: "20261231",
    top1Accuracy: 40.139,
    top3BoxAccuracy: 14.242,
    top3ExactOrderAccuracy: 4.198,
    top3PlaceRelation: 58.635,
    top3WinnerCapture: 74.043,
    top5WinnerCapture: 88.237,
  },
  nar: {
    category: "nar",
    categoryLabel: "地方競馬",
    fromDate: "20070101",
    pairScore: 82.431,
    place1Accuracy: 58.055,
    place2Accuracy: 36.094,
    place3Accuracy: 28.607,
    raceCount: 258966,
    target: "local",
    toDate: "20261231",
    top1Accuracy: 58.055,
    top3BoxAccuracy: 37.176,
    top3ExactOrderAccuracy: 15.412,
    top3PlaceRelation: 74.993,
    top3WinnerCapture: 90.125,
    top5WinnerCapture: 97.518,
  },
};

export const getFinishPredictionEvaluationCategory = ({
  keibajoCode,
  source,
}: {
  keibajoCode: string | null | undefined;
  source: RaceSource;
}): FinishPredictionEvaluationCategory => {
  if (source === "jra") {
    return "jra";
  }
  return isBanEiKeibajoCode(keibajoCode) ? "ban-ei" : "nar";
};

export const getFinishPredictionEvaluation = (params: {
  keibajoCode: string | null | undefined;
  source: RaceSource;
}): FinishPredictionEvaluationMetrics =>
  FINISH_POSITION_PREDICTION_EVALUATIONS[getFinishPredictionEvaluationCategory(params)];
