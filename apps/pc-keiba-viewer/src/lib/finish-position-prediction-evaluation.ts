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
  // Fallback constants reflect the v7-lineage walk-forward (2024-2025 holdout)
  // figures from memory project_v7_lineage_deployed. They are only rendered when
  // the DB-driven model_prediction_evaluations path is unavailable.
  "ban-ei": {
    category: "ban-ei",
    categoryLabel: "ばんえい競馬",
    fromDate: "20240101",
    pairScore: 68.13,
    place1Accuracy: 34.71,
    place2Accuracy: 21.03,
    place3Accuracy: 15.43,
    raceCount: 3480,
    target: "local",
    toDate: "20251231",
    top1Accuracy: 34.71,
    top3BoxAccuracy: 12.7,
    top3ExactOrderAccuracy: 3.12,
    top3PlaceRelation: 57.46,
    top3WinnerCapture: 70.1,
    top5WinnerCapture: 87.55,
  },
  jra: {
    category: "jra",
    categoryLabel: "中央競馬",
    fromDate: "20240101",
    pairScore: 71.73,
    place1Accuracy: 52.509,
    place2Accuracy: 28.691,
    place3Accuracy: 20.377,
    raceCount: 11101,
    target: "local",
    toDate: "20251231",
    top1Accuracy: 52.509,
    top3BoxAccuracy: 12.61,
    top3ExactOrderAccuracy: 3.79,
    top3PlaceRelation: 56.87,
    top3WinnerCapture: 71.57,
    top5WinnerCapture: 86.25,
  },
  nar: {
    category: "nar",
    categoryLabel: "地方競馬",
    fromDate: "20240101",
    pairScore: 72.17,
    place1Accuracy: 58.562,
    place2Accuracy: 35.834,
    place3Accuracy: 27.392,
    raceCount: 27103,
    target: "local",
    toDate: "20251231",
    top1Accuracy: 58.562,
    top3BoxAccuracy: 34.635,
    top3ExactOrderAccuracy: 4.83,
    top3PlaceRelation: 61.05,
    top3WinnerCapture: 77.23,
    top5WinnerCapture: 90.8,
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
