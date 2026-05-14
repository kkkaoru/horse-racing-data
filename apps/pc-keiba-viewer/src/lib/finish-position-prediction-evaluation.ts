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
  "ban-ei": {
    category: "ban-ei",
    categoryLabel: "ばんえい競馬",
    fromDate: "20160514",
    pairScore: 68.13,
    place1Accuracy: 35.4,
    place2Accuracy: 20.6,
    place3Accuracy: 15.69,
    raceCount: 17093,
    target: "local",
    toDate: "20260514",
    top1Accuracy: 35.4,
    top3BoxAccuracy: 12.23,
    top3ExactOrderAccuracy: 3.12,
    top3PlaceRelation: 57.46,
    top3WinnerCapture: 70.1,
    top5WinnerCapture: 87.55,
  },
  jra: {
    category: "jra",
    categoryLabel: "中央競馬",
    fromDate: "20160514",
    pairScore: 71.73,
    place1Accuracy: 38.95,
    place2Accuracy: 20.98,
    place3Accuracy: 15.87,
    raceCount: 58294,
    target: "local",
    toDate: "20260514",
    top1Accuracy: 38.95,
    top3BoxAccuracy: 12.61,
    top3ExactOrderAccuracy: 3.79,
    top3PlaceRelation: 56.87,
    top3WinnerCapture: 71.57,
    top5WinnerCapture: 86.25,
  },
  nar: {
    category: "nar",
    categoryLabel: "地方競馬",
    fromDate: "20160514",
    pairScore: 72.17,
    place1Accuracy: 43.88,
    place2Accuracy: 23.15,
    place3Accuracy: 17.5,
    raceCount: 132336,
    target: "local",
    toDate: "20260514",
    top1Accuracy: 43.88,
    top3BoxAccuracy: 15.86,
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
