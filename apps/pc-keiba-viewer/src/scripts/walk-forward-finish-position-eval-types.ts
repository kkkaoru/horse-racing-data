// Run with: imported from walk-forward-finish-position-eval.ts (bun runtime)

export interface CliArgs {
  category: "all" | "ban-ei" | "jra" | "nar";
  concurrency: number;
  holdoutYears: number[];
  outputPath: string | null;
  target: "local" | "neon";
  tuningConfigPath: string;
}

export interface CompareFinishJson {
  fromDate: string;
  pairScore: number;
  place1Accuracy: number;
  place2Accuracy: number;
  place3Accuracy: number;
  raceCount: number;
  toDate: string;
  top1Accuracy: number;
  top3BoxAccuracy: number;
  top3ExactOrderAccuracy: number;
  top3PlaceRelation: number;
  top3WinnerCapture: number;
  top5WinnerCapture: number;
}

export interface FoldResult extends CompareFinishJson {
  year: number;
}

export interface AggregateStats {
  count: number;
  max: number;
  mean: number;
  min: number;
  stdev: number;
}

export interface AggregateMetrics {
  pairScore: AggregateStats;
  place1Accuracy: AggregateStats;
  place2Accuracy: AggregateStats;
  place3Accuracy: AggregateStats;
  raceCount: AggregateStats;
  top1Accuracy: AggregateStats;
  top3BoxAccuracy: AggregateStats;
  top3ExactOrderAccuracy: AggregateStats;
  top3PlaceRelation: AggregateStats;
  top3WinnerCapture: AggregateStats;
  top5WinnerCapture: AggregateStats;
}

export interface ReportPayload {
  aggregate: AggregateMetrics;
  category: string;
  folds: FoldResult[];
  generatedAt: string;
  target: string;
  tuningConfigPath: string;
}

export type AggregateMetricKey =
  | "pairScore"
  | "place1Accuracy"
  | "place2Accuracy"
  | "place3Accuracy"
  | "raceCount"
  | "top1Accuracy"
  | "top3BoxAccuracy"
  | "top3ExactOrderAccuracy"
  | "top3PlaceRelation"
  | "top3WinnerCapture"
  | "top5WinnerCapture";

export type CompareFinishRunner = (year: number, compareArgs: readonly string[]) => Promise<string>;

export const AGGREGATE_METRIC_KEYS: readonly AggregateMetricKey[] = [
  "pairScore",
  "place1Accuracy",
  "place2Accuracy",
  "place3Accuracy",
  "raceCount",
  "top1Accuracy",
  "top3BoxAccuracy",
  "top3ExactOrderAccuracy",
  "top3PlaceRelation",
  "top3WinnerCapture",
  "top5WinnerCapture",
];
