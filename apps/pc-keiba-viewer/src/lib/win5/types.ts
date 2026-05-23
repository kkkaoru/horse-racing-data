export const WIN5_DEFAULT_BUDGET_YEN = 2000;
export const WIN5_TICKET_UNIT_YEN = 100;
export const WIN5_MODEL_VERSION = "win5-heuristic-v1";

export interface Win5RaceLeg {
  legIndex: number;
  keibajoCode: string;
  kaisaiKai: string;
  kaisaiNichime: string;
  raceBango: string;
  keibajoName?: string;
  raceLabel?: string;
  startTime?: string;
}

export interface Win5Schedule {
  kaisaiNen: string;
  kaisaiTsukihi: string;
  saleDeadline?: string | null;
  legs: Win5RaceLeg[];
  source: "jra_web" | "jvd_wf";
  fetchedAt: string;
}

export interface Win5HorseCandidate {
  horseNumber: string;
  horseName: string;
  jockeyName?: string | null;
  winProbability: number;
  popularity?: number | null;
  odds?: number | null;
  score?: number | null;
}

export interface Win5LegPrediction {
  leg: Win5RaceLeg;
  horses: Win5HorseCandidate[];
}

export interface Win5Combination {
  legs: string[];
  probability: number;
}

export interface Win5TicketPlan {
  budgetYen: number;
  combinationCount: number;
  totalCostYen: number;
  expectedHitProbability: number;
  selections: Array<{
    legIndex: number;
    horseNumbers: string[];
  }>;
  topCombinations: Win5Combination[];
}

export interface Win5PredictionPayload {
  modelVersion: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  recommendedBudgetYen: number;
  defaultBudgetYen: number;
  legs: Win5LegPrediction[];
  plans: Record<string, Win5TicketPlan>;
  predictedAt: string;
}

export interface Win5DaySummary {
  kaisaiNen: string;
  kaisaiTsukihi: string;
  month: string;
  day: string;
  legCount: number;
  hasPrediction: boolean;
}

export interface Win5YearSummary {
  year: string;
  dayCount: number;
}

export interface Win5PayoutInfo {
  winningHorseNumbers: string[];
  payoutYen: number;
  winningTicketCount: number;
}

export interface Win5ValidationResult {
  kaisaiNen: string;
  kaisaiTsukihi: string;
  actualWinners: string[];
  payoutYen: number;
  defaultBudgetYen: number;
  recommendedBudgetYen: number;
  defaultHit: boolean;
  recommendedHit: boolean;
  defaultCostYen: number;
  recommendedCostYen: number;
  defaultReturnYen: number;
  recommendedReturnYen: number;
}

export interface Win5ValidationSummary {
  periodStart: string;
  periodEnd: string;
  evaluatedDays: number;
  skippedDays: number;
  defaultBudgetYen: number;
  defaultHitCount: number;
  defaultHitRate: number;
  defaultTotalCostYen: number;
  defaultTotalReturnYen: number;
  defaultRecoveryRate: number;
  recommendedBudgetAverageYen: number;
  recommendedHitCount: number;
  recommendedHitRate: number;
  recommendedTotalCostYen: number;
  recommendedTotalReturnYen: number;
  recommendedRecoveryRate: number;
}
