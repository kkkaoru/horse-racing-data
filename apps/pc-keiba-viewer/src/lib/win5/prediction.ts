import { KEIBAJO_NAMES } from "../codes";
import { recommendWin5BudgetYen, optimizeWin5TicketPlan, buildWin5PlansByBudget } from "./budget-optimizer";
import {
  WIN5_DEFAULT_BUDGET_YEN,
  WIN5_MODEL_VERSION,
  type Win5HorseCandidate,
  type Win5LegPrediction,
  type Win5PredictionPayload,
  type Win5RaceLeg,
} from "./types";

const HEURISTIC_SOFTMAX_TEMPERATURE = 0.65;
const MODEL_SCORE_SOFTMAX_TEMPERATURE = 0.4;
const MIN_WIN_PROBABILITY = 0.001;

export interface Win5RunnerInput {
  horseNumber: string;
  horseName: string;
  jockeyName?: string | null;
  popularity?: number | null;
  odds?: number | null;
  historicalScore?: number | null;
  modelScore?: number | null;
}

export interface Win5LegInput {
  leg: Win5RaceLeg;
  runners: Win5RunnerInput[];
}

const normalizeHorseNumber = (value: string): string => value.replace(/^0+/u, "") || "0";

const clamp = (value: number, min: number, max: number): number =>
  Math.min(max, Math.max(min, value));

const softmax = (params: { scores: readonly number[]; temperature: number }): number[] => {
  const { scores, temperature } = params;
  if (scores.length === 0) {
    return [];
  }
  const maxScore = Math.max(...scores);
  const exponents = scores.map((score) => Math.exp((score - maxScore) / temperature));
  const total = exponents.reduce((sum, value) => sum + value, 0);
  if (total <= 0) {
    return scores.map(() => 1 / scores.length);
  }
  return exponents.map((value) => value / total);
};

const hasModelScore = (runner: Win5RunnerInput): boolean =>
  runner.modelScore !== null && runner.modelScore !== undefined;

const heuristicScore = (params: { runner: Win5RunnerInput; runnerCount: number }): number => {
  const { runner, runnerCount } = params;
  const base = runner.historicalScore ?? 0;
  const popularityTerm =
    runner.popularity !== null && runner.popularity !== undefined && runner.popularity > 0
      ? (runnerCount - runner.popularity + 1) / runnerCount
      : 0;
  const oddsTerm = runner.odds !== null && runner.odds !== undefined && runner.odds > 0
    ? 1 / runner.odds
    : 0;
  return base + popularityTerm + oddsTerm;
};

const allHaveModelScore = (runners: readonly Win5RunnerInput[]): boolean =>
  runners.length > 0 && runners.every(hasModelScore);

interface ScoreLegParams {
  runners: readonly Win5RunnerInput[];
  useModelScore: boolean;
}

const scoreLegRunners = (params: ScoreLegParams): number[] =>
  params.useModelScore
    ? params.runners.map((runner) => runner.modelScore ?? 0)
    : params.runners.map((runner) =>
        heuristicScore({ runner, runnerCount: params.runners.length }),
      );

export const buildWin5LegPredictions = (legInputs: readonly Win5LegInput[]): Win5LegPrediction[] =>
  legInputs.map(({ leg, runners }) => {
    const activeRunners = runners.filter((runner) => runner.horseNumber.trim().length > 0);
    const useModelScore = allHaveModelScore(activeRunners);
    const scores = scoreLegRunners({ runners: activeRunners, useModelScore });
    const temperature = useModelScore
      ? MODEL_SCORE_SOFTMAX_TEMPERATURE
      : HEURISTIC_SOFTMAX_TEMPERATURE;
    const probabilities = softmax({ scores, temperature }).map((probability) =>
      Math.max(MIN_WIN_PROBABILITY, probability),
    );

    const horses: Win5HorseCandidate[] = activeRunners
      .map((runner, index) => ({
        horseName: runner.horseName,
        horseNumber: normalizeHorseNumber(runner.horseNumber),
        jockeyName: runner.jockeyName ?? null,
        odds: runner.odds ?? null,
        popularity: runner.popularity ?? null,
        score: scores[index] ?? null,
        winProbability: probabilities[index] ?? MIN_WIN_PROBABILITY,
      }))
      .toSorted((left, right) => right.winProbability - left.winProbability);

    const keibajoName = leg.keibajoName ?? KEIBAJO_NAMES[leg.keibajoCode] ?? leg.keibajoCode;
    return {
      horses,
      leg: {
        ...leg,
        keibajoName,
        raceLabel: leg.raceLabel ?? `${keibajoName}${leg.raceBango}R`,
      },
    };
  });

export const buildWin5PredictionPayload = (params: {
  kaisaiNen: string;
  kaisaiTsukihi: string;
  legInputs: readonly Win5LegInput[];
  averagePayoutYen?: number;
  predictedAt?: string;
}): Win5PredictionPayload => {
  const legs = buildWin5LegPredictions(params.legInputs);
  const averagePayoutYen = params.averagePayoutYen ?? 250_000;
  const recommendedBudgetYen = recommendWin5BudgetYen(legs, averagePayoutYen);
  const budgets = Array.from(
    new Set([WIN5_DEFAULT_BUDGET_YEN, recommendedBudgetYen, 5000, 10000, 20000]),
  ).toSorted((left, right) => left - right);

  return {
    defaultBudgetYen: WIN5_DEFAULT_BUDGET_YEN,
    kaisaiNen: params.kaisaiNen,
    kaisaiTsukihi: params.kaisaiTsukihi,
    legs,
    modelVersion: WIN5_MODEL_VERSION,
    plans: buildWin5PlansByBudget(legs, budgets),
    predictedAt: params.predictedAt ?? new Date().toISOString(),
    recommendedBudgetYen,
  };
};

export const getWin5PlanForBudget = (
  payload: Win5PredictionPayload,
  budgetYen: number,
): ReturnType<typeof optimizeWin5TicketPlan> => {
  const cached = payload.plans[String(budgetYen)];
  if (cached) {
    return cached;
  }
  return optimizeWin5TicketPlan(payload.legs, budgetYen);
};

export const computeHistoricalWinScore = (params: {
  wins: number;
  runs: number;
  recencyWeight?: number;
}): number => {
  const { wins, runs, recencyWeight = 1 } = params;
  if (runs <= 0) {
    return 0;
  }
  const base = wins / runs;
  return clamp(base * recencyWeight, 0, 1);
};

export {
  WIN5_DEFAULT_BUDGET_YEN,
  WIN5_MODEL_VERSION,
  WIN5_TICKET_UNIT_YEN,
} from "./types";
