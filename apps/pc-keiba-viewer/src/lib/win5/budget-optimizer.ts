import {
  WIN5_DEFAULT_BUDGET_YEN,
  WIN5_TICKET_UNIT_YEN,
  type Win5Combination,
  type Win5LegPrediction,
  type Win5TicketPlan,
} from "./types";

const RECOMMENDED_BUDGET_CANDIDATES_YEN = [
  2000, 3000, 5000, 8000, 10000, 15000, 20000, 30000, 50000, 80000, 100000,
] as const;

const TOP_COMBINATION_LIMIT = 12;

const normalizeHorseNumber = (value: string): string => value.replace(/^0+/u, "") || "0";

const product = (values: readonly number[]): number =>
  values.reduce((total, value) => total * Math.max(1, value), 1);

const buildProbabilityMap = (leg: Win5LegPrediction): Map<string, number> => {
  const entries = leg.horses.map((horse) => [normalizeHorseNumber(horse.horseNumber), horse.winProbability] as const);
  const total = entries.reduce((sum, [, probability]) => sum + Math.max(0, probability), 0);
  const map = new Map<string, number>();
  for (const [horseNumber, probability] of entries) {
    map.set(horseNumber, total > 0 ? Math.max(0, probability) / total : 0);
  }
  return map;
};

const enumerateCombinations = (
  legs: Win5LegPrediction[],
  selections: ReadonlyArray<readonly string[]>,
  limit: number,
): Win5Combination[] => {
  const combinations: Win5Combination[] = [];
  const probabilityMaps = legs.map((leg) => buildProbabilityMap(leg));

  const walk = (legIndex: number, picked: string[], probability: number): void => {
    if (combinations.length >= limit) {
      return;
    }
    if (legIndex >= selections.length) {
      combinations.push({ legs: [...picked], probability });
      return;
    }
    for (const horseNumber of selections[legIndex] ?? []) {
      const normalized = normalizeHorseNumber(horseNumber);
      const legProbability = probabilityMaps[legIndex]?.get(normalized) ?? 0;
      walk(legIndex + 1, [...picked, normalized], probability * legProbability);
    }
  };

  walk(0, [], 1);
  return combinations.toSorted((left, right) => right.probability - left.probability).slice(0, limit);
};

const computeExpectedHitProbability = (
  legs: Win5LegPrediction[],
  selections: ReadonlyArray<readonly string[]>,
): number =>
  enumerateCombinations(legs, selections, Number.MAX_SAFE_INTEGER).reduce(
    (total, combination) => total + combination.probability,
    0,
  );

const getInitialSelections = (legs: Win5LegPrediction[]): string[][] =>
  legs.map((leg) => {
    const topHorse = leg.horses[0];
    return topHorse ? [normalizeHorseNumber(topHorse.horseNumber)] : [];
  });

export const optimizeWin5TicketPlan = (
  legs: Win5LegPrediction[],
  budgetYen: number,
): Win5TicketPlan => {
  const maxCombinations = Math.max(1, Math.floor(budgetYen / WIN5_TICKET_UNIT_YEN));
  const selections = getInitialSelections(legs);

  while (product(selections.map((selection) => selection.length)) < maxCombinations) {
    let bestLegIndex = -1;
    let bestHorseNumber: string | null = null;
    let bestGain = 0;
    const currentExpected = computeExpectedHitProbability(legs, selections);

    for (let legIndex = 0; legIndex < legs.length; legIndex += 1) {
      const leg = legs[legIndex];
      if (!leg) {
        continue;
      }
      const selected = new Set(selections[legIndex] ?? []);
      for (const horse of leg.horses) {
        const horseNumber = normalizeHorseNumber(horse.horseNumber);
        if (selected.has(horseNumber)) {
          continue;
        }
        const candidateSelections = selections.map((current, index) =>
          index === legIndex ? [...current, horseNumber] : [...current],
        );
        if (product(candidateSelections.map((selection) => selection.length)) > maxCombinations) {
          continue;
        }
        const nextExpected = computeExpectedHitProbability(legs, candidateSelections);
        const gain = nextExpected - currentExpected;
        if (gain > bestGain) {
          bestGain = gain;
          bestLegIndex = legIndex;
          bestHorseNumber = horseNumber;
        }
      }
    }

    if (bestLegIndex < 0 || bestHorseNumber === null || bestGain <= 0) {
      break;
    }
    selections[bestLegIndex] = [...(selections[bestLegIndex] ?? []), bestHorseNumber];
  }

  const combinationCount = product(selections.map((selection) => selection.length));
  const totalCostYen = combinationCount * WIN5_TICKET_UNIT_YEN;
  const expectedHitProbability = computeExpectedHitProbability(legs, selections);

  return {
    budgetYen,
    combinationCount,
    expectedHitProbability,
    selections: selections.map((horseNumbers, index) => ({
      horseNumbers,
      legIndex: index + 1,
    })),
    topCombinations: enumerateCombinations(legs, selections, TOP_COMBINATION_LIMIT),
    totalCostYen,
  };
};

export const recommendWin5BudgetYen = (
  legs: Win5LegPrediction[],
  averagePayoutYen: number,
): number => {
  let bestBudget = WIN5_DEFAULT_BUDGET_YEN;
  let bestScore = Number.NEGATIVE_INFINITY;

  for (const budgetYen of RECOMMENDED_BUDGET_CANDIDATES_YEN) {
    const plan = optimizeWin5TicketPlan(legs, budgetYen);
    const expectedReturnYen = plan.expectedHitProbability * averagePayoutYen;
    const score = expectedReturnYen / Math.max(plan.totalCostYen, WIN5_TICKET_UNIT_YEN);
    if (score > bestScore) {
      bestScore = score;
      bestBudget = budgetYen;
    }
  }

  return bestBudget;
};

export const buildWin5PlansByBudget = (
  legs: Win5LegPrediction[],
  budgets: readonly number[],
): Record<string, Win5TicketPlan> =>
  Object.fromEntries(
    budgets.map((budgetYen) => [String(budgetYen), optimizeWin5TicketPlan(legs, budgetYen)]),
  );
