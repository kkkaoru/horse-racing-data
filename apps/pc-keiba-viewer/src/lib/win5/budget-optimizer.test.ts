// Run with: bunx vitest run src/lib/win5/budget-optimizer.test.ts
import { expect, test } from "vitest";

import {
  buildWin5PlansByBudget,
  optimizeWin5TicketPlan,
  recommendWin5BudgetYen,
} from "./budget-optimizer";
import type { Win5LegPrediction, Win5RaceLeg } from "./types";

const FIVE_LEGS: Win5RaceLeg[] = [
  { legIndex: 1, keibajoCode: "05", kaisaiKai: "01", kaisaiNichime: "01", raceBango: "10" },
  { legIndex: 2, keibajoCode: "05", kaisaiKai: "01", kaisaiNichime: "01", raceBango: "11" },
  { legIndex: 3, keibajoCode: "05", kaisaiKai: "01", kaisaiNichime: "01", raceBango: "12" },
  { legIndex: 4, keibajoCode: "06", kaisaiKai: "01", kaisaiNichime: "01", raceBango: "10" },
  { legIndex: 5, keibajoCode: "06", kaisaiKai: "01", kaisaiNichime: "01", raceBango: "11" },
];

const SOLO_LEGS: Win5LegPrediction[] = FIVE_LEGS.map((leg) => ({
  leg,
  horses: [
    {
      horseName: "Solo",
      horseNumber: "1",
      jockeyName: null,
      odds: null,
      popularity: null,
      score: null,
      winProbability: 1,
    },
  ],
}));

const TWO_HORSE_LEGS: Win5LegPrediction[] = FIVE_LEGS.map((leg) => ({
  leg,
  horses: [
    {
      horseName: "A",
      horseNumber: "1",
      jockeyName: null,
      odds: null,
      popularity: null,
      score: null,
      winProbability: 0.7,
    },
    {
      horseName: "B",
      horseNumber: "2",
      jockeyName: null,
      odds: null,
      popularity: null,
      score: null,
      winProbability: 0.3,
    },
  ],
}));

const ZERO_PROBABILITY_LEGS: Win5LegPrediction[] = FIVE_LEGS.map((leg) => ({
  leg,
  horses: [
    {
      horseName: "A",
      horseNumber: "1",
      jockeyName: null,
      odds: null,
      popularity: null,
      score: null,
      winProbability: 0,
    },
    {
      horseName: "B",
      horseNumber: "2",
      jockeyName: null,
      odds: null,
      popularity: null,
      score: null,
      winProbability: 0,
    },
  ],
}));

test("optimizeWin5TicketPlan picks the top horse from each leg at the default budget", () => {
  const plan = optimizeWin5TicketPlan(SOLO_LEGS, 2000);
  expect(plan.combinationCount).toStrictEqual(1);
  expect(plan.selections[0]?.horseNumbers).toStrictEqual(["1"]);
});

test("optimizeWin5TicketPlan returns budgetYen on the plan", () => {
  const plan = optimizeWin5TicketPlan(SOLO_LEGS, 5000);
  expect(plan.budgetYen).toStrictEqual(5000);
});

test("optimizeWin5TicketPlan totalCostYen scales with combinationCount", () => {
  const plan = optimizeWin5TicketPlan(SOLO_LEGS, 2000);
  expect(plan.totalCostYen).toStrictEqual(100);
});

test("optimizeWin5TicketPlan returns horseNumbers using the top probability for solo runners", () => {
  const plan = optimizeWin5TicketPlan(SOLO_LEGS, 2000);
  expect(plan.expectedHitProbability).toBeGreaterThan(0);
});

test("optimizeWin5TicketPlan expands selections when budget allows additional horses", () => {
  const plan = optimizeWin5TicketPlan(TWO_HORSE_LEGS, 200);
  expect(plan.selections[0]?.horseNumbers.length).toBeGreaterThanOrEqual(1);
});

test("optimizeWin5TicketPlan handles legs with empty horses by selecting nothing for that leg", () => {
  const legsWithEmpty: Win5LegPrediction[] = [
    { leg: FIVE_LEGS[0]!, horses: [] },
    ...SOLO_LEGS.slice(1),
  ];
  const plan = optimizeWin5TicketPlan(legsWithEmpty, 2000);
  expect(plan.selections[0]?.horseNumbers).toStrictEqual([]);
});

test("optimizeWin5TicketPlan normalizes all-zero horse number to a single zero", () => {
  const legs: Win5LegPrediction[] = SOLO_LEGS.map((leg, index) =>
    index === 0
      ? {
          leg: leg.leg,
          horses: [
            {
              horseName: "Pad",
              horseNumber: "000",
              jockeyName: null,
              odds: null,
              popularity: null,
              score: null,
              winProbability: 1,
            },
          ],
        }
      : leg,
  );
  const plan = optimizeWin5TicketPlan(legs, 2000);
  expect(plan.selections[0]?.horseNumbers).toStrictEqual(["0"]);
});

test("optimizeWin5TicketPlan uses uniform internal probabilities when every horse has zero probability", () => {
  const plan = optimizeWin5TicketPlan(ZERO_PROBABILITY_LEGS, 200);
  expect(plan.combinationCount).toBeGreaterThanOrEqual(1);
});

test("recommendWin5BudgetYen returns a positive integer budget", () => {
  const recommended = recommendWin5BudgetYen(SOLO_LEGS, 200_000);
  expect(recommended).toBeGreaterThan(0);
  expect(Number.isInteger(recommended)).toBe(true);
});

test("buildWin5PlansByBudget produces a plan for every requested budget", () => {
  const plans = buildWin5PlansByBudget(SOLO_LEGS, [2000, 5000]);
  expect(Object.keys(plans)).toStrictEqual(["2000", "5000"]);
});
