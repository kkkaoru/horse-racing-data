import { expect, test } from "vitest";

import {
  buildWin5LegPredictions,
  buildWin5PredictionPayload,
  computeHistoricalWinScore,
  getWin5PlanForBudget,
  type Win5LegInput,
  type Win5RunnerInput,
} from "./prediction";
import type { Win5RaceLeg } from "./types";

const TEST_LEG: Win5RaceLeg = {
  legIndex: 1,
  keibajoCode: "05",
  kaisaiKai: "01",
  kaisaiNichime: "01",
  raceBango: "10",
};

const MODEL_SCORE_TOP = 0.95;
const MODEL_SCORE_MID = 0.5;
const MODEL_SCORE_LOW = 0.05;

const heuristicRunnerA: Win5RunnerInput = {
  horseName: "Heuristic A",
  horseNumber: "1",
  jockeyName: "Jockey A",
  popularity: 1,
  odds: 2.5,
  historicalScore: 0.2,
};

const heuristicRunnerB: Win5RunnerInput = {
  horseName: "Heuristic B",
  horseNumber: "2",
  jockeyName: "Jockey B",
  popularity: 5,
  odds: 12.0,
  historicalScore: 0.1,
};

const heuristicRunnerC: Win5RunnerInput = {
  horseName: "Heuristic C",
  horseNumber: "3",
  jockeyName: "Jockey C",
  popularity: 8,
  odds: 40.0,
  historicalScore: 0.05,
};

test("buildWin5LegPredictions assigns higher probability to the highest model score", () => {
  const legInputs: Win5LegInput[] = [
    {
      leg: TEST_LEG,
      runners: [
        { ...heuristicRunnerA, modelScore: MODEL_SCORE_LOW },
        { ...heuristicRunnerB, modelScore: MODEL_SCORE_TOP },
        { ...heuristicRunnerC, modelScore: MODEL_SCORE_MID },
      ],
    },
  ];
  const result = buildWin5LegPredictions(legInputs);
  const sortedByProbability = result[0]?.horses ?? [];
  expect(sortedByProbability[0]?.horseNumber).toBe("2");
  expect(sortedByProbability[1]?.horseNumber).toBe("3");
  expect(sortedByProbability[2]?.horseNumber).toBe("1");
});

test("buildWin5LegPredictions uses heuristic scoring when no modelScore present", () => {
  const legInputs: Win5LegInput[] = [
    {
      leg: TEST_LEG,
      runners: [heuristicRunnerA, heuristicRunnerB, heuristicRunnerC],
    },
  ];
  const result = buildWin5LegPredictions(legInputs);
  const sortedByProbability = result[0]?.horses ?? [];
  expect(sortedByProbability[0]?.horseNumber).toBe("1");
});

test("buildWin5LegPredictions falls back to heuristic when any runner lacks modelScore", () => {
  const legInputs: Win5LegInput[] = [
    {
      leg: TEST_LEG,
      runners: [
        { ...heuristicRunnerA, modelScore: null },
        { ...heuristicRunnerB, modelScore: MODEL_SCORE_TOP },
      ],
    },
  ];
  const result = buildWin5LegPredictions(legInputs);
  const sortedByProbability = result[0]?.horses ?? [];
  expect(sortedByProbability[0]?.horseNumber).toBe("1");
});

test("buildWin5LegPredictions ranks by modelScore when present, ignoring heuristic features", () => {
  const trickyInputs: Win5LegInput[] = [
    {
      leg: TEST_LEG,
      runners: [
        { ...heuristicRunnerA, modelScore: MODEL_SCORE_LOW },
        { ...heuristicRunnerB, modelScore: MODEL_SCORE_TOP },
      ],
    },
  ];
  const result = buildWin5LegPredictions(trickyInputs);
  const topHorse = result[0]?.horses[0];
  expect(topHorse?.horseNumber).toBe("2");
  expect(topHorse?.score).toBe(MODEL_SCORE_TOP);
});

test("buildWin5LegPredictions returns at minimum probability for excluded runners", () => {
  const legInputs: Win5LegInput[] = [
    {
      leg: TEST_LEG,
      runners: [
        { horseName: "Solo", horseNumber: "1" },
      ],
    },
  ];
  const result = buildWin5LegPredictions(legInputs);
  expect(result[0]?.horses[0]?.winProbability).toBe(1);
});

test("buildWin5LegPredictions enriches leg with keibajoName and raceLabel defaults", () => {
  const legInputs: Win5LegInput[] = [
    {
      leg: { ...TEST_LEG, keibajoCode: "05" },
      runners: [{ horseName: "Solo", horseNumber: "1" }],
    },
  ];
  const result = buildWin5LegPredictions(legInputs);
  expect(result[0]?.leg.keibajoName).toBe("東京");
  expect(result[0]?.leg.raceLabel).toBe("東京10R");
});

test("buildWin5LegPredictions normalises horseNumber leading zeros", () => {
  const legInputs: Win5LegInput[] = [
    {
      leg: TEST_LEG,
      runners: [{ horseName: "Padded", horseNumber: "007" }],
    },
  ];
  const result = buildWin5LegPredictions(legInputs);
  expect(result[0]?.horses[0]?.horseNumber).toBe("7");
});

test("buildWin5PredictionPayload uses provided modelVersion-bound payload structure", () => {
  const legInputs: Win5LegInput[] = Array.from({ length: 5 }, (_, index) => ({
    leg: { ...TEST_LEG, legIndex: index + 1, raceBango: String(10 + index) },
    runners: [{ horseName: `H${index}`, horseNumber: "1" }],
  }));
  const payload = buildWin5PredictionPayload({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0524",
    legInputs,
    averagePayoutYen: 100_000,
    predictedAt: "2026-05-24T00:00:00.000Z",
  });
  expect(payload.modelVersion).toBe("win5-xgb-cb-ensemble-v1");
  expect(payload.legs.length).toBe(5);
});

test("getWin5PlanForBudget returns the cached plan when present", () => {
  const legInputs: Win5LegInput[] = Array.from({ length: 5 }, (_, index) => ({
    leg: { ...TEST_LEG, legIndex: index + 1, raceBango: String(10 + index) },
    runners: [{ horseName: "Solo", horseNumber: "1" }],
  }));
  const payload = buildWin5PredictionPayload({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0524",
    legInputs,
  });
  const plan = getWin5PlanForBudget(payload, 2000);
  expect(plan.budgetYen).toBe(2000);
});

test("getWin5PlanForBudget falls through to optimizer for uncached budget", () => {
  const legInputs: Win5LegInput[] = Array.from({ length: 5 }, (_, index) => ({
    leg: { ...TEST_LEG, legIndex: index + 1, raceBango: String(10 + index) },
    runners: [
      { horseName: "A", horseNumber: "1" },
      { horseName: "B", horseNumber: "2" },
    ],
  }));
  const payload = buildWin5PredictionPayload({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0524",
    legInputs,
  });
  const plan = getWin5PlanForBudget(payload, 999_999);
  expect(plan.budgetYen).toBe(999_999);
});

test("computeHistoricalWinScore returns 0 when there are no runs", () => {
  expect(computeHistoricalWinScore({ runs: 0, wins: 0 })).toBe(0);
});

test("computeHistoricalWinScore returns the win rate clamped to [0,1]", () => {
  expect(computeHistoricalWinScore({ runs: 10, wins: 4 })).toBe(0.4);
});

test("computeHistoricalWinScore applies recency weight", () => {
  expect(computeHistoricalWinScore({ runs: 10, wins: 4, recencyWeight: 0.5 })).toBe(0.2);
});
