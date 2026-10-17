// Run with bun (bunx vitest).
import { expect, it } from "vitest";

import {
  betStyleLabel,
  betTypeLabel,
  buildClefBetsUrl,
  describePlan,
  formatPlanOdds,
  formatPlanPayout,
  formatPlanStake,
  formatYen,
  normalizeBudget,
  planGroups,
  readClefBets,
  summarizePlanSet,
} from "./clef-bets";

const widePlan = {
  betType: "wide",
  style: "nagashi",
  axis: 11,
  legs: [[11], [4, 7]],
  budget: 1200,
  totalStake: 1200,
  expectedReturn: 1300,
  tickets: [
    { horses: [4, 11], odds: 2.4, maxOdds: 2.7, stake: 600, payout: 1440, payoutMax: 1620 },
    { horses: [7, 11], odds: 3.1, maxOdds: 3.6, stake: 600, payout: 1860, payoutMax: 2160 },
  ],
};

const tanshoPlan = {
  betType: "tansho",
  style: "single",
  axis: 11,
  legs: [[11]],
  budget: 800,
  totalStake: 800,
  expectedReturn: 900,
  tickets: [{ horses: [11], odds: 4.3, maxOdds: null, stake: 800, payout: 3440, payoutMax: null }],
};

const payload = {
  raceKey: "20261018:08:03",
  oddsFetchedAt: "2026-10-18T06:55:31+09:00",
  runners: [
    {
      horseNumber: 11,
      frameNumber: 8,
      horseName: "イレブン",
      jockey: "騎手A",
      trainer: "調教師A",
      owner: "馬主A",
    },
  ],
  suggestion: widePlan,
  plans: { budget: 2000, totalStake: 2000, expectedReturn: 2200, plans: [widePlan, tanshoPlan] },
};

it("builds the Worker URL with padded parts and the budget", () => {
  expect(
    buildClefBetsUrl(
      { year: "2026", month: "1", day: "8", keibajoCode: "08", raceNumber: "3" },
      2500,
    ),
  ).toBe("/api/clef-rerank/bets/20260108/08/03?budget=2500");
});

it("decodes the multi-type plans and drops unused fields", () => {
  expect(readClefBets(payload)).toStrictEqual({
    oddsFetchedAt: "2026-10-18T06:55:31+09:00",
    runners: payload.runners,
    plans: payload.plans,
  });
});

it("rejects malformed payloads and single-type plans", () => {
  expect(readClefBets({ ...payload, oddsFetchedAt: 1 })).toBeNull();
  expect(readClefBets({ ...payload, runners: [{ horseNumber: 1 }] })).toBeNull();
  expect(readClefBets({ ...payload, plans: { ...payload.plans, plans: [widePlan] } })).toBeNull();
  expect(readClefBets({ ...payload, plans: { ...payload.plans, totalStake: "x" } })).toBeNull();
  expect(
    readClefBets({
      ...payload,
      plans: { ...payload.plans, plans: [widePlan, { ...tanshoPlan, tickets: [] }] },
    }),
  ).toBeNull();
  expect(
    readClefBets({
      ...payload,
      plans: { ...payload.plans, plans: [widePlan, { ...tanshoPlan, legs: [["1"]] }] },
    }),
  ).toBeNull();
  expect(
    readClefBets({
      ...payload,
      plans: {
        ...payload.plans,
        plans: [widePlan, { ...tanshoPlan, tickets: [{ ...tanshoPlan.tickets[0], maxOdds: "x" }] }],
      },
    }),
  ).toBeNull();
  expect(readClefBets({ error: "odds are not available" })).toBeNull();
  expect(readClefBets([payload])).toBeNull();
});

it("snaps budgets onto the 100-yen grid within the accepted range", () => {
  expect(normalizeBudget(2549)).toBe(2500);
  expect(normalizeBudget(20)).toBe(200);
  expect(normalizeBudget(5_000_000)).toBe(1_000_000);
  expect(normalizeBudget(Number.NaN)).toBe(2000);
});

it("labels bet types and styles in Japanese and keeps unknown codes", () => {
  expect(betTypeLabel("3rentan")).toBe("3連単");
  expect(betTypeLabel("win5")).toBe("win5");
  expect(betStyleLabel("multi")).toBe("軸流しマルチ");
  expect(betStyleLabel("other")).toBe("other");
  expect(formatYen(12000)).toBe("12,000円");
});

it("describes each ticket structure like a betting slip", () => {
  expect(describePlan(tanshoPlan)).toBe("11番");
  expect(describePlan(widePlan)).toBe("軸 11 → 相手 4・7");
  expect(describePlan({ ...widePlan, betType: "umatan" })).toBe("1着軸 11 → 相手 4・7");
  expect(describePlan({ ...widePlan, betType: "umatan", style: "multi" })).toBe(
    "軸 11 マルチ → 相手 4・7",
  );
  expect(describePlan({ ...widePlan, style: "box", legs: [[11, 4, 7]] })).toBe("ボックス 11・4・7");
  expect(
    describePlan({
      betType: "3rentan",
      style: "formation",
      axis: 1,
      legs: [[1], [2, 3], [2, 3, 4]],
    }),
  ).toBe("フォーメーション 1着 1 ／ 2着 2・3 ／ 3着 2・3・4");
  expect(
    describePlan({ betType: "3renpuku", style: "formation", axis: 1, legs: [[1], [2, 3]] }),
  ).toBe("フォーメーション 1頭目 1 ／ 2頭目 2・3");
  expect(describePlan({ ...widePlan, style: "nagashi", legs: [[11]] })).toBe("軸 11 → 相手 ");
  expect(describePlan({ ...widePlan, style: "unknown" })).toBe("11番");
});

it("groups horses per leg for every structure", () => {
  expect(planGroups(tanshoPlan)).toStrictEqual([{ label: "", horses: [11] }]);
  expect(planGroups(widePlan)).toStrictEqual([
    { label: "軸", horses: [11] },
    { label: "相手", horses: [4, 7] },
  ]);
  expect(planGroups({ ...widePlan, betType: "umatan" })).toStrictEqual([
    { label: "1着軸", horses: [11] },
    { label: "相手", horses: [4, 7] },
  ]);
  expect(planGroups({ ...widePlan, betType: "umatan", style: "multi" })).toStrictEqual([
    { label: "軸（マルチ）", horses: [11] },
    { label: "相手", horses: [4, 7] },
  ]);
  expect(planGroups({ ...widePlan, style: "multi", legs: [[11]] })).toStrictEqual([
    { label: "軸（マルチ）", horses: [11] },
    { label: "相手", horses: [] },
  ]);
  expect(planGroups({ ...widePlan, style: "box", legs: [[11, 4, 7]] })).toStrictEqual([
    { label: "ボックス", horses: [11, 4, 7] },
  ]);
  expect(
    planGroups({ betType: "3rentan", style: "formation", axis: 1, legs: [[1], [2, 3]] }),
  ).toStrictEqual([
    { label: "1着", horses: [1] },
    { label: "2着", horses: [2, 3] },
  ]);
});

it("formats plan odds, stakes and payouts as ranges", () => {
  expect(formatPlanOdds(widePlan)).toBe("2.4〜3.6");
  expect(formatPlanOdds(tanshoPlan)).toBe("4.3");
  expect(formatPlanStake(widePlan)).toBe("各600円 × 2点 = 1,200円");
  expect(formatPlanStake(tanshoPlan)).toBe("800円");
  expect(
    formatPlanStake({
      totalStake: 1000,
      tickets: [
        { horses: [4, 11], odds: 2.4, maxOdds: 2.7, stake: 700, payout: 1680, payoutMax: 1890 },
        { horses: [7, 11], odds: 3.1, maxOdds: 3.6, stake: 300, payout: 930, payoutMax: 1080 },
      ],
    }),
  ).toBe("2点 計1,000円");
  expect(formatPlanPayout(widePlan)).toBe("1,440円〜2,160円");
  expect(formatPlanPayout(tanshoPlan)).toBe("3,440円");
});

it("summarizes the plan set in one line", () => {
  expect(summarizePlanSet(payload.plans)).toBe("2種類 ／ 3点 ／ 合計 2,000円 ／ 期待払戻 2,200円");
});
