// Run with bun (bunx vitest).
import { expect, it } from "vitest";

import {
  betStyleLabel,
  betTypeLabel,
  buildClefBetsUrl,
  formatLegs,
  formatOdds,
  formatPayout,
  formatYen,
  normalizeBudget,
  readClefBets,
  summarizeSuggestion,
} from "./clef-bets";

const payload = {
  raceKey: "20261018:08:03",
  oddsFetchedAt: "2026-10-18T06:55:31+09:00",
  generatedAt: "2026-10-17T21:59:10.598Z",
  runners: [
    {
      horseNumber: 11,
      frameNumber: 8,
      horseName: "イレブン",
      jockey: "騎手A",
      trainer: "調教師A",
      owner: "馬主A",
      clefWin: 0.4,
      clefTop3: 0.9,
    },
  ],
  suggestion: {
    betType: "wide",
    style: "nagashi",
    axis: 11,
    legs: [[11], [4, 7]],
    budget: 2000,
    totalStake: 2000,
    expectedReturn: 2100,
    tickets: [
      {
        horses: [4, 11],
        odds: 2.4,
        maxOdds: 2.7,
        probability: 0.4,
        stake: 1000,
        payout: 2400,
        payoutMax: 2700,
      },
    ],
  },
};

it("builds the Worker URL with padded parts and the budget", () => {
  expect(
    buildClefBetsUrl(
      { year: "2026", month: "1", day: "8", keibajoCode: "08", raceNumber: "3" },
      2500,
    ),
  ).toBe("/api/clef-rerank/bets/20260108/08/03?budget=2500");
});

it("decodes a valid payload and drops unused fields", () => {
  expect(readClefBets(payload)).toStrictEqual({
    oddsFetchedAt: "2026-10-18T06:55:31+09:00",
    runners: payload.runners,
    suggestion: payload.suggestion,
  });
});

it("rejects malformed payloads", () => {
  expect(readClefBets({ ...payload, oddsFetchedAt: 1 })).toBeNull();
  expect(readClefBets({ ...payload, runners: [{ horseNumber: 1 }] })).toBeNull();
  expect(
    readClefBets({ ...payload, suggestion: { ...payload.suggestion, tickets: [] } }),
  ).toBeNull();
  expect(
    readClefBets({ ...payload, suggestion: { ...payload.suggestion, legs: [["1"]] } }),
  ).toBeNull();
  expect(
    readClefBets({
      ...payload,
      suggestion: {
        ...payload.suggestion,
        tickets: [{ ...payload.suggestion.tickets[0], maxOdds: "x" }],
      },
    }),
  ).toBeNull();
  expect(readClefBets({ error: "odds are not available" })).toBeNull();
  expect(readClefBets([payload])).toBeNull();
});

it("snaps budgets onto the 100-yen grid within the accepted range", () => {
  expect(normalizeBudget(2549)).toBe(2500);
  expect(normalizeBudget(20)).toBe(100);
  expect(normalizeBudget(5_000_000)).toBe(1_000_000);
  expect(normalizeBudget(Number.NaN)).toBe(2000);
});

it("labels bet types and styles in Japanese and keeps unknown codes", () => {
  expect(betTypeLabel("3rentan")).toBe("3連単");
  expect(betTypeLabel("win5")).toBe("win5");
  expect(betStyleLabel("multi")).toBe("軸流しマルチ");
  expect(betStyleLabel("other")).toBe("other");
});

it("formats yen, odds ranges and payout ranges", () => {
  expect(formatYen(12000)).toBe("12,000円");
  expect(formatOdds({ odds: 4.3, maxOdds: null })).toBe("4.3");
  expect(formatOdds({ odds: 1.7, maxOdds: 2.4 })).toBe("1.7〜2.4");
  expect(formatPayout({ payout: 3400, payoutMax: null })).toBe("3,400円");
  expect(formatPayout({ payout: 3400, payoutMax: 4800 })).toBe("3,400円〜4,800円");
});

it("formats legs as columns except for boxes and single legs", () => {
  expect(formatLegs({ style: "formation", legs: [[1], [2, 3], [2, 3, 4]] })).toBe(
    "1列目: 1 ／ 2列目: 2, 3 ／ 3列目: 2, 3, 4",
  );
  expect(formatLegs({ style: "box", legs: [[1, 2, 3]] })).toBe("1, 2, 3");
  expect(formatLegs({ style: "single", legs: [[5]] })).toBe("5");
});

it("summarizes the suggestion in one line", () => {
  expect(summarizeSuggestion(payload.suggestion)).toBe(
    "ワイド 軸流し ／ 軸 11 ／ 1列目: 11 ／ 2列目: 4, 7 ／ 1点 ／ 合計 2,000円 ／ 期待払戻 2,100円",
  );
});
