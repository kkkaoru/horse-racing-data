// Run with: bun run --filter finish-position-cron test
import { expect, test } from "vitest";
import { applyEtop2Scores, isEtop2OverrideActive } from "./etop2";

test("applyEtop2Scores promotes CB#2 when XGB#1 equals CB#2 and class is not 701", () => {
  const scores = applyEtop2Scores({
    cbScores: [3, 2, 1, 0.5],
    raceClass: "703",
    xgbScores: [0.1, 9, 0.2, 0.3],
  });
  expect(scores).toStrictEqual([3.5, 4, 1, 0.5]);
});

test("applyEtop2Scores keeps pure CatBoost when XGB#1 equals CB#1", () => {
  const scores = applyEtop2Scores({
    cbScores: [3, 2, 1, 0.5],
    raceClass: "703",
    xgbScores: [9, 0.1, 0.2, 0.3],
  });
  expect(scores).toStrictEqual([3, 2, 1, 0.5]);
});

test("applyEtop2Scores keeps pure CatBoost when XGB#1 is CB#3 or lower", () => {
  const scores = applyEtop2Scores({
    cbScores: [3, 2, 1, 0.5],
    raceClass: "703",
    xgbScores: [0.1, 0.2, 9, 0.3],
  });
  expect(scores).toStrictEqual([3, 2, 1, 0.5]);
});

test("applyEtop2Scores suppresses override for excluded class 701", () => {
  const scores = applyEtop2Scores({
    cbScores: [3, 2, 1, 0.5],
    raceClass: "701",
    xgbScores: [0.1, 9, 0.2, 0.3],
  });
  expect(scores).toStrictEqual([3, 2, 1, 0.5]);
});

test("applyEtop2Scores treats null class as override-eligible", () => {
  const scores = applyEtop2Scores({
    cbScores: [3, 2, 1, 0.5],
    raceClass: null,
    xgbScores: [0.1, 9, 0.2, 0.3],
  });
  expect(scores).toStrictEqual([3.5, 4, 1, 0.5]);
});

test("applyEtop2Scores returns the scores unchanged for a single horse", () => {
  const scores = applyEtop2Scores({ cbScores: [3], raceClass: "703", xgbScores: [9] });
  expect(scores).toStrictEqual([3]);
});

test("isEtop2OverrideActive is true when XGB#1 matches CB#2 for an eligible class", () => {
  const active = isEtop2OverrideActive({
    cbScores: [3, 2, 1, 0.5],
    raceClass: "703",
    xgbScores: [0.1, 9, 0.2, 0.3],
  });
  expect(active).toBe(true);
});

test("isEtop2OverrideActive is false when XGB#1 matches CB#1", () => {
  const active = isEtop2OverrideActive({
    cbScores: [3, 2, 1, 0.5],
    raceClass: "703",
    xgbScores: [9, 0.1, 0.2, 0.3],
  });
  expect(active).toBe(false);
});

test("isEtop2OverrideActive is false for excluded class 701", () => {
  const active = isEtop2OverrideActive({
    cbScores: [3, 2, 1, 0.5],
    raceClass: "701",
    xgbScores: [0.1, 9, 0.2, 0.3],
  });
  expect(active).toBe(false);
});

test("isEtop2OverrideActive is false for a single-horse race", () => {
  const active = isEtop2OverrideActive({ cbScores: [3], raceClass: "703", xgbScores: [9] });
  expect(active).toBe(false);
});
