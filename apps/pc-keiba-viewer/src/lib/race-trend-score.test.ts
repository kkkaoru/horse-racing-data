// Run with bun.
import { expect, test } from "vitest";

import {
  computeRawUmabanScores,
  DEFAULT_RACE_TREND_SCORE_CONDITIONS,
  normalizeUmabanScores,
  RACE_TREND_SCORE_CONDITION_KEYS,
  scoreSinglePastRace,
} from "./race-trend-score";
import type { ScoreDetailInput, UmabanContext } from "./race-trend-score";

test("RACE_TREND_SCORE_CONDITION_KEYS lists four condition keys", () => {
  expect(RACE_TREND_SCORE_CONDITION_KEYS).toStrictEqual([
    "frame",
    "jockey",
    "trainer",
    "frameRunningStyle",
  ]);
});

test("DEFAULT_RACE_TREND_SCORE_CONDITIONS enables frame, jockey, and trainer", () => {
  expect(DEFAULT_RACE_TREND_SCORE_CONDITIONS).toStrictEqual({
    frame: true,
    jockey: true,
    trainer: true,
    frameRunningStyle: false,
  });
});

test("scoreSinglePastRace favorite-win: 1 pop 1 fin odds 2.0 returns top-tier bonus only", () => {
  const detail: ScoreDetailInput = {
    popularity: 1,
    finishPosition: 1,
    winOdds: 2.0,
    frameNumber: null,
    jockeyKey: null,
    trainerKey: null,
    runningStyle: null,
  };
  expect(scoreSinglePastRace(detail)).toBe(3.9030899869919438);
});

test("scoreSinglePastRace longshot-win: 3 pop 1 fin odds 50.0 returns big positive", () => {
  const detail: ScoreDetailInput = {
    popularity: 3,
    finishPosition: 1,
    winOdds: 50.0,
    frameNumber: null,
    jockeyKey: null,
    trainerKey: null,
    runningStyle: null,
  };
  expect(scoreSinglePastRace(detail)).toBe(10.096910013008056);
});

test("scoreSinglePastRace favorite-collapses-board: 1 pop 4 fin odds 1.5 returns -3", () => {
  const detail: ScoreDetailInput = {
    popularity: 1,
    finishPosition: 4,
    winOdds: 1.5,
    frameNumber: null,
    jockeyKey: null,
    trainerKey: null,
    runningStyle: null,
  };
  expect(scoreSinglePastRace(detail)).toBe(-3);
});

test("scoreSinglePastRace midshot-board: 5 pop 4 fin odds 6.0 returns base + tier", () => {
  const detail: ScoreDetailInput = {
    popularity: 5,
    finishPosition: 4,
    winOdds: 6.0,
    frameNumber: null,
    jockeyKey: null,
    trainerKey: null,
    runningStyle: null,
  };
  expect(scoreSinglePastRace(detail)).toBe(1.889075625191822);
});

test("scoreSinglePastRace longshot-board: 10 pop 5 fin odds 50.0 returns large bonus", () => {
  const detail: ScoreDetailInput = {
    popularity: 10,
    finishPosition: 5,
    winOdds: 50.0,
    frameNumber: null,
    jockeyKey: null,
    trainerKey: null,
    runningStyle: null,
  };
  expect(scoreSinglePastRace(detail)).toBe(11.747425010840047);
});

test("scoreSinglePastRace outside-board-with-positive-spread clamp: 5 pop 6 fin odds 8.0 returns -1", () => {
  const detail: ScoreDetailInput = {
    popularity: 5,
    finishPosition: 6,
    winOdds: 8.0,
    frameNumber: null,
    jockeyKey: null,
    trainerKey: null,
    runningStyle: null,
  };
  expect(scoreSinglePastRace(detail)).toBe(-1);
});

test("scoreSinglePastRace outside-board-with-favorite-collapse: 18 pop 17 fin odds 200.0 returns 0", () => {
  const detail: ScoreDetailInput = {
    popularity: 18,
    finishPosition: 17,
    winOdds: 200.0,
    frameNumber: null,
    jockeyKey: null,
    trainerKey: null,
    runningStyle: null,
  };
  expect(scoreSinglePastRace(detail)).toBe(0);
});

test("scoreSinglePastRace tierBonus-zero-branch: 8 pop 7 fin odds 30 returns 0", () => {
  const detail: ScoreDetailInput = {
    popularity: 8,
    finishPosition: 7,
    winOdds: 30.0,
    frameNumber: null,
    jockeyKey: null,
    trainerKey: null,
    runningStyle: null,
  };
  expect(scoreSinglePastRace(detail)).toBe(0);
});

test("scoreSinglePastRace favorite-collapsed-late: 1 pop 6 fin odds 1.5 returns -5", () => {
  const detail: ScoreDetailInput = {
    popularity: 1,
    finishPosition: 6,
    winOdds: 1.5,
    frameNumber: null,
    jockeyKey: null,
    trainerKey: null,
    runningStyle: null,
  };
  expect(scoreSinglePastRace(detail)).toBe(-5);
});

test("scoreSinglePastRace dnf: finishPosition 0 returns 0", () => {
  const detail: ScoreDetailInput = {
    popularity: 1,
    finishPosition: 0,
    winOdds: 2.0,
    frameNumber: null,
    jockeyKey: null,
    trainerKey: null,
    runningStyle: null,
  };
  expect(scoreSinglePastRace(detail)).toBe(0);
});

test("scoreSinglePastRace popularity-null-top-tier: null pop 1 fin null odds returns 3", () => {
  const detail: ScoreDetailInput = {
    popularity: null,
    finishPosition: 1,
    winOdds: null,
    frameNumber: null,
    jockeyKey: null,
    trainerKey: null,
    runningStyle: null,
  };
  expect(scoreSinglePastRace(detail)).toBe(3);
});

test("scoreSinglePastRace popularity-null-board-tier: null pop 4 fin odds 10 returns 0", () => {
  const detail: ScoreDetailInput = {
    popularity: null,
    finishPosition: 4,
    winOdds: 10.0,
    frameNumber: null,
    jockeyKey: null,
    trainerKey: null,
    runningStyle: null,
  };
  expect(scoreSinglePastRace(detail)).toBe(0);
});

test("computeRawUmabanScores returns all-null map when no condition is enabled", () => {
  const contexts: UmabanContext[] = [
    { umaban: "1", frameNumber: "1", jockeyKey: null, trainerKey: null, runningStyle: null },
    { umaban: "2", frameNumber: "2", jockeyKey: null, trainerKey: null, runningStyle: null },
  ];
  const details: ScoreDetailInput[] = [
    {
      popularity: 1,
      finishPosition: 1,
      winOdds: 2.0,
      frameNumber: "1",
      jockeyKey: null,
      trainerKey: null,
      runningStyle: null,
    },
  ];
  expect(
    computeRawUmabanScores({
      contexts,
      details,
      conditions: { frame: false, jockey: false, trainer: false, frameRunningStyle: false },
    }),
  ).toStrictEqual(
    new Map([
      ["1", null],
      ["2", null],
    ]),
  );
});

test("computeRawUmabanScores single-condition-sum: frame-only sums all matching details", () => {
  const contexts: UmabanContext[] = [
    { umaban: "1", frameNumber: "3", jockeyKey: null, trainerKey: null, runningStyle: null },
  ];
  const details: ScoreDetailInput[] = [
    {
      popularity: null,
      finishPosition: 1,
      winOdds: null,
      frameNumber: "3",
      jockeyKey: null,
      trainerKey: null,
      runningStyle: null,
    },
    {
      popularity: null,
      finishPosition: 1,
      winOdds: null,
      frameNumber: "3",
      jockeyKey: null,
      trainerKey: null,
      runningStyle: null,
    },
    {
      popularity: null,
      finishPosition: 1,
      winOdds: null,
      frameNumber: "3",
      jockeyKey: null,
      trainerKey: null,
      runningStyle: null,
    },
  ];
  expect(
    computeRawUmabanScores({
      contexts,
      details,
      conditions: { frame: true, jockey: false, trainer: false, frameRunningStyle: false },
    }),
  ).toStrictEqual(new Map([["1", 12.476649250079015]]));
});

test("computeRawUmabanScores single-condition-mismatch: no matching detail returns null", () => {
  const contexts: UmabanContext[] = [
    { umaban: "1", frameNumber: "3", jockeyKey: null, trainerKey: null, runningStyle: null },
  ];
  const details: ScoreDetailInput[] = [
    {
      popularity: 1,
      finishPosition: 1,
      winOdds: 2.0,
      frameNumber: "5",
      jockeyKey: null,
      trainerKey: null,
      runningStyle: null,
    },
  ];
  expect(
    computeRawUmabanScores({
      contexts,
      details,
      conditions: { frame: true, jockey: false, trainer: false, frameRunningStyle: false },
    }),
  ).toStrictEqual(new Map([["1", null]]));
});

test("computeRawUmabanScores two-conditions-additive-sum: each match contributes separately", () => {
  const contexts: UmabanContext[] = [
    { umaban: "1", frameNumber: "3", jockeyKey: "yamada", trainerKey: null, runningStyle: null },
  ];
  const details: ScoreDetailInput[] = [
    {
      popularity: null,
      finishPosition: 1,
      winOdds: null,
      frameNumber: "3",
      jockeyKey: "yamada",
      trainerKey: null,
      runningStyle: null,
    },
    {
      popularity: null,
      finishPosition: 1,
      winOdds: null,
      frameNumber: "3",
      jockeyKey: "ito",
      trainerKey: null,
      runningStyle: null,
    },
    {
      popularity: null,
      finishPosition: 1,
      winOdds: null,
      frameNumber: "5",
      jockeyKey: "yamada",
      trainerKey: null,
      runningStyle: null,
    },
    {
      popularity: null,
      finishPosition: 1,
      winOdds: null,
      frameNumber: "5",
      jockeyKey: "ito",
      trainerKey: null,
      runningStyle: null,
    },
  ];
  expect(
    computeRawUmabanScores({
      contexts,
      details,
      conditions: { frame: true, jockey: true, trainer: false, frameRunningStyle: false },
    }),
  ).toStrictEqual(new Map([["1", expect.closeTo(13.183347464017315, 10)]]));
});

test("computeRawUmabanScores three-conditions: record matching all three counts three times", () => {
  const contexts: UmabanContext[] = [
    { umaban: "1", frameNumber: "3", jockeyKey: "yamada", trainerKey: null, runningStyle: "senko" },
  ];
  const details: ScoreDetailInput[] = [
    {
      popularity: null,
      finishPosition: 1,
      winOdds: null,
      frameNumber: "3",
      jockeyKey: "yamada",
      trainerKey: null,
      runningStyle: "senko",
    },
  ];
  expect(
    computeRawUmabanScores({
      contexts,
      details,
      conditions: { frame: true, jockey: true, trainer: false, frameRunningStyle: true },
    }),
  ).toStrictEqual(new Map([["1", 6.238324625039507]]));
});

test("computeRawUmabanScores umaban-with-no-history yields null entry", () => {
  const contexts: UmabanContext[] = [
    { umaban: "1", frameNumber: "3", jockeyKey: null, trainerKey: null, runningStyle: null },
    { umaban: "2", frameNumber: "7", jockeyKey: null, trainerKey: null, runningStyle: null },
  ];
  const details: ScoreDetailInput[] = [
    {
      popularity: null,
      finishPosition: 1,
      winOdds: null,
      frameNumber: "3",
      jockeyKey: null,
      trainerKey: null,
      runningStyle: null,
    },
  ];
  expect(
    computeRawUmabanScores({
      contexts,
      details,
      conditions: { frame: true, jockey: false, trainer: false, frameRunningStyle: false },
    }),
  ).toStrictEqual(
    new Map([
      ["1", 2.0794415416798357],
      ["2", null],
    ]),
  );
});

test("computeRawUmabanScores record-filter-omitted: all details eligible", () => {
  const contexts: UmabanContext[] = [
    { umaban: "1", frameNumber: "3", jockeyKey: null, trainerKey: null, runningStyle: null },
  ];
  const details: ScoreDetailInput[] = [
    {
      popularity: null,
      finishPosition: 1,
      winOdds: null,
      frameNumber: "3",
      jockeyKey: null,
      trainerKey: null,
      runningStyle: null,
    },
    {
      popularity: null,
      finishPosition: 1,
      winOdds: null,
      frameNumber: "3",
      jockeyKey: null,
      trainerKey: null,
      runningStyle: null,
    },
  ];
  expect(
    computeRawUmabanScores({
      contexts,
      details,
      conditions: { frame: true, jockey: false, trainer: false, frameRunningStyle: false },
    }),
  ).toStrictEqual(new Map([["1", expect.closeTo(6.591673732008657, 10)]]));
});

test("computeRawUmabanScores record-filter-excludes: filter rejects every detail returns null", () => {
  const contexts: UmabanContext[] = [
    { umaban: "1", frameNumber: "3", jockeyKey: null, trainerKey: null, runningStyle: null },
  ];
  const details: ScoreDetailInput[] = [
    {
      popularity: null,
      finishPosition: 1,
      winOdds: null,
      frameNumber: "3",
      jockeyKey: null,
      trainerKey: null,
      runningStyle: null,
    },
    {
      popularity: null,
      finishPosition: 1,
      winOdds: null,
      frameNumber: "3",
      jockeyKey: null,
      trainerKey: null,
      runningStyle: null,
    },
  ];
  expect(
    computeRawUmabanScores({
      contexts,
      details,
      conditions: { frame: true, jockey: false, trainer: false, frameRunningStyle: false },
      recordFilter: () => false,
    }),
  ).toStrictEqual(new Map([["1", null]]));
});

test("computeRawUmabanScores record-filter-includes-subset: only filter-pass details contribute", () => {
  const contexts: UmabanContext[] = [
    { umaban: "1", frameNumber: "3", jockeyKey: null, trainerKey: null, runningStyle: null },
  ];
  const details: ScoreDetailInput[] = [
    {
      popularity: null,
      finishPosition: 1,
      winOdds: null,
      frameNumber: "3",
      jockeyKey: null,
      trainerKey: null,
      runningStyle: "senko",
    },
    {
      popularity: null,
      finishPosition: 1,
      winOdds: null,
      frameNumber: "3",
      jockeyKey: null,
      trainerKey: null,
      runningStyle: "senko",
    },
    {
      popularity: null,
      finishPosition: 1,
      winOdds: null,
      frameNumber: "3",
      jockeyKey: null,
      trainerKey: null,
      runningStyle: "oikomi",
    },
  ];
  expect(
    computeRawUmabanScores({
      contexts,
      details,
      conditions: { frame: true, jockey: false, trainer: false, frameRunningStyle: false },
      recordFilter: ({ detail }) => detail.runningStyle === "senko",
    }),
  ).toStrictEqual(new Map([["1", expect.closeTo(6.591673732008657, 10)]]));
});

test("normalizeUmabanScores all-null returns all-null map", () => {
  const raw = new Map<string, number | null>([
    ["1", null],
    ["2", null],
  ]);
  expect(normalizeUmabanScores(raw)).toStrictEqual(
    new Map([
      ["1", null],
      ["2", null],
    ]),
  );
});

test("normalizeUmabanScores all-tied returns 0.5 for every numeric entry", () => {
  const raw = new Map<string, number | null>([
    ["1", 3],
    ["2", 3],
    ["3", 3],
  ]);
  expect(normalizeUmabanScores(raw)).toStrictEqual(
    new Map([
      ["1", 0.5],
      ["2", 0.5],
      ["3", 0.5],
    ]),
  );
});

test("normalizeUmabanScores three-umaban-relative min-max scales to [0, 1]", () => {
  const raw = new Map<string, number | null>([
    ["1", 2],
    ["2", 6],
    ["3", 4],
  ]);
  expect(normalizeUmabanScores(raw)).toStrictEqual(
    new Map([
      ["1", 0],
      ["2", 1],
      ["3", 0.5],
    ]),
  );
});

test("normalizeUmabanScores normalization-after-sum: raw 10/30/20 scales to 0/1/0.5", () => {
  const raw = new Map<string, number | null>([
    ["1", 10],
    ["2", 30],
    ["3", 20],
  ]);
  expect(normalizeUmabanScores(raw)).toStrictEqual(
    new Map([
      ["1", 0],
      ["2", 1],
      ["3", 0.5],
    ]),
  );
});

test("normalizeUmabanScores mixed-null keeps null entries and scales the rest", () => {
  const raw = new Map<string, number | null>([
    ["1", 2],
    ["2", null],
    ["3", 6],
  ]);
  expect(normalizeUmabanScores(raw)).toStrictEqual(
    new Map([
      ["1", 0],
      ["2", null],
      ["3", 1],
    ]),
  );
});

test("normalizeUmabanScores tied with null mixed returns 0.5 for numerics and null for nulls", () => {
  const raw = new Map<string, number | null>([
    ["1", 4],
    ["2", null],
    ["3", 4],
  ]);
  expect(normalizeUmabanScores(raw)).toStrictEqual(
    new Map([
      ["1", 0.5],
      ["2", null],
      ["3", 0.5],
    ]),
  );
});

test("computeRawUmabanScores trainer-condition: matches details that share a trainer key", () => {
  const contexts: UmabanContext[] = [
    {
      umaban: "1",
      frameNumber: "3",
      jockeyKey: null,
      trainerKey: "yamamoto",
      runningStyle: null,
    },
  ];
  const details: ScoreDetailInput[] = [
    {
      popularity: null,
      finishPosition: 1,
      winOdds: null,
      frameNumber: "3",
      jockeyKey: null,
      trainerKey: "yamamoto",
      runningStyle: null,
    },
    {
      popularity: null,
      finishPosition: 1,
      winOdds: null,
      frameNumber: "3",
      jockeyKey: null,
      trainerKey: "other",
      runningStyle: null,
    },
  ];
  expect(
    computeRawUmabanScores({
      contexts,
      details,
      conditions: { frame: false, jockey: false, trainer: true, frameRunningStyle: false },
    }),
  ).toStrictEqual(new Map([["1", 2.0794415416798357]]));
});

test("computeRawUmabanScores trainer-condition: no record matches when context trainerKey is null", () => {
  const contexts: UmabanContext[] = [
    { umaban: "1", frameNumber: "3", jockeyKey: null, trainerKey: null, runningStyle: null },
  ];
  const details: ScoreDetailInput[] = [
    {
      popularity: null,
      finishPosition: 1,
      winOdds: null,
      frameNumber: "3",
      jockeyKey: null,
      trainerKey: "yamamoto",
      runningStyle: null,
    },
  ];
  expect(
    computeRawUmabanScores({
      contexts,
      details,
      conditions: { frame: false, jockey: false, trainer: true, frameRunningStyle: false },
    }),
  ).toStrictEqual(new Map([["1", null]]));
});
