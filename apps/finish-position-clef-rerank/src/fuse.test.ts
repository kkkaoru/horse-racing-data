// Run with bun (bunx vitest).
import { expect, it } from "vitest";

import {
  buildClefInput,
  fuseRanks,
  latestBase,
  parseBasePredictions,
  parseBundle,
  parseClefAnswers,
  toBaseRanks,
  type RerankBundle,
} from "./fuse";

const bundle = (): RerankBundle => ({
  modelVersion: "clef-rerank-v1",
  raceKey: "jra:2026:1018:08:11",
  label: "clef-flash 補正",
  weights: { base: 1, feature: 0.5, clefTop3: 0.5, clefWin: 0.25 },
  runners: [
    { horseNumber: 1, featureScore: 0.1 },
    { horseNumber: 2, featureScore: 0.9 },
    { horseNumber: 3, featureScore: 0.2 },
  ],
  request: {
    state: {
      race: { surface: "turf" },
      runners: [
        { no: 1, br: null, fr: 3 },
        { no: 2, br: null, fr: 1 },
        { no: 3, br: null, fr: 2 },
      ],
    },
    questions: {
      winner: {
        type: "choice",
        instructions: "Which horse wins?",
        criteria: { "1": "Horse 1", "2": "Horse 2", "3": "Horse 3" },
      },
      top3_1: { type: "noul", instructions: "Horse 1 top3?" },
      top3_2: { type: "noul", instructions: "Horse 2 top3?" },
      top3_3: { type: "noul", instructions: "Horse 3 top3?" },
    },
  },
});

const basePrediction = (horseNumber: string, norm: number, at: string) => ({
  horseNumber,
  modelVersion: "jra-cb-v9-sim-2013-clean-nowin1",
  predictedFinishNorm: norm,
  predictionGeneratedAt: at,
});

it("parses a valid bundle and rejects malformed ones", () => {
  expect(parseBundle(bundle())?.modelVersion).toBe("clef-rerank-v1");
  expect(parseBundle({ ...bundle(), weights: { base: 1 } })).toBeNull();
  expect(parseBundle({ ...bundle(), runners: [{ horseNumber: "1" }] })).toBeNull();
  expect(
    parseBundle({ ...bundle(), request: { state: { runners: [{ no: "1" }] }, questions: {} } }),
  ).toBeNull();
  expect(parseBundle("bundle")).toBeNull();
});

it("parses base predictions only when every row is complete", () => {
  expect(
    parseBasePredictions([basePrediction("1", 0, "a"), basePrediction("2", 1, "b")])?.length,
  ).toBe(2);
  expect(parseBasePredictions([basePrediction("1", 0, "a")])).toBeNull();
  expect(parseBasePredictions([basePrediction("1", 0, "a"), { horseNumber: "2" }])).toBeNull();
  expect(parseBasePredictions(null)).toBeNull();
});

it("recovers served ranks from normalized finish positions with horse-number ties", () => {
  expect(
    toBaseRanks([
      basePrediction("3", 0.5, "a"),
      basePrediction("2", 0, "a"),
      basePrediction("1", 0.5, "a"),
    ]),
  ).toStrictEqual([
    { horseNumber: 2, rank: 1 },
    { horseNumber: 1, rank: 2 },
    { horseNumber: 3, rank: 3 },
  ]);
});

it("picks the most recently generated base row", () => {
  expect(
    latestBase([
      basePrediction("1", 0, "2026-10-18T01:00:00Z"),
      basePrediction("2", 1, "2026-10-18T03:00:00Z"),
      basePrediction("3", 0.5, "2026-10-18T02:00:00Z"),
    ]).horseNumber,
  ).toBe("2");
});

it("injects served ranks and drops scratched runners from the clef request", () => {
  expect(
    buildClefInput(bundle(), [
      { horseNumber: 2, rank: 1 },
      { horseNumber: 1, rank: 2 },
    ]),
  ).toStrictEqual({
    model: "clef-flash",
    state: {
      race: { surface: "turf" },
      runners: [
        { no: 1, br: 2, fr: 3 },
        { no: 2, br: 1, fr: 1 },
      ],
    },
    questions: {
      winner: {
        type: "choice",
        instructions: "Which horse wins?",
        criteria: { "1": "Horse 1", "2": "Horse 2" },
      },
      top3_1: { type: "noul", instructions: "Horse 1 top3?" },
      top3_2: { type: "noul", instructions: "Horse 2 top3?" },
    },
  });
});

it("keeps a winner question without criteria unchanged", () => {
  const input = bundle();
  input.request.questions.winner = { type: "choice", instructions: "Which horse wins?" };
  expect(buildClefInput(input, [{ horseNumber: 1, rank: 1 }]).questions).toStrictEqual({
    winner: { type: "choice", instructions: "Which horse wins?" },
    top3_1: { type: "noul", instructions: "Horse 1 top3?" },
  });
});

it("reads binding and REST answer shapes", () => {
  const answers = {
    winner: { probabilities: { "1": 0.6, "2": 0.4 } },
    top3_1: { noul: 0.8 },
    top3_2: { noul: 0.5 },
  };
  expect(parseClefAnswers({ answers }, [1, 2])).toStrictEqual([
    { horseNumber: 1, top3: 0.8, win: 0.6 },
    { horseNumber: 2, top3: 0.5, win: 0.4 },
  ]);
  expect(parseClefAnswers({ result: { answers } }, [1])).toStrictEqual([
    { horseNumber: 1, top3: 0.8, win: 0.6 },
  ]);
});

it("rejects incomplete clef answers", () => {
  expect(parseClefAnswers({ answers: { winner: {} } }, [1])).toBeNull();
  expect(parseClefAnswers({ answers: { winner: { probabilities: { "1": 1 } } } }, [1])).toBeNull();
  expect(parseClefAnswers({ nothing: true }, [1])).toBeNull();
  expect(parseClefAnswers("text", [1])).toBeNull();
});

it("fuses base, feature and clef evidence into a new order", () => {
  expect(
    fuseRanks({
      bundle: bundle(),
      baseRanks: [
        { horseNumber: 1, rank: 1 },
        { horseNumber: 2, rank: 2 },
        { horseNumber: 3, rank: 3 },
      ],
      answers: [
        { horseNumber: 1, win: 0.2, top3: 0.5 },
        { horseNumber: 2, win: 0.7, top3: 0.9 },
        { horseNumber: 3, win: 0.1, top3: 0.2 },
      ],
    }).map((row) => [row.horseNumber, row.rank, row.baseRank]),
  ).toStrictEqual([
    [2, 1, 2],
    [1, 2, 1],
    [3, 3, 3],
  ]);
});

it("breaks equal fused scores by served rank and then horse number", () => {
  const input = bundle();
  input.weights = { base: 0, feature: 0, clefTop3: 0, clefWin: 0 };
  expect(
    fuseRanks({
      bundle: input,
      baseRanks: [
        { horseNumber: 3, rank: 1 },
        { horseNumber: 2, rank: 1 },
        { horseNumber: 1, rank: 2 },
      ],
      answers: [
        { horseNumber: 1, win: 0, top3: 1 },
        { horseNumber: 2, win: 0.5, top3: 0.5 },
        { horseNumber: 3, win: 0.5, top3: 0.5 },
      ],
    }).map((row) => [row.horseNumber, row.rank, row.score]),
  ).toStrictEqual([
    [2, 1, 0],
    [3, 2, 0],
    [1, 3, 0],
  ]);
});

it("ignores runners missing from the bundle or the clef answers", () => {
  expect(
    fuseRanks({
      bundle: bundle(),
      baseRanks: [
        { horseNumber: 1, rank: 1 },
        { horseNumber: 9, rank: 2 },
      ],
      answers: [{ horseNumber: 1, win: 0.5, top3: 0.5 }],
    }),
  ).toStrictEqual([
    { horseNumber: 1, baseRank: 1, score: 0, clefWin: 0.5, clefTop3: 0.5, rank: 1 },
  ]);
});
