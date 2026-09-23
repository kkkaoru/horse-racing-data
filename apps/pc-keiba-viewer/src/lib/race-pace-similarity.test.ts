// Run with bun (vitest).
import { expect, it } from "vitest";

import {
  computeFinishPositionSimilarity,
  computeRacePaceSimilarity,
  nearestWeighted,
  parseVectorText,
  type FinishPositionCandidate,
  type RacePaceCornerCandidate,
} from "./race-pace-similarity";

it("parses pgvector text and rejects malformed values", () => {
  expect(parseVectorText("[0.5,1,0]")).toStrictEqual([0.5, 1, 0]);
  expect(parseVectorText(" [1] ")).toStrictEqual([1]);
  expect(parseVectorText(null)).toBeNull();
  expect(parseVectorText("0.5,1")).toBeNull();
  expect(parseVectorText("[0.5")).toBeNull();
  expect(parseVectorText("[a,1]")).toBeNull();
});

it("returns null when there are no candidates", () => {
  expect(
    computeRacePaceSimilarity({
      candidates: [],
      horseNumber: "1",
      runnerCount: 10,
      vector: [0, 0],
    }),
  ).toBeNull();
});

it("weights corners by 1 / (1 + L2 distance) and scales them to the field size", () => {
  const candidates: RacePaceCornerCandidate[] = [
    { corners: [0, 0, 0, 0], vector: [0, 0] },
    { corners: [1, 1, null, null], vector: [3, 4] },
  ];
  const result = computeRacePaceSimilarity({
    candidates,
    horseNumber: "7",
    runnerCount: 11,
    vector: [0, 0],
  });
  // weights: 1/(1+0)=1 and 1/(1+5)=1/6; corner1 = (0*1 + 1*(1/6)) / (7/6) = 1/7.
  expect(result).toStrictEqual({
    corner1: (1 / 7) * 10 + 1,
    corner2: (1 / 7) * 10 + 1,
    corner3: 1,
    corner4: 1,
    horseNumber: "7",
    neighborCount: 2,
    similarityScore: 7 / 12,
  });
});

it("keeps only the 40 nearest candidates", () => {
  const near: RacePaceCornerCandidate[] = Array.from({ length: 40 }, () => ({
    corners: [0, 0, 0, 0],
    vector: [0],
  }));
  const far: RacePaceCornerCandidate = { corners: [1, 1, 1, 1], vector: [100] };
  const result = computeRacePaceSimilarity({
    candidates: [far, ...near],
    horseNumber: "1",
    runnerCount: 2,
    vector: [0],
  });
  expect(result?.neighborCount).toBe(40);
  expect(result?.corner1).toBe(1);
});

it("returns null corners when every neighbour lacks that corner", () => {
  const result = computeRacePaceSimilarity({
    candidates: [{ corners: [null, 0.5, null, null], vector: [1, 1] }],
    horseNumber: "2",
    runnerCount: 3,
    vector: [1],
  });
  expect([result?.corner1, result?.corner2, result?.corner3, result?.corner4]).toStrictEqual([
    null,
    2,
    null,
    null,
  ]);
});

it("returns null finish similarity when there are no candidates", () => {
  expect(
    computeFinishPositionSimilarity({
      candidates: [],
      horseNumber: "1",
      runnerCount: 12,
      vector: [0],
    }),
  ).toBeNull();
});

it("scores finish similarity with weighted finish norm and plain win and show shares", () => {
  const candidates: FinishPositionCandidate[] = [
    { finishNorm: 0, finishPosition: 1, vector: [0, 0] },
    { finishNorm: 1, finishPosition: 3, vector: [3, 4] },
    { finishNorm: 0.5, finishPosition: null, vector: [0, 0] },
    { finishNorm: 1, finishPosition: 9, vector: [30, 40] },
  ];
  const result = computeFinishPositionSimilarity({
    candidates,
    horseNumber: "4",
    runnerCount: 13,
    vector: [0, 0],
  });
  // weights 1, 1, 1/6, 1/51; shares over 4 neighbours: 1 win, 2 top-3.
  const totalWeight = 1 + 1 + 1 / 6 + 1 / 51;
  const finishNorm = (0.5 + 1 / 6 + 1 / 51) / totalWeight;
  expect(result?.averageFinishPosition).toBeCloseTo(finishNorm * 12 + 1, 10);
  expect(result?.neighborCount).toBe(4);
  expect(result?.winRate).toBe(0.25);
  expect(result?.showRate).toBe(0.5);
  expect(result?.similarityScore).toBeCloseTo(totalWeight / 4, 10);
  expect(result?.horseNumber).toBe("4");
});

it("orders nearest neighbours by distance with 1 / (1 + distance) weights", () => {
  expect(
    nearestWeighted(
      [
        { name: "far", vector: [6, 8] },
        { name: "near", vector: [0, 0] },
      ],
      [0, 0],
      1,
    ),
  ).toStrictEqual([{ candidate: { name: "near", vector: [0, 0] }, weight: 1 }]);
});
