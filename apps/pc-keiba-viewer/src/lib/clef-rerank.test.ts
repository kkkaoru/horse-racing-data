// Run with bun (bunx vitest).
import { expect, it } from "vitest";

import { applyClefRerank, applyClefRerankToSection, clefRerankResultKey } from "./clef-rerank";

const feature = (horseNumber: string, norm: number) => ({
  confidenceTier: null,
  horseNumber,
  modelVersion: "jra-joken-703-pooled-yetirank-v2-noexo1",
  predictedFinishNorm: norm,
  predictionGeneratedAt: "2026-10-17T16:11:03.882Z",
  showProbability: null,
  winProbability: null,
});

const served = [feature("01", 0), feature("02", 0.5), feature("03", 1)];

it("builds the Worker result key", () => {
  expect(
    clefRerankResultKey({ year: "2026", mmdd: "1018", keibajoCode: "04", raceBango: "01" }),
  ).toBe("clefrr:v1:result:20261018:04:01");
});

it("replaces the served order with the rerank for the same generation", () => {
  const rerank = JSON.stringify({
    baseGeneratedAt: "2026-10-17T16:11:03.882Z",
    predictions: [{ horseNumber: 3, rank: 1 }, { horseNumber: 1, rank: 2 }, { bad: true }],
  });
  expect(applyClefRerank(served, rerank).map((row) => row.predictedFinishNorm)).toStrictEqual([
    1, 0.5, 0,
  ]);
});

it("keeps the served order for another generation or an unusable result", () => {
  expect(
    applyClefRerank(
      served,
      JSON.stringify({ baseGeneratedAt: "old", predictions: [{ horseNumber: 3, rank: 1 }] }),
    ),
  ).toStrictEqual(served);
  expect(
    applyClefRerank(
      served,
      JSON.stringify({ baseGeneratedAt: "2026-10-17T16:11:03.882Z", predictions: "x" }),
    ),
  ).toStrictEqual(served);
  expect(applyClefRerank(served, "[1]")).toStrictEqual(served);
  expect(applyClefRerank(served, "{broken")).toStrictEqual(served);
  expect(applyClefRerank(served, null)).toStrictEqual(served);
  expect(applyClefRerank([], null)).toStrictEqual([]);
});

it("treats a single reranked horse as rank one", () => {
  expect(
    applyClefRerank(
      [feature("1", 0.3)],
      JSON.stringify({
        baseGeneratedAt: "2026-10-17T16:11:03.882Z",
        predictions: [{ horseNumber: 1, rank: 1 }],
      }),
    ).map((row) => row.predictedFinishNorm),
  ).toStrictEqual([0]);
});

it("applies the rerank inside a finish-prediction section body", () => {
  const rerank = JSON.stringify({
    baseGeneratedAt: "2026-10-17T16:11:03.882Z",
    predictions: [
      { horseNumber: 3, rank: 1 },
      { horseNumber: 1, rank: 2 },
    ],
  });
  expect(
    applyClefRerankToSection(
      {
        type: "finish-prediction",
        inputs: { currentDistance: "1400", modelPredictionFeatures: served },
      },
      rerank,
    ),
  ).toStrictEqual({
    type: "finish-prediction",
    inputs: {
      currentDistance: "1400",
      modelPredictionFeatures: [
        { ...served[0], predictedFinishNorm: 1 },
        served[1],
        { ...served[2], predictedFinishNorm: 0 },
      ],
    },
  });
  expect(
    applyClefRerankToSection({ inputs: { modelPredictionFeatures: "x" } }, rerank),
  ).toStrictEqual({ inputs: { modelPredictionFeatures: "x" } });
  expect(applyClefRerankToSection({ inputs: [] }, rerank)).toStrictEqual({ inputs: [] });
  expect(applyClefRerankToSection("text", rerank)).toBe("text");
});
