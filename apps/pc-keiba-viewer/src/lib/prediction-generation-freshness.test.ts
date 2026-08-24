// Run with bun. `bunx vitest run src/lib/prediction-generation-freshness.test.ts`

import { expect, it } from "vitest";

import {
  arePredictionFeaturesFreshForGeneration,
  normalizeExpectedPredictionGeneratedAt,
} from "./prediction-generation-freshness";

it("normalizes a valid expected prediction generation timestamp", () => {
  expect(normalizeExpectedPredictionGeneratedAt("2026-08-24T03:40:15Z")).toBe(
    "2026-08-24T03:40:15.000Z",
  );
});

it("rejects an invalid expected prediction generation timestamp", () => {
  expect(normalizeExpectedPredictionGeneratedAt("invalid")).toBe(null);
});

it("accepts prediction rows when every timestamp is at least the expected generation", () => {
  expect(
    arePredictionFeaturesFreshForGeneration(
      [
        { predictionGeneratedAt: "2026-08-24T03:40:15.000Z" },
        { predictionGeneratedAt: "2026-08-24T03:40:16.000Z" },
      ],
      "2026-08-24T03:40:15.000Z",
    ),
  ).toBe(true);
});

it("rejects an empty prediction result", () => {
  expect(arePredictionFeaturesFreshForGeneration([], "2026-08-24T03:40:15.000Z")).toBe(false);
});

it("rejects mixed fresh and stale prediction rows", () => {
  expect(
    arePredictionFeaturesFreshForGeneration(
      [
        { predictionGeneratedAt: "2026-08-24T03:40:15.000Z" },
        { predictionGeneratedAt: "2026-08-24T03:40:14.999Z" },
      ],
      "2026-08-24T03:40:15.000Z",
    ),
  ).toBe(false);
});

it("rejects missing and invalid prediction row timestamps", () => {
  expect(
    arePredictionFeaturesFreshForGeneration(
      [{ predictionGeneratedAt: null }],
      "2026-08-24T03:40:15.000Z",
    ),
  ).toBe(false);
  expect(
    arePredictionFeaturesFreshForGeneration(
      [{ predictionGeneratedAt: "invalid" }],
      "2026-08-24T03:40:15.000Z",
    ),
  ).toBe(false);
});
