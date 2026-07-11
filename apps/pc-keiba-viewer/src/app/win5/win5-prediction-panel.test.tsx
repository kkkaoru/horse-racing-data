// Run with bun (bunx vitest)

import { cleanup, render, screen } from "@testing-library/react";
import React from "react";
import { afterEach, expect, test } from "vitest";

import type { Win5PredictionPayload } from "../../lib/win5/types";
import { Win5PredictionPanel } from "./win5-prediction-panel";

afterEach(() => {
  cleanup();
});

test("Win5PredictionPanel shows the recommended-budget preset button when recommendedBudgetYen is present", () => {
  const payloadWithRecommendation: Win5PredictionPayload = {
    defaultBudgetYen: 2000,
    kaisaiNen: "2026",
    kaisaiTsukihi: "0524",
    legs: [],
    modelVersion: "win5-xgb-v7-lineage-v1",
    plans: {},
    predictedAt: "2026-05-24T00:00:00.000Z",
    recommendedBudgetYen: 8000,
  };
  render(
    <Win5PredictionPanel day="24" month="05" prediction={payloadWithRecommendation} year="2026" />,
  );
  expect(screen.getByRole("button", { name: "推奨 8,000円" })).toBeTruthy();
});

// Regression guard: a prior 250_000 fallback meant this button always
// rendered a recommendation, even one built on a fabricated payout average.
// When recommendedBudgetYen is omitted (no real average available), the
// button must be omitted too rather than falling back to a guessed value.
test("Win5PredictionPanel omits the recommended-budget preset button when recommendedBudgetYen is omitted", () => {
  const payloadWithoutRecommendation: Win5PredictionPayload = {
    defaultBudgetYen: 2000,
    kaisaiNen: "2026",
    kaisaiTsukihi: "0524",
    legs: [],
    modelVersion: "win5-xgb-v7-lineage-v1",
    plans: {},
    predictedAt: "2026-05-24T00:00:00.000Z",
  };
  render(
    <Win5PredictionPanel
      day="24"
      month="05"
      prediction={payloadWithoutRecommendation}
      year="2026"
    />,
  );
  expect(screen.queryByRole("button", { name: /推奨/u })).toBeNull();
  expect(screen.getByRole("button", { name: "標準 2,000円" })).toBeTruthy();
});
