// Run with bun (bunx vitest).
import { expect, it } from "vitest";

import {
  getPredictionProbabilityAvailability,
  parsePredictionProbability,
} from "./prediction-probability";

it("preserves numeric probabilities including zero and one", () => {
  expect([0, 1, 0.25].map(parsePredictionProbability)).toStrictEqual([0, 1, 0.25]);
});

it("parses PostgreSQL numeric strings without losing zero", () => {
  expect(["0", "1", "0.25", " 0.25 "].map(parsePredictionProbability)).toStrictEqual([
    0, 1, 0.25, 0.25,
  ]);
});

it.each([
  null,
  undefined,
  "",
  "   ",
  "NaN",
  "invalid",
  -0.1,
  1.1,
  Number.NaN,
  Infinity,
  -Infinity,
  true,
  {},
  [],
])("does not fabricate a probability from invalid or missing input: %s", (value) => {
  expect(parsePredictionProbability(value)).toBeNull();
});

it("distinguishes genuine zero probabilities from missing model outputs", () => {
  expect(
    getPredictionProbabilityAvailability([
      { showProbability: 1, winProbability: 0 },
      { showProbability: 0, winProbability: 1 },
    ]),
  ).toStrictEqual({ show: "available", win: "available" });
  expect(
    getPredictionProbabilityAvailability([{ showProbability: null, winProbability: null }]),
  ).toStrictEqual({ show: "not_provided", win: "not_provided" });
  expect(getPredictionProbabilityAvailability([])).toStrictEqual({
    show: "not_provided",
    win: "not_provided",
  });
});

it("reports mixed coverage independently for each probability field", () => {
  expect(
    getPredictionProbabilityAvailability([
      { showProbability: 0.5, winProbability: 0.25 },
      { showProbability: null, winProbability: 0 },
    ]),
  ).toStrictEqual({ show: "partial", win: "available" });
  expect(
    getPredictionProbabilityAvailability([
      { showProbability: 0.5, winProbability: null },
      { showProbability: 0.8, winProbability: 0.25 },
    ]),
  ).toStrictEqual({ show: "available", win: "partial" });
});
