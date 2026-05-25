// run with: bun run test
import { expect, it } from "vitest";
import {
  applyRaceLevelNigeConstraint,
  applyRaceLevelNigeConstraintForRace,
} from "./running-style-race-constraint";

const LABELS = ["nige", "senkou", "sashi", "oikomi"] as const;

it("applyRaceLevelNigeConstraint zeros nige when below threshold", () => {
  const prediction = applyRaceLevelNigeConstraint(
    Float64Array.from([0.1, 0.5, 0.3, 0.1]),
    LABELS,
    { minNigeProbability: 0.2 },
  );
  expect(prediction.probabilities.nige).toBe(0);
  expect(prediction.predictedLabel).toBe("senkou");
});

it("applyRaceLevelNigeConstraint preserves nige when above threshold", () => {
  const prediction = applyRaceLevelNigeConstraint(
    Float64Array.from([0.4, 0.3, 0.2, 0.1]),
    LABELS,
    { minNigeProbability: 0.2 },
  );
  expect(prediction.predictedLabel).toBe("nige");
});

it("applyRaceLevelNigeConstraint uses the default 0.18 threshold when not specified", () => {
  const prediction = applyRaceLevelNigeConstraint(
    Float64Array.from([0.1, 0.5, 0.3, 0.1]),
    LABELS,
  );
  expect(prediction.probabilities.nige).toBe(0);
});

it("applyRaceLevelNigeConstraintForRace applies single-horse constraint when only one prediction", () => {
  const predictions = applyRaceLevelNigeConstraintForRace(
    [Float64Array.from([0.05, 0.6, 0.25, 0.1])],
    LABELS,
    4,
  );
  expect(predictions.length).toBe(1);
  expect(predictions[0]!.predictedLabel).toBe("senkou");
});

it("applyRaceLevelNigeConstraintForRace returns empty array when no input rows", () => {
  const predictions = applyRaceLevelNigeConstraintForRace([], LABELS, 4);
  expect(predictions).toStrictEqual([]);
});

it("applyRaceLevelNigeConstraintForRace ignores nige cap when disableNigeCap=true with single row", () => {
  const predictions = applyRaceLevelNigeConstraintForRace(
    [Float64Array.from([0.05, 0.6, 0.25, 0.1])],
    LABELS,
    4,
    { disableNigeCap: true },
  );
  expect(predictions[0]!.probabilities.nige).toBeCloseTo(0.05);
});
