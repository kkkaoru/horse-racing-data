// Run with bun test apps/sync-realtime-data/src/running-style-race-constraint.test.ts
import { expect, test } from "vitest";

import { applyRaceLevelNigeConstraintForRace } from "./running-style-race-constraint";

const LABELS = ["nige", "senkou", "sashi", "oikomi"] as const;

test("applyRaceLevelNigeConstraintForRace keeps only the strongest nige candidate", () => {
  const predictions = applyRaceLevelNigeConstraintForRace(
    [
      Float64Array.from([0.4, 0.3, 0.2, 0.1]),
      Float64Array.from([0.35, 0.25, 0.25, 0.15]),
      Float64Array.from([0.05, 0.2, 0.5, 0.25]),
    ],
    LABELS,
    4,
  );
  expect(predictions.filter((prediction) => prediction.predictedLabel === "nige")).toHaveLength(1);
  expect(predictions[0]?.predictedLabel).toBe("nige");
});

test("applyRaceLevelNigeConstraintForRace suppresses nige when confidence is low", () => {
  const predictions = applyRaceLevelNigeConstraintForRace(
    [Float64Array.from([0.12, 0.4, 0.3, 0.18]), Float64Array.from([0.11, 0.35, 0.34, 0.2])],
    LABELS,
    4,
  );
  expect(predictions.every((prediction) => prediction.predictedLabel !== "nige")).toBe(true);
});

test("applyRaceLevelNigeConstraintForRace keeps multiple nige horses when disableNigeCap=true", () => {
  const predictions = applyRaceLevelNigeConstraintForRace(
    [
      Float64Array.from([0.4, 0.3, 0.2, 0.1]),
      Float64Array.from([0.35, 0.25, 0.25, 0.15]),
      Float64Array.from([0.05, 0.2, 0.5, 0.25]),
    ],
    LABELS,
    4,
    { disableNigeCap: true },
  );
  expect(predictions.filter((prediction) => prediction.predictedLabel === "nige")).toHaveLength(2);
  expect(predictions[0]?.probabilities.nige).toBeCloseTo(0.4);
  expect(predictions[1]?.probabilities.nige).toBeCloseTo(0.35);
});
