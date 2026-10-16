// Runs with Bun through Vitest; all inputs are in-memory fixtures.
import { expect, it } from "vitest";

import { isArcPredictionRace, readArcPrediction, type ArcRunner } from "./arc-prediction";

const rows = Array.from({ length: 16 }, (_, index) => ({
  horseId: `horse-${index + 1}`,
  umaban: index + 1,
  name: `Horse ${index + 1}`,
  baseScore: index / 100,
  winScore: -index / 100,
  predictedRank: 16 - index,
}));
const runners: ArcRunner[] = rows.map((row) => ({
  umaban: String(row.umaban),
  bamei: row.name,
  kettoTorokuBango: row.horseId,
}));
const card = {
  raceDate: "2026-10-04",
  featureCutoffExclusive: "2026-10-04",
  liveFeatures: false,
  evaluationWasTuned: true,
  policy: "top2-win-rerank",
  modelVersion: "arc-test-v2",
  modelSha256: "a".repeat(64),
  featureSnapshotAt: "2026-10-03T17:46:11.825Z",
  trainingMaxDate: "2025-10-05",
  predictions: rows,
};
const scope = {
  source: "jra",
  year: "2026",
  month: "10",
  day: "04",
  keibajoCode: "A8",
  raceNumber: "05",
};

it("routes only the supported Arc card, accepting unpadded route numbers", () => {
  expect(isArcPredictionRace(scope)).toBe(true);
  expect(isArcPredictionRace({ ...scope, day: "4", raceNumber: "5" })).toBe(true);
});
it.each([
  { source: "nar" },
  { year: "2027" },
  { month: "09" },
  { day: "03" },
  { keibajoCode: "C8" },
  { raceNumber: "06" },
])("does not route a different race: %j", (change) => {
  expect(isArcPredictionRace({ ...scope, ...change })).toBe(false);
});
it("retains dedicated ranks and raw margins without making probabilities", () => {
  const parsed = readArcPrediction(card, runners);
  expect(parsed.predictions.map((row) => row.umaban)).toStrictEqual([
    16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1,
  ]);
  expect(parsed.predictions[0]).toStrictEqual({
    horseId: "horse-16",
    umaban: 16,
    name: "Horse 16",
    baseScore: 0.15,
    winScore: -0.15,
    predictedRank: 1,
  });
  expect(parsed.modelVersion).toBe("arc-test-v2");
  expect(parsed.trainingMaxDate).toBe("2025-10-05");
  expect(parsed.featureSnapshotAt).toBe("2026-10-03T17:46:11.825Z");
});
it("validates unknown registration runners by current horse number and name", () => {
  expect(
    readArcPrediction(
      card,
      runners.map((runner) => ({ ...runner, kettoTorokuBango: "0000000000" })),
    ).predictions,
  ).toHaveLength(16);
});
it.each([null, [], "invalid"])("rejects a non-object response %j", (value) => {
  expect(() => readArcPrediction(value, runners)).toThrow("Invalid Arc prediction card");
});
it.each([
  { raceDate: "2026-10-05" },
  { featureCutoffExclusive: "2026-10-05" },
  { liveFeatures: true },
  { evaluationWasTuned: false },
  { policy: "base-only" },
  { modelVersion: null },
  { modelVersion: "jra-test" },
  { modelSha256: null },
  { modelSha256: "bad" },
  { featureSnapshotAt: null },
  { featureSnapshotAt: "bad" },
  { trainingMaxDate: null },
  { trainingMaxDate: "bad" },
  { trainingMaxDate: "2025-99-99" },
  { trainingMaxDate: "2026-10-04" },
  { predictions: null },
  { predictions: [] },
])("rejects invalid provenance or field size: %j", (change) => {
  expect(() => readArcPrediction({ ...card, ...change }, runners)).toThrow(
    "Invalid Arc prediction card",
  );
});
it("refuses a current field changed by a withdrawal", () => {
  expect(() => readArcPrediction(card, runners.slice(1))).toThrow("Invalid Arc prediction card");
});
it.each([
  { horseId: null },
  { horseId: " " },
  { name: null },
  { name: " " },
  { umaban: "1" },
  { umaban: 1.5 },
  { umaban: 0 },
  { umaban: 17 },
  { predictedRank: "1" },
  { predictedRank: 1.5 },
  { predictedRank: 0 },
  { predictedRank: 17 },
  { baseScore: null },
  { baseScore: Infinity },
  { winScore: null },
  { winScore: NaN },
])("rejects invalid prediction data: %j", (change) => {
  expect(() =>
    readArcPrediction(
      { ...card, predictions: [{ ...rows[0], ...change }, ...rows.slice(1)] },
      runners,
    ),
  ).toThrow("Invalid Arc prediction row");
});
it("rejects a non-object prediction row", () => {
  expect(() =>
    readArcPrediction({ ...card, predictions: [null, ...rows.slice(1)] }, runners),
  ).toThrow("Invalid Arc prediction row");
});
it.each([{ name: "Different horse" }, { horseId: "different-registration" }])(
  "refuses a replacement horse: %j",
  (change) => {
    expect(() =>
      readArcPrediction(
        { ...card, predictions: [{ ...rows[0], ...change }, ...rows.slice(1)] },
        runners,
      ),
    ).toThrow("Arc prediction roster does not match the current declarations");
  },
);
it("rejects duplicate numbers even when every individual row matches", () => {
  expect(() =>
    readArcPrediction({ ...card, predictions: [rows[1], ...rows.slice(1)] }, runners),
  ).toThrow("Arc prediction roster does not match the current declarations");
});
it("rejects duplicated source identities for unknown registrations", () => {
  expect(() =>
    readArcPrediction(
      { ...card, predictions: rows.map((row) => ({ ...row, horseId: "shared" })) },
      runners.map((runner) => ({ ...runner, kettoTorokuBango: "0000000000" })),
    ),
  ).toThrow("Arc prediction roster does not match the current declarations");
});
it("rejects duplicate ranks", () => {
  expect(() =>
    readArcPrediction(
      { ...card, predictions: rows.map((row) => Object.assign({}, row, { predictedRank: 1 })) },
      runners,
    ),
  ).toThrow("Arc prediction roster does not match the current declarations");
});
it("refuses missing current names rather than guessing identity", () => {
  expect(() =>
    readArcPrediction(
      card,
      runners.map((runner) => Object.assign({}, runner, { bamei: null })),
    ),
  ).toThrow("Arc prediction roster does not match the current declarations");
});
