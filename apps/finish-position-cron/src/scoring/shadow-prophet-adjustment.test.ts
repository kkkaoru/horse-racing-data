// Run with bun run test. Pure shared adjustment checks, no I/O.
import { expect, test } from "vitest";
import {
  adjustShadowProphetScores,
  type ShadowProphetInput,
  type ShadowScore,
} from "./shadow-prophet-adjustment";

const INPUT: ShadowProphetInput<ShadowScore> = {
  category: "nar",
  decision: { enabled: true, weight: 0.5 },
  entries: [
    { ketto_toroku_bango: "a", prophet_entity_coverage: 1, prophet_entity_performance_mean: 0 },
    { ketto_toroku_bango: "b", prophet_entity_coverage: "1", prophet_entity_performance_mean: "2" },
  ],
  predictions: [
    { kettoTorokuBango: "a", predictedScore: 0 },
    { kettoTorokuBango: "b", predictedScore: 2 },
  ],
};

test("applies the same centered population-stddev formula", () => {
  expect(adjustShadowProphetScores(INPUT)).toStrictEqual([
    { kettoTorokuBango: "a", predictedScore: -0.5 },
    { kettoTorokuBango: "b", predictedScore: 2.5 },
  ]);
});
test("honors category lists and weight overrides", () => {
  expect(
    adjustShadowProphetScores({ ...INPUT, config: { enabled: "ban-ei, nar", weight: "1" } }),
  ).toStrictEqual([
    { kettoTorokuBango: "a", predictedScore: -1 },
    { kettoTorokuBango: "b", predictedScore: 3 },
  ]);
});
test.each(["0", "FALSE", "off", "disabled", "jra"])("disables with %s", (enabled) => {
  expect(adjustShadowProphetScores({ ...INPUT, config: { enabled } })).toStrictEqual([
    { kettoTorokuBango: "a", predictedScore: 0 },
    { kettoTorokuBango: "b", predictedScore: 2 },
  ]);
});
test("does not enable a disabled policy with an override", () => {
  expect(
    adjustShadowProphetScores({
      ...INPUT,
      decision: { enabled: false, weight: 0.5 },
      config: { enabled: "nar", weight: "1" },
    }),
  ).toStrictEqual([
    { kettoTorokuBango: "a", predictedScore: 0 },
    { kettoTorokuBango: "b", predictedScore: 2 },
  ]);
});
test.each(["bad", "Infinity", "0", "-1", "1.1"])("rejects invalid weight %s", (weight) => {
  expect(adjustShadowProphetScores({ ...INPUT, config: { weight } })).toStrictEqual([
    { kettoTorokuBango: "a", predictedScore: 0 },
    { kettoTorokuBango: "b", predictedScore: 2 },
  ]);
});
test("empty input is a no-op", () => {
  expect(adjustShadowProphetScores({ ...INPUT, predictions: [], entries: [] })).toStrictEqual([]);
});
test("entry count mismatch is a no-op", () => {
  expect(adjustShadowProphetScores({ ...INPUT, entries: [] })).toStrictEqual([
    { kettoTorokuBango: "a", predictedScore: 0 },
    { kettoTorokuBango: "b", predictedScore: 2 },
  ]);
});
test.each([null, true, "", " ", "NaN"])("invalid trend %j is not coerced to zero", (trend) => {
  expect(
    adjustShadowProphetScores({
      ...INPUT,
      entries: [INPUT.entries[0]!, { ...INPUT.entries[1], prophet_entity_performance_mean: trend }],
    }),
  ).toStrictEqual([
    { kettoTorokuBango: "a", predictedScore: 0 },
    { kettoTorokuBango: "b", predictedScore: 2 },
  ]);
});
test.each([
  {},
  { ketto_toroku_bango: "", prophet_entity_coverage: 1, prophet_entity_performance_mean: 2 },
  { ketto_toroku_bango: {}, prophet_entity_coverage: 1, prophet_entity_performance_mean: 2 },
  { ketto_toroku_bango: "b", prophet_entity_coverage: 0, prophet_entity_performance_mean: 2 },
])("rejects invalid coverage/identity %j", (entry) => {
  expect(
    adjustShadowProphetScores({ ...INPUT, entries: [INPUT.entries[0]!, entry] }),
  ).toStrictEqual([
    { kettoTorokuBango: "a", predictedScore: 0 },
    { kettoTorokuBango: "b", predictedScore: 2 },
  ]);
});
test("numeric identity and missing runner trends use the race mean", () => {
  expect(
    adjustShadowProphetScores({
      ...INPUT,
      entries: [...INPUT.entries, { ketto_toroku_bango: 3, prophet_entity_coverage: 0 }],
      predictions: [...INPUT.predictions, { kettoTorokuBango: "3", predictedScore: 1 }],
    }).map((prediction) =>
      prediction.kettoTorokuBango === "3" ? prediction.predictedScore : null,
    ),
  ).toStrictEqual([null, null, 1]);
});
test("accepts eligible numeric identity", () => {
  expect(
    adjustShadowProphetScores({
      ...INPUT,
      entries: [INPUT.entries[0]!, { ...INPUT.entries[1], ketto_toroku_bango: 2 }],
      predictions: [INPUT.predictions[0]!, { kettoTorokuBango: "2", predictedScore: 2 }],
    }),
  ).toStrictEqual([
    { kettoTorokuBango: "a", predictedScore: -0.5 },
    { kettoTorokuBango: "2", predictedScore: 2.5 },
  ]);
});
test("invalid score is a no-op", () => {
  expect(
    adjustShadowProphetScores({
      ...INPUT,
      predictions: [{ kettoTorokuBango: "a", predictedScore: Infinity }, INPUT.predictions[1]!],
    }),
  ).toStrictEqual([
    { kettoTorokuBango: "a", predictedScore: Infinity },
    { kettoTorokuBango: "b", predictedScore: 2 },
  ]);
});
test("degenerate score spread is a no-op", () => {
  expect(
    adjustShadowProphetScores({
      ...INPUT,
      predictions: [{ kettoTorokuBango: "a", predictedScore: 2 }, INPUT.predictions[1]!],
    }),
  ).toStrictEqual([
    { kettoTorokuBango: "a", predictedScore: 2 },
    { kettoTorokuBango: "b", predictedScore: 2 },
  ]);
});
test("degenerate trend spread is a no-op", () => {
  expect(
    adjustShadowProphetScores({
      ...INPUT,
      entries: [INPUT.entries[0]!, { ...INPUT.entries[1], prophet_entity_performance_mean: 0 }],
    }),
  ).toStrictEqual([
    { kettoTorokuBango: "a", predictedScore: 0 },
    { kettoTorokuBango: "b", predictedScore: 2 },
  ]);
});
