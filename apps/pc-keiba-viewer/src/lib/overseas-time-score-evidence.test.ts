// Runs with bun. Cache acceptance depends on selected evidence, not roster size alone.
import { expect, it } from "vitest";

import published from "../fixtures/overseas-published.json";
import { hasOverseasTimeScoreEvidence } from "./overseas-time-score-evidence";

const runner = { umaban: "01", sourceHorseId: "000a029d42", kettoTorokuBango: "0000000000" };
const evidence = {
  generation: "overseas-readers-v1",
  historyHorseId: "000a029d42",
  selectedHistoryCount: 11,
  overseasHistoryCount: 11,
  selection: "overseas",
};
const row = { horseNumber: "1", historyEvidence: evidence };
const payload = { type: "time-score", runners: [runner], rows: [row] };
it("accepts selected provider evidence and explicit no-history evidence", () => {
  expect(hasOverseasTimeScoreEvidence(JSON.stringify(payload))).toBe(true);
  expect(
    hasOverseasTimeScoreEvidence(
      JSON.stringify({
        ...payload,
        rows: [
          {
            ...row,
            historyEvidence: { ...evidence, selectedHistoryCount: 0, overseasHistoryCount: 0 },
          },
        ],
      }),
    ),
  ).toBe(true);
});
it("rejects stale two-of-sixteen rows as well as sixteen fabricated neutral rows", () => {
  const runners = published.mappings.map((mapping) => ({
    umaban: mapping.umaban,
    sourceHorseId: mapping.source_horse_id,
    kettoTorokuBango: "0000000000",
  }));
  expect(
    hasOverseasTimeScoreEvidence(
      JSON.stringify({
        type: "time-score",
        runners,
        rows: [{ horseNumber: "4" }, { horseNumber: "5" }],
      }),
    ),
  ).toBe(false);
  expect(
    hasOverseasTimeScoreEvidence(
      JSON.stringify({
        type: "time-score",
        runners,
        rows: runners.map((entry) => ({ horseNumber: String(Number(entry.umaban)), score: 0.5 })),
      }),
    ),
  ).toBe(false);
});
it("accepts domestic preference but keeps missing mappings uncacheable", () => {
  expect(
    hasOverseasTimeScoreEvidence(
      JSON.stringify({
        ...payload,
        rows: [
          {
            ...row,
            historyEvidence: {
              ...evidence,
              selectedHistoryCount: 1,
              selection: "domestic-preferred",
            },
          },
        ],
      }),
    ),
  ).toBe(true);
  expect(
    hasOverseasTimeScoreEvidence(
      JSON.stringify({ ...payload, runners: [{ ...runner, sourceHorseId: null }], rows: [] }),
    ),
  ).toBe(false);
  expect(
    hasOverseasTimeScoreEvidence(
      JSON.stringify({
        ...payload,
        runners: [{ ...runner, sourceHorseId: "2021105369", kettoTorokuBango: "2021105369" }],
        rows: [
          {
            ...row,
            historyEvidence: {
              ...evidence,
              historyHorseId: "2021105369",
              selection: "domestic-preferred",
            },
          },
        ],
      }),
    ),
  ).toBe(true);
});
it.each([
  null,
  {},
  { ...payload, type: "similar" },
  { ...payload, runners: null },
  { ...payload, rows: null },
  { ...payload, rows: [null] },
  { ...payload, rows: [{}] },
  { ...payload, rows: [row, row] },
  { ...payload, runners: [null] },
  { ...payload, runners: [{}] },
  { ...payload, runners: [{ ...runner, umaban: "0" }] },
  { ...payload, runners: [{ umaban: "01", kettoTorokuBango: "0000000000" }] },
  { ...payload, runners: [runner, runner] },
  { ...payload, rows: [{ ...row, historyEvidence: { ...evidence, generation: "legacy" } }] },
  {
    ...payload,
    rows: [{ ...row, historyEvidence: { ...evidence, historyHorseId: "000a02cbc8" } }],
  },
  { ...payload, rows: [{ ...row, historyEvidence: { ...evidence, selectedHistoryCount: -1 } }] },
  { ...payload, rows: [{ ...row, historyEvidence: { ...evidence, overseasHistoryCount: 0.5 } }] },
  { ...payload, rows: [{ ...row, historyEvidence: { ...evidence, selection: "unknown" } }] },
  { ...payload, rows: [{ ...row, historyEvidence: { ...evidence, selectedHistoryCount: 0 } }] },
  { ...payload, runners: [] },
])("rejects malformed or inconsistent evidence %j", (value) => {
  expect(hasOverseasTimeScoreEvidence(JSON.stringify(value))).toBe(false);
});
it("rejects unreadable JSON", () => {
  expect(hasOverseasTimeScoreEvidence("{")).toBe(false);
});

it("rejects a newly computed partial roster even with the current evidence generation", () => {
  const runners = published.mappings.map((mapping) => ({
    umaban: mapping.umaban,
    sourceHorseId: ["04", "05"].includes(mapping.umaban) ? mapping.source_horse_id : null,
  }));
  const rows = published.mappings
    .filter((mapping) => ["04", "05"].includes(mapping.umaban))
    .map((mapping) => ({
      horseNumber: String(Number(mapping.umaban)),
      historyEvidence: {
        ...evidence,
        historyHorseId: mapping.source_horse_id,
        selectedHistoryCount: 1,
        selection: "domestic-preferred",
      },
    }));
  expect(hasOverseasTimeScoreEvidence(JSON.stringify({ type: "time-score", runners, rows }))).toBe(
    false,
  );
});
