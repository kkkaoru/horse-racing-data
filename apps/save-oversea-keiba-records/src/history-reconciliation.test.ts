// This file runs with Bun. Reconciliation is pure and has no network or filesystem access.
import { expect, it } from "vitest";
import { verifyHistoryRowsIncluded } from "./history-reconciliation";
import type { SecondaryHorseResult } from "./sources/secondary-result-parser";

const row: SecondaryHorseResult = {
  sourceHorseId: "horse1",
  sourceRaceId: "race1",
  raceDate: "2026-09-01",
  venue: "Venue",
  raceDaySequence: 1,
  raceName: "Race",
  sourceRaceUrl: "https://example.test/race/race1",
  finishPosition: 2,
  finishPositionText: "2",
  jockeyName: "Jockey",
  sourceJockeyId: "jockey1",
  surface: "Turf",
  distanceMetres: 1600,
  going: "Good",
};
it("matches every occurrence irrespective of order and leaves inputs unchanged", () => {
  const original: readonly SecondaryHorseResult[] = Object.freeze([row, row]);
  const current: readonly SecondaryHorseResult[] = Object.freeze([
    { ...row, sourceRaceId: "race2" },
    row,
    row,
  ]);
  expect(() => verifyHistoryRowsIncluded(original, current)).not.toThrow();
  expect(original).toHaveLength(2);
  expect(current).toHaveLength(3);
});
it("rejects losing one duplicate occurrence", () => {
  expect(() => verifyHistoryRowsIncluded([row, row], [row])).toThrow(
    "duplicate occurrence is absent",
  );
});
it("rejects missing rows and changed fields", () => {
  expect(() => verifyHistoryRowsIncluded([row], [])).toThrow("source row");
  expect(() => verifyHistoryRowsIncluded([row], [{ ...row, venue: "Changed" }])).toThrow(
    "source row",
  );
});
it("allows empty original evidence", () => {
  expect(() => verifyHistoryRowsIncluded([], [row])).not.toThrow();
});
