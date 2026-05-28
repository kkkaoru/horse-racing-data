// run with: bun run test
import { expect, it } from "vitest";
import { buildRealtimeRaceKey, raceKeyFromRealtimePath } from "./race-key";

it("builds a realtime race key padding the race number to two digits", () => {
  expect(buildRealtimeRaceKey("jra", "2026", "0512", "08", "1")).toBe("jra:2026:0512:08:01");
});

it("preserves the race number when it already has two digits", () => {
  expect(buildRealtimeRaceKey("nar", "2026", "0512", "55", "12")).toBe("nar:2026:0512:55:12");
});

it("parses a jra realtime path into a race key", () => {
  expect(raceKeyFromRealtimePath("/api/jra/races/2026/05/12/08/01/realtime")).toBe(
    "jra:2026:0512:08:01",
  );
});

it("parses a nar realtime path into a race key", () => {
  expect(raceKeyFromRealtimePath("/api/nar/races/2026/05/12/55/12/realtime")).toBe(
    "nar:2026:0512:55:12",
  );
});

it("returns null for a non-matching path", () => {
  expect(raceKeyFromRealtimePath("/api/jra/races/2026/05/12/08/01/result")).toBeNull();
});

it("returns null when the source segment is invalid", () => {
  expect(raceKeyFromRealtimePath("/api/xx/races/2026/05/12/08/01/realtime")).toBeNull();
});
