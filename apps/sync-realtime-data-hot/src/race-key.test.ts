// run with: bun run test
import { expect, it } from "vitest";
import {
  buildRealtimeRaceKey,
  extractYyyymmddFromRaceKey,
  raceKeyFromRealtimePath,
} from "./race-key";

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

it("extractYyyymmddFromRaceKey returns 20260529 for a valid jra raceKey", () => {
  expect(extractYyyymmddFromRaceKey("jra:2026:0529:08:01")).toBe("20260529");
});

it("extractYyyymmddFromRaceKey returns 20260529 for a valid nar raceKey", () => {
  expect(extractYyyymmddFromRaceKey("nar:2026:0529:47:01")).toBe("20260529");
});

it("extractYyyymmddFromRaceKey returns null for a 4-segment raceKey", () => {
  expect(extractYyyymmddFromRaceKey("nar:20260529:47:01")).toBeNull();
});

it("extractYyyymmddFromRaceKey returns null for a 6-segment raceKey", () => {
  expect(extractYyyymmddFromRaceKey("nar:2026:0529:47:01:99")).toBeNull();
});

it("extractYyyymmddFromRaceKey returns null when year is non-numeric", () => {
  expect(extractYyyymmddFromRaceKey("nar:20XY:0529:47:01")).toBeNull();
});

it("extractYyyymmddFromRaceKey returns null when monthDay is non-numeric", () => {
  expect(extractYyyymmddFromRaceKey("nar:2026:05AB:47:01")).toBeNull();
});

it("extractYyyymmddFromRaceKey returns null for empty string", () => {
  expect(extractYyyymmddFromRaceKey("")).toBeNull();
});

it("extractYyyymmddFromRaceKey returns null when source is not jra or nar", () => {
  expect(extractYyyymmddFromRaceKey("xxx:2026:0529:47:01")).toBeNull();
});
