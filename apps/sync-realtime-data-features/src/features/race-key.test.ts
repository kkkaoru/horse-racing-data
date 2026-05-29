// Run with: bun run --filter sync-realtime-data-features test
import { expect, it } from "vitest";

import { parseRaceKey, tryParseRaceKey } from "./race-key";

it("parses a valid nar race_key", () => {
  expect(parseRaceKey("nar:2026:0529:42:01")).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "42",
    raceBango: "01",
    raceKey: "nar:2026:0529:42:01",
    source: "nar",
  });
});

it("parses a valid jra race_key", () => {
  expect(parseRaceKey("jra:2026:0529:08:11")).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "08",
    raceBango: "11",
    raceKey: "jra:2026:0529:08:11",
    source: "jra",
  });
});

it("tryParseRaceKey returns null when part count is wrong", () => {
  expect(tryParseRaceKey("nar:20260529:30:08")).toBe(null);
});

it("tryParseRaceKey returns null when source is unknown", () => {
  expect(tryParseRaceKey("ban-ei:2026:0529:83:01")).toBe(null);
});

it("parseRaceKey throws on invalid input", () => {
  expect(() => parseRaceKey("garbage")).toThrowError(
    "raceKey must match {source}:{kaisaiNen}:{kaisaiTsukihi}:{keibajoCode}:{raceBango}: garbage",
  );
});
