// Runs with bun. Saved publication evidence; no provider I/O.
import { expect, it } from "vitest";
import fixture from "./fixtures/overseas-published.json";
import {
  buildOverseasRunnerMappingSql,
  isOverseasRunnerRace,
  parseOverseasRunnerMappings,
} from "./overseas-runner-mapping";

it("retains all sixteen published provider IDs and official gates", () => {
  const rows = parseOverseasRunnerMappings(fixture.mappings);
  expect(rows.map((row) => row.sourceHorseId)).toStrictEqual([
    "000a029d42",
    "000a02cbc8",
    "000a02d860",
    "2021105369",
    "2021103272",
    "000a02bb45",
    "000a024a34",
    "000a029d4f",
    "000a024a0d",
    "000a024841",
    "000a02d8c0",
    "000a02caba",
    "000a02daf6",
    "000a02d68d",
    "000a02d40c",
    "000a02ca5a",
  ]);
  expect(rows.map((row) => row.gateNumber)).toStrictEqual([
    5, 9, 2, 12, 13, 14, 15, 10, 6, 7, 4, 1, 8, 11, 3, 16,
  ]);
});
it("keeps a genuinely absent mapping or gate unknown", () => {
  expect(parseOverseasRunnerMappings([])).toStrictEqual([]);
  expect(
    parseOverseasRunnerMappings([
      { umaban: "01", source_horse_id: "000a029d42", gate_number: null },
    ]),
  ).toStrictEqual([{ umaban: "01", sourceHorseId: "000a029d42", gateNumber: null }]);
});
it.each([
  null,
  [],
  {},
  { umaban: "1" },
  { umaban: "01", source_horse_id: "0000000000", gate_number: 1 },
  { umaban: "01", source_horse_id: "x' OR true", gate_number: 1 },
  { umaban: "01", source_horse_id: "000a029d42", gate_number: "12" },
  { umaban: "01", source_horse_id: "000a029d42", gate_number: 0 },
  { umaban: "01", source_horse_id: "000a029d42", gate_number: 1.5 },
  { umaban: "01", source_horse_id: "000a029d42", gate_number: 32768 },
])("rejects malformed source mapping %j", (row) => {
  expect(() => parseOverseasRunnerMappings([row])).toThrow("Invalid overseas source mapping");
});
it("rejects duplicates and oversized mappings", () => {
  expect(() => parseOverseasRunnerMappings([fixture.mappings[0], fixture.mappings[0]])).toThrow(
    "Invalid overseas source mapping",
  );
  expect(() =>
    parseOverseasRunnerMappings(Array.from({ length: 19 }, () => fixture.mappings[0])),
  ).toThrow("Too many overseas source mappings");
});
it("qualifies arbitrary overseas race keys without touching domestic cards", () => {
  const input = {
    namespace: "pc_keiba",
    source: "jra",
    date: "20270918",
    keibajoCode: "C4",
    raceBango: "03",
  } satisfies Parameters<typeof buildOverseasRunnerMappingSql>[0];
  expect(buildOverseasRunnerMappingSql(input)).toMatch("race_bango = '03'");
  expect(isOverseasRunnerRace(input)).toBe(true);
  expect(isOverseasRunnerRace({ ...input, source: "nar" })).toBe(false);
  expect(isOverseasRunnerRace({ ...input, keibajoCode: "05" })).toBe(false);
});
