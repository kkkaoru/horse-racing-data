// Run with bun.
import { expect, it } from "vitest";

import fixture from "./__fixtures__/arc-qualified-stats.json";
import {
  arcQualifiedCells,
  isArcQualifiedStats,
  requireArcQualifiedStats,
} from "./arc-qualified-stats";

it("accepts the stored source-qualified contract and matches the ten-cell count oracle", () => {
  const stats = requireArcQualifiedStats(fixture);
  expect(stats.rows).toHaveLength(160);
  expect(
    Object.entries(arcQualifiedCells(stats, "1")).map(([key, cell]) => [
      key,
      cell.starts,
      cell.winCount,
      cell.quinellaCount,
      cell.showCount,
    ]),
  ).toStrictEqual([
    ["jockey", 8, 1, 2, 2],
    ["trainer", 5, 1, 2, 2],
    ["owner", 1, 1, 1, 1],
    ["horse", 1, 1, 1, 1],
    ["horseJockey", 1, 1, 1, 1],
    ["jockeyVenue", 2, 1, 1, 1],
    ["jockeyTrainerVenue", 2, 1, 1, 1],
    ["ownerVenue", 1, 1, 1, 1],
    ["jockeyTrainerOwner", 1, 1, 1, 1],
    ["jockeyFrame", null, null, null, null],
  ]);
});
it("distinguishes zero wins from unknown identity and never restores owner 13 from its visible name", () => {
  const stats = requireArcQualifiedStats(fixture);
  expect(arcQualifiedCells(stats, "13").owner).toMatchObject({ starts: null, winRate: null });
  expect(arcQualifiedCells(undefined, "1").horse).toMatchObject({ starts: null, winRate: null });
  expect(arcQualifiedCells(stats, "99").horse).toMatchObject({ starts: null, winRate: null });
  expect(
    arcQualifiedCells({ ...stats, rows: stats.rows.map((row) => ({ ...row, wins: 0 })) }, "1")
      .horse,
  ).toMatchObject({ starts: 1, winCount: 0, winRate: 0 });
  expect(arcQualifiedCells(stats, "1").jockeyVenue?.provenance).toMatch(
    /2023-10-04.*rolling3年.*パリロンシャン/u,
  );
  expect(arcQualifiedCells(stats, "1").jockey?.provenance).toMatch(/2016-10-04.*シリーズ全開催地/u);
});
it.each([
  null,
  {},
  { coverage: { complete: true }, rows: [] },
  { ...fixture, coverage: { complete: false, rawRows: -1, invalidHorseRows: 0 } },
  { ...fixture, coverage: { complete: false, rawRows: 0, invalidHorseRows: 1 } },
  { ...fixture, rows: [fixture.rows[0], fixture.rows[0]] },
])("rejects malformed envelopes %j", (value) => {
  expect(isArcQualifiedStats(value)).toBe(false);
  expect(() => requireArcQualifiedStats(value)).toThrow(
    "Arc qualified statistics are missing or malformed",
  );
});
it.each([
  { source: "jv" },
  { kind: "career" },
  { since: "2016-01-01" },
  { before: "2026-10-05" },
  { scope: "stored-results-10y" },
  { umaban: "17" },
  { umaban: 1 },
  { starts: -1 },
  { wins: 1.5 },
  { wins: 9 },
  { top2: 9 },
  { top3: 9 },
  { starts: Infinity },
  { status: "unobserved" },
  { gate: 1 },
  { people: null },
  { people: { horse: "000a029d42", jockey: "00000", trainer: "05701", owner: "a00762" } },
  { evidenceKeys: [] },
  { evidenceKeys: ["2026-10-04:000a029d42"] },
  { evidenceKeys: ["2015-10-04:000a029d42"] },
  { evidenceKeys: ["2025-02-30:000a029d42"] },
  { evidenceKeys: ["2025-10-04:0000000000"] },
  { evidenceKeys: [1] },
  { evidenceKeys: null },
  { unknownKeys: ["2017-10-01:000a013189"] },
  { unverifiedGateKeys: ["2025-10-04:000a029d42"] },
])("rejects incoherent count/ID/provenance rows %j", (patch) => {
  expect(isArcQualifiedStats({ ...fixture, rows: [{ ...fixture.rows[0], ...patch }] })).toBe(false);
});
it("rejects inconsistent runner identity, duplicate horse assignments and revived owner 13", () => {
  expect(
    isArcQualifiedStats({
      ...fixture,
      rows: [
        fixture.rows[0],
        { ...fixture.rows[1], people: { ...fixture.rows[1]!.people, jockey: "12345" } },
      ],
    }),
  ).toBe(false);
  expect(
    isArcQualifiedStats({
      ...fixture,
      rows: [fixture.rows[0], { ...fixture.rows[0], umaban: "02" }],
    }),
  ).toBe(false);
  expect(isArcQualifiedStats({ ...fixture, rows: [{ ...fixture.rows[0], umaban: "13" }] })).toBe(
    false,
  );
});

it.each([
  { gate: 0 },
  { gate: 25 },
  { gate: 1.5 },
  { gate: null, status: "observed" },
  {
    gate: null,
    starts: 1,
    wins: 0,
    top2: 0,
    top3: 0,
    evidenceKeys: ["2025-10-05:000a029d42"],
    status: "missing-identity",
  },
  {
    gate: 1,
    status: "unobserved",
    unknownKeys: ["2025-10-05:000a029d42", "2025-10-05:000a029d42"],
  },
])("rejects invalid gate/status/exclusion combinations %j", (patch) => {
  expect(isArcQualifiedStats({ ...fixture, rows: [{ ...fixture.rows[9], ...patch }] })).toBe(false);
});

it("accepts missing gate and missing horse as unknown and checks horse evidence identity", () => {
  const stats = requireArcQualifiedStats(fixture);
  const frame = stats.rows[9]!;
  expect(
    isArcQualifiedStats({
      ...stats,
      rows: [{ ...frame, gate: null, status: "missing-identity", unverifiedGateKeys: [] }],
    }),
  ).toBe(true);
  expect(
    isArcQualifiedStats({
      ...stats,
      rows: [
        { ...frame, umaban: "01", people: { ...frame.people, horse: null } },
        { ...frame, umaban: "02", people: { ...frame.people, horse: null } },
      ],
    }),
  ).toBe(true);
  expect(
    isArcQualifiedStats({
      ...stats,
      rows: [{ ...stats.rows[3], evidenceKeys: ["2025-10-05:000a000001"] }],
    }),
  ).toBe(false);
  expect(isArcQualifiedStats({ ...stats, rows: [null] })).toBe(false);
});
