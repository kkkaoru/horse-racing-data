// Run with bun via Vitest. Independent 619 oracle, proposed unpublished profiles.
import { expect, it } from "vitest";
import {
  aggregateArcBloodlines,
  arcBloodlineProfilesQuery,
  type ArcBloodlineInput,
} from "./arc-id-bloodline";
import qualifiedFixture from "./fixtures/arc-id-bloodline/qualified.json";
import profiles from "./fixtures/arc-id-bloodline/profiles.json";
import oracle from "./fixtures/arc-id-bloodline/oracle.json";
import type { ArcQualifiedResult } from "./arc-qualified-people";
const qualified: ArcQualifiedResult = JSON.parse(JSON.stringify(qualifiedFixture));
const input: ArcBloodlineInput = {
  filters: {
    date: "20261004",
    source: "jra",
    keibajoCode: "A8",
    raceBango: "05",
    years: 10,
    includeVenue: true,
    includeSurface: true,
    includeDistance: false,
    includeTurn: false,
  },
  qualified,
  profiles,
  labels: [],
};
it("matches every independent source-ID oracle cell and preserves zero wins", () => {
  const result = aggregateArcBloodlines(input);
  expect(result.rows.length).toBe(112);
  expect(result.knownBloodlineCount).toBe(112);
  expect(result.coverage.eligibleStarts).toBe(124);
  expect(result.coverage.complete).toBe(false);
  expect(result.rows.filter((r) => r.starts > 0 && r.wins === 0).length).toBe(39);
  expect(
    result.rows.map((r) => [
      String(r.umaban).padStart(2, "0"),
      r.category,
      r.starts,
      r.wins,
      r.places,
      r.shows,
    ]),
  ).toStrictEqual(oracle.cells.map((c) => [c.umaban, c.role, c.starts, c.wins, c.top2, c.top3]));
  expect(result.coverage.roles.map((r) => [r.knownAncestry, r.unknownAncestry])).toStrictEqual([
    [124, 0],
    [124, 0],
    [124, 0],
    [124, 0],
    [124, 0],
    [124, 0],
    [124, 0],
  ]);
});
it("uses native labels only for presentation; aliases and homonyms cannot change counts", () => {
  const renamed = profiles.map((p) => ({
    ...p,
    sire_name: "same name",
    sire_sire_name: "same name",
    dam_sire_name: "same name",
  }));
  const result = aggregateArcBloodlines({
    ...input,
    profiles: renamed,
    labels: [{ umaban: 1, category: "sire", name: "native preferred" }],
  });
  expect(result.rows[0]).toMatchObject({ name: "native preferred", starts: 12, wins: 1 });
  expect(result.rows.map((r) => [r.starts, r.wins, r.places, r.shows])).toStrictEqual(
    oracle.cells.map((c) => [c.starts, c.wins, c.top2, c.top3]),
  );
});
it("missing ancestry is unavailable, never an invented zero identity", () => {
  const result = aggregateArcBloodlines({ ...input, profiles: [] });
  expect(result.rows).toStrictEqual([]);
  expect(result.knownBloodlineCount).toBe(0);
  expect(result.coverage.roles.map((r) => r.unknownAncestry)).toStrictEqual([
    124, 124, 124, 124, 124, 124, 124,
  ]);
});
it("deduplicates provider horse date and rejects contradictory outcomes or venues", () => {
  const event = qualified.evidence.find((e) => e.date === "2025-10-05" && e.horse === "000a029d42");
  if (!event) throw new Error("Missing fixture event");
  expect(
    aggregateArcBloodlines({
      ...input,
      qualified: { ...qualified, evidence: [...qualified.evidence, event] },
    }).coverage.eligibleStarts,
  ).toBe(124);
  expect(() =>
    aggregateArcBloodlines({
      ...input,
      qualified: { ...qualified, evidence: [...qualified.evidence, { ...event, outcome: 2 }] },
    }),
  ).toThrow("Conflicting");
  expect(() =>
    aggregateArcBloodlines({
      ...input,
      qualified: { ...qualified, evidence: [...qualified.evidence, { ...event, venue: "France" }] },
    }),
  ).toThrow("Conflicting");
});
it("rejects truncation, conflicting parent IDs, invalid roots and unsafe namespaces", () => {
  expect(() =>
    aggregateArcBloodlines({ ...input, profiles: Array.from({ length: 5001 }, () => ({})) }),
  ).toThrow("bounded");
  const first = profiles[0];
  if (!first) throw new Error("Missing fixture profile");
  expect(() =>
    aggregateArcBloodlines({
      ...input,
      profiles: [...profiles, { ...first, sire_source_id: "000a999999" }],
    }),
  ).toThrow("Conflicting");
  expect(() =>
    aggregateArcBloodlines({
      ...input,
      profiles: [{ source: "netkeiba", source_horse_id: "0000000000" }],
    }),
  ).toThrow("identity");
  expect(() => arcBloodlineProfilesQuery("bad;sql")).toThrow("namespace");
  expect(arcBloodlineProfilesQuery("pc_keiba")).toMatch(
    /WHERE source = 'netkeiba' ORDER BY source, source_horse_id LIMIT 5001$/u,
  );
});
it("keeps providers separate and accepts same-ID aliases", () => {
  const first = profiles[0];
  if (!first) throw new Error("Missing fixture profile");
  expect(
    aggregateArcBloodlines({
      ...input,
      profiles: [
        ...profiles,
        { ...first, sire_name: "alias" },
        { ...first, source: "other", sire_source_id: "000a999999" },
      ],
    }).coverage.eligibleStarts,
  ).toBe(124);
});
it.each([
  "includeDistance",
  "includeTurn",
  "includeGrade",
  "includeTrackCode",
  "includeAge",
  "includeClass",
  "includeConditionKey",
])("rejects unsupported %s", (flag) => {
  expect(() =>
    aggregateArcBloodlines({ ...input, filters: { ...input.filters, [flag]: true } }),
  ).toThrow("Unsupported");
});
it.each([{ years: 9 }, { includeVenue: false }, { includeSurface: false }, { date: "20261005" }])(
  "fails closed outside supported scope %j",
  (change) => {
    expect(() =>
      aggregateArcBloodlines({ ...input, filters: { ...input.filters, ...change } }),
    ).toThrow("Unsupported");
  },
);
it("honors date boundaries, exact venue and surface, DNF, withdrawal, unknown outcome", () => {
  const event = qualified.evidence.find((e) => e.horse === "000a029d42");
  if (!event) throw new Error("Missing fixture event");
  const evidence = [
    { ...event, date: "2016-10-04", outcome: 0, venue: "パリロンシャン", surface: "芝" },
    { ...event, date: "2016-10-03", outcome: 1, venue: "パリロンシャン", surface: "芝" },
    { ...event, date: "2026-10-04", outcome: 1, venue: "パリロンシャン", surface: "芝" },
    { ...event, date: "2020-10-04", outcome: 1, venue: "France", surface: "芝" },
    { ...event, date: "2021-10-04", outcome: 1, venue: "パリロンシャン", surface: "ダート" },
    { ...event, date: "2022-10-04", outcome: "withdrawn", venue: "パリロンシャン", surface: "芝" },
    { ...event, date: "2023-10-04", outcome: "unknown", venue: "パリロンシャン", surface: "芝" },
  ] satisfies ArcQualifiedResult["evidence"];
  const result = aggregateArcBloodlines({ ...input, qualified: { ...qualified, evidence } });
  expect(result.rows[0]).toMatchObject({ starts: 1, wins: 0, places: 0, shows: 0 });
  expect(result.coverage.eligibleStarts).toBe(1);
});
it("does not display zero when historical ancestry is missing", () => {
  const event = qualified.evidence[0];
  if (!event) throw new Error("Missing event");
  const result = aggregateArcBloodlines({
    ...input,
    qualified: {
      ...qualified,
      evidence: [
        {
          ...event,
          horse: "000a999999",
          date: "2025-10-05",
          venue: "パリロンシャン",
          surface: "芝",
          outcome: 1,
        },
      ],
    },
  });
  expect(result.knownBloodlineCount).toBe(112);
  expect(result.rows).toStrictEqual([]);
  expect(result.coverage.roles.map((r) => r.unknownAncestry)).toStrictEqual([1, 1, 1, 1, 1, 1, 1]);
});
it("unknown display label does not replace identity", () => {
  const result = aggregateArcBloodlines({
    ...input,
    profiles: profiles.map((p) => ({ ...p, sire_name: "", sire_sire_name: "", dam_sire_name: "" })),
    labels: [{ umaban: 1, category: "sire", name: "不明" }],
  });
  expect(result.rows[0]).toMatchObject({ name: "不明", starts: 12, wins: 1 });
});
