// Run with bun. Pure injected canonical SQL fixtures; no production access.
import { expect, test, vi } from "vitest";
import {
  arcQualifiedDependencyTuple,
  buildArcQualifiedQueries,
  loadArcQualifiedPeople,
} from "./arc-qualified-people";
import type { ArcFilters, ArcQualifiedResult } from "./arc-qualified-people";
const filters: ArcFilters = {
  source: "jra",
  date: "20261004",
  keibajoCode: "A8",
  raceBango: "05",
  event: "arc",
};
const config: { R2_SQL_NAMESPACE: string } = { R2_SQL_NAMESPACE: "pc_keiba" };
const target: Record<string, unknown> = {
  source: "netkeiba",
  race_source: "jra",
  kaisai_nen: "2026",
  kaisai_tsukihi: "1004",
  keibajo_code: "A8",
  race_bango: "05",
  umaban: "01",
  source_horse_id: "2020100001",
  source_jockey_id: "a1234",
  source_trainer_id: "01234",
  source_owner_id: "a12345",
  gate_number: 7,
};
const history: Record<string, unknown> = {
  source: "netkeiba",
  person_kind: "jockey",
  source_person_id: "a1234",
  source_race_id: "raw-race",
  source_horse_id: "2020100001",
  race_date: "2025-10-05",
  race_name: "凱旋門賞(GI)",
  venue: "パリロンシャン",
  surface: "芝",
  finish_position: 2,
  finish_position_text: "2",
};
const mapping: Record<string, unknown> = {
  ...target,
  kaisai_nen: "2025",
  kaisai_tsukihi: "1005",
  umaban: "12",
  gate_number: 7,
};
const load = (input: {
  current?: Record<string, unknown>[];
  raw?: Record<string, unknown>[];
  mappings?: Record<string, unknown>[];
}): Promise<ArcQualifiedResult> =>
  loadArcQualifiedPeople({
    config,
    filters,
    execute: vi
      .fn()
      .mockResolvedValueOnce(input.current ?? [target])
      .mockResolvedValueOnce(input.raw ?? [history])
      .mockResolvedValueOnce(input.mappings ?? [mapping]),
  });

test("keeps fullwidth-padded stored Arc titles in the source-qualified cohort", async () => {
  const result: ArcQualifiedResult = await load({
    raw: [{ ...history, race_name: "凱旋門賞　　" }],
  });
  expect(result.rows.find((r) => r.kind === "jockeyFrame10Y")).toMatchObject({
    starts: 1,
    top2: 1,
  });
});

test("counts one event/horse across role duplicates; repeated horse in previous Arc remains eligible", async () => {
  const result: ArcQualifiedResult = await load({
    raw: [
      history,
      history,
      { ...history, person_kind: "trainer", source_person_id: "01234" },
      { ...history, person_kind: "owner", source_person_id: "a12345" },
      { ...history, race_date: "2024-10-06", finish_position: 1, finish_position_text: "1" },
    ],
  });
  expect(result.rows.find((r) => r.kind === "jockey")).toMatchObject({
    starts: 2,
    wins: 1,
    top2: 2,
    top3: 2,
    status: "observed",
    scope: "arc-series",
  });
  expect(result.rows.find((r) => r.kind === "triple30Y")).toMatchObject({ starts: 1, wins: 0 });
  expect(result.rows.find((r) => r.kind === "horseJockey30Y")).toMatchObject({
    starts: 2,
    since: "1996-10-04",
  });
  expect(result.rows.find((r) => r.kind === "horse10Y")).toMatchObject({ starts: 2 });
  expect(result.rows.find((r) => r.kind === "jockeyFrame10Y")).toMatchObject({
    starts: 1,
    gate: 7,
    unverifiedGateKeys: ["2024-10-06:2020100001"],
  });
  expect(result.evidence).toHaveLength(2);
});

test("does not pair people riding/training different horses", async () => {
  const result: ArcQualifiedResult = await load({
    raw: [
      history,
      {
        ...history,
        source_horse_id: "2020100002",
        person_kind: "trainer",
        source_person_id: "01234",
      },
      {
        ...history,
        source_horse_id: "2020100003",
        person_kind: "owner",
        source_person_id: "a12345",
      },
    ],
  });
  expect(result.rows.find((r) => r.kind === "jockeyTrainerVenue10Y")).toMatchObject({
    starts: 0,
    status: "unobserved",
  });
  expect(result.rows.find((r) => r.kind === "triple30Y")).toMatchObject({
    starts: 0,
    status: "unobserved",
  });
  expect(result.rows.find((r) => r.kind === "ownerVenue30Y")).toMatchObject({ starts: 1, wins: 0 });
});

test("DNF and DSQ count as nonwinning starts; withdrawal and unknown are separately audited", async () => {
  const result: ArcQualifiedResult = await load({
    raw: [
      { ...history, finish_position: null, finish_position_text: "中" },
      { ...history, source_horse_id: "2020100002", finish_position: 0, finish_position_text: "失" },
      {
        ...history,
        source_horse_id: "2020100003",
        finish_position: null,
        finish_position_text: "取",
      },
      { ...history, source_horse_id: "2020100004", finish_position: -1, finish_position_text: "?" },
    ],
  });
  expect(result.rows[0]).toMatchObject({
    starts: 2,
    wins: 0,
    top2: 0,
    top3: 0,
    status: "observed",
    unknownKeys: ["2025-10-05:2020100004"],
    withdrawnKeys: ["2025-10-05:2020100003"],
  });
});

test("rolling boundaries are inclusive lower and exclusive target; venue cohorts exclude Chantilly", async () => {
  const result: ArcQualifiedResult = await load({
    raw: [
      { ...history, race_date: "1996-10-03" },
      { ...history, race_date: "1996-10-04" },
      { ...history, race_date: "2016-10-02", venue: "シャンティイ" },
      { ...history, race_date: "2016-10-04", venue: "シャンティイ" },
      { ...history, race_date: "2023-10-03" },
      { ...history, race_date: "2023-10-04" },
      { ...history, race_date: "2026-10-04" },
      { ...history, race_date: "2026-10-05" },
      { ...history, race_name: "別レース" },
      { ...history, source: "other" },
      { ...history, person_kind: "breeder" },
      { ...history, race_date: "2025-10-05", surface: "ダート" },
    ],
  });
  expect(result.rows.find((r) => r.kind === "horseJockey30Y")).toMatchObject({ starts: 6 });
  expect(result.rows.find((r) => r.kind === "jockey")).toMatchObject({
    starts: 4,
    since: "2016-10-04",
  });
  expect(result.rows.find((r) => r.kind === "jockeyVenue3Y")).toMatchObject({
    starts: 1,
    since: "2023-10-04",
    before: "2026-10-04",
    scope: "arc-parislongchamp-turf",
  });
});

test("strict source IDs, NULL owner13, and missing identities never create zero-rate cohorts", async () => {
  const result: ArcQualifiedResult = await load({
    current: [
      {
        ...target,
        umaban: "13",
        source_owner_id: null,
        source_jockey_id: "00000",
        source_trainer_id: "123456",
        source_horse_id: "0000000000",
        gate_number: 0,
      },
    ],
    raw: [history, { ...history, source_horse_id: "bad" }, { ...history, source_horse_id: null }],
  });
  expect(result.rows.every((r) => r.status === "missing-identity")).toBe(true);
  expect(result.coverage).toStrictEqual({ complete: false, rawRows: 3, invalidHorseRows: 2 });
  expect((await load({ current: [{ ...target, source: "jv" }], raw: [] })).rows).toStrictEqual([]);
});

test("four targets sharing a trainer each receive the same deduplicated cohort", async () => {
  const result: ArcQualifiedResult = await load({
    current: [
      target,
      target,
      { ...target, umaban: "02", source_horse_id: "2020100002" },
      { ...target, umaban: "03", source_horse_id: "2020100003" },
      { ...target, umaban: "04", source_horse_id: "2020100004" },
    ],
    raw: [
      { ...history, person_kind: "trainer", source_person_id: "01234", finish_position: 3 },
      { ...history, person_kind: "trainer", source_person_id: "01234", finish_position: 3 },
    ],
  });
  expect(
    result.rows
      .filter((r) => r.kind === "trainer")
      .map((r) => [r.umaban, r.starts, r.wins, r.top3]),
  ).toStrictEqual([
    ["01", 1, 0, 1],
    ["02", 1, 0, 1],
    ["03", 1, 0, 1],
    ["04", 1, 0, 1],
  ]);
});

test.each([
  { source_person_id: "b1234" },
  { finish_position: 1 },
  { venue: "シャンティイ" },
  { surface: "ダート" },
  { finish_position: null, finish_position_text: "取" },
])("rejects conflicts from all people, even non-target IDs: %j", async (change) => {
  await expect(load({ raw: [history, { ...history, ...change }] })).rejects.toThrow(
    "Conflicting Arc evidence",
  );
});

test.each(["中", "失", "取"])("rejects positive rank with status %s", async (status) => {
  await expect(load({ raw: [{ ...history, finish_position_text: status }] })).rejects.toThrow(
    "Conflicting Arc outcome",
  );
});

test("invalid person IDs do not identify anyone; no observed start stays unavailable", async () => {
  const result: ArcQualifiedResult = await load({
    raw: [
      { ...history, source_person_id: "00000", finish_position: 0 },
      { ...history, source_person_id: null, finish_position: 0 },
    ],
  });
  expect(result.rows[0]).toMatchObject({ starts: 0, status: "unobserved" });
  expect(result.rows.find((r) => r.kind === "horse10Y")).toMatchObject({
    starts: 0,
    status: "unobserved",
    unknownKeys: ["2025-10-05:2020100001"],
  });
});

test("gate differs from horse number; missing, conflicting and mismatched gates do not match", async () => {
  expect(
    (await load({ mappings: [{ ...mapping, gate_number: 12 }] })).rows.find(
      (r) => r.kind === "jockeyFrame10Y",
    ),
  ).toMatchObject({ starts: 0 });
  expect(
    (await load({ mappings: [mapping, { ...mapping, gate_number: 8 }] })).rows.find(
      (r) => r.kind === "jockeyFrame10Y",
    ),
  ).toMatchObject({ starts: 0, unverifiedGateKeys: ["2025-10-05:2020100001"] });
  expect(
    (await load({ mappings: [mapping, { ...mapping, umaban: "13" }] })).evidence[0]?.gate,
  ).toBeNull();
  expect(
    (await load({ mappings: [{ ...mapping, gate_number: null }] })).evidence[0]?.gate,
  ).toBeNull();
  expect(
    (
      await load({
        mappings: [
          { ...mapping, source: "jv" },
          { ...mapping, race_source: "nar" },
          { ...mapping, source_horse_id: "0000000000" },
          { ...mapping, kaisai_nen: "1995" },
          target,
        ],
      })
    ).evidence[0]?.gate,
  ).toBeNull();
});

test.each([{ source_jockey_id: "b1234" }, { gate_number: 8 }])(
  "current conflicting assignments reject %j",
  async (change) => {
    await expect(load({ current: [target, { ...target, ...change }] })).rejects.toThrow(
      "Conflicting Arc evidence",
    );
  },
);

test("ambiguous current horse and invalid number reject", async () => {
  await expect(load({ current: [target, { ...target, umaban: "02" }] })).rejects.toThrow(
    "Ambiguous current horse",
  );
  await expect(load({ current: [{ ...target, umaban: "17" }] })).rejects.toThrow(
    "Invalid Arc target number",
  );
});

test.each(["2025-02-30", "invalid", "9999-99-99"])(
  "rejects malformed dates %s",
  async (race_date) => {
    await expect(load({ raw: [{ ...history, race_date }] })).rejects.toThrow("Invalid Arc date");
  },
);

test("bounded sentinel aborts all three inputs; exact 5000 remains accepted", async () => {
  await expect(load({ current: Array(5001).fill(target) })).rejects.toThrow("bounded read");
  await expect(load({ raw: Array(5001).fill(history) })).rejects.toThrow("bounded read");
  await expect(load({ mappings: Array(5001).fill(mapping) })).rejects.toThrow("bounded read");
  expect((await load({ raw: Array(5000).fill(history) })).rows[0]?.starts).toBe(1);
});

test("queries bound exact Arc dates, every person's records, and verified event mapping", () => {
  const queries: string[] = buildArcQualifiedQueries({ config, filters });
  expect(queries).toHaveLength(3);
  expect(queries.every((sql) => sql.endsWith("LIMIT 5001"))).toBe(true);
  expect(queries[1]).toMatch(/person_kind IN \('jockey','trainer','owner'\)/u);
  expect(queries[1]).toMatch(
    "btrim(replace(race_name, chr(12288), '')) IN ('凱旋門賞', '凱旋門賞(GI)')",
  );
  expect(queries[2]).toMatch(
    "btrim(replace(r.kyosomei_hondai, chr(12288), '')) IN ('凱旋門賞', '凱旋門賞(GI)')",
  );
  expect(queries[1]).not.toMatch(/JOIN|finish_position >|source_person_id =/u);
  expect(queries[2]).toMatch(/EXISTS.*jvd_ra.*r.race_bango = m.race_bango/u);
  expect(() =>
    buildArcQualifiedQueries({ config: { R2_SQL_NAMESPACE: "bad;sql" }, filters }),
  ).toThrow("exact Arc");
  expect(() =>
    buildArcQualifiedQueries({ config, filters: { ...filters, date: "20251005" } }),
  ).toThrow("exact Arc");
});

test("dependency tuples are stable under row/property ordering and identical duplicates", async () => {
  expect(
    arcQualifiedDependencyTuple([
      [
        { a: 1, b: 2 },
        { b: 2, a: 1 },
      ],
    ]),
  ).toStrictEqual(arcQualifiedDependencyTuple([[{ b: 2, a: 1 }]]));
  const first: ArcQualifiedResult = await load({
    raw: [history, { ...history, person_kind: "trainer", source_person_id: "01234" }],
  });
  const second: ArcQualifiedResult = await load({
    raw: [{ ...history, person_kind: "trainer", source_person_id: "01234" }, history],
  });
  expect(first.dependencyTuple).toStrictEqual(second.dependencyTuple);
  expect(first.dependencyTuple).not.toStrictEqual((await load({ mappings: [] })).dependencyTuple);
});

test("one historical runner slot cannot prove two source horses", async () => {
  const result: ArcQualifiedResult = await load({
    mappings: [mapping, { ...mapping, source_horse_id: "2020100002" }],
  });
  expect(result.evidence[0]?.gate).toBeNull();
  expect(result.rows.find((r) => r.kind === "jockeyFrame10Y")).toMatchObject({
    status: "unobserved",
    unverifiedGateKeys: ["2025-10-05:2020100001"],
  });
});

test("non-target people also have conflicting assignments checked", async () => {
  await expect(
    load({
      raw: [
        { ...history, source_person_id: "b1234" },
        { ...history, source_person_id: "c1234" },
      ],
    }),
  ).rejects.toThrow("Conflicting Arc evidence");
});
