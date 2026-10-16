// Run with bun (bunx vitest).
import { expect, test, vi } from "vitest";
import {
  buildForeignOwnerQuery,
  foreignOwnerEligible,
  loadForeignOwnerStats,
  normaliseForeignOwnerRows,
} from "./foreign-owner-reader";
import { normaliseSimilarRow } from "./win-rate-heatmap-stats";
import { heatmapStatsDescriptor, kvKeyFor } from "./cache";
import type { R2SqlCatalogConfig, WinRateHeatmapStatsFilters } from "./types";
const config: R2SqlCatalogConfig = {
  R2_SQL_NAMESPACE: "pc_keiba",
  R2_SQL_ACCOUNT_ID: "a",
  R2_SQL_BUCKET_NAME: "b",
  R2_SQL_TOKEN: "t",
};
const filters: WinRateHeatmapStatsFilters = {
  source: "jra",
  date: "20261004",
  keibajoCode: "A8",
  raceBango: "05",
  years: 10,
  includeForeignSourceStats: true,
  includeOwner: true,
  includeVenue: false,
  includeDistance: false,
  includeSurface: false,
  includeTurn: false,
};
const row = {
  source_person_id: "a002e7",
  umaban: "01",
  kind: "owner",
  starts: 3,
  wins: 0,
  places: 1,
  shows: 2,
  conflicts: 0,
};
test("exact identity, raw cohorts, canonical dedup and bounded SQL", () => {
  const sql: string = buildForeignOwnerQuery(config);
  expect(sql).toMatch(/oversea_person_race_history/);
  expect(sql).toMatch("count(c.source_person_id) AS starts");
  expect(sql).not.toMatch("count(*) AS starts");
  expect(sql).toMatch(/race_source = 'jra'/);
  expect(sql).toMatch(/kaisai_nen = '2026' AND kaisai_tsukihi = '1004'/);
  expect(sql).toMatch(/keibajo_code = 'A8' AND race_bango = '05'/);
  expect(sql).toMatch(/2016-10-04/);
  expect(sql).toMatch(/1996-10-04/);
  expect(sql).toMatch(/venue = 'パリロンシャン' AND surface = '芝'/);
  expect(sql).toMatch(/count\(DISTINCT h.finish_position\)/);
  expect(sql).toMatch(/AND h.finish_position > 0/);
  expect(sql).toMatch(/count\(DISTINCT coalesce\(h.venue, ''\)\)/);
  expect(sql).toMatch(/count\(DISTINCT coalesce\(h.surface, ''\)\)/);
  expect(sql).toMatch(/LIMIT 49$/);
  expect(sql).not.toMatch(/snapshot/);
});
test.each([
  { includeForeignSourceStats: false },
  { source: "nar" },
  { date: "20261003" },
  { keibajoCode: "05" },
  { raceBango: "06" },
  { years: 9 },
  { includeVenue: true },
  { includeDistance: true },
  { includeSurface: true },
  { includeTurn: true },
  { includeAge: true },
  { includeClass: true },
  { includeConditionKey: true },
  { includeGrade: true },
  { includeTrackCode: true },
  { includeRaceTitle: true },
] satisfies Partial<WinRateHeatmapStatsFilters>[])(
  "unsupported settings skip query %j",
  async (patch) => {
    const execute = vi.fn();
    expect(foreignOwnerEligible({ ...filters, ...patch })).toBe(false);
    expect(
      await loadForeignOwnerStats({
        config,
        filters: { ...filters, ...patch },
        nativeRows: [],
        execute,
      }),
    ).toStrictEqual({ similarRows: [], foreignOwnerVenueRows: [] });
    expect(execute).not.toHaveBeenCalled();
  },
);
test("zero wins, duplicates, absent history and metadata", () => {
  const result = normaliseForeignOwnerRows([row, row, { ...row, kind: "ownerVenue" }]);
  expect(result.similarRows).toHaveLength(1);
  expect(result.similarRows[0]?.wins).toBe(0);
  expect(result.foreignOwnerVenueRows[0]?.statsScope).toBe("stored-results-venue-30y");
  expect(normaliseSimilarRow({ ...result.similarRows[0] }).statsSource).toBe("netkeiba");
  expect(normaliseForeignOwnerRows([])).toStrictEqual({
    similarRows: [],
    foreignOwnerVenueRows: [],
  });
});
test.each([
  { starts: null },
  { starts: "" },
  { starts: " " },
  { starts: -1 },
  { starts: 1.1 },
  { starts: Infinity },
  { starts: 0 },
  { wins: 2 },
  { places: 3 },
  { shows: 4 },
  { conflicts: 1 },
  { umaban: 0 },
  { umaban: 17 },
  { source_person_id: null },
  { source_person_id: "unknown" },
  { kind: "trainer" },
])("reject malformed aggregate %j", (patch) => {
  expect(() => normaliseForeignOwnerRows([{ ...row, ...patch }])).toThrow();
});
test("reject conflicting aggregate and excessive results", () => {
  expect(() => normaliseForeignOwnerRows([row, { ...row, wins: 1 }])).toThrow(/Conflicting/);
  expect(() => normaliseForeignOwnerRows(Array.from({ length: 49 }, () => row))).toThrow(/bound/);
});
test("native known owners survive, owner branch requires flag, venue remains separate", async () => {
  const native = normaliseSimilarRow({ ...row, name: "Native owner" });
  const execute = vi.fn().mockResolvedValue([row, { ...row, kind: "ownerVenue" }]);
  expect(
    (await loadForeignOwnerStats({ config, filters, nativeRows: [native], execute })).similarRows,
  ).toStrictEqual([]);
  expect(
    (await loadForeignOwnerStats({ config, filters, nativeRows: [], execute })).similarRows,
  ).toHaveLength(1);
  const result = await loadForeignOwnerStats({
    config,
    filters: { ...filters, includeOwner: false },
    nativeRows: [],
    execute,
  });
  expect(result.similarRows).toStrictEqual([]);
  expect(result.foreignOwnerVenueRows).toHaveLength(1);
  expect(kvKeyFor(heatmapStatsDescriptor(filters))).not.toBe(
    kvKeyFor(heatmapStatsDescriptor({ ...filters, includeForeignSourceStats: false })),
  );
});

test("actual default selects race scope and preserves its contract", async () => {
  const execute = vi
    .fn()
    .mockResolvedValue([
      row,
      { ...row, kind: "ownerRace", starts: 2 },
      { ...row, kind: "ownerVenue" },
    ]);
  const result = await loadForeignOwnerStats({
    config,
    filters: { ...filters, includeVenue: true, includeRaceTitle: true },
    nativeRows: [],
    execute,
  });
  expect(result.similarRows).toHaveLength(1);
  expect(result.similarRows[0]?.statsScope).toBe("stored-results-race-10y");
  expect(result.similarRows[0]?.starts).toBe(2);
  expect(normaliseSimilarRow({ ...result.similarRows[0] }).statsScope).toBe(
    "stored-results-race-10y",
  );
  expect(result.foreignOwnerVenueRows).toHaveLength(1);
  const zero = normaliseSimilarRow({ ...row, name: "Native", starts: 0 });
  expect(
    (await loadForeignOwnerStats({ config, filters, nativeRows: [zero], execute })).similarRows,
  ).toHaveLength(1);
});
test.each([{ umaban: "1" }, { umaban: "001" }, { umaban: 1 }, { source_person_id: "000000" }])(
  "rejects noncanonical identities %j",
  (patch) => {
    expect(() => normaliseForeignOwnerRows([{ ...row, ...patch }])).toThrow();
  },
);
test.each([
  "includeDistance",
  "includeSurface",
  "includeTurn",
  "includeAge",
  "includeClass",
  "includeConditionKey",
  "includeGrade",
  "includeTrackCode",
])("default rejects additional filter %s", (flag) => {
  expect(
    foreignOwnerEligible({ ...filters, includeVenue: true, includeRaceTitle: true, [flag]: true }),
  ).toBe(false);
});

test("restricts all three stored-result branches to observed Arc names", () => {
  expect(
    buildForeignOwnerQuery(config).match(/race_name IN \('凱旋門賞', '凱旋門賞\(GI\)'\)/gu)?.length,
  ).toBe(3);
});
