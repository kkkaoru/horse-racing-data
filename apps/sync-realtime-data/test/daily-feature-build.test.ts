// Run with bun test apps/sync-realtime-data/test/daily-feature-build.test.ts
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import {
  buildDailyFeatureSelectSql,
  fetchDailyRaceEntriesFromPostgres,
  listDailyRaceEntriesForRace,
  shouldSkipDailyFeatureBuild,
  triggerViewerCacheWarmForDate,
  upsertDailyRaceEntriesToD1,
  type DailyRaceEntryRow,
} from "../src/daily-feature-build";
import type { Env } from "../src/types";

const buildRow = (overrides: Partial<DailyRaceEntryRow> = {}): DailyRaceEntryRow => ({
  babajotai_code_dirt: null,
  babajotai_code_shiba: null,
  bamei: "テスト馬",
  banushimei: null,
  barei: 4,
  bataiju: null,
  chokyoshimei_ryakusho: null,
  corner1_norm: null,
  corner2_norm: null,
  corner3_norm: null,
  corner4_norm: null,
  corner_1: null,
  corner_2: null,
  corner_3: null,
  corner_4: null,
  finish_norm: null,
  finish_position: null,
  futan_juryo: 55,
  grade_code: null,
  hasso_jikoku: null,
  juryo_shubetsu_code: null,
  kaisai_nen: "2026",
  kaisai_tsukihi: "0525",
  keibajo_code: "35",
  ketto_toroku_bango: "2023100001",
  kishumei_ryakusho: null,
  kohan_3f: null,
  kyori: 1200,
  kyoso_joken_code: null,
  kyoso_shubetsu_code: null,
  race_bango: "01",
  race_date: "20260525",
  race_name: null,
  seibetsu_code: "1",
  shusso_tosu: 10,
  soha_time: null,
  source: "nar",
  tansho_ninkijun: 1,
  tansho_odds: 2.5,
  time_sa: null,
  track_code: "24",
  umaban: 1,
  wakuban: null,
  zogen_fugo: null,
  zogen_sa: null,
  ...overrides,
});

beforeEach(() => {
  vi.restoreAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

test("buildDailyFeatureSelectSql includes both jra and nar selects for scope=all", () => {
  const sql = buildDailyFeatureSelectSql({ fromDate: "20260525", sourceScope: "all" });
  expect(sql).toMatch(/from jvd_se se/);
  expect(sql).toMatch(/from nvd_se se/);
});

test("buildDailyFeatureSelectSql excludes jra when scope=nar", () => {
  const sql = buildDailyFeatureSelectSql({ fromDate: "20260525", sourceScope: "nar" });
  expect(sql).not.toMatch(/from jvd_se se/);
  expect(sql).toMatch(/from nvd_se se/);
  expect(sql).toMatch(/and ra\.keibajo_code <> '83'/);
});

test("buildDailyFeatureSelectSql filters to ban-ei venue only for scope=ban-ei", () => {
  const sql = buildDailyFeatureSelectSql({ fromDate: "20260525", sourceScope: "ban-ei" });
  expect(sql).toMatch(/and ra\.keibajo_code = '83'/);
});

test("buildDailyFeatureSelectSql excludes nar entirely when scope=jra", () => {
  const sql = buildDailyFeatureSelectSql({ fromDate: "20260525", sourceScope: "jra" });
  expect(sql).toMatch(/from jvd_se se/);
  expect(sql).not.toMatch(/from nvd_se se/);
});

test("buildDailyFeatureSelectSql defaults toDate to fromDate when only fromDate is supplied", () => {
  const sql = buildDailyFeatureSelectSql({ fromDate: "20260525" });
  expect(sql).toMatch(/se\.kaisai_nen \|\| se\.kaisai_tsukihi >= '20260525'/);
  expect(sql).toMatch(/se\.kaisai_nen \|\| se\.kaisai_tsukihi <= '20260525'/);
});

test("buildDailyFeatureSelectSql respects supplied date range", () => {
  const sql = buildDailyFeatureSelectSql({
    fromDate: "20260525",
    sourceScope: "all",
    toDate: "20260526",
  });
  expect(sql).toMatch(/se\.kaisai_nen \|\| se\.kaisai_tsukihi >= '20260525'/);
  expect(sql).toMatch(/se\.kaisai_nen \|\| se\.kaisai_tsukihi <= '20260526'/);
});

test("buildDailyFeatureSelectSql rejects non YYYYMMDD fromDate", () => {
  expect(() => buildDailyFeatureSelectSql({ fromDate: "2026-05-25" })).toThrow(
    "fromDate must match YYYYMMDD: 2026-05-25",
  );
});

test("buildDailyFeatureSelectSql rejects non YYYYMMDD toDate", () => {
  expect(() => buildDailyFeatureSelectSql({ fromDate: "20260525", toDate: "5/25" })).toThrow(
    "toDate must match YYYYMMDD: 5/25",
  );
});

test("upsertDailyRaceEntriesToD1 returns zero and skips D1 when input is empty", async () => {
  const db = { prepare: vi.fn(), batch: vi.fn() } as unknown as D1Database;
  const written = await upsertDailyRaceEntriesToD1(db, []);
  expect(written).toBe(0);
  expect(db.prepare).not.toHaveBeenCalled();
  expect(db.batch).not.toHaveBeenCalled();
});

test("upsertDailyRaceEntriesToD1 batches statements into D1.batch with a single prepare", async () => {
  const bind = vi.fn().mockReturnValue({});
  const prepared = { bind };
  const batch = vi.fn(async () => []);
  const db = {
    prepare: vi.fn(() => prepared),
    batch,
  } as unknown as D1Database;

  const rows = [buildRow(), buildRow({ ketto_toroku_bango: "2023100002", umaban: 2 })];
  const written = await upsertDailyRaceEntriesToD1(db, rows, new Date("2026-05-25T00:00:00.000Z"));

  expect(written).toBe(2);
  expect(db.prepare).toHaveBeenCalledOnce();
  expect(batch).toHaveBeenCalledOnce();
  const firstBindArgs = bind.mock.calls[0];
  expect(firstBindArgs?.[0]).toBe("nar:20260525:35:01");
});

test("upsertDailyRaceEntriesToD1 splits large input into batches of 50", async () => {
  const bind = vi.fn().mockReturnValue({});
  const prepared = { bind };
  const batch = vi.fn(async () => []);
  const db = {
    prepare: vi.fn(() => prepared),
    batch,
  } as unknown as D1Database;

  const rows = Array.from({ length: 120 }, (_, index) =>
    buildRow({
      ketto_toroku_bango: `2023${String(index).padStart(6, "0")}`,
      umaban: index + 1,
    }),
  );
  const written = await upsertDailyRaceEntriesToD1(db, rows);

  expect(written).toBe(120);
  expect(batch).toHaveBeenCalledTimes(3);
});

test("fetchDailyRaceEntriesFromPostgres normalises raw rows into typed rows", async () => {
  const pool = {
    query: vi.fn(async () => ({
      rows: [
        {
          babajotai_code_dirt: null,
          babajotai_code_shiba: null,
          bamei: "サンプル馬",
          banushimei: null,
          barei: 5,
          bataiju: "498",
          chokyoshimei_ryakusho: null,
          corner1_norm: null,
          corner2_norm: null,
          corner3_norm: null,
          corner4_norm: null,
          corner_1: 3,
          corner_2: 2,
          corner_3: 2,
          corner_4: 1,
          finish_norm: null,
          finish_position: null,
          futan_juryo: "55",
          grade_code: null,
          hasso_jikoku: "1430",
          juryo_shubetsu_code: null,
          kaisai_nen: "2026",
          kaisai_tsukihi: "0525",
          keibajo_code: "35",
          ketto_toroku_bango: "2023100001",
          kishumei_ryakusho: null,
          kohan_3f: null,
          kyori: "1200",
          kyoso_joken_code: null,
          kyoso_shubetsu_code: null,
          race_bango: "01",
          race_date: "20260525",
          race_name: "サンプルS",
          seibetsu_code: "1",
          shusso_tosu: "10",
          soha_time: null,
          source: "nar",
          tansho_ninkijun: "1",
          tansho_odds: "2.5",
          time_sa: null,
          track_code: "24",
          umaban: "1",
          wakuban: "03",
          zogen_fugo: "+",
          zogen_sa: 4,
        },
      ],
    })),
  };

  const rows = await fetchDailyRaceEntriesFromPostgres(pool as never, { fromDate: "20260525" });

  expect(rows).toHaveLength(1);
  expect(rows[0]?.bamei).toBe("サンプル馬");
  expect(rows[0]?.kyori).toBe(1200);
  expect(rows[0]?.tansho_odds).toBe(2.5);
  expect(rows[0]?.bataiju).toBe(498);
  expect(rows[0]?.zogen_fugo).toBe("+");
  expect(rows[0]?.zogen_sa).toBe(4);
  expect(rows[0]?.wakuban).toBe("03");
  expect(rows[0]?.race_name).toBe("サンプルS");
  expect(rows[0]?.hasso_jikoku).toBe("1430");
  expect(rows[0]?.corner_1).toBe(3);
});

test("listDailyRaceEntriesForRace binds the composite race_key as the only parameter", async () => {
  const allFn = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn<(value: string) => { all: typeof allFn }>(() => ({ all: allFn }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;

  await listDailyRaceEntriesForRace(db, {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0525",
    keibajoCode: "5",
    raceBango: "1",
    source: "jra",
  });

  expect(bind).toHaveBeenCalledOnce();
  expect(bind.mock.calls[0]?.[0]).toBe("jra:20260525:05:01");
});

test("listDailyRaceEntriesForRace maps row fields with the proper coercions", async () => {
  const allFn = vi.fn(async () => ({
    results: [
      {
        babajotai_code_dirt: null,
        babajotai_code_shiba: null,
        bamei: "馬名",
        banushimei: null,
        barei: 4,
        bataiju: 498,
        chokyoshimei_ryakusho: null,
        corner1_norm: null,
        corner2_norm: null,
        corner3_norm: null,
        corner4_norm: null,
        corner_1: 3,
        corner_2: 2,
        corner_3: 2,
        corner_4: 1,
        finish_norm: null,
        finish_position: null,
        futan_juryo: "55",
        grade_code: null,
        hasso_jikoku: "1430",
        juryo_shubetsu_code: null,
        kaisai_nen: "2026",
        kaisai_tsukihi: "0525",
        keibajo_code: "35",
        ketto_toroku_bango: "2023100001",
        kishumei_ryakusho: "騎手",
        kohan_3f: null,
        kyori: 1200,
        kyoso_joken_code: null,
        kyoso_shubetsu_code: null,
        race_bango: "01",
        race_date: "20260525",
        race_name: "サンプルS",
        seibetsu_code: "1",
        shusso_tosu: 10,
        soha_time: null,
        source: "nar",
        tansho_ninkijun: 1,
        tansho_odds: 2.5,
        time_sa: null,
        track_code: "24",
        umaban: 1,
        wakuban: "03",
        zogen_fugo: "+",
        zogen_sa: 4,
      },
    ],
  }));
  const db = {
    prepare: vi.fn(() => ({ bind: vi.fn(() => ({ all: allFn })) })),
  } as unknown as D1Database;

  const rows = await listDailyRaceEntriesForRace(db, {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0525",
    keibajoCode: "35",
    raceBango: "01",
    source: "nar",
  });

  expect(rows).toHaveLength(1);
  expect(rows[0]?.source).toBe("nar");
  expect(rows[0]?.kishumei_ryakusho).toBe("騎手");
  expect(rows[0]?.shusso_tosu).toBe(10);
});

test("listDailyRaceEntriesForRace rejects unexpected source values", async () => {
  const allFn = vi.fn(async () => ({
    results: [
      {
        babajotai_code_dirt: null,
        babajotai_code_shiba: null,
        bamei: null,
        banushimei: null,
        barei: null,
        bataiju: null,
        chokyoshimei_ryakusho: null,
        corner1_norm: null,
        corner2_norm: null,
        corner3_norm: null,
        corner4_norm: null,
        corner_1: null,
        corner_2: null,
        corner_3: null,
        corner_4: null,
        finish_norm: null,
        finish_position: null,
        futan_juryo: null,
        grade_code: null,
        hasso_jikoku: null,
        juryo_shubetsu_code: null,
        kaisai_nen: "2026",
        kaisai_tsukihi: "0525",
        keibajo_code: "35",
        ketto_toroku_bango: "2023100001",
        kishumei_ryakusho: null,
        kohan_3f: null,
        kyori: null,
        kyoso_joken_code: null,
        kyoso_shubetsu_code: null,
        race_bango: "01",
        race_date: "20260525",
        race_name: null,
        seibetsu_code: null,
        shusso_tosu: null,
        soha_time: null,
        source: "international",
        tansho_ninkijun: null,
        tansho_odds: null,
        time_sa: null,
        track_code: null,
        umaban: null,
        wakuban: null,
        zogen_fugo: null,
        zogen_sa: null,
      },
    ],
  }));
  const db = {
    prepare: vi.fn(() => ({ bind: vi.fn(() => ({ all: allFn })) })),
  } as unknown as D1Database;

  await expect(
    listDailyRaceEntriesForRace(db, {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0525",
      keibajoCode: "35",
      raceBango: "01",
      source: "nar",
    }),
  ).rejects.toThrow("unexpected source value: international");
});

test("triggerViewerCacheWarmForDate posts to the viewer origin and returns warm metrics on success", async () => {
  const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(
    new Response(JSON.stringify({ raceCount: 5, warmed: 4 }), {
      headers: { "content-type": "application/json" },
      status: 200,
    }),
  );
  const env = { RUNNING_STYLE_CACHE_ORIGIN: "https://example.test" } as unknown as Env;

  const result = await triggerViewerCacheWarmForDate(env, "20260525");

  expect(fetchSpy).toHaveBeenCalledOnce();
  expect(result.status).toBe("ok");
  expect(result.raceCount).toBe(5);
  expect(result.warmed).toBe(4);
});

test("triggerViewerCacheWarmForDate reports HTTP errors", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("nope", { status: 500 }));
  const env = { RUNNING_STYLE_CACHE_ORIGIN: "https://example.test" } as unknown as Env;

  const result = await triggerViewerCacheWarmForDate(env, "20260525");

  expect(result.status).toBe("error");
  expect(result.message).toBe("HTTP 500");
});

test("triggerViewerCacheWarmForDate captures thrown errors as messages", async () => {
  vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("boom"));
  const env = { RUNNING_STYLE_CACHE_ORIGIN: "https://example.test" } as unknown as Env;

  const result = await triggerViewerCacheWarmForDate(env, "20260525");

  expect(result.status).toBe("error");
  expect(result.message).toBe("boom");
});

const NOW_2026_05_28_2100_JST = new Date("2026-05-28T21:00:00+09:00");

test("shouldSkipDailyFeatureBuild returns null when no rows exist yet", () => {
  expect(
    shouldSkipDailyFeatureBuild({
      fromDate: "20260528",
      now: NOW_2026_05_28_2100_JST,
      probe: { latestUpdatedAt: null, rowCount: 0 },
      toDate: "20260528",
    }),
  ).toBeNull();
});

test("shouldSkipDailyFeatureBuild skips past-only ranges that are already populated", () => {
  const reason = shouldSkipDailyFeatureBuild({
    fromDate: "20260525",
    now: NOW_2026_05_28_2100_JST,
    probe: { latestUpdatedAt: "2026-05-26T01:00:00Z", rowCount: 5500 },
    toDate: "20260527",
  });
  expect(reason).toStrictEqual({ kind: "past-date-already-populated", rowCount: 5500 });
});

test("shouldSkipDailyFeatureBuild skips today when latestUpdatedAt is within the freshness window", () => {
  const reason = shouldSkipDailyFeatureBuild({
    fromDate: "20260528",
    now: NOW_2026_05_28_2100_JST,
    probe: { latestUpdatedAt: "2026-05-28T11:30:00Z", rowCount: 480 },
    toDate: "20260528",
  });
  expect(reason).toStrictEqual({
    kind: "today-recently-refreshed",
    latestUpdatedAt: "2026-05-28T11:30:00Z",
    rowCount: 480,
  });
});

test("shouldSkipDailyFeatureBuild re-runs today when latestUpdatedAt is outside the freshness window", () => {
  expect(
    shouldSkipDailyFeatureBuild({
      fromDate: "20260528",
      now: NOW_2026_05_28_2100_JST,
      probe: { latestUpdatedAt: "2026-05-28T09:00:00Z", rowCount: 480 },
      toDate: "20260528",
    }),
  ).toBeNull();
});

test("shouldSkipDailyFeatureBuild re-runs today when latestUpdatedAt is unparseable", () => {
  expect(
    shouldSkipDailyFeatureBuild({
      fromDate: "20260528",
      now: NOW_2026_05_28_2100_JST,
      probe: { latestUpdatedAt: "not-a-date", rowCount: 480 },
      toDate: "20260528",
    }),
  ).toBeNull();
});

test("shouldSkipDailyFeatureBuild re-runs windows that start in the future", () => {
  expect(
    shouldSkipDailyFeatureBuild({
      fromDate: "20260529",
      now: NOW_2026_05_28_2100_JST,
      probe: { latestUpdatedAt: "2026-05-28T11:55:00Z", rowCount: 100 },
      toDate: "20260529",
    }),
  ).toBeNull();
});
