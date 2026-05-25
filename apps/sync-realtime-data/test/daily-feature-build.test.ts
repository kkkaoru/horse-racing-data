// Run with bun test apps/sync-realtime-data/test/daily-feature-build.test.ts
import { expect, test, vi } from "vitest";

import {
  buildDailyFeatureSelectSql,
  upsertDailyRaceEntriesToD1,
  type DailyRaceEntryRow,
} from "../src/daily-feature-build";

const buildRow = (overrides: Partial<DailyRaceEntryRow> = {}): DailyRaceEntryRow => ({
  babajotai_code_dirt: null,
  babajotai_code_shiba: null,
  bamei: "テスト馬",
  banushimei: null,
  barei: 4,
  chokyoshimei_ryakusho: null,
  corner1_norm: null,
  corner2_norm: null,
  corner3_norm: null,
  corner4_norm: null,
  finish_norm: null,
  finish_position: null,
  futan_juryo: 55,
  grade_code: null,
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
  seibetsu_code: "1",
  shusso_tosu: 10,
  soha_time: null,
  source: "nar",
  tansho_ninkijun: 1,
  tansho_odds: 2.5,
  time_sa: null,
  track_code: "24",
  umaban: 1,
  ...overrides,
});

test("buildDailyFeatureSelectSql includes both jra and nar for scope=all", () => {
  const sql = buildDailyFeatureSelectSql({ fromDate: "20260525", sourceScope: "all" });
  expect(sql).toMatch(/from jvd_se se/);
  expect(sql).toMatch(/from nvd_se se/);
});

test("buildDailyFeatureSelectSql limits NAR to non-ban-ei when scope=nar", () => {
  const sql = buildDailyFeatureSelectSql({ fromDate: "20260525", sourceScope: "nar" });
  expect(sql).toMatch(/from nvd_se se/);
  expect(sql).toMatch(/and ra\.keibajo_code <> '83'/);
  expect(sql).not.toMatch(/from jvd_se se/);
});

test("buildDailyFeatureSelectSql limits NAR to ban-ei when scope=ban-ei", () => {
  const sql = buildDailyFeatureSelectSql({ fromDate: "20260525", sourceScope: "ban-ei" });
  expect(sql).toMatch(/and ra\.keibajo_code = '83'/);
});

test("buildDailyFeatureSelectSql rejects bad date", () => {
  expect(() => buildDailyFeatureSelectSql({ fromDate: "20260" })).toThrow(
    "fromDate must match YYYYMMDD: 20260",
  );
});

test("buildDailyFeatureSelectSql honors toDate when provided", () => {
  const sql = buildDailyFeatureSelectSql({
    fromDate: "20260525",
    toDate: "20260526",
    sourceScope: "all",
  });
  expect(sql).toMatch(/se\.kaisai_nen \|\| se\.kaisai_tsukihi >= '20260525'/);
  expect(sql).toMatch(/se\.kaisai_nen \|\| se\.kaisai_tsukihi <= '20260526'/);
});

test("upsertDailyRaceEntriesToD1 writes 0 rows when input is empty", async () => {
  const db = { prepare: vi.fn(), batch: vi.fn() } as unknown as D1Database;
  const written = await upsertDailyRaceEntriesToD1(db, []);
  expect(written).toBe(0);
  expect(db.prepare).not.toHaveBeenCalled();
  expect(db.batch).not.toHaveBeenCalled();
});

test("upsertDailyRaceEntriesToD1 batches rows into D1.batch calls", async () => {
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
