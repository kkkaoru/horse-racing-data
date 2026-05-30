// run with: bun run test
import { createHash } from "node:crypto";
import { afterEach, expect, it, vi } from "vitest";
import type { Pool } from "pg";
import {
  buildRunningStyleBatchFeatureSql,
  buildRunningStyleFeaturesForRaceFromD1Target,
  buildRunningStyleFeaturesForRaceFromPostgres,
  buildRunningStylePostgresFeatureSql,
  buildRunningStylePostgresFeatureSqlWithD1Target,
  type DailyTargetRow,
} from "./running-style-feature-sql";

const PER_RACE_SQL_SHA256_REFERENCE =
  "339af73a3c06e12160d640401f2dcc0867f79610e09476a22a55d71bcd4f44de";
const PER_RACE_SQL_LENGTH_REFERENCE = 41762;
const D1_TARGET_SQL_SHA256_REFERENCE =
  "b5a4d8228ba07214a65160738a14380edbd08128b6e502cdd89ce97d0da9b88e";
const D1_TARGET_SQL_LENGTH_REFERENCE = 41977;

const BATCH_ARGS_JRA = {
  featureSchemaVersion: "v1",
  fromDate: "20050101",
  source: "jra" as const,
  toDate: "20260531",
};

const sha256 = (text: string): string => createHash("sha256").update(text).digest("hex");

const PARAMS = {
  kaisaiNen: "2026",
  kaisaiTsukihi: "0512",
  keibajoCode: "08",
  raceBango: "01",
  source: "jra" as const,
};

const TARGET_ROW: DailyTargetRow = {
  babajotai_code_dirt: null,
  babajotai_code_shiba: null,
  bamei: "サンプル",
  chokyoshimei_ryakusho: null,
  grade_code: null,
  kaisai_nen: "2026",
  kaisai_tsukihi: "0512",
  keibajo_code: "08",
  ketto_toroku_bango: "2024100001",
  kishumei_ryakusho: null,
  kyori: 2000,
  kyoso_joken_code: null,
  race_bango: "01",
  race_date: "20260512",
  shusso_tosu: 16,
  source: "jra",
  track_code: null,
  umaban: 1,
};

const SQL_ROW = {
  bamei: "サンプル",
  career_win_rate: "0.2",
  kaisai_nen: "2026",
  kaisai_tsukihi: "0512",
  keibajo_code: "08",
  ketto_toroku_bango: "2024100001",
  race_bango: "01",
  source: "jra",
  umaban: "1",
};

afterEach(() => {
  vi.restoreAllMocks();
});

it("buildRunningStylePostgresFeatureSql returns SQL string", () => {
  const sql = buildRunningStylePostgresFeatureSql();
  expect(typeof sql).toBe("string");
  expect(sql.length).toBeGreaterThan(0);
});

it("buildRunningStylePostgresFeatureSql output is byte-identical to pre-refactor reference", () => {
  const sql = buildRunningStylePostgresFeatureSql();
  expect(sql.length).toBe(PER_RACE_SQL_LENGTH_REFERENCE);
  expect(sha256(sql)).toBe(PER_RACE_SQL_SHA256_REFERENCE);
});

it("buildRunningStylePostgresFeatureSqlWithD1Target output is byte-identical to pre-refactor reference", () => {
  const sql = buildRunningStylePostgresFeatureSqlWithD1Target();
  expect(sql.length).toBe(D1_TARGET_SQL_LENGTH_REFERENCE);
  expect(sha256(sql)).toBe(D1_TARGET_SQL_SHA256_REFERENCE);
});

it("buildRunningStyleBatchFeatureSql returns SQL string with no $N placeholders for jra", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(sql.length).toBeGreaterThan(0);
  expect(sql.match(/\$\d/)).toBeNull();
});

it("buildRunningStyleBatchFeatureSql injects source/date/featureSchemaVersion literals for jra", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(sql.includes("'jra'::text as source")).toBe(true);
  expect(sql.includes("'20050101'::text as race_date_min")).toBe(true);
  expect(sql.includes("'20260531'::text as race_date")).toBe(true);
  expect(sql.includes("'v1' as feature_schema_version")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql injects nar source literal", () => {
  const sql = buildRunningStyleBatchFeatureSql({
    featureSchemaVersion: "v2",
    fromDate: "20100101",
    source: "nar",
    toDate: "20260531",
  });
  expect(sql.includes("'nar'::text as source")).toBe(true);
  expect(sql.includes("'v2' as feature_schema_version")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql includes target.race_date date-range filter", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(sql.includes("r.race_date between p.race_date_min and p.race_date")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql derives history_start from fromDate", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(sql.includes("to_date('20050101', 'YYYYMMDD') - interval '10 years'")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql shares the same suffix CTE list as the per-race builder", () => {
  const perRace = buildRunningStylePostgresFeatureSql();
  const batch = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  const perRaceCteNames = [...perRace.matchAll(/^(\w+) as \(/gm)].map((m) => m[1]);
  const batchCteNames = [...batch.matchAll(/^(\w+) as \(/gm)].map((m) => m[1]);
  expect(batchCteNames).toStrictEqual(perRaceCteNames);
});

it("buildRunningStyleBatchFeatureSql rejects an invalid source", () => {
  expect(() =>
    buildRunningStyleBatchFeatureSql({
      featureSchemaVersion: "v1",
      fromDate: "20050101",
      source: "bad" as unknown as "jra",
      toDate: "20260531",
    }),
  ).toThrow("invalid batch source");
});

it("buildRunningStyleBatchFeatureSql rejects non-YYYYMMDD fromDate", () => {
  expect(() =>
    buildRunningStyleBatchFeatureSql({
      featureSchemaVersion: "v1",
      fromDate: "2005",
      source: "jra",
      toDate: "20260531",
    }),
  ).toThrow("invalid batch fromDate");
});

it("buildRunningStyleBatchFeatureSql rejects non-numeric toDate", () => {
  expect(() =>
    buildRunningStyleBatchFeatureSql({
      featureSchemaVersion: "v1",
      fromDate: "20050101",
      source: "jra",
      toDate: "abcd1234",
    }),
  ).toThrow("invalid batch toDate");
});

it("buildRunningStyleBatchFeatureSql rejects unsafe featureSchemaVersion", () => {
  expect(() =>
    buildRunningStyleBatchFeatureSql({
      featureSchemaVersion: "v1;DROP TABLE",
      fromDate: "20050101",
      source: "jra",
      toDate: "20260531",
    }),
  ).toThrow("invalid batch featureSchemaVersion");
});

it("buildRunningStyleBatchFeatureSql rejects fromDate > toDate", () => {
  expect(() =>
    buildRunningStyleBatchFeatureSql({
      featureSchemaVersion: "v1",
      fromDate: "20260601",
      source: "jra",
      toDate: "20260531",
    }),
  ).toThrow("invalid batch date range");
});

it("buildRunningStylePostgresFeatureSqlWithD1Target swaps the rec target CTE for the JSONB target CTE", () => {
  const sql = buildRunningStylePostgresFeatureSqlWithD1Target();
  expect(sql.length).toBeGreaterThan(0);
  expect(sql.match(/jsonb_to_recordset\(\$6/)).not.toBeNull();
});

it("buildRunningStyleFeaturesForRaceFromPostgres throws on unexpected raceKey in results", async () => {
  const query = vi.fn(async () => ({
    rowCount: 1,
    rows: [{ ...SQL_ROW, keibajo_code: "07" }],
  }));
  const pool = { query } as unknown as Pool;
  await expect(
    buildRunningStyleFeaturesForRaceFromPostgres(pool, PARAMS, ["career_win_rate"]),
  ).rejects.toThrow("unexpected race key in PostgreSQL feature result");
});

it("buildRunningStyleFeaturesForRaceFromPostgres returns rows for matching raceKey", async () => {
  const query = vi.fn(async () => ({ rowCount: 1, rows: [SQL_ROW] }));
  const pool = { query } as unknown as Pool;
  const result = await buildRunningStyleFeaturesForRaceFromPostgres(pool, PARAMS, [
    "career_win_rate",
  ]);
  expect(result.rows.length).toBe(1);
  expect(result.rows[0]!.raceKey).toBe("jra:20260512:08:01");
});

it("buildRunningStyleFeaturesForRaceFromD1Target throws when target rows empty", async () => {
  const query = vi.fn(async () => ({ rowCount: 0, rows: [] }));
  const pool = { query } as unknown as Pool;
  await expect(
    buildRunningStyleFeaturesForRaceFromD1Target(pool, PARAMS, ["career_win_rate"], []),
  ).rejects.toThrow("no D1 target rows provided");
});

it("buildRunningStyleFeaturesForRaceFromD1Target throws on unexpected raceKey in results", async () => {
  const query = vi.fn(async () => ({
    rowCount: 1,
    rows: [{ ...SQL_ROW, keibajo_code: "07" }],
  }));
  const pool = { query } as unknown as Pool;
  await expect(
    buildRunningStyleFeaturesForRaceFromD1Target(pool, PARAMS, ["career_win_rate"], [TARGET_ROW]),
  ).rejects.toThrow("unexpected race key in PostgreSQL feature result");
});

it("buildRunningStyleFeaturesForRaceFromD1Target returns rows for matching raceKey", async () => {
  const query = vi.fn(async () => ({ rowCount: 1, rows: [SQL_ROW] }));
  const pool = { query } as unknown as Pool;
  const result = await buildRunningStyleFeaturesForRaceFromD1Target(
    pool,
    PARAMS,
    ["career_win_rate"],
    [TARGET_ROW],
  );
  expect(result.rows.length).toBe(1);
  expect(result.sqlRows).toBe(1);
});

it("buildRunningStyleFeaturesForRaceFromPostgres parses non-string numeric values (number, bigint)", async () => {
  const query = vi.fn(async () => ({
    rowCount: 1,
    rows: [
      {
        bamei: "サンプル",
        career_win_rate: 0.5,
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        ketto_toroku_bango: 2024100001n,
        race_bango: "01",
        source: "jra",
        umaban: 1n,
      },
    ],
  }));
  const pool = { query } as unknown as Pool;
  const result = await buildRunningStyleFeaturesForRaceFromPostgres(pool, PARAMS, [
    "career_win_rate",
  ]);
  expect(result.rows[0]?.umaban).toBe(1);
  expect(result.rows[0]?.perHorseFeatures.career_win_rate).toBe(0.5);
});

it("buildRunningStyleFeaturesForRaceFromPostgres maps Date and boolean values via toStringOrNull/toNumberOrNull", async () => {
  const query = vi.fn(async () => ({
    rowCount: 1,
    rows: [
      {
        bamei: new Date("2026-05-12T00:00:00Z"),
        career_win_rate: true,
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        ketto_toroku_bango: "2024100001",
        race_bango: "01",
        source: "jra",
        umaban: new Date("2026-05-12T00:00:00Z"),
      },
    ],
  }));
  const pool = { query } as unknown as Pool;
  const result = await buildRunningStyleFeaturesForRaceFromPostgres(pool, PARAMS, [
    "career_win_rate",
  ]);
  expect(result.rows[0]?.bamei).toBe("2026-05-12T00:00:00.000Z");
  expect(result.rows[0]?.umaban).toBe(0);
  expect(result.rows[0]?.perHorseFeatures.career_win_rate).toBe(1);
});

it("buildRunningStyleFeaturesForRaceFromPostgres treats unknown value types (plain object) as null", async () => {
  const query = vi.fn(async () => ({
    rowCount: 1,
    rows: [
      {
        bamei: { unexpected: "object" } as unknown,
        career_win_rate: { weird: true } as unknown,
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        ketto_toroku_bango: "2024100001",
        race_bango: "01",
        source: "jra",
        umaban: 1,
      },
    ],
  }));
  const pool = { query } as unknown as Pool;
  const result = await buildRunningStyleFeaturesForRaceFromPostgres(pool, PARAMS, [
    "career_win_rate",
  ]);
  expect(result.rows[0]?.bamei).toBeNull();
  expect(result.rows[0]?.perHorseFeatures.career_win_rate).toBeNull();
});

it("buildRunningStyleFeaturesForRaceFromPostgres treats NaN/Infinity/empty as null", async () => {
  const query = vi.fn(async () => ({
    rowCount: 1,
    rows: [
      {
        bamei: "",
        career_win_rate: "not-a-number",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        ketto_toroku_bango: "2024100001",
        race_bango: "01",
        source: "jra",
        umaban: Number.NaN,
      },
    ],
  }));
  const pool = { query } as unknown as Pool;
  const result = await buildRunningStyleFeaturesForRaceFromPostgres(pool, PARAMS, [
    "career_win_rate",
  ]);
  expect(result.rows[0]?.bamei).toBeNull();
  expect(result.rows[0]?.perHorseFeatures.career_win_rate).toBeNull();
  expect(result.rows[0]?.umaban).toBe(0);
});

it("buildRunningStyleFeaturesForRaceFromD1Target falls back to rows.length when rowCount missing", async () => {
  const query = vi.fn(async () => ({ rows: [SQL_ROW] }));
  const pool = { query } as unknown as Pool;
  const result = await buildRunningStyleFeaturesForRaceFromD1Target(
    pool,
    PARAMS,
    ["career_win_rate"],
    [TARGET_ROW],
  );
  expect(result.sqlRows).toBe(1);
});

it("buildRunningStyleFeaturesForRaceFromPostgres falls back to rows.length when rowCount missing", async () => {
  const query = vi.fn(async () => ({ rows: [SQL_ROW] }));
  const pool = { query } as unknown as Pool;
  const result = await buildRunningStyleFeaturesForRaceFromPostgres(pool, PARAMS, [
    "career_win_rate",
  ]);
  expect(result.sqlRows).toBe(1);
});

it("buildRunningStyleFeaturesForRaceFromPostgres treats undefined feature column and boolean false as null/0", async () => {
  const query = vi.fn(async () => ({
    rowCount: 1,
    rows: [
      {
        bamei: "サンプル",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        ketto_toroku_bango: "2024100001",
        race_bango: "01",
        source: "jra",
        speed_index_avg_5: false,
        umaban: 1,
      },
    ],
  }));
  const pool = { query } as unknown as Pool;
  const result = await buildRunningStyleFeaturesForRaceFromPostgres(pool, PARAMS, [
    "career_win_rate",
    "speed_index_avg_5",
  ]);
  expect(result.rows[0]?.perHorseFeatures.career_win_rate).toBeNull();
  expect(result.rows[0]?.perHorseFeatures.speed_index_avg_5).toBe(0);
});
