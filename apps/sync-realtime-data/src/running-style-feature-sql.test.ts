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

// Wave P4c P0 + P1 + P3: 11 new columns added — past_{nige,senkou,sashi,oikomi}_rate_self_recent_{3,5}
// (8 short-window self-rate features), jockey_horse_pair_nige_rate +
// trainer_horse_pair_nige_rate (2 pair-level nige rate features), and
// field_nige_pressure_rank (1 in-race rank of past_nige_rate_self_recent_5).
// Snapshots were re-pinned after the addition; downstream parquet schema is
// additive (existing columns unchanged).
const PER_RACE_SQL_SHA256_REFERENCE =
  "4613ff8b55fc1160527b0ce19ac3bb18537cec4482ddecbf18f63a85e257e347";
const PER_RACE_SQL_LENGTH_REFERENCE = 45475;
const D1_TARGET_SQL_SHA256_REFERENCE =
  "c2671a6ca2eb72ab91a598218bab79d4946be13113b90853365d307c448b1058";
const D1_TARGET_SQL_LENGTH_REFERENCE = 45690;
// Batch SQL gets `MATERIALIZED` hints injected for 10 heavy CTEs (rec, target,
// target_horses, se_lookup, ra_lookup, horse_history_base, jockey_history,
// trainer_history, pedigree_rec_um, target_months) so PG materializes them once
// instead of re-inlining per reference. This snapshot pins the post-hint
// output so any accidental regression on the materialization list trips here.
const BATCH_JRA_SQL_SHA256_REFERENCE =
  "b57b5b2e776ed6f445a1e44af1a0266c39b9062fc9a5b4f2324b7be779a7b242";
const BATCH_JRA_SQL_LENGTH_REFERENCE = 45341;
const BATCH_MATERIALIZED_HINT_COUNT = 10;

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
  const perRaceCteNames = [...perRace.matchAll(/^(\w+) as (?:materialized )?\(/gm)].map(
    (m) => m[1],
  );
  const batchCteNames = [...batch.matchAll(/^(\w+) as (?:materialized )?\(/gm)].map((m) => m[1]);
  expect(batchCteNames).toStrictEqual(perRaceCteNames);
});

it("buildRunningStyleBatchFeatureSql output for jra is byte-identical to materialized-hint snapshot", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(sql.length).toBe(BATCH_JRA_SQL_LENGTH_REFERENCE);
  expect(sha256(sql)).toBe(BATCH_JRA_SQL_SHA256_REFERENCE);
});

it("buildRunningStyleBatchFeatureSql injects MATERIALIZED on exactly the heavy CTE list", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  const matches = sql.match(/ as materialized \(/g) ?? [];
  expect(matches.length).toBe(BATCH_MATERIALIZED_HINT_COUNT);
});

it("buildRunningStyleBatchFeatureSql materializes specific named CTEs", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(sql.includes("rec as materialized (")).toBe(true);
  expect(sql.includes("target as materialized (")).toBe(true);
  expect(sql.includes("horse_history_base as materialized (")).toBe(true);
  expect(sql.includes("pedigree_rec_um as materialized (")).toBe(true);
  expect(sql.includes("target_months as materialized (")).toBe(true);
});

it("buildRunningStylePostgresFeatureSql contains no MATERIALIZED hints (per-race uses default inlining)", () => {
  const sql = buildRunningStylePostgresFeatureSql();
  expect(sql.match(/ as materialized \(/g)).toBeNull();
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

it("buildRunningStyleBatchFeatureSql strictNigeTarget=false (default) is byte-identical to lax snapshot", () => {
  const sql = buildRunningStyleBatchFeatureSql({
    featureSchemaVersion: "v1",
    fromDate: "20050101",
    source: "jra",
    strictNigeTarget: false,
    toDate: "20260531",
  });
  expect(sql.length).toBe(BATCH_JRA_SQL_LENGTH_REFERENCE);
  expect(sha256(sql)).toBe(BATCH_JRA_SQL_SHA256_REFERENCE);
});

it("buildRunningStyleBatchFeatureSql strict omitted matches strict=false", () => {
  const omitted = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  const explicitFalse = buildRunningStyleBatchFeatureSql({
    featureSchemaVersion: "v1",
    fromDate: "20050101",
    source: "jra",
    strictNigeTarget: false,
    toDate: "20260531",
  });
  expect(omitted).toBe(explicitFalse);
});

it("buildRunningStyleBatchFeatureSql strictNigeTarget=true injects f.corner2_norm into the rec CTE", () => {
  const sql = buildRunningStyleBatchFeatureSql({
    featureSchemaVersion: "v1",
    fromDate: "20050101",
    source: "jra",
    strictNigeTarget: true,
    toDate: "20260531",
  });
  expect(sql.includes("    f.corner1_norm,\n    f.corner2_norm,\n")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql strictNigeTarget=true propagates r.corner2_norm as target_corner_2_norm", () => {
  const sql = buildRunningStyleBatchFeatureSql({
    featureSchemaVersion: "v1",
    fromDate: "20050101",
    source: "jra",
    strictNigeTarget: true,
    toDate: "20260531",
  });
  expect(
    sql.includes(
      "    r.corner1_norm as target_corner_1_norm,\n    r.corner2_norm as target_corner_2_norm,\n",
    ),
  ).toBe(true);
});

it("buildRunningStyleBatchFeatureSql strictNigeTarget=true rewrites the nige case arm to require corner2_norm=0", () => {
  const sql = buildRunningStyleBatchFeatureSql({
    featureSchemaVersion: "v1",
    fromDate: "20050101",
    source: "jra",
    strictNigeTarget: true,
    toDate: "20260531",
  });
  expect(sql.includes("when r.corner1_norm = 0 and r.corner2_norm = 0 then 0")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql strictNigeTarget=true removes the lax single-corner nige case arm", () => {
  const sql = buildRunningStyleBatchFeatureSql({
    featureSchemaVersion: "v1",
    fromDate: "20050101",
    source: "jra",
    strictNigeTarget: true,
    toDate: "20260531",
  });
  expect(sql.includes("when r.corner1_norm = 0 then 0")).toBe(false);
});

it("buildRunningStyleBatchFeatureSql strictNigeTarget=true keeps senkou/sashi/oikomi thresholds unchanged", () => {
  const sql = buildRunningStyleBatchFeatureSql({
    featureSchemaVersion: "v1",
    fromDate: "20050101",
    source: "jra",
    strictNigeTarget: true,
    toDate: "20260531",
  });
  expect(sql.includes("when r.corner1_norm <= 0.3 then 1")).toBe(true);
  expect(sql.includes("when r.corner1_norm <= 0.7 then 2")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql strictNigeTarget=true preserves the NULL-corner1 short-circuit", () => {
  const sql = buildRunningStyleBatchFeatureSql({
    featureSchemaVersion: "v1",
    fromDate: "20050101",
    source: "jra",
    strictNigeTarget: true,
    toDate: "20260531",
  });
  expect(sql.includes("when r.corner1_norm is null then null")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql strictNigeTarget=true changes the rendered SQL length vs lax", () => {
  const lax = buildRunningStyleBatchFeatureSql({
    featureSchemaVersion: "v1",
    fromDate: "20050101",
    source: "jra",
    strictNigeTarget: false,
    toDate: "20260531",
  });
  const strict = buildRunningStyleBatchFeatureSql({
    featureSchemaVersion: "v1",
    fromDate: "20050101",
    source: "jra",
    strictNigeTarget: true,
    toDate: "20260531",
  });
  expect(strict.length).toBeGreaterThan(lax.length);
});

it("buildRunningStyleBatchFeatureSql strictNigeTarget=true still materializes the heavy CTE list", () => {
  const sql = buildRunningStyleBatchFeatureSql({
    featureSchemaVersion: "v1",
    fromDate: "20050101",
    source: "jra",
    strictNigeTarget: true,
    toDate: "20260531",
  });
  const matches = sql.match(/ as materialized \(/g) ?? [];
  expect(matches.length).toBe(BATCH_MATERIALIZED_HINT_COUNT);
});

it("buildRunningStyleBatchFeatureSql strictNigeTarget=true accepts nar source", () => {
  const sql = buildRunningStyleBatchFeatureSql({
    featureSchemaVersion: "v1",
    fromDate: "20100101",
    source: "nar",
    strictNigeTarget: true,
    toDate: "20260531",
  });
  expect(sql.includes("'nar'::text as source")).toBe(true);
  expect(sql.includes("    f.corner2_norm,\n")).toBe(true);
  expect(sql.includes("when r.corner1_norm = 0 and r.corner2_norm = 0 then 0")).toBe(true);
});

it("buildRunningStylePostgresFeatureSql does not contain the strict-nige clause (per-race unaffected)", () => {
  const sql = buildRunningStylePostgresFeatureSql();
  expect(sql.includes("when r.corner1_norm = 0 and r.corner2_norm = 0 then 0")).toBe(false);
  expect(sql.includes("when r.corner1_norm = 0 then 0")).toBe(true);
});

it("buildRunningStylePostgresFeatureSqlWithD1Target does not contain the strict-nige clause (D1 target unaffected)", () => {
  const sql = buildRunningStylePostgresFeatureSqlWithD1Target();
  expect(sql.includes("when r.corner1_norm = 0 and r.corner2_norm = 0 then 0")).toBe(false);
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

it("buildRunningStyleBatchFeatureSql exposes past_nige_rate_self_recent_5 column", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(sql.includes("as past_nige_rate_self_recent_5")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql exposes past_senkou_rate_self_recent_5 column", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(sql.includes("as past_senkou_rate_self_recent_5")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql exposes past_sashi_rate_self_recent_5 column", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(sql.includes("as past_sashi_rate_self_recent_5")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql exposes past_oikomi_rate_self_recent_5 column", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(sql.includes("as past_oikomi_rate_self_recent_5")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql exposes past_nige_rate_self_recent_3 column", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(sql.includes("as past_nige_rate_self_recent_3")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql exposes past_senkou_rate_self_recent_3 column", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(sql.includes("as past_senkou_rate_self_recent_3")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql exposes past_sashi_rate_self_recent_3 column", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(sql.includes("as past_sashi_rate_self_recent_3")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql exposes past_oikomi_rate_self_recent_3 column", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(sql.includes("as past_oikomi_rate_self_recent_3")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql exposes jockey_horse_pair_nige_rate column", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(sql.includes("as jockey_horse_pair_nige_rate")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql exposes trainer_horse_pair_nige_rate column", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(sql.includes("as trainer_horse_pair_nige_rate")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql exposes field_nige_pressure_rank column", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(sql.includes("as field_nige_pressure_rank")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql uses recent_rank <= 5 filter for short-window self-rate", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(sql.includes("filter (where b.recent_rank <= 5 and b.corner1_norm = 0)")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql uses recent_rank <= 3 filter for short-window self-rate", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(sql.includes("filter (where b.recent_rank <= 3 and b.corner1_norm = 0)")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql derives jockey_horse_pair_nige_rate via history_horse = target_horse filter", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(
    sql.includes(
      "avg(case when corner1_norm = 0 then 1.0 when corner1_norm is null then null else 0.0 end) filter (where history_horse = target_horse) as jockey_horse_pair_nige_rate",
    ),
  ).toBe(true);
});

it("buildRunningStyleBatchFeatureSql derives trainer_horse_pair_nige_rate via history_horse = target_horse filter", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(
    sql.includes(
      "avg(case when corner1_norm = 0 then 1.0 when corner1_norm is null then null else 0.0 end) filter (where history_horse = target_horse) as trainer_horse_pair_nige_rate",
    ),
  ).toBe(true);
});

it("buildRunningStyleBatchFeatureSql declares the race_by_past_nige_recent_5_desc window", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(
    sql.includes(
      "race_by_past_nige_recent_5_desc as (partition by b.source, b.kaisai_nen, b.kaisai_tsukihi, b.keibajo_code, b.race_bango order by b.past_nige_rate_self_recent_5 desc nulls last)",
    ),
  ).toBe(true);
});

it("buildRunningStyleBatchFeatureSql projects field_nige_pressure_rank via rank() over the new window", () => {
  const sql = buildRunningStyleBatchFeatureSql(BATCH_ARGS_JRA);
  expect(
    sql.includes("rank() over race_by_past_nige_recent_5_desc as field_nige_pressure_rank"),
  ).toBe(true);
});

it("buildRunningStylePostgresFeatureSql exposes all 11 new running-style features", () => {
  const sql = buildRunningStylePostgresFeatureSql();
  expect(sql.includes("past_nige_rate_self_recent_5")).toBe(true);
  expect(sql.includes("past_senkou_rate_self_recent_5")).toBe(true);
  expect(sql.includes("past_sashi_rate_self_recent_5")).toBe(true);
  expect(sql.includes("past_oikomi_rate_self_recent_5")).toBe(true);
  expect(sql.includes("past_nige_rate_self_recent_3")).toBe(true);
  expect(sql.includes("past_senkou_rate_self_recent_3")).toBe(true);
  expect(sql.includes("past_sashi_rate_self_recent_3")).toBe(true);
  expect(sql.includes("past_oikomi_rate_self_recent_3")).toBe(true);
  expect(sql.includes("jockey_horse_pair_nige_rate")).toBe(true);
  expect(sql.includes("trainer_horse_pair_nige_rate")).toBe(true);
  expect(sql.includes("field_nige_pressure_rank")).toBe(true);
});

it("buildRunningStylePostgresFeatureSqlWithD1Target exposes all 11 new running-style features", () => {
  const sql = buildRunningStylePostgresFeatureSqlWithD1Target();
  expect(sql.includes("past_nige_rate_self_recent_5")).toBe(true);
  expect(sql.includes("past_senkou_rate_self_recent_5")).toBe(true);
  expect(sql.includes("past_sashi_rate_self_recent_5")).toBe(true);
  expect(sql.includes("past_oikomi_rate_self_recent_5")).toBe(true);
  expect(sql.includes("past_nige_rate_self_recent_3")).toBe(true);
  expect(sql.includes("past_senkou_rate_self_recent_3")).toBe(true);
  expect(sql.includes("past_sashi_rate_self_recent_3")).toBe(true);
  expect(sql.includes("past_oikomi_rate_self_recent_3")).toBe(true);
  expect(sql.includes("jockey_horse_pair_nige_rate")).toBe(true);
  expect(sql.includes("trainer_horse_pair_nige_rate")).toBe(true);
  expect(sql.includes("field_nige_pressure_rank")).toBe(true);
});

it("buildRunningStyleBatchFeatureSql strictNigeTarget=true still exposes the 11 new running-style features", () => {
  const sql = buildRunningStyleBatchFeatureSql({
    featureSchemaVersion: "v1",
    fromDate: "20050101",
    source: "jra",
    strictNigeTarget: true,
    toDate: "20260531",
  });
  expect(sql.includes("past_nige_rate_self_recent_5")).toBe(true);
  expect(sql.includes("jockey_horse_pair_nige_rate")).toBe(true);
  expect(sql.includes("trainer_horse_pair_nige_rate")).toBe(true);
  expect(sql.includes("field_nige_pressure_rank")).toBe(true);
});
