import { describe, expect, it } from "vitest";
import {
  buildConfig,
  buildDependencyPlan,
  buildDependencySql,
  buildFingerprintSql,
  buildIncrementalApplySql,
  buildIncrementalCopyFromSql,
  buildJsonlRecord,
  buildMetadataSql,
  buildNeonApplySql,
  buildStageTableName,
  buildTableFilterSql,
  buildTableProfileSql,
  buildTimestampFingerprintSql,
  calculateEtaSeconds,
  computeBackoffDelayMs,
  computeChunkEtaSeconds,
  computeChunkPlan,
  decideVerifyMismatchAction,
  formatRowsPerSecond,
  incrementalComparatorForTimestampColumn,
  isVerifyMismatchSkipError,
  parseConcurrency,
  parseApplyMode,
  parseDependencyEdges,
  parseBoolean,
  parseFingerprintLine,
  parsePositiveInteger,
  parseSelectedTables,
  parseStrategyMode,
  parseTableMetadata,
  parseTableProfiles,
  pkExpression,
  quoteIdentifier,
  quoteLiteral,
  resolveConcurrency,
  resolveDefaultFullReplaceBatchRows,
  resolveNonNegativeSecondsEnv,
  resolveOperationTimeoutPolicy,
  resolvePerTableIdleMs,
  resolvePerTableWallClockMs,
  resolvePositiveIntegerEnv,
  resolveRetryBackoffConfig,
  resolveSkipTables,
  resolveStrategy,
  resolveVerifyMismatchPolicy,
  runPushSync,
  runWithRetry,
  buildNeonPsqlArgs,
  DEFAULT_NEON_PSQL_CONTAINER,
  LOCAL_CONTAINER_NAME,
  shouldRefreshInclusiveIncrementalMarker,
  timestampKeyExpression,
  VerifyMismatchSkipError,
  waitForNeonReady,
  type ProgressEvent,
  type RetryBackoffConfig,
  type RetryFailureInfo,
  type RetryGaveUpInfo,
  type RetryAttemptInfo,
  type SyncStrategyThresholds,
  type TableMetadata,
  type VerifyMismatchPolicy,
} from "./core";

const defaultThresholds: SyncStrategyThresholds = {
  smallTableMaxRows: 10_000,
  updateChurnMinTuples: 1000,
};

const tableA: TableMetadata = {
  tableName: "table_a",
  estimatedRows: 100,
  estimatedBytes: 1_000,
  columnList: '"id", "name"',
  primaryKeyList: '"id"',
  primaryKeyJoin: 'target."id" = stage."id"',
  updateList: '"name" = excluded."name"',
};

const tableB: TableMetadata = {
  tableName: 'weird"table',
  estimatedRows: 50,
  estimatedBytes: 500,
  columnList: '"id"',
  primaryKeyList: '"id"',
  primaryKeyJoin: 'target."id" = stage."id"',
  updateList: "",
};

describe("config parsing", () => {
  it("parses positive integers with fallbacks", () => {
    expect(parsePositiveInteger("8", 4)).toBe(8);
    expect(parsePositiveInteger("0", 4)).toBe(4);
    expect(parsePositiveInteger("-1", 4)).toBe(4);
    expect(parsePositiveInteger("1.5", 4)).toBe(4);
    expect(parsePositiveInteger("abc", 4)).toBe(4);
    expect(parsePositiveInteger(undefined, 4)).toBe(4);
  });

  it("parses sync concurrency", () => {
    expect(parseConcurrency("8")).toBe(8);
    expect(parseConcurrency("auto")).toBe("auto");
    expect(parseConcurrency("0")).toBe("auto");
    expect(parseConcurrency(undefined)).toBe("auto");
  });

  it("parses booleans with fallbacks", () => {
    expect(parseBoolean("true", false)).toBe(true);
    expect(parseBoolean("YES", false)).toBe(true);
    expect(parseBoolean("1", false)).toBe(true);
    expect(parseBoolean("off", true)).toBe(false);
    expect(parseBoolean("NO", true)).toBe(false);
    expect(parseBoolean("unknown", true)).toBe(true);
    expect(parseBoolean(undefined, false)).toBe(false);
  });

  it("parses apply mode", () => {
    expect(parseApplyMode("upsert")).toBe("upsert");
    expect(parseApplyMode("replace")).toBe("replace");
    expect(parseApplyMode(undefined)).toBe("replace");
    expect(parseApplyMode("unknown")).toBe("replace");
  });

  it("builds config from env", () => {
    expect(
      buildConfig({
        REPLICA_SYNC_CONCURRENCY: "6",
        REPLICA_SYNC_DELETE: "false",
        REPLICA_SYNC_APPLY_MODE: "upsert",
        NEON_CONNECT_TIMEOUT_SECONDS: "30",
        NEON_CONNECT_RETRY_SECONDS: "3",
        REPLICA_SYNC_TABLES: " jvd_ra, nvd_ra ,, ",
      }),
    ).toEqual({
      concurrency: 6,
      deleteMissingRows: false,
      applyMode: "upsert",
      neonConnectTimeoutSeconds: 30,
      neonConnectRetrySeconds: 3,
      selectedTables: ["jvd_ra", "nvd_ra"],
      strategyMode: "auto",
      strategyThresholds: {
        smallTableMaxRows: 10000,
        updateChurnMinTuples: 1000,
      },
    });
  });

  it("returns undefined for empty table selection", () => {
    expect(parseSelectedTables(" , ")).toBeUndefined();
    expect(parseSelectedTables(undefined)).toBeUndefined();
  });
});

describe("SQL helpers", () => {
  it("quotes identifiers and literals", () => {
    expect(quoteIdentifier('a"b')).toBe('"a""b"');
    expect(quoteLiteral("a'b")).toBe("'a''b'");
  });

  it("builds table filter SQL", () => {
    expect(buildTableFilterSql(undefined)).toBe("true");
    expect(buildTableFilterSql(["a", "b'c"])).toBe("c.relname in ('a','b''c')");
  });

  it("builds metadata SQL with selected tables", () => {
    const sql = buildMetadataSql(["jvd_ra"]);
    expect(sql).toContain("with pk_cols as");
    expect(sql).toContain("c.relname in ('jvd_ra')");
    expect(sql).toContain("pk_join");
  });

  it("builds dependency SQL with selected tables", () => {
    const sql = buildDependencySql(["child_table"]);
    expect(sql).toContain("constraint_info.contype = 'f'");
    expect(sql).toContain("child.relname in ('child_table')");
    expect(sql).toContain("parent.relname in ('child_table')");
  });

  it("parses table metadata", () => {
    const rows = parseTableMetadata(
      'jvd_ra\t123\t456\t"id", "name"\t"id"\ttarget."id" = stage."id"\t"name" = excluded."name"\n',
    );
    expect(rows).toEqual([
      {
        tableName: "jvd_ra",
        estimatedRows: 123,
        estimatedBytes: 456,
        columnList: '"id", "name"',
        primaryKeyList: '"id"',
        primaryKeyJoin: 'target."id" = stage."id"',
        updateList: '"name" = excluded."name"',
      },
    ]);
  });

  it("rejects invalid metadata", () => {
    expect(() => parseTableMetadata("broken\t1")).toThrow("Invalid table metadata row");
  });

  it("parses dependency edges", () => {
    expect(parseDependencyEdges("child\tparent\n")).toEqual([
      { childTable: "child", parentTable: "parent" },
    ]);
    expect(() => parseDependencyEdges("broken")).toThrow("Invalid dependency row");
  });

  it("clamps negative estimated rows", () => {
    expect(
      parseTableMetadata('t\t-5\t10\t"id"\t"id"\ttarget."id" = stage."id"\t')[0]?.estimatedRows,
    ).toBe(0);
  });

  it("builds Neon apply SQL for upsert and delete", () => {
    const sql = buildNeonApplySql(tableA, true, "replica_sync_stage", true, "upsert");
    expect(sql.preCopySql).toContain('CREATE TEMP TABLE "replica_sync_stage"');
    expect(sql.copySql).toContain('COPY "replica_sync_stage" ("id", "name")');
    expect(sql.copySql).toContain("FORMAT csv");
    expect(sql.postCopySql).toContain(
      'CREATE INDEX "replica_sync_stage_pk" ON "replica_sync_stage" ("id")',
    );
    expect(sql.postCopySql).toContain(
      'INSERT INTO public."table_a" ("id", "name") OVERRIDING SYSTEM VALUE SELECT "id", "name"',
    );
    expect(sql.postCopySql).toContain('SELECT DISTINCT ON ("id") "id", "name"');
    expect(sql.postCopySql).toContain('ON CONFLICT ("id") DO UPDATE SET "name" = excluded."name"');
    expect(sql.postCopySql).toContain('DELETE FROM public."table_a" AS target');
    expect(sql.postCopySql).toContain("COMMIT;");
  });

  it("builds Neon apply SQL for replace mode", () => {
    const sql = buildNeonApplySql(tableA, true);
    expect(sql.postCopySql).toContain('TRUNCATE TABLE public."table_a"');
    expect(sql.postCopySql).toContain(
      'INSERT INTO public."table_a" ("id", "name") OVERRIDING SYSTEM VALUE SELECT "id", "name"\nFROM (',
    );
    expect(sql.postCopySql).toContain('SELECT DISTINCT ON ("id") "id", "name"');
    expect(sql.postCopySql).not.toContain("ON CONFLICT");
    expect(sql.postCopySql).not.toContain("DELETE FROM");
  });

  it("builds Neon apply SQL without update/delete when requested", () => {
    const sql = buildNeonApplySql(tableB, false, "replica_sync_stage", true, "upsert");
    expect(sql.preCopySql).toContain('public."weird""table"');
    expect(sql.postCopySql).toContain('ON CONFLICT ("id") DO NOTHING');
    expect(sql.postCopySql).not.toContain("DELETE FROM");
  });
});

describe("strategy resolver", () => {
  it("returns full-replace for small tables", () => {
    expect(
      resolveStrategy({
        rowCount: 100,
        hasUpdateChurn: false,
        timestampColumn: null,
        hasPrimaryKey: true,
        thresholds: defaultThresholds,
        mode: "auto",
      }),
    ).toBe("full-replace");
  });

  it("returns full-replace when forced", () => {
    expect(
      resolveStrategy({
        rowCount: 1_000_000,
        hasUpdateChurn: false,
        timestampColumn: "updated_at",
        hasPrimaryKey: true,
        thresholds: defaultThresholds,
        mode: "full",
      }),
    ).toBe("full-replace");
  });

  it("returns full-replace for tables without a primary key", () => {
    expect(
      resolveStrategy({
        rowCount: 1_000_000,
        hasUpdateChurn: false,
        timestampColumn: null,
        hasPrimaryKey: false,
        thresholds: defaultThresholds,
        mode: "auto",
      }),
    ).toBe("full-replace");
  });

  it("returns timestamp-incremental for high-churn tables with timestamp column", () => {
    expect(
      resolveStrategy({
        rowCount: 6_543_348,
        hasUpdateChurn: true,
        timestampColumn: "updated_at",
        hasPrimaryKey: true,
        thresholds: defaultThresholds,
        mode: "auto",
      }),
    ).toBe("timestamp-incremental");
  });

  it("returns timestamp-incremental for append-only with timestamp", () => {
    expect(
      resolveStrategy({
        rowCount: 3_215_898,
        hasUpdateChurn: false,
        timestampColumn: "update_timestamp",
        hasPrimaryKey: true,
        thresholds: defaultThresholds,
        mode: "auto",
      }),
    ).toBe("timestamp-incremental");
  });

  it("returns pk-incremental for static large tables without timestamp", () => {
    expect(
      resolveStrategy({
        rowCount: 2_854_555,
        hasUpdateChurn: false,
        timestampColumn: null,
        hasPrimaryKey: true,
        thresholds: defaultThresholds,
        mode: "auto",
      }),
    ).toBe("pk-incremental");
  });

  it("returns full-replace for high-churn tables without timestamp (safety)", () => {
    expect(
      resolveStrategy({
        rowCount: 5_000_000,
        hasUpdateChurn: true,
        timestampColumn: null,
        hasPrimaryKey: true,
        thresholds: defaultThresholds,
        mode: "auto",
      }),
    ).toBe("full-replace");
  });
});

describe("strategy mode parser", () => {
  it("returns auto for empty or unrecognized values", () => {
    expect(parseStrategyMode(undefined)).toBe("auto");
    expect(parseStrategyMode("")).toBe("auto");
    expect(parseStrategyMode("smart")).toBe("auto");
  });

  it("returns full for force-full overrides", () => {
    expect(parseStrategyMode("full")).toBe("full");
    expect(parseStrategyMode("force-full")).toBe("full");
    expect(parseStrategyMode("FULL")).toBe("full");
  });
});

describe("table profile SQL + parser", () => {
  it("builds profile SQL with selected tables filter", () => {
    const sql = buildTableProfileSql(["jvd_se", "nvd_se"]);
    expect(sql).toContain("with candidate as");
    expect(sql).toContain("c.relname in ('jvd_se','nvd_se')");
    expect(sql).toContain("pg_stat_user_tables");
    expect(sql).toContain("array_position");
  });

  it("parses table profiles with auto strategy resolution", () => {
    const output = [
      "jvd_se\t2854555\t0\tt\tdata_sakusei_nengappi",
      "race_entry_corner_features\t6543348\t2099345\tt\tupdated_at",
      "apd_se_jv\t1851831\t0\tt\tupdate_timestamp",
      "model_prediction_evaluations\t29\t0\tt\tevaluated_at",
      "no_pk_table\t100000\t0\tf\t",
    ].join("\n");
    const profiles = parseTableProfiles(output, defaultThresholds, "auto");
    expect(profiles).toEqual([
      {
        tableName: "jvd_se",
        rowCount: 2854555,
        hasUpdateChurn: false,
        timestampColumn: "data_sakusei_nengappi",
        hasPrimaryKey: true,
        strategy: "timestamp-incremental",
      },
      {
        tableName: "race_entry_corner_features",
        rowCount: 6543348,
        hasUpdateChurn: true,
        timestampColumn: "updated_at",
        hasPrimaryKey: true,
        strategy: "timestamp-incremental",
      },
      {
        tableName: "apd_se_jv",
        rowCount: 1851831,
        hasUpdateChurn: false,
        timestampColumn: "update_timestamp",
        hasPrimaryKey: true,
        strategy: "timestamp-incremental",
      },
      {
        tableName: "model_prediction_evaluations",
        rowCount: 29,
        hasUpdateChurn: false,
        timestampColumn: "evaluated_at",
        hasPrimaryKey: true,
        strategy: "full-replace",
      },
      {
        tableName: "no_pk_table",
        rowCount: 100000,
        hasUpdateChurn: false,
        timestampColumn: null,
        hasPrimaryKey: false,
        strategy: "full-replace",
      },
    ]);
  });

  it("forces full-replace when mode override is set", () => {
    const output = "jvd_se\t2854555\t0\tt\t";
    const profiles = parseTableProfiles(output, defaultThresholds, "full");
    expect(profiles[0]?.strategy).toBe("full-replace");
  });
});

describe("fingerprint SQL", () => {
  it("builds primary-key fingerprint SQL", () => {
    const sql = buildFingerprintSql(tableA);
    expect(sql).toContain("count(*)::text");
    expect(sql).toContain("max(");
    expect(sql).toContain('public."table_a"');
    expect(sql).toContain('"id"');
  });

  it("builds timestamp fingerprint SQL", () => {
    const sql = buildTimestampFingerprintSql(tableA, "updated_at");
    expect(sql).toContain('max("updated_at")::text');
    expect(sql).toContain('public."table_a"');
  });

  it("parses fingerprint output", () => {
    expect(parseFingerprintLine("1234\t2026-05-21 10:00:00")).toEqual({
      count: 1234,
      marker: "2026-05-21 10:00:00",
    });
    expect(parseFingerprintLine("0\t")).toEqual({ count: 0, marker: "" });
  });

  it("builds incremental COPY-from SQL with marker comparator", () => {
    const sql = buildIncrementalCopyFromSql(tableA, {
      keyExpression: pkExpression(tableA),
      neonMarker: "2026-05-21",
      comparator: ">",
    });
    expect(sql).toContain('COPY (SELECT "id", "name" FROM public."table_a"');
    expect(sql).toContain("where (\"id\")::text > '2026-05-21'");
  });

  it("builds inclusive incremental COPY-from SQL for date-only source markers", () => {
    const sql = buildIncrementalCopyFromSql(tableA, {
      keyExpression: timestampKeyExpression("data_sakusei_nengappi"),
      neonMarker: "20260522",
      comparator: incrementalComparatorForTimestampColumn("data_sakusei_nengappi"),
    });
    expect(sql).toContain("where (\"data_sakusei_nengappi\")::text >= '20260522'");
  });

  it("omits WHERE clause when neon marker is empty (empty target)", () => {
    const sql = buildIncrementalCopyFromSql(tableA, {
      keyExpression: timestampKeyExpression("updated_at"),
      neonMarker: "",
      comparator: ">",
    });
    expect(sql).toContain('COPY (SELECT "id", "name" FROM public."table_a"');
    expect(sql).not.toContain("where");
  });

  it("escapes single quotes in neon marker", () => {
    const sql = buildIncrementalCopyFromSql(tableA, {
      keyExpression: pkExpression(tableA),
      neonMarker: "o'brien",
      comparator: ">",
    });
    expect(sql).toContain("where (\"id\")::text > 'o''brien'");
  });
});

describe("incremental apply SQL", () => {
  it("builds upsert-only path with no truncate/delete", () => {
    const sql = buildIncrementalApplySql(tableA);
    expect(sql.preCopySql).toContain('CREATE TEMP TABLE "replica_sync_stage_inc"');
    expect(sql.postCopySql).toContain(
      'INSERT INTO public."table_a" ("id", "name") OVERRIDING SYSTEM VALUE SELECT "id", "name"',
    );
    expect(sql.postCopySql).toContain('ON CONFLICT ("id") DO UPDATE SET "name" = excluded."name"');
    expect(sql.postCopySql).not.toContain("TRUNCATE");
    expect(sql.postCopySql).not.toContain("DELETE FROM");
  });

  it("uses DO NOTHING when no non-PK columns to update", () => {
    const sql = buildIncrementalApplySql(tableB);
    expect(sql.postCopySql).toContain('ON CONFLICT ("id") DO NOTHING');
  });

  it("uses persistent stage table when temporary=false", () => {
    const sql = buildIncrementalApplySql(tableA, "custom_stage", false);
    expect(sql.preCopySql).toContain('DROP TABLE IF EXISTS public."custom_stage"');
    expect(sql.preCopySql).toContain('CREATE UNLOGGED TABLE public."custom_stage"');
    expect(sql.postCopySql).toContain("BEGIN;");
    expect(sql.postCopySql).toContain('DROP TABLE public."custom_stage"');
    expect(sql.cleanupSql).toBe('DROP TABLE IF EXISTS public."custom_stage";');
  });

  it("rolls back the implicit transaction for temporary stages", () => {
    const sql = buildIncrementalApplySql(tableA);
    expect(sql.cleanupSql).toBe("ROLLBACK;");
  });
});

describe("buildStageTableName", () => {
  it("returns the unmodified concatenation when full-replace name fits", () => {
    expect(buildStageTableName({ kind: "full", pid: 11052, tableName: "jvd_se" })).toBe(
      "replica_sync_stage_11052_jvd_se",
    );
  });

  it("returns the unmodified concatenation when incremental name fits", () => {
    expect(buildStageTableName({ kind: "incremental", pid: 11052, tableName: "jvd_se" })).toBe(
      "replica_sync_stage_inc_11052_jvd_se",
    );
  });

  it("returns the unmodified concatenation when reincremental name fits", () => {
    expect(buildStageTableName({ kind: "reincremental", pid: 11052, tableName: "jvd_se" })).toBe(
      "replica_sync_stage_reinc_11052_jvd_se",
    );
  });

  it("sanitises invalid identifier characters into underscores", () => {
    expect(buildStageTableName({ kind: "full", pid: 42, tableName: "weird.table-name" })).toBe(
      "replica_sync_stage_42_weird_table_name",
    );
  });

  it("truncates and hashes the long production-collision name to stay within 60 chars", () => {
    const name = buildStageTableName({
      kind: "full",
      pid: 11052,
      tableName: "race_finish_position_model_predictions",
    });
    expect(name).toBe("replica_sync_stage_11052_race_finish_position_model_fdb19d20");
  });

  it("keeps the truncated stage name + _pk suffix within PostgreSQL NAMEDATALEN", () => {
    const name = buildStageTableName({
      kind: "full",
      pid: 11052,
      tableName: "race_finish_position_model_predictions",
    });
    expect(`${name}_pk`.length).toBe(63);
  });

  it("produces deterministic hashed output across calls", () => {
    const first = buildStageTableName({
      kind: "full",
      pid: 11052,
      tableName: "race_finish_position_model_predictions",
    });
    const second = buildStageTableName({
      kind: "full",
      pid: 11052,
      tableName: "race_finish_position_model_predictions",
    });
    expect(first).toBe(second);
  });

  it("hashes incremental prefix collision name to stay within 60 chars", () => {
    const name = buildStageTableName({
      kind: "incremental",
      pid: 99999,
      tableName: "race_finish_position_model_predictions",
    });
    expect(name).toBe("replica_sync_stage_inc_99999_race_finish_position_m_fdb19d20");
  });

  it("hashes reincremental prefix collision name to stay within 60 chars", () => {
    const name = buildStageTableName({
      kind: "reincremental",
      pid: 99999,
      tableName: "race_finish_position_model_predictions",
    });
    expect(name).toBe("replica_sync_stage_reinc_99999_race_finish_position_fdb19d20");
  });

  it("differentiates hashes for distinct long source tables", () => {
    const left = buildStageTableName({
      kind: "full",
      pid: 11052,
      tableName: "race_finish_position_model_predictions_variant_a",
    });
    const right = buildStageTableName({
      kind: "full",
      pid: 11052,
      tableName: "race_finish_position_model_predictions_variant_b",
    });
    expect(left === right).toBe(false);
  });

  it("keeps the result ≤ 60 chars even when the table name is far longer than the budget", () => {
    const name = buildStageTableName({
      kind: "reincremental",
      pid: 999999,
      tableName: "a".repeat(200),
    });
    expect(name.length).toBe(60);
  });
});

describe("incremental copy edge cases", () => {
  it("returns COPY without WHERE when neonMarker is empty", () => {
    const sql = buildIncrementalCopyFromSql(tableA, {
      keyExpression: timestampKeyExpression("updated_at"),
      neonMarker: "",
      comparator: ">",
    });
    expect(sql).not.toContain("where");
  });

  it("falls back to pk-incremental key when no timestamp column", () => {
    const expr = pkExpression(tableB);
    expect(expr).toBe('("id")::text');
  });

  it("refreshes only inclusive timestamp marker columns", () => {
    expect(shouldRefreshInclusiveIncrementalMarker("data_sakusei_nengappi")).toBe(true);
    expect(shouldRefreshInclusiveIncrementalMarker("updated_at")).toBe(false);
    expect(shouldRefreshInclusiveIncrementalMarker(null)).toBe(false);
  });
});

describe("strategy mode env edge", () => {
  it("treats unrecognized strategy mode as auto", () => {
    expect(parseStrategyMode("incremental")).toBe("auto");
    expect(parseStrategyMode(" ")).toBe("auto");
  });
});

describe("strategy threshold edge", () => {
  it("crosses small-table boundary correctly", () => {
    const justBelow = resolveStrategy({
      rowCount: 10_000,
      hasUpdateChurn: false,
      timestampColumn: null,
      hasPrimaryKey: true,
      thresholds: defaultThresholds,
      mode: "auto",
    });
    const justAbove = resolveStrategy({
      rowCount: 10_001,
      hasUpdateChurn: false,
      timestampColumn: null,
      hasPrimaryKey: true,
      thresholds: defaultThresholds,
      mode: "auto",
    });
    expect(justBelow).toBe("full-replace");
    expect(justAbove).toBe("pk-incremental");
  });
});

describe("progress calculations", () => {
  it("calculates ETA from synced rows", () => {
    expect(calculateEtaSeconds(25, 100, 10)).toBe(30);
    expect(calculateEtaSeconds(0, 100, 10)).toBe(0);
    expect(calculateEtaSeconds(100, 100, 10)).toBe(0);
  });
});

describe("dependency planning", () => {
  it("orders tables by dependency levels and row count", () => {
    const child = { ...tableA, tableName: "child", estimatedRows: 10 };
    const parent = { ...tableA, tableName: "parent", estimatedRows: 1 };
    const other = { ...tableA, tableName: "other", estimatedRows: 100 };

    expect(
      buildDependencyPlan(
        [child, parent, other],
        [{ childTable: "child", parentTable: "parent" }],
      ).map((level) => level.map((table) => table.tableName)),
    ).toEqual([["other", "parent"], ["child"]]);
  });

  it("ignores dependencies outside the selected table set", () => {
    const plan = buildDependencyPlan([tableA], [{ childTable: "table_a", parentTable: "missing" }]);
    expect(plan).toEqual([[tableA]]);
  });

  it("ignores self-edges instead of treating them as cycles", () => {
    const plan = buildDependencyPlan([tableA], [{ childTable: "table_a", parentTable: "table_a" }]);
    expect(plan).toEqual([[tableA]]);
  });

  it("rejects cyclic dependencies", () => {
    expect(() =>
      buildDependencyPlan(
        [tableA, { ...tableA, tableName: "table_c" }],
        [
          { childTable: "table_a", parentTable: "table_c" },
          { childTable: "table_c", parentTable: "table_a" },
        ],
      ),
    ).toThrow("Circular table dependencies detected");
  });

  it("resolves automatic concurrency from level size", () => {
    expect(resolveConcurrency([tableA], "auto")).toBe(1);
    expect(resolveConcurrency([tableA, tableB], 8)).toBe(2);
    expect(resolveConcurrency([{ ...tableA, estimatedRows: 3_000_000 }, tableB], "auto")).toBe(2);
    expect(
      resolveConcurrency(
        [
          { ...tableA, estimatedRows: 600_000 },
          { ...tableB, estimatedRows: 600_000 },
          { ...tableA, tableName: "c", estimatedRows: 1 },
        ],
        "auto",
      ),
    ).toBe(3);
    expect(
      resolveConcurrency(
        [
          { ...tableA, estimatedRows: 60_000 },
          { ...tableB, estimatedRows: 60_000 },
          { ...tableA, tableName: "c", estimatedRows: 1 },
          { ...tableA, tableName: "d", estimatedRows: 1 },
          { ...tableA, tableName: "e", estimatedRows: 1 },
        ],
        "auto",
      ),
    ).toBe(4);
    expect(resolveConcurrency([], "auto")).toBe(0);
  });
});

describe("Neon warm wait", () => {
  it("retries until Neon is ready", async () => {
    let now = 0;
    let attempts = 0;
    const events: ProgressEvent[] = [];

    await waitForNeonReady(
      {
        concurrency: 2,
        deleteMissingRows: true,
        applyMode: "replace",
        strategyMode: "auto",
        strategyThresholds: { smallTableMaxRows: 10000, updateChurnMinTuples: 1000 },
        neonConnectTimeoutSeconds: 20,
        neonConnectRetrySeconds: 5,
      },
      {
        nowSeconds: () => now,
        sleep: async (milliseconds) => {
          now += milliseconds / 1000;
        },
        checkNeonReady: async () => {
          attempts += 1;
          return attempts === 3;
        },
        report: (event) => events.push(event),
      },
    );

    expect(attempts).toBe(3);
    expect(events.map((event) => event.type)).toEqual([
      "neon-wait-start",
      "neon-wait-retry",
      "neon-wait-retry",
      "neon-ready",
    ]);
  });

  it("times out when Neon never becomes ready", async () => {
    let now = 0;

    await expect(
      waitForNeonReady(
        {
          concurrency: 2,
          deleteMissingRows: true,
          applyMode: "replace",
          strategyMode: "auto",
          strategyThresholds: { smallTableMaxRows: 10000, updateChurnMinTuples: 1000 },
          neonConnectTimeoutSeconds: 10,
          neonConnectRetrySeconds: 5,
        },
        {
          nowSeconds: () => now,
          sleep: async (milliseconds) => {
            now += milliseconds / 1000;
          },
          checkNeonReady: async () => false,
          report: () => undefined,
        },
      ),
    ).rejects.toThrow("Timed out waiting for Neon after 10s");
  });
});

describe("parallel push runner", () => {
  it("runs tables with bounded concurrency and reports aggregate progress", async () => {
    let now = 0;
    let active = 0;
    let maxActive = 0;
    const events: ProgressEvent[] = [];
    const synced: string[] = [];
    const resolvers: Array<() => void> = [];

    const run = runPushSync(
      [tableA, tableB, { ...tableA, tableName: "table_c", estimatedRows: 25, estimatedBytes: 25 }],
      {
        concurrency: 2,
        deleteMissingRows: true,
        applyMode: "replace",
        strategyMode: "auto",
        strategyThresholds: { smallTableMaxRows: 10000, updateChurnMinTuples: 1000 },
        neonConnectTimeoutSeconds: 10,
        neonConnectRetrySeconds: 1,
      },
      {
        nowSeconds: () => now,
        sleep: async () => undefined,
        checkNeonReady: async () => true,
        report: (event) => events.push(event),
        syncTable: async (table) => {
          active += 1;
          maxActive = Math.max(maxActive, active);
          synced.push(table.tableName);
          await new Promise<void>((resolve) => resolvers.push(resolve));
          now += 2;
          active -= 1;
        },
      },
    );

    await Promise.resolve();
    await Promise.resolve();
    expect(maxActive).toBe(2);
    expect(synced).toEqual(["table_a", 'weird"table']);

    resolvers.shift()?.();
    await Promise.resolve();
    resolvers.shift()?.();
    await Promise.resolve();
    expect(synced).toContain("table_c");
    resolvers.shift()?.();

    await run;

    expect(events[0]).toEqual({
      type: "start",
      totalTables: 3,
      totalEstimatedRows: 175,
      dependencyLevels: 1,
      concurrency: 2,
    });
    expect(events.some((event) => event.type === "level-start")).toBe(true);
    expect(events.some((event) => event.type === "complete")).toBe(true);
    const doneEvents = events.filter((event) => event.type === "table-done");
    expect(doneEvents).toHaveLength(3);
    expect(maxActive).toBeLessThanOrEqual(2);
  });

  it("waits for parent dependency levels before syncing children", async () => {
    const synced: string[] = [];

    await runPushSync(
      [tableA, { ...tableA, tableName: "child", estimatedRows: 1 }],
      {
        concurrency: "auto",
        deleteMissingRows: true,
        applyMode: "replace",
        strategyMode: "auto",
        strategyThresholds: { smallTableMaxRows: 10000, updateChurnMinTuples: 1000 },
        neonConnectTimeoutSeconds: 1,
        neonConnectRetrySeconds: 1,
      },
      {
        nowSeconds: () => 0,
        sleep: async () => undefined,
        checkNeonReady: async () => true,
        syncTable: async (table) => {
          synced.push(table.tableName);
        },
        report: () => undefined,
      },
      [{ childTable: "child", parentTable: "table_a" }],
    );

    expect(synced).toEqual(["table_a", "child"]);
  });

  it("uses persistent Neon apply stage with replace mode", () => {
    const sql = buildNeonApplySql(tableA, false, "stage_persist", false, "replace");
    expect(sql.preCopySql).toContain('DROP TABLE IF EXISTS public."stage_persist";');
    expect(sql.preCopySql).toContain('CREATE UNLOGGED TABLE public."stage_persist"');
    expect(sql.preCopySql).not.toContain("BEGIN;");
    expect(sql.postCopySql).toContain("BEGIN;");
    expect(sql.postCopySql).toContain('TRUNCATE TABLE public."table_a"');
    expect(sql.postCopySql).toContain('DROP TABLE public."stage_persist"');
    expect(sql.cleanupSql).toBe('DROP TABLE IF EXISTS public."stage_persist";');
  });

  it("places DROP TABLE IF EXISTS before CREATE UNLOGGED TABLE in non-temporary preCopySql for self-heal on retry", () => {
    const sql = buildNeonApplySql(tableA, true, "replica_sync_stage_x", false, "upsert");
    const dropIndex = sql.preCopySql.indexOf('DROP TABLE IF EXISTS public."replica_sync_stage_x";');
    const createIndex = sql.preCopySql.indexOf(
      'CREATE UNLOGGED TABLE public."replica_sync_stage_x"',
    );
    expect(dropIndex).toBeGreaterThanOrEqual(0);
    expect(createIndex).toBeGreaterThan(dropIndex);
  });

  it("uses persistent Neon apply stage with upsert mode and skips delete when disabled", () => {
    const sql = buildNeonApplySql(tableA, false, "stage_persist", false, "upsert");
    expect(sql.postCopySql).toContain('ON CONFLICT ("id") DO UPDATE SET "name" = excluded."name"');
    expect(sql.postCopySql).not.toContain("DELETE FROM");
  });

  it("parses a malformed table profile line by falling back to defaults for missing cells", () => {
    const profiles = parseTableProfiles("orphan_line", defaultThresholds, "auto");
    expect(profiles).toEqual([
      {
        tableName: "orphan_line",
        rowCount: 0,
        hasUpdateChurn: false,
        timestampColumn: null,
        hasPrimaryKey: false,
        strategy: "full-replace",
      },
    ]);
  });

  it("treats an empty fingerprint line as zero count and empty marker", () => {
    expect(parseFingerprintLine("")).toEqual({ count: 0, marker: "" });
  });

  it("returns the inclusive comparator for whitelisted timestamp columns", () => {
    expect(incrementalComparatorForTimestampColumn("data_sakusei_nengappi")).toBe(">=");
    expect(incrementalComparatorForTimestampColumn("updated_at")).toBe(">");
    expect(incrementalComparatorForTimestampColumn(null)).toBe(">");
  });

  it("orders dependency-plan tables by row count when bytes tie", () => {
    const equalA = { ...tableA, tableName: "a", estimatedRows: 200, estimatedBytes: 100 };
    const equalB = { ...tableA, tableName: "b", estimatedRows: 100, estimatedBytes: 100 };
    const equalC = { ...tableA, tableName: "c", estimatedRows: 200, estimatedBytes: 100 };
    expect(
      buildDependencyPlan([equalB, equalC, equalA], []).map((level) =>
        level.map((table) => table.tableName),
      ),
    ).toEqual([["a", "c", "b"]]);
  });

  it("rejects empty table lists", async () => {
    await expect(
      runPushSync(
        [],
        {
          concurrency: 1,
          deleteMissingRows: true,
          applyMode: "replace",
          strategyMode: "auto",
          strategyThresholds: { smallTableMaxRows: 10000, updateChurnMinTuples: 1000 },
          neonConnectTimeoutSeconds: 1,
          neonConnectRetrySeconds: 1,
        },
        {
          nowSeconds: () => 0,
          sleep: async () => undefined,
          checkNeonReady: async () => true,
          syncTable: async () => undefined,
          report: () => undefined,
        },
      ),
    ).rejects.toThrow("No primary-key tables matched");
  });
});

describe("buildNeonPsqlArgs", () => {
  it("defaults to container exec against the long-lived local container", () => {
    expect(
      buildNeonPsqlArgs({
        neonUrl: "postgresql://user:pass@neon.example/db",
        containerName: undefined,
      }),
    ).toStrictEqual([
      "exec",
      "-i",
      "horse-racing-local-postgresql",
      "psql",
      "postgresql://user:pass@neon.example/db",
    ]);
  });

  it("uses the default container name from the exported constant", () => {
    expect(DEFAULT_NEON_PSQL_CONTAINER).toBe("horse-racing-local-postgresql");
  });

  it("exports LOCAL_CONTAINER_NAME matching the long-lived Apple Container", () => {
    expect(LOCAL_CONTAINER_NAME).toBe("horse-racing-local-postgresql");
  });

  it("respects a custom container name", () => {
    expect(
      buildNeonPsqlArgs({
        neonUrl: "postgresql://user:pass@neon.example/db",
        containerName: "my-psql-sidecar",
      }),
    ).toStrictEqual([
      "exec",
      "-i",
      "my-psql-sidecar",
      "psql",
      "postgresql://user:pass@neon.example/db",
    ]);
  });

  it("falls back to default when container name is an empty string", () => {
    expect(
      buildNeonPsqlArgs({
        neonUrl: "postgresql://user:pass@neon.example/db",
        containerName: "",
      }),
    ).toStrictEqual([
      "exec",
      "-i",
      "horse-racing-local-postgresql",
      "psql",
      "postgresql://user:pass@neon.example/db",
    ]);
  });

  it("appends extra args after the connection URL", () => {
    expect(
      buildNeonPsqlArgs({
        neonUrl: "postgresql://user:pass@neon.example/db",
        containerName: undefined,
        extraArgs: ["-v", "ON_ERROR_STOP=1", "-qAtc", "select 1"],
      }),
    ).toStrictEqual([
      "exec",
      "-i",
      "horse-racing-local-postgresql",
      "psql",
      "postgresql://user:pass@neon.example/db",
      "-v",
      "ON_ERROR_STOP=1",
      "-qAtc",
      "select 1",
    ]);
  });

  it("throws when neonUrl is undefined", () => {
    expect(() => buildNeonPsqlArgs({ neonUrl: undefined, containerName: undefined })).toThrow(
      "NEON_DIRECT_DATABASE_URL is required",
    );
  });

  it("throws when neonUrl is an empty string", () => {
    expect(() => buildNeonPsqlArgs({ neonUrl: "", containerName: undefined })).toThrow(
      "NEON_DIRECT_DATABASE_URL is required",
    );
  });
});

describe("resolvePositiveIntegerEnv", () => {
  it("returns the override when provided", () => {
    expect(resolvePositiveIntegerEnv(7, "3", 5)).toBe(7);
  });

  it("falls back to default when env is undefined", () => {
    expect(resolvePositiveIntegerEnv(null, undefined, 5)).toBe(5);
  });

  it("falls back to default when env is an empty string", () => {
    expect(resolvePositiveIntegerEnv(null, "", 5)).toBe(5);
  });

  it("parses a positive integer from env", () => {
    expect(resolvePositiveIntegerEnv(null, "8", 5)).toBe(8);
  });

  it("falls back when env value is zero", () => {
    expect(resolvePositiveIntegerEnv(null, "0", 5)).toBe(5);
  });

  it("falls back when env value is negative", () => {
    expect(resolvePositiveIntegerEnv(null, "-2", 5)).toBe(5);
  });

  it("falls back when env value is not an integer", () => {
    expect(resolvePositiveIntegerEnv(null, "1.5", 5)).toBe(5);
  });

  it("falls back when env value is not numeric", () => {
    expect(resolvePositiveIntegerEnv(null, "abc", 5)).toBe(5);
  });
});

describe("resolveNonNegativeSecondsEnv", () => {
  it("returns the default in milliseconds when env is undefined", () => {
    expect(resolveNonNegativeSecondsEnv(undefined, 5)).toBe(5000);
  });

  it("returns the default in milliseconds when env is empty", () => {
    expect(resolveNonNegativeSecondsEnv("", 5)).toBe(5000);
  });

  it("parses seconds to milliseconds for a positive value", () => {
    expect(resolveNonNegativeSecondsEnv("3", 5)).toBe(3000);
  });

  it("allows zero seconds", () => {
    expect(resolveNonNegativeSecondsEnv("0", 5)).toBe(0);
  });

  it("supports fractional seconds", () => {
    expect(resolveNonNegativeSecondsEnv("0.5", 5)).toBe(500);
  });

  it("falls back when env value is negative", () => {
    expect(resolveNonNegativeSecondsEnv("-1", 5)).toBe(5000);
  });

  it("falls back when env value is not numeric", () => {
    expect(resolveNonNegativeSecondsEnv("abc", 5)).toBe(5000);
  });
});

describe("runWithRetry", () => {
  it("returns the result without invoking callbacks when the operation succeeds first", async () => {
    const succeededCalls: RetryAttemptInfo[] = [];
    const failedCalls: RetryFailureInfo[] = [];
    const gaveUpCalls: RetryGaveUpInfo[] = [];
    const sleepCalls: number[] = [];
    const result = await runWithRetry(async () => "ok", {
      maxAttempts: 3,
      retryDelayMs: 10,
      sleep: async (milliseconds) => {
        sleepCalls.push(milliseconds);
      },
      onAttemptFailed: (info) => failedCalls.push(info),
      onGaveUp: (info) => gaveUpCalls.push(info),
      onRetrySucceeded: (info) => succeededCalls.push(info),
    });
    expect(result).toBe("ok");
    expect(failedCalls).toStrictEqual([]);
    expect(gaveUpCalls).toStrictEqual([]);
    expect(succeededCalls).toStrictEqual([]);
    expect(sleepCalls).toStrictEqual([]);
  });

  it("retries once and reports success on attempt 2", async () => {
    const succeededCalls: RetryAttemptInfo[] = [];
    const failedCalls: RetryFailureInfo[] = [];
    const sleepCalls: number[] = [];
    const error = new Error("boom");
    let attempts = 0;
    const result = await runWithRetry(
      async () => {
        attempts += 1;
        if (attempts === 1) throw error;
        return "second";
      },
      {
        maxAttempts: 3,
        retryDelayMs: 250,
        sleep: async (milliseconds) => {
          sleepCalls.push(milliseconds);
        },
        onAttemptFailed: (info) => failedCalls.push(info),
        onRetrySucceeded: (info) => succeededCalls.push(info),
      },
    );
    expect(result).toBe("second");
    expect(attempts).toBe(2);
    expect(failedCalls).toStrictEqual([{ attempt: 1, maxAttempts: 3, error, retryDelayMs: 250 }]);
    expect(succeededCalls).toStrictEqual([{ attempt: 2, maxAttempts: 3 }]);
    expect(sleepCalls).toStrictEqual([250]);
  });

  it("gives up after exhausting maxAttempts and rethrows the original error", async () => {
    const failedCalls: RetryFailureInfo[] = [];
    const gaveUpCalls: RetryGaveUpInfo[] = [];
    const sleepCalls: number[] = [];
    const error = new Error("always fails");
    let attempts = 0;
    await expect(
      runWithRetry(
        async () => {
          attempts += 1;
          throw error;
        },
        {
          maxAttempts: 3,
          retryDelayMs: 50,
          sleep: async (milliseconds) => {
            sleepCalls.push(milliseconds);
          },
          onAttemptFailed: (info) => failedCalls.push(info),
          onGaveUp: (info) => gaveUpCalls.push(info),
        },
      ),
    ).rejects.toBe(error);
    expect(attempts).toBe(3);
    expect(failedCalls).toStrictEqual([
      { attempt: 1, maxAttempts: 3, error, retryDelayMs: 50 },
      { attempt: 2, maxAttempts: 3, error, retryDelayMs: 50 },
    ]);
    expect(gaveUpCalls).toStrictEqual([{ attempt: 3, maxAttempts: 3, error }]);
    expect(sleepCalls).toStrictEqual([50, 50]);
  });

  it("throws immediately without sleeping when maxAttempts is 1", async () => {
    const failedCalls: RetryFailureInfo[] = [];
    const gaveUpCalls: RetryGaveUpInfo[] = [];
    const sleepCalls: number[] = [];
    const error = new Error("once");
    await expect(
      runWithRetry(
        async () => {
          throw error;
        },
        {
          maxAttempts: 1,
          retryDelayMs: 1000,
          sleep: async (milliseconds) => {
            sleepCalls.push(milliseconds);
          },
          onAttemptFailed: (info) => failedCalls.push(info),
          onGaveUp: (info) => gaveUpCalls.push(info),
        },
      ),
    ).rejects.toBe(error);
    expect(failedCalls).toStrictEqual([]);
    expect(gaveUpCalls).toStrictEqual([{ attempt: 1, maxAttempts: 1, error }]);
    expect(sleepCalls).toStrictEqual([]);
  });

  it("works without any callbacks supplied", async () => {
    let attempts = 0;
    const sleepCalls: number[] = [];
    const result = await runWithRetry(
      async () => {
        attempts += 1;
        if (attempts === 1) throw new Error("retry me");
        return 42;
      },
      {
        maxAttempts: 2,
        retryDelayMs: 5,
        sleep: async (milliseconds) => {
          sleepCalls.push(milliseconds);
        },
      },
    );
    expect(result).toBe(42);
    expect(attempts).toBe(2);
    expect(sleepCalls).toStrictEqual([5]);
  });

  it("wraps a non-Error throw and passes it through unchanged", async () => {
    const gaveUpCalls: RetryGaveUpInfo[] = [];
    const sleepCalls: number[] = [];
    await expect(
      runWithRetry(
        async () => {
          throw "string failure";
        },
        {
          maxAttempts: 1,
          retryDelayMs: 1,
          sleep: async (milliseconds) => {
            sleepCalls.push(milliseconds);
          },
          onGaveUp: (info) => gaveUpCalls.push(info),
        },
      ),
    ).rejects.toBe("string failure");
    expect(gaveUpCalls).toStrictEqual([{ attempt: 1, maxAttempts: 1, error: "string failure" }]);
    expect(sleepCalls).toStrictEqual([]);
  });

  it("uses computeDelayMs to derive per-attempt delay and reports it in failure info", async () => {
    const failedCalls: RetryFailureInfo[] = [];
    const sleepCalls: number[] = [];
    const computedAttempts: number[] = [];
    let attempts = 0;
    const result = await runWithRetry(
      async () => {
        attempts += 1;
        if (attempts < 3) throw new Error("flaky");
        return "ok";
      },
      {
        maxAttempts: 5,
        retryDelayMs: 999,
        sleep: async (milliseconds) => {
          sleepCalls.push(milliseconds);
        },
        computeDelayMs: (attempt) => {
          computedAttempts.push(attempt);
          return attempt * 100;
        },
        onAttemptFailed: (info) => failedCalls.push(info),
      },
    );
    expect(result).toBe("ok");
    expect(attempts).toBe(3);
    expect(computedAttempts).toStrictEqual([1, 2]);
    expect(sleepCalls).toStrictEqual([100, 200]);
    expect(failedCalls.map((info) => info.retryDelayMs)).toStrictEqual([100, 200]);
  });

  it("succeeds on the final allowed attempt and reports retry succeeded with that attempt number", async () => {
    const succeededCalls: RetryAttemptInfo[] = [];
    const failedCalls: RetryFailureInfo[] = [];
    const gaveUpCalls: RetryGaveUpInfo[] = [];
    const sleepCalls: number[] = [];
    let attempts = 0;
    const result = await runWithRetry(
      async () => {
        attempts += 1;
        if (attempts < 4) throw new Error("flaky");
        return "final-ok";
      },
      {
        maxAttempts: 4,
        retryDelayMs: 10,
        sleep: async (milliseconds) => {
          sleepCalls.push(milliseconds);
        },
        onAttemptFailed: (info) => failedCalls.push(info),
        onGaveUp: (info) => gaveUpCalls.push(info),
        onRetrySucceeded: (info) => succeededCalls.push(info),
      },
    );
    expect(result).toBe("final-ok");
    expect(attempts).toBe(4);
    expect(succeededCalls).toStrictEqual([{ attempt: 4, maxAttempts: 4 }]);
    expect(failedCalls.map((info) => info.attempt)).toStrictEqual([1, 2, 3]);
    expect(gaveUpCalls).toStrictEqual([]);
    expect(sleepCalls).toStrictEqual([10, 10, 10]);
  });

  it("supports many retries beyond stack-friendly counts via async recursion", async () => {
    const sleepCalls: number[] = [];
    let attempts = 0;
    const result = await runWithRetry(
      async () => {
        attempts += 1;
        if (attempts < 25) throw new Error("noisy");
        return attempts;
      },
      {
        maxAttempts: 30,
        retryDelayMs: 1,
        sleep: async (milliseconds) => {
          sleepCalls.push(milliseconds);
        },
      },
    );
    expect(result).toBe(25);
    expect(sleepCalls).toHaveLength(24);
  });
});

describe("computeBackoffDelayMs", () => {
  const baseConfig: RetryBackoffConfig = {
    baseMs: 5000,
    maxMs: 60_000,
    jitterMs: 0,
  };

  it("returns the base delay on the first attempt without jitter", () => {
    expect(computeBackoffDelayMs(1, baseConfig)).toBe(5000);
  });

  it("doubles the delay exponentially on later attempts", () => {
    expect(computeBackoffDelayMs(2, baseConfig)).toBe(10_000);
    expect(computeBackoffDelayMs(3, baseConfig)).toBe(20_000);
    expect(computeBackoffDelayMs(4, baseConfig)).toBe(40_000);
  });

  it("caps the delay at maxMs", () => {
    expect(computeBackoffDelayMs(10, baseConfig)).toBe(60_000);
  });

  it("normalizes attempt values below 1 to a single base delay", () => {
    expect(computeBackoffDelayMs(0, baseConfig)).toBe(5000);
    expect(computeBackoffDelayMs(-3, baseConfig)).toBe(5000);
  });

  it("floors fractional attempt values before computing the exponent", () => {
    expect(computeBackoffDelayMs(2.9, baseConfig)).toBe(10_000);
  });

  it("adds deterministic jitter when a custom random source is supplied", () => {
    expect(
      computeBackoffDelayMs(1, {
        baseMs: 1000,
        maxMs: 60_000,
        jitterMs: 1000,
        random: () => 0.25,
      }),
    ).toBe(1250);
  });

  it("ignores jitter when jitterMs is zero", () => {
    expect(
      computeBackoffDelayMs(1, {
        baseMs: 2000,
        maxMs: 60_000,
        jitterMs: 0,
        random: () => 0.999,
      }),
    ).toBe(2000);
  });

  it("uses Math.random by default when no random source is supplied", () => {
    const value = computeBackoffDelayMs(1, {
      baseMs: 1000,
      maxMs: 60_000,
      jitterMs: 500,
    });
    expect(value).toBeGreaterThanOrEqual(1000);
    expect(value).toBeLessThanOrEqual(1500);
  });

  it("clamps jitter overflow at maxMs", () => {
    expect(
      computeBackoffDelayMs(1, {
        baseMs: 4000,
        maxMs: 4200,
        jitterMs: 1000,
        random: () => 0.9,
      }),
    ).toBe(4200);
  });

  it("returns zero when base is zero and jitter is zero", () => {
    expect(
      computeBackoffDelayMs(5, {
        baseMs: 0,
        maxMs: 60_000,
        jitterMs: 0,
      }),
    ).toBe(0);
  });

  it("treats negative jitterMs as no jitter", () => {
    expect(
      computeBackoffDelayMs(1, {
        baseMs: 1000,
        maxMs: 60_000,
        jitterMs: -500,
        random: () => 0.5,
      }),
    ).toBe(1000);
  });
});

describe("resolveRetryBackoffConfig", () => {
  it("returns documented defaults when no env vars are set", () => {
    expect(resolveRetryBackoffConfig({})).toStrictEqual({
      baseMs: 5000,
      maxMs: 60_000,
      jitterMs: 1000,
    });
  });

  it("reads the explicit base and max env vars", () => {
    expect(
      resolveRetryBackoffConfig({
        REPLICA_SYNC_RETRY_BASE_DELAY_SECONDS: "3",
        REPLICA_SYNC_RETRY_MAX_DELAY_SECONDS: "30",
      }),
    ).toStrictEqual({
      baseMs: 3000,
      maxMs: 30_000,
      jitterMs: 1000,
    });
  });

  it("falls back to the legacy REPLICA_SYNC_RETRY_DELAY_SECONDS when base is unset", () => {
    expect(
      resolveRetryBackoffConfig({
        REPLICA_SYNC_RETRY_DELAY_SECONDS: "7",
      }),
    ).toStrictEqual({
      baseMs: 7000,
      maxMs: 60_000,
      jitterMs: 1000,
    });
  });

  it("prefers the explicit base over the legacy delay env when both are set", () => {
    expect(
      resolveRetryBackoffConfig({
        REPLICA_SYNC_RETRY_BASE_DELAY_SECONDS: "2",
        REPLICA_SYNC_RETRY_DELAY_SECONDS: "11",
      }),
    ).toStrictEqual({
      baseMs: 2000,
      maxMs: 60_000,
      jitterMs: 1000,
    });
  });

  it("ignores empty env strings and uses defaults", () => {
    expect(
      resolveRetryBackoffConfig({
        REPLICA_SYNC_RETRY_BASE_DELAY_SECONDS: "",
        REPLICA_SYNC_RETRY_MAX_DELAY_SECONDS: "",
        REPLICA_SYNC_RETRY_DELAY_SECONDS: "",
      }),
    ).toStrictEqual({
      baseMs: 5000,
      maxMs: 60_000,
      jitterMs: 1000,
    });
  });

  it("ignores negative or non-numeric values and uses defaults", () => {
    expect(
      resolveRetryBackoffConfig({
        REPLICA_SYNC_RETRY_BASE_DELAY_SECONDS: "-1",
        REPLICA_SYNC_RETRY_MAX_DELAY_SECONDS: "abc",
      }),
    ).toStrictEqual({
      baseMs: 5000,
      maxMs: 60_000,
      jitterMs: 1000,
    });
  });

  it("clamps the configured max so it is never less than base", () => {
    expect(
      resolveRetryBackoffConfig({
        REPLICA_SYNC_RETRY_BASE_DELAY_SECONDS: "30",
        REPLICA_SYNC_RETRY_MAX_DELAY_SECONDS: "5",
      }),
    ).toStrictEqual({
      baseMs: 30_000,
      maxMs: 30_000,
      jitterMs: 1000,
    });
  });

  it("accepts zero as an explicit base seconds value", () => {
    expect(
      resolveRetryBackoffConfig({
        REPLICA_SYNC_RETRY_BASE_DELAY_SECONDS: "0",
        REPLICA_SYNC_RETRY_MAX_DELAY_SECONDS: "10",
      }),
    ).toStrictEqual({
      baseMs: 0,
      maxMs: 10_000,
      jitterMs: 1000,
    });
  });

  it("accepts zero seconds for both base and max", () => {
    expect(
      resolveRetryBackoffConfig({
        REPLICA_SYNC_RETRY_BASE_DELAY_SECONDS: "0",
        REPLICA_SYNC_RETRY_MAX_DELAY_SECONDS: "0",
      }),
    ).toStrictEqual({
      baseMs: 0,
      maxMs: 0,
      jitterMs: 1000,
    });
  });

  it("supports fractional seconds for both base and max", () => {
    expect(
      resolveRetryBackoffConfig({
        REPLICA_SYNC_RETRY_BASE_DELAY_SECONDS: "0.5",
        REPLICA_SYNC_RETRY_MAX_DELAY_SECONDS: "1.5",
      }),
    ).toStrictEqual({
      baseMs: 500,
      maxMs: 1500,
      jitterMs: 1000,
    });
  });

  it("ignores the legacy delay env when it is negative and falls back to default base", () => {
    expect(
      resolveRetryBackoffConfig({
        REPLICA_SYNC_RETRY_DELAY_SECONDS: "-2",
      }),
    ).toStrictEqual({
      baseMs: 5000,
      maxMs: 60_000,
      jitterMs: 1000,
    });
  });
});

describe("resolveVerifyMismatchPolicy", () => {
  it("returns documented defaults when no env vars are set", () => {
    expect(resolveVerifyMismatchPolicy({})).toStrictEqual({
      thresholdRows: 10,
      largeTableRows: 100_000,
      forceFullReplace: false,
      reincrementalMaxDiffPercent: 1,
    });
  });

  it("reads valid threshold env vars", () => {
    expect(
      resolveVerifyMismatchPolicy({
        REPLICA_VERIFY_MISMATCH_THRESHOLD_ROWS: "25",
        REPLICA_VERIFY_MISMATCH_LARGE_TABLE_ROWS: "500000",
        REPLICA_VERIFY_MISMATCH_FORCE_FULL_REPLACE: "true",
      }),
    ).toStrictEqual({
      thresholdRows: 25,
      largeTableRows: 500_000,
      forceFullReplace: true,
      reincrementalMaxDiffPercent: 1,
    });
  });

  it("falls back to defaults for empty env strings", () => {
    expect(
      resolveVerifyMismatchPolicy({
        REPLICA_VERIFY_MISMATCH_THRESHOLD_ROWS: "",
        REPLICA_VERIFY_MISMATCH_LARGE_TABLE_ROWS: "",
        REPLICA_VERIFY_MISMATCH_FORCE_FULL_REPLACE: "",
      }),
    ).toStrictEqual({
      thresholdRows: 10,
      largeTableRows: 100_000,
      forceFullReplace: false,
      reincrementalMaxDiffPercent: 1,
    });
  });

  it("falls back to defaults for negative or fractional integers", () => {
    expect(
      resolveVerifyMismatchPolicy({
        REPLICA_VERIFY_MISMATCH_THRESHOLD_ROWS: "-1",
        REPLICA_VERIFY_MISMATCH_LARGE_TABLE_ROWS: "1.5",
      }),
    ).toStrictEqual({
      thresholdRows: 10,
      largeTableRows: 100_000,
      forceFullReplace: false,
      reincrementalMaxDiffPercent: 1,
    });
  });

  it("accepts zero as an explicit threshold value", () => {
    expect(
      resolveVerifyMismatchPolicy({
        REPLICA_VERIFY_MISMATCH_THRESHOLD_ROWS: "0",
      }),
    ).toStrictEqual({
      thresholdRows: 0,
      largeTableRows: 100_000,
      forceFullReplace: false,
      reincrementalMaxDiffPercent: 1,
    });
  });

  it("reads explicit REPLICA_SYNC_FULL_REPLACE_THRESHOLD_PERCENT", () => {
    expect(
      resolveVerifyMismatchPolicy({
        REPLICA_SYNC_FULL_REPLACE_THRESHOLD_PERCENT: "2.5",
      }),
    ).toStrictEqual({
      thresholdRows: 10,
      largeTableRows: 100_000,
      forceFullReplace: false,
      reincrementalMaxDiffPercent: 2.5,
    });
  });

  it("falls back to default percent when env value is not numeric", () => {
    expect(
      resolveVerifyMismatchPolicy({
        REPLICA_SYNC_FULL_REPLACE_THRESHOLD_PERCENT: "abc",
      }),
    ).toStrictEqual({
      thresholdRows: 10,
      largeTableRows: 100_000,
      forceFullReplace: false,
      reincrementalMaxDiffPercent: 1,
    });
  });

  it("accepts zero as an explicit percent value", () => {
    expect(
      resolveVerifyMismatchPolicy({
        REPLICA_SYNC_FULL_REPLACE_THRESHOLD_PERCENT: "0",
      }),
    ).toStrictEqual({
      thresholdRows: 10,
      largeTableRows: 100_000,
      forceFullReplace: false,
      reincrementalMaxDiffPercent: 0,
    });
  });
});

describe("decideVerifyMismatchAction", () => {
  const defaultPolicy: VerifyMismatchPolicy = {
    thresholdRows: 10,
    largeTableRows: 100_000,
    forceFullReplace: false,
    reincrementalMaxDiffPercent: 1,
  };

  it("returns skip when the table is large and the diff is small", () => {
    const action = decideVerifyMismatchAction({
      tableName: "jvd_hc",
      localCount: 11_793_564,
      neonCount: 11_793_565,
      rowCount: 11_793_564,
      policy: defaultPolicy,
    });
    expect(action.kind).toBe("skip");
    expect(action.message).toBe(
      "jvd_hc: verify mismatch (local=11793564, neon=11793565, diff=1) — full-replace too costly (11793564 rows, ≥100000 threshold). Skipping. Run reconcile or set REPLICA_VERIFY_MISMATCH_FORCE_FULL_REPLACE=true.",
    );
  });

  it("returns re-incremental when diff exceeds row threshold but stays under percent threshold on large tables", () => {
    const action = decideVerifyMismatchAction({
      tableName: "jvd_hc",
      localCount: 11_793_564,
      neonCount: 11_793_500,
      rowCount: 11_793_564,
      policy: defaultPolicy,
    });
    expect(action.kind).toBe("re-incremental");
  });

  it("returns fallback-full when the diff exceeds both the row and percent thresholds on a large table", () => {
    const action = decideVerifyMismatchAction({
      tableName: "jvd_hc",
      localCount: 1_000_000,
      neonCount: 980_000,
      rowCount: 1_000_000,
      policy: defaultPolicy,
    });
    expect(action.kind).toBe("fallback-full");
    expect(action.message).toBe(
      "jvd_hc: verify mismatch (local=1000000, neon=980000, diff=20000) — falling back to full-replace",
    );
  });

  it("returns fallback-full when the table is small", () => {
    const action = decideVerifyMismatchAction({
      tableName: "small_table",
      localCount: 100,
      neonCount: 101,
      rowCount: 1000,
      policy: defaultPolicy,
    });
    expect(action.kind).toBe("fallback-full");
  });

  it("returns fallback-full when forceFullReplace is true", () => {
    const action = decideVerifyMismatchAction({
      tableName: "jvd_hc",
      localCount: 11_793_564,
      neonCount: 11_793_565,
      rowCount: 11_793_564,
      policy: {
        thresholdRows: 10,
        largeTableRows: 100_000,
        forceFullReplace: true,
        reincrementalMaxDiffPercent: 1,
      },
    });
    expect(action.kind).toBe("fallback-full");
  });

  it("treats the largeTableRows threshold as inclusive", () => {
    const action = decideVerifyMismatchAction({
      tableName: "edge",
      localCount: 100_000,
      neonCount: 100_001,
      rowCount: 100_000,
      policy: defaultPolicy,
    });
    expect(action.kind).toBe("skip");
  });

  it("treats the diff threshold as inclusive", () => {
    const action = decideVerifyMismatchAction({
      tableName: "edge",
      localCount: 1_000_000,
      neonCount: 1_000_010,
      rowCount: 1_000_000,
      policy: defaultPolicy,
    });
    expect(action.kind).toBe("skip");
  });

  it("returns re-incremental when the diff is one above the row threshold but well below the percent threshold", () => {
    const action = decideVerifyMismatchAction({
      tableName: "edge",
      localCount: 1_000_000,
      neonCount: 1_000_011,
      rowCount: 1_000_000,
      policy: defaultPolicy,
    });
    expect(action.kind).toBe("re-incremental");
  });

  it("includes the reason on skip actions", () => {
    const action = decideVerifyMismatchAction({
      tableName: "jvd_hc",
      localCount: 100_000,
      neonCount: 100_001,
      rowCount: 100_000,
      policy: defaultPolicy,
    });
    expect(action.kind === "skip" ? action.reason : "").toBe(
      "verify mismatch (local=100000, neon=100001, diff=1)",
    );
  });

  it("uses absolute diff so neon ahead of local is treated the same", () => {
    const action = decideVerifyMismatchAction({
      tableName: "jvd_hc",
      localCount: 11_793_565,
      neonCount: 11_793_564,
      rowCount: 11_793_565,
      policy: defaultPolicy,
    });
    expect(action.kind).toBe("skip");
  });

  it("returns fallback-full when rowCount is exactly one below the large table threshold", () => {
    const action = decideVerifyMismatchAction({
      tableName: "near_edge",
      localCount: 99_999,
      neonCount: 100_000,
      rowCount: 99_999,
      policy: defaultPolicy,
    });
    expect(action.kind).toBe("fallback-full");
  });

  it("returns skip with the documented message when local equals neon (diff zero) on a large table", () => {
    const action = decideVerifyMismatchAction({
      tableName: "stable",
      localCount: 200_000,
      neonCount: 200_000,
      rowCount: 200_000,
      policy: defaultPolicy,
    });
    expect(action.kind).toBe("skip");
    expect(action.message).toBe(
      "stable: verify mismatch (local=200000, neon=200000, diff=0) — full-replace too costly (200000 rows, ≥100000 threshold). Skipping. Run reconcile or set REPLICA_VERIFY_MISMATCH_FORCE_FULL_REPLACE=true.",
    );
  });

  it("returns re-incremental when threshold rows is zero but diff stays under percent threshold on a large table", () => {
    const action = decideVerifyMismatchAction({
      tableName: "strict",
      localCount: 200_000,
      neonCount: 200_001,
      rowCount: 200_000,
      policy: {
        thresholdRows: 0,
        largeTableRows: 100_000,
        forceFullReplace: false,
        reincrementalMaxDiffPercent: 1,
      },
    });
    expect(action.kind).toBe("re-incremental");
  });

  it("returns skip when threshold is zero and diff is exactly zero on a large table", () => {
    const action = decideVerifyMismatchAction({
      tableName: "strict",
      localCount: 200_000,
      neonCount: 200_000,
      rowCount: 200_000,
      policy: {
        thresholdRows: 0,
        largeTableRows: 100_000,
        forceFullReplace: false,
        reincrementalMaxDiffPercent: 1,
      },
    });
    expect(action.kind).toBe("skip");
  });

  it("falls back to full-replace when diff percent meets or exceeds the configured percent on a large table", () => {
    const action = decideVerifyMismatchAction({
      tableName: "drifty",
      localCount: 1_000_000,
      neonCount: 985_000,
      rowCount: 1_000_000,
      policy: defaultPolicy,
    });
    expect(action.kind).toBe("fallback-full");
  });

  it("returns fallback-full when forceFullReplace is true even when percent is small", () => {
    const action = decideVerifyMismatchAction({
      tableName: "drifty",
      localCount: 1_000_000,
      neonCount: 1_000_500,
      rowCount: 1_000_000,
      policy: {
        thresholdRows: 10,
        largeTableRows: 100_000,
        forceFullReplace: true,
        reincrementalMaxDiffPercent: 1,
      },
    });
    expect(action.kind).toBe("fallback-full");
  });

  it("returns fallback-full when reincrementalMaxDiffPercent is zero and diff is small percent", () => {
    const action = decideVerifyMismatchAction({
      tableName: "edge",
      localCount: 1_000_000,
      neonCount: 1_000_500,
      rowCount: 1_000_000,
      policy: {
        thresholdRows: 10,
        largeTableRows: 100_000,
        forceFullReplace: false,
        reincrementalMaxDiffPercent: 0,
      },
    });
    expect(action.kind).toBe("fallback-full");
  });
});

describe("VerifyMismatchSkipError", () => {
  it("captures the table name and counts on the error instance", () => {
    const error = new VerifyMismatchSkipError({
      tableName: "jvd_hc",
      localCount: 100,
      neonCount: 101,
      rowCount: 100,
      message: "verify mismatch — skip",
    });
    expect(error.name).toBe("VerifyMismatchSkipError");
    expect(error.message).toBe("verify mismatch — skip");
    expect(error.tableName).toBe("jvd_hc");
    expect(error.localCount).toBe(100);
    expect(error.neonCount).toBe(101);
    expect(error.rowCount).toBe(100);
  });

  it("is identified by isVerifyMismatchSkipError", () => {
    const error = new VerifyMismatchSkipError({
      tableName: "x",
      localCount: 0,
      neonCount: 0,
      rowCount: 0,
      message: "x",
    });
    expect(isVerifyMismatchSkipError(error)).toBe(true);
  });

  it("returns false for unrelated errors", () => {
    expect(isVerifyMismatchSkipError(new Error("other"))).toBe(false);
  });

  it("returns false for non-error values", () => {
    expect(isVerifyMismatchSkipError("string")).toBe(false);
    expect(isVerifyMismatchSkipError(undefined)).toBe(false);
    expect(isVerifyMismatchSkipError(null)).toBe(false);
  });
});

describe("resolveDefaultFullReplaceBatchRows", () => {
  it("returns the documented default 500_000 when env is unset", () => {
    expect(resolveDefaultFullReplaceBatchRows({})).toBe(500_000);
  });

  it("returns the documented default 500_000 when env is empty", () => {
    expect(resolveDefaultFullReplaceBatchRows({ REPLICA_SYNC_COPY_BATCH_ROWS: "" })).toBe(500_000);
  });

  it("returns the documented default 500_000 when env is not a positive integer", () => {
    expect(resolveDefaultFullReplaceBatchRows({ REPLICA_SYNC_COPY_BATCH_ROWS: "abc" })).toBe(
      500_000,
    );
    expect(resolveDefaultFullReplaceBatchRows({ REPLICA_SYNC_COPY_BATCH_ROWS: "0" })).toBe(500_000);
    expect(resolveDefaultFullReplaceBatchRows({ REPLICA_SYNC_COPY_BATCH_ROWS: "-50" })).toBe(
      500_000,
    );
    expect(resolveDefaultFullReplaceBatchRows({ REPLICA_SYNC_COPY_BATCH_ROWS: "1.5" })).toBe(
      500_000,
    );
  });

  it("returns the env-provided positive integer", () => {
    expect(resolveDefaultFullReplaceBatchRows({ REPLICA_SYNC_COPY_BATCH_ROWS: "250000" })).toBe(
      250_000,
    );
  });
});

describe("computeChunkPlan", () => {
  it("returns zero chunks for empty tables", () => {
    expect(computeChunkPlan(0, 500_000)).toStrictEqual({ chunkRows: 500_000, chunkCount: 0 });
  });

  it("returns a single chunk when rows fit in one batch", () => {
    expect(computeChunkPlan(100_000, 500_000)).toStrictEqual({
      chunkRows: 500_000,
      chunkCount: 1,
    });
  });

  it("returns the ceiling chunk count when rows exceed one batch", () => {
    expect(computeChunkPlan(2_857_566, 500_000)).toStrictEqual({
      chunkRows: 500_000,
      chunkCount: 6,
    });
  });

  it("falls back to a single chunk when batch size is non-positive", () => {
    expect(computeChunkPlan(100, 0)).toStrictEqual({ chunkRows: 100, chunkCount: 1 });
    expect(computeChunkPlan(100, -10)).toStrictEqual({ chunkRows: 100, chunkCount: 1 });
  });
});

describe("resolveOperationTimeoutPolicy", () => {
  it("returns the documented defaults when env vars are unset", () => {
    expect(resolveOperationTimeoutPolicy({})).toStrictEqual({
      wallClockMs: 3_600_000,
      idleMs: 900_000,
      warningRatio: 0.8,
    });
  });

  it("reads explicit positive integers", () => {
    expect(
      resolveOperationTimeoutPolicy({
        REPLICA_SYNC_OPERATION_TIMEOUT_SECONDS: "7200",
        REPLICA_SYNC_IDLE_TIMEOUT_SECONDS: "60",
      }),
    ).toStrictEqual({
      wallClockMs: 7_200_000,
      idleMs: 60_000,
      warningRatio: 0.8,
    });
  });

  it("falls back to default when env is empty", () => {
    expect(
      resolveOperationTimeoutPolicy({
        REPLICA_SYNC_OPERATION_TIMEOUT_SECONDS: "",
        REPLICA_SYNC_IDLE_TIMEOUT_SECONDS: "",
      }),
    ).toStrictEqual({
      wallClockMs: 3_600_000,
      idleMs: 900_000,
      warningRatio: 0.8,
    });
  });

  it("falls back to default when env is not an integer", () => {
    expect(
      resolveOperationTimeoutPolicy({
        REPLICA_SYNC_OPERATION_TIMEOUT_SECONDS: "abc",
        REPLICA_SYNC_IDLE_TIMEOUT_SECONDS: "1.5",
      }),
    ).toStrictEqual({
      wallClockMs: 3_600_000,
      idleMs: 900_000,
      warningRatio: 0.8,
    });
  });
});

describe("resolvePerTableWallClockMs", () => {
  it("returns the fallback when no per-table env var is set", () => {
    expect(
      resolvePerTableWallClockMs({
        env: {},
        tableName: "jvd_se",
        fallbackWallClockMs: 3_600_000,
      }),
    ).toBe(3_600_000);
  });

  it("reads a per-table override when set", () => {
    expect(
      resolvePerTableWallClockMs({
        env: { REPLICA_SYNC_OPERATION_TIMEOUT_SECONDS_jvd_se: "5400" },
        tableName: "jvd_se",
        fallbackWallClockMs: 3_600_000,
      }),
    ).toBe(5_400_000);
  });

  it("falls back when per-table env value is empty", () => {
    expect(
      resolvePerTableWallClockMs({
        env: { REPLICA_SYNC_OPERATION_TIMEOUT_SECONDS_jvd_se: "" },
        tableName: "jvd_se",
        fallbackWallClockMs: 3_600_000,
      }),
    ).toBe(3_600_000);
  });

  it("falls back when per-table env value is not a positive integer", () => {
    expect(
      resolvePerTableWallClockMs({
        env: { REPLICA_SYNC_OPERATION_TIMEOUT_SECONDS_jvd_se: "0" },
        tableName: "jvd_se",
        fallbackWallClockMs: 3_600_000,
      }),
    ).toBe(3_600_000);
    expect(
      resolvePerTableWallClockMs({
        env: { REPLICA_SYNC_OPERATION_TIMEOUT_SECONDS_jvd_se: "-1" },
        tableName: "jvd_se",
        fallbackWallClockMs: 3_600_000,
      }),
    ).toBe(3_600_000);
    expect(
      resolvePerTableWallClockMs({
        env: { REPLICA_SYNC_OPERATION_TIMEOUT_SECONDS_jvd_se: "1.5" },
        tableName: "jvd_se",
        fallbackWallClockMs: 3_600_000,
      }),
    ).toBe(3_600_000);
  });

  it("does not match a different table name", () => {
    expect(
      resolvePerTableWallClockMs({
        env: { REPLICA_SYNC_OPERATION_TIMEOUT_SECONDS_jvd_se: "5400" },
        tableName: "nvd_se",
        fallbackWallClockMs: 1234,
      }),
    ).toBe(1234);
  });
});

describe("resolvePerTableIdleMs", () => {
  it("returns the fallback when no per-table env var is set", () => {
    expect(
      resolvePerTableIdleMs({
        env: {},
        tableName: "race_finish_position_model_predictions",
        fallbackIdleMs: 900_000,
      }),
    ).toBe(900_000);
  });

  it("reads a per-table override when set", () => {
    expect(
      resolvePerTableIdleMs({
        env: { REPLICA_SYNC_IDLE_TIMEOUT_SECONDS_race_finish_position_model_predictions: "1800" },
        tableName: "race_finish_position_model_predictions",
        fallbackIdleMs: 900_000,
      }),
    ).toBe(1_800_000);
  });

  it("falls back when per-table env value is empty", () => {
    expect(
      resolvePerTableIdleMs({
        env: { REPLICA_SYNC_IDLE_TIMEOUT_SECONDS_race_finish_position_model_predictions: "" },
        tableName: "race_finish_position_model_predictions",
        fallbackIdleMs: 900_000,
      }),
    ).toBe(900_000);
  });

  it("falls back when per-table env value is zero", () => {
    expect(
      resolvePerTableIdleMs({
        env: { REPLICA_SYNC_IDLE_TIMEOUT_SECONDS_race_finish_position_model_predictions: "0" },
        tableName: "race_finish_position_model_predictions",
        fallbackIdleMs: 900_000,
      }),
    ).toBe(900_000);
  });

  it("falls back when per-table env value is negative", () => {
    expect(
      resolvePerTableIdleMs({
        env: { REPLICA_SYNC_IDLE_TIMEOUT_SECONDS_race_finish_position_model_predictions: "-1" },
        tableName: "race_finish_position_model_predictions",
        fallbackIdleMs: 900_000,
      }),
    ).toBe(900_000);
  });

  it("falls back when per-table env value is not an integer", () => {
    expect(
      resolvePerTableIdleMs({
        env: { REPLICA_SYNC_IDLE_TIMEOUT_SECONDS_race_finish_position_model_predictions: "1.5" },
        tableName: "race_finish_position_model_predictions",
        fallbackIdleMs: 900_000,
      }),
    ).toBe(900_000);
  });

  it("does not match a different table name", () => {
    expect(
      resolvePerTableIdleMs({
        env: { REPLICA_SYNC_IDLE_TIMEOUT_SECONDS_race_finish_position_model_predictions: "1800" },
        tableName: "nvd_se",
        fallbackIdleMs: 4321,
      }),
    ).toBe(4321);
  });
});

describe("formatRowsPerSecond", () => {
  it("returns zero when elapsed is zero", () => {
    expect(formatRowsPerSecond(100, 0)).toBe(0);
  });

  it("returns zero when rowsDone is zero", () => {
    expect(formatRowsPerSecond(0, 10)).toBe(0);
  });

  it("returns rows divided by elapsed seconds", () => {
    expect(formatRowsPerSecond(8000, 1)).toBe(8000);
    expect(formatRowsPerSecond(8000, 2)).toBe(4000);
  });

  it("treats negative inputs as zero", () => {
    expect(formatRowsPerSecond(-1, 1)).toBe(0);
    expect(formatRowsPerSecond(1, -1)).toBe(0);
  });
});

describe("computeChunkEtaSeconds", () => {
  it("returns zero when no rows are done yet", () => {
    expect(computeChunkEtaSeconds(0, 100, 10)).toBe(0);
  });

  it("returns zero when elapsed is zero", () => {
    expect(computeChunkEtaSeconds(50, 100, 0)).toBe(0);
  });

  it("returns the remaining seconds linearly extrapolated from current rate", () => {
    expect(computeChunkEtaSeconds(25, 100, 10)).toBe(30);
  });

  it("returns zero when all rows are already done", () => {
    expect(computeChunkEtaSeconds(100, 100, 10)).toBe(0);
  });

  it("returns zero when rowsDone exceeds rowsTotal", () => {
    expect(computeChunkEtaSeconds(200, 100, 10)).toBe(0);
  });
});

describe("buildJsonlRecord", () => {
  it("encodes a chunk-plan event with chunk meta and rows total", () => {
    const event: ProgressEvent = {
      type: "chunk-plan",
      tableName: "jvd_se",
      rowCount: 2_857_566,
      chunkCount: 6,
      chunkRows: 500_000,
      wallClockTimeoutSeconds: 3600,
      idleTimeoutSeconds: 300,
    };
    expect(
      buildJsonlRecord({ tsIso: "2026-06-02T10:00:00Z", event, elapsedSeconds: 12 }),
    ).toStrictEqual({
      ts: "2026-06-02T10:00:00Z",
      event: "chunk-plan",
      elapsed_s: 12,
      table: "jvd_se",
      chunk_count: 6,
      rows: 500_000,
      rows_total: 2_857_566,
    });
  });

  it("encodes a chunk-done event with throughput and ETA", () => {
    const event: ProgressEvent = {
      type: "chunk-done",
      tableName: "jvd_se",
      chunkIndex: 3,
      chunkCount: 6,
      rowsDone: 1_500_000,
      rowsTotal: 2_857_566,
      chunkElapsedSeconds: 60,
      tableElapsedSeconds: 180,
      rowsPerSecond: 25_000,
      etaTableSeconds: 50,
    };
    expect(
      buildJsonlRecord({
        tsIso: "2026-06-02T10:03:00Z",
        event,
        elapsedSeconds: 200,
        attempt: 1,
        attemptMax: 5,
      }),
    ).toStrictEqual({
      ts: "2026-06-02T10:03:00Z",
      event: "chunk-done",
      elapsed_s: 200,
      table: "jvd_se",
      chunk_index: 3,
      chunk_count: 6,
      rows: 1_500_000,
      rows_total: 2_857_566,
      rows_per_second: 25_000,
      eta_table_s: 50,
      attempt: 1,
      attempt_max: 5,
    });
  });

  it("encodes a table-start event with rows total and ETA", () => {
    const event: ProgressEvent = {
      type: "table-start",
      tableName: "jvd_se",
      estimatedRows: 2_857_566,
      dependencyLevel: 0,
      levelConcurrency: 2,
      runningTables: 1,
      runningTableNames: ["jvd_se"],
      completedTables: 0,
      completedTableNames: [],
      remainingTables: 97,
      remainingTableNames: [],
      syncedEstimatedRows: 0,
      remainingEstimatedRows: 2_857_566,
      elapsedSeconds: 5,
      etaSeconds: 720,
    };
    expect(
      buildJsonlRecord({ tsIso: "2026-06-02T10:00:00Z", event, elapsedSeconds: 5 }),
    ).toStrictEqual({
      ts: "2026-06-02T10:00:00Z",
      event: "table-start",
      elapsed_s: 5,
      table: "jvd_se",
      rows_total: 2_857_566,
      eta_total_s: 720,
    });
  });

  it("encodes a table-done event with rows total and ETA", () => {
    const event: ProgressEvent = {
      type: "table-done",
      tableName: "jvd_se",
      estimatedRows: 2_857_566,
      dependencyLevel: 0,
      levelConcurrency: 2,
      tableElapsedSeconds: 763,
      runningTables: 1,
      runningTableNames: [],
      completedTables: 1,
      completedTableNames: ["jvd_se"],
      totalTables: 98,
      syncedEstimatedRows: 2_857_566,
      totalEstimatedRows: 100_000_000,
      remainingTables: 97,
      remainingTableNames: [],
      remainingEstimatedRows: 97_142_434,
      elapsedSeconds: 768,
      etaSeconds: 360,
    };
    expect(
      buildJsonlRecord({ tsIso: "2026-06-02T10:12:00Z", event, elapsedSeconds: 768 }),
    ).toStrictEqual({
      ts: "2026-06-02T10:12:00Z",
      event: "table-done",
      elapsed_s: 768,
      table: "jvd_se",
      rows_total: 2_857_566,
      eta_total_s: 360,
    });
  });

  it("encodes a timeout-warning event with table name", () => {
    const event: ProgressEvent = {
      type: "timeout-warning",
      tableName: "jvd_se",
      label: "COPY pipeline",
      elapsedSeconds: 480,
      timeoutSeconds: 600,
      kind: "idle",
    };
    expect(
      buildJsonlRecord({ tsIso: "2026-06-02T10:08:00Z", event, elapsedSeconds: 480 }),
    ).toStrictEqual({
      ts: "2026-06-02T10:08:00Z",
      event: "timeout-warning",
      elapsed_s: 480,
      table: "jvd_se",
    });
  });

  it("encodes a non-table-scoped event without table fields", () => {
    const event: ProgressEvent = {
      type: "start",
      totalTables: 98,
      totalEstimatedRows: 100_000_000,
      dependencyLevels: 1,
      concurrency: 2,
    };
    expect(
      buildJsonlRecord({ tsIso: "2026-06-02T10:00:00Z", event, elapsedSeconds: 0 }),
    ).toStrictEqual({
      ts: "2026-06-02T10:00:00Z",
      event: "start",
      elapsed_s: 0,
    });
  });
});

describe("decideVerifyMismatchAction re-incremental action", () => {
  const defaultPolicy: VerifyMismatchPolicy = {
    thresholdRows: 10,
    largeTableRows: 100_000,
    forceFullReplace: false,
    reincrementalMaxDiffPercent: 1,
  };

  it("returns re-incremental with diffPercent that matches actual ratio", () => {
    const action = decideVerifyMismatchAction({
      tableName: "jvd_se",
      localCount: 2_857_566,
      neonCount: 2_856_779,
      rowCount: 2_857_566,
      policy: defaultPolicy,
    });
    expect(action.kind).toBe("re-incremental");
    if (action.kind === "re-incremental") {
      expect(action.diffPercent).toBeCloseTo(0.02753, 4);
      expect(
        action.reason.startsWith("verify mismatch (local=2857566, neon=2856779, diff=787"),
      ).toBe(true);
    }
  });

  it("does not return re-incremental for small tables (still falls back to full-replace)", () => {
    const action = decideVerifyMismatchAction({
      tableName: "tiny",
      localCount: 50_000,
      neonCount: 50_005,
      rowCount: 50_000,
      policy: defaultPolicy,
    });
    expect(action.kind).toBe("fallback-full");
  });

  it("returns re-incremental message that names the configured percent threshold", () => {
    const action = decideVerifyMismatchAction({
      tableName: "jvd_se",
      localCount: 1_000_000,
      neonCount: 999_900,
      rowCount: 1_000_000,
      policy: { ...defaultPolicy, reincrementalMaxDiffPercent: 2 },
    });
    if (action.kind !== "re-incremental") throw new Error("expected re-incremental");
    expect(action.message).toBe(
      "jvd_se: verify mismatch (local=1000000, neon=999900, diff=100, 0.010%) — under 2% drift, retrying as re-incremental instead of full-replace",
    );
  });
});

describe("resolveSkipTables", () => {
  it("returns an empty set when REPLICA_SYNC_SKIP_TABLES is unset", () => {
    expect(resolveSkipTables({})).toStrictEqual(new Set());
  });

  it("returns an empty set when REPLICA_SYNC_SKIP_TABLES is empty string", () => {
    expect(resolveSkipTables({ REPLICA_SYNC_SKIP_TABLES: "" })).toStrictEqual(new Set());
  });

  it("returns an empty set when REPLICA_SYNC_SKIP_TABLES is whitespace only", () => {
    expect(resolveSkipTables({ REPLICA_SYNC_SKIP_TABLES: "   " })).toStrictEqual(new Set());
  });

  it("returns a single-entry set for one table", () => {
    expect(resolveSkipTables({ REPLICA_SYNC_SKIP_TABLES: "legacy_logs" })).toStrictEqual(
      new Set(["legacy_logs"]),
    );
  });

  it("returns a three-entry set for three comma-separated tables", () => {
    expect(
      resolveSkipTables({ REPLICA_SYNC_SKIP_TABLES: "table_a,table_b,table_c" }),
    ).toStrictEqual(new Set(["table_a", "table_b", "table_c"]));
  });

  it("trims surrounding whitespace from each entry", () => {
    expect(
      resolveSkipTables({ REPLICA_SYNC_SKIP_TABLES: "  table_a , table_b ,table_c  " }),
    ).toStrictEqual(new Set(["table_a", "table_b", "table_c"]));
  });

  it("ignores empty entries produced by trailing commas", () => {
    expect(resolveSkipTables({ REPLICA_SYNC_SKIP_TABLES: "table_a,table_b," })).toStrictEqual(
      new Set(["table_a", "table_b"]),
    );
  });

  it("ignores empty entries produced by consecutive commas", () => {
    expect(resolveSkipTables({ REPLICA_SYNC_SKIP_TABLES: "table_a,,table_b" })).toStrictEqual(
      new Set(["table_a", "table_b"]),
    );
  });
});
