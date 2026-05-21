import { describe, expect, it } from "vitest";
import {
  buildConfig,
  buildDependencyPlan,
  buildDependencySql,
  buildFingerprintSql,
  buildIncrementalApplySql,
  buildIncrementalCopyFromSql,
  buildMetadataSql,
  buildNeonApplySql,
  buildTableFilterSql,
  buildTableProfileSql,
  buildTimestampFingerprintSql,
  calculateEtaSeconds,
  incrementalComparatorForTimestampColumn,
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
  resolveStrategy,
  runPushSync,
  shouldRefreshInclusiveIncrementalMarker,
  timestampKeyExpression,
  waitForNeonReady,
  type ProgressEvent,
  type SyncStrategyThresholds,
  type TableMetadata,
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
    expect(sql.postCopySql).toContain('SELECT DISTINCT ON ("id") "id", "name"');
    expect(sql.postCopySql).toContain('ON CONFLICT ("id") DO UPDATE SET "name" = excluded."name"');
    expect(sql.postCopySql).toContain('DELETE FROM public."table_a" AS target');
    expect(sql.postCopySql).toContain("COMMIT;");
  });

  it("builds Neon apply SQL for replace mode", () => {
    const sql = buildNeonApplySql(tableA, true);
    expect(sql.postCopySql).toContain('TRUNCATE TABLE public."table_a"');
    expect(sql.postCopySql).toContain(
      'INSERT INTO public."table_a" ("id", "name") SELECT "id", "name"\nFROM (',
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
      'INSERT INTO public."table_a" ("id", "name") SELECT "id", "name"',
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
