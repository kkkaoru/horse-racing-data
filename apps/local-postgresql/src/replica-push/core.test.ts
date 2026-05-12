import { describe, expect, it } from "vitest";
import {
  buildConfig,
  buildDependencyPlan,
  buildDependencySql,
  buildMetadataSql,
  buildNeonApplySql,
  buildTableFilterSql,
  calculateEtaSeconds,
  parseConcurrency,
  parseApplyMode,
  parseDependencyEdges,
  parseBoolean,
  parsePositiveInteger,
  parseSelectedTables,
  parseTableMetadata,
  quoteIdentifier,
  quoteLiteral,
  resolveConcurrency,
  runPushSync,
  waitForNeonReady,
  type ProgressEvent,
  type TableMetadata,
} from "./core";

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
      'CREATE UNIQUE INDEX "replica_sync_stage_pk" ON "replica_sync_stage" ("id")',
    );
    expect(sql.postCopySql).toContain('ON CONFLICT ("id") DO UPDATE SET "name" = excluded."name"');
    expect(sql.postCopySql).toContain('DELETE FROM public."table_a" AS target');
    expect(sql.postCopySql).toContain("COMMIT;");
  });

  it("builds Neon apply SQL for replace mode", () => {
    const sql = buildNeonApplySql(tableA, true);
    expect(sql.postCopySql).toContain('TRUNCATE TABLE public."table_a"');
    expect(sql.postCopySql).toContain(
      'INSERT INTO public."table_a" ("id", "name") SELECT "id", "name" FROM "replica_sync_stage" AS stage',
    );
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
