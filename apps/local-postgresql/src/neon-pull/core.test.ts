// Run with bun (bunx vitest).
import { expect, it } from "vitest";
import {
  buildColumnsSql,
  buildMaxDateSql,
  buildPrimaryKeySql,
  buildSelectWindowSql,
  buildUpsertSql,
  chunk,
  commonColumns,
  pullTable,
  quoteIdentifier,
  resolvePullWindow,
  runPull,
  subtractDays,
  type PullDatabase,
  type PullTarget,
  type Row,
} from "./core";

interface RecordedQuery {
  sql: string;
  params: readonly unknown[] | undefined;
}

interface FakeTableData {
  maxDate: string | null;
  columns: string[];
  primaryKey: string[];
  windowRows: Row[];
}

interface FakeDatabase extends PullTarget {
  queries: RecordedQuery[];
  transactions: number;
}

const fakeDatabase = (tables: Record<string, FakeTableData>): FakeDatabase => {
  const queries: RecordedQuery[] = [];
  const state = { transactions: 0 };
  const tableFor = (
    sql: string,
    params: readonly unknown[] | undefined,
  ): FakeTableData | undefined => {
    const name =
      typeof params?.[0] === "string" && !/^\d{8}$/u.test(params[0])
        ? params[0]
        : (/from "([a-z_0-9]+)"/u.exec(sql)?.[1] ?? "");
    return tables[name];
  };
  const database: FakeDatabase = {
    queries,
    get transactions() {
      return state.transactions;
    },
    query: async (sql, params) => {
      queries.push({ sql, params });
      const table = tableFor(sql, params);
      if (sql.startsWith("select max(")) return [{ max_date: table?.maxDate ?? null }];
      if (sql.includes("information_schema.columns"))
        return (table?.columns ?? []).map((column_name) => ({ column_name }));
      if (sql.includes("PRIMARY KEY"))
        return (table?.primaryKey ?? []).map((column_name) => ({ column_name }));
      if (sql.startsWith("select ")) return table?.windowRows ?? [];
      return [];
    },
    transaction: async (work) => {
      state.transactions += 1;
      return work();
    },
  };
  return database;
};

it("subtractDays crosses month and year boundaries", () => {
  expect(subtractDays("20261004", 14)).toBe("20260920");
  expect(subtractDays("20260105", 7)).toBe("20251229");
});

it("resolvePullWindow spans local max minus lookback through Neon max", () => {
  expect(
    resolvePullWindow({ localMax: "20261004", neonMax: "20261018", lookbackDays: 14 }),
  ).toStrictEqual({
    from: "20260920",
    to: "20261018",
  });
});

it("resolvePullWindow skips when either side has no valid race date", () => {
  expect(resolvePullWindow({ localMax: null, neonMax: "20261018", lookbackDays: 14 })).toBe(null);
  expect(resolvePullWindow({ localMax: "20261004", neonMax: null, lookbackDays: 14 })).toBe(null);
  expect(resolvePullWindow({ localMax: "2026100", neonMax: "20261018", lookbackDays: 14 })).toBe(
    null,
  );
});

it("quoteIdentifier rejects unsafe identifiers", () => {
  expect(quoteIdentifier("jvd_se")).toBe('"jvd_se"');
  expect(() => quoteIdentifier("jvd_se; drop table x")).toThrow(
    "Unsafe SQL identifier: jvd_se; drop table x",
  );
});

it("commonColumns keeps the local order of columns present in Neon", () => {
  expect(commonColumns(["a", "b", "c"], ["c", "a", "z"])).toStrictEqual(["a", "c"]);
});

it("buildMaxDateSql, buildColumnsSql, buildPrimaryKeySql and buildSelectWindowSql render the expected SQL", () => {
  expect(buildMaxDateSql("jvd_se")).toBe(
    'select max(kaisai_nen || kaisai_tsukihi) as max_date from "jvd_se"',
  );
  expect(buildColumnsSql()).toBe(
    "select column_name from information_schema.columns where table_schema = 'public' and table_name = $1 order by ordinal_position",
  );
  expect(buildPrimaryKeySql()).toBe(
    "select kcu.column_name from information_schema.table_constraints tc join information_schema.key_column_usage kcu on kcu.constraint_name = tc.constraint_name and kcu.table_schema = tc.table_schema where tc.table_schema = 'public' and tc.table_name = $1 and tc.constraint_type = 'PRIMARY KEY' order by kcu.ordinal_position",
  );
  expect(buildSelectWindowSql("jvd_se", ["kaisai_nen", "umaban"])).toBe(
    'select "kaisai_nen", "umaban" from "jvd_se" where kaisai_nen || kaisai_tsukihi between $1 and $2',
  );
});

it("buildUpsertSql updates non-key columns on conflict", () => {
  expect(buildUpsertSql({ table: "t", columns: ["k", "v"], primaryKey: ["k"], rowCount: 2 })).toBe(
    'insert into "t" ("k", "v") values ($1, $2), ($3, $4) on conflict ("k") do update set "v" = excluded."v"',
  );
});

it("buildUpsertSql does nothing on conflict when every column is a key", () => {
  expect(buildUpsertSql({ table: "t", columns: ["k"], primaryKey: ["k"], rowCount: 1 })).toBe(
    'insert into "t" ("k") values ($1) on conflict ("k") do nothing',
  );
});

it("chunk splits into fixed-size batches", () => {
  expect(chunk([1, 2, 3, 4, 5], 2)).toStrictEqual([[1, 2], [3, 4], [5]]);
  expect(chunk([], 2)).toStrictEqual([]);
});

it("pullTable upserts Neon window rows in batches inside one transaction", async () => {
  const source = fakeDatabase({
    jvd_se: {
      maxDate: "20261018",
      columns: ["kaisai_nen", "kaisai_tsukihi", "umaban", "neon_only"],
      primaryKey: [],
      windowRows: [
        { kaisai_nen: "2026", kaisai_tsukihi: "1018", umaban: "01", neon_only: 1 },
        { kaisai_nen: "2026", kaisai_tsukihi: "1018", umaban: "02", neon_only: 2 },
        { kaisai_nen: "2026", kaisai_tsukihi: "1011", umaban: "01", neon_only: 3 },
      ],
    },
  });
  const target = fakeDatabase({
    jvd_se: {
      maxDate: "20261004",
      columns: ["kaisai_nen", "kaisai_tsukihi", "umaban"],
      primaryKey: ["kaisai_nen", "kaisai_tsukihi", "umaban"],
      windowRows: [],
    },
  });

  const result = await pullTable(source, target, "jvd_se", {
    tables: ["jvd_se"],
    lookbackDays: 14,
    batchRows: 2,
  });

  expect(result).toStrictEqual({
    table: "jvd_se",
    window: { from: "20260920", to: "20261018" },
    upserted: 3,
  });
  expect(source.queries.at(-1)).toStrictEqual({
    sql: 'select "kaisai_nen", "kaisai_tsukihi", "umaban" from "jvd_se" where kaisai_nen || kaisai_tsukihi between $1 and $2',
    params: ["20260920", "20261018"],
  });
  expect(target.queries.slice(-2)).toStrictEqual([
    {
      sql: 'insert into "jvd_se" ("kaisai_nen", "kaisai_tsukihi", "umaban") values ($1, $2, $3), ($4, $5, $6) on conflict ("kaisai_nen", "kaisai_tsukihi", "umaban") do nothing',
      params: ["2026", "1018", "01", "2026", "1018", "02"],
    },
    {
      sql: 'insert into "jvd_se" ("kaisai_nen", "kaisai_tsukihi", "umaban") values ($1, $2, $3) on conflict ("kaisai_nen", "kaisai_tsukihi", "umaban") do nothing',
      params: ["2026", "1011", "01"],
    },
  ]);
  expect(target.transactions).toBe(1);
});

it("pullTable skips a table without a local race date", async () => {
  const source = fakeDatabase({
    t: { maxDate: "20261018", columns: ["k"], primaryKey: [], windowRows: [] },
  });
  const target = fakeDatabase({
    t: { maxDate: null, columns: ["k"], primaryKey: ["k"], windowRows: [] },
  });

  const result = await pullTable(source, target, "t", {
    tables: ["t"],
    lookbackDays: 14,
    batchRows: 500,
  });

  expect(result).toStrictEqual({ table: "t", window: null, upserted: 0 });
  expect(target.transactions).toBe(0);
});

it("pullTable refuses a target table without a primary key", async () => {
  const source = fakeDatabase({
    t: { maxDate: "20261018", columns: ["k"], primaryKey: [], windowRows: [] },
  });
  const target = fakeDatabase({
    t: { maxDate: "20261004", columns: ["k"], primaryKey: [], windowRows: [] },
  });

  await expect(
    pullTable(source, target, "t", { tables: ["t"], lookbackDays: 14, batchRows: 500 }),
  ).rejects.toThrow("Table has no primary key to upsert on: t");
});

it("runPull pulls each listed table in order", async () => {
  const source: PullDatabase = fakeDatabase({
    a: { maxDate: "20261018", columns: ["k"], primaryKey: [], windowRows: [{ k: "x" }] },
    b: { maxDate: "20261018", columns: ["k"], primaryKey: [], windowRows: [] },
  });
  const target = fakeDatabase({
    a: { maxDate: "20261004", columns: ["k"], primaryKey: ["k"], windowRows: [] },
    b: { maxDate: "20261010", columns: ["k"], primaryKey: ["k"], windowRows: [] },
  });

  const results = await runPull(source, target, {
    tables: ["a", "b"],
    lookbackDays: 0,
    batchRows: 500,
  });

  expect(results).toStrictEqual([
    { table: "a", window: { from: "20261004", to: "20261018" }, upserted: 1 },
    { table: "b", window: { from: "20261010", to: "20261018" }, upserted: 0 },
  ]);
});
