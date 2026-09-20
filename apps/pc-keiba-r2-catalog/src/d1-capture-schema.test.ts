// Runs with bun via Vitest; metadata errors must fail before any capture DDL is installed.
import { expect, test, vi } from "vitest";
import { discoverD1CaptureSchema, type D1CaptureMetadataQuery } from "./d1-capture-schema";

interface Fixtures {
  definitions?: Record<string, unknown>[];
  columns?: Record<string, unknown>[];
  indexes?: Record<string, unknown>[];
  details?: Record<string, unknown>[];
  predicates?: Record<string, unknown>[];
}
const metadata = (fixture: Fixtures = {}) =>
  vi
    .fn<D1CaptureMetadataQuery>()
    .mockResolvedValueOnce(
      fixture.definitions ?? [
        { type: "table", sql: "CREATE TABLE items (id INTEGER PRIMARY KEY, key TEXT UNIQUE)" },
      ],
    )
    .mockResolvedValueOnce(
      fixture.columns ?? [
        { cid: 0, name: "id", hidden: 0 },
        { cid: 1, name: "key", hidden: 0 },
      ],
    )
    .mockResolvedValueOnce([])
    .mockResolvedValueOnce(
      fixture.indexes ?? [
        { unique: 0, name: "ordinary" },
        { unique: 1, partial: 0, name: "uq'key" },
      ],
    )
    .mockResolvedValueOnce(
      fixture.details ?? [
        { seqno: 0, cid: 1, name: "key", coll: "NOCASE", key: 1 },
        { seqno: 1, cid: -1, name: null, coll: "BINARY", key: 0 },
      ],
    )
    .mockResolvedValue(fixture.predicates ?? []);

test("discovers all unique key columns and ignores only auxiliary/nonunique index entries", async () => {
  const query = metadata();
  const result = await discoverD1CaptureSchema("items", query);
  expect(result.uniqueKeys).toStrictEqual([
    { partial: false, columns: [{ name: "key", collation: "NOCASE" }] },
  ]);
  expect(result.definitionHash).toMatch(/^[a-f0-9]{64}$/u);
  expect(query).toHaveBeenCalledWith({ sql: "PRAGMA index_xinfo('uq''key')", params: [] });
  expect(query.mock.calls.every(([request]) => /^(?:SELECT|PRAGMA)/u.test(request.sql))).toBe(true);
  expect(await discoverD1CaptureSchema("items", metadata())).toStrictEqual(result);
});

test.each(["", "_cf_KV", "sqlite_sequence", "__pc_keiba_catalog_cdc_v1"])(
  "rejects internal tables before I/O",
  async (table) => {
    const query = metadata();
    await expect(discoverD1CaptureSchema(table, query)).rejects.toThrow("source table");
    expect(query).not.toHaveBeenCalled();
  },
);

test.each<Fixtures>([
  { definitions: [] },
  { definitions: [{ type: "view", sql: "CREATE VIEW x AS SELECT 1" }] },
  { definitions: [{ type: "table", sql: null }] },
  { definitions: [{ type: "table", sql: "CREATE VIRTUAL TABLE x USING fts5(v)" }] },
  { columns: [{ cid: 0, name: null, hidden: 0 }] },
  { columns: [{ cid: 0, name: "generated", hidden: 3 }] },
  { columns: [{ cid: -1, name: "id", hidden: 0 }] },
  { columns: [{ cid: "0", name: "id", hidden: 0 }] },
  { indexes: [{ unique: 2, name: "bad", partial: 0 }] },
  { indexes: [{ unique: 1, name: null, partial: 0 }] },
  { indexes: [{ unique: 1, name: "partial", partial: 1 }] },
  { indexes: [{ unique: 1, name: "bad", partial: 2 }] },
  { details: [{ key: 2 }] },
  { details: [{ key: 1, cid: -2, seqno: 0, name: null, coll: "BINARY" }] },
  { details: [{ key: 1, cid: 1, seqno: 0.5, name: "key", coll: "BINARY" }] },
  { details: [{ key: 1, cid: 1, seqno: 0, name: null, coll: "BINARY" }] },
  { details: [{ key: 1, cid: 1, seqno: 0, name: "key", coll: null }] },
  { details: [{ key: 1, cid: 1, seqno: 1, name: "key", coll: "BINARY" }] },
  { details: [] },
])("fails closed on unsupported or incomplete metadata", async (fixture) => {
  await expect(discoverD1CaptureSchema("items", metadata(fixture))).rejects.toThrow();
});

test.each([
  "CREATE UNIQUE INDEX uq ON items(key) WHERE key IS NOT NULL",
  'CREATE UNIQUE INDEX uq ON items(key) WHERE "KEY" IS NOT NULL;',
])("normalizes only redundant indexed-key null exclusions", async (sql) => {
  const result = await discoverD1CaptureSchema(
    "items",
    metadata({ indexes: [{ unique: 1, name: "uq", partial: 1 }], predicates: [{ sql }] }),
  );
  expect(result.uniqueKeys).toStrictEqual([
    { partial: false, columns: [{ name: "key", collation: "NOCASE" }] },
  ]);
});

test.each([
  { sql: null },
  { sql: "CREATE INDEX uq ON items(key) WHERE key IS NOT NULL" },
  { sql: "CREATE UNIQUE INDEX uq ON items(key) WHERE id > 0 -- WHERE key IS NOT NULL" },
  { sql: "CREATE UNIQUE INDEX uq ON items(key) WHERE id IS NOT NULL" },
  { sql: "CREATE UNIQUE INDEX uq ON items(key) WHERE key IS NOT NULL AND id > 0" },
])("rejects partial predicates that are not proven redundant", async (predicate) => {
  await expect(
    discoverD1CaptureSchema(
      "items",
      metadata({ indexes: [{ unique: 1, name: "uq", partial: 1 }], predicates: [predicate] }),
    ),
  ).rejects.toThrow("Unsupported unique index predicate");
});

test("rejects a sparse definition result", async () => {
  const definitions: Record<string, unknown>[] = [];
  definitions.length = 1;
  await expect(discoverD1CaptureSchema("items", metadata({ definitions }))).rejects.toThrow(
    "ordinary source table",
  );
});

test("orders composite key columns by position and fingerprints schema changes", async () => {
  const result = await discoverD1CaptureSchema(
    "items",
    metadata({
      details: [
        { key: 1, cid: 1, seqno: 1, name: "key", coll: "BINARY" },
        { key: 1, cid: 0, seqno: 0, name: "id", coll: "BINARY" },
      ],
    }),
  );
  expect(result.uniqueKeys[0]?.columns.map(({ name }) => name)).toStrictEqual(["id", "key"]);
  expect(result.definitionHash).not.toBe(
    (await discoverD1CaptureSchema("items", metadata())).definitionHash,
  );
});

test("propagates a failed rowid probe instead of accepting WITHOUT ROWID tables", async () => {
  const query = vi
    .fn<D1CaptureMetadataQuery>()
    .mockResolvedValueOnce([
      { type: "table", sql: "CREATE TABLE items (id INTEGER PRIMARY KEY) WITHOUT ROWID" },
    ])
    .mockResolvedValueOnce([{ name: "id", cid: 0, hidden: 0 }])
    .mockRejectedValueOnce(new Error("no such column: rowid"));
  await expect(discoverD1CaptureSchema("items", query)).rejects.toThrow("rowid");
});
