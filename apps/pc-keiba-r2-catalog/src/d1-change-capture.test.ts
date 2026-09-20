// Runs with bun via Vitest; native D1 verifies SQL triggers, not a simulated mutation log.
import { Miniflare } from "miniflare";
import { expect, test } from "vitest";
import { buildD1CapturePlan, D1_CAPTURE_TABLE } from "./d1-change-capture";
import { discoverD1CaptureSchema, type D1CaptureMetadataQuery } from "./d1-capture-schema";
import { buildD1SnapshotRowEncoding } from "./d1-snapshot";

const input = {
  captureId: "capture-test",
  table: "items",
  uniqueKeys: [],
  columns: [{ name: "id" }, { name: "rowid" }, { name: "b" }, { name: "r" }, { name: "n" }],
};

test("generates deterministic non-destructive DDL and qualified row encodings", () => {
  const plan = buildD1CapturePlan(input);
  expect(plan).toStrictEqual(buildD1CapturePlan(input));
  expect(plan.triggers).toHaveLength(5);
  expect(plan.planHash).toMatch(/^[a-f0-9]{64}$/u);
  expect(plan.planHash === buildD1CapturePlan({ ...input, captureId: "another" }).planHash).toBe(
    false,
  );
  expect(
    plan.triggers.every(({ sql }) => !sql.includes("IF NOT EXISTS") && !sql.includes("DROP ")),
  ).toBe(true);
  expect(buildD1SnapshotRowEncoding(input, "NEW").rowId).toBe("NEW._rowid_");
  expect(buildD1SnapshotRowEncoding(input, "OLD").payloadSql).toContain('typeof(OLD."id")');
  expect(buildD1CapturePlan({ ...input, table: `table'"name` }).triggers[0]?.sql).toContain(
    `table''"name`,
  );
});

test.each(["", "bad'capture", "x".repeat(65)])(
  "rejects malformed capture identities",
  (captureId) => {
    expect(() => buildD1CapturePlan({ ...input, captureId })).toThrow("identity");
  },
);

test.each([D1_CAPTURE_TABLE, "_CF_KV", "sqlite_sequence"])("refuses internal tables", (table) => {
  expect(() => buildD1CapturePlan({ ...input, table })).toThrow("internal");
});

test.each([
  [{ partial: true, columns: [{ name: "id", collation: "BINARY" }] }],
  [{ partial: false, columns: [] }],
  [{ partial: false, columns: [{ name: "expression", collation: "BINARY" }] }],
  [{ partial: false, columns: [{ name: "id", collation: "unknown" }] }],
])("refuses unsupported unique indexes", (key) => {
  expect(() => buildD1CapturePlan({ ...input, uniqueKeys: [key] })).toThrow(
    "Unsupported unique index",
  );
});

test("rejects non-array unique index columns", () => {
  expect(() =>
    Reflect.apply(buildD1CapturePlan, undefined, [
      { ...input, uniqueKeys: [{ partial: false, columns: null }] },
    ]),
  ).toThrow("Unsupported unique index");
});

test("requires explicit complete unique index metadata", () => {
  expect(() =>
    Reflect.apply(buildD1CapturePlan, undefined, [{ ...input, uniqueKeys: undefined }]),
  ).toThrow("metadata is required");
});

test("rejects triggers that exceed D1 SQL size limits", () => {
  expect(() =>
    buildD1CapturePlan({
      ...input,
      columns: Array.from({ length: 100 }, (_, index) => ({
        name: `column_${index}_${"x".repeat(400)}`,
      })),
    }),
  ).toThrow("SQL byte limit");
});

test("D1 captures insert/update/delete, exact int64, binary values and collated text changes", async () => {
  const runtime = new Miniflare({
    modules: true,
    script: "export default {fetch() {return new Response('ok')}}",
    compatibilityDate: "2026-06-01",
    d1Databases: ["DB"],
  });
  try {
    const db = await runtime.getD1Database("DB");
    await db
      .prepare(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, rowid TEXT COLLATE NOCASE, b BLOB, r REAL, n TEXT)",
      )
      .run();
    const plan = buildD1CapturePlan(input);
    await db.prepare(plan.createOutbox).run();
    for (const trigger of plan.triggers) await db.prepare(trigger.sql).run();
    await db
      .prepare("INSERT INTO items VALUES (9223372036854775807, 'A', X'00FF', 1.25, NULL)")
      .run();
    const imageRow = await db
      .prepare(`SELECT ${buildD1SnapshotRowEncoding(input).payloadSql} AS payload FROM items`)
      .first<{ payload: string }>();
    if (imageRow === null) throw new Error("Missing source image");
    await db.prepare("UPDATE items SET rowid = rowid").run();
    await db.prepare("UPDATE items SET rowid = 'a', id = -7").run();
    await db.prepare("DELETE FROM items").run();
    const result = await db
      .prepare(`SELECT operation, before_key, after_key FROM ${D1_CAPTURE_TABLE} ORDER BY sequence`)
      .all<{
        operation: string;
        before_key: string | null;
        after_key: string | null;
      }>();
    expect(result.results.map(({ operation }) => operation)).toStrictEqual([
      "insert",
      "update",
      "delete",
    ]);
    const inserted = result.results[0];
    if (inserted === undefined) throw new Error("Missing captured insert");
    expect(inserted.after_key).toBe("9223372036854775807");
    const payload: unknown = JSON.parse(imageRow.payload);
    expect(payload).toStrictEqual({
      id: { type: "integer", value: "9223372036854775807" },
      rowid: { type: "text", value: "A" },
      b: { type: "blob", value: "00FF" },
      r: { type: "real", value: "1.25" },
      n: { type: "null", value: null },
    });
    expect(result.results[1]).toMatchObject({ before_key: "9223372036854775807", after_key: "-7" });
    expect(result.results[2]).toMatchObject({
      before_key: "-7",
      after_key: null,
    });
    await db.prepare("INSERT INTO items VALUES (2, 'B', NULL, NULL, NULL)").run();
    await db.prepare("UPDATE items SET rowid = 'b'").run();
    expect(
      (await db.prepare(`SELECT count(*) AS n FROM ${D1_CAPTURE_TABLE}`).first<{ n: number }>())?.n,
    ).toBe(5);
    const wide = {
      ...input,
      table: "wide",
      columns: Array.from({ length: 47 }, (_, index) => ({ name: `c${index}` })),
    };
    await db
      .prepare(`CREATE TABLE wide (${wide.columns.map(({ name }) => name).join(", ")})`)
      .run();
    for (const trigger of buildD1CapturePlan(wide).triggers) await db.prepare(trigger.sql).run();
    await db.prepare("INSERT INTO wide DEFAULT VALUES").run();
    const wideRow = await db
      .prepare(`SELECT ${buildD1SnapshotRowEncoding(wide).payloadSql} AS payload FROM wide`)
      .first<{ payload: string }>();
    if (wideRow === null) throw new Error("Missing wide capture");
    const widePayload: unknown = JSON.parse(wideRow.payload);
    if (typeof widePayload !== "object" || widePayload === null)
      throw new Error("Malformed wide capture");
    expect(Object.keys(widePayload)).toHaveLength(47);
    await expect(
      db.batch([
        db.prepare("INSERT INTO items (id) VALUES (3)"),
        db.prepare("INSERT INTO items (id) VALUES (2)"),
      ]),
    ).rejects.toThrow("UNIQUE");
    expect(await db.prepare("SELECT id FROM items WHERE id = 3").first()).toBeNull();
    expect(
      (await db.prepare(`SELECT count(*) AS n FROM ${D1_CAPTURE_TABLE}`).first<{ n: number }>())?.n,
    ).toBe(6);
    // Full JSON images would hex-expand these legal source BLOBs beyond D1's 2 MB limit.
    await db.prepare("INSERT INTO items (id, b) VALUES (4, zeroblob(1500000))").run();
    await db.prepare("UPDATE items SET b = zeroblob(1600000) WHERE id = 4").run();
    expect(
      (await db.prepare("SELECT length(b) AS n FROM items WHERE id = 4").first<{ n: number }>())?.n,
    ).toBe(1600000);
    const largeEvents = await db
      .prepare(`SELECT * FROM ${D1_CAPTURE_TABLE} WHERE after_key = '4'`)
      .all();
    expect(largeEvents.results).toHaveLength(2);
    expect(JSON.stringify(largeEvents.results).length).toBeLessThan(1024);
    const replacement = {
      ...input,
      table: "replacements",
      uniqueKeys: [{ partial: false, columns: [{ name: "value", collation: "NOCASE" }] }],
      columns: [{ name: "id" }, { name: "value" }],
    };
    await db
      .prepare(
        "CREATE TABLE replacements (id INTEGER PRIMARY KEY, value TEXT COLLATE NOCASE UNIQUE)",
      )
      .run();
    const queryMetadata: D1CaptureMetadataQuery = async (request) =>
      (
        await db
          .prepare(request.sql)
          .bind(...request.params)
          .all<Record<string, unknown>>()
      ).results;
    const discovered = await discoverD1CaptureSchema(replacement.table, queryMetadata);
    expect(discovered.uniqueKeys).toStrictEqual(replacement.uniqueKeys);
    for (const trigger of buildD1CapturePlan({ ...discovered, captureId: input.captureId })
      .triggers)
      await db.prepare(trigger.sql).run();
    await db.prepare("INSERT INTO replacements VALUES (10, 'same')").run();
    await db.prepare("INSERT OR REPLACE INTO replacements VALUES (11, 'SAME')").run();
    const replacementEvents = await db
      .prepare(
        `SELECT operation, before_key, after_key FROM ${D1_CAPTURE_TABLE} WHERE table_name = 'replacements' ORDER BY sequence`,
      )
      .all();
    expect(replacementEvents.results).toContainEqual({
      operation: "touch",
      before_key: "10",
      after_key: "10",
    });
    expect(await db.prepare("SELECT id FROM replacements WHERE id = 10").first()).toBeNull();
    await db.prepare("INSERT INTO replacements VALUES (12, 'second')").run();
    await db.prepare("UPDATE OR REPLACE replacements SET value = 'second' WHERE id = 11").run();
    const touched = await db
      .prepare(
        `SELECT before_key FROM ${D1_CAPTURE_TABLE} WHERE table_name = 'replacements' AND operation = 'touch'`,
      )
      .all();
    expect(touched.results).toContainEqual({ before_key: "12" });
    expect(await db.prepare("SELECT id FROM replacements WHERE id = 12").first()).toBeNull();
    await db.prepare("CREATE UNIQUE INDEX partial_key ON replacements(value) WHERE id > 0").run();
    await expect(discoverD1CaptureSchema(replacement.table, queryMetadata)).rejects.toThrow(
      "Unsupported unique index predicate",
    );
    await db
      .prepare(
        "CREATE TABLE dedupe (id INTEGER PRIMARY KEY, message TEXT, redrive INTEGER NOT NULL)",
      )
      .run();
    await db
      .prepare(
        "CREATE UNIQUE INDEX dedupe_key ON dedupe(message, redrive) WHERE message IS NOT NULL",
      )
      .run();
    const dedupe = await discoverD1CaptureSchema("dedupe", queryMetadata);
    for (const trigger of buildD1CapturePlan({ ...dedupe, captureId: input.captureId }).triggers)
      await db.prepare(trigger.sql).run();
    await db.prepare("INSERT INTO dedupe VALUES (31, NULL, 0), (32, NULL, 0), (33, 'm', 0)").run();
    await db.prepare("INSERT OR REPLACE INTO dedupe VALUES (34, 'm', 0)").run();
    expect(
      (
        await db
          .prepare(
            `SELECT before_key FROM ${D1_CAPTURE_TABLE} WHERE table_name = 'dedupe' AND operation = 'touch'`,
          )
          .all()
      ).results,
    ).toStrictEqual([{ before_key: "33" }]);
    expect(await db.prepare("SELECT id FROM dedupe WHERE id = 33").first()).toBeNull();
    const ignored = await db
      .prepare("INSERT OR IGNORE INTO dedupe VALUES (34, 'm', 0) RETURNING id")
      .all();
    expect(ignored.results).toStrictEqual([]);
    // A row conflicting through both its rowid and unique key must be journalled once.
    expect(
      (
        await db
          .prepare(
            `SELECT before_key FROM ${D1_CAPTURE_TABLE} WHERE table_name='dedupe' AND before_key='34' AND operation='touch'`,
          )
          .all()
      ).results,
    ).toStrictEqual([{ before_key: "34" }]);
    const claimed = await db
      .prepare(
        "INSERT INTO dedupe VALUES (34, 'm', 0) ON CONFLICT(id) DO UPDATE SET message=excluded.message WHERE dedupe.redrive=0 RETURNING id",
      )
      .all();
    expect(claimed.results).toStrictEqual([{ id: 34 }]);
    const busy = await db
      .prepare(
        "INSERT INTO dedupe VALUES (34, 'm', 0) ON CONFLICT(id) DO UPDATE SET message=excluded.message WHERE dedupe.redrive=1 RETURNING id",
      )
      .all();
    expect(busy.results).toStrictEqual([]);
    await db.prepare("CREATE TABLE no_rowid (id TEXT PRIMARY KEY) WITHOUT ROWID").run();
    await expect(discoverD1CaptureSchema("no_rowid", queryMetadata)).rejects.toThrow("rowid");
  } finally {
    await runtime.dispose();
  }
}, 20000);
