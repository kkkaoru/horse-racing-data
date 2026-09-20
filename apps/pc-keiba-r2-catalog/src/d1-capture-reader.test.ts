// Runs with bun via Vitest; exact journal cursors and operation validation.
import { Miniflare } from "miniflare";
import { expect, test } from "vitest";
import {
  buildD1CapturePageQuery,
  parseD1CapturePage,
  type D1CapturePageRequest,
  type D1CaptureRegistration,
} from "./d1-capture-reader";
import { buildD1CapturePlan } from "./d1-change-capture";

const registration: D1CaptureRegistration = {
  captureId: "test",
  table: "items",
  schemaHash: "0".repeat(64),
};
const request: D1CapturePageRequest = {
  afterSequence: "0",
  throughSequence: "9223372036854775807",
  limit: 1000,
};
const row: Record<string, unknown> = {
  sequence: "9007199254740993",
  capture_id: "test",
  table_name: "items",
  schema_hash: "0".repeat(64),
  operation: "insert",
  before_key: null,
  after_key: "9223372036854775807",
  captured_at: "2026-09-16T00:00:00.000Z",
};

test("uses an indexed global range, exact bound strings and a bounded limit", () => {
  const query = buildD1CapturePageQuery(request);
  expect(query.params).toStrictEqual(["0", "9223372036854775807", 1000]);
  expect(query.sql).toMatch(
    /WHERE sequence > CAST\(\? AS INTEGER\) AND sequence <= CAST\(\? AS INTEGER\) ORDER BY sequence LIMIT \?$/u,
  );
  expect(parseD1CapturePage({ rows: [row], request, registrations: [registration] })).toMatchObject(
    {
      lastSequence: "9007199254740993",
      rangeExhausted: true,
      events: [{ afterKey: "9223372036854775807", beforeKey: null }],
    },
  );
});

test.each([
  { afterSequence: "1\n" },
  { afterSequence: "-1" },
  { afterSequence: "01" },
  { afterSequence: "-0" },
  { afterSequence: "x" },
  { afterSequence: "9223372036854775808" },
  { afterSequence: "123456789012345678901" },
  { throughSequence: "-1" },
  { afterSequence: "3", throughSequence: "2" },
  { limit: 0 },
  { limit: 1001 },
  { limit: 1.5 },
])("rejects invalid page bounds", (override) => {
  expect(() => buildD1CapturePageQuery({ ...request, ...override })).toThrow(
    "Invalid capture page range",
  );
});

test("empty pages preserve their cursor; full pages require another read", () => {
  expect(parseD1CapturePage({ rows: [], request, registrations: [registration] })).toStrictEqual({
    events: [],
    lastSequence: "0",
    rangeExhausted: true,
  });
  expect(
    parseD1CapturePage({
      rows: [row],
      request: { ...request, limit: 1 },
      registrations: [registration],
    }).rangeExhausted,
  ).toBe(false);
  expect(() =>
    parseD1CapturePage({
      rows: [row, row],
      request: { ...request, limit: 1 },
      registrations: [registration],
    }),
  ).toThrow("exceeded requested limit");
});

test.each([
  { sequence: 1 },
  { capture_id: null },
  { table_name: null },
  { schema_hash: null },
  { operation: "replace" },
  { before_key: 1 },
  { after_key: "-9223372036854775809" },
  { captured_at: null },
  { captured_at: "bad" },
  { captured_at: "2026-99-16T00:00:00.000Z" },
  { captured_at: "2026-02-30T00:00:00.000Z" },
])("rejects malformed event fields", (override) => {
  expect(() =>
    parseD1CapturePage({ rows: [{ ...row, ...override }], request, registrations: [registration] }),
  ).toThrow("Invalid capture event");
});

test.each([
  { operation: "insert", before_key: "1", after_key: "1" },
  { operation: "insert", before_key: null, after_key: null },
  { operation: "delete", before_key: null, after_key: null },
  { operation: "delete", before_key: "1", after_key: "1" },
  { operation: "update", before_key: null, after_key: "1" },
  { operation: "update", before_key: "1", after_key: null },
  { operation: "touch", before_key: null, after_key: null },
  { operation: "touch", before_key: "1", after_key: "2" },
])("rejects impossible operation/key pairs", (override) => {
  expect(() =>
    parseD1CapturePage({ rows: [{ ...row, ...override }], request, registrations: [registration] }),
  ).toThrow("operation keys");
});

test.each([
  { captureId: "" },
  { captureId: "test\n" },
  { table: "" },
  { table: "a".repeat(257) },
  { schemaHash: "bad" },
  { schemaHash: "0".repeat(64) + "\n" },
  { schemaHash: "g".repeat(64) },
])("rejects invalid registrations", (override) => {
  expect(() =>
    parseD1CapturePage({ rows: [], request, registrations: [{ ...registration, ...override }] }),
  ).toThrow("Invalid capture registration");
});

test("fails closed for duplicate registrations, drift, unknown sources, and unordered cursors", () => {
  expect(() =>
    parseD1CapturePage({ rows: [], request, registrations: [registration, registration] }),
  ).toThrow("Invalid capture registration");
  expect(() => parseD1CapturePage({ rows: [row], request, registrations: [] })).toThrow(
    "Unregistered capture schema",
  );
  expect(() =>
    parseD1CapturePage({
      rows: [{ ...row, schema_hash: "1".repeat(64) }],
      request,
      registrations: [registration],
    }),
  ).toThrow("Unregistered capture schema");
  expect(() =>
    parseD1CapturePage({ rows: [row, row], request, registrations: [registration] }),
  ).toThrow("outside ordered range");
  expect(() =>
    parseD1CapturePage({
      rows: [row],
      request: { ...request, throughSequence: "1" },
      registrations: [registration],
    }),
  ).toThrow("outside ordered range");
});

test("native REPLACE, rowid moves and deletions remain dirty keys with exact int64 values", async () => {
  const runtime = new Miniflare({
    modules: true,
    script: "export default {}",
    compatibilityDate: "2026-06-18",
    d1Databases: { DB: "capture-reader" },
  });
  try {
    const db = await runtime.getD1Database("DB");
    await db.prepare("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)").run();
    const plan = buildD1CapturePlan({
      table: "items",
      columns: [{ name: "id" }, { name: "value" }],
      captureId: "test",
      uniqueKeys: [],
    });
    await db.batch(
      [plan.createOutbox, ...plan.triggers.map((trigger) => trigger.sql)].map((sql) =>
        db.prepare(sql),
      ),
    );
    await db.prepare("INSERT INTO items VALUES (9007199254740993, 'first')").run();
    await db.prepare("UPDATE items SET id = -9223372036854775808").run();
    await db.prepare("INSERT OR REPLACE INTO items VALUES (-9223372036854775808, 'second')").run();
    await db.prepare("DELETE FROM items").run();
    const query = buildD1CapturePageQuery(request);
    const rows = (
      await db
        .prepare(query.sql)
        .bind(...query.params)
        .all<Record<string, unknown>>()
    ).results;
    const page = parseD1CapturePage({
      rows,
      request,
      registrations: [{ captureId: "test", table: "items", schemaHash: plan.schemaHash }],
    });
    expect(
      page.events.map(({ operation, beforeKey, afterKey }) => [operation, beforeKey, afterKey]),
    ).toStrictEqual([
      ["insert", null, "9007199254740993"],
      ["update", "9007199254740993", "-9223372036854775808"],
      ["touch", "-9223372036854775808", "-9223372036854775808"],
      ["insert", null, "-9223372036854775808"],
      ["delete", "-9223372036854775808", null],
    ]);
    expect(page.lastSequence).toBe("5");
  } finally {
    await runtime.dispose();
  }
});
