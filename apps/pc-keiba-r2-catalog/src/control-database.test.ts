// Runs with bun via Vitest; private RPC validation and atomic outbox/receipt boundaries.
import { createHash } from "node:crypto";
import { expect, test } from "vitest";
import { anyString, mockDeep } from "vitest-mock-extended";
import { IngestionControlDatabase, parseControlCommand } from "./control-database";

const cursor = (rows: Record<string, SqlStorageValue>[], written = 0) => {
  const value = mockDeep<SqlStorageCursor<Record<string, SqlStorageValue>>>();
  value.toArray.mockReturnValue(rows);
  value.one.mockImplementation(() => {
    const first = rows[0];
    if (first === undefined) throw new Error("Missing fixture row");
    return first;
  });
  Object.defineProperty(value, Symbol.iterator, { value: () => rows[Symbol.iterator]() });
  Object.defineProperty(value, "rowsWritten", { value: written });
  Object.defineProperty(value, "rowsRead", { value: rows.length });
  return value;
};
const database = (initialized = true) => {
  const ctx = mockDeep<DurableObjectState>();
  ctx.storage.kv.get.mockReturnValue(initialized ? "initialized" : undefined);
  ctx.storage.kv.put.mockImplementation((_key, value) => {
    ctx.storage.kv.get.mockReturnValue(value);
  });
  ctx.storage.transactionSync.mockImplementation((action) => action());
  ctx.storage.sql.exec.calledWith(anyString()).mockReturnValue(cursor([]));
  ctx.storage.sql.exec
    .calledWith("SELECT changes() AS count")
    .mockReturnValue(cursor([{ count: 1 }]));
  ctx.storage.sql.exec
    .calledWith("SELECT last_insert_rowid() AS value")
    .mockReturnValue(cursor([{ value: 1 }]));
  Object.defineProperty(ctx.storage.sql, "databaseSize", { value: 4096 });
  return { ctx, service: new IngestionControlDatabase(ctx, {}) };
};
const command = { requestId: "r1", statements: [{ sql: "SELECT v FROM items", params: [] }] };

test("accepts bounded primitive SQL bindings", () => {
  expect(
    parseControlCommand({
      requestId: "r",
      statements: [{ sql: "SELECT ?, ?, ?", params: [null, "a", 1.5] }],
    }).statements[0]?.params,
  ).toStrictEqual([null, "a", 1.5]);
});

test.each([
  null,
  [],
  {},
  { ...command, requestId: 1 },
  { ...command, requestId: "../bad" },
  { ...command, statements: [] },
  { ...command, statements: Array.from({ length: 101 }, () => command.statements[0]) },
  { ...command, statements: [null] },
  { ...command, statements: [{ sql: "SELECT 1", params: {} }] },
  { ...command, statements: [{ sql: "SELECT 1", params: Array.from({ length: 1001 }, () => 1) }] },
  { ...command, statements: [{ sql: "SELECT ?", params: [true] }] },
  { ...command, statements: [{ sql: "SELECT ?", params: [Number.NaN] }] },
  { ...command, statements: [{ sql: "", params: [] }] },
  { ...command, statements: [{ sql: 1, params: [] }] },
  { ...command, statements: [{ sql: "x".repeat(65537), params: [] }] },
  { ...command, statements: [{ sql: "SELECT * FROM __control_receipts", params: [] }] },
])("rejects unsafe control commands", (value) => {
  expect(() => parseControlCommand(value)).toThrow();
});

test("rejects command byte overflow before touching storage", () => {
  expect(() =>
    parseControlCommand({
      requestId: "r",
      statements: [{ sql: "SELECT ?", params: ["x".repeat(1024 * 1024)] }],
    }),
  ).toThrow("byte limit");
});

test("bootstraps once and rejects a changed schema", () => {
  const { ctx, service } = database(false);
  const schema = ["CREATE TABLE items (v INTEGER)"];
  expect(service.bootstrap(schema)).toBe(true);
  expect(service.bootstrap(schema)).toBe(false);
  expect(() => service.bootstrap(["CREATE TABLE other (v TEXT)"])).toThrow("differently");
  expect(ctx.storage.kv.put).toHaveBeenCalledOnce();
});

test.each([
  null,
  [],
  Array.from({ length: 101 }, () => "CREATE TABLE t (v TEXT)"),
  ["DROP TABLE items"],
])("rejects invalid bootstrap input", (schema) => {
  const { service } = database(false);
  expect(() => Reflect.apply(service.bootstrap, service, [schema])).toThrow();
});

test("bounds total bootstrap bytes", () => {
  const { service } = database(false);
  expect(() =>
    service.bootstrap(
      Array.from({ length: 60 }, () => `CREATE TABLE t${" ".repeat(20000)}(v TEXT)`),
    ),
  ).toThrow("schema exceeds byte limit");
});

test("refuses commands before bootstrap", () => {
  expect(() => database(false).service.execute(command)).toThrow("not initialized");
});

test("read-only queries retain retry receipts without generating mutation events", async () => {
  const { ctx, service } = database();
  ctx.storage.sql.exec.calledWith("SELECT v FROM items").mockReturnValue(cursor([{ v: "value" }]));
  const response = service.execute(command);
  expect(response.headers.get("Cache-Control")).toBe("no-store");
  expect(await response.json()).toMatchObject([
    {
      success: true,
      results: [{ v: "value" }],
      meta: {
        changes: 0,
        changed_db: false,
        last_row_id: 1,
        rows_read: 1,
        rows_written: 0,
        served_by_primary: true,
      },
    },
  ]);
  expect(
    ctx.storage.sql.exec.mock.calls.filter(([sql]) => sql.startsWith("INSERT INTO __control_")),
  ).toHaveLength(1);
  expect(
    ctx.storage.sql.exec.mock.calls.some(([sql]) => sql.startsWith("INSERT INTO __control_outbox")),
  ).toBe(false);
});

test("mutations persist their receipt and export event before acknowledgement", async () => {
  const { ctx, service } = database();
  ctx.storage.sql.exec
    .calledWith("INSERT INTO items VALUES (?)", "a")
    .mockReturnValue(cursor([], 1));
  const response = service.execute({
    requestId: "write",
    statements: [{ sql: "INSERT INTO items VALUES (?)", params: ["a"] }],
  });
  expect(await response.json()).toMatchObject([{ meta: { changes: 1, changed_db: true } }]);
  expect(
    ctx.storage.sql.exec.mock.calls.filter(([sql]) => sql.startsWith("INSERT INTO __control_")),
  ).toHaveLength(2);
});

test("replays an acknowledged mutation and rejects request-id reuse", async () => {
  const { ctx, service } = database();
  const digest = createHash("sha256").update(JSON.stringify(command.statements)).digest("hex");
  ctx.storage.sql.exec
    .calledWith("SELECT digest, response FROM __control_receipts WHERE request_id = ?", "r1")
    .mockReturnValue(cursor([{ digest, response: "[]" }]));
  expect(await service.execute(command).json()).toStrictEqual([]);
  expect(() =>
    service.execute({ ...command, statements: [{ sql: "SELECT other FROM items", params: [] }] }),
  ).toThrow("reused");
  expect(ctx.storage.sql.exec.mock.calls.some(([sql]) => sql === "SELECT v FROM items")).toBe(
    false,
  );
});

test("rejects unencoded binary results, excessive rows and excessive serialized data", () => {
  const { ctx, service } = database();
  ctx.storage.sql.exec
    .calledWith("SELECT v FROM items")
    .mockReturnValueOnce(cursor([{ v: new ArrayBuffer(1) }]))
    .mockReturnValueOnce(cursor(Array.from({ length: 10001 }, () => ({ v: 1 }))))
    .mockReturnValueOnce(cursor(Array.from({ length: 9 }, () => ({ v: "x".repeat(1024 * 1024) }))));
  expect(() => service.execute(command)).toThrow("Binary control results");
  expect(() => service.execute(command)).toThrow("row limit");
  expect(() => service.execute(command)).toThrow("byte limit");
});

test("caps outbox bytes while retaining a forward cursor", async () => {
  const { ctx, service } = database();
  const event = {
    sequence: 1,
    request_id: "large",
    statements: "x".repeat(900000),
    created_at: "2026-09-16",
  };
  ctx.storage.sql.exec
    .calledWith(
      "SELECT sequence, request_id, statements, created_at FROM __control_outbox WHERE sequence > ? ORDER BY sequence LIMIT ?",
      0,
      100,
    )
    .mockReturnValue(
      cursor(Array.from({ length: 6 }, (_, index) => ({ ...event, sequence: index + 1 }))),
    );
  const response = service.outbox(0, 100);
  expect(response.headers.get("Cache-Control")).toBe("no-store");
  expect(await response.json()).toHaveLength(4);
});

test("reads a bounded, ordered Catalog export outbox", async () => {
  const { ctx, service } = database();
  expect(await service.outbox(0, 100).json()).toStrictEqual([]);
  expect(ctx.storage.sql.exec).toHaveBeenCalledWith(
    "SELECT sequence, request_id, statements, created_at FROM __control_outbox WHERE sequence > ? ORDER BY sequence LIMIT ?",
    0,
    100,
  );
  for (const [after, limit] of [
    [-1, 1],
    [1.5, 1],
    [0, 0],
    [0, 101],
    [0, 1.5],
  ]) {
    expect(() => Reflect.apply(service.outbox, service, [after, limit])).toThrow("cursor");
  }
});
