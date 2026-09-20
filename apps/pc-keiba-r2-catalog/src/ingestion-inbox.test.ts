// Runs with bun via Vitest; acceptance/replay validation and storage backpressure boundaries.
import { expect, test } from "vitest";
import { mockDeep } from "vitest-mock-extended";
import { prepareIngestionEnvelope, parseIngestionPointer } from "./ingestion-buffer";
import { IngestionInbox } from "./ingestion-inbox";

const input = { source: "hot", requestId: "message-1", payload: "{}" };

test("persists first receipt, replays it, and rejects changed content and foreign sources", () => {
  const ctx = mockDeep<DurableObjectState>();
  const cursor = mockDeep<SqlStorageCursor<Record<string, SqlStorageValue>>>();
  cursor.toArray.mockReturnValue([]);
  cursor.one.mockReturnValue({ sequence: "9007199254740993" });
  ctx.storage.sql.exec.mockReturnValue(cursor);
  ctx.storage.transactionSync.mockImplementation((action) => action());
  ctx.blockConcurrencyWhile.mockImplementation(async (action) => action());
  Object.defineProperty(ctx.storage.sql, "databaseSize", { value: 4096 });
  const inbox = new IngestionInbox(ctx, {});
  const pointer = prepareIngestionEnvelope(input).pointer;
  expect(inbox.accept(pointer)).toMatchObject({
    source: "hot",
    requestId: "message-1",
    sequence: "9007199254740993",
    accepted: true,
  });
  expect(ctx.storage.kv.put).toHaveBeenCalledWith("ingestion-inbox/source-v1", "hot");
  cursor.toArray.mockReturnValue([
    { sequence: "9007199254740993", descriptor: JSON.stringify(parseIngestionPointer(pointer)) },
  ]);
  ctx.storage.kv.get.mockReturnValue("hot");
  expect(inbox.accept(pointer)).toMatchObject({ sequence: "9007199254740993", accepted: true });
  expect(() => inbox.accept(prepareIngestionEnvelope({ ...input, payload: "[]" }).pointer)).toThrow(
    "reused with different content",
  );
  expect(() =>
    inbox.accept(prepareIngestionEnvelope({ ...input, source: "other" }).pointer),
  ).toThrow("source mismatch");
  expect(() => inbox.accept(null)).toThrow("Invalid ingestion pointer");
});

test("storage budget rejects new work without binding a source, but permits existing receipt replay", () => {
  const ctx = mockDeep<DurableObjectState>();
  const cursor = mockDeep<SqlStorageCursor<Record<string, SqlStorageValue>>>();
  cursor.toArray.mockReturnValue([]);
  ctx.storage.sql.exec.mockReturnValue(cursor);
  ctx.storage.transactionSync.mockImplementation((action) => action());
  Object.defineProperty(ctx.storage.sql, "databaseSize", { value: 8000000000 });
  const inbox = new IngestionInbox(ctx, {});
  const pointer = prepareIngestionEnvelope(input).pointer;
  expect(() => inbox.accept(pointer)).toThrow("storage budget reached");
  expect(ctx.storage.kv.put).not.toHaveBeenCalled();
  ctx.storage.kv.get.mockReturnValue("hot");
  cursor.toArray.mockReturnValue([
    { sequence: "1", descriptor: JSON.stringify(parseIngestionPointer(pointer)) },
  ]);
  expect(inbox.accept(pointer)).toMatchObject({ sequence: "1", accepted: true });
});

test.each([
  [null, 1],
  [1, 1],
  ["-1", 1],
  ["01", 1],
  ["1\n", 1],
  ["9223372036854775808", 1],
  ["0", "1"],
  ["0", 0],
  ["0", 101],
  ["0", 1.5],
  ["0", Number.NaN],
])("rejects unsafe page cursor %s/%s", (after, limit) => {
  const inbox = new IngestionInbox(mockDeep<DurableObjectState>(), {});
  expect(() => inbox.entries(after, limit)).toThrow("Invalid ingestion inbox cursor");
});

test("bounds read pages and rejects corrupt or cross-source stored descriptors", () => {
  const ctx = mockDeep<DurableObjectState>();
  const cursor = mockDeep<SqlStorageCursor<Record<string, SqlStorageValue>>>();
  cursor.toArray.mockReturnValue([]);
  ctx.storage.sql.exec.mockReturnValue(cursor);
  const inbox = new IngestionInbox(ctx, {});
  expect(inbox.entries("0", 1)).toStrictEqual([]);
  ctx.storage.kv.get.mockReturnValue("hot");
  cursor.toArray.mockReturnValue([
    {
      sequence: "9007199254740993",
      descriptor: JSON.stringify(prepareIngestionEnvelope(input).pointer),
    },
  ]);
  expect(inbox.entries("9007199254740992", 100)).toMatchObject([
    { sequence: "9007199254740993", pointer: { source: "hot", requestId: "message-1" } },
  ]);
  expect(ctx.storage.sql.exec).toHaveBeenLastCalledWith(
    "SELECT CAST(sequence AS TEXT) AS sequence, descriptor FROM __ingestion_inbox_v1 WHERE sequence > CAST(? AS INTEGER) ORDER BY sequence LIMIT ?",
    "9007199254740992",
    100,
  );
  ctx.storage.kv.get.mockReturnValue("other");
  expect(() => inbox.entries("0", 1)).toThrow("stored source mismatch");
  cursor.toArray.mockReturnValue([{ sequence: "1", descriptor: "{}" }]);
  expect(() => inbox.entries("0", 1)).toThrow("Invalid ingestion pointer");
});
