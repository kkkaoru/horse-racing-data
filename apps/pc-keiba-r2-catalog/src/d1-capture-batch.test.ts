// Runs with bun via Vitest; immutable identities distinguish database recreation and event content.
import { expect, test } from "vitest";
import { prepareD1CaptureBatch, type D1CaptureBatchInput } from "./d1-capture-batch";

const input: D1CaptureBatchInput = {
  databaseName: "sample-db",
  databaseId: "00000000-0000-0000-0000-000000000001",
  request: { afterSequence: "0", throughSequence: "9", limit: 1000 },
  registrations: [{ captureId: "test", table: "items", schemaHash: "0".repeat(64) }],
  rows: [
    {
      sequence: "1",
      capture_id: "test",
      table_name: "items",
      schema_hash: "0".repeat(64),
      operation: "insert",
      before_key: null,
      after_key: "9007199254740993",
      captured_at: "2026-09-16T00:00:00.000Z",
    },
  ],
};

test("serializes exact events without treating read bounds as publication identity", () => {
  const first = prepareD1CaptureBatch(input);
  expect(first).not.toBeNull();
  expect(first).toMatchObject({
    eventCount: 1,
    lastSequence: "1",
    batchId: "e15ddaf1e5949b8dba1549904f394b0c0f7f1d342244e28eced8652abe6fa5ad",
  });
  if (first === null) throw new Error("Expected nonempty artifact");
  expect(
    prepareD1CaptureBatch({ ...input, request: { ...input.request, throughSequence: "100" } }),
  ).toMatchObject(first);
  const parsed: unknown = JSON.parse(first.serialized);
  expect(parsed).toMatchObject({
    formatVersion: 1,
    databaseName: "sample-db",
    databaseId: "00000000-0000-0000-0000-000000000001",
    events: [{ sequence: "1", afterKey: "9007199254740993" }],
  });
});

test("does not publish empty pages or advance their cursors", () => {
  expect(prepareD1CaptureBatch({ ...input, rows: [] })).toBeNull();
});

test.each([
  { databaseName: "bad/name" },
  { databaseName: "sample\n" },
  { databaseId: "bad" },
  { databaseId: "00000000-0000-0000-0000-000000000001\n" },
])("rejects invalid database identities", (override) => {
  expect(() => prepareD1CaptureBatch({ ...input, ...override })).toThrow("database identity");
});

test("database recreation and different events cannot reuse an artifact identity", () => {
  const original = prepareD1CaptureBatch(input);
  const recreated = prepareD1CaptureBatch({
    ...input,
    databaseId: "00000000-0000-0000-0000-000000000002",
  });
  const changed = prepareD1CaptureBatch({
    ...input,
    rows: input.rows.map((row) => ({ ...row, after_key: "2" })),
  });
  expect(new Set([original?.batchId, recreated?.batchId, changed?.batchId]).size).toBe(3);
});

test("counts UTF-8 bytes and rejects a large artifact before any caller can publish it", () => {
  const table = "馬".repeat(256);
  const captureId = "a".repeat(64);
  const rows = Array.from({ length: 1000 }, (_, index) => ({
    ...input.rows[0],
    sequence: String(index + 1),
    table_name: table,
    capture_id: captureId,
  }));
  expect(() =>
    prepareD1CaptureBatch({
      ...input,
      request: { ...input.request, throughSequence: "1000" },
      registrations: [{ table, captureId, schemaHash: "0".repeat(64) }],
      rows,
    }),
  ).toThrow("exceeds byte limit");
});
