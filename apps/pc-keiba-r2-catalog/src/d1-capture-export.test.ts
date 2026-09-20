// Runs with bun via Vitest; fault injection for immutable publication/checkpoint ordering.
import { expect, test, vi } from "vitest";
import { prepareD1CaptureBatch, type D1CaptureBatchArtifact } from "./d1-capture-batch";
import {
  exportD1CaptureStep,
  parseD1CaptureExportState,
  type D1CaptureExportConfig,
  type D1CaptureExportDependencies,
  type D1CaptureExportState,
} from "./d1-capture-export";

const config: D1CaptureExportConfig = {
  databaseName: "sample-db",
  databaseId: "00000000-0000-0000-0000-000000000001",
  registrations: [{ captureId: "test", table: "items", schemaHash: "0".repeat(64) }],
};
const state: D1CaptureExportState = {
  databaseName: "sample-db",
  databaseId: "00000000-0000-0000-0000-000000000001",
  afterSequence: "0",
  pending: null,
};
const rows: readonly Record<string, unknown>[] = [
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
];
const receipt = {
  batchId: "e15ddaf1e5949b8dba1549904f394b0c0f7f1d342244e28eced8652abe6fa5ad",
  eventCount: 1,
  lastSequence: "1",
  reconciled: true,
};
const artifact = (): D1CaptureBatchArtifact => {
  const result = prepareD1CaptureBatch({
    ...config,
    rows,
    request: { afterSequence: "0", throughSequence: "1", limit: 1000 },
  });
  if (result === null) throw new Error("Expected fixture artifact");
  return result;
};
const dependencies = () => ({
  load: vi
    .fn<D1CaptureExportDependencies["load"]>()
    .mockResolvedValue({ rows, throughSequence: "1" }),
  retainArtifact: vi
    .fn<D1CaptureExportDependencies["retainArtifact"]>()
    .mockResolvedValue(undefined),
  checkpoint: vi.fn<D1CaptureExportDependencies["checkpoint"]>().mockResolvedValue(undefined),
  publish: vi.fn<D1CaptureExportDependencies["publish"]>().mockResolvedValue(receipt),
});

test("persists artifact and pending cursor before publication; only then advances", async () => {
  const calls: string[] = [];
  const deps: D1CaptureExportDependencies = {
    load: async () => {
      calls.push("load");
      return { rows, throughSequence: "1" };
    },
    retainArtifact: async () => {
      calls.push("artifact");
    },
    checkpoint: async (next) => {
      calls.push(next.pending === null ? "advance" : "pending");
    },
    publish: async () => {
      calls.push("publish");
      return receipt;
    },
  };
  expect(
    await exportD1CaptureStep({ config, state, pageSize: 1000, dependencies: deps }),
  ).toStrictEqual({
    databaseName: "sample-db",
    databaseId: "00000000-0000-0000-0000-000000000001",
    afterSequence: "1",
    pending: null,
  });
  expect(calls).toStrictEqual(["load", "artifact", "pending", "publish", "advance"]);
});

test("empty reads do not publish or advance even across an exhausted range", async () => {
  const deps = dependencies();
  deps.load.mockResolvedValue({ rows: [], throughSequence: "9" });
  expect(
    (await exportD1CaptureStep({ config, state, pageSize: 1000, dependencies: deps }))
      .afterSequence,
  ).toBe("0");
  expect(deps.retainArtifact).not.toHaveBeenCalled();
  expect(deps.checkpoint).not.toHaveBeenCalled();
  expect(deps.publish).not.toHaveBeenCalled();
});

test("replays validated immutable pending bytes without reading or regenerating source data", async () => {
  const deps = dependencies();
  expect(
    (
      await exportD1CaptureStep({
        config,
        state: { ...state, pending: artifact() },
        pageSize: 1000,
        dependencies: deps,
      })
    ).afterSequence,
  ).toBe("1");
  expect(deps.load).not.toHaveBeenCalled();
  expect(deps.retainArtifact).not.toHaveBeenCalled();
  expect(deps.publish).toHaveBeenCalledTimes(1);
  expect(deps.checkpoint).toHaveBeenCalledTimes(1);
});

test("artifact or pending-checkpoint failures prevent publication", async () => {
  const deps = dependencies();
  deps.retainArtifact.mockRejectedValueOnce(new Error("artifact disk failed"));
  await expect(
    exportD1CaptureStep({ config, state, pageSize: 1000, dependencies: deps }),
  ).rejects.toThrow("artifact disk failed");
  expect(deps.checkpoint).not.toHaveBeenCalled();
  deps.checkpoint.mockRejectedValueOnce(new Error("checkpoint disk failed"));
  await expect(
    exportD1CaptureStep({ config, state, pageSize: 1000, dependencies: deps }),
  ).rejects.toThrow("checkpoint disk failed");
  expect(deps.publish).not.toHaveBeenCalled();
});

test("lost acknowledgement leaves a durable pending cursor and never retries blindly", async () => {
  const deps = dependencies();
  deps.publish.mockRejectedValueOnce(new Error("ack lost"));
  await expect(
    exportD1CaptureStep({ config, state, pageSize: 1000, dependencies: deps }),
  ).rejects.toThrow("ack lost");
  expect(deps.publish).toHaveBeenCalledTimes(1);
  expect(deps.checkpoint).toHaveBeenCalledTimes(1);
  expect(deps.checkpoint.mock.calls[0]?.[0]).toMatchObject({
    afterSequence: "0",
    pending: { lastSequence: "1" },
  });
});

test("failed final checkpoint can replay a verified commit from the retained pending state", async () => {
  const deps = dependencies();
  deps.checkpoint
    .mockResolvedValueOnce(undefined)
    .mockRejectedValueOnce(new Error("advance failed"));
  await expect(
    exportD1CaptureStep({ config, state, pageSize: 1000, dependencies: deps }),
  ).rejects.toThrow("advance failed");
  const retained = deps.checkpoint.mock.calls[0]?.[0];
  if (retained === undefined) throw new Error("Expected retained state");
  deps.load.mockClear();
  expect(
    (await exportD1CaptureStep({ config, state: retained, pageSize: 1000, dependencies: deps }))
      .afterSequence,
  ).toBe("1");
  expect(deps.load).not.toHaveBeenCalled();
});

test.each([
  null,
  { ...receipt, reconciled: false },
  { ...receipt, batchId: "wrong" },
  { ...receipt, lastSequence: "2" },
  { ...receipt, eventCount: 2 },
])("invalid receipts cannot advance the durable cursor", async (invalid) => {
  const deps = dependencies();
  deps.publish.mockResolvedValue(invalid);
  await expect(
    exportD1CaptureStep({ config, state, pageSize: 1000, dependencies: deps }),
  ).rejects.toThrow("receipt mismatch");
  expect(deps.checkpoint).toHaveBeenCalledTimes(1);
});

test.each([
  null,
  { ...state, databaseId: "other" },
  { ...state, databaseName: "other" },
  { ...state, afterSequence: 1 },
  { ...state, afterSequence: "-1" },
  { ...state, pending: undefined },
])("invalid checkpoints fail closed", (invalid) => {
  expect(() => parseD1CaptureExportState(invalid, config)).toThrow();
});

test.each([
  { serialized: 1 },
  { serialized: "x".repeat(1048577) },
  { lastSequence: 1 },
  { batchId: "wrong" },
  { lastSequence: "2" },
  { eventCount: 2 },
])("rejects altered pending artifact metadata", (invalid) => {
  expect(() =>
    parseD1CaptureExportState({ ...state, pending: { ...artifact(), ...invalid } }, config),
  ).toThrow();
});

test.each([
  null,
  {
    formatVersion: 2,
    databaseName: "sample-db",
    databaseId: "00000000-0000-0000-0000-000000000001",
    events: [],
  },
  {
    formatVersion: 1,
    databaseName: "other",
    databaseId: "00000000-0000-0000-0000-000000000001",
    events: [],
  },
  { formatVersion: 1, databaseName: "sample-db", databaseId: "other", events: [] },
  {
    formatVersion: 1,
    databaseName: "sample-db",
    databaseId: "00000000-0000-0000-0000-000000000001",
    events: null,
  },
  {
    formatVersion: 1,
    databaseName: "sample-db",
    databaseId: "00000000-0000-0000-0000-000000000001",
    events: [null],
  },
  {
    formatVersion: 1,
    databaseName: "sample-db",
    databaseId: "00000000-0000-0000-0000-000000000001",
    events: [],
  },
])("rejects malformed or wrong-database pending bodies", (document) => {
  expect(() =>
    parseD1CaptureExportState(
      { ...state, pending: { ...artifact(), serialized: JSON.stringify(document) } },
      config,
    ),
  ).toThrow();
});

test("rejects noncanonical pending bytes and an already advanced cursor", () => {
  expect(() =>
    parseD1CaptureExportState(
      { ...state, pending: { ...artifact(), serialized: artifact().serialized + " " } },
      config,
    ),
  ).toThrow("artifact mismatch");
  expect(() =>
    parseD1CaptureExportState({ ...state, afterSequence: "1", pending: artifact() }, config),
  ).toThrow("ordered range");
});
