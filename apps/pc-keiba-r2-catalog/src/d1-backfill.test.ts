// Runs with bun via Vitest; source, files and Catalog publication are mocked.
import { expect, test, vi } from "vitest";
import {
  parseD1BackfillState,
  copyD1BackfillStep,
  type D1BackfillState,
  type D1BackfillStep,
} from "./d1-backfill";

const state: D1BackfillState = {
  snapshotId: "s1",
  databaseName: "db",
  tableName: "events",
  afterRowId: null,
  copiedRows: 0,
  pending: null,
  phase: "copying",
};
const row = { row_key: "1", payload: '{"v":{"type":"integer","value":"9223372036854775807"}}' };
const limits = { batchRows: 50_000, batchBytes: 32 * 1024 * 1024, pageRows: 1000 };

test("validates resumable and completed checkpoints", () => {
  expect(parseD1BackfillState(state, state).copiedRows).toBe(0);
  expect(
    parseD1BackfillState({ ...state, afterRowId: "1", copiedRows: 1, phase: "copied" }, state)
      .phase,
  ).toBe("copied");
  expect(
    parseD1BackfillState(
      { ...state, pending: { path: "/batch", digest: "a".repeat(64), rows: 1, lastRowId: "2" } },
      state,
    ).pending?.rows,
  ).toBe(1);
});

test.each([
  null,
  [],
  {},
  { ...state, databaseName: "other" },
  { ...state, tableName: "other" },
  { ...state, afterRowId: 1 },
  { ...state, afterRowId: "bad" },
  { ...state, afterRowId: "-9223372036854775809" },
  { ...state, afterRowId: "9223372036854775808" },
  { ...state, copiedRows: "1" },
  { ...state, copiedRows: -1 },
  { ...state, copiedRows: 1.5 },
  { ...state, phase: "unknown" },
])("rejects corrupt or mismatched checkpoints", (value) => {
  expect(() => parseD1BackfillState(value, state)).toThrow();
});

test.each([
  undefined,
  {},
  { path: "", digest: "a".repeat(64), rows: 1, lastRowId: "1" },
  { path: "/batch", digest: "bad", rows: 1, lastRowId: "1" },
  { path: "/batch", digest: "a".repeat(64), rows: 0, lastRowId: "1" },
  { path: "/batch", digest: "a".repeat(64), rows: 50001, lastRowId: "1" },
  { path: "/batch", digest: "a".repeat(64), rows: "1", lastRowId: "1" },
  { path: "/batch", digest: "a".repeat(64), rows: 1.5, lastRowId: "1" },
  { path: "/batch", digest: "a".repeat(64), rows: 1, lastRowId: "bad" },
])("rejects invalid pending artifact records", (pending) => {
  expect(() => parseD1BackfillState({ ...state, pending }, state)).toThrow();
});

test("rejects inconsistent progress and non-advancing pending cursors", () => {
  expect(() => parseD1BackfillState({ ...state, copiedRows: 1 }, state)).toThrow(
    "row count disagree",
  );
  expect(() => parseD1BackfillState({ ...state, afterRowId: "1" }, state)).toThrow(
    "row count disagree",
  );
  const pending = { path: "/batch", digest: "a".repeat(64), rows: 1, lastRowId: "1" };
  expect(() =>
    parseD1BackfillState({ ...state, afterRowId: "1", copiedRows: 1, pending }, state),
  ).toThrow("did not advance");
  expect(
    parseD1BackfillState({ ...state, afterRowId: "0", copiedRows: 1, pending }, state).pending
      ?.rows,
  ).toBe(1);
});

test("rejects pending data on a completed checkpoint", () => {
  expect(() =>
    parseD1BackfillState(
      {
        ...state,
        phase: "copied",
        pending: { path: "/batch", digest: "a".repeat(64), rows: 1, lastRowId: "2" },
      },
      state,
    ),
  ).toThrow("still has pending data");
});

test("stages immutable data and pending checkpoint before any Catalog write", async () => {
  const dependencies = {
    read: vi.fn().mockResolvedValueOnce([row]).mockResolvedValue([]),
    stage: vi.fn().mockResolvedValue({ path: "/batch", digest: "hash" }),
    publish: vi.fn().mockResolvedValue({ rows: 1, reconciled: true }),
    checkpoint: vi.fn(),
  };
  const next: D1BackfillState = await copyD1BackfillStep({ ...limits, state, dependencies });
  expect(next).toStrictEqual({
    snapshotId: "s1",
    databaseName: "db",
    tableName: "events",
    afterRowId: "1",
    copiedRows: 1,
    pending: null,
    phase: "copying",
  });
  expect(dependencies.checkpoint.mock.calls[0]?.[0].pending).toStrictEqual({
    path: "/batch",
    digest: "hash",
    lastRowId: "1",
    rows: 1,
  });
  expect(dependencies.stage.mock.invocationCallOrder[0]).toBeLessThan(
    Number(dependencies.checkpoint.mock.invocationCallOrder[0]),
  );
  expect(dependencies.checkpoint.mock.invocationCallOrder[0]).toBeLessThan(
    Number(dependencies.publish.mock.invocationCallOrder[0]),
  );
});

test("resumes the pending artifact without rereading changed source data", async () => {
  const dependencies = {
    read: vi.fn(),
    stage: vi.fn(),
    publish: vi.fn().mockResolvedValue({ rows: 1, reconciled: true }),
    checkpoint: vi.fn(),
  };
  const next: D1BackfillState = await copyD1BackfillStep({
    ...limits,
    state: { ...state, pending: { path: "/saved", digest: "expected", lastRowId: "9", rows: 1 } },
    dependencies,
  });
  expect(next.afterRowId).toBe("9");
  expect(dependencies.read).not.toHaveBeenCalled();
  expect(dependencies.stage).not.toHaveBeenCalled();
  expect(dependencies.publish).toHaveBeenCalledWith({ path: "/saved", digest: "expected" });
});

test("keeps pending progress after an uncertain external commit", async () => {
  const dependencies = {
    read: vi.fn().mockResolvedValueOnce([row]).mockResolvedValue([]),
    stage: vi.fn().mockResolvedValue({ path: "/saved", digest: "hash" }),
    publish: vi.fn().mockRejectedValue(new Error("Commit acknowledgement lost")),
    checkpoint: vi.fn(),
  };
  await expect(copyD1BackfillStep({ ...limits, state, dependencies })).rejects.toThrow(
    "acknowledgement lost",
  );
  expect(dependencies.checkpoint).toHaveBeenCalledTimes(1);
  expect(dependencies.checkpoint.mock.calls[0]?.[0].afterRowId).toBeNull();
  expect(dependencies.checkpoint.mock.calls[0]?.[0].pending.path).toBe("/saved");
});

test("does not publish when persisting pending progress fails", async () => {
  const dependencies = {
    read: vi.fn().mockResolvedValueOnce([row]).mockResolvedValue([]),
    stage: vi.fn().mockResolvedValue({ path: "/saved", digest: "hash" }),
    publish: vi.fn(),
    checkpoint: vi.fn().mockRejectedValue(new Error("Disk unavailable")),
  };
  await expect(copyD1BackfillStep({ ...limits, state, dependencies })).rejects.toThrow(
    "Disk unavailable",
  );
  expect(dependencies.publish).not.toHaveBeenCalled();
});

test.each([
  { rows: 1, reconciled: false },
  { rows: 2, reconciled: true },
])("does not advance a failed reconciliation: %j", async (receipt) => {
  const dependencies = {
    read: vi.fn(),
    stage: vi.fn(),
    publish: vi.fn().mockResolvedValue(receipt),
    checkpoint: vi.fn(),
  };
  await expect(
    copyD1BackfillStep({
      ...limits,
      state: { ...state, pending: { path: "/saved", digest: "hash", lastRowId: "1", rows: 1 } },
      dependencies,
    }),
  ).rejects.toThrow("reconciliation did not match");
  expect(dependencies.checkpoint).not.toHaveBeenCalled();
});

test("marks an exhausted or empty source as copied without creating empty data files", async () => {
  const dependencies = {
    read: vi.fn().mockResolvedValue([]),
    stage: vi.fn(),
    publish: vi.fn(),
    checkpoint: vi.fn(),
  };
  expect((await copyD1BackfillStep({ ...limits, state, dependencies })).phase).toBe("copied");
  expect(dependencies.stage).not.toHaveBeenCalled();
  expect(dependencies.publish).not.toHaveBeenCalled();
});

test("does not restart a copied source", async () => {
  const dependencies = { read: vi.fn(), stage: vi.fn(), publish: vi.fn(), checkpoint: vi.fn() };
  await copyD1BackfillStep({ ...limits, state: { ...state, phase: "copied" }, dependencies });
  expect(dependencies.read).not.toHaveBeenCalled();
});

test("accumulates pages into one bounded row-count batch", async () => {
  const dependencies = {
    read: vi
      .fn()
      .mockResolvedValueOnce([row])
      .mockResolvedValueOnce([{ ...row, row_key: "2" }]),
    stage: vi.fn().mockResolvedValue({ path: "/saved", digest: "hash" }),
    publish: vi.fn().mockResolvedValue({ rows: 2, reconciled: true }),
    checkpoint: vi.fn(),
  };
  await copyD1BackfillStep({ ...limits, batchRows: 2, pageRows: 1, state, dependencies });
  expect(dependencies.read.mock.calls).toStrictEqual([
    [null, 1],
    ["1", 1],
  ]);
  expect(dependencies.publish).toHaveBeenCalledTimes(1);
});

test("stops at the byte budget and leaves excess rows for the next cursor", async () => {
  const dependencies = {
    read: vi.fn().mockResolvedValue([row, { ...row, row_key: "2" }]),
    stage: vi.fn().mockResolvedValue({ path: "/saved", digest: "hash" }),
    publish: vi.fn().mockResolvedValue({ rows: 1, reconciled: true }),
    checkpoint: vi.fn(),
  };
  const next: D1BackfillState = await copyD1BackfillStep({
    ...limits,
    batchBytes: 230,
    state,
    dependencies,
  });
  expect(next.afterRowId).toBe("1");
  expect(next.copiedRows).toBe(1);
});

test("rejects a row that cannot fit the byte budget", async () => {
  const dependencies = {
    read: vi.fn().mockResolvedValue([row]),
    stage: vi.fn(),
    publish: vi.fn(),
    checkpoint: vi.fn(),
  };
  await expect(
    copyD1BackfillStep({ ...limits, batchBytes: 1, state, dependencies }),
  ).rejects.toThrow("row exceeds batch byte limit");
});

test("rejects duplicate source cursors and excess pages", async () => {
  const dependencies = {
    read: vi.fn().mockResolvedValue([row, row]),
    stage: vi.fn(),
    publish: vi.fn(),
    checkpoint: vi.fn(),
  };
  await expect(copyD1BackfillStep({ ...limits, state, dependencies })).rejects.toThrow(
    "cursor did not advance",
  );
  await expect(copyD1BackfillStep({ ...limits, pageRows: 1, state, dependencies })).rejects.toThrow(
    "source exceeded page limit",
  );
});

test.each<Partial<D1BackfillStep>>([
  { batchRows: 0 },
  { batchRows: 50_001 },
  { batchRows: 1.5 },
  { batchBytes: 0 },
  { batchBytes: 64 * 1024 * 1024 + 1 },
  { batchBytes: 1.5 },
  { pageRows: 0 },
  { pageRows: 1001 },
  { pageRows: 1.5 },
])("rejects invalid limits before I/O: %j", async (overrides) => {
  const dependencies = { read: vi.fn(), stage: vi.fn(), publish: vi.fn(), checkpoint: vi.fn() };
  await expect(
    copyD1BackfillStep({ ...limits, state, dependencies, ...overrides }),
  ).rejects.toThrow("Invalid D1 backfill batch configuration");
  expect(dependencies.read).not.toHaveBeenCalled();
});
