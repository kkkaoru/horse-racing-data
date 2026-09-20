// Runs with bun via Vitest; source, target and checkpoint I/O are mocked.
import { expect, test, vi } from "vitest";
import {
  parseVectorBackfillCheckpoint,
  submitVectorBackfillBatch,
  type VectorBackfillCheckpoint,
} from "./vector-backfill";

const state: VectorBackfillCheckpoint = {
  namespace: "corner-20260916",
  cursor: { source: "", year: "", monthDay: "", venue: "", race: "", horse: "" },
  submittedRows: 0,
  lastMutationId: null,
  phase: "submitting",
};
const row: Record<string, unknown> = {
  source: "jra",
  kaisai_nen: "2026",
  kaisai_tsukihi: "0913",
  keibajo_code: "06",
  race_bango: "01",
  ketto_toroku_bango: "2023100001",
  race_date: "20260913",
  feature_vector: "[0,0,0,0,0,0,0,0]",
};

test("parses persisted progress with a matching generation", () => {
  expect(parseVectorBackfillCheckpoint(state, "corner-20260916").submittedRows).toBe(0);
  expect(
    parseVectorBackfillCheckpoint(
      { ...state, phase: "submitted", lastMutationId: "m1" },
      "corner-20260916",
    ).phase,
  ).toBe("submitted");
});

test.each([
  null,
  [],
  {},
  { ...state, namespace: "other" },
  { ...state, cursor: null },
  { ...state, submittedRows: "1" },
  { ...state, submittedRows: -1 },
  { ...state, submittedRows: 1.5 },
  { ...state, lastMutationId: 1 },
  { ...state, phase: "complete" },
  ...["source", "year", "monthDay", "venue", "race", "horse"].map((key) => ({
    ...state,
    cursor: { ...state.cursor, [key]: null },
  })),
])("rejects corrupt or wrong-generation checkpoints", (value) => {
  expect(() => parseVectorBackfillCheckpoint(value, "corner-20260916")).toThrow();
});

test("advances only after a bounded batch is accepted", async () => {
  const dependencies = {
    load: vi.fn().mockResolvedValue([row]),
    upsert: vi.fn().mockResolvedValue({ mutationId: "m1" }),
    checkpoint: vi.fn(),
  };
  const next: VectorBackfillCheckpoint = await submitVectorBackfillBatch({
    batchSize: 100,
    state,
    dependencies,
  });
  expect(next).toStrictEqual({
    namespace: "corner-20260916",
    cursor: {
      source: "jra",
      year: "2026",
      monthDay: "0913",
      venue: "06",
      race: "01",
      horse: "2023100001",
    },
    submittedRows: 1,
    lastMutationId: "m1",
    phase: "submitting",
  });
  expect(dependencies.load).toHaveBeenCalledWith(["", "", "", "", "", "", 100]);
  expect(dependencies.upsert.mock.invocationCallOrder[0]).toBeLessThan(
    Number(dependencies.checkpoint.mock.invocationCallOrder[0]),
  );
});

test("marks submission exhausted without claiming visibility or cutover", async () => {
  const dependencies = {
    load: vi.fn().mockResolvedValue([]),
    upsert: vi.fn(),
    checkpoint: vi.fn(),
  };
  const next: VectorBackfillCheckpoint = await submitVectorBackfillBatch({
    batchSize: 100,
    state,
    dependencies,
  });
  expect(next.phase).toBe("submitted");
  expect(dependencies.upsert).not.toHaveBeenCalled();
  expect(dependencies.checkpoint).toHaveBeenCalledTimes(1);
});

test("does not reread an exhausted cursor", async () => {
  const dependencies = { load: vi.fn(), upsert: vi.fn(), checkpoint: vi.fn() };
  await submitVectorBackfillBatch({
    batchSize: 100,
    state: { ...state, phase: "submitted" },
    dependencies,
  });
  expect(dependencies.load).not.toHaveBeenCalled();
});

test.each([0, 501, 1.5])("rejects an invalid batch size: %s", async (batchSize) => {
  const dependencies = { load: vi.fn(), upsert: vi.fn(), checkpoint: vi.fn() };
  await expect(submitVectorBackfillBatch({ batchSize, state, dependencies })).rejects.toThrow(
    "Invalid vector backfill batch size",
  );
  expect(dependencies.load).not.toHaveBeenCalled();
});

test("rejects a source that violates the batch bound", async () => {
  const dependencies = {
    load: vi.fn().mockResolvedValue([row, row]),
    upsert: vi.fn(),
    checkpoint: vi.fn(),
  };
  await expect(submitVectorBackfillBatch({ batchSize: 1, state, dependencies })).rejects.toThrow(
    "Source exceeded",
  );
  expect(dependencies.checkpoint).not.toHaveBeenCalled();
});

test("does not advance after a target error", async () => {
  const dependencies = {
    load: vi.fn().mockResolvedValue([row]),
    upsert: vi.fn().mockRejectedValue(new Error("Target unavailable")),
    checkpoint: vi.fn(),
  };
  await expect(submitVectorBackfillBatch({ batchSize: 100, state, dependencies })).rejects.toThrow(
    "Target unavailable",
  );
  expect(dependencies.checkpoint).not.toHaveBeenCalled();
});

test("does not advance without an acceptance receipt", async () => {
  const dependencies = {
    load: vi.fn().mockResolvedValue([row]),
    upsert: vi.fn().mockResolvedValue({ mutationId: "" }),
    checkpoint: vi.fn(),
  };
  await expect(submitVectorBackfillBatch({ batchSize: 100, state, dependencies })).rejects.toThrow(
    "Missing Vectorize",
  );
  expect(dependencies.checkpoint).not.toHaveBeenCalled();
});

test("replays deterministic IDs if checkpoint persistence fails after acceptance", async () => {
  const dependencies = {
    load: vi.fn().mockResolvedValue([row]),
    upsert: vi.fn().mockResolvedValue({ mutationId: "m1" }),
    checkpoint: vi
      .fn()
      .mockRejectedValueOnce(new Error("Disk unavailable"))
      .mockResolvedValue(undefined),
  };
  await expect(submitVectorBackfillBatch({ batchSize: 100, state, dependencies })).rejects.toThrow(
    "Disk unavailable",
  );
  await submitVectorBackfillBatch({ batchSize: 100, state, dependencies });
  expect(new Set(dependencies.upsert.mock.calls.map((call) => JSON.stringify(call[0]))).size).toBe(
    1,
  );
});
