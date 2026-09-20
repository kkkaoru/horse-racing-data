// Run with bun.
import { afterEach, expect, test, vi } from "vitest";
import { syncChangedDays } from "./sync-checkpoint";

const makeInput = () => ({
  dateFrom: "20260911",
  dateTo: "20260913",
  now: 3600000,
  probe: vi.fn(async () => [{ date: "20260911", fingerprint: "changed" }]),
  store: {
    get: vi.fn(async () => ({ fingerprint: "original", syncedAt: 3500000 })),
    put: vi.fn(async () => undefined),
  },
  sync: vi.fn(async () => undefined),
});

afterEach(() => vi.restoreAllMocks());

test("syncs changed days and checkpoints only after success", async () => {
  const input = makeInput();
  await syncChangedDays(input);
  expect(input.sync).toHaveBeenCalledWith("20260911", "20260911");
  expect(input.store.put).toHaveBeenCalledWith("preview-source-v1:20260911", {
    fingerprint: "changed",
    syncedAt: 3600000,
  });
});

test("unchanged source does not start a Container", async () => {
  const input = makeInput();
  input.store.get.mockResolvedValue({ fingerprint: "changed", syncedAt: 3500000 });
  await syncChangedDays(input);
  expect(input.sync).not.toHaveBeenCalled();
  expect(input.store.put).not.toHaveBeenCalled();
});

test("hourly reconciliation repairs independently changed MLflow data", async () => {
  const input = makeInput();
  input.store.get.mockResolvedValue({ fingerprint: "changed", syncedAt: 0 });
  await syncChangedDays(input);
  expect(input.sync).toHaveBeenCalledOnce();
});

test("clock reversal forces reconciliation", async () => {
  const input = makeInput();
  input.store.get.mockResolvedValue({ fingerprint: "changed", syncedAt: 3600001 });
  await syncChangedDays(input);
  expect(input.sync).toHaveBeenCalledOnce();
});

test("missing checkpoint syncs", async () => {
  const input = makeInput();
  await syncChangedDays({ ...input, store: { ...input.store, get: async () => undefined } });
  expect(input.sync).toHaveBeenCalledOnce();
});

test("probe failure falls back to the full window without checkpointing", async () => {
  const input = makeInput();
  vi.spyOn(console, "warn").mockImplementation(() => undefined);
  input.probe.mockRejectedValue(new Error("database unavailable"));
  await syncChangedDays(input);
  expect(input.sync).toHaveBeenCalledWith("20260911", "20260913");
  expect(input.store.put).not.toHaveBeenCalled();
});

test("failed range is never checkpointed and later ranges are not processed", async () => {
  const input = makeInput();
  input.probe.mockResolvedValue([
    { date: "20260911", fingerprint: "a" },
    { date: "20260912", fingerprint: "b" },
    { date: "20260914", fingerprint: "c" },
  ]);
  input.sync.mockRejectedValue(new Error("sync failed"));
  await expect(syncChangedDays(input)).rejects.toThrow("sync failed");
  expect(input.store.put).not.toHaveBeenCalled();
  expect(input.sync).toHaveBeenCalledOnce();
});

test("adjacent changed and deleted days share one CLI with per-day checkpoints", async () => {
  const input = makeInput();
  input.probe.mockResolvedValue([
    { date: "20260911", fingerprint: "a" },
    { date: "20260912", fingerprint: "empty" },
  ]);
  await syncChangedDays(input);
  expect(input.sync.mock.calls).toStrictEqual([["20260911", "20260912"]]);
  expect(input.store.put.mock.calls).toStrictEqual([
    ["preview-source-v1:20260911", { fingerprint: "a", syncedAt: 3600000 }],
    ["preview-source-v1:20260912", { fingerprint: "empty", syncedAt: 3600000 }],
  ]);
});

test("unchanged middle date splits ranges instead of being resynced", async () => {
  const input = makeInput();
  input.probe.mockResolvedValue([
    { date: "20260911", fingerprint: "a" },
    { date: "20260912", fingerprint: "original" },
    { date: "20260913", fingerprint: "c" },
  ]);
  await syncChangedDays(input);
  expect(input.sync.mock.calls).toStrictEqual([
    ["20260911", "20260911"],
    ["20260913", "20260913"],
  ]);
  expect(input.store.put.mock.calls).toStrictEqual([
    ["preview-source-v1:20260911", { fingerprint: "a", syncedAt: 3600000 }],
    ["preview-source-v1:20260913", { fingerprint: "c", syncedAt: 3600000 }],
  ]);
});

test("missing calendar dates are not silently added to a range", async () => {
  const input = makeInput();
  input.probe.mockResolvedValue([
    { date: "20260911", fingerprint: "a" },
    { date: "20260913", fingerprint: "b" },
    { date: "20260914", fingerprint: "c" },
  ]);
  await syncChangedDays(input);
  expect(input.sync.mock.calls).toStrictEqual([
    ["20260911", "20260911"],
    ["20260913", "20260914"],
  ]);
});

test.each([
  ["20260930", "20261001"],
  ["20261231", "20270101"],
  ["20280228", "20280229"],
  ["20280229", "20280301"],
])("batches consecutive calendar dates %s and %s", async (from, to) => {
  const input = makeInput();
  input.probe.mockResolvedValue([
    { date: from, fingerprint: "a" },
    { date: to, fingerprint: "b" },
  ]);
  await syncChangedDays({ ...input, dateFrom: from, dateTo: to });
  expect(input.sync).toHaveBeenCalledExactlyOnceWith(from, to);
  expect(input.store.put).toHaveBeenCalledTimes(2);
});

test("does not bridge a missing leap day", async () => {
  const input = makeInput();
  input.probe.mockResolvedValue([
    { date: "20280228", fingerprint: "a" },
    { date: "20280301", fingerprint: "b" },
  ]);
  await syncChangedDays(input);
  expect(input.sync.mock.calls).toStrictEqual([
    ["20280228", "20280228"],
    ["20280301", "20280301"],
  ]);
});

test("checkpoints and subsequent ranges wait for the active CLI", async () => {
  const input = makeInput();
  const active = Promise.withResolvers<void>();
  const started = Promise.withResolvers<void>();
  input.probe.mockResolvedValue([
    { date: "20260911", fingerprint: "a" },
    { date: "20260912", fingerprint: "b" },
    { date: "20260914", fingerprint: "c" },
  ]);
  input.sync.mockImplementationOnce(async () => {
    started.resolve();
    await active.promise;
  });
  const completion = syncChangedDays(input);
  await started.promise;
  expect(input.sync).toHaveBeenCalledExactlyOnceWith("20260911", "20260912");
  expect(input.store.put).not.toHaveBeenCalled();
  active.resolve();
  await completion;
  expect(input.sync.mock.calls).toStrictEqual([
    ["20260911", "20260912"],
    ["20260914", "20260914"],
  ]);
  expect(input.store.put).toHaveBeenCalledTimes(3);
});

test("a three-day reconciliation runs one CLI rather than three", async () => {
  const input = makeInput();
  input.probe.mockResolvedValue([
    { date: "20260911", fingerprint: "original" },
    { date: "20260912", fingerprint: "original" },
    { date: "20260913", fingerprint: "original" },
  ]);
  input.store.get.mockResolvedValue({ fingerprint: "original", syncedAt: 0 });
  await syncChangedDays(input);
  expect(input.sync).toHaveBeenCalledExactlyOnceWith("20260911", "20260913");
  expect(input.store.put).toHaveBeenCalledTimes(3);
});
