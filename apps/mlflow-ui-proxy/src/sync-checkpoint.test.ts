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

test("failed sync is never checkpointed and later days are not processed", async () => {
  const input = makeInput();
  input.probe.mockResolvedValue([
    { date: "20260911", fingerprint: "a" },
    { date: "20260912", fingerprint: "b" },
  ]);
  input.sync.mockRejectedValue(new Error("sync failed"));
  await expect(syncChangedDays(input)).rejects.toThrow("sync failed");
  expect(input.store.put).not.toHaveBeenCalled();
  expect(input.sync).toHaveBeenCalledOnce();
});

test("changed and deleted-day fingerprints are processed serially", async () => {
  const input = makeInput();
  input.probe.mockResolvedValue([
    { date: "20260911", fingerprint: "a" },
    { date: "20260912", fingerprint: "empty" },
  ]);
  await syncChangedDays(input);
  expect(input.sync.mock.calls).toStrictEqual([
    ["20260911", "20260911"],
    ["20260912", "20260912"],
  ]);
});
