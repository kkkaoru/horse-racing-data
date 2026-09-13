// Run with bun.
import { afterEach, expect, test, vi } from "vitest";
import { fetchWeightForRace } from "./rescore-realtime";
import { verifyRescoreWeightGeneration } from "./rescore-preflight";

vi.mock("./rescore-realtime", () => ({ fetchWeightForRace: vi.fn() }));
const input = {
  source: "jra",
  runYmd: "20260913",
  keibajoCode: "05",
  raceBango: "01",
  fetchImpl: fetch,
  weightGeneration: {
    weightSnapshotCount: 1,
    weightSnapshotFetchedAt: "2026-09-13T00:00:00Z",
    weightSnapshotHash: "hash",
  },
};
afterEach(() => vi.resetAllMocks());

test("validates the complete expected generation", async () => {
  vi.mocked(fetchWeightForRace).mockResolvedValue(new Map([[1, 480]]));
  await verifyRescoreWeightGeneration(input);
  expect(fetchWeightForRace).toHaveBeenCalledOnce();
  expect(fetchWeightForRace).toHaveBeenCalledWith(
    expect.objectContaining({
      weightGeneration: {
        weightSnapshotCount: 1,
        weightSnapshotFetchedAt: "2026-09-13T00:00:00Z",
        weightSnapshotHash: "hash",
      },
    }),
  );
});

test("maps obsolete generations to existing superseded handling", async () => {
  vi.mocked(fetchWeightForRace).mockRejectedValue(
    new Error("horse weight snapshot generation mismatch: race"),
  );
  await expect(verifyRescoreWeightGeneration(input)).rejects.toThrow(
    "post-weight snapshot generation mismatch:",
  );
});

test("transient upstream failure remains retryable rather than obsolete", async () => {
  vi.mocked(fetchWeightForRace).mockRejectedValue(new Error("HTTP 502"));
  await expect(verifyRescoreWeightGeneration(input)).rejects.toThrow("HTTP 502");
});

test("unknown thrown values are preserved", async () => {
  vi.mocked(fetchWeightForRace).mockRejectedValue("unavailable");
  await expect(verifyRescoreWeightGeneration(input)).rejects.toBe("unavailable");
});
