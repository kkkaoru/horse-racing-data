// Run with: bun run --filter sync-realtime-data-features test
import { expect, it, vi } from "vitest";

import { readNextBatchSize, recordRecomputeOutcome } from "./adaptive-batch-kv";
import type { Env } from "../types";

interface MockKv {
  get: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
}

const buildEnv = (): { env: Env; kv: MockKv } => {
  const kv: MockKv = { get: vi.fn(), put: vi.fn() };
  return {
    env: { FEATURES_KV: kv as unknown as KVNamespace } as unknown as Env,
    kv,
  };
};

it("readNextBatchSize returns 5 when KV miss", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(null);
  await expect(readNextBatchSize(env)).resolves.toBe(5);
});

it("readNextBatchSize returns existing batchSize when window has fewer than 10 samples", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(
    JSON.stringify({
      batchSize: 7,
      recent: [true, true, false, true, true],
    }),
  );
  await expect(readNextBatchSize(env)).resolves.toBe(7);
});

it("readNextBatchSize increments to current+1 when success rate >= 80% (>=10 samples)", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(
    JSON.stringify({
      batchSize: 5,
      recent: [true, true, true, true, true, true, true, true, false, false],
    }),
  );
  await expect(readNextBatchSize(env)).resolves.toBe(6);
});

it("readNextBatchSize caps batchSize at 30", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(
    JSON.stringify({
      batchSize: 30,
      recent: [true, true, true, true, true, true, true, true, true, true],
    }),
  );
  await expect(readNextBatchSize(env)).resolves.toBe(30);
});

it("readNextBatchSize decrements to current-1 when success rate < 50% (>=10 samples)", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(
    JSON.stringify({
      batchSize: 8,
      recent: [false, false, false, false, false, true, true, true, false, false],
    }),
  );
  await expect(readNextBatchSize(env)).resolves.toBe(7);
});

it("readNextBatchSize floors batchSize at 3", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(
    JSON.stringify({
      batchSize: 3,
      recent: [false, false, false, false, false, false, false, false, false, false],
    }),
  );
  await expect(readNextBatchSize(env)).resolves.toBe(3);
});

it("readNextBatchSize keeps batchSize when success rate is between 50 and 80", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(
    JSON.stringify({
      batchSize: 12,
      recent: [true, true, true, true, true, true, false, false, false, false],
    }),
  );
  await expect(readNextBatchSize(env)).resolves.toBe(12);
});

it("readNextBatchSize falls back to 5 when JSON is malformed", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce("not-json");
  await expect(readNextBatchSize(env)).resolves.toBe(5);
});

it("readNextBatchSize falls back to 5 when stored shape is invalid", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(JSON.stringify({ recent: ["bad"], batchSize: 5 }));
  await expect(readNextBatchSize(env)).resolves.toBe(5);
});

it("readNextBatchSize falls back to 5 when KV throws", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockRejectedValueOnce(new Error("kv down"));
  await expect(readNextBatchSize(env)).resolves.toBe(5);
});

it("recordRecomputeOutcome seeds window on first success", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(null);
  await recordRecomputeOutcome(env, true);
  expect(kv.put).toHaveBeenCalledWith(
    "features:metrics:recompute:window:v1",
    JSON.stringify({ batchSize: 5, recent: [true] }),
    { expirationTtl: 86_400 },
  );
});

it("recordRecomputeOutcome seeds window on first failure", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(null);
  await recordRecomputeOutcome(env, false);
  expect(kv.put).toHaveBeenCalledWith(
    "features:metrics:recompute:window:v1",
    JSON.stringify({ batchSize: 5, recent: [false] }),
    { expirationTtl: 86_400 },
  );
});

it("recordRecomputeOutcome prepends to recent and keeps batchSize when window <10", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(JSON.stringify({ batchSize: 5, recent: [false, true] }));
  await recordRecomputeOutcome(env, true);
  expect(kv.put).toHaveBeenCalledWith(
    "features:metrics:recompute:window:v1",
    JSON.stringify({ batchSize: 5, recent: [true, false, true] }),
    { expirationTtl: 86_400 },
  );
});

it("recordRecomputeOutcome adjusts batchSize when window reaches 10 samples", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(
    JSON.stringify({
      batchSize: 5,
      recent: [true, true, true, true, true, true, true, true, true],
    }),
  );
  await recordRecomputeOutcome(env, true);
  expect(kv.put).toHaveBeenCalledWith(
    "features:metrics:recompute:window:v1",
    JSON.stringify({
      batchSize: 6,
      recent: [true, true, true, true, true, true, true, true, true, true],
    }),
    { expirationTtl: 86_400 },
  );
});

it("recordRecomputeOutcome trims recent to WINDOW_SIZE", async () => {
  const { env, kv } = buildEnv();
  const recent = Array.from({ length: 50 }, () => true);
  kv.get.mockResolvedValueOnce(JSON.stringify({ batchSize: 10, recent }));
  await recordRecomputeOutcome(env, false);
  expect(kv.put).toHaveBeenCalledTimes(1);
  const call = kv.put.mock.calls[0]!;
  expect(call[0]).toBe("features:metrics:recompute:window:v1");
  const parsed = JSON.parse(String(call[1])) as { recent: boolean[]; batchSize: number };
  expect(parsed.recent.length).toBe(50);
  expect(parsed.recent[0]).toBe(false);
});

it("recordRecomputeOutcome silently swallows KV write errors", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(null);
  kv.put.mockRejectedValueOnce(new Error("kv put failed"));
  await expect(recordRecomputeOutcome(env, true)).resolves.toBeUndefined();
});

it("recordRecomputeOutcome silently swallows KV read errors", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockRejectedValueOnce(new Error("kv get failed"));
  await expect(recordRecomputeOutcome(env, true)).resolves.toBeUndefined();
  expect(kv.put).toHaveBeenCalledTimes(1);
});
