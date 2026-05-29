// Run with: bun run --filter sync-realtime-data-features test
import { expect, it, vi } from "vitest";

import { acquireEnqueueLock, isEnqueueLocked } from "./enqueue-lock-kv";
import type { Env } from "../types";

interface MockKv {
  get: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
}

const buildEnv = (ttl?: string): { env: Env; kv: MockKv } => {
  const kv: MockKv = { get: vi.fn(), put: vi.fn() };
  return {
    env: {
      FEATURES_KV: kv as unknown as KVNamespace,
      FEATURES_ENQUEUE_LOCK_TTL_SECONDS: ttl,
    } as unknown as Env,
    kv,
  };
};

it("returns true when KV reports an existing lock", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce("1");
  await expect(isEnqueueLocked(env, "r", "build-race-features")).resolves.toBe(true);
});

it("returns false when KV reports null", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(null);
  await expect(isEnqueueLocked(env, "r", "predict-running-style")).resolves.toBe(false);
});

it("writes lock with default TTL when env var unset", async () => {
  const { env, kv } = buildEnv();
  await acquireEnqueueLock(env, "race-1", "build-race-features");
  expect(kv.put).toHaveBeenCalledWith("features:enqueue-lock:build-race-features:race-1", "1", {
    expirationTtl: 60,
  });
});

it("writes lock with TTL from env when valid", async () => {
  const { env, kv } = buildEnv("30");
  await acquireEnqueueLock(env, "race-1", "predict-running-style");
  expect(kv.put).toHaveBeenCalledWith("features:enqueue-lock:predict-running-style:race-1", "1", {
    expirationTtl: 30,
  });
});

it("falls back to default TTL when env value is non-numeric", async () => {
  const { env, kv } = buildEnv("nope");
  await acquireEnqueueLock(env, "r", "build-race-features");
  expect(kv.put).toHaveBeenCalledWith("features:enqueue-lock:build-race-features:r", "1", {
    expirationTtl: 60,
  });
});

it("falls back to default TTL when env value is zero", async () => {
  const { env, kv } = buildEnv("0");
  await acquireEnqueueLock(env, "r", "build-race-features");
  expect(kv.put).toHaveBeenCalledWith("features:enqueue-lock:build-race-features:r", "1", {
    expirationTtl: 60,
  });
});
