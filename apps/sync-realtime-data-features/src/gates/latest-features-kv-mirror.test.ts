// Run with: bun run --filter sync-realtime-data-features test
import { expect, it, vi } from "vitest";

import { readLatestFeaturesFromKv, writeLatestFeaturesToKv } from "./latest-features-kv-mirror";
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
      FEATURES_LATEST_KV_TTL_SECONDS: ttl,
    } as unknown as Env,
    kv,
  };
};

it("returns null when KV miss", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(null);
  await expect(readLatestFeaturesFromKv(env, "r")).resolves.toBeNull();
});

it("returns parsed rows when KV hit", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce("[]");
  await expect(readLatestFeaturesFromKv(env, "r")).resolves.toStrictEqual([]);
});

it("writes rows with default TTL when env unset", async () => {
  const { env, kv } = buildEnv();
  await writeLatestFeaturesToKv(env, "r", []);
  expect(kv.put).toHaveBeenCalledWith("features:latest:r", "[]", {
    expirationTtl: 600,
  });
});

it("writes rows with TTL from env when valid", async () => {
  const { env, kv } = buildEnv("120");
  await writeLatestFeaturesToKv(env, "r", []);
  expect(kv.put).toHaveBeenCalledWith("features:latest:r", "[]", {
    expirationTtl: 120,
  });
});

it("falls back to default TTL when env value is non-numeric", async () => {
  const { env, kv } = buildEnv("x");
  await writeLatestFeaturesToKv(env, "r", []);
  expect(kv.put).toHaveBeenCalledWith("features:latest:r", "[]", {
    expirationTtl: 600,
  });
});

it("falls back to default TTL when env value is zero", async () => {
  const { env, kv } = buildEnv("0");
  await writeLatestFeaturesToKv(env, "r", []);
  expect(kv.put).toHaveBeenCalledWith("features:latest:r", "[]", {
    expirationTtl: 600,
  });
});
