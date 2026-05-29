// Run with: bun run --filter sync-realtime-data-features test
import { expect, it, vi } from "vitest";

import { readR2ListFromKv, writeR2ListToKv } from "./r2-list-cache";
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
      FEATURES_R2_LIST_CACHE_TTL_SECONDS: ttl,
    } as unknown as Env,
    kv,
  };
};

it("returns parsed keys when KV hit", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce('["a","b"]');
  await expect(readR2ListFromKv(env, "p")).resolves.toStrictEqual(["a", "b"]);
});

it("returns null when KV miss", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(null);
  await expect(readR2ListFromKv(env, "p")).resolves.toBeNull();
});

it("writes list with default TTL when env unset", async () => {
  const { env, kv } = buildEnv();
  await writeR2ListToKv(env, "p", ["a"]);
  expect(kv.put).toHaveBeenCalledWith("features:r2-list:v1:p", '["a"]', {
    expirationTtl: 600,
  });
});

it("writes list with TTL from env when valid", async () => {
  const { env, kv } = buildEnv("300");
  await writeR2ListToKv(env, "p", []);
  expect(kv.put).toHaveBeenCalledWith("features:r2-list:v1:p", "[]", {
    expirationTtl: 300,
  });
});

it("falls back to default TTL when env value is non-numeric", async () => {
  const { env, kv } = buildEnv("x");
  await writeR2ListToKv(env, "p", []);
  expect(kv.put).toHaveBeenCalledWith("features:r2-list:v1:p", "[]", {
    expirationTtl: 600,
  });
});

it("falls back to default TTL when env value is zero", async () => {
  const { env, kv } = buildEnv("0");
  await writeR2ListToKv(env, "p", []);
  expect(kv.put).toHaveBeenCalledWith("features:r2-list:v1:p", "[]", {
    expirationTtl: 600,
  });
});
