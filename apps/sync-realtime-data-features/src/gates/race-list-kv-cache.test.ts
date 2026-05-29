// Run with: bun run --filter sync-realtime-data-features test
import { expect, it, vi } from "vitest";

import { getRaceListFromKv, invalidateRaceListInKv, putRaceListToKv } from "./race-list-kv-cache";
import type { Env } from "../types";

interface MockKv {
  get: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
  delete: ReturnType<typeof vi.fn>;
}

const buildEnv = (ttl?: string): { env: Env; kv: MockKv } => {
  const kv: MockKv = { get: vi.fn(), put: vi.fn(), delete: vi.fn() };
  return {
    env: {
      FEATURES_KV: kv as unknown as KVNamespace,
      FEATURES_RACE_LIST_KV_TTL_SECONDS: ttl,
    } as unknown as Env,
    kv,
  };
};

it("returns parsed list when KV has entry", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(
    JSON.stringify([{ raceKey: "nar:x", source: "nar", raceStartAtJst: "t", lastBuiltAt: null }]),
  );
  const list = await getRaceListFromKv(env, "nar", "20260529");
  expect(list).toStrictEqual([
    { raceKey: "nar:x", source: "nar", raceStartAtJst: "t", lastBuiltAt: null },
  ]);
});

it("returns null when KV miss", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(null);
  await expect(getRaceListFromKv(env, "jra", "20260529")).resolves.toBeNull();
});

it("writes list with default TTL when env var unset", async () => {
  const { env, kv } = buildEnv();
  await putRaceListToKv(env, "nar", "20260529", []);
  expect(kv.put).toHaveBeenCalledWith("features:race-list:v1:nar:20260529", "[]", {
    expirationTtl: 21_600,
  });
});

it("writes list with TTL from env when valid", async () => {
  const { env, kv } = buildEnv("600");
  await putRaceListToKv(env, "nar", "20260529", []);
  expect(kv.put).toHaveBeenCalledWith("features:race-list:v1:nar:20260529", "[]", {
    expirationTtl: 600,
  });
});

it("falls back to default TTL when env value is non-numeric", async () => {
  const { env, kv } = buildEnv("not-a-number");
  await putRaceListToKv(env, "jra", "20260529", []);
  expect(kv.put).toHaveBeenCalledWith("features:race-list:v1:jra:20260529", "[]", {
    expirationTtl: 21_600,
  });
});

it("falls back to default TTL when env value is zero", async () => {
  const { env, kv } = buildEnv("0");
  await putRaceListToKv(env, "jra", "20260529", []);
  expect(kv.put).toHaveBeenCalledWith("features:race-list:v1:jra:20260529", "[]", {
    expirationTtl: 21_600,
  });
});

it("deletes race list key", async () => {
  const { env, kv } = buildEnv();
  await invalidateRaceListInKv(env, "nar", "20260529");
  expect(kv.delete).toHaveBeenCalledWith("features:race-list:v1:nar:20260529");
});
