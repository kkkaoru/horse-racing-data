// Run with: bun run --filter sync-realtime-data-hot test
import { afterEach, beforeEach, expect, it, vi } from "vitest";

import {
  getCachedOddsFetchStateCount,
  invalidateOddsFetchStateCount,
} from "./odds-fetch-state-count-cache";
import type { Env } from "./types";

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

interface BuildEnvOptions {
  kvGet?: () => Promise<string | null>;
  kvPut?: () => Promise<void>;
  kvDelete?: () => Promise<void>;
  d1Value?: number;
}

const buildEnv = (options: BuildEnvOptions = {}): Env => {
  const kvGet = options.kvGet ?? vi.fn(async () => null);
  const kvPut = options.kvPut ?? vi.fn(async () => undefined);
  const kvDelete = options.kvDelete ?? vi.fn(async () => undefined);
  const kv = {
    delete: kvDelete,
    get: kvGet,
    put: kvPut,
  } as unknown as KVNamespace;
  const prepareMock = vi.fn(() => ({
    bind: vi.fn(() => ({
      first: vi.fn(async () => ({ count: options.d1Value ?? 0 })),
    })),
  }));
  const db = { prepare: prepareMock } as unknown as D1Database;
  return {
    ODDS_HOT_KV: kv,
    REALTIME_HOT_DB: db,
  } as unknown as Env;
};

it("returns the cached number when KV hits", async () => {
  const kvGet = vi.fn(async () => "42");
  const env = buildEnv({ kvGet });
  const result = await getCachedOddsFetchStateCount(env, "20260528");
  expect(result).toBe(42);
  expect(env.REALTIME_HOT_DB.prepare).not.toHaveBeenCalled();
});

it("falls through to D1 on cache miss and writes the fresh value to KV", async () => {
  const kvPut = vi.fn(async () => undefined);
  const env = buildEnv({ d1Value: 100, kvPut });
  const result = await getCachedOddsFetchStateCount(env, "20260528");
  expect(result).toBe(100);
  expect(env.ODDS_HOT_KV.put).toHaveBeenCalledWith("odds-fetch-state-count:20260528", "100", {
    expirationTtl: 600,
  });
});

it("treats an unparseable cached value as a miss", async () => {
  const kvGet = vi.fn(async () => "not-a-number");
  const env = buildEnv({ d1Value: 7, kvGet });
  const result = await getCachedOddsFetchStateCount(env, "20260528");
  expect(result).toBe(7);
});

it("falls through to D1 when KV read throws", async () => {
  const kvGet = vi.fn(async () => {
    throw new Error("kv down");
  });
  const env = buildEnv({ d1Value: 3, kvGet });
  const result = await getCachedOddsFetchStateCount(env, "20260528");
  expect(result).toBe(3);
});

it("returns the D1 value even when KV write throws", async () => {
  const kvPut = vi.fn(async () => {
    throw new Error("kv put down");
  });
  const env = buildEnv({ d1Value: 12, kvPut });
  const result = await getCachedOddsFetchStateCount(env, "20260528");
  expect(result).toBe(12);
});

it("invalidateOddsFetchStateCount deletes the KV entry", async () => {
  const kvDelete = vi.fn(async () => undefined);
  const env = buildEnv({ kvDelete });
  await invalidateOddsFetchStateCount(env, "20260528");
  expect(env.ODDS_HOT_KV.delete).toHaveBeenCalledWith("odds-fetch-state-count:20260528");
});

it("invalidateOddsFetchStateCount swallows KV delete errors", async () => {
  const kvDelete = vi.fn(async () => {
    throw new Error("kv delete down");
  });
  const env = buildEnv({ kvDelete });
  await invalidateOddsFetchStateCount(env, "20260528");
  expect(env.ODDS_HOT_KV.delete).toHaveBeenCalledWith("odds-fetch-state-count:20260528");
});
