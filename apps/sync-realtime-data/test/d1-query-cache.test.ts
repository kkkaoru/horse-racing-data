// Run with bun test apps/sync-realtime-data/test/d1-query-cache.test.ts
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { putD1QueryCache, type PutD1QueryCacheOptions } from "../src/d1-query-cache";

type KvForTest = NonNullable<PutD1QueryCacheOptions["kv"]>;

interface FakeCache {
  put: ReturnType<typeof vi.fn>;
}

interface FakeKv {
  put: ReturnType<typeof vi.fn>;
}

interface MutableCachesHost {
  caches?: { default: FakeCache } | undefined;
}

const HOST = globalThis as unknown as MutableCachesHost;
const ORIGINAL_CACHES = HOST.caches;

const setupFakeCache = (): FakeCache => {
  const cache: FakeCache = { put: vi.fn(async () => undefined) };
  HOST.caches = { default: cache };
  return cache;
};

beforeEach(() => {
  vi.restoreAllMocks();
});

afterEach(() => {
  HOST.caches = ORIGINAL_CACHES;
});

test("putD1QueryCache writes to URL cache and KV when both are provided", async () => {
  const cache = setupFakeCache();
  const kv: FakeKv = { put: vi.fn(async () => undefined) };

  await putD1QueryCache(
    "running-style-race",
    ["getRaceRunningStylesFromD1", "jra:20260524:04:01"],
    [{ predictedLabel: "nige" }],
    {
      kv: kv as unknown as KvForTest,
      raceDay: { kaisaiNen: "2026", kaisaiTsukihi: "0524" },
    },
  );

  expect(cache.put).toHaveBeenCalledOnce();
  expect(kv.put).toHaveBeenCalledOnce();
  const [kvKey, kvBody, kvOptions] = kv.put.mock.calls[0]!;
  expect(typeof kvKey).toBe("string");
  expect(kvBody).toBe(JSON.stringify([{ predictedLabel: "nige" }]));
  expect((kvOptions as { expirationTtl: number }).expirationTtl).toBeGreaterThan(0);
});

test("putD1QueryCache writes only to URL cache when KV is absent", async () => {
  const cache = setupFakeCache();

  await putD1QueryCache("realtime-short", ["sampleKey"], { hello: "world" });

  expect(cache.put).toHaveBeenCalledOnce();
});

test("putD1QueryCache writes only to KV when URL cache is unavailable", async () => {
  HOST.caches = undefined;
  const kv: FakeKv = { put: vi.fn(async () => undefined) };

  await putD1QueryCache("realtime-short", ["sampleKey"], { hello: "world" }, {
    kv: kv as unknown as KvForTest,
  });

  expect(kv.put).toHaveBeenCalledOnce();
});

test("putD1QueryCache skips writes when TTL is zero", async () => {
  const cache = setupFakeCache();
  const kv: FakeKv = { put: vi.fn(async () => undefined) };

  await putD1QueryCache(
    "running-style-race",
    ["getRaceRunningStylesFromD1", "jra:19990101:04:01"],
    [],
    {
      kv: kv as unknown as KvForTest,
      raceDay: { kaisaiNen: "1999", kaisaiTsukihi: "0101" },
    },
  );

  expect(cache.put).not.toHaveBeenCalled();
  expect(kv.put).not.toHaveBeenCalled();
});
