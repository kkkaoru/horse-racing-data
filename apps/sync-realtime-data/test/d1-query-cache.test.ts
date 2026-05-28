// Run with bun test apps/sync-realtime-data/test/d1-query-cache.test.ts
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import {
  buildD1QueryCacheKey,
  putD1QueryCache,
  resolveD1QueryCacheTtlSeconds,
  withD1QueryCache,
  type PutD1QueryCacheOptions,
} from "../src/d1-query-cache";

type KvForTest = NonNullable<PutD1QueryCacheOptions["kv"]>;

interface FakeCache {
  delete: ReturnType<typeof vi.fn>;
  match: ReturnType<typeof vi.fn>;
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

const setupFakeCache = (matchResult?: Response): FakeCache => {
  const cache: FakeCache = {
    delete: vi.fn(async () => true),
    match: vi.fn(async () => matchResult),
    put: vi.fn(async () => undefined),
  };
  HOST.caches = { default: cache };
  return cache;
};

const FUTURE_RACE_DAY = { kaisaiNen: "2099", kaisaiTsukihi: "1231" };

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
    ["getRaceRunningStylesFromD1", "jra:20990101:04:01"],
    [{ predictedLabel: "nige" }],
    {
      kv: kv as unknown as KvForTest,
      raceDay: FUTURE_RACE_DAY,
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

  await putD1QueryCache(
    "realtime-short",
    ["sampleKey"],
    { hello: "world" },
    {
      kv: kv as unknown as KvForTest,
    },
  );

  expect(kv.put).toHaveBeenCalledOnce();
});

test("putD1QueryCache skips writes when TTL is zero (race day in the past)", async () => {
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

test("putD1QueryCache defers writes to executionContext.waitUntil when ctx is provided", async () => {
  setupFakeCache();
  const waitUntil = vi.fn();
  const ctx = { waitUntil, passThroughOnException: vi.fn() } as unknown as ExecutionContext;

  await putD1QueryCache("realtime-short", ["sampleKey"], { hello: "world" }, { ctx });

  expect(waitUntil).toHaveBeenCalledOnce();
});

test("buildD1QueryCacheKey is deterministic for identical inputs", () => {
  const left = buildD1QueryCacheKey("running-style-race", ["key", 1, "abc"]);
  const right = buildD1QueryCacheKey("running-style-race", ["key", 1, "abc"]);
  expect(left).toBe(right);
  expect(left).toMatch(/^[0-9a-f]{8}$/);
});

test("buildD1QueryCacheKey differs when keyParts change", () => {
  const left = buildD1QueryCacheKey("running-style-race", ["key", 1]);
  const right = buildD1QueryCacheKey("running-style-race", ["key", 2]);
  expect(left === right).toBe(false);
});

test("resolveD1QueryCacheTtlSeconds uses default profile TTL when raceDay is absent", () => {
  const ttl = resolveD1QueryCacheTtlSeconds("realtime-short");
  expect(ttl).toBe(60);
});

test("resolveD1QueryCacheTtlSeconds returns 0 when race day already passed", () => {
  const ttl = resolveD1QueryCacheTtlSeconds("running-style-race", {
    kaisaiNen: "1999",
    kaisaiTsukihi: "0101",
  });
  expect(ttl).toBe(0);
});

test("resolveD1QueryCacheTtlSeconds returns positive seconds for a future race day", () => {
  const ttl = resolveD1QueryCacheTtlSeconds("running-style-race", FUTURE_RACE_DAY);
  expect(ttl > 0).toBe(true);
});

test("withD1QueryCache returns cached value without calling load when URL cache hits", async () => {
  const cachedBody = JSON.stringify({ cached: true });
  const matchResult = new Response(cachedBody);
  setupFakeCache(matchResult);
  const load = vi.fn();

  const result = await withD1QueryCache<{ cached: boolean }>("realtime-short", ["sampleKey"], load);

  expect(load).not.toHaveBeenCalled();
  expect(result).toStrictEqual({ cached: true });
});

test("withD1QueryCache calls load and caches the result on miss", async () => {
  const cache = setupFakeCache(undefined);
  const load = vi.fn(async () => ({ fresh: 1 }));

  const result = await withD1QueryCache("realtime-short", ["sampleKey"], load);

  expect(load).toHaveBeenCalledOnce();
  expect(result).toStrictEqual({ fresh: 1 });
  expect(cache.put).toHaveBeenCalledOnce();
});
