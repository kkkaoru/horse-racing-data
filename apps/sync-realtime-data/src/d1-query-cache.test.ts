// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import {
  putD1QueryCache,
  readD1QueryCache,
  resolveD1QueryCacheTtlSeconds,
  withD1QueryCache,
} from "./d1-query-cache";

interface FakeCache {
  delete: ReturnType<typeof vi.fn>;
  match: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
}

interface CachesGlobal {
  default?: FakeCache;
}

const originalCaches = (globalThis as { caches?: CachesGlobal }).caches;

beforeEach(() => {
  delete (globalThis as { caches?: CachesGlobal }).caches;
});

afterEach(() => {
  vi.restoreAllMocks();
  if (originalCaches === undefined) {
    delete (globalThis as { caches?: CachesGlobal }).caches;
  } else {
    (globalThis as { caches?: CachesGlobal }).caches = originalCaches;
  }
});

it("readD1QueryCache returns null when ttl is zero (race day passed)", async () => {
  const result = await readD1QueryCache("running-style-race", ["k"], {
    raceDay: { kaisaiNen: "1999", kaisaiTsukihi: "0101" },
  });
  expect(result).toBeNull();
});

it("readD1QueryCache returns null when caches global is unavailable", async () => {
  const result = await readD1QueryCache("realtime-short", ["k"]);
  expect(result).toBeNull();
});

it("readD1QueryCache returns null when cache misses", async () => {
  const cache: FakeCache = {
    delete: vi.fn(async () => true),
    match: vi.fn(async () => undefined),
    put: vi.fn(async () => undefined),
  };
  (globalThis as { caches?: CachesGlobal }).caches = { default: cache };
  const result = await readD1QueryCache("realtime-short", ["k"]);
  expect(result).toBeNull();
});

it("readD1QueryCache returns cached body when match succeeds", async () => {
  const cache: FakeCache = {
    delete: vi.fn(async () => true),
    match: vi.fn(async () => new Response(JSON.stringify({ cached: true }))),
    put: vi.fn(async () => undefined),
  };
  (globalThis as { caches?: CachesGlobal }).caches = { default: cache };
  const result = await readD1QueryCache<{ cached: boolean }>("realtime-short", ["k"]);
  expect(result).toStrictEqual({ cached: true });
});

it("readD1QueryCache deletes the entry and returns null when cached body is unparseable", async () => {
  const deleteSpy = vi.fn(async () => true);
  const cache: FakeCache = {
    delete: deleteSpy,
    match: vi.fn(async () => new Response("not json")),
    put: vi.fn(async () => undefined),
  };
  (globalThis as { caches?: CachesGlobal }).caches = { default: cache };
  const result = await readD1QueryCache("realtime-short", ["k"]);
  expect(result).toBeNull();
  expect(deleteSpy).toHaveBeenCalledTimes(1);
});

it("withD1QueryCache bypasses cache when ttl is zero and calls load", async () => {
  const load = vi.fn(async () => ({ fresh: true }));
  const result = await withD1QueryCache("running-style-race", ["k"], load, {
    raceDay: { kaisaiNen: "1999", kaisaiTsukihi: "0101" },
  });
  expect(result).toStrictEqual({ fresh: true });
  expect(load).toHaveBeenCalledTimes(1);
});

it("withD1QueryCache stable-sorts nested object key parts when building the cache key", async () => {
  const putSpy = vi.fn<(req: Request | URL | string, res: Response) => Promise<undefined>>(
    async () => undefined,
  );
  const cache: FakeCache = {
    delete: vi.fn(async () => true),
    match: vi.fn(async () => null),
    put: putSpy,
  };
  (globalThis as { caches?: CachesGlobal }).caches = { default: cache };
  const load = vi.fn(async () => ({ ok: 1 }));
  const result1 = await withD1QueryCache<{ ok: number }>("realtime-short", [{ b: 2, a: 1 }], load);
  expect(result1.ok).toBe(1);
  const result2 = await withD1QueryCache<{ ok: number }>(
    "realtime-short",
    [{ a: 1, b: 2 }],
    vi.fn(async () => ({ ok: 99 })),
  );
  // Hash should be identical, so the second call also misses the cache the same way
  // (cache mock returns null), but importantly the put url should match.
  expect(result2.ok).toBe(99);
  expect(putSpy).toHaveBeenCalledTimes(2);
  const firstReq = putSpy.mock.calls[0]?.[0];
  const secondReq = putSpy.mock.calls[1]?.[0];
  const url1 = firstReq instanceof Request ? firstReq.url : "";
  const url2 = secondReq instanceof Request ? secondReq.url : "";
  expect(url1).toBe(url2);
});

it("resolveD1QueryCacheTtlSeconds returns 0 when race day cannot be parsed", () => {
  const ttl = resolveD1QueryCacheTtlSeconds("running-style-race", {
    kaisaiNen: "abcd",
    kaisaiTsukihi: "0101",
  });
  expect(ttl).toBe(0);
});

it("putD1QueryCache returns early when no cache and no KV are available", async () => {
  const result = await putD1QueryCache("realtime-short", ["k"], { hello: "world" });
  expect(result).toBeUndefined();
});

it("withD1QueryCache deletes corrupted body, falls through to load, and caches the result", async () => {
  const deleteSpy = vi.fn(async () => true);
  const putSpy = vi.fn(async () => undefined);
  const cache: FakeCache = {
    delete: deleteSpy,
    match: vi.fn(async () => new Response("not json")),
    put: putSpy,
  };
  (globalThis as { caches?: CachesGlobal }).caches = { default: cache };
  const load = vi.fn(async () => ({ fresh: 1 }));
  const result = await withD1QueryCache<{ fresh: number }>("realtime-short", ["k"], load);
  expect(result).toStrictEqual({ fresh: 1 });
  expect(deleteSpy).toHaveBeenCalledTimes(1);
  expect(load).toHaveBeenCalledTimes(1);
  expect(putSpy).toHaveBeenCalledTimes(1);
});
