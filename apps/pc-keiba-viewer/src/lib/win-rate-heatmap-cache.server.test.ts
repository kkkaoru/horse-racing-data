// bun で実行する (bunx vitest)
import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const { getCloudflareContextMock } = vi.hoisted(() => ({
  getCloudflareContextMock: vi.fn<() => Promise<unknown>>(),
}));
vi.mock("@opennextjs/cloudflare", () => ({
  getCloudflareContext: getCloudflareContextMock,
}));

import {
  WIN_RATE_HEATMAP_CACHE_TTL_SECONDS,
  type WinRateHeatmapSectionPayload,
} from "./win-rate-heatmap-cache";
import {
  getCachedWinRateHeatmapPayload,
  putWinRateHeatmapCache,
} from "./win-rate-heatmap-cache.server";

type CacheMatchFn = (request: Request) => Promise<Response | undefined>;
type CachePutFn = (request: Request, response: Response) => Promise<void>;
type CacheDeleteFn = (request: Request) => Promise<boolean>;
type KvGetFn = (key: string) => Promise<string | null>;
type KvPutFn = (key: string, value: string, options?: { expirationTtl: number }) => Promise<void>;

interface CacheStub {
  delete: ReturnType<typeof vi.fn<CacheDeleteFn>>;
  match: ReturnType<typeof vi.fn<CacheMatchFn>>;
  put: ReturnType<typeof vi.fn<CachePutFn>>;
}

interface KvStub {
  get: ReturnType<typeof vi.fn<KvGetFn>>;
  put: ReturnType<typeof vi.fn<KvPutFn>>;
}

const HEATMAP_PAYLOAD: WinRateHeatmapSectionPayload = {
  bloodlineRows: [],
  carriedWeightClassStats: [],
  frameStats: [],
  horseResults: [],
  runners: [],
  similarRows: [],
  type: "win-rate-heatmap",
  weightClassStats: [],
};

const buildCacheStub = (): CacheStub => ({
  delete: vi.fn<CacheDeleteFn>().mockResolvedValue(true),
  match: vi.fn<CacheMatchFn>().mockResolvedValue(undefined),
  put: vi.fn<CachePutFn>().mockResolvedValue(undefined),
});

const buildKvStub = (): KvStub => ({
  get: vi.fn<KvGetFn>().mockResolvedValue(null),
  put: vi.fn<KvPutFn>().mockResolvedValue(undefined),
});

const setDefaultCache = (cache: CacheStub): void => {
  Object.defineProperty(globalThis, "caches", {
    configurable: true,
    value: { default: cache },
  });
};

beforeEach(() => {
  getCloudflareContextMock.mockReset();
});

afterEach(() => {
  Reflect.deleteProperty(globalThis, "caches");
});

it("returns a Cache API heatmap payload without reading KV", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  cache.match.mockResolvedValue(new Response(JSON.stringify(HEATMAP_PAYLOAD), { status: 200 }));
  setDefaultCache(cache);
  getCloudflareContextMock.mockResolvedValue({ ctx: null, env: { DETAIL_SECTION_CACHE_KV: kv } });

  await expect(getCachedWinRateHeatmapPayload("heatmap-key")).resolves.toStrictEqual(
    HEATMAP_PAYLOAD,
  );
  expect(kv.get).not.toHaveBeenCalled();
});

it("deletes a corrupt Cache API heatmap entry and falls through to KV", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  cache.match.mockResolvedValue(new Response("not-json", { status: 200 }));
  kv.get.mockResolvedValue(JSON.stringify(HEATMAP_PAYLOAD));
  setDefaultCache(cache);
  getCloudflareContextMock.mockResolvedValue({ ctx: null, env: { DETAIL_SECTION_CACHE_KV: kv } });

  await expect(getCachedWinRateHeatmapPayload("heatmap-key")).resolves.toStrictEqual(
    HEATMAP_PAYLOAD,
  );
  expect(cache.delete).toHaveBeenCalledTimes(1);
  expect(cache.put).toHaveBeenCalledTimes(1);
  const stored = cache.put.mock.calls[0]?.[1];
  expect(stored?.headers.get("Cache-Control")).toBe("public, max-age=129600");
});

it("rejects a Cache API heatmap body that is not the section payload", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  cache.match.mockResolvedValue(
    new Response(JSON.stringify({ type: "condition" }), { status: 200 }),
  );
  kv.get.mockResolvedValue(null);
  setDefaultCache(cache);
  getCloudflareContextMock.mockResolvedValue({ ctx: null, env: { DETAIL_SECTION_CACHE_KV: kv } });

  await expect(getCachedWinRateHeatmapPayload("heatmap-key")).resolves.toBe(null);
  expect(cache.delete).toHaveBeenCalledTimes(1);
});

it("populates Cache API from KV on a heatmap cache miss", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  kv.get.mockResolvedValue(JSON.stringify(HEATMAP_PAYLOAD));
  setDefaultCache(cache);
  getCloudflareContextMock.mockResolvedValue({ ctx: null, env: { DETAIL_SECTION_CACHE_KV: kv } });

  await expect(getCachedWinRateHeatmapPayload("heatmap-key")).resolves.toStrictEqual(
    HEATMAP_PAYLOAD,
  );
  expect(cache.put).toHaveBeenCalledTimes(1);
});

it("uses waitUntil when populating Cache API from KV", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  const waitUntil = vi.fn<(promise: Promise<unknown>) => void>();
  kv.get.mockResolvedValue(JSON.stringify(HEATMAP_PAYLOAD));
  setDefaultCache(cache);
  getCloudflareContextMock.mockResolvedValue({
    ctx: { waitUntil },
    env: { DETAIL_SECTION_CACHE_KV: kv },
  });

  await expect(getCachedWinRateHeatmapPayload("heatmap-key")).resolves.toStrictEqual(
    HEATMAP_PAYLOAD,
  );
  expect(waitUntil).toHaveBeenCalledTimes(1);
});

it("returns null when KV heatmap JSON is invalid", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  kv.get.mockResolvedValue("{");
  setDefaultCache(cache);
  getCloudflareContextMock.mockResolvedValue({ ctx: null, env: { DETAIL_SECTION_CACHE_KV: kv } });

  await expect(getCachedWinRateHeatmapPayload("heatmap-key")).resolves.toBe(null);
  expect(cache.put).not.toHaveBeenCalled();
});

it("serves the previous heatmap namespace without copying it onto the current key", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  kv.get.mockResolvedValueOnce(null).mockResolvedValueOnce(JSON.stringify(HEATMAP_PAYLOAD));
  setDefaultCache(cache);
  getCloudflareContextMock.mockResolvedValue({ ctx: null, env: { DETAIL_SECTION_CACHE_KV: kv } });

  await expect(
    getCachedWinRateHeatmapPayload("pc-keiba-viewer:win-rate-heatmap:v16:2026:08:29:04:08:default"),
  ).resolves.toStrictEqual(HEATMAP_PAYLOAD);
  expect(kv.get.mock.calls[0]?.[0]).toBe(
    "pc-keiba-viewer:win-rate-heatmap:v16:2026:08:29:04:08:default",
  );
  expect(kv.get.mock.calls[1]?.[0]).toBe(
    "pc-keiba-viewer:win-rate-heatmap:v15:2026:08:29:04:08:default",
  );
  expect(cache.put).not.toHaveBeenCalled();
});

it("returns null when both Cache API and KV miss", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  setDefaultCache(cache);
  getCloudflareContextMock.mockResolvedValue({ ctx: null, env: { DETAIL_SECTION_CACHE_KV: kv } });

  await expect(getCachedWinRateHeatmapPayload("heatmap-key")).resolves.toBe(null);
});

it("throws when heatmap KV is unavailable", async () => {
  const cache = buildCacheStub();
  setDefaultCache(cache);
  getCloudflareContextMock.mockResolvedValue({ ctx: null, env: {} });
  await expect(
    putWinRateHeatmapCache({ cacheKey: "heatmap-key", payload: HEATMAP_PAYLOAD }),
  ).rejects.toThrow("DETAIL_SECTION_CACHE_KV is unavailable");
});

it("keeps the KV write when Cache API put fails", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  cache.put.mockRejectedValue(new Error("cache api failed"));
  setDefaultCache(cache);
  getCloudflareContextMock.mockResolvedValue({ ctx: null, env: { DETAIL_SECTION_CACHE_KV: kv } });
  await putWinRateHeatmapCache({ cacheKey: "heatmap-key", payload: HEATMAP_PAYLOAD });
  expect(kv.put).toHaveBeenCalledTimes(1);
});

it("writes heatmap payloads to Cache API and KV with a 36 hour TTL", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  setDefaultCache(cache);
  getCloudflareContextMock.mockResolvedValue({ ctx: null, env: { DETAIL_SECTION_CACHE_KV: kv } });

  await putWinRateHeatmapCache({ cacheKey: "heatmap-key", payload: HEATMAP_PAYLOAD });

  expect(cache.put).toHaveBeenCalledTimes(1);
  expect(kv.put).toHaveBeenCalledTimes(1);
  expect(kv.put.mock.calls[0]?.[2]).toStrictEqual({
    expirationTtl: WIN_RATE_HEATMAP_CACHE_TTL_SECONDS,
  });
  const stored = cache.put.mock.calls[0]?.[1];
  expect(stored?.headers.get("Cache-Control")).toBe("public, max-age=129600");
  expect(await stored?.json()).toStrictEqual(HEATMAP_PAYLOAD);
});
