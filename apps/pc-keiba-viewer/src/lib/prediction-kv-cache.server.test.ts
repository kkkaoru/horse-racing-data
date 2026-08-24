// Run with bun. `bunx vitest run src/lib/prediction-kv-cache.server.test.ts`

import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

type CachePutFn = (request: Request, response: Response) => Promise<void>;
type CacheMatchFn = (request: Request) => Promise<Response | undefined>;
type KvPutFn = (key: string, value: string, options: { expirationTtl: number }) => Promise<void>;
type KvGetFn = (key: string) => Promise<string | null>;
type WaitUntilFn = (promise: Promise<unknown>) => void;

const { safeGetCloudflareRuntimeMock } = vi.hoisted(() => ({
  safeGetCloudflareRuntimeMock: vi.fn<() => Promise<unknown>>(),
}));

vi.mock("./cloudflare-context.server", () => ({
  safeGetCloudflareRuntime: safeGetCloudflareRuntimeMock,
}));

import { readPredictionKvText, writePredictionKvText } from "./prediction-kv-cache.server";

const TODAY_MS = Date.parse("2026-08-24T12:00:00+09:00");

beforeEach(() => {
  safeGetCloudflareRuntimeMock.mockReset();
});

afterEach(() => {
  Reflect.deleteProperty(globalThis, "caches");
});

it("awaits prediction Cache API writes when awaitWrite is true", async () => {
  const cachePut = Promise.withResolvers<void>();
  const cache = { put: vi.fn<CachePutFn>().mockReturnValue(cachePut.promise) };
  const kv = { put: vi.fn<KvPutFn>().mockResolvedValue(undefined) };
  const waitUntil = vi.fn<WaitUntilFn>();
  Object.defineProperty(globalThis, "caches", {
    configurable: true,
    value: { default: cache },
  });
  safeGetCloudflareRuntimeMock.mockResolvedValue({
    ctx: { waitUntil },
    env: { DETAIL_SECTION_CACHE_KV: kv },
  });
  const completed = vi.fn<() => void>();
  const write = writePredictionKvText({
    awaitWrite: true,
    body: "fresh-predictions",
    cacheKey: "pred:fp:v1:20260824:35:05",
    nowMs: TODAY_MS,
    raceYmd: "20260824",
  });
  void write.then(completed);
  await vi.waitFor(() => expect(cache.put).toHaveBeenCalledTimes(1));
  expect(completed).not.toHaveBeenCalled();
  expect(waitUntil).not.toHaveBeenCalled();
  cachePut.resolve();
  await write;
  expect(completed).toHaveBeenCalledTimes(1);
});

it("keeps normal prediction writes deferred through waitUntil", async () => {
  const cachePut = Promise.withResolvers<void>();
  const cache = { put: vi.fn<CachePutFn>().mockReturnValue(cachePut.promise) };
  const kv = { put: vi.fn<KvPutFn>().mockResolvedValue(undefined) };
  const waitUntil = vi.fn<WaitUntilFn>();
  Object.defineProperty(globalThis, "caches", {
    configurable: true,
    value: { default: cache },
  });
  safeGetCloudflareRuntimeMock.mockResolvedValue({
    ctx: { waitUntil },
    env: { DETAIL_SECTION_CACHE_KV: kv },
  });
  await writePredictionKvText({
    body: "fresh-predictions",
    cacheKey: "pred:fp:v1:20260824:35:05",
    nowMs: TODAY_MS,
    raceYmd: "20260824",
  });
  expect(waitUntil).toHaveBeenCalledTimes(1);
  cachePut.resolve();
  await waitUntil.mock.calls[0]?.[0];
});

it("reads today's prediction from KV before a stale colo Cache API entry", async () => {
  const cache = {
    match: vi
      .fn<CacheMatchFn>()
      .mockResolvedValue(new Response("stale-predictions", { status: 200 })),
    put: vi.fn<CachePutFn>().mockResolvedValue(undefined),
  };
  const kv = {
    get: vi.fn<KvGetFn>().mockResolvedValue("fresh-predictions"),
  };
  const waitUntil = vi.fn<WaitUntilFn>();
  Object.defineProperty(globalThis, "caches", {
    configurable: true,
    value: { default: cache },
  });
  safeGetCloudflareRuntimeMock.mockResolvedValue({
    ctx: { waitUntil },
    env: { DETAIL_SECTION_CACHE_KV: kv },
  });

  await expect(
    readPredictionKvText("pred:fp:v1:20260824:35:05", "20260824", TODAY_MS),
  ).resolves.toBe("fresh-predictions");
  expect(kv.get).toHaveBeenCalledWith("pred:fp:v1:20260824:35:05");
  expect(cache.match).not.toHaveBeenCalled();
  expect(cache.put).toHaveBeenCalledTimes(1);
  await waitUntil.mock.calls[0]?.[0];
});
