// Run with bun (vitest).
import { afterEach, beforeEach, expect, it, vi } from "vitest";

import type { RaceDetail } from "./race-types";

vi.mock("server-only", () => ({}));

const { getCloudflareContextMock } = vi.hoisted(() => ({
  getCloudflareContextMock: vi.fn<() => Promise<unknown>>(),
}));
vi.mock("@opennextjs/cloudflare", () => ({
  getCloudflareContext: getCloudflareContextMock,
}));

import {
  buildFinishPredictionInputsCacheKey,
  buildFinishPredictionInputsCacheKeyFromRaceParts,
  deleteFinishPredictionInputsCache,
  getCachedFinishPredictionInputs,
  putFinishPredictionInputsCache,
} from "./finish-prediction-inputs-cache.server";

type CacheDeleteFn = (request: Request) => Promise<boolean>;
type CacheMatchFn = (request: Request) => Promise<Response | undefined>;
type CachePutFn = (request: Request, response: Response) => Promise<void>;
type KvDeleteFn = (key: string) => Promise<void>;
type KvGetFn = (key: string) => Promise<string | null>;
type KvPutFn = (key: string, value: string, options: { expirationTtl: number }) => Promise<void>;
type WaitUntilFn = (promise: Promise<unknown>) => void;

interface CacheStub {
  delete: ReturnType<typeof vi.fn<CacheDeleteFn>>;
  match: ReturnType<typeof vi.fn<CacheMatchFn>>;
  put: ReturnType<typeof vi.fn<CachePutFn>>;
}

interface KvStub {
  delete: ReturnType<typeof vi.fn<KvDeleteFn>>;
  get: ReturnType<typeof vi.fn<KvGetFn>>;
  put: ReturnType<typeof vi.fn<KvPutFn>>;
}

const buildCacheStub = (): CacheStub => ({
  delete: vi.fn<CacheDeleteFn>().mockResolvedValue(true),
  match: vi.fn<CacheMatchFn>().mockResolvedValue(undefined),
  put: vi.fn<CachePutFn>().mockResolvedValue(undefined),
});

const buildKvStub = (): KvStub => ({
  delete: vi.fn<KvDeleteFn>().mockResolvedValue(undefined),
  get: vi.fn<KvGetFn>().mockResolvedValue(null),
  put: vi.fn<KvPutFn>().mockResolvedValue(undefined),
});

const FUTURE_RACE: RaceDetail = {
  babajotaiCodeDirt: null,
  babajotaiCodeShiba: null,
  gradeCode: null,
  hassoJikoku: "1500",
  jockeyNames: [],
  juryoShubetsuCode: "1",
  kaisaiKai: "1",
  kaisaiNen: "2026",
  kaisaiNichime: "1",
  kaisaiTsukihi: "0824",
  keibajoCode: "35",
  kyori: "1600",
  kyosoJokenCode: "703",
  kyosoJokenMeisho: null,
  kyosoKigoCode: null,
  kyosoShubetsuCode: "11",
  kyosomeiFukudai: null,
  kyosomeiHondai: null,
  kyosomeiKakkonai: null,
  raceBango: "05",
  shussoTosu: "12",
  source: "nar",
  tenkoCode: null,
  torokuTosu: "12",
  trackCode: "10",
};

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
  vi.useRealTimers();
  Reflect.deleteProperty(globalThis, "caches");
});

it("buildFinishPredictionInputsCacheKey zero-pads month day venue and race", () => {
  expect(
    buildFinishPredictionInputsCacheKey({
      day: "9",
      keibajoCode: "5",
      month: "8",
      raceNumber: "1",
      year: "2026",
    }),
  ).toBe("pc-keiba-viewer:finish-prediction-inputs:v4:2026:08:09:05:01:inputs");
});

it("buildFinishPredictionInputsCacheKey isolates overseas history inputs", () => {
  expect(
    buildFinishPredictionInputsCacheKey({
      day: "16",
      keibajoCode: "A8",
      month: "08",
      raceNumber: "04",
      year: "2026",
    }),
  ).toBe("pc-keiba-viewer:finish-prediction-inputs:v5:2026:08:16:A8:04:inputs");
});

it("buildFinishPredictionInputsCacheKeyFromRaceParts matches the section key", () => {
  expect(
    buildFinishPredictionInputsCacheKeyFromRaceParts({
      keibajoCode: "05",
      mmdd: "0809",
      raceBango: "11",
      year: "2026",
    }),
  ).toBe("pc-keiba-viewer:finish-prediction-inputs:v4:2026:08:09:05:11:inputs");
});

it("buildFinishPredictionInputsCacheKeyFromRaceParts pads unpadded producer values", () => {
  expect(
    buildFinishPredictionInputsCacheKeyFromRaceParts({
      keibajoCode: "5",
      mmdd: "809",
      raceBango: "1",
      year: "2026",
    }),
  ).toBe("pc-keiba-viewer:finish-prediction-inputs:v4:2026:08:09:05:01:inputs");
});

it("buildFinishPredictionInputsCacheKeyFromRaceParts keeps Ban-ei venue 83 distinct", () => {
  expect(
    buildFinishPredictionInputsCacheKeyFromRaceParts({
      keibajoCode: "83",
      mmdd: "0809",
      raceBango: "12",
      year: "2026",
    }),
  ).toBe("pc-keiba-viewer:finish-prediction-inputs:v4:2026:08:09:83:12:inputs");
});

it("deletes both the edge cache entry and the kv entry", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  setDefaultCache(cache);
  getCloudflareContextMock.mockResolvedValue({ ctx: null, env: { DETAIL_SECTION_CACHE_KV: kv } });
  await deleteFinishPredictionInputsCache("inputs-key");
  expect(cache.delete).toHaveBeenCalledTimes(1);
  expect(cache.delete.mock.calls[0]?.[0]?.url).toBe(
    "https://pc-keiba-viewer.local/finish-prediction-inputs-cache/inputs-key",
  );
  expect(kv.delete).toHaveBeenCalledWith("inputs-key");
});

it("skips the edge cache delete when the default cache is unavailable", async () => {
  const kv = buildKvStub();
  getCloudflareContextMock.mockResolvedValue({ ctx: null, env: { DETAIL_SECTION_CACHE_KV: kv } });
  await deleteFinishPredictionInputsCache("inputs-key");
  expect(kv.delete).toHaveBeenCalledWith("inputs-key");
});

it("skips the kv delete when the cloudflare env is unavailable", async () => {
  const cache = buildCacheStub();
  setDefaultCache(cache);
  getCloudflareContextMock.mockRejectedValue(new Error("no cloudflare context"));
  await deleteFinishPredictionInputsCache("inputs-key");
  expect(cache.delete).toHaveBeenCalledTimes(1);
});

it("resolves without throwing when both deletes reject", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  cache.delete.mockRejectedValue(new Error("edge boom"));
  kv.delete.mockRejectedValue(new Error("kv boom"));
  setDefaultCache(cache);
  getCloudflareContextMock.mockResolvedValue({ ctx: null, env: { DETAIL_SECTION_CACHE_KV: kv } });
  await expect(deleteFinishPredictionInputsCache("inputs-key")).resolves.toBeUndefined();
});

it("awaits finish prediction input cache writes when awaitWrite is true", async () => {
  vi.useFakeTimers();
  vi.setSystemTime("2026-08-24T12:00:00+09:00");
  const cachePut = Promise.withResolvers<void>();
  const cache = buildCacheStub();
  const kv = buildKvStub();
  const waitUntil = vi.fn<WaitUntilFn>();
  cache.put.mockReturnValue(cachePut.promise);
  setDefaultCache(cache);
  getCloudflareContextMock.mockResolvedValue({
    ctx: { waitUntil },
    env: { DETAIL_SECTION_CACHE_KV: kv },
  });
  const completed = vi.fn<() => void>();
  const write = putFinishPredictionInputsCache({
    awaitWrite: true,
    body: "fresh-inputs",
    cacheKey: "inputs-key",
    race: FUTURE_RACE,
  });
  void write.then(completed);
  await vi.waitFor(() => expect(cache.put).toHaveBeenCalledTimes(1));
  expect(completed).not.toHaveBeenCalled();
  expect(waitUntil).not.toHaveBeenCalled();
  cachePut.resolve();
  await write;
  expect(completed).toHaveBeenCalledTimes(1);
});

it("keeps normal finish prediction input writes deferred through waitUntil", async () => {
  vi.useFakeTimers();
  vi.setSystemTime("2026-08-24T12:00:00+09:00");
  const cachePut = Promise.withResolvers<void>();
  const cache = buildCacheStub();
  const kv = buildKvStub();
  const waitUntil = vi.fn<WaitUntilFn>();
  cache.put.mockReturnValue(cachePut.promise);
  setDefaultCache(cache);
  getCloudflareContextMock.mockResolvedValue({
    ctx: { waitUntil },
    env: { DETAIL_SECTION_CACHE_KV: kv },
  });
  await putFinishPredictionInputsCache({
    body: "fresh-inputs",
    cacheKey: "inputs-key",
    race: FUTURE_RACE,
  });
  expect(waitUntil).toHaveBeenCalledTimes(1);
  cachePut.resolve();
  await waitUntil.mock.calls[0]?.[0];
});

it("reads the global KV input snapshot before a stale colo Cache API copy", async () => {
  vi.useFakeTimers();
  vi.setSystemTime("2026-08-24T12:00:00+09:00");
  const cache = buildCacheStub();
  cache.match.mockResolvedValue(new Response("stale", { status: 200 }));
  const kv = buildKvStub();
  kv.get.mockResolvedValue(
    JSON.stringify({
      evaluation: {},
      inputs: { modelPredictionFeatures: [{ horseNumber: "1" }] },
    }),
  );
  const waitUntil = vi.fn<WaitUntilFn>();
  setDefaultCache(cache);
  getCloudflareContextMock.mockResolvedValue({
    ctx: { waitUntil },
    env: { DETAIL_SECTION_CACHE_KV: kv },
  });

  const cacheKey = "pc-keiba-viewer:finish-prediction-inputs:v4:2026:08:24:83:02:inputs";
  const result = await getCachedFinishPredictionInputs(cacheKey);

  expect(result?.inputs.modelPredictionFeatures).toEqual([{ horseNumber: "1" }]);
  expect(kv.get).toHaveBeenCalledWith(cacheKey);
  expect(cache.match).not.toHaveBeenCalled();
  expect(cache.put).toHaveBeenCalledTimes(1);
  await waitUntil.mock.calls[0]?.[0];
});
