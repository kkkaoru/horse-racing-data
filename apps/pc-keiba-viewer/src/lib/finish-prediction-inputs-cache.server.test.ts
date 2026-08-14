// Run with bun (vitest).
import { afterEach, beforeEach, expect, it, vi } from "vitest";

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
} from "./finish-prediction-inputs-cache.server";

type CacheDeleteFn = (request: Request) => Promise<boolean>;
type KvDeleteFn = (key: string) => Promise<void>;

interface CacheStub {
  delete: ReturnType<typeof vi.fn<CacheDeleteFn>>;
}

interface KvStub {
  delete: ReturnType<typeof vi.fn<KvDeleteFn>>;
}

const buildCacheStub = (): CacheStub => ({
  delete: vi.fn<CacheDeleteFn>().mockResolvedValue(true),
});

const buildKvStub = (): KvStub => ({
  delete: vi.fn<KvDeleteFn>().mockResolvedValue(undefined),
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
