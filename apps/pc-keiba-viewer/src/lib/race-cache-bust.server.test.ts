// Run with bun: `bun run --filter pc-keiba-viewer test`
import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const { getCloudflareContextMock } = vi.hoisted(() => ({
  getCloudflareContextMock: vi.fn<() => Promise<unknown>>(),
}));
vi.mock("@opennextjs/cloudflare", () => ({
  getCloudflareContext: getCloudflareContextMock,
}));

import { bustRaceCachesForRace, resolveRaceCacheKey } from "./race-cache-bust.server";

type KvGetFn = (key: string) => Promise<string | null>;
type KvPutFn = (key: string, value: string, options?: { expirationTtl: number }) => Promise<void>;
type KvDeleteFn = (key: string) => Promise<void>;

interface KvStub {
  delete: ReturnType<typeof vi.fn<KvDeleteFn>>;
  get: ReturnType<typeof vi.fn<KvGetFn>>;
  put: ReturnType<typeof vi.fn<KvPutFn>>;
}

const FIXED_NOW_MS = Date.parse("2026-10-24T12:00:00+09:00");

const buildKvStub = (): KvStub => ({
  delete: vi.fn<KvDeleteFn>().mockResolvedValue(undefined),
  get: vi.fn<KvGetFn>().mockResolvedValue(null),
  put: vi.fn<KvPutFn>().mockResolvedValue(undefined),
});

const mockEnvWithKv = (kv: KvStub): void => {
  getCloudflareContextMock.mockResolvedValue({ ctx: null, env: { DETAIL_SECTION_CACHE_KV: kv } });
};

beforeEach(() => {
  getCloudflareContextMock.mockReset();
  vi.useFakeTimers();
  vi.setSystemTime(FIXED_NOW_MS);
});

afterEach(() => {
  vi.useRealTimers();
});

it("bustRaceCachesForRace is a no-op without the KV binding", async () => {
  getCloudflareContextMock.mockResolvedValue({ ctx: null, env: {} });
  expect(
    await bustRaceCachesForRace({
      keibajoCode: "05",
      mmdd: "1024",
      raceBango: "11",
      source: "jra",
      year: "2026",
    }),
  ).toStrictEqual({ busted: 0, generation: 0 });
});

it("bustRaceCachesForRace rotates the generation with one put and no read or delete", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  expect(
    await bustRaceCachesForRace({
      keibajoCode: "05",
      mmdd: "1024",
      raceBango: "11",
      source: "jra",
      year: "2026",
    }),
  ).toStrictEqual({ busted: 1, generation: 1792810800000 });
  expect(kv.put.mock.calls).toStrictEqual([
    ["race-cache:gen:v2:2026:1024:05:11", "1792810800000", { expirationTtl: 2592000 }],
  ]);
  expect(kv.get).not.toHaveBeenCalled();
  expect(kv.delete).not.toHaveBeenCalled();
});

it("bustRaceCachesForRace reports a failed generation put", async () => {
  const kv = buildKvStub();
  kv.put.mockRejectedValue(new Error("kv put failed"));
  mockEnvWithKv(kv);
  expect(
    await bustRaceCachesForRace({
      keibajoCode: "50",
      mmdd: "1024",
      raceBango: "07",
      source: "nar",
      year: "2026",
    }),
  ).toStrictEqual({ busted: 0, generation: 1792810800000 });
});

it("training bust deletes only the training sections of the current generation", async () => {
  const kv = buildKvStub();
  kv.get.mockResolvedValue("1761270000000");
  mockEnvWithKv(kv);
  expect(
    await bustRaceCachesForRace({
      keibajoCode: "06",
      mmdd: "1024",
      raceBango: "03",
      scope: "training",
      source: "jra",
      year: "2026",
    }),
  ).toStrictEqual({ busted: 2, generation: 1761270000000 });
  expect(kv.delete.mock.calls).toStrictEqual([
    ["race-detail-section:v6:2026:10:24:06:03:training:default:g1761270000000"],
    ["race-detail-section:v2:2026:10:24:06:03:premium-data-top:default:g1761270000000"],
  ]);
  expect(kv.put).not.toHaveBeenCalled();
});

it("training bust of a never-busted race also deletes the legacy keys", async () => {
  const kv = buildKvStub();
  kv.delete.mockResolvedValueOnce(undefined).mockRejectedValueOnce(new Error("kv delete failed"));
  mockEnvWithKv(kv);
  expect(
    await bustRaceCachesForRace({
      keibajoCode: "06",
      mmdd: "1024",
      raceBango: "03",
      scope: "training",
      source: "jra",
      year: "2026",
    }),
  ).toStrictEqual({ busted: 5, generation: 0 });
  expect(kv.delete.mock.calls).toStrictEqual([
    ["race-detail-section:v6:2026:10:24:06:03:training:default:g0"],
    ["race-detail-section:v6:2026:10:24:06:03:training:default"],
    ["stale:race-detail-section:v6:2026:10:24:06:03:training:default"],
    ["race-detail-section:v2:2026:10:24:06:03:premium-data-top:default:g0"],
    ["race-detail-section:v2:2026:10:24:06:03:premium-data-top:default"],
    ["stale:race-detail-section:v2:2026:10:24:06:03:premium-data-top:default"],
  ]);
});

it("training bust rotates the whole race generation when the generation read fails", async () => {
  const kv = buildKvStub();
  kv.get.mockRejectedValue(new Error("kv get failed"));
  mockEnvWithKv(kv);
  expect(
    await bustRaceCachesForRace({
      keibajoCode: "06",
      mmdd: "1024",
      raceBango: "03",
      scope: "training",
      source: "jra",
      year: "2026",
    }),
  ).toStrictEqual({ busted: 1, generation: 1792810800000 });
  expect(kv.put.mock.calls).toStrictEqual([
    ["race-cache:gen:v2:2026:1024:06:03", "1792810800000", { expirationTtl: 2592000 }],
  ]);
  expect(kv.delete).not.toHaveBeenCalled();
});

it("resolveRaceCacheKey appends the stored race generation", async () => {
  const kv = buildKvStub();
  kv.get.mockResolvedValue("1761270000000");
  mockEnvWithKv(kv);
  expect(
    await resolveRaceCacheKey("base-key", {
      keibajoCode: "05",
      mmdd: "1024",
      raceBango: "11",
      year: "2026",
    }),
  ).toBe("base-key:g1761270000000");
  expect(kv.get.mock.calls).toStrictEqual([["race-cache:gen:v2:2026:1024:05:11"]]);
});

it("resolveRaceCacheKey uses generation zero for a never-busted race", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  expect(
    await resolveRaceCacheKey("base-key", {
      keibajoCode: "05",
      mmdd: "1024",
      raceBango: "11",
      year: "2026",
    }),
  ).toBe("base-key:g0");
});

it("resolveRaceCacheKey uses generation zero without the KV binding", async () => {
  getCloudflareContextMock.mockResolvedValue({ ctx: null, env: {} });
  expect(
    await resolveRaceCacheKey("base-key", {
      keibajoCode: "05",
      mmdd: "1024",
      raceBango: "11",
      year: "2026",
    }),
  ).toBe("base-key:g0");
});

it("resolveRaceCacheKey returns null when the generation read fails", async () => {
  const kv = buildKvStub();
  kv.get.mockRejectedValue(new Error("kv get failed"));
  mockEnvWithKv(kv);
  expect(
    await resolveRaceCacheKey("base-key", {
      keibajoCode: "05",
      mmdd: "1024",
      raceBango: "11",
      year: "2026",
    }),
  ).toBeNull();
});
