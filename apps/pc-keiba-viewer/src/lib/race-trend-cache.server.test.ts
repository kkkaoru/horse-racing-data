// Run with bun (vitest).
import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const { getCloudflareContextMock } = vi.hoisted(() => ({
  getCloudflareContextMock: vi.fn<() => Promise<unknown>>(),
}));
vi.mock("@opennextjs/cloudflare", () => ({
  getCloudflareContext: getCloudflareContextMock,
}));

vi.mock("./realtime-trend-day-cache.server", () => ({
  bustRealtimeRowsForDay: vi.fn<() => Promise<undefined>>(async () => undefined),
}));

import { bustRaceTrendCachesForDay } from "./race-trend-cache.server";

type AnyMockFn = (...args: never[]) => unknown;

interface KvStub {
  delete: ReturnType<typeof vi.fn<AnyMockFn>>;
  get: ReturnType<typeof vi.fn<AnyMockFn>>;
  put: ReturnType<typeof vi.fn<AnyMockFn>>;
}

interface CacheStub {
  delete: ReturnType<typeof vi.fn<AnyMockFn>>;
  match: ReturnType<typeof vi.fn<AnyMockFn>>;
  put: ReturnType<typeof vi.fn<AnyMockFn>>;
}

const buildKvStub = (): KvStub => ({
  delete: vi.fn<AnyMockFn>().mockResolvedValue(undefined),
  get: vi.fn<AnyMockFn>().mockResolvedValue(null),
  put: vi.fn<AnyMockFn>().mockResolvedValue(undefined),
});

const buildCacheStub = (): CacheStub => ({
  delete: vi.fn<AnyMockFn>().mockResolvedValue(true),
  match: vi.fn<AnyMockFn>().mockResolvedValue(undefined),
  put: vi.fn<AnyMockFn>().mockResolvedValue(undefined),
});

beforeEach(() => {
  getCloudflareContextMock.mockReset();
  Object.defineProperty(globalThis, "caches", {
    configurable: true,
    value: { default: buildCacheStub() },
  });
});

afterEach(() => {
  Reflect.deleteProperty(globalThis, "caches");
});

it("bustRaceTrendCachesForDay emits the v8 outer race-trend key and v9 per-venue today key for NAR", async () => {
  const kv = buildKvStub();
  getCloudflareContextMock.mockResolvedValue({ env: { DETAIL_SECTION_CACHE_KV: kv }, ctx: null });
  const result = await bustRaceTrendCachesForDay({
    races: [{ keibajoCode: "50", raceBango: "07" }],
    source: "nar",
    targetYmd: "20260529",
  });
  expect(result.keys).toStrictEqual([
    "race-trend:v8:nar:50:07:20260515:20260529:20260515:20260529:1",
    "race-trend:v8:nar:50:07:20260515:20260529:20260515:20260529:0",
    "race-trend-past14:v8:nar:50:07:20260515:20260528",
    "race-trend-today:v9:nar:20260529:50",
  ]);
});

it("bustRaceTrendCachesForDay emits the v8 outer key and v9 per-venue today key for JRA", async () => {
  const kv = buildKvStub();
  getCloudflareContextMock.mockResolvedValue({ env: { DETAIL_SECTION_CACHE_KV: kv }, ctx: null });
  const result = await bustRaceTrendCachesForDay({
    races: [{ keibajoCode: "05", raceBango: "11" }],
    source: "jra",
    targetYmd: "20260520",
  });
  expect(result.keys).toStrictEqual([
    "race-trend:v8:jra:05:11:20260506:20260520:20260506:20260520:1",
    "race-trend:v8:jra:05:11:20260506:20260520:20260506:20260520:0",
    "race-trend-past14:v8:jra:05:11:20260506:20260519",
    "race-trend-today:v9:jra:20260520:05",
  ]);
});

it("bustRaceTrendCachesForDay deletes each key from KV", async () => {
  const kv = buildKvStub();
  getCloudflareContextMock.mockResolvedValue({ env: { DETAIL_SECTION_CACHE_KV: kv }, ctx: null });
  await bustRaceTrendCachesForDay({
    races: [{ keibajoCode: "50", raceBango: "07" }],
    source: "nar",
    targetYmd: "20260529",
  });
  expect(kv.delete).toHaveBeenCalledTimes(4);
  const calledKeys = kv.delete.mock.calls.map((call) => String(call[0])).toSorted();
  expect(calledKeys).toStrictEqual([
    "race-trend-past14:v8:nar:50:07:20260515:20260528",
    "race-trend-today:v9:nar:20260529:50",
    "race-trend:v8:nar:50:07:20260515:20260529:20260515:20260529:0",
    "race-trend:v8:nar:50:07:20260515:20260529:20260515:20260529:1",
  ]);
});

it("bustRaceTrendCachesForDay deduplicates today key across multiple races on the same venue", async () => {
  const kv = buildKvStub();
  getCloudflareContextMock.mockResolvedValue({ env: { DETAIL_SECTION_CACHE_KV: kv }, ctx: null });
  const result = await bustRaceTrendCachesForDay({
    races: [
      { keibajoCode: "50", raceBango: "07" },
      { keibajoCode: "50", raceBango: "08" },
    ],
    source: "nar",
    targetYmd: "20260529",
  });
  // Two races, same venue -> 2 outer trend × 2 includeRealtimeResults
  // variants + 2 past14 + 1 today (deduped per venue) = 7 keys.
  expect(result.keys.length).toBe(7);
});

it("bustRaceTrendCachesForDay emits one today key per distinct venue", async () => {
  const kv = buildKvStub();
  getCloudflareContextMock.mockResolvedValue({ env: { DETAIL_SECTION_CACHE_KV: kv }, ctx: null });
  const result = await bustRaceTrendCachesForDay({
    races: [
      { keibajoCode: "50", raceBango: "07" },
      { keibajoCode: "44", raceBango: "03" },
    ],
    source: "nar",
    targetYmd: "20260529",
  });
  const todayKeys = result.keys.filter((key) => key.startsWith("race-trend-today:")).toSorted();
  expect(todayKeys).toStrictEqual([
    "race-trend-today:v9:nar:20260529:44",
    "race-trend-today:v9:nar:20260529:50",
  ]);
});
