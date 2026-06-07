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

import { bustRaceTrendCachesForDay, putRaceTrendCache } from "./race-trend-cache.server";
import type { RaceDetail } from "./race-types";

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

const buildRaceDetail = (overrides: Partial<RaceDetail> = {}): RaceDetail => ({
  babajotaiCodeDirt: null,
  babajotaiCodeShiba: null,
  gradeCode: null,
  hassoJikoku: "1500",
  juryoShubetsuCode: null,
  kaisaiKai: null,
  kaisaiNen: "2099",
  kaisaiNichime: null,
  kaisaiTsukihi: "0529",
  keibajoCode: "05",
  kyori: null,
  kyosoJokenCode: null,
  kyosoJokenMeisho: null,
  kyosoKigoCode: null,
  kyosoShubetsuCode: null,
  kyosomeiFukudai: null,
  kyosomeiHondai: null,
  kyosomeiKakkonai: null,
  raceBango: "07",
  shussoTosu: null,
  source: "jra",
  tenkoCode: null,
  torokuTosu: null,
  trackCode: null,
  ...overrides,
});

it("putRaceTrendCache skips KV PUT when an in-flight gate marker is present", async () => {
  const kv = buildKvStub();
  kv.get.mockResolvedValue("1");
  getCloudflareContextMock.mockResolvedValue({ env: { DETAIL_SECTION_CACHE_KV: kv }, ctx: null });
  await putRaceTrendCache({ body: "{}", cacheKey: "race-trend-key-A", race: buildRaceDetail() });
  expect(kv.put).not.toHaveBeenCalled();
});

it("putRaceTrendCache writes the in-flight marker then the body when no gate is present", async () => {
  const kv = buildKvStub();
  kv.get.mockResolvedValue(null);
  getCloudflareContextMock.mockResolvedValue({ env: { DETAIL_SECTION_CACHE_KV: kv }, ctx: null });
  await putRaceTrendCache({
    body: '{"k":1}',
    cacheKey: "race-trend-key-B",
    race: buildRaceDetail(),
  });
  expect(kv.put).toHaveBeenCalledTimes(2);
  expect(kv.put.mock.calls[0]?.[0]).toBe("race-trend-kv-put-in-flight:race-trend-key-B");
  expect(kv.put.mock.calls[1]?.[0]).toBe("race-trend-key-B");
});

it("putRaceTrendCache treats KV gate get failure as no-gate and still writes the body", async () => {
  const kv = buildKvStub();
  kv.get.mockRejectedValue(new Error("kv get boom"));
  getCloudflareContextMock.mockResolvedValue({ env: { DETAIL_SECTION_CACHE_KV: kv }, ctx: null });
  await putRaceTrendCache({
    body: '{"k":2}',
    cacheKey: "race-trend-key-C",
    race: buildRaceDetail(),
  });
  const putCalls = kv.put.mock.calls.map((call) => String(call[0]));
  expect(putCalls).toStrictEqual([
    "race-trend-kv-put-in-flight:race-trend-key-C",
    "race-trend-key-C",
  ]);
});

it("putRaceTrendCache still writes body when in-flight marker put fails", async () => {
  const kv = buildKvStub();
  kv.get.mockResolvedValue(null);
  kv.put.mockRejectedValueOnce(new Error("marker put boom")).mockResolvedValueOnce(undefined);
  getCloudflareContextMock.mockResolvedValue({ env: { DETAIL_SECTION_CACHE_KV: kv }, ctx: null });
  await putRaceTrendCache({
    body: '{"k":3}',
    cacheKey: "race-trend-key-D",
    race: buildRaceDetail(),
  });
  expect(kv.put).toHaveBeenCalledTimes(2);
  expect(kv.put.mock.calls[1]?.[0]).toBe("race-trend-key-D");
});

it("putRaceTrendCache is a no-op when ttlSeconds is zero (past race)", async () => {
  const kv = buildKvStub();
  getCloudflareContextMock.mockResolvedValue({ env: { DETAIL_SECTION_CACHE_KV: kv }, ctx: null });
  await putRaceTrendCache({
    body: "{}",
    cacheKey: "race-trend-key-E",
    race: buildRaceDetail({ kaisaiNen: "2000" }),
  });
  expect(kv.put).not.toHaveBeenCalled();
  expect(kv.get).not.toHaveBeenCalled();
});
