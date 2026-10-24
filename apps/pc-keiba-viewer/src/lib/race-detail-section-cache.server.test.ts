// Run with bun (vitest).
import { afterEach, beforeEach, expect, it, vi } from "vitest";

import { parseStaleDetailSectionEnvelope } from "./race-detail-section-stale";
import type { RaceDetail } from "./race-types";

vi.mock("server-only", () => ({}));

const { getCloudflareContextMock } = vi.hoisted(() => ({
  getCloudflareContextMock: vi.fn<() => Promise<unknown>>(),
}));
vi.mock("@opennextjs/cloudflare", () => ({
  getCloudflareContext: getCloudflareContextMock,
}));

import {
  getCachedDetailSectionResponse,
  getDetailSectionCacheTtlSeconds,
  getStaleDetailSectionBody,
  putDetailSectionCache,
  resolveDetailSectionCacheKey,
} from "./race-detail-section-cache.server";

type KvGetFn = (key: string) => Promise<string | null>;
type KvPutFn = (key: string, value: string, options?: { expirationTtl: number }) => Promise<void>;
type CacheMatchFn = (request: Request) => Promise<Response | undefined>;
type CachePutFn = (request: Request, response: Response) => Promise<void>;

interface KvStub {
  get: ReturnType<typeof vi.fn<KvGetFn>>;
  put: ReturnType<typeof vi.fn<KvPutFn>>;
}

interface CacheStub {
  match: ReturnType<typeof vi.fn<CacheMatchFn>>;
  put: ReturnType<typeof vi.fn<CachePutFn>>;
}

const FIXED_NOW_MS = Date.parse("2026-07-04T00:00:00+09:00");
const EXPECTED_EMPTY_TTL_SECONDS = 10 * 60;

// hassoJikoku is ~1.5 days after FIXED_NOW_MS, so the "race start + 6h" TTL
// comfortably exceeds EXPECTED_EMPTY_TTL_SECONDS, letting the two TTL tiers
// be distinguished unambiguously in assertions.
const FUTURE_RACE: RaceDetail = {
  babajotaiCodeDirt: "0",
  babajotaiCodeShiba: "0",
  gradeCode: null,
  hassoJikoku: "1200",
  jockeyNames: [],
  kaisaiKai: "2",
  kaisaiNen: "2026",
  kaisaiNichime: "5",
  kaisaiTsukihi: "0705",
  keibajoCode: "05",
  kyori: "1600",
  kyosoJokenCode: "703",
  kyosoJokenMeisho: null,
  kyosoKigoCode: null,
  kyosomeiFukudai: null,
  kyosomeiHondai: null,
  kyosomeiKakkonai: null,
  kyosoShubetsuCode: "11",
  juryoShubetsuCode: "1",
  raceBango: "11",
  shussoTosu: "16",
  source: "jra",
  tenkoCode: "1",
  torokuTosu: "16",
  trackCode: "10",
};

const buildKvStub = (): KvStub => ({
  get: vi.fn<KvGetFn>().mockResolvedValue(null),
  put: vi.fn<KvPutFn>().mockResolvedValue(undefined),
});

const buildCacheStub = (): CacheStub => ({
  match: vi.fn<CacheMatchFn>().mockResolvedValue(undefined),
  put: vi.fn<CachePutFn>().mockResolvedValue(undefined),
});

const readFreshUntil = (raw: string | undefined): number | undefined =>
  parseStaleDetailSectionEnvelope(raw ?? "")?.freshUntil;

const mockEnvWithKv = (kv: KvStub): void => {
  getCloudflareContextMock.mockResolvedValue({ ctx: null, env: { DETAIL_SECTION_CACHE_KV: kv } });
};

const emptyTrainingBody = JSON.stringify({
  sourceLabel: "JRA",
  stableComments: [],
  trainings: [{ chokyoNengappi: "2026/07/03", umaban: "1" }],
  type: "training",
});

const filledTrainingBody = JSON.stringify({
  sourceLabel: "JRA",
  stableComments: [],
  trainings: [
    {
      chokyoNengappi: "2026/07/03",
      premiumEvaluationGrade: "A",
      umaban: "1",
    },
  ],
  type: "training",
});

const emptyPremiumDataTopBody = JSON.stringify({
  dataTopHorses: [],
  type: "premium-data-top",
});

const filledPremiumDataTopBody = JSON.stringify({
  dataTopHorses: [{ horseNumber: "1", rank: 1 }],
  type: "premium-data-top",
});

// NAR (non-Ban-ei): fetchPremiumRacePayload short-circuits to empty training
// content for every source other than "jra", so an empty NAR training body
// is a permanent state, not a transient one.
const NAR_RACE: RaceDetail = { ...FUTURE_RACE, keibajoCode: "44", source: "nar" };

// Ban-ei keibajoCode (81-84): getPremiumDataTopHorsesWithCache short-circuits
// to an empty payload for Ban-ei specifically, so an empty premium-data-top
// body there is likewise permanent.
const BAN_EI_RACE: RaceDetail = { ...FUTURE_RACE, keibajoCode: "83", source: "nar" };

beforeEach(() => {
  getCloudflareContextMock.mockReset();
  vi.useFakeTimers();
  vi.setSystemTime(FIXED_NOW_MS);
});

afterEach(() => {
  vi.useRealTimers();
  Reflect.deleteProperty(globalThis, "caches");
});

it("computes the full race-start+6h TTL for a normal race payload", () => {
  const ttlSeconds = getDetailSectionCacheTtlSeconds(FUTURE_RACE, null, FIXED_NOW_MS);
  expect(ttlSeconds > EXPECTED_EMPTY_TTL_SECONDS).toBe(true);
});

it("returns a non-empty training section from edge cache", async () => {
  const kv = buildKvStub();
  const cache = buildCacheStub();
  cache.match.mockResolvedValue(new Response(filledTrainingBody));
  mockEnvWithKv(kv);
  Object.defineProperty(globalThis, "caches", {
    configurable: true,
    value: { default: cache },
  });
  const response = await getCachedDetailSectionResponse("training-key");
  expect(await response?.json()).toStrictEqual(JSON.parse(filledTrainingBody));
  expect(response?.headers.get("X-Detail-Section-Cache")).toBe("HIT-cache-api");
  expect(kv.get).not.toHaveBeenCalled();
});

it("rejects an empty edge training entry and returns the non-empty KV entry", async () => {
  const kv = buildKvStub();
  kv.get.mockResolvedValue(filledTrainingBody);
  const cache = buildCacheStub();
  cache.match.mockResolvedValue(new Response(emptyTrainingBody));
  mockEnvWithKv(kv);
  Object.defineProperty(globalThis, "caches", {
    configurable: true,
    value: { default: cache },
  });
  const response = await getCachedDetailSectionResponse("training-key");
  expect(await response?.json()).toStrictEqual(JSON.parse(filledTrainingBody));
  expect(response?.headers.get("X-Detail-Section-Cache")).toBe("HIT-kv");
});

it("treats an empty KV training entry as a cache miss", async () => {
  const kv = buildKvStub();
  kv.get.mockResolvedValue(emptyTrainingBody);
  mockEnvWithKv(kv);
  expect(await getCachedDetailSectionResponse("training-key")).toBeNull();
});

it("treats a fresh but empty stale training entry as a cache miss", async () => {
  const kv = buildKvStub();
  kv.get.mockResolvedValue(JSON.stringify({ payload: emptyTrainingBody, writtenAt: FIXED_NOW_MS }));
  mockEnvWithKv(kv);
  expect(await getStaleDetailSectionBody("training-key", FIXED_NOW_MS)).toBeNull();
});

it("does not cache an empty training section body", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  await putDetailSectionCache({
    body: emptyTrainingBody,
    cacheKey: "training-key",
    race: FUTURE_RACE,
  });
  expect(kv.put).not.toHaveBeenCalled();
});

it("caches a training section body with a compact workout date using the full long TTL", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  await putDetailSectionCache({
    body: JSON.stringify({
      sourceLabel: "JRA",
      stableComments: [],
      trainings: [{ chokyoNengappi: "20260903", umaban: "1" }],
      type: "training",
    }),
    cacheKey: "training-key",
    race: FUTURE_RACE,
  });
  const mainPutCall = kv.put.mock.calls.find((call) => call[0] === "training-key");
  expect(mainPutCall?.[2]).toStrictEqual({ expirationTtl: 2592000 });
  expect(readFreshUntil(mainPutCall?.[1])).toBe(1783242000000);
});

it("caches a training section body with a premium evaluation grade using the full long TTL", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  await putDetailSectionCache({
    body: filledTrainingBody,
    cacheKey: "training-key",
    race: FUTURE_RACE,
  });
  const mainPutCall = kv.put.mock.calls.find((call) => call[0] === "training-key");
  expect(mainPutCall?.[2]).toStrictEqual({ expirationTtl: 2592000 });
  expect(readFreshUntil(mainPutCall?.[1])).toBe(1783242000000);
});

it("does not cache an empty premium-data-top section body", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  await putDetailSectionCache({
    body: emptyPremiumDataTopBody,
    cacheKey: "data-top-key",
    race: FUTURE_RACE,
  });
  expect(kv.put).not.toHaveBeenCalled();
});

it("caches a non-empty premium-data-top section body using the full long TTL", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  await putDetailSectionCache({
    body: filledPremiumDataTopBody,
    cacheKey: "data-top-key",
    race: FUTURE_RACE,
  });
  const mainPutCall = kv.put.mock.calls.find((call) => call[0] === "data-top-key");
  expect(mainPutCall?.[2]).toStrictEqual({ expirationTtl: 2592000 });
  expect(readFreshUntil(mainPutCall?.[1])).toBe(1783242000000);
});

it("does not cache an empty NAR training section body", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  await putDetailSectionCache({
    body: emptyTrainingBody,
    cacheKey: "training-key",
    race: NAR_RACE,
  });
  expect(kv.put).not.toHaveBeenCalled();
});

it("does not cache an empty premium-data-top section for a NAR race", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  await putDetailSectionCache({
    body: emptyPremiumDataTopBody,
    cacheKey: "data-top-key",
    race: NAR_RACE,
  });
  expect(kv.put).not.toHaveBeenCalled();
});

it("does not cache an empty premium-data-top section for a Ban-ei race", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  await putDetailSectionCache({
    body: emptyPremiumDataTopBody,
    cacheKey: "data-top-key",
    race: BAN_EI_RACE,
  });
  expect(kv.put).not.toHaveBeenCalled();
});

it("uses the full long TTL for a section type the emptiness check does not recognize", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  const body = JSON.stringify({ overallScore: 87, type: "overall-score" });
  await putDetailSectionCache({ body, cacheKey: "overall-score-key", race: FUTURE_RACE });
  const mainPutCall = kv.put.mock.calls.find((call) => call[0] === "overall-score-key");
  expect(mainPutCall?.[2]).toStrictEqual({ expirationTtl: 2592000 });
  expect(readFreshUntil(mainPutCall?.[1])).toBe(1783242000000);
});

it("treats malformed JSON as non-empty and uses the full long TTL", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  await putDetailSectionCache({
    body: "{not valid json",
    cacheKey: "broken-key",
    race: FUTURE_RACE,
  });
  const mainPutCall = kv.put.mock.calls.find((call) => call[0] === "broken-key");
  expect(mainPutCall?.[2]).toStrictEqual({ expirationTtl: 2592000 });
  expect(readFreshUntil(mainPutCall?.[1])).toBe(1783242000000);
});

it("does not write an empty training body to edge cache", async () => {
  const kv = buildKvStub();
  const cache = buildCacheStub();
  mockEnvWithKv(kv);
  Object.defineProperty(globalThis, "caches", {
    configurable: true,
    value: { default: cache },
  });
  await putDetailSectionCache({
    body: emptyTrainingBody,
    cacheKey: "training-key",
    race: FUTURE_RACE,
  });
  expect(cache.put).not.toHaveBeenCalled();
});

it("does not write an empty training body to stale cache", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  await putDetailSectionCache({
    body: emptyTrainingBody,
    cacheKey: "training-key",
    race: FUTURE_RACE,
  });
  expect(kv.put).not.toHaveBeenCalled();
});

it("rejects populated legacy overseas time-score entries at edge, KV, stale and write boundaries", async () => {
  const key = "race-detail-section:v12-overseas-readers:2026:10:04:A8:05:time-score:default";
  const body = JSON.stringify({
    type: "time-score",
    runners: [
      { umaban: "04", kettoTorokuBango: "2021105369" },
      { umaban: "05", kettoTorokuBango: "2021103272" },
    ],
    rows: [{ horseNumber: "4" }, { horseNumber: "5" }],
  });
  const kv = buildKvStub();
  const cache = buildCacheStub();
  cache.match.mockResolvedValue(new Response(body));
  kv.get.mockResolvedValue(body);
  mockEnvWithKv(kv);
  Object.defineProperty(globalThis, "caches", { configurable: true, value: { default: cache } });
  expect(await getCachedDetailSectionResponse(key)).toBeNull();
  kv.get.mockResolvedValue(JSON.stringify({ payload: body, writtenAt: FIXED_NOW_MS }));
  expect(await getStaleDetailSectionBody(key, FIXED_NOW_MS)).toBeNull();
  await putDetailSectionCache({ cacheKey: key, body, race: { ...FUTURE_RACE, keibajoCode: "A8" } });
  expect(kv.put).not.toHaveBeenCalled();
  expect(cache.put).not.toHaveBeenCalled();
});
it("accepts overseas time-score caches with explicit unknown history evidence", async () => {
  const key = "race-detail-section:v12-overseas-readers:2026:10:04:A8:05:time-score:default";
  const body = JSON.stringify({
    type: "time-score",
    runners: [{ umaban: "01", sourceHorseId: "000a029d42", kettoTorokuBango: "0000000000" }],
    rows: [
      {
        horseNumber: "1",
        historyEvidence: {
          generation: "overseas-readers-v1",
          historyHorseId: "000a029d42",
          selectedHistoryCount: 0,
          overseasHistoryCount: 0,
          selection: "overseas",
        },
      },
    ],
  });
  const kv = buildKvStub();
  kv.get.mockResolvedValue(body);
  mockEnvWithKv(kv);
  expect(await getCachedDetailSectionResponse(key)).not.toBeNull();
});

it("rejects a legacy overseas condition cache without gate provenance", async () => {
  const kv = buildKvStub();
  kv.get.mockResolvedValue(
    JSON.stringify({ type: "condition", frameStats: [{ frameNumber: "1", count: 123 }] }),
  );
  mockEnvWithKv(kv);
  expect(
    await getCachedDetailSectionResponse(
      "race-detail-section:v19:2026:10:04:A8:05:condition:default",
    ),
  ).toBeNull();
});

it("never reads the cache when the race generation could not be resolved", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  expect(await getCachedDetailSectionResponse(null)).toBeNull();
  expect(kv.get).not.toHaveBeenCalled();
});

it("serves a generational envelope as fresh before freshUntil", async () => {
  const kv = buildKvStub();
  kv.get.mockResolvedValue(
    JSON.stringify({
      freshUntil: 1783090860000,
      payload: '{"type":"results"}',
      writtenAt: 1783090000000,
    }),
  );
  mockEnvWithKv(kv);
  const response = await getCachedDetailSectionResponse(
    "race-detail-section:v4:2026:07:05:05:11:results:default:g1783000000000",
  );
  expect(await response?.text()).toBe('{"type":"results"}');
  expect(response?.headers.get("X-Detail-Section-Cache")).toBe("HIT-kv");
  expect(kv.get.mock.calls).toStrictEqual([
    ["race-detail-section:v4:2026:07:05:05:11:results:default:g1783000000000"],
  ]);
});

it("treats a generational envelope past freshUntil as a fresh-tier miss", async () => {
  const kv = buildKvStub();
  kv.get.mockResolvedValue(
    JSON.stringify({
      freshUntil: 1783090800000,
      payload: '{"type":"results"}',
      writtenAt: 1783090000000,
    }),
  );
  mockEnvWithKv(kv);
  expect(
    await getCachedDetailSectionResponse(
      "race-detail-section:v4:2026:07:05:05:11:results:default:g1783000000000",
    ),
  ).toBeNull();
  expect(kv.get.mock.calls).toStrictEqual([
    ["race-detail-section:v4:2026:07:05:05:11:results:default:g1783000000000"],
  ]);
});

it("treats a generational envelope without freshUntil as a fresh-tier miss", async () => {
  const kv = buildKvStub();
  kv.get.mockResolvedValue(
    JSON.stringify({ payload: '{"type":"results"}', writtenAt: 1783090000000 }),
  );
  mockEnvWithKv(kv);
  expect(
    await getCachedDetailSectionResponse(
      "race-detail-section:v4:2026:07:05:05:11:results:default:g1783000000000",
    ),
  ).toBeNull();
});

it("falls back to the legacy raw entry while the race has never been busted", async () => {
  const kv = buildKvStub();
  kv.get.mockResolvedValueOnce(null).mockResolvedValueOnce('{"type":"results"}');
  mockEnvWithKv(kv);
  const response = await getCachedDetailSectionResponse(
    "race-detail-section:v4:2026:07:05:05:11:results:default:g0",
  );
  expect(await response?.text()).toBe('{"type":"results"}');
  expect(kv.get.mock.calls).toStrictEqual([
    ["race-detail-section:v4:2026:07:05:05:11:results:default:g0"],
    ["race-detail-section:v4:2026:07:05:05:11:results:default"],
  ]);
});

it("reads the stale tier from the generational envelope itself", async () => {
  const kv = buildKvStub();
  kv.get.mockResolvedValue(
    JSON.stringify({
      freshUntil: 1783090800000,
      payload: '{"type":"results"}',
      writtenAt: 1783090800000,
    }),
  );
  mockEnvWithKv(kv);
  expect(
    await getStaleDetailSectionBody(
      "race-detail-section:v4:2026:07:05:05:11:results:default:g1783000000000",
      FIXED_NOW_MS,
    ),
  ).toBe('{"type":"results"}');
  expect(kv.get.mock.calls).toStrictEqual([
    ["race-detail-section:v4:2026:07:05:05:11:results:default:g1783000000000"],
  ]);
});

it("reads the legacy stale key while the race has never been busted", async () => {
  const kv = buildKvStub();
  kv.get
    .mockResolvedValueOnce(null)
    .mockResolvedValueOnce(
      JSON.stringify({ payload: '{"type":"results"}', writtenAt: 1783090800000 }),
    );
  mockEnvWithKv(kv);
  expect(
    await getStaleDetailSectionBody(
      "race-detail-section:v4:2026:07:05:05:11:results:default:g0",
      FIXED_NOW_MS,
    ),
  ).toBe('{"type":"results"}');
  expect(kv.get.mock.calls).toStrictEqual([
    ["race-detail-section:v4:2026:07:05:05:11:results:default:g0"],
    ["stale:race-detail-section:v4:2026:07:05:05:11:results:default"],
  ]);
});

it("does not read legacy stale keys once the race has been busted", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  expect(
    await getStaleDetailSectionBody(
      "race-detail-section:v4:2026:07:05:05:11:results:default:g1783000000000",
      FIXED_NOW_MS,
    ),
  ).toBeNull();
  expect(kv.get.mock.calls).toStrictEqual([
    ["race-detail-section:v4:2026:07:05:05:11:results:default:g1783000000000"],
  ]);
});

it("writes one KV entry and the edge cache for an upcoming race", async () => {
  const kv = buildKvStub();
  const cache = buildCacheStub();
  mockEnvWithKv(kv);
  Object.defineProperty(globalThis, "caches", { configurable: true, value: { default: cache } });
  await putDetailSectionCache({
    body: '{"type":"results"}',
    cacheKey: "race-detail-section:v4:2026:07:05:05:11:results:default:g1783000000000",
    race: FUTURE_RACE,
  });
  expect(kv.put.mock.calls).toStrictEqual([
    [
      "race-detail-section:v4:2026:07:05:05:11:results:default:g1783000000000",
      '{"freshUntil":1783242000000,"payload":"{\\"type\\":\\"results\\"}","writtenAt":1783090800000}',
      { expirationTtl: 2592000 },
    ],
  ]);
  expect(cache.put).toHaveBeenCalledTimes(1);
});

it("writes only a stale-tier envelope once the fresh window has passed", async () => {
  const kv = buildKvStub();
  const cache = buildCacheStub();
  mockEnvWithKv(kv);
  Object.defineProperty(globalThis, "caches", { configurable: true, value: { default: cache } });
  await putDetailSectionCache({
    body: '{"type":"results"}',
    cacheKey: "race-detail-section:v4:2026:07:01:05:11:results:default:g1783000000000",
    race: { ...FUTURE_RACE, kaisaiTsukihi: "0701" },
  });
  expect(kv.put.mock.calls).toStrictEqual([
    [
      "race-detail-section:v4:2026:07:01:05:11:results:default:g1783000000000",
      '{"freshUntil":1783090800000,"payload":"{\\"type\\":\\"results\\"}","writtenAt":1783090800000}',
      { expirationTtl: 2592000 },
    ],
  ]);
  expect(cache.put).not.toHaveBeenCalled();
});

it("resolveDetailSectionCacheKey qualifies the section key with the race generation", async () => {
  const kv = buildKvStub();
  kv.get.mockResolvedValue("1783000000000");
  mockEnvWithKv(kv);
  expect(
    await resolveDetailSectionCacheKey({
      day: "05",
      keibajoCode: "05",
      month: "07",
      raceNumber: "11",
      section: "results",
      year: "2026",
    }),
  ).toBe("race-detail-section:v4:2026:07:05:05:11:results:default:g1783000000000");
  expect(kv.get.mock.calls).toStrictEqual([["race-cache:gen:v2:2026:0705:05:11"]]);
});
