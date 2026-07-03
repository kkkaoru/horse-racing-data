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
  getDetailSectionCacheTtlSeconds,
  putDetailSectionCache,
} from "./race-detail-section-cache.server";

type KvGetFn = (key: string) => Promise<string | null>;
type KvPutFn = (key: string, value: string, options?: { expirationTtl: number }) => Promise<void>;
type CachePutFn = (request: Request, response: Response) => Promise<void>;

interface KvStub {
  get: ReturnType<typeof vi.fn<KvGetFn>>;
  put: ReturnType<typeof vi.fn<KvPutFn>>;
}

interface CacheStub {
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
  put: vi.fn<CachePutFn>().mockResolvedValue(undefined),
});

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

it("caches an empty training section body (no reviews, no comments) with the short self-heal TTL", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  await putDetailSectionCache({
    body: emptyTrainingBody,
    cacheKey: "training-key",
    race: FUTURE_RACE,
  });
  const mainPutCall = kv.put.mock.calls.find((call) => call[0] === "training-key");
  expect(mainPutCall?.[2]).toStrictEqual({ expirationTtl: EXPECTED_EMPTY_TTL_SECONDS });
});

it("caches a training section body with a premium evaluation grade using the full long TTL", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  await putDetailSectionCache({
    body: filledTrainingBody,
    cacheKey: "training-key",
    race: FUTURE_RACE,
  });
  const fullTtlSeconds = getDetailSectionCacheTtlSeconds(FUTURE_RACE, null, FIXED_NOW_MS);
  const mainPutCall = kv.put.mock.calls.find((call) => call[0] === "training-key");
  expect(mainPutCall?.[2]).toStrictEqual({ expirationTtl: fullTtlSeconds });
});

it("caches an empty premium-data-top section body with the short self-heal TTL", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  await putDetailSectionCache({
    body: emptyPremiumDataTopBody,
    cacheKey: "data-top-key",
    race: FUTURE_RACE,
  });
  const mainPutCall = kv.put.mock.calls.find((call) => call[0] === "data-top-key");
  expect(mainPutCall?.[2]).toStrictEqual({ expirationTtl: EXPECTED_EMPTY_TTL_SECONDS });
});

it("caches a non-empty premium-data-top section body using the full long TTL", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  await putDetailSectionCache({
    body: filledPremiumDataTopBody,
    cacheKey: "data-top-key",
    race: FUTURE_RACE,
  });
  const fullTtlSeconds = getDetailSectionCacheTtlSeconds(FUTURE_RACE, null, FIXED_NOW_MS);
  const mainPutCall = kv.put.mock.calls.find((call) => call[0] === "data-top-key");
  expect(mainPutCall?.[2]).toStrictEqual({ expirationTtl: fullTtlSeconds });
});

it("caches an empty NAR training section body using the full long TTL (no premium content ever lands for NAR)", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  await putDetailSectionCache({
    body: emptyTrainingBody,
    cacheKey: "training-key",
    race: NAR_RACE,
  });
  const fullTtlSeconds = getDetailSectionCacheTtlSeconds(NAR_RACE, null, FIXED_NOW_MS);
  const mainPutCall = kv.put.mock.calls.find((call) => call[0] === "training-key");
  expect(mainPutCall?.[2]).toStrictEqual({ expirationTtl: fullTtlSeconds });
});

it("caches an empty premium-data-top section body for a non-Ban-ei NAR race using the short self-heal TTL", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  await putDetailSectionCache({
    body: emptyPremiumDataTopBody,
    cacheKey: "data-top-key",
    race: NAR_RACE,
  });
  const mainPutCall = kv.put.mock.calls.find((call) => call[0] === "data-top-key");
  expect(mainPutCall?.[2]).toStrictEqual({ expirationTtl: EXPECTED_EMPTY_TTL_SECONDS });
});

it("caches an empty premium-data-top section body for a Ban-ei race using the full long TTL (data-top never lands for Ban-ei)", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  await putDetailSectionCache({
    body: emptyPremiumDataTopBody,
    cacheKey: "data-top-key",
    race: BAN_EI_RACE,
  });
  const fullTtlSeconds = getDetailSectionCacheTtlSeconds(BAN_EI_RACE, null, FIXED_NOW_MS);
  const mainPutCall = kv.put.mock.calls.find((call) => call[0] === "data-top-key");
  expect(mainPutCall?.[2]).toStrictEqual({ expirationTtl: fullTtlSeconds });
});

it("uses the full long TTL for a section type the emptiness check does not recognize", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  const body = JSON.stringify({ overallScore: 87, type: "overall-score" });
  await putDetailSectionCache({ body, cacheKey: "overall-score-key", race: FUTURE_RACE });
  const fullTtlSeconds = getDetailSectionCacheTtlSeconds(FUTURE_RACE, null, FIXED_NOW_MS);
  const mainPutCall = kv.put.mock.calls.find((call) => call[0] === "overall-score-key");
  expect(mainPutCall?.[2]).toStrictEqual({ expirationTtl: fullTtlSeconds });
});

it("treats malformed JSON as non-empty and uses the full long TTL", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  await putDetailSectionCache({
    body: "{not valid json",
    cacheKey: "broken-key",
    race: FUTURE_RACE,
  });
  const fullTtlSeconds = getDetailSectionCacheTtlSeconds(FUTURE_RACE, null, FIXED_NOW_MS);
  const mainPutCall = kv.put.mock.calls.find((call) => call[0] === "broken-key");
  expect(mainPutCall?.[2]).toStrictEqual({ expirationTtl: fullTtlSeconds });
});

it("writes the short-TTL Cache-Control header to the edge cache for an empty training body", async () => {
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
  const cacheControl = cache.put.mock.calls[0]?.[1].headers.get("Cache-Control");
  expect(cacheControl).toBe(`public, max-age=${EXPECTED_EMPTY_TTL_SECONDS}`);
});

it("still writes the 30-day stale snapshot even when the fresh tier gets the short empty TTL", async () => {
  const kv = buildKvStub();
  mockEnvWithKv(kv);
  await putDetailSectionCache({
    body: emptyTrainingBody,
    cacheKey: "training-key",
    race: FUTURE_RACE,
  });
  const staleCall = kv.put.mock.calls.find((call) => call[0] === "stale:training-key");
  expect(staleCall?.[2]).toStrictEqual({ expirationTtl: 30 * 24 * 60 * 60 });
});
