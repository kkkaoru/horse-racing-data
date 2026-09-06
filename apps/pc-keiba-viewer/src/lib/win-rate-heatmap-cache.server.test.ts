// bun で実行する (bunx vitest)
import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const { getCloudflareContextMock } = vi.hoisted(() => ({
  getCloudflareContextMock: vi.fn<() => Promise<unknown>>(),
}));
vi.mock("@opennextjs/cloudflare", () => ({
  getCloudflareContext: getCloudflareContextMock,
}));

import type { Runner } from "./race-types";
import {
  WIN_RATE_HEATMAP_CACHE_FRAGMENT_KINDS,
  WIN_RATE_HEATMAP_CACHE_TTL_SECONDS,
  buildWinRateHeatmapFragmentCacheKey,
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
  values: Map<string, string>;
}

interface KvStub {
  get: ReturnType<typeof vi.fn<KvGetFn>>;
  put: ReturnType<typeof vi.fn<KvPutFn>>;
  values: Map<string, string>;
}

const RUNNER: Runner = {
  banushimei: "馬主",
  barei: "4",
  bamei: "テスト馬",
  bataiju: "480",
  chokyoshimeiRyakusho: "調教師",
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  damSireName: "母父",
  futanJuryo: "560",
  kakuteiChakujun: "00",
  kettoTorokuBango: "2023100001",
  kishumeiRyakusho: "騎手",
  kohan3f: null,
  seibetsuCode: "1",
  sireName: "父",
  sireSireName: "父父",
  sohaTime: null,
  tanshoNinkijun: "00",
  tanshoOdds: "0000",
  timeSa: null,
  umaban: "01",
  wakuban: "1",
  zogenFugo: null,
  zogenSa: null,
};

const UNUSED_DETAIL = {
  date: "20260901",
  frameNumber: "1",
  horseName: "過去馬",
  horseNumber: "1",
  jockeyName: "過去騎手",
  keibajoCode: "44",
  popularity: "1",
  raceName: "過去レース",
  raceNumber: "1",
  raceTime: "1200",
  rank: "01",
  winOdds: "20",
};

const rateRow = (category: "jockey" | "jockeyFrame" | "trainer") => ({
  category,
  currentHorseNumbers: "1",
  details: [UNUSED_DETAIL],
  horseCount: 0,
  name: `${category}-name`,
  quinellaCount: 0,
  quinellaRate: 0,
  showCount: 0,
  showRate: 0,
  starts: 0,
  winCount: 0,
  winRate: 0,
});

const bloodlineRow = (
  category:
    | "damDamSire"
    | "damSire"
    | "damSireSire"
    | "sire"
    | "sireDamSire"
    | "sireSire"
    | "sireSireSire",
) => ({ ...rateRow("jockey"), category, name: `${category}-name` });

const HEATMAP_PAYLOAD: WinRateHeatmapSectionPayload = {
  bloodlineRows: [
    bloodlineRow("sire"),
    bloodlineRow("damSire"),
    bloodlineRow("sireSire"),
    bloodlineRow("sireDamSire"),
    bloodlineRow("sireSireSire"),
    bloodlineRow("damSireSire"),
    bloodlineRow("damDamSire"),
  ],
  carriedWeightClassStats: [
    {
      key: "55.5-57",
      quinellaCount: 0,
      quinellaRate: 0,
      showCount: 0,
      showRate: 0,
      starts: 0,
      winCount: 0,
      winRate: 0,
    },
  ],
  frameStats: [
    {
      averageFinish: null,
      averagePopularity: null,
      count: 0,
      details: [UNUSED_DETAIL],
      frameNumber: "1",
      medianFinish: null,
      medianPopularity: null,
      quinellaCount: 0,
      quinellaRate: 0,
      runnerCount: null,
      score: 0,
      showCount: 0,
      showRate: 0,
      winCount: 0,
      winRate: 0,
    },
  ],
  horseResults: [
    {
      bataiju: "480",
      currentUmaban: "01",
      futanJuryo: "560",
      kakuteiChakujun: "02",
      keibajoCode: "44",
    },
  ],
  runners: [RUNNER],
  similarRows: [rateRow("jockeyFrame"), rateRow("jockey"), rateRow("trainer")],
  type: "win-rate-heatmap",
  weightClassStats: [
    {
      key: "480-499",
      quinellaCount: 0,
      quinellaRate: 0,
      showCount: 0,
      showRate: 0,
      starts: 0,
      winCount: 0,
      winRate: 0,
    },
  ],
};

const buildCacheStub = (): CacheStub => {
  const values = new Map<string, string>();
  return {
    delete: vi.fn<CacheDeleteFn>(async (request) => values.delete(request.url)),
    match: vi.fn<CacheMatchFn>(async (request) => {
      const value = values.get(request.url);
      return value === undefined ? undefined : new Response(value, { status: 200 });
    }),
    put: vi.fn<CachePutFn>(async (request, response) => {
      values.set(request.url, await response.text());
    }),
    values,
  };
};

const buildKvStub = (): KvStub => {
  const values = new Map<string, string>();
  return {
    get: vi.fn<KvGetFn>(async (key) => values.get(key) ?? null),
    put: vi.fn<KvPutFn>(async (key, value) => {
      values.set(key, value);
    }),
    values,
  };
};

const setDefaultCache = (cache: CacheStub): void => {
  Object.defineProperty(globalThis, "caches", {
    configurable: true,
    value: { default: cache },
  });
};

const setRuntime = (kv: KvStub, waitUntil?: (promise: Promise<unknown>) => void): void => {
  getCloudflareContextMock.mockResolvedValue({
    ctx: waitUntil === undefined ? null : { waitUntil },
    env: { DETAIL_SECTION_CACHE_KV: kv },
  });
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const parseRecord = (text: string): Record<string, unknown> => {
  const value: unknown = JSON.parse(text);
  if (!isRecord(value)) throw new Error("Expected JSON object");
  return value;
};

const storePayload = async (cache: CacheStub, kv: KvStub): Promise<string> => {
  setDefaultCache(cache);
  setRuntime(kv);
  await putWinRateHeatmapCache({ cacheKey: "heatmap-key", payload: HEATMAP_PAYLOAD });
  const manifest = parseRecord(kv.values.get("heatmap-key") ?? "null");
  if (typeof manifest.generation !== "string") throw new Error("Expected cache generation");
  return manifest.generation;
};

beforeEach(() => {
  getCloudflareContextMock.mockReset();
});

afterEach(() => {
  Reflect.deleteProperty(globalThis, "caches");
});

it("writes compact column fragments first and publishes the manifest last", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  const generation = await storePayload(cache, kv);

  expect(kv.put).toHaveBeenCalledTimes(WIN_RATE_HEATMAP_CACHE_FRAGMENT_KINDS.length + 1);
  expect(kv.put.mock.calls.at(-1)?.[0]).toBe("heatmap-key");
  expect(cache.put).toHaveBeenCalledTimes(WIN_RATE_HEATMAP_CACHE_FRAGMENT_KINDS.length + 1);
  expect(kv.put.mock.calls.every((call) => call[2]?.expirationTtl === 129600)).toBe(true);

  const jockeyKey = buildWinRateHeatmapFragmentCacheKey("heatmap-key", generation, "jockey");
  const jockeyBody = kv.values.get(jockeyKey) ?? "";
  expect(jockeyBody).not.toContain("details");
  expect(jockeyBody).not.toContain("horseCount");
  expect(jockeyBody).toContain('"starts":0');
  expect(jockeyBody).toContain('"winRate":0');

  const horseKey = buildWinRateHeatmapFragmentCacheKey("heatmap-key", generation, "horse");
  expect(kv.values.get(horseKey)).not.toContain("bamei");
  expect(kv.values.get(horseKey)).not.toContain("kakuteiChakujun");
  expect(kv.values.get(horseKey)).not.toContain("bataiju");
  expect(kv.values.get(horseKey)).toContain('"starts":1');
  expect(kv.values.get(horseKey)).toContain('"winCount":0');
  expect(parseRecord(kv.values.get("heatmap-key") ?? "null")).not.toHaveProperty("runners");
});

it("assembles all column fragments and preserves real zero values", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  await storePayload(cache, kv);

  const hit = await getCachedWinRateHeatmapPayload("heatmap-key", [RUNNER]);
  expect(hit?.type).toBe("win-rate-heatmap");
  expect(hit?.runners).toStrictEqual([RUNNER]);
  expect(hit?.bloodlineRows).toHaveLength(7);
  expect(hit?.bloodlineRows[0]).toMatchObject({ starts: 0, winCount: 0, winRate: 0 });
  expect(hit?.horseResults).toStrictEqual([]);
  expect(hit?.horseRateStats).toStrictEqual([
    { horseNumber: "1", quinellaCount: 1, showCount: 1, starts: 1, winCount: 0 },
  ]);
  expect(hit?.similarRows).toHaveLength(3);
  expect(hit?.similarRows[0]).toMatchObject({ starts: 0, winRate: 0 });
  expect(hit?.carriedWeightClassStats[0]).toMatchObject({ starts: 0, winRate: 0 });
  expect(hit?.weightClassStats[0]).toMatchObject({ starts: 0, winRate: 0 });
  expect(hit?.frameStats[0]).toMatchObject({ count: 0, details: [], winRate: 0 });
});

it("serves all fragments from Cache API without reading KV", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  await storePayload(cache, kv);
  kv.get.mockClear();

  await expect(getCachedWinRateHeatmapPayload("heatmap-key", [RUNNER])).resolves.not.toBe(null);
  expect(kv.get).not.toHaveBeenCalled();
});

it("returns a miss when the runner generation changes before reading fragments", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  await storePayload(cache, kv);
  cache.match.mockClear();
  kv.get.mockClear();

  await expect(
    getCachedWinRateHeatmapPayload("heatmap-key", [{ ...RUNNER, kettoTorokuBango: "2023109999" }]),
  ).resolves.toBe(null);
  expect(cache.match).toHaveBeenCalledTimes(1);
  expect(kv.get).not.toHaveBeenCalled();
});

it("returns a miss when one required column fragment is absent", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  const generation = await storePayload(cache, kv);
  const key = buildWinRateHeatmapFragmentCacheKey("heatmap-key", generation, "trainer");
  kv.values.delete(key);
  cache.values.delete(
    `https://pc-keiba-viewer.local/win-rate-heatmap-cache/${encodeURIComponent(key)}`,
  );

  await expect(getCachedWinRateHeatmapPayload("heatmap-key", [RUNNER])).resolves.toBe(null);
});

it("distinguishes an explicitly empty available fragment from a missing fragment", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  const payload: WinRateHeatmapSectionPayload = {
    ...HEATMAP_PAYLOAD,
    horseRateStats: [{ horseNumber: "1", quinellaCount: 0, showCount: 0, starts: 0, winCount: 0 }],
    similarRows: [],
  };
  setDefaultCache(cache);
  setRuntime(kv);
  await putWinRateHeatmapCache({ cacheKey: "heatmap-key", payload });

  const hit = await getCachedWinRateHeatmapPayload("heatmap-key", [RUNNER]);
  expect(hit).not.toBe(null);
  expect(hit?.horseRateStats).toStrictEqual([
    { horseNumber: "1", quinellaCount: 0, showCount: 0, starts: 0, winCount: 0 },
  ]);
  expect(hit?.similarRows).toStrictEqual([]);
});

it("rejects malformed numeric rows instead of confusing missing data with zero", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  const generation = await storePayload(cache, kv);
  const key = buildWinRateHeatmapFragmentCacheKey("heatmap-key", generation, "jockey");
  const fragment = parseRecord(kv.values.get(key) ?? "null");
  fragment.rows = [{ starts: "0", winRate: 0 }];
  const malformed = JSON.stringify(fragment);
  kv.values.set(key, malformed);
  cache.values.set(
    `https://pc-keiba-viewer.local/win-rate-heatmap-cache/${encodeURIComponent(key)}`,
    malformed,
  );

  await expect(getCachedWinRateHeatmapPayload("heatmap-key", [RUNNER])).resolves.toBe(null);
});

it("deletes malformed Cache API JSON and repopulates it from KV", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  await storePayload(cache, kv);
  cache.values.set("https://pc-keiba-viewer.local/win-rate-heatmap-cache/heatmap-key", "not-json");
  cache.delete.mockClear();
  cache.put.mockClear();

  await expect(getCachedWinRateHeatmapPayload("heatmap-key", [RUNNER])).resolves.not.toBe(null);
  expect(cache.delete).toHaveBeenCalledTimes(1);
  expect(cache.put).toHaveBeenCalledTimes(1);
  const stored = cache.put.mock.calls[0]?.[1];
  expect(stored?.headers.get("Cache-Control")).toBe(
    `public, max-age=${WIN_RATE_HEATMAP_CACHE_TTL_SECONDS}`,
  );
});

it("uses waitUntil when promoting KV fragments into Cache API", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  setDefaultCache(cache);
  setRuntime(kv);
  await putWinRateHeatmapCache({ cacheKey: "heatmap-key", payload: HEATMAP_PAYLOAD });
  cache.values.clear();
  const waitUntil = vi.fn<(promise: Promise<unknown>) => void>();
  setRuntime(kv, waitUntil);

  await expect(getCachedWinRateHeatmapPayload("heatmap-key", [RUNNER])).resolves.not.toBe(null);
  expect(waitUntil).toHaveBeenCalledTimes(WIN_RATE_HEATMAP_CACHE_FRAGMENT_KINDS.length + 1);
});

it("returns a miss for invalid KV JSON or an absent manifest", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  setDefaultCache(cache);
  setRuntime(kv);
  kv.values.set("heatmap-key", "{");
  await expect(getCachedWinRateHeatmapPayload("heatmap-key", [RUNNER])).resolves.toBe(null);
  kv.values.delete("heatmap-key");
  await expect(getCachedWinRateHeatmapPayload("heatmap-key", [RUNNER])).resolves.toBe(null);
});

it("returns a miss without storage reads when the current runner signature is unavailable", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  setDefaultCache(cache);
  setRuntime(kv);
  await expect(
    getCachedWinRateHeatmapPayload("heatmap-key", [
      { ...RUNNER, bamei: null, kettoTorokuBango: null, umaban: null },
    ]),
  ).resolves.toBe(null);
  expect(cache.match).not.toHaveBeenCalled();
  expect(kv.get).not.toHaveBeenCalled();
});

it("throws when KV is unavailable or the payload runner signature is unavailable", async () => {
  const cache = buildCacheStub();
  setDefaultCache(cache);
  getCloudflareContextMock.mockResolvedValue({ ctx: null, env: {} });
  await expect(
    putWinRateHeatmapCache({ cacheKey: "heatmap-key", payload: HEATMAP_PAYLOAD }),
  ).rejects.toThrow("DETAIL_SECTION_CACHE_KV is unavailable");
  await expect(
    putWinRateHeatmapCache({
      cacheKey: "heatmap-key",
      payload: {
        ...HEATMAP_PAYLOAD,
        runners: [{ ...RUNNER, bamei: null, kettoTorokuBango: null, umaban: null }],
      },
    }),
  ).rejects.toThrow("Heatmap runner signature is unavailable");
});

it("keeps every KV fragment when Cache API writes fail", async () => {
  const cache = buildCacheStub();
  const kv = buildKvStub();
  cache.put.mockRejectedValue(new Error("cache api failed"));
  setDefaultCache(cache);
  setRuntime(kv);

  await putWinRateHeatmapCache({ cacheKey: "heatmap-key", payload: HEATMAP_PAYLOAD });
  expect(kv.put).toHaveBeenCalledTimes(WIN_RATE_HEATMAP_CACHE_FRAGMENT_KINDS.length + 1);
});
