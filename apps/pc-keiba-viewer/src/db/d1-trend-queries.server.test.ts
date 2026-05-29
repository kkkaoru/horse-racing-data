// Run with bun.
import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const { getCloudflareContextMock } = vi.hoisted(() => ({
  getCloudflareContextMock: vi.fn<() => Promise<unknown>>(),
}));
vi.mock("@opennextjs/cloudflare", () => ({
  getCloudflareContext: getCloudflareContextMock,
}));

import {
  buildPast14WindowForTarget,
  getLatestTanshoOddsFromHotD1,
  getRaceTrendPast14StarterRows,
  getRaceTrendRunningStylesFromD1,
  getRaceTrendTodayRunningStylesFromD1,
  getRaceTrendTodayStarterRows,
} from "./d1-trend-queries.server";

type AnyMockFn = (...args: never[]) => unknown;

interface PreparedStub {
  all: ReturnType<typeof vi.fn<AnyMockFn>>;
  bind: ReturnType<typeof vi.fn<AnyMockFn>>;
}

interface D1Stub {
  batch: ReturnType<typeof vi.fn<AnyMockFn>>;
  exec: ReturnType<typeof vi.fn<AnyMockFn>>;
  prepare: ReturnType<typeof vi.fn<AnyMockFn>>;
}

interface KvStub {
  get: ReturnType<typeof vi.fn<AnyMockFn>>;
  put: ReturnType<typeof vi.fn<AnyMockFn>>;
}

interface CacheStub {
  match: ReturnType<typeof vi.fn<AnyMockFn>>;
  put: ReturnType<typeof vi.fn<AnyMockFn>>;
}

interface FeaturesStub {
  fetch: ReturnType<typeof vi.fn<AnyMockFn>>;
}

interface BuildContextArgs {
  cache?: CacheStub | null;
  db?: D1Stub;
  features?: FeaturesStub;
  hotDb?: D1Stub;
  kv?: KvStub;
}

const SAMPLE_RAW_ROW = {
  source: "nar",
  raceKey: "nar:20260528:50:04",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0528",
  keibajoCode: "50",
  raceBango: "04",
  raceName: "Test Race",
  hassoJikoku: "2026-05-28T16:30:00+09:00",
  wakuban: "3",
  umaban: "05",
  bamei: "TestHorse",
  jockeyName: "TestJockey",
  tanshoOddsTenth: 123,
  tanshoPopularity: 4,
  finishPosition: 1,
  sohaTime: "1234",
  bataijuInt: 480,
  zogenFugo: "+",
  zogenSaInt: 2,
};

const buildPreparedStub = (rows: unknown[]): PreparedStub => {
  const all = vi.fn<AnyMockFn>().mockResolvedValue({ results: rows });
  const bind = vi.fn<AnyMockFn>().mockReturnValue({ all });
  return { all, bind };
};

const buildD1Stub = (rows: unknown[]): { db: D1Stub; prepared: PreparedStub } => {
  const prepared = buildPreparedStub(rows);
  const db: D1Stub = {
    batch: vi.fn<AnyMockFn>().mockResolvedValue([]),
    exec: vi.fn<AnyMockFn>().mockResolvedValue({ success: true }),
    prepare: vi.fn<AnyMockFn>().mockReturnValue(prepared),
  };
  return { db, prepared };
};

const buildKvStub = (initial?: string | null): KvStub => ({
  get: vi.fn<AnyMockFn>().mockResolvedValue(initial),
  put: vi.fn<AnyMockFn>().mockResolvedValue(undefined),
});

const buildCacheStub = (match?: Response): CacheStub => ({
  match: vi.fn<AnyMockFn>().mockResolvedValue(match),
  put: vi.fn<AnyMockFn>().mockResolvedValue(undefined),
});

const buildFeaturesStub = (response: Response): FeaturesStub => ({
  fetch: vi.fn<AnyMockFn>().mockResolvedValue(response),
});

const buildFeaturesJsonResponse = (payload: unknown): Response =>
  new Response(JSON.stringify(payload), {
    headers: { "Content-Type": "application/json" },
  });

interface BuildHotEnvArgs {
  hotDb: D1Stub | undefined;
}

const isPreparedStatement = (value: unknown): value is PcKeibaD1PreparedStatement =>
  typeof value === "object" &&
  value !== null &&
  "bind" in value &&
  typeof value.bind === "function";

const emptyBatch = <T = unknown>(): Promise<PcKeibaD1Result<T>[]> => Promise.resolve([]);
const noopExec = (): Promise<PcKeibaD1RunResult> => Promise.resolve({ success: true });

const buildHotEnv = ({ hotDb }: BuildHotEnvArgs): CloudflareEnv => {
  if (hotDb === undefined) return {};
  const typedPrepare = (query: string): PcKeibaD1PreparedStatement => {
    const result = Reflect.apply(hotDb.prepare, hotDb, [query]);
    if (!isPreparedStatement(result)) {
      throw new Error("Stub returned an invalid prepared statement");
    }
    return result;
  };
  const typed: PcKeibaD1Database = {
    prepare: typedPrepare,
    batch: emptyBatch,
    exec: noopExec,
  };
  return { REALTIME_HOT_DB: typed };
};

const installContext = ({ cache, db, features, hotDb, kv }: BuildContextArgs): void => {
  if (cache === null) {
    Reflect.deleteProperty(globalThis, "caches");
  } else if (cache !== undefined) {
    Object.defineProperty(globalThis, "caches", {
      configurable: true,
      value: { default: cache },
    });
  }
  getCloudflareContextMock.mockResolvedValue({
    env: {
      REALTIME_DB: db,
      REALTIME_FEATURES: features,
      REALTIME_HOT_DB: hotDb,
      DETAIL_SECTION_CACHE_KV: kv,
    },
  });
};

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

it("buildPast14WindowForTarget returns 14 day lookback ending the day before target", () => {
  expect(buildPast14WindowForTarget("20260520")).toStrictEqual({
    endYmd: "20260519",
    startYmd: "20260506",
  });
});

it("getRaceTrendTodayStarterRows binds source and targetYmd as a single-day window", async () => {
  const { db, prepared } = buildD1Stub([SAMPLE_RAW_ROW]);
  const cache = buildCacheStub();
  installContext({ cache, db, kv: buildKvStub() });
  const rows = await getRaceTrendTodayStarterRows({ source: "nar", targetYmd: "20260528" });
  expect(rows).toStrictEqual([
    {
      source: "nar",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0528",
      keibajoCode: "50",
      raceBango: "04",
      raceName: "Test Race",
      hassoJikoku: "1630",
      runnerCount: null,
      wakuban: null,
      umaban: "05",
      bamei: "TestHorse",
      jockeyName: "TestJockey",
      tanshoOdds: null,
      tanshoPopularity: null,
      finishPosition: 1,
      sohaTime: "1234",
      corner1: null,
      corner2: null,
      corner3: null,
      corner4: null,
      bataiju: "480",
      zogenFugo: "+",
      zogenSa: "2",
    },
  ]);
  expect(prepared.bind).toHaveBeenCalledWith("nar", "20260528", "20260528");
  expect(cache.put).toHaveBeenCalledTimes(1);
});

it("getRaceTrendTodayStarterRows writes Cache API entry under the race-trend-today:v8 key", async () => {
  const { db } = buildD1Stub([SAMPLE_RAW_ROW]);
  const cache = buildCacheStub();
  installContext({ cache, db, kv: buildKvStub() });
  await getRaceTrendTodayStarterRows({ source: "nar", targetYmd: "20260528" });
  const firstArg: unknown = cache.put.mock.calls[0]?.[0];
  if (!(firstArg instanceof Request)) {
    throw new Error("Cache.put first arg was not a Request");
  }
  expect(firstArg.url).toBe(
    "https://pc-keiba-viewer.local/d1-trend-today-cache/race-trend-today%3Av8%3Anar%3A20260528",
  );
});

it("getRaceTrendTodayStarterRows returns Cache API hit without hitting D1", async () => {
  const cached = [
    {
      source: "nar",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0528",
      keibajoCode: "50",
      raceBango: "03",
      raceName: null,
      hassoJikoku: null,
      runnerCount: null,
      wakuban: null,
      umaban: "01",
      bamei: null,
      jockeyName: null,
      tanshoOdds: null,
      tanshoPopularity: null,
      finishPosition: 3,
      sohaTime: null,
      corner1: null,
      corner2: null,
      corner3: null,
      corner4: null,
      bataiju: null,
      zogenFugo: null,
      zogenSa: null,
    },
  ];
  const cache = buildCacheStub(
    new Response(JSON.stringify(cached), { headers: { "Content-Type": "application/json" } }),
  );
  const { db } = buildD1Stub([SAMPLE_RAW_ROW]);
  installContext({ cache, db, kv: buildKvStub() });
  const rows = await getRaceTrendTodayStarterRows({ source: "nar", targetYmd: "20260528" });
  expect(rows).toStrictEqual(cached);
  expect(db.prepare).not.toHaveBeenCalled();
});

it("getRaceTrendTodayStarterRows does not read KV (Cache API only)", async () => {
  const cached = [
    {
      source: "jra",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0528",
      keibajoCode: "06",
      raceBango: "11",
      raceName: null,
      hassoJikoku: null,
      runnerCount: null,
      wakuban: null,
      umaban: "07",
      bamei: null,
      jockeyName: null,
      tanshoOdds: null,
      tanshoPopularity: null,
      finishPosition: 5,
      sohaTime: null,
      corner1: null,
      corner2: null,
      corner3: null,
      corner4: null,
      bataiju: null,
      zogenFugo: null,
      zogenSa: null,
    },
  ];
  const cache = buildCacheStub();
  const kv = buildKvStub(JSON.stringify(cached));
  const { db } = buildD1Stub([]);
  installContext({ cache, db, kv });
  const rows = await getRaceTrendTodayStarterRows({ source: "jra", targetYmd: "20260528" });
  expect(rows).toStrictEqual([]);
  expect(kv.get).not.toHaveBeenCalled();
  expect(kv.put).not.toHaveBeenCalled();
});

it("getRaceTrendTodayStarterRows returns [] when REALTIME_DB binding is missing", async () => {
  installContext({ cache: buildCacheStub(), db: undefined, kv: buildKvStub() });
  const rows = await getRaceTrendTodayStarterRows({ source: "nar", targetYmd: "20260528" });
  expect(rows).toStrictEqual([]);
});

it("getRaceTrendTodayStarterRows returns [] when D1 query throws", async () => {
  const failing: D1Stub = {
    batch: vi.fn<AnyMockFn>().mockResolvedValue([]),
    exec: vi.fn<AnyMockFn>().mockResolvedValue({ success: true }),
    prepare: vi.fn<AnyMockFn>().mockReturnValue({
      bind: vi.fn<AnyMockFn>().mockReturnValue({
        all: vi.fn<AnyMockFn>().mockRejectedValue(new Error("boom")),
      }),
    }),
  };
  installContext({ cache: buildCacheStub(), db: failing, kv: buildKvStub() });
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  const rows = await getRaceTrendTodayStarterRows({ source: "nar", targetYmd: "20260528" });
  expect(rows).toStrictEqual([]);
  consoleSpy.mockRestore();
});

it("getRaceTrendTodayStarterRows filters out invalid D1 rows", async () => {
  const invalid = { source: "nar", finishPosition: "1" };
  const { db } = buildD1Stub([invalid, SAMPLE_RAW_ROW]);
  installContext({ cache: buildCacheStub(), db, kv: buildKvStub() });
  const rows = await getRaceTrendTodayStarterRows({ source: "nar", targetYmd: "20260528" });
  expect(rows.length).toBe(1);
  expect(rows[0]?.bamei).toBe("TestHorse");
});

it("getRaceTrendTodayStarterRows handles null hasso / bataiju / zogen fields", async () => {
  const sparse = {
    ...SAMPLE_RAW_ROW,
    hassoJikoku: null,
    bataijuInt: null,
    zogenSaInt: null,
    tanshoOddsTenth: null,
    tanshoPopularity: null,
  };
  const { db } = buildD1Stub([sparse]);
  installContext({ cache: buildCacheStub(), db, kv: buildKvStub() });
  const rows = await getRaceTrendTodayStarterRows({ source: "nar", targetYmd: "20260528" });
  expect(rows[0]?.hassoJikoku).toBe(null);
  expect(rows[0]?.bataiju).toBe(null);
  expect(rows[0]?.zogenSa).toBe(null);
  expect(rows[0]?.tanshoOdds).toBe(null);
  expect(rows[0]?.tanshoPopularity).toBe(null);
});

it("getRaceTrendTodayStarterRows treats short hassoJikoku as null", async () => {
  const tooShort = { ...SAMPLE_RAW_ROW, hassoJikoku: "2026-05-28" };
  const { db } = buildD1Stub([tooShort]);
  installContext({ cache: buildCacheStub(), db, kv: buildKvStub() });
  const rows = await getRaceTrendTodayStarterRows({ source: "nar", targetYmd: "20260528" });
  expect(rows[0]?.hassoJikoku).toBe(null);
});

it("getRaceTrendTodayStarterRows returns [] without caching when global caches is unavailable", async () => {
  const { db } = buildD1Stub([SAMPLE_RAW_ROW]);
  installContext({ cache: null, db, kv: buildKvStub() });
  const rows = await getRaceTrendTodayStarterRows({ source: "nar", targetYmd: "20260528" });
  expect(rows.length).toBe(1);
});

it("getRaceTrendTodayStarterRows skips REALTIME_HOT_DB lookup when starter rows are empty", async () => {
  const { db } = buildD1Stub([]);
  const { db: hotDb } = buildD1Stub([]);
  installContext({ cache: buildCacheStub(), db, hotDb, kv: buildKvStub() });
  const rows = await getRaceTrendTodayStarterRows({ source: "nar", targetYmd: "20260528" });
  expect(rows).toStrictEqual([]);
  expect(hotDb.prepare).not.toHaveBeenCalled();
});

it("getRaceTrendTodayStarterRows overrides tansho odds when REALTIME_HOT_DB returns a match", async () => {
  const sparseFallback = { ...SAMPLE_RAW_ROW, tanshoOddsTenth: null, tanshoPopularity: null };
  const { db } = buildD1Stub([sparseFallback]);
  const { db: hotDb } = buildD1Stub([
    { race_key: "nar:20260528:50:04", combination: "5", odds: 8.4, rank: 3 },
  ]);
  installContext({ cache: buildCacheStub(), db, hotDb, kv: buildKvStub() });
  const rows = await getRaceTrendTodayStarterRows({ source: "nar", targetYmd: "20260528" });
  expect(rows[0]?.tanshoOdds).toBe("0084");
  expect(rows[0]?.tanshoPopularity).toBe("03");
});

it("getRaceTrendTodayStarterRows leaves tansho fields null when REALTIME_HOT_DB has no entry", async () => {
  const { db } = buildD1Stub([SAMPLE_RAW_ROW]);
  const { db: hotDb } = buildD1Stub([]);
  installContext({ cache: buildCacheStub(), db, hotDb, kv: buildKvStub() });
  const rows = await getRaceTrendTodayStarterRows({ source: "nar", targetYmd: "20260528" });
  expect(rows[0]?.tanshoOdds).toBe(null);
  expect(rows[0]?.tanshoPopularity).toBe(null);
});

it("getRaceTrendTodayStarterRows leaves tansho fields null when REALTIME_HOT_DB binding is missing", async () => {
  const { db } = buildD1Stub([SAMPLE_RAW_ROW]);
  installContext({ cache: buildCacheStub(), db, hotDb: undefined, kv: buildKvStub() });
  const rows = await getRaceTrendTodayStarterRows({ source: "nar", targetYmd: "20260528" });
  expect(rows[0]?.tanshoOdds).toBe(null);
  expect(rows[0]?.tanshoPopularity).toBe(null);
});

it("getRaceTrendTodayStarterRows uses HOT odds and null rank when HOT rank is null", async () => {
  const { db } = buildD1Stub([SAMPLE_RAW_ROW]);
  const { db: hotDb } = buildD1Stub([
    { race_key: "nar:20260528:50:04", combination: "5", odds: 9.9, rank: null },
  ]);
  installContext({ cache: buildCacheStub(), db, hotDb, kv: buildKvStub() });
  const rows = await getRaceTrendTodayStarterRows({ source: "nar", targetYmd: "20260528" });
  expect(rows[0]?.tanshoOdds).toBe("0099");
  expect(rows[0]?.tanshoPopularity).toBe(null);
});

it("getRaceTrendTodayStarterRows uses HOT rank and null odds when HOT odds is null", async () => {
  const { db } = buildD1Stub([SAMPLE_RAW_ROW]);
  const { db: hotDb } = buildD1Stub([
    { race_key: "nar:20260528:50:04", combination: "5", odds: null, rank: 2 },
  ]);
  installContext({ cache: buildCacheStub(), db, hotDb, kv: buildKvStub() });
  const rows = await getRaceTrendTodayStarterRows({ source: "nar", targetYmd: "20260528" });
  expect(rows[0]?.tanshoOdds).toBe(null);
  expect(rows[0]?.tanshoPopularity).toBe("02");
});

it("getRaceTrendTodayStarterRows leaves tansho fields null when umaban is null and HOT odds unavailable", async () => {
  const noUmaban = {
    ...SAMPLE_RAW_ROW,
    umaban: null,
    tanshoOddsTenth: null,
    tanshoPopularity: null,
  };
  const { db } = buildD1Stub([noUmaban]);
  const { db: hotDb } = buildD1Stub([
    { race_key: "nar:20260528:50:04", combination: "5", odds: 8.4, rank: 3 },
  ]);
  installContext({ cache: buildCacheStub(), db, hotDb, kv: buildKvStub() });
  const rows = await getRaceTrendTodayStarterRows({ source: "nar", targetYmd: "20260528" });
  expect(rows[0]?.tanshoOdds).toBe(null);
  expect(rows[0]?.tanshoPopularity).toBe(null);
});

it("getRaceTrendPast14StarterRows sends source/keibajoCode/raceBango/from/to to the features worker", async () => {
  const payloadRow = {
    source: "jra",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0520",
    keibajoCode: "06",
    raceBango: "11",
    raceName: "Past Race",
    hassoJikoku: "1530",
    runnerCount: "16",
    wakuban: "8",
    umaban: "12",
    bamei: "PastHorse",
    jockeyName: "PastJockey",
    tanshoOdds: "0056",
    tanshoPopularity: "03",
    finishPosition: 2,
    sohaTime: "950",
    corner1: "04",
    corner2: "05",
    corner3: "03",
    corner4: "02",
    bataiju: "466",
    zogenFugo: "-",
    zogenSa: "4",
  };
  const features = buildFeaturesStub(buildFeaturesJsonResponse({ starterRows: [payloadRow] }));
  installContext({ cache: buildCacheStub(), features, kv: buildKvStub() });
  const rows = await getRaceTrendPast14StarterRows({
    endYmd: "20260527",
    keibajoCode: "06",
    raceBango: "11",
    source: "jra",
    startYmd: "20260514",
  });
  expect(rows).toStrictEqual([payloadRow]);
  const firstArg: unknown = features.fetch.mock.calls[0]?.[0];
  if (typeof firstArg !== "string") {
    throw new Error("features worker first arg was not a string URL");
  }
  expect(firstArg).toBe(
    "https://sync-realtime-data-features.kkk4oru.com/api/features/race-trend?source=jra&keibajoCode=06&raceBango=11&from=20260514&to=20260527",
  );
});

it("getRaceTrendPast14StarterRows writes KV with the race-trend-past14:v8 key", async () => {
  const payloadRow = {
    source: "nar",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0518",
    keibajoCode: "50",
    raceBango: "07",
    raceName: null,
    hassoJikoku: null,
    runnerCount: null,
    wakuban: null,
    umaban: "03",
    bamei: "Past14Horse",
    jockeyName: null,
    tanshoOdds: null,
    tanshoPopularity: null,
    finishPosition: 4,
    sohaTime: null,
    corner1: null,
    corner2: null,
    corner3: null,
    corner4: null,
    bataiju: null,
    zogenFugo: null,
    zogenSa: null,
  };
  const features = buildFeaturesStub(buildFeaturesJsonResponse({ starterRows: [payloadRow] }));
  const kv = buildKvStub();
  installContext({ cache: buildCacheStub(), features, kv });
  await getRaceTrendPast14StarterRows({
    endYmd: "20260527",
    keibajoCode: "50",
    raceBango: "07",
    source: "nar",
    startYmd: "20260514",
  });
  expect(kv.put).toHaveBeenCalledTimes(1);
  const putArgs = kv.put.mock.calls[0];
  expect(putArgs?.[0]).toBe("race-trend-past14:v8:nar:50:07:20260514:20260527");
  expect(putArgs?.[2]).toStrictEqual({ expirationTtl: 1800 });
});

it("getRaceTrendPast14StarterRows returns Cache API hit without calling the features worker", async () => {
  const cached = [
    {
      source: "jra",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0501",
      keibajoCode: "06",
      raceBango: "10",
      raceName: null,
      hassoJikoku: null,
      runnerCount: null,
      wakuban: null,
      umaban: "01",
      bamei: null,
      jockeyName: null,
      tanshoOdds: null,
      tanshoPopularity: null,
      finishPosition: 1,
      sohaTime: null,
      corner1: null,
      corner2: null,
      corner3: null,
      corner4: null,
      bataiju: null,
      zogenFugo: null,
      zogenSa: null,
    },
  ];
  const cache = buildCacheStub(
    new Response(JSON.stringify(cached), { headers: { "Content-Type": "application/json" } }),
  );
  const features = buildFeaturesStub(buildFeaturesJsonResponse({ starterRows: [] }));
  installContext({ cache, features, kv: buildKvStub() });
  const rows = await getRaceTrendPast14StarterRows({
    endYmd: "20260527",
    keibajoCode: "06",
    raceBango: "10",
    source: "jra",
    startYmd: "20260514",
  });
  expect(rows).toStrictEqual(cached);
  expect(features.fetch).not.toHaveBeenCalled();
});

it("getRaceTrendPast14StarterRows falls back to KV when Cache API misses", async () => {
  const cached = [
    {
      source: "nar",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0501",
      keibajoCode: "50",
      raceBango: "02",
      raceName: null,
      hassoJikoku: null,
      runnerCount: null,
      wakuban: null,
      umaban: "02",
      bamei: null,
      jockeyName: null,
      tanshoOdds: null,
      tanshoPopularity: null,
      finishPosition: 4,
      sohaTime: null,
      corner1: null,
      corner2: null,
      corner3: null,
      corner4: null,
      bataiju: null,
      zogenFugo: null,
      zogenSa: null,
    },
  ];
  const cache = buildCacheStub();
  const kv = buildKvStub(JSON.stringify(cached));
  const features = buildFeaturesStub(buildFeaturesJsonResponse({ starterRows: [] }));
  installContext({ cache, features, kv });
  const rows = await getRaceTrendPast14StarterRows({
    endYmd: "20260527",
    keibajoCode: "50",
    raceBango: "02",
    source: "nar",
    startYmd: "20260514",
  });
  expect(rows).toStrictEqual(cached);
  expect(features.fetch).not.toHaveBeenCalled();
});

it("getRaceTrendPast14StarterRows returns [] when REALTIME_FEATURES binding is missing", async () => {
  installContext({ cache: buildCacheStub(), features: undefined, kv: buildKvStub() });
  const rows = await getRaceTrendPast14StarterRows({
    endYmd: "20260527",
    keibajoCode: "06",
    raceBango: "11",
    source: "jra",
    startYmd: "20260514",
  });
  expect(rows).toStrictEqual([]);
});

it("getRaceTrendPast14StarterRows returns [] when features worker throws", async () => {
  const features: FeaturesStub = {
    fetch: vi.fn<AnyMockFn>().mockRejectedValue(new Error("net error")),
  };
  installContext({ cache: buildCacheStub(), features, kv: buildKvStub() });
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  const rows = await getRaceTrendPast14StarterRows({
    endYmd: "20260527",
    keibajoCode: "06",
    raceBango: "11",
    source: "jra",
    startYmd: "20260514",
  });
  expect(rows).toStrictEqual([]);
  consoleSpy.mockRestore();
});

it("getRaceTrendPast14StarterRows returns [] when features worker returns non-ok response", async () => {
  const features = buildFeaturesStub(new Response("error", { status: 500 }));
  installContext({ cache: buildCacheStub(), features, kv: buildKvStub() });
  const rows = await getRaceTrendPast14StarterRows({
    endYmd: "20260527",
    keibajoCode: "06",
    raceBango: "11",
    source: "jra",
    startYmd: "20260514",
  });
  expect(rows).toStrictEqual([]);
});

it("getRaceTrendPast14StarterRows filters out invalid daily rows from the features worker payload", async () => {
  const invalid = { source: "jra", finishPosition: null };
  const validRow = {
    source: "jra",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0518",
    keibajoCode: "06",
    raceBango: "11",
    raceName: null,
    hassoJikoku: null,
    runnerCount: null,
    wakuban: null,
    umaban: null,
    bamei: "ValidHorse",
    jockeyName: null,
    tanshoOdds: null,
    tanshoPopularity: null,
    finishPosition: 2,
    sohaTime: null,
    corner1: null,
    corner2: null,
    corner3: null,
    corner4: null,
    bataiju: null,
    zogenFugo: null,
    zogenSa: null,
  };
  const features = buildFeaturesStub(
    buildFeaturesJsonResponse({ starterRows: [invalid, validRow] }),
  );
  installContext({ cache: buildCacheStub(), features, kv: buildKvStub() });
  const rows = await getRaceTrendPast14StarterRows({
    endYmd: "20260527",
    keibajoCode: "06",
    raceBango: "11",
    source: "jra",
    startYmd: "20260514",
  });
  expect(rows.length).toBe(1);
  expect(rows[0]?.bamei).toBe("ValidHorse");
});

it("getRaceTrendPast14StarterRows returns [] when payload is not an object", async () => {
  const features = buildFeaturesStub(buildFeaturesJsonResponse(null));
  installContext({ cache: buildCacheStub(), features, kv: buildKvStub() });
  const rows = await getRaceTrendPast14StarterRows({
    endYmd: "20260527",
    keibajoCode: "06",
    raceBango: "11",
    source: "jra",
    startYmd: "20260514",
  });
  expect(rows).toStrictEqual([]);
});

it("getRaceTrendPast14StarterRows returns [] when starterRows field is missing from payload", async () => {
  const features = buildFeaturesStub(buildFeaturesJsonResponse({ raceCount: 0 }));
  installContext({ cache: buildCacheStub(), features, kv: buildKvStub() });
  const rows = await getRaceTrendPast14StarterRows({
    endYmd: "20260527",
    keibajoCode: "06",
    raceBango: "11",
    source: "jra",
    startYmd: "20260514",
  });
  expect(rows).toStrictEqual([]);
});

it("getRaceTrendPast14StarterRows does not write KV when worker returns empty list", async () => {
  const features = buildFeaturesStub(buildFeaturesJsonResponse({ starterRows: [] }));
  const kv = buildKvStub();
  installContext({ cache: buildCacheStub(), features, kv });
  await getRaceTrendPast14StarterRows({
    endYmd: "20260527",
    keibajoCode: "06",
    raceBango: "11",
    source: "jra",
    startYmd: "20260514",
  });
  expect(kv.put).not.toHaveBeenCalled();
});

it("getRaceTrendRunningStylesFromD1 returns empty array when no race keys are supplied", async () => {
  const { db } = buildD1Stub([]);
  installContext({ cache: null, db, kv: buildKvStub() });
  const rows = await getRaceTrendRunningStylesFromD1([]);
  expect(rows).toStrictEqual([]);
  expect(db.prepare).not.toHaveBeenCalled();
});

it("getRaceTrendRunningStylesFromD1 deduplicates and filters blank race keys before binding", async () => {
  const { db, prepared } = buildD1Stub([
    { race_key: "nar:20260528:50:01", horse_number: 1, predicted_label: "nige" },
  ]);
  installContext({ cache: null, db, kv: buildKvStub() });
  await getRaceTrendRunningStylesFromD1([
    "nar:20260528:50:01",
    "nar:20260528:50:01",
    "",
    "nar:20260528:50:02",
  ]);
  expect(db.prepare).toHaveBeenCalledTimes(1);
  expect(prepared.bind).toHaveBeenCalledWith("nar:20260528:50:01", "nar:20260528:50:02");
});

it("getRaceTrendRunningStylesFromD1 batches IN clause at 200 race keys per chunk", async () => {
  const { db, prepared } = buildD1Stub([]);
  installContext({ cache: null, db, kv: buildKvStub() });
  const keys = Array.from(
    { length: 450 },
    (_, index) => `nar:20260528:50:${String(index).padStart(2, "0")}`,
  );
  await getRaceTrendRunningStylesFromD1(keys);
  expect(db.prepare).toHaveBeenCalledTimes(3);
  expect(prepared.bind).toHaveBeenCalledTimes(3);
});

it("getRaceTrendRunningStylesFromD1 maps result rows into the public cache shape", async () => {
  const { db } = buildD1Stub([
    { race_key: "nar:20260528:50:01", horse_number: 7, predicted_label: "senkou" },
    { race_key: "nar:20260528:50:01", horse_number: 9, predicted_label: "sashi" },
    { race_key: "nar:20260528:50:01", horse_number: 13, predicted_label: "oikomi" },
  ]);
  installContext({ cache: null, db, kv: buildKvStub() });
  const rows = await getRaceTrendRunningStylesFromD1(["nar:20260528:50:01"]);
  expect(rows).toStrictEqual([
    { raceKey: "nar:20260528:50:01", horseNumber: "7", predictedLabel: "senkou" },
    { raceKey: "nar:20260528:50:01", horseNumber: "9", predictedLabel: "sashi" },
    { raceKey: "nar:20260528:50:01", horseNumber: "13", predictedLabel: "oikomi" },
  ]);
});

it("getRaceTrendRunningStylesFromD1 ignores rows that fail validation", async () => {
  const { db } = buildD1Stub([
    { race_key: "nar:20260528:50:01", horse_number: 1, predicted_label: "nige" },
    { race_key: "nar:20260528:50:01", horse_number: 2, predicted_label: "invalid" },
    { race_key: "nar:20260528:50:01", horse_number: "not-a-number", predicted_label: "sashi" },
  ]);
  installContext({ cache: null, db, kv: buildKvStub() });
  const rows = await getRaceTrendRunningStylesFromD1(["nar:20260528:50:01"]);
  expect(rows).toStrictEqual([
    { raceKey: "nar:20260528:50:01", horseNumber: "1", predictedLabel: "nige" },
  ]);
});

it("getRaceTrendRunningStylesFromD1 returns empty array when REALTIME_DB binding is missing", async () => {
  getCloudflareContextMock.mockResolvedValue({ env: {} });
  const rows = await getRaceTrendRunningStylesFromD1(["nar:20260528:50:01"]);
  expect(rows).toStrictEqual([]);
});

it("getRaceTrendRunningStylesFromD1 swallows D1 errors and returns empty array", async () => {
  const all = vi.fn<AnyMockFn>().mockRejectedValue(new Error("D1 overloaded"));
  const bind = vi.fn<AnyMockFn>().mockReturnValue({ all });
  const prepared = { all, bind };
  const db: D1Stub = {
    batch: vi.fn<AnyMockFn>().mockResolvedValue([]),
    exec: vi.fn<AnyMockFn>().mockResolvedValue({ success: true }),
    prepare: vi.fn<AnyMockFn>().mockReturnValue(prepared),
  };
  installContext({ cache: null, db, kv: buildKvStub() });
  const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const rows = await getRaceTrendRunningStylesFromD1(["nar:20260528:50:01"]);
  expect(rows).toStrictEqual([]);
  expect(consoleError).toHaveBeenCalledWith(
    "D1 race_running_styles query failed",
    expect.any(Error),
  );
  consoleError.mockRestore();
});

it("getRaceTrendRunningStylesFromD1 limits in-flight queries to 3 across many chunks", async () => {
  const inFlightState = { current: 0, peak: 0 };
  const all = vi.fn<AnyMockFn>().mockImplementation(async () => {
    inFlightState.current += 1;
    if (inFlightState.current > inFlightState.peak) {
      inFlightState.peak = inFlightState.current;
    }
    await Promise.resolve();
    await Promise.resolve();
    inFlightState.current -= 1;
    return { results: [] };
  });
  const bind = vi.fn<AnyMockFn>().mockReturnValue({ all });
  const prepared = { all, bind };
  const db: D1Stub = {
    batch: vi.fn<AnyMockFn>().mockResolvedValue([]),
    exec: vi.fn<AnyMockFn>().mockResolvedValue({ success: true }),
    prepare: vi.fn<AnyMockFn>().mockReturnValue(prepared),
  };
  installContext({ cache: null, db, kv: buildKvStub() });
  const keys = Array.from(
    { length: 2000 },
    (_, index) => `nar:20260528:50:${String(index).padStart(4, "0")}`,
  );
  await getRaceTrendRunningStylesFromD1(keys);
  expect(db.prepare).toHaveBeenCalledTimes(10);
  expect(inFlightState.peak).toBeLessThanOrEqual(3);
});

it("getRaceTrendRunningStylesFromD1 writes non-empty results to KV with the prefixed cache key", async () => {
  const { db } = buildD1Stub([
    { race_key: "nar:20260524:47:01", horse_number: 1, predicted_label: "nige" },
    { race_key: "nar:20260524:47:01", horse_number: 2, predicted_label: "sashi" },
  ]);
  const kv = buildKvStub();
  installContext({ cache: null, db, kv });
  await getRaceTrendRunningStylesFromD1(["nar:20260524:47:01"]);
  expect(kv.put).toHaveBeenCalledTimes(1);
  const putArgs = kv.put.mock.calls[0];
  expect(putArgs?.[0]).toBe(
    "race-trend-running-styles:v1:1:4d3816e7cc1b4d37d604e96941b368ef063ba677",
  );
  expect(JSON.parse(String(putArgs?.[1]))).toStrictEqual([
    { raceKey: "nar:20260524:47:01", horseNumber: "1", predictedLabel: "nige" },
    { raceKey: "nar:20260524:47:01", horseNumber: "2", predictedLabel: "sashi" },
  ]);
  expect(putArgs?.[2]).toStrictEqual({ expirationTtl: 1800 });
});

it("getRaceTrendRunningStylesFromD1 does not write to KV when the D1 result is empty", async () => {
  const { db } = buildD1Stub([]);
  const kv = buildKvStub();
  installContext({ cache: null, db, kv });
  const rows = await getRaceTrendRunningStylesFromD1(["nar:20260524:47:01"]);
  expect(rows).toStrictEqual([]);
  expect(kv.put).not.toHaveBeenCalled();
});

it("getRaceTrendRunningStylesFromD1 short-circuits to KV without hitting D1 when cache hit", async () => {
  const cached = [
    { raceKey: "nar:20260524:47:01", horseNumber: "5", predictedLabel: "oikomi" as const },
  ];
  const { db } = buildD1Stub([]);
  const kv = buildKvStub(JSON.stringify(cached));
  installContext({ cache: null, db, kv });
  const rows = await getRaceTrendRunningStylesFromD1(["nar:20260524:47:01"]);
  expect(rows).toStrictEqual(cached);
  expect(db.prepare).not.toHaveBeenCalled();
  expect(kv.put).not.toHaveBeenCalled();
});

it("getRaceTrendRunningStylesFromD1 falls through to D1 when KV body is corrupt", async () => {
  const { db, prepared } = buildD1Stub([
    { race_key: "nar:20260524:47:01", horse_number: 8, predicted_label: "senkou" },
  ]);
  const kv = buildKvStub("{not-valid-json");
  installContext({ cache: null, db, kv });
  const rows = await getRaceTrendRunningStylesFromD1(["nar:20260524:47:01"]);
  expect(rows).toStrictEqual([
    { raceKey: "nar:20260524:47:01", horseNumber: "8", predictedLabel: "senkou" },
  ]);
  expect(prepared.bind).toHaveBeenCalledTimes(1);
});

it("getRaceTrendRunningStylesFromD1 sorts race keys before building the cache key", async () => {
  const { db } = buildD1Stub([
    { race_key: "nar:20260524:42:03", horse_number: 1, predicted_label: "nige" },
  ]);
  const kv = buildKvStub();
  installContext({ cache: null, db, kv });
  await getRaceTrendRunningStylesFromD1([
    "nar:20260524:47:01",
    "nar:20260524:42:03",
    "nar:20260524:30:05",
  ]);
  const cacheKey = kv.put.mock.calls[0]?.[0];
  expect(cacheKey).toBe("race-trend-running-styles:v1:3:3438aed254b1038f225e6c8738bb7272bcc0f086");
});

it("getRaceTrendTodayRunningStylesFromD1 returns empty array when no race keys are supplied", async () => {
  const { db } = buildD1Stub([]);
  installContext({ cache: null, db, kv: buildKvStub() });
  const rows = await getRaceTrendTodayRunningStylesFromD1([]);
  expect(rows).toStrictEqual([]);
  expect(db.prepare).not.toHaveBeenCalled();
});

it("getRaceTrendTodayRunningStylesFromD1 returns empty array when REALTIME_FEATURES_DB binding is missing", async () => {
  getCloudflareContextMock.mockResolvedValue({ env: {} });
  const rows = await getRaceTrendTodayRunningStylesFromD1(["nar:20260528:50:01"]);
  expect(rows).toStrictEqual([]);
});

it("getRaceTrendTodayRunningStylesFromD1 skips KV entirely (no read, no write)", async () => {
  const { db } = buildD1Stub([
    { race_key: "nar:20260528:50:01", horse_number: 1, predicted_label: "nige" },
  ]);
  const kv = buildKvStub("ignored-cache-value");
  installContext({ cache: null, db, kv });
  const rows = await getRaceTrendTodayRunningStylesFromD1(["nar:20260528:50:01"]);
  expect(rows).toStrictEqual([
    { raceKey: "nar:20260528:50:01", horseNumber: "1", predictedLabel: "nige" },
  ]);
  expect(kv.get).not.toHaveBeenCalled();
  expect(kv.put).not.toHaveBeenCalled();
});

it("getRaceTrendTodayRunningStylesFromD1 swallows D1 errors and returns empty array", async () => {
  const all = vi.fn<AnyMockFn>().mockRejectedValue(new Error("today D1 boom"));
  const bind = vi.fn<AnyMockFn>().mockReturnValue({ all });
  const db: D1Stub = {
    batch: vi.fn<AnyMockFn>().mockResolvedValue([]),
    exec: vi.fn<AnyMockFn>().mockResolvedValue({ success: true }),
    prepare: vi.fn<AnyMockFn>().mockReturnValue({ all, bind }),
  };
  installContext({ cache: null, db, kv: buildKvStub() });
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  const rows = await getRaceTrendTodayRunningStylesFromD1(["nar:20260528:50:01"]);
  expect(rows).toStrictEqual([]);
  expect(consoleSpy).toHaveBeenCalledWith(
    "D1 race_running_styles today query failed",
    expect.any(Error),
  );
  consoleSpy.mockRestore();
});

it("getLatestTanshoOddsFromHotD1 returns empty map when raceKeys is empty", async () => {
  const { db: hotDb } = buildD1Stub([]);
  const result = await getLatestTanshoOddsFromHotD1({
    env: buildHotEnv({ hotDb }),
    raceKeys: [],
  });
  expect(result.size).toBe(0);
  expect(hotDb.prepare).not.toHaveBeenCalled();
});

it("getLatestTanshoOddsFromHotD1 returns empty map when REALTIME_HOT_DB binding is missing", async () => {
  const result = await getLatestTanshoOddsFromHotD1({
    env: buildHotEnv({ hotDb: undefined }),
    raceKeys: ["nar:20260528:50:04"],
  });
  expect(result.size).toBe(0);
});

it("getLatestTanshoOddsFromHotD1 returns empty map when env is null", async () => {
  const result = await getLatestTanshoOddsFromHotD1({
    env: null,
    raceKeys: ["nar:20260528:50:04"],
  });
  expect(result.size).toBe(0);
});

it("getLatestTanshoOddsFromHotD1 deduplicates and filters blank race keys before binding", async () => {
  const { db: hotDb, prepared } = buildD1Stub([]);
  await getLatestTanshoOddsFromHotD1({
    env: buildHotEnv({ hotDb }),
    raceKeys: ["nar:20260528:50:04", "nar:20260528:50:04", "", "nar:20260528:50:05"],
  });
  expect(prepared.bind).toHaveBeenCalledWith("nar:20260528:50:04", "nar:20260528:50:05");
});

it("getLatestTanshoOddsFromHotD1 groups rows by race_key and normalizes combination", async () => {
  const { db: hotDb } = buildD1Stub([
    { race_key: "nar:20260528:50:04", combination: "05", odds: 12.3, rank: 4 },
    { race_key: "nar:20260528:50:04", combination: "7", odds: 8.1, rank: 2 },
    { race_key: "nar:20260528:50:05", combination: "1", odds: 2.5, rank: 1 },
  ]);
  const result = await getLatestTanshoOddsFromHotD1({
    env: buildHotEnv({ hotDb }),
    raceKeys: ["nar:20260528:50:04", "nar:20260528:50:05"],
  });
  expect(result.get("nar:20260528:50:04")?.get("5")).toStrictEqual({ odds: 12.3, rank: 4 });
  expect(result.get("nar:20260528:50:04")?.get("7")).toStrictEqual({ odds: 8.1, rank: 2 });
  expect(result.get("nar:20260528:50:05")?.get("1")).toStrictEqual({ odds: 2.5, rank: 1 });
});

it("getLatestTanshoOddsFromHotD1 returns empty map when D1 throws", async () => {
  const failing: D1Stub = {
    batch: vi.fn<AnyMockFn>().mockResolvedValue([]),
    exec: vi.fn<AnyMockFn>().mockResolvedValue({ success: true }),
    prepare: vi.fn<AnyMockFn>().mockReturnValue({
      bind: vi.fn<AnyMockFn>().mockReturnValue({
        all: vi.fn<AnyMockFn>().mockRejectedValue(new Error("hot boom")),
      }),
    }),
  };
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  const result = await getLatestTanshoOddsFromHotD1({
    env: buildHotEnv({ hotDb: failing }),
    raceKeys: ["nar:20260528:50:04"],
  });
  expect(result.size).toBe(0);
  expect(consoleSpy).toHaveBeenCalledWith("D1 hot tansho odds query failed", expect.any(Error));
  consoleSpy.mockRestore();
});

it("getLatestTanshoOddsFromHotD1 skips rows that fail validation", async () => {
  const { db: hotDb } = buildD1Stub([
    { race_key: "nar:20260528:50:04", combination: "1", odds: 3.4, rank: 1 },
    { race_key: 123, combination: "2", odds: 5.5, rank: 2 },
    { race_key: "nar:20260528:50:04", combination: "", odds: 9.9, rank: 9 },
    { race_key: "nar:20260528:50:04", combination: "abc", odds: 9.9, rank: 9 },
  ]);
  const result = await getLatestTanshoOddsFromHotD1({
    env: buildHotEnv({ hotDb }),
    raceKeys: ["nar:20260528:50:04"],
  });
  expect(result.size).toBe(1);
  expect(result.get("nar:20260528:50:04")?.size).toBe(1);
  expect(result.get("nar:20260528:50:04")?.get("1")).toStrictEqual({ odds: 3.4, rank: 1 });
});

it("getLatestTanshoOddsFromHotD1 accepts null odds and rank", async () => {
  const { db: hotDb } = buildD1Stub([
    { race_key: "nar:20260528:50:04", combination: "1", odds: null, rank: null },
  ]);
  const result = await getLatestTanshoOddsFromHotD1({
    env: buildHotEnv({ hotDb }),
    raceKeys: ["nar:20260528:50:04"],
  });
  expect(result.get("nar:20260528:50:04")?.get("1")).toStrictEqual({ odds: null, rank: null });
});

it("getRaceTrendRunningStylesFromD1 does not throw with 300 race keys (regression: KV key length limit)", async () => {
  const keys = Array.from(
    { length: 300 },
    (_, index) =>
      `nar:2026:0515:${String(50 + (index % 5))}:${String((index % 12) + 1).padStart(2, "0")}`,
  );
  const { db } = buildD1Stub([]);
  const kv = buildKvStub();
  installContext({ cache: null, db, kv });
  const rows = await getRaceTrendRunningStylesFromD1(keys);
  expect(Array.isArray(rows)).toBe(true);
  const cacheKeyArg: unknown = kv.get.mock.calls[0]?.[0];
  if (typeof cacheKeyArg !== "string") {
    throw new Error("kv.get first arg was not a string");
  }
  const utf8Length = new TextEncoder().encode(cacheKeyArg).length;
  expect(utf8Length <= 512).toBe(true);
});

it("getRaceTrendRunningStylesFromD1 cache key is a fixed prefix + 40 hex char hash", async () => {
  const { db } = buildD1Stub([]);
  const kv = buildKvStub();
  installContext({ cache: null, db, kv });
  await getRaceTrendRunningStylesFromD1(["nar:20260524:47:01"]);
  const cacheKeyArg: unknown = kv.get.mock.calls[0]?.[0];
  expect(cacheKeyArg).toBe(
    "race-trend-running-styles:v1:1:4d3816e7cc1b4d37d604e96941b368ef063ba677",
  );
});

it("getRaceTrendRunningStylesFromD1 falls back to D1 when KV get throws (regression: KV 414 error)", async () => {
  const { db } = buildD1Stub([
    { race_key: "nar:20260524:47:01", horse_number: 3, predicted_label: "nige" },
  ]);
  const kv: KvStub = {
    get: vi.fn<AnyMockFn>().mockRejectedValue(new Error("KV GET failed: 414")),
    put: vi.fn<AnyMockFn>().mockResolvedValue(undefined),
  };
  installContext({ cache: null, db, kv });
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const rows = await getRaceTrendRunningStylesFromD1(["nar:20260524:47:01"]);
  expect(rows).toStrictEqual([
    { raceKey: "nar:20260524:47:01", horseNumber: "3", predictedLabel: "nige" },
  ]);
  expect(consoleSpy).toHaveBeenCalledWith("KV get for running-styles failed", expect.any(Error));
  consoleSpy.mockRestore();
});

it("getRaceTrendRunningStylesFromD1 swallows KV put errors and still returns D1 rows", async () => {
  const { db } = buildD1Stub([
    { race_key: "nar:20260524:47:01", horse_number: 9, predicted_label: "oikomi" },
  ]);
  const kv: KvStub = {
    get: vi.fn<AnyMockFn>().mockResolvedValue(null),
    put: vi.fn<AnyMockFn>().mockRejectedValue(new Error("KV PUT failed")),
  };
  installContext({ cache: null, db, kv });
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const rows = await getRaceTrendRunningStylesFromD1(["nar:20260524:47:01"]);
  expect(rows).toStrictEqual([
    { raceKey: "nar:20260524:47:01", horseNumber: "9", predictedLabel: "oikomi" },
  ]);
  expect(consoleSpy).toHaveBeenCalledWith("KV put for running-styles failed", expect.any(Error));
  consoleSpy.mockRestore();
});
