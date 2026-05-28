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
  getRaceTrendD1StarterRows,
  getRaceTrendDailyStarterRows,
} from "./d1-trend-queries.server";

type AnyMockFn = (...args: never[]) => unknown;

interface PreparedStub {
  all: ReturnType<typeof vi.fn<AnyMockFn>>;
  bind: ReturnType<typeof vi.fn<AnyMockFn>>;
}

interface D1Stub {
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

interface BuildContextArgs {
  cache?: CacheStub | null;
  db?: D1Stub;
  kv?: KvStub;
}

const SAMPLE_RAW_ROW = {
  source: "nar",
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

const SAMPLE_RAW_DAILY_ROW = {
  source: "jra",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0528",
  keibajoCode: "06",
  raceBango: "11",
  raceName: "Daily Race",
  hassoJikoku: "1530",
  runnerCount: 16,
  wakuban: "8",
  umaban: 12,
  bamei: "DailyHorse",
  jockeyName: "DailyJockey",
  tanshoOddsTenth: 56,
  tanshoPopularity: 3,
  finishPosition: 2,
  sohaTime: 950,
  corner1: 4,
  corner2: 5,
  corner3: 3,
  corner4: 2,
  bataijuInt: 466,
  zogenFugo: "-",
  zogenSaInt: 4,
};

const buildPreparedStub = (rows: unknown[]): PreparedStub => {
  const all = vi.fn<AnyMockFn>().mockResolvedValue({ results: rows });
  const bind = vi.fn<AnyMockFn>().mockReturnValue({ all });
  return { all, bind };
};

const buildD1Stub = (rows: unknown[]): { db: D1Stub; prepared: PreparedStub } => {
  const prepared = buildPreparedStub(rows);
  const db: D1Stub = { prepare: vi.fn<AnyMockFn>().mockReturnValue(prepared) };
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

const installContext = ({ cache, db, kv }: BuildContextArgs): void => {
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

it("maps D1 trend rows when no cache exists", async () => {
  const { db, prepared } = buildD1Stub([SAMPLE_RAW_ROW]);
  const cache = buildCacheStub();
  const kv = buildKvStub();
  installContext({ cache, db, kv });
  const rows = await getRaceTrendD1StarterRows({
    source: "nar",
    startYmd: "20260501",
    endYmd: "20260528",
  });
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
      wakuban: "3",
      umaban: "05",
      bamei: "TestHorse",
      jockeyName: "TestJockey",
      tanshoOdds: "0123",
      tanshoPopularity: "04",
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
  expect(prepared.bind).toHaveBeenCalledWith("nar", "20260501", "20260528");
  expect(cache.put).toHaveBeenCalledTimes(1);
  expect(kv.put).toHaveBeenCalledTimes(1);
});

it("returns Cache API hit without hitting D1", async () => {
  const cached = [
    {
      source: "nar",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0501",
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
    new Response(JSON.stringify(cached), {
      headers: { "Content-Type": "application/json" },
    }),
  );
  const { db } = buildD1Stub([SAMPLE_RAW_ROW]);
  installContext({ cache, db, kv: buildKvStub() });
  const rows = await getRaceTrendD1StarterRows({
    source: "nar",
    startYmd: "20260501",
    endYmd: "20260528",
  });
  expect(rows).toStrictEqual(cached);
  expect(db.prepare).not.toHaveBeenCalled();
});

it("falls back to KV cache when Cache API misses", async () => {
  const cached = [
    {
      source: "jra",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0501",
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
  const { db } = buildD1Stub([SAMPLE_RAW_ROW]);
  installContext({ cache, db, kv });
  const rows = await getRaceTrendD1StarterRows({
    source: "jra",
    startYmd: "20260501",
    endYmd: "20260528",
  });
  expect(rows).toStrictEqual(cached);
  expect(db.prepare).not.toHaveBeenCalled();
});

it("returns [] when REALTIME_DB binding is missing", async () => {
  installContext({ cache: buildCacheStub(), db: undefined, kv: buildKvStub() });
  const rows = await getRaceTrendD1StarterRows({
    source: "nar",
    startYmd: "20260501",
    endYmd: "20260528",
  });
  expect(rows).toStrictEqual([]);
});

it("returns [] when D1 query throws", async () => {
  const failing: D1Stub = {
    prepare: vi.fn<AnyMockFn>().mockReturnValue({
      bind: vi.fn<AnyMockFn>().mockReturnValue({
        all: vi.fn<AnyMockFn>().mockRejectedValue(new Error("boom")),
      }),
    }),
  };
  installContext({ cache: buildCacheStub(), db: failing, kv: buildKvStub() });
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  const rows = await getRaceTrendD1StarterRows({
    source: "nar",
    startYmd: "20260501",
    endYmd: "20260528",
  });
  expect(rows).toStrictEqual([]);
  consoleSpy.mockRestore();
});

it("filters out invalid D1 trend rows", async () => {
  const invalid = { source: "nar", finishPosition: "1" };
  const { db } = buildD1Stub([invalid, SAMPLE_RAW_ROW]);
  installContext({ cache: buildCacheStub(), db, kv: buildKvStub() });
  const rows = await getRaceTrendD1StarterRows({
    source: "nar",
    startYmd: "20260501",
    endYmd: "20260528",
  });
  expect(rows.length).toBe(1);
  expect(rows[0]?.bamei).toBe("TestHorse");
});

it("handles null hasso/bataiju/zogen fields in trend rows", async () => {
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
  const rows = await getRaceTrendD1StarterRows({
    source: "nar",
    startYmd: "20260501",
    endYmd: "20260528",
  });
  expect(rows[0]?.hassoJikoku).toBe(null);
  expect(rows[0]?.bataiju).toBe(null);
  expect(rows[0]?.zogenSa).toBe(null);
  expect(rows[0]?.tanshoOdds).toBe(null);
  expect(rows[0]?.tanshoPopularity).toBe(null);
});

it("treats short hassoJikoku as null", async () => {
  const tooShort = { ...SAMPLE_RAW_ROW, hassoJikoku: "2026-05-28" };
  const { db } = buildD1Stub([tooShort]);
  installContext({ cache: buildCacheStub(), db, kv: buildKvStub() });
  const rows = await getRaceTrendD1StarterRows({
    source: "nar",
    startYmd: "20260501",
    endYmd: "20260528",
  });
  expect(rows[0]?.hassoJikoku).toBe(null);
});

it("falls back to KV when Cache API global is unavailable", async () => {
  const cached = [
    {
      source: "nar",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0501",
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
  const kv = buildKvStub(JSON.stringify(cached));
  const { db } = buildD1Stub([SAMPLE_RAW_ROW]);
  installContext({ cache: null, db, kv });
  const rows = await getRaceTrendD1StarterRows({
    source: "nar",
    startYmd: "20260501",
    endYmd: "20260528",
  });
  expect(rows).toStrictEqual(cached);
});

it("maps daily trend rows when no cache exists", async () => {
  const { db, prepared } = buildD1Stub([SAMPLE_RAW_DAILY_ROW]);
  const cache = buildCacheStub();
  const kv = buildKvStub();
  installContext({ cache, db, kv });
  const rows = await getRaceTrendDailyStarterRows({
    source: "jra",
    startYmd: "20260501",
    endYmd: "20260528",
  });
  expect(rows).toStrictEqual([
    {
      source: "jra",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0528",
      keibajoCode: "06",
      raceBango: "11",
      raceName: "Daily Race",
      hassoJikoku: "1530",
      runnerCount: "16",
      wakuban: "8",
      umaban: "12",
      bamei: "DailyHorse",
      jockeyName: "DailyJockey",
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
    },
  ]);
  expect(prepared.bind).toHaveBeenCalledWith("jra", "20260501", "20260528");
});

it("returns Cache API hit for daily trends without hitting D1", async () => {
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
    new Response(JSON.stringify(cached), {
      headers: { "Content-Type": "application/json" },
    }),
  );
  const { db } = buildD1Stub([SAMPLE_RAW_DAILY_ROW]);
  installContext({ cache, db, kv: buildKvStub() });
  const rows = await getRaceTrendDailyStarterRows({
    source: "jra",
    startYmd: "20260501",
    endYmd: "20260528",
  });
  expect(rows).toStrictEqual(cached);
  expect(db.prepare).not.toHaveBeenCalled();
});

it("falls back to KV cache for daily trends when Cache API misses", async () => {
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
  const { db } = buildD1Stub([SAMPLE_RAW_DAILY_ROW]);
  installContext({ cache, db, kv });
  const rows = await getRaceTrendDailyStarterRows({
    source: "nar",
    startYmd: "20260501",
    endYmd: "20260528",
  });
  expect(rows).toStrictEqual(cached);
  expect(db.prepare).not.toHaveBeenCalled();
});

it("returns [] when daily D1 binding is missing", async () => {
  installContext({ cache: buildCacheStub(), db: undefined, kv: buildKvStub() });
  const rows = await getRaceTrendDailyStarterRows({
    source: "jra",
    startYmd: "20260501",
    endYmd: "20260528",
  });
  expect(rows).toStrictEqual([]);
});

it("returns [] when daily D1 query throws", async () => {
  const failing: D1Stub = {
    prepare: vi.fn<AnyMockFn>().mockReturnValue({
      bind: vi.fn<AnyMockFn>().mockReturnValue({
        all: vi.fn<AnyMockFn>().mockRejectedValue(new Error("daily boom")),
      }),
    }),
  };
  installContext({ cache: buildCacheStub(), db: failing, kv: buildKvStub() });
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  const rows = await getRaceTrendDailyStarterRows({
    source: "jra",
    startYmd: "20260501",
    endYmd: "20260528",
  });
  expect(rows).toStrictEqual([]);
  consoleSpy.mockRestore();
});

it("filters out invalid daily D1 rows", async () => {
  const invalid = { source: "jra", finishPosition: null };
  const { db } = buildD1Stub([invalid, SAMPLE_RAW_DAILY_ROW]);
  installContext({ cache: buildCacheStub(), db, kv: buildKvStub() });
  const rows = await getRaceTrendDailyStarterRows({
    source: "jra",
    startYmd: "20260501",
    endYmd: "20260528",
  });
  expect(rows.length).toBe(1);
  expect(rows[0]?.bamei).toBe("DailyHorse");
});

it("handles null fields in daily trend rows", async () => {
  const sparse = {
    ...SAMPLE_RAW_DAILY_ROW,
    runnerCount: null,
    umaban: null,
    tanshoOddsTenth: null,
    tanshoPopularity: null,
    sohaTime: null,
    corner1: null,
    corner2: null,
    corner3: null,
    corner4: null,
    bataijuInt: null,
    zogenSaInt: null,
  };
  const { db } = buildD1Stub([sparse]);
  installContext({ cache: buildCacheStub(), db, kv: buildKvStub() });
  const rows = await getRaceTrendDailyStarterRows({
    source: "jra",
    startYmd: "20260501",
    endYmd: "20260528",
  });
  expect(rows[0]?.runnerCount).toBe(null);
  expect(rows[0]?.umaban).toBe(null);
  expect(rows[0]?.tanshoOdds).toBe(null);
  expect(rows[0]?.tanshoPopularity).toBe(null);
  expect(rows[0]?.sohaTime).toBe(null);
  expect(rows[0]?.corner1).toBe(null);
  expect(rows[0]?.bataiju).toBe(null);
  expect(rows[0]?.zogenSa).toBe(null);
});

it("falls back to KV for daily trends when Cache API global is unavailable", async () => {
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
  const kv = buildKvStub(JSON.stringify(cached));
  const { db } = buildD1Stub([SAMPLE_RAW_DAILY_ROW]);
  installContext({ cache: null, db, kv });
  const rows = await getRaceTrendDailyStarterRows({
    source: "jra",
    startYmd: "20260501",
    endYmd: "20260528",
  });
  expect(rows).toStrictEqual(cached);
});
