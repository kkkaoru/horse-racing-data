// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { Env } from "./types";

vi.mock("./finish-position-lite-pool", () => ({
  getFinishPositionPool: vi.fn(() => ({
    query: vi.fn(async () => ({ rows: [] })),
  })),
}));

const DAILY_ROW = {
  bamei: "サンプル",
  babajotai_code_dirt: null,
  babajotai_code_shiba: null,
  bataiju: 480,
  chokyoshimei_ryakusho: "調教師",
  corner1_norm: null,
  corner2_norm: null,
  corner3_norm: null,
  corner4_norm: null,
  corner_1: null,
  corner_2: null,
  corner_3: null,
  corner_4: null,
  finish_norm: null,
  finish_position: null,
  futan_juryo: 555,
  grade_code: null,
  hasso_jikoku: "1500",
  juryo_shubetsu_code: null,
  kaisai_nen: "2026",
  kaisai_tsukihi: "0512",
  keibajo_code: "08",
  ketto_toroku_bango: "2024100001",
  kishumei_ryakusho: "騎手",
  kohan_3f: null,
  kyori: 2000,
  kyoso_joken_code: null,
  kyoso_shubetsu_code: null,
  race_bango: "01",
  race_date: "20260512",
  race_name: "サンプル",
  seibetsu_code: null,
  shusso_tosu: 16,
  soha_time: null,
  source: "jra",
  tansho_ninkijun: 1,
  tansho_odds: 1.5,
  time_sa: null,
  track_code: null,
  umaban: 1,
  wakuban: "1",
  zogen_fugo: "+",
  zogen_sa: 2,
};

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("listDailyRaceEntriesForRace returns mapped rows for the resolved race key", async () => {
  const { listDailyRaceEntriesForRace } = await import("./daily-feature-build");
  const all = vi.fn(async () => ({ results: [DAILY_ROW] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listDailyRaceEntriesForRace(db, {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "8",
    raceBango: "1",
    source: "jra",
  });
  expect(result.length).toBe(1);
  expect(result[0]!.kaisai_nen).toBe("2026");
  expect(bind).toHaveBeenCalledWith("jra:20260512:08:01");
});

it("listDailyRaceEntriesForRace maps unknown value types (plain object) to null", async () => {
  const { listDailyRaceEntriesForRace } = await import("./daily-feature-build");
  const all = vi.fn(async () => ({
    results: [
      {
        ...DAILY_ROW,
        bataiju: { weird: true } as unknown,
        tansho_odds: [1, 2] as unknown,
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listDailyRaceEntriesForRace(db, {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "8",
    raceBango: "1",
    source: "jra",
  });
  expect(result[0]?.bataiju).toBeNull();
  expect(result[0]?.tansho_odds).toBeNull();
});

it("listDailyRaceEntriesForRace converts numeric strings and treats non-finite numbers as null", async () => {
  const { listDailyRaceEntriesForRace } = await import("./daily-feature-build");
  const all = vi.fn(async () => ({
    results: [
      {
        ...DAILY_ROW,
        bataiju: Number.NaN,
        corner_1: "3",
        finish_position: "non-numeric",
        tansho_odds: Number.POSITIVE_INFINITY,
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listDailyRaceEntriesForRace(db, {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "8",
    raceBango: "1",
    source: "jra",
  });
  expect(result[0]?.bataiju).toBeNull();
  expect(result[0]?.tansho_odds).toBeNull();
  expect(result[0]?.corner_1).toBe(3);
  expect(result[0]?.finish_position).toBeNull();
});

it("triggerViewerCacheWarmForDate returns ok and parses warmed/raceCount from response", async () => {
  const { triggerViewerCacheWarmForDate } = await import("./daily-feature-build");
  vi.spyOn(globalThis, "fetch").mockResolvedValue(
    new Response(JSON.stringify({ raceCount: 12, warmed: 12 }), {
      headers: { "content-type": "application/json" },
      status: 200,
    }),
  );
  const env = {} as unknown as Env;
  const result = await triggerViewerCacheWarmForDate(env, "20260512");
  expect(result.status).toBe("ok");
  expect(result.raceCount).toBe(12);
  expect(result.warmed).toBe(12);
});

it("triggerViewerCacheWarmForDate returns error status on non-2xx response", async () => {
  const { triggerViewerCacheWarmForDate } = await import("./daily-feature-build");
  vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("boom", { status: 500 }));
  const env = {} as unknown as Env;
  const result = await triggerViewerCacheWarmForDate(env, "20260512");
  expect(result.status).toBe("error");
  expect(result.message).toBe("HTTP 500");
});

it("triggerViewerCacheWarmForDate returns error when fetch throws", async () => {
  const { triggerViewerCacheWarmForDate } = await import("./daily-feature-build");
  vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("network down"));
  const env = {} as unknown as Env;
  const result = await triggerViewerCacheWarmForDate(env, "20260512");
  expect(result.status).toBe("error");
  expect(result.message).toBe("network down");
});

it("triggerViewerCacheWarmForDate returns ok with no payload fields when response body is not JSON", async () => {
  const { triggerViewerCacheWarmForDate } = await import("./daily-feature-build");
  vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("not json", { status: 200 }));
  const env = {} as unknown as Env;
  const result = await triggerViewerCacheWarmForDate(env, "20260512");
  expect(result.status).toBe("ok");
  expect(result.raceCount).toBeUndefined();
  expect(result.warmed).toBeUndefined();
});

it("triggerViewerCacheWarmForDate uses RUNNING_STYLE_CACHE_ORIGIN when configured", async () => {
  const { triggerViewerCacheWarmForDate } = await import("./daily-feature-build");
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response("{}", { status: 200 }));
  const env = { RUNNING_STYLE_CACHE_ORIGIN: "https://configured.example" } as unknown as Env;
  await triggerViewerCacheWarmForDate(env, "20260512");
  const request = fetchSpy.mock.calls[0]![0] as Request;
  expect(request.url.startsWith("https://configured.example")).toBe(true);
});

it("runDailyFeatureBuildForEnv skips warmCache when zero rows written", async () => {
  const { runDailyFeatureBuildForEnv } = await import("./daily-feature-build");
  const { getFinishPositionPool } = await import("./finish-position-lite-pool");
  vi.mocked(getFinishPositionPool).mockReturnValue({
    query: vi.fn(async () => ({ rows: [] })),
  } as never);
  const env = { REALTIME_DB: {} } as unknown as Env;
  const result = await runDailyFeatureBuildForEnv(env, {
    fromDate: "20260512",
  });
  expect(result.rowsWritten).toBe(0);
  expect(result.cacheWarm?.status).toBe("skipped");
});
