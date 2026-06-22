// Run with bun.
import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("./fetch-odds", () => ({
  fetchAndStoreOdds: vi.fn(async () => null),
}));

vi.mock("./odds-cache", () => ({
  OddsCacheHot: class {},
  readCachedOdds: vi.fn(async () => null),
  writeCachedOdds: vi.fn(async () => undefined),
}));

vi.mock("./scheduled-race-list", () => ({
  populateMultiDayOddsFetchState: vi.fn(async () => ({ inserted: 0, perDay: [], total: 0 })),
  populateTodayOddsFetchState: vi.fn(async () => ({ inserted: 0, total: 0 })),
}));

vi.mock("./expected-race-count", () => ({
  getExpectedRaceCountForDate: vi.fn(async () => 0),
}));

import { getExpectedRaceCountForDate } from "./expected-race-count";
import { fetchAndStoreOdds } from "./fetch-odds";
import { readCachedOdds } from "./odds-cache";
import { populateMultiDayOddsFetchState, populateTodayOddsFetchState } from "./scheduled-race-list";
import worker, {
  buildOddsPayloadFromD1,
  collectPlanDates,
  groupRowsForFinalBackup,
  handleFetchRequest,
  handleGetMigrationState,
  handleGetOdds,
  handleImportOddsChunk,
  handleMigrationState,
  handleQueue,
  handleR2ArchiveRows,
  handleRunPopulateMultiDay,
  handleRunPopulateToday,
  handleScheduled,
  handleUpsertOddsFetchState,
  isAuthorizedInternalRequest,
  parseRaceKeyFromPath,
  processArchiveJob,
  processFetchOddsJob,
  reportScheduledOuterThrow,
  runScheduledArchive,
  runScheduledPlan,
  runScheduledPopulateMultiDay,
} from "./worker";
import type { Env, Job } from "./types";

interface CacheMockHandle {
  match: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
  delete: ReturnType<typeof vi.fn>;
}

let cacheMock: CacheMockHandle;

beforeEach(() => {
  cacheMock = {
    delete: vi.fn(async () => true),
    match: vi.fn(async () => undefined),
    put: vi.fn(async () => undefined),
  };
  vi.stubGlobal("caches", { default: cacheMock });
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  vi.mocked(populateTodayOddsFetchState).mockClear();
  vi.mocked(populateMultiDayOddsFetchState).mockClear();
  vi.mocked(getExpectedRaceCountForDate).mockReset();
  vi.mocked(getExpectedRaceCountForDate).mockResolvedValue(0);
  vi.mocked(readCachedOdds).mockReset();
  vi.mocked(readCachedOdds).mockResolvedValue(null);
});

const POLLING_WINDOW_KV_KEY = "odds-polling-window:active";

const buildKv = (): KVNamespace =>
  ({
    delete: vi.fn(async () => undefined),
    // Default polling-window gate to active so existing planner-cron tests
    // exercise the same runScheduledPlan path they did before the gate.
    get: vi.fn(async (key: string) => (key === POLLING_WINDOW_KV_KEY ? "true" : null)),
    put: vi.fn(async () => undefined),
  }) as unknown as KVNamespace;

const buildR2 = (): R2Bucket =>
  ({
    delete: vi.fn(async () => undefined),
    get: vi.fn(async () => null),
    head: vi.fn(async () => null),
    list: vi.fn(async () => ({ objects: [] })),
    put: vi.fn(async () => ({})),
  }) as unknown as R2Bucket;

const buildQueue = (): Queue<Job> =>
  ({
    send: vi.fn(async () => undefined),
    sendBatch: vi.fn(async () => undefined),
  }) as unknown as Queue<Job>;

interface BuildDbOptions {
  latest?: { results: unknown[] };
  tansho?: { results: unknown[] };
  byType?: { results: unknown[] };
  upsertRun?: ReturnType<typeof vi.fn>;
  logRun?: ReturnType<typeof vi.fn>;
  archiveCandidates?: { results: unknown[] };
  stateCount?: number;
}

const buildDb = (options: BuildDbOptions = {}): D1Database => {
  const prepareMock = vi.fn((sql: string) => {
    const lowered = sql.toLowerCase();
    if (lowered.includes("from odds_snapshots") && lowered.includes("max(fetched_at)")) {
      const all = vi.fn(async () => options.latest ?? { results: [] });
      return { bind: vi.fn(() => ({ all })) };
    }
    if (lowered.includes("odds_type = 'tansho'")) {
      const all = vi.fn(async () => options.tansho ?? { results: [] });
      return { bind: vi.fn(() => ({ all })) };
    }
    if (lowered.includes("from odds_snapshots") && lowered.includes("group by")) {
      const all = vi.fn(async () => options.archiveCandidates ?? { results: [] });
      return { bind: vi.fn(() => ({ all })) };
    }
    if (lowered.includes("from odds_snapshots")) {
      const all = vi.fn(async () => options.byType ?? { results: [] });
      return { bind: vi.fn(() => ({ all })) };
    }
    if (lowered.includes("count(*)") && lowered.includes("from odds_fetch_state")) {
      const first = vi.fn(async () => ({ count: options.stateCount ?? 0 }));
      return { bind: vi.fn(() => ({ first })) };
    }
    if (lowered.includes("from odds_fetch_state")) {
      const all = vi.fn(async () => ({ results: [] }));
      return { bind: vi.fn(() => ({ all })) };
    }
    if (lowered.includes("insert into fetch_logs")) {
      const run = options.logRun ?? vi.fn(async () => ({ meta: { changes: 1 } }));
      return { bind: vi.fn(() => ({ run })) };
    }
    if (lowered.includes("insert into odds_fetch_state")) {
      const run = options.upsertRun ?? vi.fn(async () => ({ meta: { changes: 1 } }));
      return { bind: vi.fn(() => ({ run })) };
    }
    const run = vi.fn(async () => ({ meta: { changes: 1 } }));
    return { bind: vi.fn(() => ({ run })) };
  });
  const batch = vi.fn(async () => []);
  return { batch, prepare: prepareMock } as unknown as D1Database;
};

const buildEnv = (overrides: Partial<Env> = {}): Env =>
  ({
    ODDS_ARCHIVE: buildR2(),
    ODDS_HOT_KV: buildKv(),
    PC_KEIBA_VIEWER_INTERNAL_TOKEN: "secret",
    REALTIME_HOT_DB: buildDb(),
    REALTIME_HOT_JOBS: buildQueue(),
    ...overrides,
  }) as unknown as Env;

const buildCtx = (): ExecutionContext =>
  ({
    passThroughOnException: vi.fn(),
    waitUntil: vi.fn(),
  }) as unknown as ExecutionContext;

it("parseRaceKeyFromPath returns the decoded race key", () => {
  expect(parseRaceKeyFromPath("/api/odds/nar%3A20260528%3A42%3A01")).toBe("nar:20260528:42:01");
});

it("parseRaceKeyFromPath returns null for unmatched path", () => {
  expect(parseRaceKeyFromPath("/health")).toBeNull();
});

it("isAuthorizedInternalRequest returns false when env token is unset", () => {
  const env = buildEnv({ PC_KEIBA_VIEWER_INTERNAL_TOKEN: undefined });
  expect(
    isAuthorizedInternalRequest(
      new Request("https://x/", { headers: { "x-pc-keiba-internal-token": "secret" } }),
      env,
    ),
  ).toBe(false);
});

it("isAuthorizedInternalRequest returns true when header matches token", () => {
  const env = buildEnv();
  expect(
    isAuthorizedInternalRequest(
      new Request("https://x/", { headers: { "x-pc-keiba-internal-token": "secret" } }),
      env,
    ),
  ).toBe(true);
});

it("isAuthorizedInternalRequest returns false when header missing", () => {
  expect(isAuthorizedInternalRequest(new Request("https://x/"), buildEnv())).toBe(false);
});

it("buildOddsPayloadFromD1 returns empty payload when D1 has no rows", async () => {
  const payload = await buildOddsPayloadFromD1(buildEnv(), "nar:20260528:42:01");
  expect(payload).toStrictEqual({
    fetchedAt: null,
    history: [],
    historyByType: {},
    latest: {},
  });
});

it("buildOddsPayloadFromD1 returns payload with fetched data", async () => {
  const env = buildEnv({
    REALTIME_HOT_DB: buildDb({
      latest: {
        results: [
          {
            average_odds: null,
            combination: "01",
            fetched_at: "2026-05-28T10:00:00+09:00",
            max_odds: null,
            min_odds: null,
            odds: 2.5,
            odds_type: "tansho",
            rank: 1,
          },
        ],
      },
      tansho: {
        results: [
          {
            combination: "01",
            fetched_at: "2026-05-28T10:00:00+09:00",
            odds: 2.5,
            rank: 1,
          },
        ],
      },
    }),
  });
  const payload = await buildOddsPayloadFromD1(env, "nar:20260528:42:01");
  expect(payload.fetchedAt).toBe("2026-05-28T10:00:00+09:00");
});

it("handleGetOdds returns force-fresh payload directly from D1", async () => {
  const env = buildEnv();
  const response = await handleGetOdds(
    env,
    new Request("https://x/api/odds/nar:20260528:42:01?fresh=1"),
    "nar:20260528:42:01",
  );
  expect(response.status).toBe(200);
  expect(cacheMock.put).toHaveBeenCalled();
});

it("handleGetOdds returns the edge-cached response when present", async () => {
  const cached = new Response(JSON.stringify({ cached: true }), { status: 200 });
  cacheMock.match.mockResolvedValueOnce(cached);
  const response = await handleGetOdds(
    buildEnv(),
    new Request("https://x/api/odds/nar:20260528:42:01"),
    "nar:20260528:42:01",
  );
  expect(response).toBe(cached);
});

it("handleGetOdds returns the KV mirror payload but does not write it to the edge cache", async () => {
  const env = buildEnv();
  const kvGet = env.ODDS_HOT_KV.get as unknown as ReturnType<typeof vi.fn>;
  kvGet.mockResolvedValueOnce(
    JSON.stringify({
      fetchedAt: new Date().toISOString(),
      latest: { tansho: [{ combination: "01" }] },
    }),
  );
  const response = await handleGetOdds(
    env,
    new Request("https://x/api/odds/nar:20260528:42:01"),
    "nar:20260528:42:01",
  );
  expect(response.status).toBe(200);
  expect(cacheMock.put).not.toHaveBeenCalled();
});

it("handleGetOdds returns the DO payload and writes it to the edge cache when tansho snapshots meet the threshold", async () => {
  vi.mocked(readCachedOdds).mockResolvedValueOnce({
    fetchedAt: "2026-05-28T10:09:00+09:00",
    history: [
      {
        horseNumber: "01",
        points: [
          { fetchedAt: "2026-05-28T10:00:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:01:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:02:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:03:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:04:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:05:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:06:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:07:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:08:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:09:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
        ],
      },
    ],
    historyByType: {},
    latest: { tansho: [{ combination: "01", odds: 2.5, rank: 1 }] },
  });
  const response = await handleGetOdds(
    buildEnv(),
    new Request("https://x/api/odds/nar:20260528:42:01"),
    "nar:20260528:42:01",
  );
  expect(response.status).toBe(200);
  expect(cacheMock.put).toHaveBeenCalledTimes(1);
});

it("handleGetOdds falls through to KV mirror when DO history is empty", async () => {
  vi.mocked(readCachedOdds).mockResolvedValueOnce({
    fetchedAt: "2026-05-28T10:00:00+09:00",
    history: [],
    historyByType: {},
    latest: {},
  });
  const env = buildEnv();
  const kvGet = env.ODDS_HOT_KV.get as unknown as ReturnType<typeof vi.fn>;
  kvGet.mockResolvedValueOnce(
    JSON.stringify({
      fetchedAt: new Date().toISOString(),
      latest: { tansho: [{ combination: "01" }] },
    }),
  );
  const response = await handleGetOdds(
    env,
    new Request("https://x/api/odds/nar:20260528:42:01"),
    "nar:20260528:42:01",
  );
  expect(response.status).toBe(200);
  expect(cacheMock.put).not.toHaveBeenCalled();
});

it("handleGetOdds falls through to KV mirror when DO tansho has only 5 distinct snapshots (below threshold)", async () => {
  vi.mocked(readCachedOdds).mockResolvedValueOnce({
    fetchedAt: "2026-05-28T10:04:00+09:00",
    history: [
      {
        horseNumber: "01",
        points: [
          { fetchedAt: "2026-05-28T10:00:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:01:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:02:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:03:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:04:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
        ],
      },
    ],
    historyByType: {},
    latest: { tansho: [{ combination: "01", odds: 2.5, rank: 1 }] },
  });
  const env = buildEnv();
  const kvGet = env.ODDS_HOT_KV.get as unknown as ReturnType<typeof vi.fn>;
  kvGet.mockResolvedValueOnce(
    JSON.stringify({
      fetchedAt: new Date().toISOString(),
      latest: { tansho: [{ combination: "01" }] },
    }),
  );
  const response = await handleGetOdds(
    env,
    new Request("https://x/api/odds/nar:20260528:42:01"),
    "nar:20260528:42:01",
  );
  expect(response.status).toBe(200);
  expect(cacheMock.put).not.toHaveBeenCalled();
});

it("handleGetOdds serves from DO when tansho has exactly 10 distinct snapshots (boundary)", async () => {
  vi.mocked(readCachedOdds).mockResolvedValueOnce({
    fetchedAt: "2026-05-28T10:09:00+09:00",
    history: [
      {
        horseNumber: "01",
        points: [
          { fetchedAt: "2026-05-28T10:00:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:01:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:02:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:03:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:04:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:05:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:06:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:07:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:08:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:09:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
        ],
      },
    ],
    historyByType: {},
    latest: { tansho: [{ combination: "01", odds: 2.5, rank: 1 }] },
  });
  const response = await handleGetOdds(
    buildEnv(),
    new Request("https://x/api/odds/nar:20260528:42:01"),
    "nar:20260528:42:01",
  );
  expect(response.status).toBe(200);
  expect(cacheMock.put).toHaveBeenCalledTimes(1);
});

it("handleGetOdds serves from DO when tansho has 50 distinct snapshots across multiple horses", async () => {
  const buildPointsForHorse = (
    horseNumber: string,
  ): { fetchedAt: string; horseNumber: string; odds: number; popularity: number }[] =>
    Array.from({ length: 25 }, (_, index) => ({
      fetchedAt: `2026-05-28T10:${String(index).padStart(2, "0")}:00+09:00`,
      horseNumber,
      odds: 2.5,
      popularity: 1,
    }));
  vi.mocked(readCachedOdds).mockResolvedValueOnce({
    fetchedAt: "2026-05-28T10:24:00+09:00",
    history: [
      { horseNumber: "01", points: buildPointsForHorse("01") },
      {
        horseNumber: "02",
        points: Array.from({ length: 25 }, (_, index) => ({
          fetchedAt: `2026-05-28T10:${String(index + 25).padStart(2, "0")}:00+09:00`,
          horseNumber: "02",
          odds: 3.0,
          popularity: 2,
        })),
      },
    ],
    historyByType: {},
    latest: { tansho: [{ combination: "01", odds: 2.5, rank: 1 }] },
  });
  const response = await handleGetOdds(
    buildEnv(),
    new Request("https://x/api/odds/nar:20260528:42:01"),
    "nar:20260528:42:01",
  );
  expect(response.status).toBe(200);
  expect(cacheMock.put).toHaveBeenCalledTimes(1);
});

it("handleGetOdds falls through to D1 result cache when DO tansho is shallow and KV mirror is missing", async () => {
  vi.mocked(readCachedOdds).mockResolvedValueOnce({
    fetchedAt: "2026-05-28T10:02:00+09:00",
    history: [
      {
        horseNumber: "01",
        points: [
          { fetchedAt: "2026-05-28T10:00:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:01:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
          { fetchedAt: "2026-05-28T10:02:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
        ],
      },
    ],
    historyByType: {},
    latest: { tansho: [{ combination: "01", odds: 2.5, rank: 1 }] },
  });
  cacheMock.match.mockResolvedValueOnce(undefined);
  cacheMock.match.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        fetchedAt: "2026-05-28T11:00:00+09:00",
        history: [],
        historyByType: {},
        latest: {},
      }),
    ),
  );
  const response = await handleGetOdds(
    buildEnv(),
    new Request("https://x/api/odds/nar:20260528:42:01"),
    "nar:20260528:42:01",
  );
  expect(response.status).toBe(200);
});

it("handleGetOdds falls through to D1 query when DO tansho is shallow and no other cache layer hits", async () => {
  vi.mocked(readCachedOdds).mockResolvedValueOnce({
    fetchedAt: "2026-05-28T10:00:00+09:00",
    history: [
      {
        horseNumber: "01",
        points: [
          { fetchedAt: "2026-05-28T10:00:00+09:00", horseNumber: "01", odds: 2.5, popularity: 1 },
        ],
      },
    ],
    historyByType: {},
    latest: { tansho: [{ combination: "01", odds: 2.5, rank: 1 }] },
  });
  const response = await handleGetOdds(
    buildEnv(),
    new Request("https://x/api/odds/nar:20260528:42:01"),
    "nar:20260528:42:01",
  );
  expect(response.status).toBe(200);
  expect(cacheMock.put).toHaveBeenCalled();
});

it("handleGetOdds is fail-soft when DO read throws", async () => {
  vi.mocked(readCachedOdds).mockRejectedValueOnce(new Error("do down"));
  const env = buildEnv();
  const kvGet = env.ODDS_HOT_KV.get as unknown as ReturnType<typeof vi.fn>;
  kvGet.mockResolvedValueOnce(
    JSON.stringify({
      fetchedAt: new Date().toISOString(),
      latest: { tansho: [{ combination: "01" }] },
    }),
  );
  const response = await handleGetOdds(
    env,
    new Request("https://x/api/odds/nar:20260528:42:01"),
    "nar:20260528:42:01",
  );
  expect(response.status).toBe(200);
  expect(cacheMock.put).not.toHaveBeenCalled();
});

it("handleGetOdds returns the D1 result cache payload when present", async () => {
  cacheMock.match.mockResolvedValueOnce(undefined);
  cacheMock.match.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        fetchedAt: "2026-05-28T10:00:00+09:00",
        history: [],
        historyByType: {},
        latest: {},
      }),
    ),
  );
  const response = await handleGetOdds(
    buildEnv(),
    new Request("https://x/api/odds/nar:20260528:42:01"),
    "nar:20260528:42:01",
  );
  expect(response.status).toBe(200);
});

it("handleGetOdds falls back to D1 query when no cache layer hits", async () => {
  const response = await handleGetOdds(
    buildEnv(),
    new Request("https://x/api/odds/nar:20260528:42:01"),
    "nar:20260528:42:01",
  );
  expect(response.status).toBe(200);
  expect(cacheMock.put).toHaveBeenCalled();
});

it("handleUpsertOddsFetchState returns 401 when unauthorized", async () => {
  const response = await handleUpsertOddsFetchState(
    buildEnv(),
    new Request("https://x/api/internal/odds-fetch-state", {
      body: "{}",
      method: "POST",
    }),
  );
  expect(response.status).toBe(401);
});

it("handleUpsertOddsFetchState writes state and invalidates KV list", async () => {
  const env = buildEnv();
  const response = await handleUpsertOddsFetchState(
    env,
    new Request("https://x/api/internal/odds-fetch-state", {
      body: JSON.stringify({
        debaUrl: "https://example.com",
        kaisaiNen: "2026",
        kaisaiTsukihi: "0528",
        keibajoCode: "42",
        oddsLinksJson: "{}",
        raceBango: "01",
        raceKey: "nar:20260528:42:01",
        raceStartAtJst: "2026-05-28T10:00:00+09:00",
        source: "nar",
      }),
      headers: { "x-pc-keiba-internal-token": "secret" },
      method: "POST",
    }),
  );
  expect(response.status).toBe(200);
  expect(vi.mocked(env.ODDS_HOT_KV.delete)).toHaveBeenCalledWith("odds:race-list:v1:nar:20260528");
});

it("handleImportOddsChunk returns 401 when unauthorized", async () => {
  const response = await handleImportOddsChunk(
    buildEnv(),
    new Request("https://x/api/internal/import-odds-chunk", { method: "POST" }),
  );
  expect(response.status).toBe(401);
});

it("handleImportOddsChunk inserts rows and returns count", async () => {
  const env = buildEnv();
  const response = await handleImportOddsChunk(
    env,
    new Request("https://x/api/internal/import-odds-chunk", {
      body: JSON.stringify({
        rows: [
          {
            average_odds: null,
            combination: "01",
            fetched_at: "2026-05-28T10:00:00+09:00",
            max_odds: null,
            min_odds: null,
            odds: 2.5,
            odds_type: "tansho",
            race_key: "nar:20260528:42:01",
            rank: 1,
          },
        ],
      }),
      headers: { "x-pc-keiba-internal-token": "secret" },
      method: "POST",
    }),
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ inserted: 1 });
});

it("handleImportOddsChunk treats missing rows array as empty", async () => {
  const env = buildEnv();
  const response = await handleImportOddsChunk(
    env,
    new Request("https://x/api/internal/import-odds-chunk", {
      body: "{}",
      headers: { "x-pc-keiba-internal-token": "secret" },
      method: "POST",
    }),
  );
  expect(await response.json()).toStrictEqual({ inserted: 0 });
});

it("groupRowsForFinalBackup groups rows by race_key, odds_type, and date", () => {
  const groups = groupRowsForFinalBackup([
    {
      average_odds: null,
      combination: "01",
      fetched_at: "2026-05-20T10:00:00+09:00",
      id: 1,
      max_odds: null,
      min_odds: null,
      odds: 2.5,
      odds_type: "tansho",
      race_key: "nar:20260520:42:01",
      rank: 1,
    },
    {
      average_odds: null,
      combination: "02",
      fetched_at: "2026-05-20T11:00:00+09:00",
      id: 2,
      max_odds: null,
      min_odds: null,
      odds: 5.0,
      odds_type: "tansho",
      race_key: "nar:20260520:42:01",
      rank: 2,
    },
    {
      average_odds: null,
      combination: "03",
      fetched_at: "2026-05-21T10:00:00+09:00",
      id: 3,
      max_odds: null,
      min_odds: null,
      odds: 3.0,
      odds_type: "tansho",
      race_key: "nar:20260520:42:01",
      rank: 1,
    },
  ]);
  expect(groups.size).toBe(2);
});

it("groupRowsForFinalBackup returns empty map for empty input", () => {
  expect(groupRowsForFinalBackup([]).size).toBe(0);
});

it("handleR2ArchiveRows returns 401 when unauthorized", async () => {
  const response = await handleR2ArchiveRows(
    buildEnv(),
    new Request("https://x/api/internal/r2-archive-rows", { method: "POST" }),
  );
  expect(response.status).toBe(401);
});

it("handleR2ArchiveRows groups rows and writes to R2", async () => {
  const env = buildEnv();
  const response = await handleR2ArchiveRows(
    env,
    new Request("https://x/api/internal/r2-archive-rows", {
      body: JSON.stringify({
        rows: [
          {
            average_odds: null,
            combination: "01",
            fetched_at: "2026-05-20T10:00:00+09:00",
            id: 1,
            max_odds: null,
            min_odds: null,
            odds: 2.5,
            odds_type: "tansho",
            race_key: "nar:20260520:42:01",
            rank: 1,
          },
        ],
      }),
      headers: { "x-pc-keiba-internal-token": "secret" },
      method: "POST",
    }),
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ groups: 1, rows: 1 });
  expect(vi.mocked(env.ODDS_ARCHIVE.put)).toHaveBeenCalled();
});

it("handleR2ArchiveRows treats missing rows array as empty", async () => {
  const env = buildEnv();
  const response = await handleR2ArchiveRows(
    env,
    new Request("https://x/api/internal/r2-archive-rows", {
      body: "{}",
      headers: { "x-pc-keiba-internal-token": "secret" },
      method: "POST",
    }),
  );
  expect(await response.json()).toStrictEqual({ groups: 0, rows: 0 });
});

it("handleRunPopulateToday returns 401 when unauthorized", async () => {
  const response = await handleRunPopulateToday(
    buildEnv(),
    new Request("https://x/api/internal/run-populate-today", { method: "POST" }),
  );
  expect(response.status).toBe(401);
  expect(vi.mocked(populateTodayOddsFetchState)).not.toHaveBeenCalled();
});

it("handleRunPopulateToday runs populateTodayOddsFetchState and returns the counts", async () => {
  vi.mocked(populateTodayOddsFetchState).mockResolvedValueOnce({ inserted: 12, total: 12 });
  const response = await handleRunPopulateToday(
    buildEnv(),
    new Request("https://x/api/internal/run-populate-today", {
      headers: { "x-pc-keiba-internal-token": "secret" },
      method: "POST",
    }),
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ inserted: 12, total: 12 });
  expect(vi.mocked(populateTodayOddsFetchState)).toHaveBeenCalledTimes(1);
});

it("handleFetchRequest routes run-populate-today endpoint", async () => {
  const response = await handleFetchRequest(
    buildEnv(),
    new Request("https://x/api/internal/run-populate-today", { method: "POST" }),
  );
  expect(response.status).toBe(401);
});

it("handleRunPopulateMultiDay returns 401 when unauthorized", async () => {
  const response = await handleRunPopulateMultiDay(
    buildEnv(),
    new Request("https://x/api/internal/run-populate-multi-day", { method: "POST" }),
  );
  expect(response.status).toBe(401);
  expect(vi.mocked(populateMultiDayOddsFetchState)).not.toHaveBeenCalled();
});

it("handleRunPopulateMultiDay runs populateMultiDayOddsFetchState and returns the aggregated counts", async () => {
  vi.mocked(populateMultiDayOddsFetchState).mockResolvedValueOnce({
    inserted: 18,
    perDay: [
      { inserted: 6, total: 6, yyyymmdd: "20260528" },
      { inserted: 6, total: 6, yyyymmdd: "20260529" },
      { inserted: 6, total: 6, yyyymmdd: "20260530" },
    ],
    total: 18,
  });
  const response = await handleRunPopulateMultiDay(
    buildEnv(),
    new Request("https://x/api/internal/run-populate-multi-day", {
      headers: { "x-pc-keiba-internal-token": "secret" },
      method: "POST",
    }),
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({
    inserted: 18,
    perDay: [
      { inserted: 6, total: 6, yyyymmdd: "20260528" },
      { inserted: 6, total: 6, yyyymmdd: "20260529" },
      { inserted: 6, total: 6, yyyymmdd: "20260530" },
    ],
    total: 18,
  });
  expect(vi.mocked(populateMultiDayOddsFetchState)).toHaveBeenCalledTimes(1);
});

it("handleFetchRequest routes run-populate-multi-day endpoint", async () => {
  const response = await handleFetchRequest(
    buildEnv(),
    new Request("https://x/api/internal/run-populate-multi-day", { method: "POST" }),
  );
  expect(response.status).toBe(401);
});

it("handleFetchRequest routes r2-archive-rows endpoint", async () => {
  const response = await handleFetchRequest(
    buildEnv(),
    new Request("https://x/api/internal/r2-archive-rows", { method: "POST" }),
  );
  expect(response.status).toBe(401);
});

it("handleMigrationState returns 401 when unauthorized", async () => {
  const response = await handleMigrationState(
    buildEnv(),
    new Request("https://x/api/internal/migration-state", { method: "POST" }),
  );
  expect(response.status).toBe(401);
});

it("handleGetMigrationState returns 401 when unauthorized", async () => {
  const response = await handleGetMigrationState(
    buildEnv(),
    new Request("https://x/api/internal/migration-state?key=b1-max-id"),
  );
  expect(response.status).toBe(401);
});

it("handleGetMigrationState returns 400 when key missing", async () => {
  const response = await handleGetMigrationState(
    buildEnv(),
    new Request("https://x/api/internal/migration-state", {
      headers: { "x-pc-keiba-internal-token": "secret" },
    }),
  );
  expect(response.status).toBe(400);
});

it("handleGetMigrationState reads value from KV under odds:migration prefix", async () => {
  const env = buildEnv();
  const getMock = env.ODDS_HOT_KV.get as unknown as ReturnType<typeof vi.fn>;
  getMock.mockResolvedValueOnce("42");
  const response = await handleGetMigrationState(
    env,
    new Request("https://x/api/internal/migration-state?key=b1-max-id", {
      headers: { "x-pc-keiba-internal-token": "secret" },
    }),
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ key: "b1-max-id", value: "42" });
});

it("handleMigrationState writes value to KV under odds:migration prefix", async () => {
  const env = buildEnv();
  const response = await handleMigrationState(
    env,
    new Request("https://x/api/internal/migration-state", {
      body: JSON.stringify({ key: "b1-max-id", value: "42" }),
      headers: { "x-pc-keiba-internal-token": "secret" },
      method: "POST",
    }),
  );
  expect(response.status).toBe(200);
  expect(vi.mocked(env.ODDS_HOT_KV.put)).toHaveBeenCalledWith("odds:migration:b1-max-id", "42");
});

it("handleFetchRequest returns health payload at root", async () => {
  const response = await handleFetchRequest(buildEnv(), new Request("https://x/"));
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ name: "sync-realtime-data-hot", ok: true });
});

it("handleFetchRequest routes upsert endpoint", async () => {
  const response = await handleFetchRequest(
    buildEnv(),
    new Request("https://x/api/internal/odds-fetch-state", { method: "POST" }),
  );
  expect(response.status).toBe(401);
});

it("handleFetchRequest routes import endpoint", async () => {
  const response = await handleFetchRequest(
    buildEnv(),
    new Request("https://x/api/internal/import-odds-chunk", { method: "POST" }),
  );
  expect(response.status).toBe(401);
});

it("handleFetchRequest routes migration-state endpoint", async () => {
  const response = await handleFetchRequest(
    buildEnv(),
    new Request("https://x/api/internal/migration-state", { method: "POST" }),
  );
  expect(response.status).toBe(401);
});

it("handleFetchRequest routes GET migration-state endpoint", async () => {
  const response = await handleFetchRequest(
    buildEnv(),
    new Request("https://x/api/internal/migration-state?key=b1-max-id"),
  );
  expect(response.status).toBe(401);
});

it("handleFetchRequest routes GET /api/odds/:raceKey", async () => {
  const response = await handleFetchRequest(
    buildEnv(),
    new Request("https://x/api/odds/nar:20260528:42:01?fresh=1"),
  );
  expect(response.status).toBe(200);
});

it("handleFetchRequest returns 404 for unknown path", async () => {
  const response = await handleFetchRequest(buildEnv(), new Request("https://x/unknown"));
  expect(response.status).toBe(404);
});

it("collectPlanDates returns today plus the next two JST days", () => {
  expect(collectPlanDates(new Date("2026-05-29T14:41:00Z"))).toStrictEqual([
    "20260529",
    "20260530",
    "20260531",
  ]);
});

it("collectPlanDates produces three dates across month boundaries", () => {
  expect(collectPlanDates(new Date("2026-05-30T15:00:00Z"))).toStrictEqual([
    "20260531",
    "20260601",
    "20260602",
  ]);
});

it("runScheduledPlan plans today and the next two days even late at night JST", async () => {
  const env = buildEnv({ REALTIME_HOT_DB: buildDb({ stateCount: 5 }) });
  await runScheduledPlan(env, new Date("2026-05-29T14:41:00Z"));
  const prepareCalls = vi.mocked(env.REALTIME_HOT_DB.prepare).mock.calls.map(([sql]) => sql);
  expect(prepareCalls.some((sql) => sql.toLowerCase().includes("count(*)"))).toBe(true);
  expect(
    prepareCalls.filter((sql) => sql.toLowerCase().includes("from odds_fetch_state")).length,
  ).toBe(7);
  expect(vi.mocked(populateTodayOddsFetchState)).not.toHaveBeenCalled();
});

it("runScheduledPlan plans 3 days inside the legacy daytime window with rows present", async () => {
  const env = buildEnv({ REALTIME_HOT_DB: buildDb({ stateCount: 5 }) });
  await runScheduledPlan(env, new Date("2026-05-28T01:00:00Z"));
  expect(vi.mocked(env.REALTIME_HOT_DB.prepare)).toHaveBeenCalled();
  expect(vi.mocked(populateTodayOddsFetchState)).not.toHaveBeenCalled();
});

it("runScheduledPlan triggers self-discovery populate when stateCount is short of expected", async () => {
  vi.mocked(getExpectedRaceCountForDate).mockResolvedValueOnce(58);
  const env = buildEnv({ REALTIME_HOT_DB: buildDb({ stateCount: 0 }) });
  await runScheduledPlan(env, new Date("2026-05-28T01:00:00Z"));
  expect(vi.mocked(populateTodayOddsFetchState)).toHaveBeenCalledTimes(1);
});

it("runScheduledPlan triggers populate when stateCount equals 45 and expectedCount equals 58 (Banei missing case)", async () => {
  vi.mocked(getExpectedRaceCountForDate).mockResolvedValueOnce(58);
  const env = buildEnv({ REALTIME_HOT_DB: buildDb({ stateCount: 45 }) });
  await runScheduledPlan(env, new Date("2026-05-28T01:00:00Z"));
  expect(vi.mocked(populateTodayOddsFetchState)).toHaveBeenCalledTimes(1);
});

it("runScheduledPlan skips populate when stateCount equals expectedCount (full day populated)", async () => {
  vi.mocked(getExpectedRaceCountForDate).mockResolvedValueOnce(58);
  const env = buildEnv({ REALTIME_HOT_DB: buildDb({ stateCount: 58 }) });
  await runScheduledPlan(env, new Date("2026-05-28T01:00:00Z"));
  expect(vi.mocked(populateTodayOddsFetchState)).not.toHaveBeenCalled();
});

it("runScheduledPlan skips populate when both stateCount and expectedCount are zero (no race day)", async () => {
  vi.mocked(getExpectedRaceCountForDate).mockResolvedValueOnce(0);
  const env = buildEnv({ REALTIME_HOT_DB: buildDb({ stateCount: 0 }) });
  await runScheduledPlan(env, new Date("2026-05-28T01:00:00Z"));
  expect(vi.mocked(populateTodayOddsFetchState)).not.toHaveBeenCalled();
});

it("runScheduledPlan logs and continues when countOddsFetchStateForDate throws", async () => {
  const logRun = vi.fn(async () => ({ meta: { changes: 1 } }));
  const prepareMock = vi.fn((sql: string) => {
    const lowered = sql.toLowerCase();
    if (lowered.includes("count(*)") && lowered.includes("from odds_fetch_state")) {
      return {
        bind: vi.fn(() => ({
          first: vi.fn(async () => {
            throw new Error("d1 count failed");
          }),
        })),
      };
    }
    if (lowered.includes("insert into fetch_logs")) {
      return { bind: vi.fn(() => ({ run: logRun })) };
    }
    if (lowered.includes("from odds_fetch_state")) {
      return { bind: vi.fn(() => ({ all: vi.fn(async () => ({ results: [] })) })) };
    }
    return { bind: vi.fn(() => ({ run: vi.fn(async () => ({ meta: { changes: 1 } })) })) };
  });
  const env = buildEnv({
    REALTIME_HOT_DB: {
      batch: vi.fn(async () => []),
      prepare: prepareMock,
    } as unknown as D1Database,
  });
  await runScheduledPlan(env, new Date("2026-05-28T01:00:00Z"));
  expect(logRun).toHaveBeenCalled();
  expect(vi.mocked(populateTodayOddsFetchState)).not.toHaveBeenCalled();
});

it("runScheduledPlan logs and continues when getExpectedRaceCountForDate throws", async () => {
  vi.mocked(getExpectedRaceCountForDate).mockRejectedValueOnce(new Error("hyperdrive down"));
  const logRun = vi.fn(async () => ({ meta: { changes: 1 } }));
  const env = buildEnv({ REALTIME_HOT_DB: buildDb({ logRun, stateCount: 5 }) });
  await runScheduledPlan(env, new Date("2026-05-28T01:00:00Z"));
  expect(logRun).toHaveBeenCalled();
  expect(vi.mocked(populateTodayOddsFetchState)).not.toHaveBeenCalled();
});

it("runScheduledPlan logs and continues when populateTodayOddsFetchState throws", async () => {
  vi.mocked(getExpectedRaceCountForDate).mockResolvedValueOnce(58);
  vi.mocked(populateTodayOddsFetchState).mockRejectedValueOnce(new Error("populate failed"));
  const logRun = vi.fn(async () => ({ meta: { changes: 1 } }));
  const env = buildEnv({ REALTIME_HOT_DB: buildDb({ logRun, stateCount: 0 }) });
  await runScheduledPlan(env, new Date("2026-05-28T01:00:00Z"));
  expect(logRun).toHaveBeenCalled();
});

it("runScheduledPlan uses allSettled so a single planOddsFetches rejection does not block others", async () => {
  let queueSendCalls = 0;
  const queueSend = vi.fn(async () => {
    queueSendCalls += 1;
    if (queueSendCalls === 1) {
      throw new Error("queue down on first call");
    }
  });
  const logRun = vi.fn(async () => ({ meta: { changes: 1 } }));
  // Cron now = 2026-05-29 13:41 UTC = 2026-05-29 22:41 JST, so plan dates
  // are 20260529, 20260530, 20260531. Each entry's raceStart is set 24h
  // ahead so the enqueue lock TTL is positive and queue.send is invoked.
  const prepareMock = vi.fn((sql: string) => {
    const lowered = sql.toLowerCase();
    if (lowered.includes("count(*)") && lowered.includes("from odds_fetch_state")) {
      return { bind: vi.fn(() => ({ first: vi.fn(async () => ({ count: 5 })) })) };
    }
    if (lowered.includes("insert into fetch_logs")) {
      return { bind: vi.fn(() => ({ run: logRun })) };
    }
    if (lowered.includes("from odds_fetch_state")) {
      return {
        bind: vi.fn(() => ({
          all: vi.fn(async () => ({
            results: [
              {
                deba_url: "https://example.com",
                kaisai_nen: "2026",
                kaisai_tsukihi: "0530",
                keibajo_code: "42",
                last_odds_fetch_at: null,
                odds_links_json: "{}",
                race_bango: "01",
                race_key: "nar:20260530:42:01",
                race_start_at_jst: "2026-05-30T15:00:00+09:00",
                source: "nar",
              },
            ],
          })),
        })),
      };
    }
    return { bind: vi.fn(() => ({ run: vi.fn(async () => ({ meta: { changes: 1 } })) })) };
  });
  const env = buildEnv({
    REALTIME_HOT_DB: {
      batch: vi.fn(async () => []),
      prepare: prepareMock,
    } as unknown as D1Database,
    REALTIME_HOT_JOBS: {
      send: queueSend,
      sendBatch: vi.fn(async () => undefined),
    } as unknown as Queue<Job>,
  });
  await runScheduledPlan(env, new Date("2026-05-29T13:41:00Z"));
  // The first queue.send threw (Promise.all inside planOddsFetches rejected
  // for that source), but Promise.allSettled in runScheduledPlan kept the
  // other dates running, so queueSend was hit more than once.
  expect(queueSend.mock.calls.length).toBeGreaterThan(1);
  expect(logRun).toHaveBeenCalled();
});

it("runScheduledPopulateMultiDay delegates to populateMultiDayOddsFetchState", async () => {
  const env = buildEnv();
  await runScheduledPopulateMultiDay(env, new Date("2026-05-28T20:55:00Z"));
  expect(vi.mocked(populateMultiDayOddsFetchState)).toHaveBeenCalledTimes(1);
});

it("runScheduledArchive lists candidates and pushes to R2", async () => {
  const env = buildEnv({
    REALTIME_HOT_DB: buildDb({
      archiveCandidates: {
        results: [
          {
            fetched_at: "2026-05-20T10:00:00+09:00",
            odds_type: "tansho",
            race_key: "nar:20260520:42:01",
            snapshot_json: "[]",
          },
        ],
      },
    }),
  });
  await runScheduledArchive(env, new Date("2026-05-28T13:00:00Z"));
  expect(vi.mocked(env.ODDS_ARCHIVE.put)).toHaveBeenCalledTimes(1);
});

it("handleScheduled dispatches plan cron to runScheduledPlan", async () => {
  const env = buildEnv();
  await handleScheduled(
    {
      cron: "* * * * *",
      noRetry: () => undefined,
      scheduledTime: new Date("2026-05-28T01:00:00Z").getTime(),
    } as unknown as ScheduledController,
    env,
    buildCtx(),
  );
  expect(vi.mocked(env.REALTIME_HOT_DB.prepare)).toHaveBeenCalled();
});

it("handleScheduled skips runScheduledPlan when the polling-window gate cache reports inactive", async () => {
  const env = buildEnv();
  const kvGet = env.ODDS_HOT_KV.get as unknown as ReturnType<typeof vi.fn>;
  kvGet.mockImplementation(async (key: string) =>
    key === "odds-polling-window:active" ? "false" : null,
  );
  await handleScheduled(
    {
      cron: "* * * * *",
      noRetry: () => undefined,
      scheduledTime: new Date("2026-05-28T01:00:00Z").getTime(),
    } as unknown as ScheduledController,
    env,
    buildCtx(),
  );
  expect(vi.mocked(getExpectedRaceCountForDate)).not.toHaveBeenCalled();
  expect(vi.mocked(populateTodayOddsFetchState)).not.toHaveBeenCalled();
});

it("handleScheduled dispatches archive cron to runScheduledArchive", async () => {
  const env = buildEnv();
  await handleScheduled(
    {
      cron: "0 4 * * *",
      noRetry: () => undefined,
      scheduledTime: new Date("2026-05-28T04:00:00Z").getTime(),
    } as unknown as ScheduledController,
    env,
    buildCtx(),
  );
  expect(vi.mocked(env.REALTIME_HOT_DB.prepare)).toHaveBeenCalled();
});

it("handleScheduled dispatches populate-multi-day cron to runScheduledPopulateMultiDay", async () => {
  const env = buildEnv();
  await handleScheduled(
    {
      cron: "55 20 * * *",
      noRetry: () => undefined,
      scheduledTime: new Date("2026-05-28T20:55:00Z").getTime(),
    } as unknown as ScheduledController,
    env,
    buildCtx(),
  );
  expect(vi.mocked(populateMultiDayOddsFetchState)).toHaveBeenCalledTimes(1);
  expect(vi.mocked(populateTodayOddsFetchState)).not.toHaveBeenCalled();
});

it("handleScheduled dispatches morning populate-multi-day cron (JST 08:00) to runScheduledPopulateMultiDay", async () => {
  const env = buildEnv();
  await handleScheduled(
    {
      cron: "0 23 * * *",
      noRetry: () => undefined,
      scheduledTime: new Date("2026-05-28T23:00:00Z").getTime(),
    } as unknown as ScheduledController,
    env,
    buildCtx(),
  );
  expect(vi.mocked(populateMultiDayOddsFetchState)).toHaveBeenCalledTimes(1);
  expect(vi.mocked(populateTodayOddsFetchState)).not.toHaveBeenCalled();
});

it("handleScheduled catches runScheduledArchive rejection and logs via logFetch", async () => {
  const logRun = vi.fn(async () => ({ meta: { changes: 1 } }));
  const prepareMock = vi.fn((sql: string) => {
    const lowered = sql.toLowerCase();
    if (lowered.includes("insert into fetch_logs")) {
      return { bind: vi.fn(() => ({ run: logRun })) };
    }
    if (lowered.includes("from odds_snapshots") && lowered.includes("group by")) {
      return {
        bind: vi.fn(() => ({
          all: vi.fn(async () => {
            throw new Error("archive list failed");
          }),
        })),
      };
    }
    return { bind: vi.fn(() => ({ run: vi.fn(async () => ({ meta: { changes: 1 } })) })) };
  });
  const env = buildEnv({
    REALTIME_HOT_DB: {
      batch: vi.fn(async () => []),
      prepare: prepareMock,
    } as unknown as D1Database,
  });
  await handleScheduled(
    {
      cron: "0 4 * * *",
      noRetry: () => undefined,
      scheduledTime: new Date("2026-05-28T04:00:00Z").getTime(),
    } as unknown as ScheduledController,
    env,
    buildCtx(),
  );
  // handleScheduled completed without rethrowing; logFetch recorded the error.
  expect(logRun).toHaveBeenCalled();
});

it("handleScheduled falls back to console.error when both inner work and logFetch throw", async () => {
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const prepareMock = vi.fn((sql: string) => {
    const lowered = sql.toLowerCase();
    if (lowered.includes("insert into fetch_logs")) {
      return {
        bind: vi.fn(() => ({
          run: vi.fn(async () => {
            throw new Error("fetch_logs also down");
          }),
        })),
      };
    }
    if (lowered.includes("from odds_snapshots") && lowered.includes("group by")) {
      return {
        bind: vi.fn(() => ({
          all: vi.fn(async () => {
            throw new Error("archive list failed");
          }),
        })),
      };
    }
    return { bind: vi.fn(() => ({ run: vi.fn(async () => ({ meta: { changes: 1 } })) })) };
  });
  const env = buildEnv({
    REALTIME_HOT_DB: {
      batch: vi.fn(async () => []),
      prepare: prepareMock,
    } as unknown as D1Database,
  });
  await handleScheduled(
    {
      cron: "0 4 * * *",
      noRetry: () => undefined,
      scheduledTime: new Date("2026-05-28T04:00:00Z").getTime(),
    } as unknown as ScheduledController,
    env,
    buildCtx(),
  );
  expect(consoleSpy).toHaveBeenCalled();
});

it("handleScheduled does nothing for unknown cron", async () => {
  const env = buildEnv();
  await handleScheduled(
    {
      cron: "*/5 * * * *",
      noRetry: () => undefined,
      scheduledTime: new Date().getTime(),
    } as unknown as ScheduledController,
    env,
    buildCtx(),
  );
  expect(vi.mocked(env.REALTIME_HOT_DB.prepare)).not.toHaveBeenCalled();
  expect(vi.mocked(populateTodayOddsFetchState)).not.toHaveBeenCalled();
  expect(vi.mocked(populateMultiDayOddsFetchState)).not.toHaveBeenCalled();
});

it("processFetchOddsJob returns early when fetchAndStoreOdds yields null", async () => {
  vi.mocked(fetchAndStoreOdds).mockResolvedValueOnce(null);
  const env = buildEnv();
  await processFetchOddsJob(env, "nar:2026:0528:42:01");
  expect(cacheMock.delete).not.toHaveBeenCalled();
  expect(vi.mocked(env.ODDS_HOT_KV.put)).not.toHaveBeenCalled();
});

it("processFetchOddsJob fans out cache writes on success (NAR)", async () => {
  vi.mocked(fetchAndStoreOdds).mockResolvedValueOnce({
    fetchedAt: "2026-05-28T10:00:00+09:00",
    inserted: 11,
    latest: { tansho: [{ combination: "01", odds: 2.5 }] },
  });
  const env = buildEnv();
  await processFetchOddsJob(env, "nar:2026:0528:42:01");
  expect(cacheMock.delete).toHaveBeenCalled();
  expect(vi.mocked(env.ODDS_HOT_KV.put)).toHaveBeenCalled();
});

it("processFetchOddsJob detects JRA source prefix on success", async () => {
  vi.mocked(fetchAndStoreOdds).mockResolvedValueOnce({
    fetchedAt: "2026-05-28T10:00:00+09:00",
    inserted: 11,
    latest: { tansho: [{ combination: "01", odds: 2.5 }] },
  });
  const env = buildEnv();
  await processFetchOddsJob(env, "jra:2026:0528:08:01");
  expect(cacheMock.delete).toHaveBeenCalled();
});

it("processFetchOddsJob passes the correct yyyymmdd to patchLastFetchInKv for a 5-segment raceKey", async () => {
  vi.mocked(fetchAndStoreOdds).mockResolvedValueOnce({
    fetchedAt: "2026-05-29T10:00:00+09:00",
    inserted: 11,
    latest: { tansho: [{ combination: "01", odds: 2.5 }] },
  });
  const env = buildEnv();
  const getMock = env.ODDS_HOT_KV.get as unknown as ReturnType<typeof vi.fn>;
  getMock.mockImplementation(async (key: string) =>
    key === "odds:race-list:v1:nar:20260529"
      ? JSON.stringify([
          {
            lastOddsFetchAt: null,
            raceKey: "nar:2026:0529:47:01",
            raceStartAtJst: "2026-05-29T10:30:00+09:00",
            source: "nar",
          },
        ])
      : null,
  );
  await processFetchOddsJob(env, "nar:2026:0529:47:01");
  expect(getMock).toHaveBeenCalledWith("odds:race-list:v1:nar:20260529");
});

it("processFetchOddsJob calls logFetch and returns early when raceKey format is invalid", async () => {
  vi.mocked(fetchAndStoreOdds).mockResolvedValueOnce({
    fetchedAt: "2026-05-28T10:00:00+09:00",
    inserted: 11,
    latest: { tansho: [{ combination: "01", odds: 2.5 }] },
  });
  const env = buildEnv();
  await processFetchOddsJob(env, "nar:20260528:42:01");
  expect(cacheMock.delete).not.toHaveBeenCalled();
  expect(vi.mocked(env.ODDS_HOT_KV.put)).not.toHaveBeenCalled();
  expect(vi.mocked(env.REALTIME_HOT_DB.prepare)).toHaveBeenCalled();
});

it("processFetchOddsJob swallows logFetch failure when raceKey format is invalid", async () => {
  vi.mocked(fetchAndStoreOdds).mockResolvedValueOnce({
    fetchedAt: "2026-05-28T10:00:00+09:00",
    inserted: 11,
    latest: { tansho: [{ combination: "01", odds: 2.5 }] },
  });
  const env = buildEnv({
    REALTIME_HOT_DB: buildDb({
      logRun: vi.fn(async () => {
        throw new Error("log fetch failed");
      }),
    }),
  });
  await processFetchOddsJob(env, "nar:20260528:42:01");
  expect(cacheMock.delete).not.toHaveBeenCalled();
  expect(vi.mocked(env.ODDS_HOT_KV.put)).not.toHaveBeenCalled();
});

it("processArchiveJob delegates to runScheduledArchive", async () => {
  const env = buildEnv();
  await processArchiveJob(env, new Date("2026-05-28T13:00:00Z"));
  expect(vi.mocked(env.REALTIME_HOT_DB.prepare)).toHaveBeenCalled();
});

it("handleQueue acks fetch-odds messages", async () => {
  const env = buildEnv();
  const ack = vi.fn();
  const retry = vi.fn();
  const batch = {
    messages: [{ ack, body: { raceKey: "nar:20260528:42:01", type: "fetch-odds" }, retry }],
  } as unknown as MessageBatch<Job>;
  await handleQueue(batch, env);
  expect(ack).toHaveBeenCalledTimes(1);
});

it("handleQueue acks archive-odds-to-r2 messages", async () => {
  const env = buildEnv();
  const ack = vi.fn();
  const retry = vi.fn();
  const batch = {
    messages: [{ ack, body: { date: "20260528", type: "archive-odds-to-r2" }, retry }],
  } as unknown as MessageBatch<Job>;
  await handleQueue(batch, env);
  expect(ack).toHaveBeenCalledTimes(1);
});

it("handleQueue acks unknown jobs without retry", async () => {
  const env = buildEnv();
  const ack = vi.fn();
  const retry = vi.fn();
  const batch = {
    messages: [{ ack, body: { date: "20260528", type: "plan-odds-fetches" }, retry }],
  } as unknown as MessageBatch<Job>;
  await handleQueue(batch, env);
  expect(ack).toHaveBeenCalledTimes(1);
  expect(retry).not.toHaveBeenCalled();
});

it("handleQueue retries on error", async () => {
  vi.mocked(fetchAndStoreOdds).mockRejectedValueOnce(new Error("scrape failed"));
  const env = buildEnv();
  const ack = vi.fn();
  const retry = vi.fn();
  const batch = {
    messages: [{ ack, body: { raceKey: "nar:20260528:42:01", type: "fetch-odds" }, retry }],
  } as unknown as MessageBatch<Job>;
  await handleQueue(batch, env);
  expect(retry).toHaveBeenCalledTimes(1);
});

it("default worker fetch dispatches to handleFetchRequest", async () => {
  const response = await worker.fetch(new Request("https://x/"), buildEnv());
  expect(response.status).toBe(200);
});

it("default worker scheduled dispatches to handleScheduled", async () => {
  const env = buildEnv();
  await worker.scheduled(
    {
      cron: "* * * * *",
      noRetry: () => undefined,
      scheduledTime: new Date("2026-05-28T01:00:00Z").getTime(),
    } as unknown as ScheduledController,
    env,
    buildCtx(),
  );
  expect(vi.mocked(env.REALTIME_HOT_DB.prepare)).toHaveBeenCalled();
});

it("default worker scheduled accepts the Module Workers (controller, env, ctx) 3-arg signature", async () => {
  const env = buildEnv();
  const waitUntil = vi.fn();
  const ctx = {
    passThroughOnException: vi.fn(),
    waitUntil,
  } as unknown as ExecutionContext;
  await worker.scheduled(
    {
      cron: "* * * * *",
      noRetry: () => undefined,
      scheduledTime: new Date("2026-05-28T01:00:00Z").getTime(),
    } as unknown as ScheduledController,
    env,
    ctx,
  );
  expect(vi.mocked(env.REALTIME_HOT_DB.prepare)).toHaveBeenCalled();
  expect(waitUntil).not.toHaveBeenCalled();
});

it("default worker scheduled outer try/catch still fires when handleScheduled body throws", async () => {
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const logRun = vi.fn(async () => ({ meta: { changes: 1 } }));
  const prepareMock = vi.fn((sql: string) => {
    void sql;
    return { bind: vi.fn(() => ({ run: logRun })) };
  });
  const env = buildEnv({
    REALTIME_HOT_DB: {
      batch: vi.fn(async () => []),
      prepare: prepareMock,
    } as unknown as D1Database,
  });
  // Passing `null` as the controller makes `controller.scheduledTime` throw
  // BEFORE the inner try/catch in handleScheduled, so the outer try/catch in
  // worker.scheduled fires and reportScheduledOuterThrow runs.
  await worker.scheduled(null as unknown as ScheduledController, env, buildCtx());
  expect(consoleSpy.mock.calls[0]?.[0]).toBe("scheduled-outer-throw");
  expect(logRun).toHaveBeenCalledTimes(1);
});

it("default worker queue dispatches to handleQueue", async () => {
  const env = buildEnv();
  const ack = vi.fn();
  const retry = vi.fn();
  const batch = {
    messages: [{ ack, body: { raceKey: "nar:20260528:42:01", type: "fetch-odds" }, retry }],
  } as unknown as MessageBatch<Job>;
  await worker.queue(batch, env);
  expect(ack).toHaveBeenCalled();
});

it("scheduled-outer-catch-logs-error-and-stack-to-d1", async () => {
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const logRun = vi.fn(async () => ({ meta: { changes: 1 } }));
  const bindMock = vi.fn((...args: unknown[]) => {
    void args;
    return { run: logRun };
  });
  const prepareMock = vi.fn((sql: string) => {
    void sql;
    return { bind: bindMock };
  });
  const env = buildEnv({
    REALTIME_HOT_DB: {
      batch: vi.fn(async () => []),
      prepare: prepareMock,
    } as unknown as D1Database,
  });
  // Passing `null` as the controller makes `controller.scheduledTime` throw
  // a TypeError BEFORE the inner try/catch in handleScheduled, so the
  // outer try/catch in worker.scheduled fires.
  await worker.scheduled(null as unknown as ScheduledController, env, buildCtx());
  expect(consoleSpy.mock.calls.length).toBe(1);
  expect(consoleSpy.mock.calls[0]?.[0]).toBe("scheduled-outer-throw");
  const insertCalls = prepareMock.mock.calls.filter(([sql]) =>
    sql.toLowerCase().includes("insert into fetch_logs"),
  );
  expect(insertCalls.length).toBe(1);
  expect(bindMock).toHaveBeenCalledTimes(1);
  expect(bindMock.mock.calls[0]?.[0]).toBeNull();
  expect(bindMock.mock.calls[0]?.[1]).toBe("scheduled-outer-throw");
  expect(bindMock.mock.calls[0]?.[2]).toBe("error");
  expect(logRun).toHaveBeenCalledTimes(1);
});

it("scheduled-outer-catch-handles-logfetch-failure", async () => {
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const prepareMock = vi.fn((sql: string) => {
    void sql;
    return {
      bind: vi.fn((...args: unknown[]) => {
        void args;
        return {
          run: vi.fn(async () => {
            throw new Error("fetch_logs down");
          }),
        };
      }),
    };
  });
  const env = buildEnv({
    REALTIME_HOT_DB: {
      batch: vi.fn(async () => []),
      prepare: prepareMock,
    } as unknown as D1Database,
  });
  await worker.scheduled(null as unknown as ScheduledController, env, buildCtx());
  expect(consoleSpy.mock.calls.length).toBe(2);
  expect(consoleSpy.mock.calls[0]?.[0]).toBe("scheduled-outer-throw");
  expect(consoleSpy.mock.calls[1]?.[0]).toBe("scheduled-outer-throw logFetch fallback");
});

it("scheduled-outer-catch-handles-non-error-object", async () => {
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const bindMock = vi.fn((...args: unknown[]) => {
    void args;
    return { run: vi.fn(async () => ({ meta: { changes: 1 } })) };
  });
  const env = buildEnv({
    REALTIME_HOT_DB: {
      batch: vi.fn(async () => []),
      prepare: vi.fn((sql: string) => {
        void sql;
        return { bind: bindMock };
      }),
    } as unknown as D1Database,
  });
  await reportScheduledOuterThrow(env, "literal string");
  expect(consoleSpy.mock.calls[0]?.[0]).toBe("scheduled-outer-throw");
  expect(consoleSpy.mock.calls[0]?.[1]).toBe("literal string");
  expect(bindMock.mock.calls[0]?.[3]).toBe("literal string");
});

it("scheduled-outer-runs-without-error-when-handler-ok", async () => {
  const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const prepareMock = vi.fn((sql: string) => {
    const lowered = sql.toLowerCase();
    if (lowered.includes("count(*)") && lowered.includes("from odds_fetch_state")) {
      return { bind: vi.fn(() => ({ first: vi.fn(async () => ({ count: 0 })) })) };
    }
    if (lowered.includes("from odds_fetch_state")) {
      return { bind: vi.fn(() => ({ all: vi.fn(async () => ({ results: [] })) })) };
    }
    return { bind: vi.fn(() => ({ run: vi.fn(async () => ({ meta: { changes: 1 } })) })) };
  });
  const env = buildEnv({
    REALTIME_HOT_DB: {
      batch: vi.fn(async () => []),
      prepare: prepareMock,
    } as unknown as D1Database,
  });
  await worker.scheduled(
    {
      cron: "* * * * *",
      noRetry: () => undefined,
      scheduledTime: new Date("2026-05-28T01:00:00Z").getTime(),
    } as unknown as ScheduledController,
    env,
    buildCtx(),
  );
  const insertLogCalls = prepareMock.mock.calls.filter(([sql]) =>
    sql.toLowerCase().includes("insert into fetch_logs"),
  );
  expect(insertLogCalls.length).toBe(0);
  expect(consoleSpy).not.toHaveBeenCalled();
});
