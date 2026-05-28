// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { Env } from "./types";

vi.mock("./storage", () => ({
  logFetch: vi.fn(async () => {}),
  upsertNarRaceSource: vi.fn(async () => {}),
  upsertJraRaceSource: vi.fn(async () => {}),
  listRaceSourceKeibajoCodesByDate: vi.fn(async () => []),
  getRaceSource: vi.fn(async () => null),
  listSchedulableRaceSourcesByDate: vi.fn(async () => []),
  getVenueLastRaceStartAtJst: vi.fn(async () => null),
  countRaceSourcesByDate: vi.fn(async () => 0),
  countJraRaceSourcesMissingRaceDateFieldsByDate: vi.fn(async () => 0),
  listJraVenueTrackConditionSchedulesByDate: vi.fn(async () => []),
  markTrackConditionQueued: vi.fn(async () => {}),
  claimTrackConditionFetch: vi.fn(async () => false),
  failTrackConditionFetch: vi.fn(async () => {}),
  completeTrackConditionFetch: vi.fn(async () => {}),
  updateOddsLinks: vi.fn(async () => {}),
  updateLastFetch: vi.fn(async () => {}),
  markResultFetchQueued: vi.fn(async () => {}),
  markOddsFetchQueued: vi.fn(async () => {}),
  claimOddsFetch: vi.fn(async () => false),
  claimResultFetch: vi.fn(async () => false),
  completeOddsFetch: vi.fn(async () => {}),
  failOddsFetch: vi.fn(async () => {}),
  completeResultFetch: vi.fn(async () => {}),
  failResultFetch: vi.fn(async () => {}),
  insertOddsSnapshot: vi.fn(async () => 0),
  insertHorseWeightSnapshot: vi.fn(async () => {}),
  insertRaceEntrySnapshot: vi.fn(async () => 0),
  insertRaceResultSnapshot: vi.fn(async () => 0),
  runD1Retention: vi.fn(async () => ({ fetchLogsDeleted: 0, oddsSnapshotsDeleted: 0 })),
  upsertPremiumRaceLink: vi.fn(async () => {}),
  getPremiumRaceLink: vi.fn(async () => null),
  replacePremiumRaceData: vi.fn(async () => {}),
  getPremiumRacePayload: vi.fn(async () => ({
    dataTopHorses: [],
    paddockBulletins: [],
    stableComments: [],
    trainingReviews: [],
  })),
  listPremiumRaceDataFetchCandidatesByDate: vi.fn(async () => []),
  markPremiumRaceDataQueued: vi.fn(async () => {}),
  getPremiumRaceDataFetchState: vi.fn(async () => null),
  updatePremiumRaceDataFetchState: vi.fn(async () => {}),
  markPremiumPaddockQueued: vi.fn(async () => {}),
  getPremiumPaddockFetchState: vi.fn(async () => null),
  updatePremiumPaddockFetchState: vi.fn(async () => {}),
  getPremiumPaddockNotificationState: vi.fn(async () => null),
  updatePremiumPaddockNotificationState: vi.fn(async () => {}),
  claimPremiumPaddockNotificationSend: vi.fn(async () => true),
  recordPremiumPaddockNotificationEvent: vi.fn(async () => {}),
  listOddsSnapshotsForExport: vi.fn(async () => []),
  listTanshoHistory: vi.fn(async () => []),
  listOddsHistoryByType: vi.fn(async () => ({})),
  getLatestOddsFromD1: vi.fn(async () => null),
  toHorseTrends: vi.fn(() => []),
  toOddsTrendsByType: vi.fn(() => ({})),
  getLatestHorseWeights: vi.fn(async () => null),
  getLatestRaceEntries: vi.fn(async () => null),
  getLatestRaceResults: vi.fn(async () => null),
  getLatestTrackConditionForRace: vi.fn(async () => null),
  insertJraTrackConditionSnapshot: vi.fn(async () => []),
  getSameDayVenueJockeyWins: vi.fn(async () => []),
  buildRealtimePayload: vi.fn(async () => ({
    horseWeights: null,
    odds: null,
    raceEntries: null,
    raceKey: "k",
    raceResults: null,
    source: null,
    trackCondition: null,
  })),
}));
vi.mock("./daily-feature-build", () => ({
  runDailyFeatureBuildForEnv: vi.fn(async () => ({})),
  listDailyRaceEntriesForRace: vi.fn(async () => []),
}));
vi.mock("./win5-queue", () => ({ handleWin5PredictionJob: vi.fn() }));
vi.mock("./win5-cron", () => ({
  WIN5_DISCOVER_CRON: "0 0 * * *",
  logWin5CronResult: vi.fn(),
}));
vi.mock("./running-style-cron", () => ({
  RUNNING_STYLE_INFERENCE_CRON: "*/10 * * * *",
  RUNNING_STYLE_PREWARM_CRON: "0 12 * * *",
  planRunningStylePredictionsForDate: vi.fn(async () => ({})),
  refreshViewerRunningStyleCachesForDate: vi.fn(async () => ({})),
  refreshViewerRunningStyleCacheForRace: vi.fn(async () => false),
}));
vi.mock("./running-style-queue", () => ({ handleRunningStylePredictionJob: vi.fn() }));
vi.mock("./postgres", () => ({
  fetchJraRacesByDate: vi.fn(async () => []),
  fetchNarRacesByDate: vi.fn(async () => []),
}));
vi.mock("./keiba-go", async () => {
  const actual = await vi.importActual<typeof import("./keiba-go")>("./keiba-go");
  return {
    ...actual,
    fetchTodayRaceListUrls: vi.fn(async () => []),
    fetchOdds: vi.fn(async () => null),
    fetchRacePage: vi.fn(async () => null),
    fetchRaceLinksFromRaceList: vi.fn(async () => []),
  };
});
vi.mock("./jra", async () => {
  const actual = await vi.importActual<typeof import("./jra")>("./jra");
  return {
    ...actual,
    fetchJraResultHtmlWithPlaywright: vi.fn(async () => "<html></html>"),
    fetchJraOddsWithPlaywright: vi.fn(async () => ({ entryHtml: "", latest: {} })),
  };
});
vi.mock("./jra-track-condition", () => ({
  fetchJraTrackConditionWithPlaywright: vi.fn(async () => ({})),
}));
vi.mock("./odds-cache", () => ({
  OddsCache: class {},
  getOddsCacheId: vi.fn(),
  readCachedOdds: vi.fn(async () => null),
  writeCachedOdds: vi.fn(async () => {}),
}));
vi.mock("./premium-data-top-cache", () => ({
  putPremiumDataTopCache: vi.fn(async () => true),
  buildPremiumDataTopCacheRequest: vi.fn(),
  getPremiumDataTopCacheTtlSeconds: vi.fn(() => 100),
}));
vi.mock("./premium-paddock-cache", () => ({
  PremiumPaddockCache: class {},
  readCachedPremiumPaddock: vi.fn(async () => null),
  writeCachedPremiumPaddock: vi.fn(async () => {}),
  clearCachedPremiumPaddock: vi.fn(async () => {}),
}));
vi.mock("./track-condition-cache", () => ({
  TrackConditionCache: class {},
  readCachedTrackCondition: vi.fn(async () => null),
  writeCachedTrackCondition: vi.fn(async () => {}),
  getTrackConditionCacheId: vi.fn(),
}));
vi.mock("./premium-race", async () => {
  const actual = await vi.importActual<typeof import("./premium-race")>("./premium-race");
  return {
    ...actual,
    discoverPremiumRaceLinks: vi.fn(() => []),
    fetchPremiumHtml: vi.fn(async () => ""),
    fetchPremiumHtmlAttempts: vi.fn(async () => []),
  };
});
vi.mock("./running-style-verification", () => ({
  parseRunningStylePostgresVerificationParams: vi.fn(() => null),
  runRunningStyleWorkerPostgresVerification: vi.fn(async () => ({ ok: true })),
}));

const buildDb = (): D1Database => {
  const all = vi.fn(async () => ({ results: [] }));
  const first = vi.fn(async () => null);
  const run = vi.fn(async () => ({ meta: { changes: 0 } }));
  const bind = vi.fn(() => ({ all, first, run, bind: vi.fn() }));
  const prepare = vi.fn(() => ({ all, bind, first, run }));
  const batch = vi.fn(async () => []);
  const exec = vi.fn(async () => ({}));
  return { batch, exec, prepare } as unknown as D1Database;
};

const buildEnv = (overrides?: Partial<Env>): Env => {
  return {
    PREMIUM_RACE_JOBS: { send: vi.fn(async () => {}), sendBatch: vi.fn(async () => {}) },
    REALTIME_ADMIN_TOKEN: "secret",
    REALTIME_DB: buildDb(),
    REALTIME_JOBS: { send: vi.fn(async () => {}), sendBatch: vi.fn(async () => {}) },
    ...overrides,
  } as unknown as Env;
};

const buildCtx = () =>
  ({
    passThroughOnException: () => {},
    waitUntil: () => {},
  }) as unknown as ExecutionContext;

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("fetch responds to OPTIONS preflight with CORS headers", async () => {
  const { default: worker } = await import("./worker");
  const response = await worker.fetch(
    new Request("https://x.test/health", { method: "OPTIONS" }),
    buildEnv(),
    buildCtx(),
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("access-control-allow-origin")).toBe("*");
});

it("fetch GET /health returns ok body", async () => {
  const { default: worker } = await import("./worker");
  const response = await worker.fetch(new Request("https://x.test/health"), buildEnv(), buildCtx());
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ ok: true });
});

it("fetch POST /api/jobs returns 403 when authorization mismatches", async () => {
  const { default: worker } = await import("./worker");
  const response = await worker.fetch(
    new Request("https://x.test/api/jobs", {
      body: JSON.stringify({ date: "20260512", type: "plan-realtime-fetches" }),
      headers: { authorization: "Bearer wrong" },
      method: "POST",
    }),
    buildEnv(),
    buildCtx(),
  );
  expect(response.status).toBe(403);
});

it("fetch POST /api/jobs enqueues the job when token matches", async () => {
  const { default: worker } = await import("./worker");
  const env = buildEnv();
  const response = await worker.fetch(
    new Request("https://x.test/api/jobs", {
      body: JSON.stringify({ date: "20260512", type: "plan-realtime-fetches" }),
      headers: { authorization: "Bearer secret" },
      method: "POST",
    }),
    env,
    buildCtx(),
  );
  expect(response.status).toBe(200);
});

it("fetch GET /api/jra/races/.../realtime returns the realtime payload", async () => {
  const { default: worker } = await import("./worker");
  const response = await worker.fetch(
    new Request("https://x.test/api/jra/races/2026/05/12/08/01/realtime"),
    buildEnv(),
    buildCtx(),
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("public, max-age=20");
});

it("fetch GET /api/jra/races/.../premium returns the premium payload with cache-control", async () => {
  const { default: worker } = await import("./worker");
  const response = await worker.fetch(
    new Request("https://x.test/api/jra/races/2026/05/12/08/01/premium"),
    buildEnv(),
    buildCtx(),
  );
  expect(response.status).toBe(200);
});

it("fetch GET /api/nar/races/.../jockey-wins returns the jockey wins list", async () => {
  const { default: worker } = await import("./worker");
  const response = await worker.fetch(
    new Request("https://x.test/api/nar/races/2026/05/12/55/01/jockey-wins"),
    buildEnv(),
    buildCtx(),
  );
  expect(response.status).toBe(200);
});

it("fetch POST /admin/running-style/verify-postgres returns 403 when authorization mismatches", async () => {
  const { default: worker } = await import("./worker");
  const { parseRunningStylePostgresVerificationParams } =
    await import("./running-style-verification");
  vi.mocked(parseRunningStylePostgresVerificationParams).mockReturnValueOnce({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "08",
    raceBango: "01",
    source: "jra",
  });
  const response = await worker.fetch(
    new Request("https://x.test/admin/running-style/verify-postgres/jra/2026/05/12/08/01", {
      headers: { authorization: "Bearer wrong" },
      method: "POST",
    }),
    buildEnv(),
    buildCtx(),
  );
  expect(response.status).toBe(403);
});

it("fetch POST /admin/running-style/verify-postgres returns the verification summary when token matches", async () => {
  const { default: worker } = await import("./worker");
  const { parseRunningStylePostgresVerificationParams, runRunningStyleWorkerPostgresVerification } =
    await import("./running-style-verification");
  vi.mocked(parseRunningStylePostgresVerificationParams).mockReturnValueOnce({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "08",
    raceBango: "01",
    source: "jra",
  });
  vi.mocked(runRunningStyleWorkerPostgresVerification).mockResolvedValueOnce({
    raceKey: "jra:2026:0512:08:01",
    readBackRows: 12,
    writtenCount: 12,
  } as never);
  const response = await worker.fetch(
    new Request("https://x.test/admin/running-style/verify-postgres/jra/2026/05/12/08/01", {
      headers: { authorization: "Bearer secret" },
      method: "POST",
    }),
    buildEnv(),
    buildCtx(),
  );
  expect(response.status).toBe(200);
});

it("fetch GET realtime payload backfills horseTrends and trendsByType when missing", async () => {
  const { default: worker } = await import("./worker");
  const { buildRealtimePayload } = await import("./storage");
  vi.mocked(buildRealtimePayload).mockResolvedValueOnce({
    horseWeights: null,
    odds: {
      fetchedAt: "now",
      history: [{ fetchedAt: "now", horseNumber: "1", odds: 2.5, popularity: 1 }],
      historyByType: { tansho: [{ combination: "1", fetchedAt: "now", odds: 2.5, rank: 1 }] },
      horseTrends: [],
      latest: {},
    },
    raceEntries: null,
    raceResults: null,
    trackCondition: null,
  } as never);
  const response = await worker.fetch(
    new Request("https://x.test/api/jra/races/2026/05/12/08/01/realtime"),
    buildEnv(),
    buildCtx(),
  );
  const body = (await response.json()) as { odds: { trendsByType: unknown; horseTrends: unknown } };
  expect(body.odds.trendsByType).toBeDefined();
});

it("fetch GET premium race merges cached paddock when payload has bulletins", async () => {
  const { default: worker } = await import("./worker");
  const { getPremiumRacePayload } = await import("./storage");
  const { readCachedPremiumPaddock } = await import("./premium-paddock-cache");
  vi.mocked(getPremiumRacePayload).mockResolvedValueOnce({
    dataTopHorses: [],
    paddockBulletins: [
      {
        commentText: "x",
        evaluationText: null,
        fetchedAt: "now",
        frameNumber: "1",
        groupKey: "favorite",
        horseName: "Y",
        horseNumber: "1",
      },
    ],
    stableComments: [],
    trainingReviews: [],
  } as never);
  vi.mocked(readCachedPremiumPaddock).mockResolvedValueOnce({
    extraCacheField: "cached",
  } as never);
  const response = await worker.fetch(
    new Request("https://x.test/api/jra/races/2026/05/12/08/01/premium"),
    buildEnv(),
    buildCtx(),
  );
  const body = (await response.json()) as { extraCacheField?: string };
  expect(body.extraCacheField).toBe("cached");
});

it("fetch GET premium race returns plain payload when cached paddock is missing", async () => {
  const { default: worker } = await import("./worker");
  const { getPremiumRacePayload } = await import("./storage");
  const { readCachedPremiumPaddock } = await import("./premium-paddock-cache");
  vi.mocked(getPremiumRacePayload).mockResolvedValueOnce({
    dataTopHorses: [],
    paddockBulletins: [
      {
        commentText: "x",
        evaluationText: null,
        fetchedAt: "now",
        frameNumber: "1",
        groupKey: "favorite",
        horseName: "Y",
        horseNumber: "1",
      },
    ],
    stableComments: [],
    trainingReviews: [],
  } as never);
  vi.mocked(readCachedPremiumPaddock).mockResolvedValueOnce(null);
  const response = await worker.fetch(
    new Request("https://x.test/api/jra/races/2026/05/12/08/01/premium"),
    buildEnv(),
    buildCtx(),
  );
  expect(response.status).toBe(200);
});

it("fetch during JST polling window seeds the planner watchdog via ctx.waitUntil", async () => {
  const { default: worker } = await import("./worker");
  const waitPromises: Promise<unknown>[] = [];
  const ctx = {
    passThroughOnException: () => {},
    waitUntil: (promise: Promise<unknown>) => {
      waitPromises.push(promise.catch(() => undefined));
    },
  } as unknown as ExecutionContext;
  await worker.fetch(
    new Request("https://x.test/health"),
    buildEnv({ REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z" } as never),
    ctx,
  );
  await Promise.all(waitPromises);
  expect(waitPromises.length).toBeGreaterThanOrEqual(1);
});

it("fetch returns 403 for /api/jobs when REALTIME_ADMIN_TOKEN is missing", async () => {
  const { default: worker } = await import("./worker");
  const env = buildEnv();
  const envWithoutToken = { ...env, REALTIME_ADMIN_TOKEN: undefined } as unknown as Env;
  const response = await worker.fetch(
    new Request("https://x.test/api/jobs", {
      body: JSON.stringify({ date: "20260512", type: "plan-realtime-fetches" }),
      headers: { authorization: "Bearer anything" },
      method: "POST",
    }),
    envWithoutToken,
    buildCtx(),
  );
  expect(response.status).toBe(403);
});

it("fetch GET unknown path returns 404 with cache-control max-age=0", async () => {
  const { default: worker } = await import("./worker");
  const response = await worker.fetch(
    new Request("https://x.test/api/unknown"),
    buildEnv(),
    buildCtx(),
  );
  expect(response.status).toBe(404);
  expect(response.headers.get("cache-control")).toBe("public, max-age=0");
});

it("fetch ignores REALTIME_TEST_NOW when not parseable as Date", async () => {
  const { default: worker } = await import("./worker");
  const response = await worker.fetch(
    new Request("https://x.test/health"),
    buildEnv({ REALTIME_TEST_NOW: "not-a-date" } as never),
    buildCtx(),
  );
  expect(response.status).toBe(200);
});

it("fetch GET to an unmatched path returns the catch-all 404", async () => {
  const { default: worker } = await import("./worker");
  const response = await worker.fetch(
    new Request("https://x.test/unknown"),
    buildEnv(),
    buildCtx(),
  );
  expect(response.status).toBe(404);
});

it("fetch POST /api/internal/export-odds-chunk returns 403 when token missing", async () => {
  const { default: worker } = await import("./worker");
  const env = buildEnv();
  const envWithoutToken = { ...env, REALTIME_ADMIN_TOKEN: undefined } as unknown as Env;
  const response = await worker.fetch(
    new Request("https://x.test/api/internal/export-odds-chunk", {
      body: JSON.stringify({ batch_size: 100, since_id: 0 }),
      method: "POST",
    }),
    envWithoutToken,
    buildCtx(),
  );
  expect(response.status).toBe(403);
});

it("fetch POST /api/internal/export-odds-chunk returns rows when authorized", async () => {
  const { default: worker } = await import("./worker");
  const env = buildEnv();
  const response = await worker.fetch(
    new Request("https://x.test/api/internal/export-odds-chunk", {
      body: JSON.stringify({ batch_size: 200, since_id: 0 }),
      headers: { authorization: "Bearer secret" },
      method: "POST",
    }),
    env,
    buildCtx(),
  );
  expect(response.status).toBe(200);
});

it("fetch POST /api/internal/export-odds-chunk accepts after_fetched_at option", async () => {
  const { default: worker } = await import("./worker");
  const env = buildEnv();
  const response = await worker.fetch(
    new Request("https://x.test/api/internal/export-odds-chunk", {
      body: JSON.stringify({
        after_fetched_at: "2026-05-27T00:00:00+09:00",
        batch_size: 200,
        since_id: 0,
      }),
      headers: { authorization: "Bearer secret" },
      method: "POST",
    }),
    env,
    buildCtx(),
  );
  expect(response.status).toBe(200);
});
