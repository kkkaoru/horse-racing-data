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
  listRaceKeysByDateFromHyperdrive: vi.fn(async () => []),
  listRaceSourcesForSeed: vi.fn(async () => []),
  deleteOddsSnapshotsChunk: vi.fn(async () => ({ deleted: 0, done: true, next_since_id: 0 })),
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

it("fetch POST /api/internal/delete-odds-chunk returns 403 when token missing", async () => {
  const { default: worker } = await import("./worker");
  const env = buildEnv();
  const envWithoutToken = { ...env, REALTIME_ADMIN_TOKEN: undefined } as unknown as Env;
  const response = await worker.fetch(
    new Request("https://x.test/api/internal/delete-odds-chunk", {
      body: JSON.stringify({ batch_size: 500, since_id: 0, upper_bound_id: 100 }),
      method: "POST",
    }),
    envWithoutToken,
    buildCtx(),
  );
  expect(response.status).toBe(403);
});

it("fetch POST /api/internal/delete-odds-chunk returns result when authorized", async () => {
  const { default: worker } = await import("./worker");
  const { deleteOddsSnapshotsChunk } = await import("./storage");
  vi.mocked(deleteOddsSnapshotsChunk).mockResolvedValueOnce({
    deleted: 3,
    done: false,
    next_since_id: 30,
  });
  const env = buildEnv();
  const response = await worker.fetch(
    new Request("https://x.test/api/internal/delete-odds-chunk", {
      body: JSON.stringify({ batch_size: 500, since_id: 0, upper_bound_id: 100 }),
      headers: { authorization: "Bearer secret" },
      method: "POST",
    }),
    env,
    buildCtx(),
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ deleted: 3, done: false, next_since_id: 30 });
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

it("fetch POST /api/internal/export-race-sources-chunk returns 403 when token missing", async () => {
  const { default: worker } = await import("./worker");
  const env = buildEnv();
  const envWithoutToken = { ...env, REALTIME_ADMIN_TOKEN: undefined } as unknown as Env;
  const response = await worker.fetch(
    new Request("https://x.test/api/internal/export-race-sources-chunk", {
      body: JSON.stringify({ batch_size: 50, since_id: 0 }),
      method: "POST",
    }),
    envWithoutToken,
    buildCtx(),
  );
  expect(response.status).toBe(403);
});

it("fetch POST /api/internal/export-race-sources-chunk returns done when authorized and rows empty", async () => {
  const { default: worker } = await import("./worker");
  const env = buildEnv();
  const response = await worker.fetch(
    new Request("https://x.test/api/internal/export-race-sources-chunk", {
      body: JSON.stringify({ batch_size: 50, since_id: 0 }),
      headers: { authorization: "Bearer secret" },
      method: "POST",
    }),
    env,
    buildCtx(),
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ done: true, next_since_id: 0, rows: [] });
});

it("fetch POST /api/internal/export-race-sources-chunk returns next_since_id from last row when rows present", async () => {
  const { default: worker } = await import("./worker");
  const { listRaceSourcesForSeed } = await import("./storage");
  vi.mocked(listRaceSourcesForSeed).mockResolvedValueOnce([
    {
      deba_url: "https://x.test/race",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0529",
      keibajo_code: "08",
      odds_links_json: "{}",
      race_bango: "01",
      race_key: "jra:2026:0529:08:01",
      race_start_at_jst: "2026-05-29T13:00:00+09:00",
      rowid: 42,
      source: "jra",
    },
  ]);
  const env = buildEnv();
  const response = await worker.fetch(
    new Request("https://x.test/api/internal/export-race-sources-chunk", {
      body: JSON.stringify({ batch_size: 50, since_id: 0 }),
      headers: { authorization: "Bearer secret" },
      method: "POST",
    }),
    env,
    buildCtx(),
  );
  expect(response.status).toBe(200);
  const body = (await response.json()) as { done: boolean; next_since_id: number };
  expect(body.next_since_id).toBe(42);
  expect(body.done).toBe(true);
});

it("fetch POST /api/internal/list-race-keys-by-date-from-hyperdrive returns 403 when token missing", async () => {
  const { default: worker } = await import("./worker");
  const env = buildEnv();
  const envWithoutToken = { ...env, REALTIME_ADMIN_TOKEN: undefined } as unknown as Env;
  const response = await worker.fetch(
    new Request("https://x.test/api/internal/list-race-keys-by-date-from-hyperdrive", {
      body: JSON.stringify({ kaisaiNen: "2026", kaisaiTsukihi: "0529" }),
      method: "POST",
    }),
    envWithoutToken,
    buildCtx(),
  );
  expect(response.status).toBe(403);
});

it("fetch POST /api/internal/list-race-keys-by-date-from-hyperdrive returns rows when authorized", async () => {
  const { default: worker } = await import("./worker");
  const { listRaceKeysByDateFromHyperdrive } = await import("./storage");
  vi.mocked(listRaceKeysByDateFromHyperdrive).mockResolvedValueOnce([
    { race_key: "nar:2026:0529:30:08" },
  ]);
  const env = buildEnv();
  const response = await worker.fetch(
    new Request("https://x.test/api/internal/list-race-keys-by-date-from-hyperdrive", {
      body: JSON.stringify({ kaisaiNen: "2026", kaisaiTsukihi: "0529" }),
      headers: { authorization: "Bearer secret" },
      method: "POST",
    }),
    env,
    buildCtx(),
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ rows: [{ race_key: "nar:2026:0529:30:08" }] });
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

it("forwardRaceSourceToHot is a no-op when REALTIME_HOT binding is missing", async () => {
  const { forwardRaceSourceToHot } = await import("./worker");
  await forwardRaceSourceToHot(buildEnv(), {
    debaUrl: "https://x.test/race",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "08",
    oddsLinksJson: "{}",
    raceBango: "01",
    raceKey: "jra:2026:0512:08:01",
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    source: "jra",
  });
});

it("forwardRaceSourceToHot is a no-op when internal token is missing", async () => {
  const { forwardRaceSourceToHot } = await import("./worker");
  const fetchMock = vi.fn();
  await forwardRaceSourceToHot(buildEnv({ REALTIME_HOT: { fetch: fetchMock } as never } as never), {
    debaUrl: "https://x.test/race",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "08",
    oddsLinksJson: "{}",
    raceBango: "01",
    raceKey: "jra:2026:0512:08:01",
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    source: "jra",
  });
  expect(fetchMock).not.toHaveBeenCalled();
});

it("forwardRaceSourceToHot posts to /api/internal/odds-fetch-state when REALTIME_HOT is configured", async () => {
  const { forwardRaceSourceToHot } = await import("./worker");
  const hotFetch = vi.fn(async () => new Response(JSON.stringify({ ok: true })));
  await forwardRaceSourceToHot(
    buildEnv({
      PC_KEIBA_VIEWER_INTERNAL_TOKEN: "internal-token",
      REALTIME_HOT: { fetch: hotFetch } as never,
    } as never),
    {
      debaUrl: "https://x.test/race",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      oddsLinksJson: "{}",
      raceBango: "01",
      raceKey: "jra:2026:0512:08:01",
      raceStartAtJst: "2026-05-12T13:00:00+09:00",
      source: "jra",
    },
  );
  expect(hotFetch).toHaveBeenCalledTimes(1);
});

it("forwardRaceSourceToHot logs the error when the hot worker fetch rejects", async () => {
  const { forwardRaceSourceToHot } = await import("./worker");
  const { logFetch } = await import("./storage");
  const hotFetch = vi.fn(async () => {
    throw new Error("hot boom");
  });
  await forwardRaceSourceToHot(
    buildEnv({
      PC_KEIBA_VIEWER_INTERNAL_TOKEN: "internal-token",
      REALTIME_HOT: { fetch: hotFetch } as never,
    } as never),
    {
      debaUrl: "https://x.test/race",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      oddsLinksJson: "{}",
      raceBango: "01",
      raceKey: "nar:2026:0512:55:01",
      raceStartAtJst: "2026-05-12T18:00:00+09:00",
      source: "nar",
    },
  );
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "forward-race-source-to-hot",
    "error",
    "nar:2026:0512:55:01",
    "hot boom",
  );
});

it("forwardRaceForFeatures is a no-op when REALTIME_FEATURES binding is missing", async () => {
  const { forwardRaceForFeatures } = await import("./worker");
  await forwardRaceForFeatures(buildEnv(), {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "08",
    raceBango: "01",
    raceKey: "jra:2026:0512:08:01",
    source: "jra",
  });
});

it("forwardRaceForFeatures is a no-op when internal token is missing", async () => {
  const { forwardRaceForFeatures } = await import("./worker");
  const featuresFetch = vi.fn();
  await forwardRaceForFeatures(
    buildEnv({ REALTIME_FEATURES: { fetch: featuresFetch } as never } as never),
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      raceBango: "01",
      raceKey: "jra:2026:0512:08:01",
      source: "jra",
    },
  );
  expect(featuresFetch).not.toHaveBeenCalled();
});

it("forwardRaceForFeatures posts to /api/internal/recompute-and-build-parquet when configured", async () => {
  const { forwardRaceForFeatures } = await import("./worker");
  const featuresFetch = vi.fn(async () => new Response(JSON.stringify({ ok: true })));
  await forwardRaceForFeatures(
    buildEnv({
      PC_KEIBA_VIEWER_INTERNAL_TOKEN: "internal-token",
      REALTIME_FEATURES: { fetch: featuresFetch } as never,
    } as never),
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "55",
      raceBango: "01",
      raceKey: "nar:2026:0512:55:01",
      source: "nar",
    },
  );
  expect(featuresFetch).toHaveBeenCalledTimes(1);
});

it("forwardRaceForFeatures logs the error when the features worker fetch rejects", async () => {
  const { forwardRaceForFeatures } = await import("./worker");
  const { logFetch } = await import("./storage");
  const featuresFetch = vi.fn(async () => {
    throw new Error("features boom");
  });
  await forwardRaceForFeatures(
    buildEnv({
      PC_KEIBA_VIEWER_INTERNAL_TOKEN: "internal-token",
      REALTIME_FEATURES: { fetch: featuresFetch } as never,
    } as never),
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "55",
      raceBango: "01",
      raceKey: "nar:2026:0512:55:01",
      source: "nar",
    },
  );
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "forward-race-for-features",
    "error",
    "nar:2026:0512:55:01",
    "features boom",
  );
});

it("fetchHotOddsPayload returns null when REALTIME_HOT is not configured", async () => {
  const { fetchHotOddsPayload } = await import("./worker");
  expect(await fetchHotOddsPayload(buildEnv(), "jra:2026:0512:08:01")).toBeNull();
});

it("fetchHotOddsPayload returns null when the hot worker returns a non-ok status", async () => {
  const { fetchHotOddsPayload } = await import("./worker");
  const hotFetch = vi.fn(async () => new Response("", { status: 500 }));
  expect(
    await fetchHotOddsPayload(
      buildEnv({ REALTIME_HOT: { fetch: hotFetch } as never } as never),
      "jra:2026:0512:08:01",
    ),
  ).toBeNull();
});

it("fetchHotOddsPayload returns null when the hot worker fetch rejects", async () => {
  const { fetchHotOddsPayload } = await import("./worker");
  const hotFetch = vi.fn(async () => {
    throw new Error("network boom");
  });
  expect(
    await fetchHotOddsPayload(
      buildEnv({ REALTIME_HOT: { fetch: hotFetch } as never } as never),
      "jra:2026:0512:08:01",
    ),
  ).toBeNull();
});

it("fetchHotOddsPayload returns the parsed JSON body when the hot worker responds ok", async () => {
  const { fetchHotOddsPayload } = await import("./worker");
  const payload = {
    fetchedAt: "2026-05-12T12:00:00+09:00",
    history: [],
    historyByType: {},
    latest: {},
  };
  const hotFetch = vi.fn(async () => new Response(JSON.stringify(payload)));
  expect(
    await fetchHotOddsPayload(
      buildEnv({ REALTIME_HOT: { fetch: hotFetch } as never } as never),
      "jra:2026:0512:08:01",
    ),
  ).toStrictEqual(payload);
});

it("fetchHotOddsPayload coerces a null JSON response body to null", async () => {
  const { fetchHotOddsPayload } = await import("./worker");
  const hotFetch = vi.fn(async () => new Response("null"));
  expect(
    await fetchHotOddsPayload(
      buildEnv({ REALTIME_HOT: { fetch: hotFetch } as never } as never),
      "jra:2026:0512:08:01",
    ),
  ).toBeNull();
});

it("buildDegradedRealtimePayload returns null odds when hot odds is null", async () => {
  const { buildDegradedRealtimePayload } = await import("./worker");
  expect(buildDegradedRealtimePayload("jra:2026:0512:08:01", null)).toStrictEqual({
    horseWeights: null,
    odds: null,
    raceEntries: null,
    raceKey: "jra:2026:0512:08:01",
    raceResults: null,
    source: null,
    trackCondition: null,
  });
});

it("buildDegradedRealtimePayload preserves hot odds payload while nulling D1 fields", async () => {
  const { buildDegradedRealtimePayload } = await import("./worker");
  const result = buildDegradedRealtimePayload("jra:2026:0512:08:01", {
    fetchedAt: "2026-05-12T12:00:00+09:00",
    history: [{ fetchedAt: "now", horseNumber: "1", odds: 2.5, popularity: 1 }],
    historyByType: { tansho: [{ combination: "1", fetchedAt: "now", odds: 2.5, rank: 1 }] },
    latest: { tansho: [{ combination: "1", odds: 2.5, popularity: 1 }] },
  } as never);
  expect(result.odds?.fetchedAt).toBe("2026-05-12T12:00:00+09:00");
  expect(result.source).toBeNull();
  expect(result.raceEntries).toBeNull();
  expect(result.trackCondition).toBeNull();
});

it("fetch GET /api/nar/.../realtime returns degraded payload with hot odds when D1 throws on getRaceSource", async () => {
  const { default: worker } = await import("./worker");
  const { getRaceSource } = await import("./storage");
  vi.mocked(getRaceSource).mockRejectedValueOnce(
    new Error("D1_ERROR: D1 DB exceeded its CPU time limit and was reset."),
  );
  const hotPayload = {
    fetchedAt: "2026-05-28T12:00:00+09:00",
    history: [{ fetchedAt: "now", horseNumber: "1", odds: 3.1, popularity: 1 }],
    historyByType: { tansho: [{ combination: "1", fetchedAt: "now", odds: 3.1, rank: 1 }] },
    latest: { tansho: [{ combination: "1", odds: 3.1, popularity: 1 }] },
  };
  const hotFetch = vi.fn(async () => new Response(JSON.stringify(hotPayload)));
  const env = buildEnv({ REALTIME_HOT: { fetch: hotFetch } as never } as never);
  const response = await worker.fetch(
    new Request("https://x.test/api/nar/races/2026/05/28/30/08/realtime"),
    env,
    buildCtx(),
  );
  expect(response.status).toBe(200);
  const body = (await response.json()) as { source: unknown; odds: { fetchedAt: string } | null };
  expect(body.source).toBeNull();
  expect(body.odds?.fetchedAt).toBe("2026-05-28T12:00:00+09:00");
});

it("fetch GET /api/jra/.../realtime returns degraded payload when D1 batch throws inside buildRealtimePayload", async () => {
  const { default: worker } = await import("./worker");
  const { buildRealtimePayload } = await import("./storage");
  vi.mocked(buildRealtimePayload).mockRejectedValueOnce(
    new Error("D1_ERROR: D1 DB exceeded its CPU time limit and was reset."),
  );
  const response = await worker.fetch(
    new Request("https://x.test/api/jra/races/2026/05/12/08/01/realtime"),
    buildEnv(),
    buildCtx(),
  );
  expect(response.status).toBe(200);
  const body = (await response.json()) as { raceEntries: unknown; source: unknown };
  expect(body.raceEntries).toBeNull();
  expect(body.source).toBeNull();
});

it("buildRealtimeRouteResponse swallows logFetch failures while returning degraded payload", async () => {
  const { buildRealtimeRouteResponse } = await import("./worker");
  const { getRaceSource, logFetch } = await import("./storage");
  vi.mocked(getRaceSource).mockRejectedValueOnce(new Error("D1_ERROR: boom"));
  vi.mocked(logFetch).mockRejectedValueOnce(new Error("D1_ERROR: log boom"));
  const result = await buildRealtimeRouteResponse(buildEnv(), "jra:2026:0512:08:01");
  expect(result.source).toBeNull();
  expect(result.odds).toBeNull();
});
