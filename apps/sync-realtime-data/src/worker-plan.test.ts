// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { Env, Job, NarRaceSource } from "./types";

const queueSendOk = async (): Promise<QueueSendResponse> => ({
  metadata: { metrics: { backlogCount: 0, backlogBytes: 0 } },
});

const queueMetricsOk = async (): Promise<QueueMetrics> => ({
  backlogCount: 0,
  backlogBytes: 0,
});

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
  claimResultCacheBust: vi.fn(async () => null),
  claimWeightFetch: vi.fn(async () => true),
  completeOddsFetch: vi.fn(async () => {}),
  failOddsFetch: vi.fn(async () => {}),
  completeResultFetch: vi.fn(async () => {}),
  completeResultCacheBust: vi.fn(async () => {}),
  recordPartialResultFetch: vi.fn(async () => {}),
  failResultFetch: vi.fn(async () => {}),
  incrementEmptyResultAttempts: vi.fn(async () => 0),
  markEmptyResultGiveUp: vi.fn(async () => {}),
  resetEmptyResultAttempts: vi.fn(async () => {}),
  insertOddsSnapshot: vi.fn(async () => 0),
  insertHorseWeightSnapshot: vi.fn(async () => {}),
  insertRaceEntrySnapshot: vi.fn(async () => 0),
  insertRaceResultSnapshot: vi.fn(async () => 0),
  listPendingResultCacheBustRaceKeys: vi.fn(async () => []),
  registerResultCacheBust: vi.fn(async () => {}),
  runD1Retention: vi.fn(async () => ({ fetchLogsDeleted: 0, oddsSnapshotsDeleted: 0 })),
  upsertPremiumRaceLink: vi.fn(async () => {}),
  getPremiumRaceLink: vi.fn(async () => null),
  replacePremiumRaceData: vi.fn(async () => {}),
  getPremiumRacePayload: vi.fn(async () => null),
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
  buildRealtimePayload: vi.fn(async () => ({}) as never),
}));
vi.mock("./daily-feature-build", () => ({
  DAILY_FEATURE_BUILD_CRON: "0 19 * * *",
  runDailyFeatureBuildForEnv: vi.fn(async () => ({})),
  listDailyRaceEntriesForRace: vi.fn(async () => []),
}));
vi.mock("./win5-queue", () => ({ handleWin5PredictionJob: vi.fn() }));
vi.mock("./win5-cron", () => ({
  WIN5_DISCOVER_CRON: "30 12 * * *",
  logWin5CronResult: vi.fn(async () => {}),
}));
vi.mock("./running-style-cron", () => ({
  RUNNING_STYLE_INFERENCE_CRON: "*/10 0-14 * * *",
  RUNNING_STYLE_PREWARM_CRON: "0 12 * * *",
  formatTomorrowYYYYMMDDInJst: vi.fn(() => "20260513"),
  formatYYYYMMDDInJst: vi.fn(() => "20260512"),
  planRunningStylePredictionsForDate: vi.fn(async () => ({})),
  refreshViewerRunningStyleCachesForDate: vi.fn(async () => ({})),
  refreshViewerRunningStyleCacheForRace: vi.fn(async () => false),
  runRunningStyleCronTick: vi.fn(async () => ({})),
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
    fetchJraResultHtmlWithFallback: vi.fn(async () => "<html></html>"),
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
    REALTIME_DB: buildDb(),
    REALTIME_JOBS: { send: vi.fn(async () => {}), sendBatch: vi.fn(async () => {}) },
    ...overrides,
  } as unknown as Env;
};

const buildWeightRetryRace = (overrides?: Partial<NarRaceSource>): NarRaceSource => ({
  babaCode: "08",
  debaUrl: "https://www.jra.go.jp/race",
  kaisaiKai: "02",
  kaisaiNen: "2026",
  kaisaiNichime: "06",
  kaisaiTsukihi: "0606",
  keibajoCode: "08",
  lastOddsFetchAt: null,
  lastWeightFetchAt: null,
  oddsLinks: {},
  raceBango: "10",
  raceKey: "jra:2026:0606:08:10",
  raceName: "Test",
  raceStartAtJst: "2026-06-06T12:30:00+09:00",
  source: "jra",
  ...overrides,
});

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("planTrackConditionFetchesForDate triggers ensureJraRaceSourcesAreCurrent upsert loop when JRA races exceed D1 count", async () => {
  const { planTrackConditionFetchesForDate } = await import("./worker");
  const { fetchJraRacesByDate } = await import("./postgres");
  const { upsertJraRaceSource } = await import("./storage");
  vi.mocked(fetchJraRacesByDate).mockResolvedValueOnce([
    {
      hasso_jikoku: "1500",
      kaisai_kai: "02",
      kaisai_nen: "2026",
      kaisai_nichime: "06",
      kaisai_tsukihi: "0512",
      keibajo_code: "08",
      kyosomei_hondai: "T",
      race_bango: "1",
    },
  ] as never);
  await planTrackConditionFetchesForDate(
    buildEnv(),
    "20260512",
    new Date("2026-05-12T03:00:00.000Z"),
  );
  expect(upsertJraRaceSource).toHaveBeenCalled();
});

it("planTrackConditionFetchesForDate emits a fetch-jra-track-condition job for due schedules", async () => {
  const { planTrackConditionFetchesForDate } = await import("./worker");
  const { listJraVenueTrackConditionSchedulesByDate } = await import("./storage");
  vi.mocked(listJraVenueTrackConditionSchedulesByDate).mockResolvedValueOnce([
    {
      firstRaceStartAtJst: "2026-05-12T13:00:00+09:00",
      keibajoCode: "08",
      lastFetchAt: null,
      lastQueuedAt: null,
      lastRaceStartAtJst: "2026-05-12T16:30:00+09:00",
    },
  ]);
  const result = await planTrackConditionFetchesForDate(
    buildEnv(),
    "20260512",
    new Date("2026-05-12T00:30:00Z"),
  );
  expect(result).toStrictEqual([
    { date: "20260512", keibajoCode: "08", type: "fetch-jra-track-condition" },
  ]);
});

it("planTrackConditionFetchesForDate returns empty array when no schedules", async () => {
  const { planTrackConditionFetchesForDate } = await import("./worker");
  const result = await planTrackConditionFetchesForDate(
    buildEnv(),
    "20260512",
    new Date("2026-05-12T03:00:00.000Z"),
  );
  expect(result).toStrictEqual([]);
});

it("planPremiumPaddockFetchesForDate returns empty array when premium config is absent", async () => {
  const { planPremiumPaddockFetchesForDate } = await import("./worker");
  const result = await planPremiumPaddockFetchesForDate(
    buildEnv(),
    "20260512",
    new Date("2026-05-12T03:00:00.000Z"),
  );
  expect(result).toStrictEqual([]);
});

it("planPremiumRaceDataFetchesForDate returns empty array when premium config is absent", async () => {
  const { planPremiumRaceDataFetchesForDate } = await import("./worker");
  const result = await planPremiumRaceDataFetchesForDate(
    buildEnv(),
    "20260512",
    new Date("2026-05-12T03:00:00.000Z"),
  );
  expect(result).toStrictEqual([]);
});

it("planRealtimeFetches returns total job count for empty inputs", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const env = buildEnv({
    REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z",
  });
  const count = await planRealtimeFetches(env, "20260512");
  expect(typeof count).toBe("number");
});

it("planRealtimeFetches returns 0 and skips D1 reads outside the JST polling window", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  // 2026-05-12T14:30:00Z = JST 23:30, outside the 06-22 polling window.
  const env = buildEnv({
    REALTIME_TEST_NOW: "2026-05-12T14:30:00.000Z",
  });
  const count = await planRealtimeFetches(env, "20260512");
  expect(count).toBe(0);
  expect(listSchedulableRaceSourcesByDate).not.toHaveBeenCalled();
});

it("planRealtimeFetches returns 0 before the JST polling window starts", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  // 2026-05-11T20:30:00Z = JST 05:30, before 06:00 polling window.
  const env = buildEnv({
    REALTIME_TEST_NOW: "2026-05-11T20:30:00.000Z",
  });
  const count = await planRealtimeFetches(env, "20260512");
  expect(count).toBe(0);
  expect(listSchedulableRaceSourcesByDate).not.toHaveBeenCalled();
});

it("planRealtimeFetches enqueues a discover-premium-races job at JST 20:00", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const env = buildEnv({
    REALTIME_TEST_NOW: "2026-05-12T11:00:00.000Z",
  });
  const sendBatch = vi.fn(async () => {});
  const send = vi.fn(async () => {});
  env.REALTIME_JOBS = { send, sendBatch } as never;
  const count = await planRealtimeFetches(env, "20260512");
  expect(count).toBeGreaterThanOrEqual(2);
});

it("planPremiumPaddockFetchesForDate enqueues fetch-premium-paddock for in-window JRA races", async () => {
  const { planPremiumPaddockFetchesForDate } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "08",
      debaUrl: "https://www.jra.go.jp/race",
      discoveredAt: "2026-05-12T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "01",
      raceKey: "jra:2026:0512:08:01",
      raceName: "Test",
      raceStartAtJst: "2026-05-12T13:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-12T00:00:00+09:00",
    },
  ] as never);
  const env = buildEnv({ PREMIUM_RACE_ORIGIN: "https://x.test" } as never);
  const result = await planPremiumPaddockFetchesForDate(
    env,
    "20260512",
    new Date("2026-05-12T03:40:00.000Z"),
  );
  expect(result.length).toBe(1);
  expect(result[0]!.type).toBe("fetch-premium-paddock");
});

const buildJraRace = (overrides?: Record<string, unknown>) =>
  ({
    babaCode: "08",
    debaUrl: "https://www.jra.go.jp/race",
    discoveredAt: "2026-05-12T00:00:00+09:00",
    kaisaiKai: "02",
    kaisaiNen: "2026",
    kaisaiNichime: "06",
    kaisaiTsukihi: "0512",
    keibajoCode: "08",
    lastOddsFetchAt: null,
    lastOddsQueuedAt: null,
    lastResultFetchAt: null,
    lastResultQueuedAt: null,
    lastWeightFetchAt: null,
    oddsFetchLockUntil: null,
    oddsLinks: {},
    raceBango: "01",
    raceKey: "jra:2026:0512:08:01",
    raceName: "T",
    raceStartAtJst: "2026-05-12T15:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "jra",
    updatedAt: "2026-05-12T00:00:00+09:00",
    ...overrides,
  }) as never;

it("planPremiumPaddockFetchesForDate skips races outside the in-window range (too far in future)", async () => {
  const { planPremiumPaddockFetchesForDate } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, getPremiumPaddockFetchState } =
    await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockReset();
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([buildJraRace()]);
  vi.mocked(getPremiumPaddockFetchState).mockReset();
  vi.mocked(getPremiumPaddockFetchState).mockResolvedValue(null);
  const result = await planPremiumPaddockFetchesForDate(
    buildEnv({ PREMIUM_RACE_ORIGIN: "https://x.test" }),
    "20260512",
    new Date("2026-05-11T01:00:00.000Z"),
  );
  expect(result).toStrictEqual([]);
});

it("planPremiumPaddockFetchesForDate skips races whose start time is in the far past", async () => {
  const { planPremiumPaddockFetchesForDate } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, getPremiumPaddockFetchState } =
    await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockReset();
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([buildJraRace()]);
  vi.mocked(getPremiumPaddockFetchState).mockReset();
  vi.mocked(getPremiumPaddockFetchState).mockResolvedValue(null);
  const result = await planPremiumPaddockFetchesForDate(
    buildEnv({ PREMIUM_RACE_ORIGIN: "https://x.test" }),
    "20260512",
    new Date("2026-05-13T10:00:00.000Z"),
  );
  expect(result).toStrictEqual([]);
});

it("planPremiumPaddockFetchesForDate skips races whose state has future retryAfter", async () => {
  const { planPremiumPaddockFetchesForDate } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, getPremiumPaddockFetchState } =
    await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockReset();
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([buildJraRace()]);
  vi.mocked(getPremiumPaddockFetchState).mockReset();
  vi.mocked(getPremiumPaddockFetchState).mockResolvedValue({
    raceKey: "jra:2026:0512:08:01",
    retryAfter: "2099-01-01T00:00:00.000Z",
    status: "failed",
  } as never);
  const result = await planPremiumPaddockFetchesForDate(
    buildEnv({ PREMIUM_RACE_ORIGIN: "https://x.test" }),
    "20260512",
    new Date("2026-05-12T05:55:00.000Z"),
  );
  expect(result).toStrictEqual([]);
});

it("planPremiumPaddockFetchesForDate skips NAR races", async () => {
  const { planPremiumPaddockFetchesForDate } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "22",
      debaUrl: "https://x.test/race",
      discoveredAt: "2026-05-12T00:00:00+09:00",
      kaisaiKai: null,
      kaisaiNen: "2026",
      kaisaiNichime: null,
      kaisaiTsukihi: "0512",
      keibajoCode: "55",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "01",
      raceKey: "nar:2026:0512:55:01",
      raceName: "Test",
      raceStartAtJst: "2026-05-12T13:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "nar",
      updatedAt: "2026-05-12T00:00:00+09:00",
    },
  ] as never);
  const env = buildEnv({ PREMIUM_RACE_ORIGIN: "https://x.test" } as never);
  const result = await planPremiumPaddockFetchesForDate(
    env,
    "20260512",
    new Date("2026-05-12T03:40:00.000Z"),
  );
  expect(result).toStrictEqual([]);
});

it("planPremiumPaddockFetchesForDate skips when recent lastQueuedAt exists", async () => {
  const { planPremiumPaddockFetchesForDate } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, getPremiumPaddockFetchState } =
    await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "08",
      debaUrl: "https://www.jra.go.jp/race",
      discoveredAt: "2026-05-12T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "01",
      raceKey: "jra:2026:0512:08:01",
      raceName: "Test",
      raceStartAtJst: "2026-05-12T13:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-12T00:00:00+09:00",
    },
  ] as never);
  vi.mocked(getPremiumPaddockFetchState).mockResolvedValue({
    lastQueuedAt: "2026-05-12T03:39:30.000Z",
    raceKey: "jra:2026:0512:08:01",
    status: "ok",
  } as never);
  const env = buildEnv({ PREMIUM_RACE_ORIGIN: "https://x.test" } as never);
  const result = await planPremiumPaddockFetchesForDate(
    env,
    "20260512",
    new Date("2026-05-12T03:40:00.000Z"),
  );
  expect(result).toStrictEqual([]);
});

it("planPremiumPaddockFetchesForDate enqueues races 110 minutes before start (inside expanded 120-min window)", async () => {
  const { planPremiumPaddockFetchesForDate } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, getPremiumPaddockFetchState } =
    await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "08",
      debaUrl: "https://www.jra.go.jp/race",
      discoveredAt: "2026-05-12T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "01",
      raceKey: "jra:2026:0512:08:01",
      raceName: "Test",
      raceStartAtJst: "2026-05-12T13:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-12T00:00:00+09:00",
    },
  ] as never);
  vi.mocked(getPremiumPaddockFetchState).mockResolvedValue(null);
  const env = buildEnv({ PREMIUM_RACE_ORIGIN: "https://x.test" } as never);
  const result = await planPremiumPaddockFetchesForDate(
    env,
    "20260512",
    new Date("2026-05-12T02:10:00.000Z"),
  );
  expect(result).toStrictEqual([{ raceKey: "jra:2026:0512:08:01", type: "fetch-premium-paddock" }]);
});

it("planPremiumPaddockFetchesForDate skips races 130 minutes before start (outside 120-min window)", async () => {
  const { planPremiumPaddockFetchesForDate } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, getPremiumPaddockFetchState } =
    await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "08",
      debaUrl: "https://www.jra.go.jp/race",
      discoveredAt: "2026-05-12T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "01",
      raceKey: "jra:2026:0512:08:01",
      raceName: "Test",
      raceStartAtJst: "2026-05-12T13:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-12T00:00:00+09:00",
    },
  ] as never);
  vi.mocked(getPremiumPaddockFetchState).mockResolvedValue(null);
  const env = buildEnv({ PREMIUM_RACE_ORIGIN: "https://x.test" } as never);
  const result = await planPremiumPaddockFetchesForDate(
    env,
    "20260512",
    new Date("2026-05-12T01:50:00.000Z"),
  );
  expect(result).toStrictEqual([]);
});

it("planPremiumPaddockFetchesForDate enqueues when lastQueuedAt is 90 seconds ago (outside 1-minute recheck gate)", async () => {
  const { planPremiumPaddockFetchesForDate } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, getPremiumPaddockFetchState } =
    await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "08",
      debaUrl: "https://www.jra.go.jp/race",
      discoveredAt: "2026-05-12T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "01",
      raceKey: "jra:2026:0512:08:01",
      raceName: "Test",
      raceStartAtJst: "2026-05-12T13:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-12T00:00:00+09:00",
    },
  ] as never);
  vi.mocked(getPremiumPaddockFetchState).mockResolvedValue({
    lastQueuedAt: "2026-05-12T03:38:30.000Z",
    raceKey: "jra:2026:0512:08:01",
    status: "ok",
  } as never);
  const env = buildEnv({ PREMIUM_RACE_ORIGIN: "https://x.test" } as never);
  const result = await planPremiumPaddockFetchesForDate(
    env,
    "20260512",
    new Date("2026-05-12T03:40:00.000Z"),
  );
  expect(result).toStrictEqual([{ raceKey: "jra:2026:0512:08:01", type: "fetch-premium-paddock" }]);
});

it("planPremiumPaddockFetchesOnly returns 0 outside the JST polling window", async () => {
  const { planPremiumPaddockFetchesOnly } = await import("./worker");
  // 2026-05-12T14:30:00Z = JST 23:30, outside the 06-22 polling window.
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-12T14:30:00.000Z" } as never);
  const count = await planPremiumPaddockFetchesOnly(env, "20260512");
  expect(count).toBe(0);
});

it("planPremiumPaddockFetchesOnly returns 0 when no premium config is configured", async () => {
  const { planPremiumPaddockFetchesOnly } = await import("./worker");
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-12T03:40:00.000Z" } as never);
  const count = await planPremiumPaddockFetchesOnly(env, "20260512");
  expect(count).toBe(0);
});

it("planPremiumPaddockFetchesOnly enqueues a fetch-premium-paddock job for a race inside the window", async () => {
  const { planPremiumPaddockFetchesOnly } = await import("./worker");
  const {
    listSchedulableRaceSourcesByDate,
    getPremiumPaddockFetchState,
    markPremiumPaddockQueued,
  } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate)
    .mockResolvedValueOnce([
      {
        babaCode: "08",
        debaUrl: "https://www.jra.go.jp/race",
        discoveredAt: "2026-05-12T00:00:00+09:00",
        kaisaiKai: "02",
        kaisaiNen: "2026",
        kaisaiNichime: "06",
        kaisaiTsukihi: "0512",
        keibajoCode: "08",
        lastOddsFetchAt: null,
        lastOddsQueuedAt: null,
        lastResultFetchAt: null,
        lastResultQueuedAt: null,
        lastWeightFetchAt: null,
        oddsFetchLockUntil: null,
        oddsLinks: {},
        raceBango: "01",
        raceKey: "jra:2026:0512:08:01",
        raceName: "Test",
        raceStartAtJst: "2026-05-12T13:00:00+09:00",
        resultCompleteAt: null,
        resultExpectedHorseCount: null,
        resultFetchLockUntil: null,
        resultSavedHorseCount: null,
        source: "jra",
        updatedAt: "2026-05-12T00:00:00+09:00",
      },
    ] as never)
    .mockResolvedValueOnce([] as never);
  vi.mocked(getPremiumPaddockFetchState).mockResolvedValue(null);
  const env = buildEnv({
    PREMIUM_RACE_ORIGIN: "https://x.test",
    REALTIME_TEST_NOW: "2026-05-12T03:40:00.000Z",
  } as never);
  const count = await planPremiumPaddockFetchesOnly(env, "20260512");
  expect(count).toBe(1);
  expect(markPremiumPaddockQueued).toHaveBeenCalled();
});

it("weightFetchPriorityTier returns 0 for Tokyo 5R", async () => {
  const { weightFetchPriorityTier } = await import("./worker");
  expect(weightFetchPriorityTier({ source: "jra", keibajoCode: "05", raceBango: "05" })).toBe(0);
});

it("weightFetchPriorityTier returns 0 for Kyoto 5R", async () => {
  const { weightFetchPriorityTier } = await import("./worker");
  expect(weightFetchPriorityTier({ source: "jra", keibajoCode: "08", raceBango: "05" })).toBe(0);
});

it("weightFetchPriorityTier returns 0 for Tokyo 11R", async () => {
  const { weightFetchPriorityTier } = await import("./worker");
  expect(weightFetchPriorityTier({ source: "jra", keibajoCode: "05", raceBango: "11" })).toBe(0);
});

it("weightFetchPriorityTier returns 0 for Kyoto 11R", async () => {
  const { weightFetchPriorityTier } = await import("./worker");
  expect(weightFetchPriorityTier({ source: "jra", keibajoCode: "08", raceBango: "11" })).toBe(0);
});

it("weightFetchPriorityTier returns 1 for Nakayama 5R (other-venue 5R is Mid)", async () => {
  const { weightFetchPriorityTier } = await import("./worker");
  expect(weightFetchPriorityTier({ source: "jra", keibajoCode: "06", raceBango: "05" })).toBe(1);
});

it("weightFetchPriorityTier returns 1 for Nakayama 11R (other-venue 11R is Mid)", async () => {
  const { weightFetchPriorityTier } = await import("./worker");
  expect(weightFetchPriorityTier({ source: "jra", keibajoCode: "06", raceBango: "11" })).toBe(1);
});

it("weightFetchPriorityTier returns 1 for Tokyo 6R", async () => {
  const { weightFetchPriorityTier } = await import("./worker");
  expect(weightFetchPriorityTier({ source: "jra", keibajoCode: "05", raceBango: "06" })).toBe(1);
});

it("weightFetchPriorityTier returns 1 for Tokyo 12R", async () => {
  const { weightFetchPriorityTier } = await import("./worker");
  expect(weightFetchPriorityTier({ source: "jra", keibajoCode: "05", raceBango: "12" })).toBe(1);
});

it("weightFetchPriorityTier returns 2 for Tokyo 1R", async () => {
  const { weightFetchPriorityTier } = await import("./worker");
  expect(weightFetchPriorityTier({ source: "jra", keibajoCode: "05", raceBango: "01" })).toBe(2);
});

it("weightFetchPriorityTier returns 2 for Tokyo 4R", async () => {
  const { weightFetchPriorityTier } = await import("./worker");
  expect(weightFetchPriorityTier({ source: "jra", keibajoCode: "05", raceBango: "04" })).toBe(2);
});

it("weightFetchPriorityTier returns 3 for NAR races", async () => {
  const { weightFetchPriorityTier } = await import("./worker");
  expect(weightFetchPriorityTier({ source: "nar", keibajoCode: "47", raceBango: "05" })).toBe(3);
});

it("compareWeightCandidates places high tier before mid tier regardless of input order", async () => {
  const { compareWeightCandidates } = await import("./worker");
  const tokyoFiveR = {
    race: { source: "jra", keibajoCode: "05", raceBango: "05" },
    minutes: 35,
  } as never;
  const kyotoTwelveR = {
    race: { source: "jra", keibajoCode: "08", raceBango: "12" },
    minutes: 10,
  } as never;
  const sorted = [kyotoTwelveR, tokyoFiveR].sort(compareWeightCandidates);
  expect(sorted[0]).toBe(tokyoFiveR);
  expect(sorted[1]).toBe(kyotoTwelveR);
});

it("compareWeightCandidates sorts same tier by minutes ascending", async () => {
  const { compareWeightCandidates } = await import("./worker");
  const nearer = {
    race: { source: "jra", keibajoCode: "06", raceBango: "07" },
    minutes: 15,
  } as never;
  const farther = {
    race: { source: "jra", keibajoCode: "06", raceBango: "08" },
    minutes: 35,
  } as never;
  const sorted = [farther, nearer].sort(compareWeightCandidates);
  expect(sorted[0]).toBe(nearer);
  expect(sorted[1]).toBe(farther);
});

it("planRealtimeFetches never enqueues JRA weight jobs because the watchdog owns scheduling", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "05",
      debaUrl: "https://www.jra.go.jp/race/tokyo1",
      discoveredAt: "2026-05-30T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0530",
      keibajoCode: "05",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "01",
      raceKey: "jra:2026:0530:05:01",
      raceName: "Tokyo1R",
      raceStartAtJst: "2026-05-30T12:30:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-30T00:00:00+09:00",
    },
    {
      babaCode: "08",
      debaUrl: "https://www.jra.go.jp/race/kyoto5",
      discoveredAt: "2026-05-30T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0530",
      keibajoCode: "08",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "05",
      raceKey: "jra:2026:0530:08:05",
      raceName: "Kyoto5R",
      raceStartAtJst: "2026-05-30T12:35:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-30T00:00:00+09:00",
    },
    {
      babaCode: "05",
      debaUrl: "https://www.jra.go.jp/race/tokyo5",
      discoveredAt: "2026-05-30T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0530",
      keibajoCode: "05",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "05",
      raceKey: "jra:2026:0530:05:05",
      raceName: "Tokyo5R",
      raceStartAtJst: "2026-05-30T12:25:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-30T00:00:00+09:00",
    },
    {
      babaCode: "05",
      debaUrl: "https://www.jra.go.jp/race/tokyo11",
      discoveredAt: "2026-05-30T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0530",
      keibajoCode: "05",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "11",
      raceKey: "jra:2026:0530:05:11",
      raceName: "Tokyo11R",
      raceStartAtJst: "2026-05-30T12:45:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-30T00:00:00+09:00",
    },
    {
      babaCode: "06",
      debaUrl: "https://www.jra.go.jp/race/nakayama6",
      discoveredAt: "2026-05-30T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0530",
      keibajoCode: "06",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "06",
      raceKey: "jra:2026:0530:06:06",
      raceName: "Nakayama6R",
      raceStartAtJst: "2026-05-30T12:38:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-30T00:00:00+09:00",
    },
    {
      babaCode: "09",
      debaUrl: "https://www.jra.go.jp/race/hanshin12",
      discoveredAt: "2026-05-30T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0530",
      keibajoCode: "09",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "12",
      raceKey: "jra:2026:0530:09:12",
      raceName: "Hanshin12R",
      raceStartAtJst: "2026-05-30T12:40:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-30T00:00:00+09:00",
    },
    {
      babaCode: "08",
      debaUrl: "https://www.jra.go.jp/race/kyoto4",
      discoveredAt: "2026-05-30T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0530",
      keibajoCode: "08",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "04",
      raceKey: "jra:2026:0530:08:04",
      raceName: "Kyoto4R",
      raceStartAtJst: "2026-05-30T12:28:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-30T00:00:00+09:00",
    },
  ] as never);
  const sendBatch = vi.fn(async () => {});
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_TEST_NOW: "2026-05-30T03:00:00.000Z",
  });
  env.REALTIME_JOBS = { send, sendBatch } as never;
  await planRealtimeFetches(env, "20260530");
  const batched =
    (
      sendBatch.mock.calls as unknown as [{ body: { raceKey: string; type: string } }[]][]
    )[0]?.[0] ?? [];
  const weightRaceKeys = batched
    .filter((m) => m.body.type === "fetch-weights")
    .map((m) => m.body.raceKey);
  expect(weightRaceKeys).toStrictEqual([]);
});

it("planRealtimeFetches leaves a race 35 minutes before post to the weight watchdog", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "05",
      debaUrl: "https://www.jra.go.jp/race/lead35",
      discoveredAt: "2026-05-30T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0530",
      keibajoCode: "05",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "07",
      raceKey: "jra:2026:0530:05:07",
      raceName: "Lead35",
      raceStartAtJst: "2026-05-30T12:35:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-30T00:00:00+09:00",
    },
  ] as never);
  const sendBatch = vi.fn(async () => {});
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_TEST_NOW: "2026-05-30T03:00:00.000Z",
  });
  env.REALTIME_JOBS = { send, sendBatch } as never;
  await planRealtimeFetches(env, "20260530");
  const sentSingle = (send.mock.calls as unknown as [{ raceKey: string; type: string }][]).map(
    (c) => c[0],
  );
  const sentBatched =
    (
      sendBatch.mock.calls as unknown as [{ body: { raceKey: string; type: string } }[]][]
    )[0]?.[0] ?? [];
  const allWeightKeys = [
    ...sentSingle.filter((j) => j.type === "fetch-weights").map((j) => j.raceKey),
    ...sentBatched.filter((m) => m.body.type === "fetch-weights").map((m) => m.body.raceKey),
  ];
  expect(allWeightKeys).toStrictEqual([]);
});

it("planRealtimeFetches leaves a race 85 minutes before post to the weight watchdog", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "05",
      debaUrl: "https://www.jra.go.jp/race/lead85",
      discoveredAt: "2026-05-30T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0530",
      keibajoCode: "05",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "07",
      raceKey: "jra:2026:0530:05:07",
      raceName: "Lead85",
      raceStartAtJst: "2026-05-30T13:25:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-30T00:00:00+09:00",
    },
  ] as never);
  const sendBatch = vi.fn(async () => {});
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_TEST_NOW: "2026-05-30T03:00:00.000Z",
  });
  env.REALTIME_JOBS = { send, sendBatch } as never;
  await planRealtimeFetches(env, "20260530");
  const sentSingle = (send.mock.calls as unknown as [{ raceKey: string; type: string }][]).map(
    (c) => c[0],
  );
  const sentBatched =
    (
      sendBatch.mock.calls as unknown as [{ body: { raceKey: string; type: string } }[]][]
    )[0]?.[0] ?? [];
  const allWeightKeys = [
    ...sentSingle.filter((j) => j.type === "fetch-weights").map((j) => j.raceKey),
    ...sentBatched.filter((m) => m.body.type === "fetch-weights").map((m) => m.body.raceKey),
  ];
  expect(allWeightKeys).toStrictEqual([]);
});

it("planRealtimeFetches excludes weight job when race is 185 minutes before post (beyond 180-min lead)", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "05",
      debaUrl: "https://www.jra.go.jp/race/lead185",
      discoveredAt: "2026-05-30T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0530",
      keibajoCode: "05",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "07",
      raceKey: "jra:2026:0530:05:07",
      raceName: "Lead185",
      raceStartAtJst: "2026-05-30T15:05:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-30T00:00:00+09:00",
    },
  ] as never);
  const sendBatch = vi.fn(async () => {});
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_TEST_NOW: "2026-05-30T03:00:00.000Z",
  });
  env.REALTIME_JOBS = { send, sendBatch } as never;
  await planRealtimeFetches(env, "20260530");
  const sentSingle = (send.mock.calls as unknown as [{ type: string }][]).map((c) => c[0]);
  const sentBatched =
    (sendBatch.mock.calls as unknown as [{ body: { type: string } }[]][])[0]?.[0] ?? [];
  const weightCount =
    sentSingle.filter((j) => j.type === "fetch-weights").length +
    sentBatched.filter((m) => m.body.type === "fetch-weights").length;
  expect(weightCount).toBe(0);
});

it("planRealtimeFetches leaves a race 175 minutes before post to the weight watchdog", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "05",
      debaUrl: "https://www.jra.go.jp/race/lead175",
      discoveredAt: "2026-05-30T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0530",
      keibajoCode: "05",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "07",
      raceKey: "jra:2026:0530:05:07",
      raceName: "Lead175",
      raceStartAtJst: "2026-05-30T14:55:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-30T00:00:00+09:00",
    },
  ] as never);
  const sendBatch = vi.fn(async () => {});
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_TEST_NOW: "2026-05-30T03:00:00.000Z",
  });
  env.REALTIME_JOBS = { send, sendBatch } as never;
  await planRealtimeFetches(env, "20260530");
  const sentSingle = (send.mock.calls as unknown as [{ raceKey: string; type: string }][]).map(
    (c) => c[0],
  );
  const sentBatched =
    (
      sendBatch.mock.calls as unknown as [{ body: { raceKey: string; type: string } }[]][]
    )[0]?.[0] ?? [];
  const allWeightKeys = [
    ...sentSingle.filter((j) => j.type === "fetch-weights").map((j) => j.raceKey),
    ...sentBatched.filter((m) => m.body.type === "fetch-weights").map((m) => m.body.raceKey),
  ];
  expect(allWeightKeys).toStrictEqual([]);
});

it("planRealtimeFetches excludes weight job when same-day lastWeightFetchAt is within 60min cooldown", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "05",
      debaUrl: "https://www.jra.go.jp/race/sameday30min",
      discoveredAt: "2026-05-30T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0530",
      keibajoCode: "05",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: "2026-05-30T11:30:00+09:00",
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "07",
      raceKey: "jra:2026:0530:05:07",
      raceName: "SameDay30min",
      raceStartAtJst: "2026-05-30T12:30:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-30T00:00:00+09:00",
    },
  ] as never);
  const sendBatch = vi.fn(async () => {});
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_TEST_NOW: "2026-05-30T03:00:00.000Z",
  });
  env.REALTIME_JOBS = { send, sendBatch } as never;
  await planRealtimeFetches(env, "20260530");
  const sentSingle = (send.mock.calls as unknown as [{ type: string }][]).map((c) => c[0]);
  const sentBatched =
    (sendBatch.mock.calls as unknown as [{ body: { type: string } }[]][])[0]?.[0] ?? [];
  const weightCount =
    sentSingle.filter((j) => j.type === "fetch-weights").length +
    sentBatched.filter((m) => m.body.type === "fetch-weights").length;
  expect(weightCount).toBe(0);
});

it("planRealtimeFetches does not reschedule an old same-day weight fetch", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "05",
      debaUrl: "https://www.jra.go.jp/race/sameday2h",
      discoveredAt: "2026-05-30T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0530",
      keibajoCode: "05",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: "2026-05-30T10:00:00+09:00",
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "07",
      raceKey: "jra:2026:0530:05:07",
      raceName: "SameDay2h",
      raceStartAtJst: "2026-05-30T12:30:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-30T00:00:00+09:00",
    },
  ] as never);
  const sendBatch = vi.fn(async () => {});
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_TEST_NOW: "2026-05-30T03:00:00.000Z",
  });
  env.REALTIME_JOBS = { send, sendBatch } as never;
  await planRealtimeFetches(env, "20260530");
  const sentSingle = (send.mock.calls as unknown as [{ raceKey: string; type: string }][]).map(
    (c) => c[0],
  );
  const sentBatched =
    (
      sendBatch.mock.calls as unknown as [{ body: { raceKey: string; type: string } }[]][]
    )[0]?.[0] ?? [];
  const allWeightKeys = [
    ...sentSingle.filter((j) => j.type === "fetch-weights").map((j) => j.raceKey),
    ...sentBatched.filter((m) => m.body.type === "fetch-weights").map((m) => m.body.raceKey),
  ];
  expect(allWeightKeys).toStrictEqual([]);
});

it("planRealtimeFetches excludes weight job when previous-day lastWeightFetchAt is within 24h cooldown", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "05",
      debaUrl: "https://www.jra.go.jp/race/prevday12h",
      discoveredAt: "2026-05-30T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0530",
      keibajoCode: "05",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: "2026-05-29T20:00:00+09:00",
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "07",
      raceKey: "jra:2026:0530:05:07",
      raceName: "PrevDay12h",
      raceStartAtJst: "2026-05-30T12:30:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-30T00:00:00+09:00",
    },
  ] as never);
  const sendBatch = vi.fn(async () => {});
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_TEST_NOW: "2026-05-30T03:00:00.000Z",
  });
  env.REALTIME_JOBS = { send, sendBatch } as never;
  await planRealtimeFetches(env, "20260530");
  const sentSingle = (send.mock.calls as unknown as [{ type: string }][]).map((c) => c[0]);
  const sentBatched =
    (sendBatch.mock.calls as unknown as [{ body: { type: string } }[]][])[0]?.[0] ?? [];
  const weightCount =
    sentSingle.filter((j) => j.type === "fetch-weights").length +
    sentBatched.filter((m) => m.body.type === "fetch-weights").length;
  expect(weightCount).toBe(0);
});

it("planRealtimeFetches leaves both JRA and NAR weight jobs to the watchdog", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "22",
      debaUrl: "https://nar.example/race",
      discoveredAt: "2026-05-30T00:00:00+09:00",
      kaisaiKai: null,
      kaisaiNen: "2026",
      kaisaiNichime: null,
      kaisaiTsukihi: "0530",
      keibajoCode: "55",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "05",
      raceKey: "nar:2026:0530:55:05",
      raceName: "NarRace",
      raceStartAtJst: "2026-05-30T12:25:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "nar",
      updatedAt: "2026-05-30T00:00:00+09:00",
    },
    {
      babaCode: "05",
      debaUrl: "https://www.jra.go.jp/race/tokyo5",
      discoveredAt: "2026-05-30T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0530",
      keibajoCode: "05",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "05",
      raceKey: "jra:2026:0530:05:05",
      raceName: "Tokyo5R",
      raceStartAtJst: "2026-05-30T12:35:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-30T00:00:00+09:00",
    },
  ] as never);
  const sendBatch = vi.fn(async () => {});
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_TEST_NOW: "2026-05-30T03:00:00.000Z",
  });
  env.REALTIME_JOBS = { send, sendBatch } as never;
  await planRealtimeFetches(env, "20260530");
  const batched =
    (
      sendBatch.mock.calls as unknown as [{ body: { raceKey: string; type: string } }[]][]
    )[0]?.[0] ?? [];
  const weightRaceKeys = batched
    .filter((m) => m.body.type === "fetch-weights")
    .map((m) => m.body.raceKey);
  expect(weightRaceKeys).toStrictEqual([]);
});

it("planRealtimeFetches does not enqueue weight jobs on a non-three-minute tick", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "05",
      debaUrl: "https://www.jra.go.jp/race/anytick",
      discoveredAt: "2026-05-30T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0530",
      keibajoCode: "05",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "05",
      raceKey: "jra:2026:0530:05:05",
      raceName: "Tokyo5R",
      raceStartAtJst: "2026-05-30T12:25:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-30T00:00:00+09:00",
    },
  ] as never);
  const sendBatch = vi.fn(async () => {});
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_TEST_NOW: "2026-05-30T03:01:00.000Z",
  });
  env.REALTIME_JOBS = { send, sendBatch } as never;
  await planRealtimeFetches(env, "20260530");
  const sentSingle = (send.mock.calls as unknown as [{ raceKey: string; type: string }][]).map(
    (c) => c[0],
  );
  const sentBatched =
    (
      sendBatch.mock.calls as unknown as [{ body: { raceKey: string; type: string } }[]][]
    )[0]?.[0] ?? [];
  const allWeightKeys = [
    ...sentSingle.filter((j) => j.type === "fetch-weights").map((j) => j.raceKey),
    ...sentBatched.filter((m) => m.body.type === "fetch-weights").map((m) => m.body.raceKey),
  ];
  expect(allWeightKeys).toStrictEqual([]);
});

it("planRealtimeFetches enqueues fetch-weights and fetch-results for races near start time", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "22",
      debaUrl: "https://nar.example/race",
      discoveredAt: "2026-05-12T00:00:00+09:00",
      kaisaiKai: null,
      kaisaiNen: "2026",
      kaisaiNichime: null,
      kaisaiTsukihi: "0512",
      keibajoCode: "55",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "01",
      raceKey: "nar:2026:0512:55:01",
      raceName: "NearStart",
      raceStartAtJst: "2026-05-12T11:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "nar",
      updatedAt: "2026-05-12T00:00:00+09:00",
    },
    {
      babaCode: "22",
      debaUrl: "https://nar.example/race2",
      discoveredAt: "2026-05-12T00:00:00+09:00",
      kaisaiKai: null,
      kaisaiNen: "2026",
      kaisaiNichime: null,
      kaisaiTsukihi: "0512",
      keibajoCode: "55",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "02",
      raceKey: "nar:2026:0512:55:02",
      raceName: "Finished",
      raceStartAtJst: "2026-05-12T10:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "nar",
      updatedAt: "2026-05-12T00:00:00+09:00",
    },
  ] as never);
  const env = buildEnv({
    REALTIME_TEST_NOW: "2026-05-12T01:48:00.000Z",
  });
  const count = await planRealtimeFetches(env, "20260512");
  expect(count).toBeGreaterThan(0);
});

it("resolveWeightFetchCooldownMinutes returns 24h when lastFetchAt is null", async () => {
  const { resolveWeightFetchCooldownMinutes } = await import("./worker");
  expect(
    resolveWeightFetchCooldownMinutes({
      lastFetchAt: null,
      raceStartAtJst: "2026-06-06T12:30:00+09:00",
    }),
  ).toBe(1440);
});

it("resolveWeightFetchCooldownMinutes returns 60min when lastFetchAt is the same JST date", async () => {
  const { resolveWeightFetchCooldownMinutes } = await import("./worker");
  expect(
    resolveWeightFetchCooldownMinutes({
      lastFetchAt: "2026-06-06T10:00:00+09:00",
      raceStartAtJst: "2026-06-06T12:30:00+09:00",
    }),
  ).toBe(60);
});

it("resolveWeightFetchCooldownMinutes returns 24h when lastFetchAt is a previous JST date", async () => {
  const { resolveWeightFetchCooldownMinutes } = await import("./worker");
  expect(
    resolveWeightFetchCooldownMinutes({
      lastFetchAt: "2026-06-05T22:00:00+09:00",
      raceStartAtJst: "2026-06-06T12:30:00+09:00",
    }),
  ).toBe(1440);
});

it("resolveWeightFetchCooldownMinutes falls back to 24h when lastFetchAt is not parseable", async () => {
  const { resolveWeightFetchCooldownMinutes } = await import("./worker");
  expect(
    resolveWeightFetchCooldownMinutes({
      lastFetchAt: "not-a-date",
      raceStartAtJst: "2026-06-06T12:30:00+09:00",
    }),
  ).toBe(1440);
});

it("resolveWeightFetchCooldownMinutes returns 10min near-race override when now is within threshold", async () => {
  const { resolveWeightFetchCooldownMinutes } = await import("./worker");
  expect(
    resolveWeightFetchCooldownMinutes({
      lastFetchAt: "2026-06-06T12:15:00+09:00",
      now: new Date("2026-06-06T12:10:00+09:00"),
      raceStartAtJst: "2026-06-06T12:30:00+09:00",
    }),
  ).toBe(10);
});

it("resolveWeightFetchCooldownMinutes falls back to same-day cooldown when race is beyond near-race window", async () => {
  const { resolveWeightFetchCooldownMinutes } = await import("./worker");
  expect(
    resolveWeightFetchCooldownMinutes({
      lastFetchAt: "2026-06-06T08:00:00+09:00",
      now: new Date("2026-06-06T08:00:00+09:00"),
      raceStartAtJst: "2026-06-06T12:30:00+09:00",
    }),
  ).toBe(60);
});

it("resolveWeightFetchCooldownMinutes falls back to 24h when race start at jst cannot be parsed for near-race check", async () => {
  const { resolveWeightFetchCooldownMinutes } = await import("./worker");
  expect(
    resolveWeightFetchCooldownMinutes({
      lastFetchAt: null,
      now: new Date("2026-06-06T08:00:00+09:00"),
      raceStartAtJst: "not-a-date",
    }),
  ).toBe(1440);
});

it("resolveWeightFetchCooldownMinutes does not apply near-race override when race finished too long ago", async () => {
  const { resolveWeightFetchCooldownMinutes } = await import("./worker");
  expect(
    resolveWeightFetchCooldownMinutes({
      lastFetchAt: "2026-06-06T12:00:00+09:00",
      now: new Date("2026-06-06T12:45:00+09:00"),
      raceStartAtJst: "2026-06-06T12:30:00+09:00",
    }),
  ).toBe(60);
});

it("getEmptyWeightRetryDelaySeconds returns 10min while an empty weight race is upcoming", async () => {
  const { getEmptyWeightRetryDelaySeconds } = await import("./worker");
  expect(
    getEmptyWeightRetryDelaySeconds(buildWeightRetryRace(), new Date("2026-06-06T11:30:00+09:00")),
  ).toBe(600);
});

it("getEmptyWeightRetryDelaySeconds returns 5min inside the near-race window", async () => {
  const { getEmptyWeightRetryDelaySeconds } = await import("./worker");
  expect(
    getEmptyWeightRetryDelaySeconds(buildWeightRetryRace(), new Date("2026-06-06T12:05:00+09:00")),
  ).toBe(300);
});

it("getEmptyWeightRetryDelaySeconds returns null outside the active fetch window", async () => {
  const { getEmptyWeightRetryDelaySeconds } = await import("./worker");
  expect(
    getEmptyWeightRetryDelaySeconds(buildWeightRetryRace(), new Date("2026-06-06T09:00:00+09:00")),
  ).toBe(null);
  expect(
    getEmptyWeightRetryDelaySeconds(buildWeightRetryRace(), new Date("2026-06-06T12:45:00+09:00")),
  ).toBe(null);
});

it("planRealtimeFetches does not use the legacy KV race list to enqueue weight jobs", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, getRaceSource } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([]);
  vi.mocked(getRaceSource).mockImplementation(async (_db, raceKey) =>
    raceKey === "jra:2026:0606:05:01"
      ? {
          babaCode: "05",
          debaUrl: "https://www.jra.go.jp/race/r1",
          kaisaiKai: "02",
          kaisaiNen: "2026",
          kaisaiNichime: "06",
          kaisaiTsukihi: "0606",
          keibajoCode: "05",
          lastOddsFetchAt: null,
          lastWeightFetchAt: null,
          oddsLinks: {},
          raceBango: "01",
          raceKey: "jra:2026:0606:05:01",
          raceName: "Fallback1",
          raceStartAtJst: "2026-06-06T13:30:00+09:00",
          source: "jra",
        }
      : raceKey === "jra:2026:0606:05:02"
        ? {
            babaCode: "05",
            debaUrl: "https://www.jra.go.jp/race/r2",
            kaisaiKai: "02",
            kaisaiNen: "2026",
            kaisaiNichime: "06",
            kaisaiTsukihi: "0606",
            keibajoCode: "05",
            lastOddsFetchAt: null,
            lastWeightFetchAt: null,
            oddsLinks: {},
            raceBango: "02",
            raceKey: "jra:2026:0606:05:02",
            raceName: "Fallback2",
            raceStartAtJst: "2026-06-06T14:00:00+09:00",
            source: "jra",
          }
        : null,
  );
  const kvGet = vi.fn(async () =>
    JSON.stringify([
      { raceKey: "jra:2026:0606:05:01", source: "jra" },
      { raceKey: "jra:2026:0606:05:02", source: "jra" },
    ]),
  );
  const kvPut = vi.fn(async () => {});
  const sendBatch = vi.fn(async () => {});
  const send = vi.fn(async () => {});
  const env = buildEnv({
    DETAIL_SECTION_CACHE_KV: { get: kvGet, put: kvPut } as unknown as KVNamespace,
    REALTIME_TEST_NOW: "2026-06-06T03:00:00.000Z",
  });
  env.REALTIME_JOBS = { send, sendBatch } as never;
  await planRealtimeFetches(env, "20260606");
  const sentSingle = (send.mock.calls as unknown as [{ raceKey: string; type: string }][]).map(
    (c) => c[0],
  );
  const sentBatched =
    (
      sendBatch.mock.calls as unknown as [{ body: { raceKey: string; type: string } }[]][]
    )[0]?.[0] ?? [];
  const allWeightKeys = [
    ...sentSingle.filter((j) => j.type === "fetch-weights").map((j) => j.raceKey),
    ...sentBatched.filter((m) => m.body.type === "fetch-weights").map((m) => m.body.raceKey),
  ];
  expect(allWeightKeys).toStrictEqual([]);
});

it("planRealtimeFetches KV fallback excludes race when within same-day cooldown", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, getRaceSource } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([]);
  vi.mocked(getRaceSource).mockResolvedValue({
    babaCode: "05",
    debaUrl: "https://www.jra.go.jp/race/cooldown",
    kaisaiKai: "02",
    kaisaiNen: "2026",
    kaisaiNichime: "06",
    kaisaiTsukihi: "0606",
    keibajoCode: "05",
    lastOddsFetchAt: null,
    lastWeightFetchAt: "2026-06-06T11:45:00+09:00",
    oddsLinks: {},
    raceBango: "03",
    raceKey: "jra:2026:0606:05:03",
    raceName: "CooldownLocked",
    raceStartAtJst: "2026-06-06T13:30:00+09:00",
    source: "jra",
  });
  const kvGet = vi.fn(async () =>
    JSON.stringify([{ raceKey: "jra:2026:0606:05:03", source: "jra" }]),
  );
  const kvPut = vi.fn(async () => {});
  const sendBatch = vi.fn(async () => {});
  const send = vi.fn(async () => {});
  const env = buildEnv({
    DETAIL_SECTION_CACHE_KV: { get: kvGet, put: kvPut } as unknown as KVNamespace,
    REALTIME_TEST_NOW: "2026-06-06T03:00:00.000Z",
  });
  env.REALTIME_JOBS = { send, sendBatch } as never;
  await planRealtimeFetches(env, "20260606");
  const sentSingle = (send.mock.calls as unknown as [{ raceKey: string; type: string }][]).map(
    (c) => c[0],
  );
  const sentBatched =
    (
      sendBatch.mock.calls as unknown as [{ body: { raceKey: string; type: string } }[]][]
    )[0]?.[0] ?? [];
  const weightCount =
    sentSingle.filter((j) => j.type === "fetch-weights").length +
    sentBatched.filter((m) => m.body.type === "fetch-weights").length;
  expect(weightCount).toBe(0);
});

it("planRealtimeFetches KV fallback excludes race when beyond 180-min lead-time", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, getRaceSource } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([]);
  vi.mocked(getRaceSource).mockResolvedValue({
    babaCode: "05",
    debaUrl: "https://www.jra.go.jp/race/leadtime",
    kaisaiKai: "02",
    kaisaiNen: "2026",
    kaisaiNichime: "06",
    kaisaiTsukihi: "0606",
    keibajoCode: "05",
    lastOddsFetchAt: null,
    lastWeightFetchAt: null,
    oddsLinks: {},
    raceBango: "04",
    raceKey: "jra:2026:0606:05:04",
    raceName: "LeadTimeAhead",
    raceStartAtJst: "2026-06-06T16:30:00+09:00",
    source: "jra",
  });
  const kvGet = vi.fn(async () =>
    JSON.stringify([{ raceKey: "jra:2026:0606:05:04", source: "jra" }]),
  );
  const kvPut = vi.fn(async () => {});
  const sendBatch = vi.fn(async () => {});
  const send = vi.fn(async () => {});
  const env = buildEnv({
    DETAIL_SECTION_CACHE_KV: { get: kvGet, put: kvPut } as unknown as KVNamespace,
    REALTIME_TEST_NOW: "2026-06-06T03:00:00.000Z",
  });
  env.REALTIME_JOBS = { send, sendBatch } as never;
  await planRealtimeFetches(env, "20260606");
  const sentSingle = (send.mock.calls as unknown as [{ raceKey: string; type: string }][]).map(
    (c) => c[0],
  );
  const sentBatched =
    (
      sendBatch.mock.calls as unknown as [{ body: { raceKey: string; type: string } }[]][]
    )[0]?.[0] ?? [];
  const weightCount =
    sentSingle.filter((j) => j.type === "fetch-weights").length +
    sentBatched.filter((m) => m.body.type === "fetch-weights").length;
  expect(weightCount).toBe(0);
});

it("planRealtimeFetches KV fallback excludes race when getRaceSource returns null", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, getRaceSource } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([]);
  vi.mocked(getRaceSource).mockResolvedValue(null);
  const kvGet = vi.fn(async () =>
    JSON.stringify([{ raceKey: "jra:2026:0606:05:05", source: "jra" }]),
  );
  const kvPut = vi.fn(async () => {});
  const sendBatch = vi.fn(async () => {});
  const send = vi.fn(async () => {});
  const env = buildEnv({
    DETAIL_SECTION_CACHE_KV: { get: kvGet, put: kvPut } as unknown as KVNamespace,
    REALTIME_TEST_NOW: "2026-06-06T03:00:00.000Z",
  });
  env.REALTIME_JOBS = { send, sendBatch } as never;
  await planRealtimeFetches(env, "20260606");
  const sentSingle = (send.mock.calls as unknown as [{ raceKey: string; type: string }][]).map(
    (c) => c[0],
  );
  const sentBatched =
    (
      sendBatch.mock.calls as unknown as [{ body: { raceKey: string; type: string } }[]][]
    )[0]?.[0] ?? [];
  const weightCount =
    sentSingle.filter((j) => j.type === "fetch-weights").length +
    sentBatched.filter((m) => m.body.type === "fetch-weights").length;
  expect(weightCount).toBe(0);
});

it("planRealtimeFetches KV fallback excludes race when raceStartAtJst is unparseable", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, getRaceSource } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([]);
  vi.mocked(getRaceSource).mockResolvedValue({
    babaCode: "05",
    debaUrl: "https://www.jra.go.jp/race/badtime",
    kaisaiKai: "02",
    kaisaiNen: "2026",
    kaisaiNichime: "06",
    kaisaiTsukihi: "0606",
    keibajoCode: "05",
    lastOddsFetchAt: null,
    lastWeightFetchAt: null,
    oddsLinks: {},
    raceBango: "06",
    raceKey: "jra:2026:0606:05:06",
    raceName: "BadTime",
    raceStartAtJst: "invalid",
    source: "jra",
  });
  const kvGet = vi.fn(async () =>
    JSON.stringify([{ raceKey: "jra:2026:0606:05:06", source: "jra" }]),
  );
  const kvPut = vi.fn(async () => {});
  const sendBatch = vi.fn(async () => {});
  const send = vi.fn(async () => {});
  const env = buildEnv({
    DETAIL_SECTION_CACHE_KV: { get: kvGet, put: kvPut } as unknown as KVNamespace,
    REALTIME_TEST_NOW: "2026-06-06T03:00:00.000Z",
  });
  env.REALTIME_JOBS = { send, sendBatch } as never;
  await planRealtimeFetches(env, "20260606");
  const sentSingle = (send.mock.calls as unknown as [{ raceKey: string; type: string }][]).map(
    (c) => c[0],
  );
  const sentBatched =
    (
      sendBatch.mock.calls as unknown as [{ body: { raceKey: string; type: string } }[]][]
    )[0]?.[0] ?? [];
  const weightCount =
    sentSingle.filter((j) => j.type === "fetch-weights").length +
    sentBatched.filter((m) => m.body.type === "fetch-weights").length;
  expect(weightCount).toBe(0);
});

it("planRealtimeFetches does not maintain the legacy weight KV race list", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "05",
      debaUrl: "https://www.jra.go.jp/race/kvwrite",
      discoveredAt: "2026-06-06T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0606",
      keibajoCode: "05",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "07",
      raceKey: "jra:2026:0606:05:07",
      raceName: "KvWrite",
      raceStartAtJst: "2026-06-06T13:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-06-06T00:00:00+09:00",
    },
  ] as never);
  const kvGet = vi.fn(async () => null);
  const kvPut = vi.fn(async () => {});
  const env = buildEnv({
    DETAIL_SECTION_CACHE_KV: { get: kvGet, put: kvPut } as unknown as KVNamespace,
    REALTIME_TEST_NOW: "2026-06-06T03:00:00.000Z",
  });
  env.REALTIME_JOBS = { send: vi.fn(async () => {}), sendBatch: vi.fn(async () => {}) } as never;
  await planRealtimeFetches(env, "20260606");
  expect(kvPut).not.toHaveBeenCalled();
});

it("planRealtimeFetches skips KV fallback when DETAIL_SECTION_CACHE_KV is absent", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([]);
  const sendBatch = vi.fn(async () => {});
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_TEST_NOW: "2026-06-06T03:00:00.000Z",
  });
  env.REALTIME_JOBS = { send, sendBatch } as never;
  await planRealtimeFetches(env, "20260606");
  const sentSingle = (send.mock.calls as unknown as [{ type: string }][]).map((c) => c[0]);
  const sentBatched =
    (sendBatch.mock.calls as unknown as [{ body: { type: string } }[]][])[0]?.[0] ?? [];
  const weightCount =
    sentSingle.filter((j) => j.type === "fetch-weights").length +
    sentBatched.filter((m) => m.body.type === "fetch-weights").length;
  expect(weightCount).toBe(0);
});

it("readWeightRaceListFallbackFromKv returns empty array when KV binding is absent", async () => {
  const { readWeightRaceListFallbackFromKv } = await import("./worker");
  const env = buildEnv();
  const result = await readWeightRaceListFallbackFromKv(env, "20260606");
  expect(result).toStrictEqual([]);
});

it("readWeightRaceListFallbackFromKv returns empty array when KV value is missing", async () => {
  const { readWeightRaceListFallbackFromKv } = await import("./worker");
  const env = buildEnv({
    DETAIL_SECTION_CACHE_KV: {
      get: vi.fn(async () => null),
      put: vi.fn(async () => {}),
    } as unknown as KVNamespace,
  });
  const result = await readWeightRaceListFallbackFromKv(env, "20260606");
  expect(result).toStrictEqual([]);
});

it("readWeightRaceListFallbackFromKv returns empty array when KV value is invalid JSON", async () => {
  const { readWeightRaceListFallbackFromKv } = await import("./worker");
  const env = buildEnv({
    DETAIL_SECTION_CACHE_KV: {
      get: vi.fn(async () => "{not-json"),
      put: vi.fn(async () => {}),
    } as unknown as KVNamespace,
  });
  const result = await readWeightRaceListFallbackFromKv(env, "20260606");
  expect(result).toStrictEqual([]);
});

it("readWeightRaceListFallbackFromKv returns empty array when KV value is non-array JSON", async () => {
  const { readWeightRaceListFallbackFromKv } = await import("./worker");
  const env = buildEnv({
    DETAIL_SECTION_CACHE_KV: {
      get: vi.fn(async () => '{"x":1}'),
      put: vi.fn(async () => {}),
    } as unknown as KVNamespace,
  });
  const result = await readWeightRaceListFallbackFromKv(env, "20260606");
  expect(result).toStrictEqual([]);
});

it("writeWeightRaceListFallbackToKv no-ops when KV binding is absent", async () => {
  const { writeWeightRaceListFallbackToKv } = await import("./worker");
  const env = buildEnv();
  await writeWeightRaceListFallbackToKv(env, "20260606", []);
  expect(env.DETAIL_SECTION_CACHE_KV).toBeUndefined();
});

it("parseFetchWeightsBatchBody returns null for missing body", async () => {
  const { parseFetchWeightsBatchBody } = await import("./worker");
  expect(parseFetchWeightsBatchBody(null)).toBeNull();
});

it("parseFetchWeightsBatchBody returns null for missing date", async () => {
  const { parseFetchWeightsBatchBody } = await import("./worker");
  expect(parseFetchWeightsBatchBody({ source: "jra" })).toBeNull();
});

it("parseFetchWeightsBatchBody returns null for malformed date", async () => {
  const { parseFetchWeightsBatchBody } = await import("./worker");
  expect(parseFetchWeightsBatchBody({ date: "2026/06/06" })).toBeNull();
});

it("parseFetchWeightsBatchBody returns null for unsupported source", async () => {
  const { parseFetchWeightsBatchBody } = await import("./worker");
  expect(parseFetchWeightsBatchBody({ date: "2026-06-06", source: "ban-ei" })).toBeNull();
});

it("parseFetchWeightsBatchBody normalizes source default to jra and force default to false", async () => {
  const { parseFetchWeightsBatchBody } = await import("./worker");
  expect(parseFetchWeightsBatchBody({ date: "2026-06-06" })).toStrictEqual({
    date: "2026-06-06",
    force: false,
    source: "jra",
  });
});

it("parseFetchWeightsBatchBody accepts explicit source=nar and force=true", async () => {
  const { parseFetchWeightsBatchBody } = await import("./worker");
  expect(
    parseFetchWeightsBatchBody({ date: "2026-06-06", force: true, source: "nar" }),
  ).toStrictEqual({
    date: "2026-06-06",
    force: true,
    source: "nar",
  });
});

it("parseFetchWeightsBatchBody accepts source=all", async () => {
  const { parseFetchWeightsBatchBody } = await import("./worker");
  expect(parseFetchWeightsBatchBody({ date: "2026-06-06", source: "all" })).toStrictEqual({
    date: "2026-06-06",
    force: false,
    source: "all",
  });
});

it("enqueueFetchWeightsBatch enqueues a fetch-weights job for each due race in the requested source", async () => {
  const { enqueueFetchWeightsBatch } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "05",
      debaUrl: "https://www.jra.go.jp/race/jrabatch",
      discoveredAt: "2026-06-06T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0606",
      keibajoCode: "05",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "01",
      raceKey: "jra:2026:0606:05:01",
      raceName: "JraBatch",
      raceStartAtJst: "2026-06-06T13:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-06-06T00:00:00+09:00",
    },
    {
      babaCode: "22",
      debaUrl: "https://nar.example/narrace",
      discoveredAt: "2026-06-06T00:00:00+09:00",
      kaisaiKai: null,
      kaisaiNen: "2026",
      kaisaiNichime: null,
      kaisaiTsukihi: "0606",
      keibajoCode: "55",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "01",
      raceKey: "nar:2026:0606:55:01",
      raceName: "NarRace",
      raceStartAtJst: "2026-06-06T13:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "nar",
      updatedAt: "2026-06-06T00:00:00+09:00",
    },
  ] as never);
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-06-06T03:00:00.000Z" });
  const result = await enqueueFetchWeightsBatch(env, {
    date: "2026-06-06",
    force: false,
    source: "jra",
  });
  expect(result).toBe(1);
});

it("enqueueFetchWeightsBatch skips races with same-day cooldown unless force is true", async () => {
  const { enqueueFetchWeightsBatch } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "05",
      debaUrl: "https://www.jra.go.jp/race/cooldown",
      discoveredAt: "2026-06-06T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0606",
      keibajoCode: "05",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: "2026-06-06T11:50:00+09:00",
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "01",
      raceKey: "jra:2026:0606:05:01",
      raceName: "Cooldown",
      raceStartAtJst: "2026-06-06T13:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-06-06T00:00:00+09:00",
    },
  ] as never);
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-06-06T03:00:00.000Z" });
  expect(
    await enqueueFetchWeightsBatch(env, { date: "2026-06-06", force: false, source: "jra" }),
  ).toBe(0);
  expect(
    await enqueueFetchWeightsBatch(env, { date: "2026-06-06", force: true, source: "jra" }),
  ).toBe(1);
});

it("enqueueFetchWeightsBatch source=all includes both JRA and NAR races", async () => {
  const { enqueueFetchWeightsBatch } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "05",
      debaUrl: "https://www.jra.go.jp/race/allj",
      discoveredAt: "2026-06-06T00:00:00+09:00",
      kaisaiKai: "02",
      kaisaiNen: "2026",
      kaisaiNichime: "06",
      kaisaiTsukihi: "0606",
      keibajoCode: "05",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "01",
      raceKey: "jra:2026:0606:05:01",
      raceName: "AllJra",
      raceStartAtJst: "2026-06-06T13:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-06-06T00:00:00+09:00",
    },
    {
      babaCode: "22",
      debaUrl: "https://nar.example/alln",
      discoveredAt: "2026-06-06T00:00:00+09:00",
      kaisaiKai: null,
      kaisaiNen: "2026",
      kaisaiNichime: null,
      kaisaiTsukihi: "0606",
      keibajoCode: "55",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "01",
      raceKey: "nar:2026:0606:55:01",
      raceName: "AllNar",
      raceStartAtJst: "2026-06-06T13:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "nar",
      updatedAt: "2026-06-06T00:00:00+09:00",
    },
  ] as never);
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-06-06T03:00:00.000Z" });
  expect(
    await enqueueFetchWeightsBatch(env, { date: "2026-06-06", force: false, source: "all" }),
  ).toBe(2);
});

it("extractJstDate returns empty string for null input", async () => {
  const { extractJstDate } = await import("./worker");
  expect(extractJstDate(null)).toBe("");
});

it("extractJstDate returns empty string for unparsable input", async () => {
  const { extractJstDate } = await import("./worker");
  expect(extractJstDate("garbage")).toBe("");
});

it("extractJstDate returns the JST date slice for a UTC iso input", async () => {
  const { extractJstDate } = await import("./worker");
  expect(extractJstDate("2026-06-06T03:00:00.000Z")).toBe("2026-06-06");
});

it("findStaleWeightFetchRaces binds post window and dynamic retry backoffs as JST strings", async () => {
  const { findStaleWeightFetchRaces } = await import("./worker");
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await findStaleWeightFetchRaces(db, new Date("2026-06-07T03:00:00.000Z"));
  expect(bind).toHaveBeenCalledWith(
    "2026-06-07T11:50:00+09:00",
    "2026-06-07T15:00:00+09:00",
    "2026-06-07T13:30:00+09:00",
    "2026-06-07T11:59:00+09:00",
    "2026-06-07T12:30:00+09:00",
    "2026-06-07T11:55:00+09:00",
    "2026-06-07T11:45:00+09:00",
    24,
  );
});

it("findStaleWeightFetchRaces binds JST iso strings that lexically compare correctly against stored race_start_at_jst values", async () => {
  const { findStaleWeightFetchRaces } = await import("./worker");
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  // 11:13 JST today (UTC 02:13). Reproduces the prod scenario where the
  // pre-fix UTC bounds lex-compared wrong against stored JST values.
  await findStaleWeightFetchRaces(db, new Date("2026-06-13T02:13:00.000Z"));
  expect(bind).toHaveBeenCalledWith(
    "2026-06-13T11:03:00+09:00",
    "2026-06-13T14:13:00+09:00",
    "2026-06-13T12:43:00+09:00",
    "2026-06-13T11:12:00+09:00",
    "2026-06-13T11:43:00+09:00",
    "2026-06-13T11:08:00+09:00",
    "2026-06-13T10:58:00+09:00",
    24,
  );
  // Watchdog SQL: race_start_at_jst > lookBack AND < lookAhead AND
  // last_weight_fetch_at IS NULL. Confirm a real stored JST
  // race-start string the bug previously missed now lex-compares correctly.
  const storedRaceStartJst = "2026-06-13T11:30:00+09:00";
  const storedLookBack = "2026-06-13T11:03:00+09:00";
  const storedLookAhead = "2026-06-13T14:13:00+09:00";
  expect(storedRaceStartJst > storedLookBack).toBe(true);
  expect(storedRaceStartJst < storedLookAhead).toBe(true);
});

it("findStaleWeightFetchRaces maps the d1 rows into StaleWeightFetchRace records", async () => {
  const { findStaleWeightFetchRaces } = await import("./worker");
  const all = vi.fn(async () => ({
    results: [
      {
        last_weight_fetch_at: null,
        last_weight_fetch_attempt_at: null,
        race_key: "jra:2026:0607:05:06",
        race_start_at_jst: "2026-06-07T12:55:00+09:00",
      },
      {
        last_weight_fetch_at: "2026-06-07T11:30:00+09:00",
        last_weight_fetch_attempt_at: "2026-06-07T11:30:00+09:00",
        race_key: "jra:2026:0607:05:11",
        race_start_at_jst: "2026-06-07T14:30:00+09:00",
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const rows = await findStaleWeightFetchRaces(db, new Date("2026-06-07T03:00:00.000Z"));
  expect(rows).toStrictEqual([
    {
      lastWeightFetchAt: null,
      lastWeightFetchAttemptAt: null,
      lastWeightFetchSoftMissAt: null,
      raceKey: "jra:2026:0607:05:06",
      raceStartAtJst: "2026-06-07T12:55:00+09:00",
    },
    {
      lastWeightFetchAt: "2026-06-07T11:30:00+09:00",
      lastWeightFetchAttemptAt: "2026-06-07T11:30:00+09:00",
      lastWeightFetchSoftMissAt: null,
      raceKey: "jra:2026:0607:05:11",
      raceStartAtJst: "2026-06-07T14:30:00+09:00",
    },
  ]);
});

// Regression test for the 2026-07-03 incident: a race that has never
// succeeded (last_weight_fetch_at is null) but was attempted moments ago
// (last_weight_fetch_attempt_at recent) must not be re-selected on every
// */2 watchdog tick. The SQL predicate that enforces this is opaque to a
// mocked D1, so this asserts the exact query text carries the new
// last_weight_fetch_attempt_at backoff clause in addition to the unchanged
// last_weight_fetch_at success-only clause, and that the deadline order is
// source-neutral before the row limit is applied.
it("findStaleWeightFetchRaces gates on attempts and prioritizes the earliest race across sources", async () => {
  const { findStaleWeightFetchRaces } = await import("./worker");
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn((..._args: unknown[]) => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await findStaleWeightFetchRaces(db, new Date("2026-07-03T03:00:00.000Z"));
  expect(prepare.mock.calls[0]![0]).toBe(
    `
        select race_key, race_start_at_jst, last_weight_fetch_at,
          last_weight_fetch_attempt_at, last_weight_fetch_soft_miss_at
        from realtime_race_sources
        where race_start_at_jst > ?
          and race_start_at_jst <= ?
          and last_weight_fetch_at is null
          and (
            last_weight_fetch_attempt_at is null
            or last_weight_fetch_attempt_at < case
              when last_weight_fetch_soft_miss_at = last_weight_fetch_attempt_at
                and race_start_at_jst <= ? then ?
              when race_start_at_jst <= ? then ?
              else ?
            end
          )
        order by race_start_at_jst
        limit ?
      `,
  );
  expect(bind).toHaveBeenCalledWith(
    "2026-07-03T11:50:00+09:00",
    "2026-07-03T15:00:00+09:00",
    "2026-07-03T13:30:00+09:00",
    "2026-07-03T11:59:00+09:00",
    "2026-07-03T12:30:00+09:00",
    "2026-07-03T11:55:00+09:00",
    "2026-07-03T11:45:00+09:00",
    24,
  );
});

it("runWeightWatchdog logs the no-stale path when there are no candidates", async () => {
  const { runWeightWatchdog } = await import("./worker");
  const { logFetch } = await import("./storage");
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const send = vi.fn(async () => {});
  const sendBatch = vi.fn(async () => {});
  const env = {
    REALTIME_DB: { prepare } as unknown as D1Database,
    REALTIME_JOBS: { send, sendBatch },
  } as unknown as Env;
  await runWeightWatchdog(env, new Date("2026-06-07T03:00:00.000Z"));
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "weight-watchdog",
    "ok",
    null,
    "no stale weight races",
    undefined,
    3600,
  );
  expect(send).not.toHaveBeenCalled();
  expect(sendBatch).not.toHaveBeenCalled();
});

it("runWeightWatchdog forwards the KV namespace to logFetch for dedupe when bound", async () => {
  const { runWeightWatchdog } = await import("./worker");
  const { logFetch } = await import("./storage");
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const send = vi.fn(async () => {});
  const sendBatch = vi.fn(async () => {});
  const kv = { get: vi.fn(), put: vi.fn() } as unknown as KVNamespace;
  const env = {
    DETAIL_SECTION_CACHE_KV: kv,
    REALTIME_DB: { prepare } as unknown as D1Database,
    REALTIME_JOBS: { send, sendBatch },
  } as unknown as Env;
  await runWeightWatchdog(env, new Date("2026-06-07T03:00:00.000Z"));
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "weight-watchdog",
    "ok",
    null,
    "no stale weight races",
    kv,
    3600,
  );
});

// 2026-07-03 incident: the watchdog used to also inline-run
// fetchAndStoreWeights for a subset of the same jobs it had just enqueued,
// duplicating the HTTP request to keiba.go.jp for the same race in the same
// tick with no backoff on failure. This lane was removed entirely (not
// zeroed) -- the watchdog now only enqueues, and getRaceSource (the first
// dependency fetchAndStoreWeights touches) must never be called from here.
it("runWeightWatchdog enqueues fetch-weights jobs for stale JRA races without inline-fetching them", async () => {
  const { runWeightWatchdog } = await import("./worker");
  const { claimWeightFetch, getRaceSource, logFetch } = await import("./storage");
  const all = vi.fn(async () => ({
    results: [
      {
        last_weight_fetch_at: null,
        last_weight_fetch_attempt_at: null,
        race_key: "jra:2026:0607:05:06",
        race_start_at_jst: "2026-06-07T12:55:00+09:00",
      },
      {
        last_weight_fetch_at: "2026-06-07T11:30:00+09:00",
        last_weight_fetch_attempt_at: "2026-06-07T11:30:00+09:00",
        race_key: "jra:2026:0607:05:11",
        race_start_at_jst: "2026-06-07T14:30:00+09:00",
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const send = vi.fn(async () => {});
  const sendBatch = vi.fn(async () => {});
  const env = {
    REALTIME_DB: { prepare } as unknown as D1Database,
    REALTIME_JOBS: { send, sendBatch },
  } as unknown as Env;
  await runWeightWatchdog(env, new Date("2026-06-07T03:00:00.000Z"));
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "weight-watchdog",
    "ok",
    null,
    '{"enqueued":2}',
    undefined,
  );
  expect(sendBatch).toHaveBeenCalledWith([
    {
      body: {
        raceKey: "jra:2026:0607:05:06",
        type: "fetch-weights",
        watchdogReservedAt: "2026-06-07T12:00:00+09:00",
      },
    },
    {
      body: {
        raceKey: "jra:2026:0607:05:11",
        type: "fetch-weights",
        watchdogReservedAt: "2026-06-07T12:00:00+09:00",
      },
    },
  ]);
  expect(claimWeightFetch).toHaveBeenCalledWith(
    expect.anything(),
    "jra:2026:0607:05:06",
    "2026-06-07T12:00:00+09:00",
    "2026-06-07T11:45:00+09:00",
  );
  expect(getRaceSource).not.toHaveBeenCalled();
});

it("runWeightWatchdog enqueues fetch-weights jobs for stale NAR races without inline-fetching them", async () => {
  const { runWeightWatchdog } = await import("./worker");
  const { claimWeightFetch, getRaceSource, logFetch } = await import("./storage");
  const all = vi.fn(async () => ({
    results: [
      {
        last_weight_fetch_at: null,
        last_weight_fetch_attempt_at: null,
        race_key: "nar:2026:0607:44:07",
        race_start_at_jst: "2026-06-07T17:45:00+09:00",
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const send = vi.fn(async () => {});
  const sendBatch = vi.fn(async () => {});
  const env = {
    REALTIME_DB: { prepare } as unknown as D1Database,
    REALTIME_JOBS: { send, sendBatch },
  } as unknown as Env;
  await runWeightWatchdog(env, new Date("2026-06-07T08:30:00.000Z"));
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "weight-watchdog",
    "ok",
    null,
    '{"enqueued":1}',
    undefined,
  );
  expect(send).toHaveBeenCalledWith({
    raceKey: "nar:2026:0607:44:07",
    type: "fetch-weights",
    watchdogReservedAt: "2026-06-07T17:30:00+09:00",
  });
  expect(claimWeightFetch).toHaveBeenCalledWith(
    expect.anything(),
    "nar:2026:0607:44:07",
    "2026-06-07T17:30:00+09:00",
    "2026-06-07T17:25:00+09:00",
  );
  expect(getRaceSource).not.toHaveBeenCalled();
});

it("runWeightWatchdog uses the next cron cadence only after a completed soft miss within 90 minutes", async () => {
  const { runWeightWatchdog } = await import("./worker");
  const { claimWeightFetch } = await import("./storage");
  const all = vi.fn(async () => ({
    results: [
      {
        last_weight_fetch_at: null,
        last_weight_fetch_attempt_at: "2026-08-24T14:01:01+09:00",
        last_weight_fetch_soft_miss_at: "2026-08-24T14:01:01+09:00",
        race_key: "nar:2026:0824:83:12",
        race_start_at_jst: "2026-08-24T15:00:00+09:00",
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const send = vi.fn(async () => {});
  const env = {
    REALTIME_DB: { prepare } as unknown as D1Database,
    REALTIME_JOBS: { send, sendBatch: vi.fn(async () => {}) },
  } as unknown as Env;

  await runWeightWatchdog(env, new Date("2026-08-24T05:03:00.000Z"));

  expect(claimWeightFetch).toHaveBeenCalledWith(
    expect.anything(),
    "nar:2026:0824:83:12",
    "2026-08-24T14:03:00+09:00",
    "2026-08-24T14:02:00+09:00",
  );
  expect(send).toHaveBeenCalledTimes(1);
});

it("runWeightWatchdog atomically enqueues one job when duplicate reads return the same race", async () => {
  const { runWeightWatchdog } = await import("./worker");
  const { claimWeightFetch } = await import("./storage");
  vi.mocked(claimWeightFetch).mockResolvedValueOnce(true).mockResolvedValueOnce(false);
  const candidate = {
    last_weight_fetch_at: null,
    last_weight_fetch_attempt_at: "2026-08-24T14:01:01+09:00",
    last_weight_fetch_soft_miss_at: "2026-08-24T14:01:01+09:00",
    race_key: "nar:2026:0824:83:12",
    race_start_at_jst: "2026-08-24T15:00:00+09:00",
  };
  const all = vi.fn(async () => ({ results: [candidate, candidate] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const send = vi.fn(async () => {});
  const sendBatch = vi.fn(async () => {});
  const env = {
    REALTIME_DB: { prepare } as unknown as D1Database,
    REALTIME_JOBS: { send, sendBatch },
  } as unknown as Env;

  await runWeightWatchdog(env, new Date("2026-08-24T05:03:00.000Z"));

  expect(send).toHaveBeenCalledTimes(1);
  expect(sendBatch).not.toHaveBeenCalled();
});

it("runWeightWatchdog does not enqueue a race when its atomic reservation loses", async () => {
  const { runWeightWatchdog } = await import("./worker");
  const { claimWeightFetch, logFetch } = await import("./storage");
  vi.mocked(claimWeightFetch).mockResolvedValueOnce(false);
  const all = vi.fn(async () => ({
    results: [
      {
        last_weight_fetch_at: null,
        last_weight_fetch_attempt_at: null,
        race_key: "nar:2026:0607:44:07",
        race_start_at_jst: "2026-06-07T17:45:00+09:00",
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const send = vi.fn(async () => {});
  const sendBatch = vi.fn(async () => {});
  const env = {
    REALTIME_DB: { prepare } as unknown as D1Database,
    REALTIME_JOBS: { send, sendBatch },
  } as unknown as Env;
  await runWeightWatchdog(env, new Date("2026-06-07T08:30:00.000Z"));
  expect(send).not.toHaveBeenCalled();
  expect(sendBatch).not.toHaveBeenCalled();
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "weight-watchdog",
    "ok",
    null,
    '{"enqueued":0}',
    undefined,
  );
});

it("runWeightWatchdog recovers a failed queue send on the next near-race backoff", async () => {
  const { runWeightWatchdog } = await import("./worker");
  const { claimWeightFetch, logFetch } = await import("./storage");
  const all = vi.fn(async () => ({
    results: [
      {
        last_weight_fetch_at: null,
        last_weight_fetch_attempt_at: "2026-06-07T17:30:00+09:00",
        race_key: "nar:2026:0607:44:07",
        race_start_at_jst: "2026-06-07T17:45:00+09:00",
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const send = vi
    .fn<() => Promise<void>>()
    .mockRejectedValueOnce(new Error("queue unavailable"))
    .mockResolvedValueOnce();
  const sendBatch = vi.fn(async () => {});
  const env = {
    REALTIME_DB: { prepare } as unknown as D1Database,
    REALTIME_JOBS: { send, sendBatch },
  } as unknown as Env;

  await runWeightWatchdog(env, new Date("2026-06-07T08:30:00.000Z"));
  await runWeightWatchdog(env, new Date("2026-06-07T08:36:00.000Z"));

  expect(send).toHaveBeenLastCalledWith({
    raceKey: "nar:2026:0607:44:07",
    type: "fetch-weights",
    watchdogReservedAt: "2026-06-07T17:36:00+09:00",
  });
  expect(claimWeightFetch).toHaveBeenLastCalledWith(
    expect.anything(),
    "nar:2026:0607:44:07",
    "2026-06-07T17:36:00+09:00",
    "2026-06-07T17:31:00+09:00",
  );
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "weight-watchdog",
    "error",
    null,
    "queue unavailable",
    undefined,
  );
});

it("runWeightWatchdog reserves and enqueues 57 races uniquely across three capped ticks", async () => {
  const { runWeightWatchdog } = await import("./worker");
  const { claimWeightFetch } = await import("./storage");
  const firstRows = Array.from({ length: 24 }, (_, index) => ({
    last_weight_fetch_at: null,
    last_weight_fetch_attempt_at: null,
    race_key: `nar:2026:0607:44:${String(index + 1).padStart(2, "0")}`,
    race_start_at_jst: "2026-06-07T17:45:00+09:00",
  }));
  const secondRows = Array.from({ length: 24 }, (_, index) => ({
    last_weight_fetch_at: null,
    last_weight_fetch_attempt_at: null,
    race_key: `nar:2026:0607:45:${String(index + 1).padStart(2, "0")}`,
    race_start_at_jst: "2026-06-07T17:45:00+09:00",
  }));
  const thirdRows = Array.from({ length: 9 }, (_, index) => ({
    last_weight_fetch_at: null,
    last_weight_fetch_attempt_at: null,
    race_key: `nar:2026:0607:46:${String(index + 1).padStart(2, "0")}`,
    race_start_at_jst: "2026-06-07T17:45:00+09:00",
  }));
  const all = vi
    .fn()
    .mockResolvedValueOnce({ results: firstRows })
    .mockResolvedValueOnce({ results: secondRows })
    .mockResolvedValueOnce({ results: thirdRows });
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const send = vi.fn(async () => {});
  const sendBatch = vi.fn(async () => {});
  const env = {
    REALTIME_DB: { prepare } as unknown as D1Database,
    REALTIME_JOBS: { send, sendBatch },
  } as unknown as Env;

  await runWeightWatchdog(env, new Date("2026-06-07T08:30:00.000Z"));
  await runWeightWatchdog(env, new Date("2026-06-07T08:32:00.000Z"));
  await runWeightWatchdog(env, new Date("2026-06-07T08:34:00.000Z"));

  const batches = sendBatch.mock.calls as unknown as [Array<{ body: Job }>][];
  const sentRaceKeys = batches.flatMap((call) =>
    call[0].flatMap((message) =>
      message.body.type === "fetch-weights" ? [message.body.raceKey] : [],
    ),
  );
  expect(batches[0]![0]).toHaveLength(24);
  expect(batches[1]![0]).toHaveLength(24);
  expect(batches[2]![0]).toHaveLength(9);
  expect(sentRaceKeys).toHaveLength(57);
  expect(new Set(sentRaceKeys).size).toBe(57);
  expect(claimWeightFetch).toHaveBeenCalledTimes(57);
  expect(batches[0]![0][0]!.body).toStrictEqual({
    raceKey: "nar:2026:0607:44:01",
    type: "fetch-weights",
    watchdogReservedAt: "2026-06-07T17:30:00+09:00",
  });
  expect(batches[2]![0][8]!.body).toStrictEqual({
    raceKey: "nar:2026:0607:46:09",
    type: "fetch-weights",
    watchdogReservedAt: "2026-06-07T17:34:00+09:00",
  });
});

it("runWeightWatchdog logs an error when the d1 query throws and does not enqueue jobs", async () => {
  const { runWeightWatchdog } = await import("./worker");
  const { logFetch } = await import("./storage");
  const all = vi.fn(async () => {
    throw new Error("d1 saturation");
  });
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const send = vi.fn(async () => {});
  const sendBatch = vi.fn(async () => {});
  const env = {
    REALTIME_DB: { prepare } as unknown as D1Database,
    REALTIME_JOBS: { send, sendBatch },
  } as unknown as Env;
  await runWeightWatchdog(env, new Date("2026-06-07T03:00:00.000Z"));
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "weight-watchdog",
    "error",
    null,
    "d1 saturation",
    undefined,
  );
  expect(send).not.toHaveBeenCalled();
  expect(sendBatch).not.toHaveBeenCalled();
});

// 2026-06-07: cover the new RESULT_FETCH_QUEUE_STALE_MINUTES branch in
// `buildResultFetchJobIfDue`. Without this guard a `last_result_queued_at`
// row left behind by an early-return path (claim race, transient skip)
// permanently blocked the planner from re-enqueueing the race even after
// the result-fetch lock expired.

it("planResultFetchesOnly re-enqueues fetch-results when lastResultQueuedAt is older than the stale threshold", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { claimResultFetch, getRaceSource, listSchedulableRaceSourcesByDate } =
    await import("./storage");
  vi.mocked(claimResultFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce(null);
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([
    {
      babaCode: "08",
      debaUrl: "https://www.jra.go.jp/race",
      discoveredAt: "2026-06-07T00:00:00+09:00",
      kaisaiKai: "03",
      kaisaiNen: "2026",
      kaisaiNichime: "01",
      kaisaiTsukihi: "0607",
      keibajoCode: "09",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: "2026-06-07T10:56:02+09:00",
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "03",
      raceKey: "jra:2026:0607:09:03",
      raceName: "阪神3R",
      raceStartAtJst: "2026-06-07T10:50:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: "2026-06-07T11:06:05+09:00",
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-06-07T00:00:00+09:00",
    },
  ] as never);
  const send = vi.fn(queueSendOk);
  const env = buildEnv({
    REALTIME_JOBS: { metrics: vi.fn(queueMetricsOk), send, sendBatch: vi.fn(queueSendOk) },
    REALTIME_TEST_NOW: "2026-06-07T04:30:00.000Z",
  });
  const count = await planResultFetchesOnly(env, "20260607");
  expect(count).toBe(1);
  expect(send).toHaveBeenCalledWith({ raceKey: "jra:2026:0607:09:03", type: "fetch-results" });
});

it("planResultFetchesOnly skips fetch-results when lastResultQueuedAt is fresh (within stale threshold)", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([
    {
      babaCode: "08",
      debaUrl: "https://www.jra.go.jp/race",
      discoveredAt: "2026-06-07T00:00:00+09:00",
      kaisaiKai: "03",
      kaisaiNen: "2026",
      kaisaiNichime: "01",
      kaisaiTsukihi: "0607",
      keibajoCode: "09",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: "2026-06-07T13:25:00+09:00",
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "03",
      raceKey: "jra:2026:0607:09:03",
      raceName: "阪神3R",
      raceStartAtJst: "2026-06-07T10:50:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-06-07T00:00:00+09:00",
    },
  ] as never);
  const send = vi.fn(queueSendOk);
  const env = buildEnv({
    REALTIME_JOBS: { metrics: vi.fn(queueMetricsOk), send, sendBatch: vi.fn(queueSendOk) },
    REALTIME_TEST_NOW: "2026-06-07T04:30:00.000Z",
  });
  const count = await planResultFetchesOnly(env, "20260607");
  expect(count).toBe(0);
  expect(send).not.toHaveBeenCalled();
});

it("planResultFetchesOnly handles a finished race inline without duplicate queue dispatch", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([
    {
      babaCode: "08",
      debaUrl: "https://www.jra.go.jp/race",
      discoveredAt: "2026-06-07T00:00:00+09:00",
      kaisaiKai: "03",
      kaisaiNen: "2026",
      kaisaiNichime: "01",
      kaisaiTsukihi: "0607",
      keibajoCode: "09",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "03",
      raceKey: "jra:2026:0607:09:03",
      raceName: "阪神3R",
      raceStartAtJst: "2026-06-07T10:50:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-06-07T00:00:00+09:00",
    },
  ] as never);
  const send = vi.fn(queueSendOk);
  const env = buildEnv({
    REALTIME_JOBS: { metrics: vi.fn(queueMetricsOk), send, sendBatch: vi.fn(queueSendOk) },
    REALTIME_TEST_NOW: "2026-06-07T04:30:00.000Z",
  });
  const count = await planResultFetchesOnly(env, "20260607");
  expect(count).toBe(1);
  expect(send).not.toHaveBeenCalled();
});

it("planResultFetchesOnly attempts inline NAR result fetches so race trends are not blocked by queue backlog", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, logFetch } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([
    {
      babaCode: "20",
      debaUrl: "https://www.keiba.go.jp/race",
      discoveredAt: "2026-06-07T00:00:00+09:00",
      kaisaiKai: null,
      kaisaiNen: "2026",
      kaisaiNichime: null,
      kaisaiTsukihi: "0607",
      keibajoCode: "44",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "12",
      raceKey: "nar:2026:0607:44:12",
      raceName: "大井12R",
      raceStartAtJst: "2026-06-07T20:50:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "nar",
      updatedAt: "2026-06-07T00:00:00+09:00",
    },
  ] as never);
  const send = vi.fn(queueSendOk);
  const env = buildEnv({
    REALTIME_JOBS: { metrics: vi.fn(queueMetricsOk), send, sendBatch: vi.fn(queueSendOk) },
    REALTIME_TEST_NOW: "2026-06-07T12:00:00.000Z",
  });
  const count = await planResultFetchesOnly(env, "20260607");
  expect(count).toBe(1);
  expect(send).not.toHaveBeenCalled();
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-results",
    "skip:claim-failed",
    "nar:2026:0607:44:12",
    null,
    undefined,
  );
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-result-fetches",
    "plan-result-fetches-summary",
    null,
    '{"enqueued":0,"eligible":1,"inlineAttempted":1,"inlineError":0,"skipped_too_recent":0}',
    undefined,
  );
});

it("planResultFetchesOnly attempts inline JRA result fetches with a low per-tick cap", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, logFetch } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([
    {
      babaCode: "08",
      debaUrl: "https://www.jra.go.jp/race/one",
      discoveredAt: "2026-06-07T00:00:00+09:00",
      kaisaiKai: "03",
      kaisaiNen: "2026",
      kaisaiNichime: "01",
      kaisaiTsukihi: "0607",
      keibajoCode: "09",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "03",
      raceKey: "jra:2026:0607:09:03",
      raceName: "阪神3R",
      raceStartAtJst: "2026-06-07T10:50:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-06-07T00:00:00+09:00",
    },
    {
      babaCode: "08",
      debaUrl: "https://www.jra.go.jp/race/two",
      discoveredAt: "2026-06-07T00:00:00+09:00",
      kaisaiKai: "03",
      kaisaiNen: "2026",
      kaisaiNichime: "01",
      kaisaiTsukihi: "0607",
      keibajoCode: "09",
      lastOddsFetchAt: null,
      lastOddsQueuedAt: null,
      lastResultFetchAt: null,
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "04",
      raceKey: "jra:2026:0607:09:04",
      raceName: "阪神4R",
      raceStartAtJst: "2026-06-07T11:20:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-06-07T00:00:00+09:00",
    },
  ] as never);
  const send = vi.fn(queueSendOk);
  const sendBatch = vi.fn(queueSendOk);
  const env = buildEnv({
    REALTIME_JOBS: { metrics: vi.fn(queueMetricsOk), send, sendBatch },
    REALTIME_TEST_NOW: "2026-06-07T03:00:00.000Z",
  });
  const count = await planResultFetchesOnly(env, "20260607");
  expect(count).toBe(2);
  expect(send).toHaveBeenCalledWith({ raceKey: "jra:2026:0607:09:04", type: "fetch-results" });
  expect(sendBatch).not.toHaveBeenCalled();
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-results",
    "skip:claim-failed",
    "jra:2026:0607:09:03",
    null,
    undefined,
  );
  expect(logFetch).not.toHaveBeenCalledWith(
    expect.anything(),
    "fetch-results",
    "skip:claim-failed",
    "jra:2026:0607:09:04",
    null,
    undefined,
  );
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-result-fetches",
    "plan-result-fetches-summary",
    null,
    '{"enqueued":1,"eligible":2,"inlineAttempted":1,"inlineError":0,"skipped_too_recent":0}',
    undefined,
  );
});
