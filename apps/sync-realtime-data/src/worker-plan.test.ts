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
  RUNNING_STYLE_INFERENCE_CRON: "*/10 * * * *",
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
  // 2026-05-12T13:30:00Z = JST 22:30, outside the 06-21 polling window.
  const env = buildEnv({
    REALTIME_TEST_NOW: "2026-05-12T13:30:00.000Z",
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

it("planPremiumPaddockFetchesForDate enqueues races 45 minutes before start (inside expanded 50-min window)", async () => {
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
    new Date("2026-05-12T03:15:00.000Z"),
  );
  expect(result).toStrictEqual([{ raceKey: "jra:2026:0512:08:01", type: "fetch-premium-paddock" }]);
});

it("planPremiumPaddockFetchesForDate skips races 55 minutes before start (outside 50-min window)", async () => {
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
    new Date("2026-05-12T03:05:00.000Z"),
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
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-12T13:30:00.000Z" } as never);
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

it("planRealtimeFetches enqueues JRA weight jobs in tier+minutes priority order", async () => {
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
  expect(weightRaceKeys).toStrictEqual([
    "jra:2026:0530:05:05",
    "jra:2026:0530:08:05",
    "jra:2026:0530:05:11",
    "jra:2026:0530:06:06",
    "jra:2026:0530:09:12",
    "jra:2026:0530:08:04",
    "jra:2026:0530:05:01",
  ]);
});

it("planRealtimeFetches includes weight job at 35 minutes before post", async () => {
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
  expect(allWeightKeys).toStrictEqual(["jra:2026:0530:05:07"]);
});

it("planRealtimeFetches includes weight job at 85 minutes before post (within 90-min lead)", async () => {
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
  expect(allWeightKeys).toStrictEqual(["jra:2026:0530:05:07"]);
});

it("planRealtimeFetches excludes weight job when race is 95 minutes before post (beyond 90-min lead)", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "05",
      debaUrl: "https://www.jra.go.jp/race/lead95",
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
      raceName: "Lead95",
      raceStartAtJst: "2026-05-30T13:35:00+09:00",
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

it("planRealtimeFetches excludes weight job when lastWeightFetchAt is within 24h", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "05",
      debaUrl: "https://www.jra.go.jp/race/fetched12h",
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
      lastWeightFetchAt: "2026-05-30T00:00:00+09:00",
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "07",
      raceKey: "jra:2026:0530:05:07",
      raceName: "Fetched12h",
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

it("planRealtimeFetches places NAR weight jobs after JRA weight jobs", async () => {
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
  expect(weightRaceKeys).toStrictEqual(["jra:2026:0530:05:05", "nar:2026:0530:55:05"]);
});

it("planRealtimeFetches still enqueues weight jobs on a non-three-minute tick (gate removed)", async () => {
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
  expect(allWeightKeys).toStrictEqual(["jra:2026:0530:05:05"]);
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
