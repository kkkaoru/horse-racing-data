// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { Env } from "./types";

vi.mock("./storage", () => ({
  logFetch: vi.fn(async () => {}),
  // satisfies the other named exports worker.ts pulls in (unused in these tests).
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
  runDailyFeatureBuildForEnv: vi.fn(async () => ({
    cacheWarm: { status: "ok" },
    fromDate: "20260512",
    rowsFetched: 0,
    rowsWritten: 0,
    sourceScope: "all",
    toDate: "20260512",
  })),
  listDailyRaceEntriesForRace: vi.fn(async () => []),
}));
vi.mock("./win5-queue", () => ({
  handleWin5PredictionJob: vi.fn(async () => ({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0511",
    legCount: 0,
    modelVersion: "v1",
  })),
}));
vi.mock("./win5-cron", () => ({
  WIN5_DISCOVER_CRON: "0 0 * * *",
  logWin5CronResult: vi.fn(async () => {}),
}));
vi.mock("./running-style-cron", () => ({
  RUNNING_STYLE_INFERENCE_CRON: "*/10 * * * *",
  RUNNING_STYLE_PREWARM_CRON: "0 12 * * *",
  planRunningStylePredictionsForDate: vi.fn(async () => ({
    alreadyQueued: 0,
    completed: 0,
    date: "20260512",
    enqueued: 0,
    featureReady: 0,
    missingFeatures: 0,
    scanned: 0,
  })),
  refreshViewerRunningStyleCachesForDate: vi.fn(async () => ({
    date: "20260512",
    refreshed: 0,
    scanned: 0,
    skipped: 0,
  })),
  refreshViewerRunningStyleCacheForRace: vi.fn(async () => false),
}));
vi.mock("./running-style-queue", () => ({
  handleRunningStylePredictionJob: vi.fn(async () => null),
}));
vi.mock("./postgres", () => ({
  fetchJraRacesByDate: vi.fn(async () => []),
  fetchNarRacesByDate: vi.fn(async () => []),
}));
vi.mock("./keiba-go", async () => {
  const actual = await vi.importActual<typeof import("./keiba-go")>("./keiba-go");
  return {
    ...actual,
    fetchTodayRaceListUrls: vi.fn(async () => []),
    fetchOdds: vi.fn(async () => ({})),
    fetchRacePage: vi.fn(async () => "<html></html>"),
    fetchRaceLinksFromRaceList: vi.fn(async () => []),
    parseHorseWeights: vi.fn(() => []),
    parseRaceEntries: vi.fn(() => []),
    parseRaceResults: vi.fn(() => []),
  };
});
vi.mock("./jra", async () => {
  const actual = await vi.importActual<typeof import("./jra")>("./jra");
  return {
    ...actual,
    fetchJraResultHtmlWithPlaywright: vi.fn(async () => "<html></html>"),
    fetchJraOddsWithPlaywright: vi.fn(async () => ({ entryHtml: "", latest: {} })),
    parseJraHorseWeights: vi.fn(() => []),
    parseJraRaceEntries: vi.fn(() => []),
  };
});
vi.mock("./jra-track-condition", () => ({
  fetchJraTrackConditionWithPlaywright: vi.fn(async () => ({
    dirt: {
      condition: null,
      measurementDate: null,
      moisture: { finalBend: null, finalFurlong: null, measuredAt: null },
    },
    fetchedAt: "now",
    sourceUpdatedAt: null,
    turf: {
      condition: null,
      courseLayout: null,
      cushionMeasuredAt: null,
      cushionValue: null,
      going: null,
      height: { japaneseZoysiaGrass: null, perennialRyegrass: null },
      measurementDate: null,
      moisture: { finalBend: null, finalFurlong: null, measuredAt: null },
    },
    weather: null,
  })),
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
    parsePremiumPaddockBulletins: vi.fn(() => ({
      authRequired: false,
      bulletins: [],
      pending: false,
      unavailable: false,
    })),
  };
});

const buildEnv = (overrides?: Partial<Env>): Env => {
  return {
    PREMIUM_RACE_JOBS: { send: vi.fn(async () => {}), sendBatch: vi.fn(async () => {}) },
    REALTIME_DB: {},
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

it("handleJob delegates build-daily-features to runDailyFeatureBuildForEnv and logs ok", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch } = await import("./storage");
  const { runDailyFeatureBuildForEnv } = await import("./daily-feature-build");
  await handleJob(buildEnv(), {
    date: "20260512",
    type: "build-daily-features",
  });
  expect(runDailyFeatureBuildForEnv).toHaveBeenCalledTimes(1);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "build-daily-features",
    "ok",
    null,
    expect.any(String),
  );
});

it("handleJob delegates generate-win5-predictions to handleWin5PredictionJob", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch } = await import("./storage");
  const { handleWin5PredictionJob } = await import("./win5-queue");
  await handleJob(buildEnv(), {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0511",
    predictedAt: "2026-05-11T11:00:00.000Z",
    type: "generate-win5-predictions",
  });
  expect(handleWin5PredictionJob).toHaveBeenCalledTimes(1);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "generate-win5-predictions",
    "ok",
    "20260511",
    expect.any(String),
  );
});

it("handleJob delegates discover-win5-schedules to logWin5CronResult", async () => {
  const { handleJob } = await import("./worker");
  const { logWin5CronResult } = await import("./win5-cron");
  await handleJob(buildEnv(), { date: "20260512", type: "discover-win5-schedules" });
  expect(logWin5CronResult).toHaveBeenCalledTimes(1);
});

it("handleJob plan-running-style-predictions logs error when plan + cacheRefresh both reject", async () => {
  const { handleJob } = await import("./worker");
  const {
    planRunningStylePredictionsForDate,
    refreshViewerRunningStyleCachesForDate,
  } = await import("./running-style-cron");
  const { logFetch } = await import("./storage");
  vi.mocked(planRunningStylePredictionsForDate).mockRejectedValueOnce(new Error("plan boom"));
  vi.mocked(refreshViewerRunningStyleCachesForDate).mockRejectedValueOnce(
    new Error("cache boom"),
  );
  await handleJob(buildEnv(), { date: "20260512", type: "plan-running-style-predictions" });
  const args = vi.mocked(logFetch).mock.calls.at(-1);
  expect(args?.[2]).toBe("ok");
  expect(args?.[4]).toContain("plan boom");
  expect(args?.[4]).toContain("cache boom");
});

it("handleJob delegates plan-running-style-predictions to planRunningStylePredictionsForDate", async () => {
  const { handleJob } = await import("./worker");
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  await handleJob(buildEnv(), { date: "20260512", type: "plan-running-style-predictions" });
  expect(planRunningStylePredictionsForDate).toHaveBeenCalledTimes(1);
});

it("handleJob delegates generate-running-style-predictions to handleRunningStylePredictionJob", async () => {
  const { handleJob } = await import("./worker");
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  await handleJob(buildEnv(), {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "08",
    predictedAt: "2026-05-12T11:00:00.000Z",
    raceBango: "01",
    raceKey: "jra:20260512:08:01",
    source: "jra",
    type: "generate-running-style-predictions",
  });
  expect(handleRunningStylePredictionJob).toHaveBeenCalledTimes(1);
});

it("handleJob logs an error and rethrows when the dispatched action throws", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch } = await import("./storage");
  const { runDailyFeatureBuildForEnv } = await import("./daily-feature-build");
  vi.mocked(runDailyFeatureBuildForEnv).mockRejectedValueOnce(new Error("boom"));
  await expect(
    handleJob(buildEnv(), { date: "20260512", type: "build-daily-features" }),
  ).rejects.toThrow("boom");
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "build-daily-features",
    "error",
    null,
    "boom",
  );
});

it("handleJob fetch-odds returns ok after claim returns false (idempotent skip)", async () => {
  const { handleJob } = await import("./worker");
  const { claimOddsFetch, logFetch } = await import("./storage");
  vi.mocked(claimOddsFetch).mockResolvedValueOnce(false);
  await handleJob(buildEnv(), { raceKey: "jra:2026:0512:08:01", type: "fetch-odds" });
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-odds",
    "ok",
    "jra:2026:0512:08:01",
    null,
  );
});

it("handleJob fetch-results returns ok after claim returns false (idempotent skip)", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, logFetch } = await import("./storage");
  vi.mocked(claimResultFetch).mockResolvedValueOnce(false);
  await handleJob(buildEnv(), { raceKey: "jra:2026:0512:08:01", type: "fetch-results" });
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-results",
    "ok",
    "jra:2026:0512:08:01",
    null,
  );
});

it("handleJob discover-urls calls upsertDiscoveredUrls + discoverPremiumRacesForDate", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch } = await import("./storage");
  await handleJob(buildEnv(), { date: "20260512", type: "discover-urls" });
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "discover-urls",
    "ok",
    null,
    expect.any(String),
  );
});

it("handleJob plan-realtime-fetches without selfSchedule logs once", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch } = await import("./storage");
  await handleJob(buildEnv(), { date: "20260512", type: "plan-realtime-fetches" });
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-realtime-fetches",
    "ok",
    null,
    expect.any(String),
  );
});

it("handleJob discover-premium-race-links delegates to discoverPremiumRacesForDate", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch } = await import("./storage");
  await handleJob(buildEnv(), { date: "20260512", type: "discover-premium-race-links" });
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "discover-premium-race-links",
    "ok",
    null,
    expect.any(String),
  );
});

it("handleJob fetch-jra-track-condition delegates to fetchAndStoreJraTrackCondition", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch, claimTrackConditionFetch } = await import("./storage");
  vi.mocked(claimTrackConditionFetch).mockResolvedValueOnce(false);
  await handleJob(buildEnv(), {
    date: "20260512",
    keibajoCode: "08",
    type: "fetch-jra-track-condition",
  });
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-jra-track-condition",
    "ok",
    null,
    expect.any(String),
  );
});

it("handleJob fetch-odds throws when claim succeeds but race source is missing", async () => {
  const { handleJob } = await import("./worker");
  const { claimOddsFetch, getRaceSource } = await import("./storage");
  vi.mocked(claimOddsFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce(null);
  await expect(
    handleJob(buildEnv(), { raceKey: "jra:2026:0512:08:01", type: "fetch-odds" }),
  ).rejects.toThrow("race source not found");
});

it("handleJob fetch-results throws when claim succeeds but race source is missing", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, getRaceSource } = await import("./storage");
  vi.mocked(claimResultFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce(null);
  await expect(
    handleJob(buildEnv(), { raceKey: "jra:2026:0512:08:01", type: "fetch-results" }),
  ).rejects.toThrow("race source not found");
});

it("handleJob fetch-weights default branch throws when no race source", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource } = await import("./storage");
  vi.mocked(getRaceSource).mockResolvedValue(null);
  await expect(
    handleJob(buildEnv(), { raceKey: "jra:2026:0512:08:01", type: "fetch-weights" }),
  ).rejects.toThrow("race source not found");
});

it("handleJob fetch-premium-race-data returns ok when config incomplete", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch } = await import("./storage");
  await handleJob(buildEnv(), { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-race-data" });
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-premium-race-data",
    "ok",
    "jra:2026:0512:08:01",
    null,
  );
});

it("handleJob fetch-premium-paddock returns ok when config incomplete", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch } = await import("./storage");
  await handleJob(buildEnv(), { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-paddock" });
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-premium-paddock",
    "ok",
    "jra:2026:0512:08:01",
    null,
  );
});

it("handleJob plan-premium-race-data-fetches with config + races + candidates enqueues jobs", async () => {
  const { handleJob } = await import("./worker");
  const {
    listSchedulableRaceSourcesByDate,
    listPremiumRaceDataFetchCandidatesByDate,
    getPremiumRaceLink,
    markPremiumRaceDataQueued,
  } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([
    {
      babaCode: "08",
      debaUrl: "https://jra.example/race",
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
      resultFetchLockUntil: null,
      source: "jra",
      updatedAt: "2026-05-12T00:00:00+09:00",
    } as never,
  ]);
  vi.mocked(getPremiumRaceLink).mockResolvedValue({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  vi.mocked(listPremiumRaceDataFetchCandidatesByDate).mockResolvedValueOnce([
    { raceKey: "jra:2026:0512:08:01" },
  ]);
  await handleJob(
    buildEnv({ PREMIUM_RACE_ORIGIN: "https://x.test" } as never),
    { date: "20260512", type: "plan-premium-race-data-fetches" },
  );
  expect(markPremiumRaceDataQueued).toHaveBeenCalled();
});

it("handleJob plan-premium-race-data-fetches delegates to planPremiumRaceDataFetchesForDate", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch } = await import("./storage");
  await handleJob(buildEnv(), { date: "20260512", type: "plan-premium-race-data-fetches" });
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-premium-race-data-fetches",
    "ok",
    null,
    expect.any(String),
  );
});

it("handleJob discover-premium-races logs the discovery summary", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch } = await import("./storage");
  await handleJob(buildEnv(), { date: "20260512", type: "discover-premium-races" });
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "discover-premium-races",
    "ok",
    null,
    expect.any(String),
  );
});

it("handleJob discover-premium-races with full config fetches top + NAR top and links races", async () => {
  const { handleJob } = await import("./worker");
  const {
    listSchedulableRaceSourcesByDate,
    upsertPremiumRaceLink,
  } = await import("./storage");
  const { fetchPremiumHtml, discoverPremiumRaceLinks } = await import("./premium-race");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([
    {
      babaCode: "08",
      debaUrl: "https://jra.example/race",
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
      raceName: "JRA",
      raceStartAtJst: "2026-05-12T15:00:00+09:00",
      resultCompleteAt: null,
      resultFetchLockUntil: null,
      source: "jra",
      updatedAt: "2026-05-12T00:00:00+09:00",
    } as never,
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
      raceName: "NAR",
      raceStartAtJst: "2026-05-12T18:00:00+09:00",
      resultCompleteAt: null,
      resultFetchLockUntil: null,
      source: "nar",
      updatedAt: "2026-05-12T00:00:00+09:00",
    } as never,
  ]);
  vi.mocked(fetchPremiumHtml).mockResolvedValue("<html></html>");
  vi.mocked(discoverPremiumRaceLinks).mockReturnValue([
    {
      entryUrl: "https://x.test/race?race_id=202605120801",
      keibajoCode: "08",
      raceBango: "01",
      sourceRaceId: "202605120801",
    } as never,
  ]);
  await handleJob(
    buildEnv({
      PREMIUM_RACE_NAR_TOP_PATH_TEMPLATE: "/nar/{date}",
      PREMIUM_RACE_ORIGIN: "https://x.test",
      PREMIUM_RACE_TOP_PATH_TEMPLATE: "/top/{date}",
    } as never),
    { date: "20260512", type: "discover-premium-races" },
  );
  expect(fetchPremiumHtml).toHaveBeenCalledTimes(2);
  expect(upsertPremiumRaceLink).toHaveBeenCalled();
});

it("handleJob plan-realtime-fetches with selfSchedule logs twice and enqueues next plan", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch } = await import("./storage");
  await handleJob(buildEnv(), {
    date: "20260512",
    selfSchedule: true,
    type: "plan-realtime-fetches",
  });
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-realtime-fetches-self",
    "ok",
    null,
    expect.any(String),
  );
});

it("handleJob fetch-odds with NAR race source skips when no current odds slot", async () => {
  const { handleJob } = await import("./worker");
  const { claimOddsFetch, getRaceSource, failOddsFetch } = await import("./storage");
  vi.mocked(claimOddsFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce({
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
    raceStartAtJst: "2026-05-13T18:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "nar",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  await handleJob(buildEnv({ REALTIME_TEST_NOW: "2026-05-12T00:00:00.000Z" } as never), {
    raceKey: "nar:2026:0512:55:01",
    type: "fetch-odds",
  });
  expect(failOddsFetch).toHaveBeenCalledTimes(1);
});

it("handleJob fetch-weights with NAR race source short-circuits when no fetch URL", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch, getRaceSource } = await import("./storage");
  vi.mocked(getRaceSource).mockResolvedValue({
    babaCode: "22",
    debaUrl: "",
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
    raceStartAtJst: "2026-05-12T18:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "nar",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  await handleJob(buildEnv(), {
    raceKey: "nar:2026:0512:55:01",
    type: "fetch-weights",
  });
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-weights",
    "ok",
    "nar:2026:0512:55:01",
    null,
  );
});

it("handleJob fetch-odds JRA branch with weights inserts horse-weight snapshot", async () => {
  const { handleJob } = await import("./worker");
  const {
    claimOddsFetch,
    getRaceSource,
    insertOddsSnapshot,
    insertHorseWeightSnapshot,
    updateLastFetch,
    completeOddsFetch,
  } = await import("./storage");
  const { parseJraHorseWeights } = await import("./jra");
  vi.mocked(claimOddsFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce({
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
  } as never);
  vi.mocked(insertOddsSnapshot).mockResolvedValueOnce(3);
  vi.mocked(parseJraHorseWeights).mockReturnValueOnce([
    { changeAmount: 2, changeSign: "+", horseName: null, horseNumber: "1", weight: 500 },
  ]);
  await handleJob(
    buildEnv({ REALTIME_TEST_NOW: "2026-05-12T03:30:00.000Z" } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-odds" },
  );
  expect(insertHorseWeightSnapshot).toHaveBeenCalledTimes(1);
  expect(updateLastFetch).toHaveBeenCalledWith(
    expect.anything(),
    "jra:2026:0512:08:01",
    "last_weight_fetch_at",
    expect.any(String),
  );
  expect(completeOddsFetch).toHaveBeenCalledTimes(1);
});

it("handleJob fetch-odds with NAR race + valid slot fetches odds, updates oddsLinks, and enqueues next fetch", async () => {
  const { handleJob } = await import("./worker");
  const {
    claimOddsFetch,
    getRaceSource,
    insertOddsSnapshot,
    completeOddsFetch,
    markOddsFetchQueued,
    updateOddsLinks,
  } = await import("./storage");
  const { fetchOdds } = await import("./keiba-go");
  vi.mocked(claimOddsFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce({
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
    raceName: "T",
    raceStartAtJst: "2026-05-12T18:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "nar",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(fetchOdds).mockResolvedValueOnce({
    tansho: [{ combination: "1", odds: 2.5, rank: 1 }],
  } as never);
  vi.mocked(insertOddsSnapshot).mockResolvedValueOnce(3);
  await handleJob(
    buildEnv({ REALTIME_TEST_NOW: "2026-05-12T08:00:00.000Z" } as never),
    { raceKey: "nar:2026:0512:55:01", type: "fetch-odds" },
  );
  expect(completeOddsFetch).toHaveBeenCalledTimes(1);
  expect(updateOddsLinks).toHaveBeenCalledTimes(1);
  expect(markOddsFetchQueued).toHaveBeenCalled();
});

it("handleJob fetch-odds throws when insertOddsSnapshot returns 0 (NAR branch)", async () => {
  const { handleJob } = await import("./worker");
  const { claimOddsFetch, getRaceSource, insertOddsSnapshot, failOddsFetch } = await import(
    "./storage"
  );
  vi.mocked(claimOddsFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce({
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
    oddsLinks: { tansho: "/odds/tansho" },
    raceBango: "01",
    raceKey: "nar:2026:0512:55:01",
    raceName: "T",
    raceStartAtJst: "2026-05-12T18:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "nar",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(insertOddsSnapshot).mockResolvedValueOnce(0);
  await expect(
    handleJob(
      buildEnv({ REALTIME_TEST_NOW: "2026-05-12T08:00:00.000Z" } as never),
      { raceKey: "nar:2026:0512:55:01", type: "fetch-odds" },
    ),
  ).rejects.toThrow("odds rows are empty");
  expect(failOddsFetch).toHaveBeenCalledTimes(1);
});

it("handleJob fetch-odds with JRA race + valid odds slot writes a successful snapshot", async () => {
  const { handleJob } = await import("./worker");
  const {
    claimOddsFetch,
    getRaceSource,
    insertOddsSnapshot,
    completeOddsFetch,
    logFetch,
  } = await import("./storage");
  vi.mocked(claimOddsFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce({
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
  } as never);
  vi.mocked(insertOddsSnapshot).mockResolvedValueOnce(3);
  await handleJob(
    buildEnv({ REALTIME_TEST_NOW: "2026-05-12T03:30:00.000Z" } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-odds" },
  );
  expect(completeOddsFetch).toHaveBeenCalledTimes(1);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-odds",
    "ok",
    "jra:2026:0512:08:01",
    null,
  );
});

it("handleJob fetch-weights with NAR race source + debaUrl runs fetchOdds + insert weight", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, logFetch } = await import("./storage");
  vi.mocked(getRaceSource).mockResolvedValue({
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
    raceStartAtJst: "2026-05-12T18:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "nar",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  await handleJob(buildEnv(), {
    raceKey: "nar:2026:0512:55:01",
    type: "fetch-weights",
  });
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-weights",
    "ok",
    "nar:2026:0512:55:01",
    null,
  );
});

it("handleJob discover-urls exercises upsertDiscoveredUrls with NAR + JRA race rows", async () => {
  const { handleJob } = await import("./worker");
  const { fetchNarRacesByDate, fetchJraRacesByDate } = await import("./postgres");
  const { fetchTodayRaceListUrls, fetchRacePage } = await import("./keiba-go");
  vi.mocked(fetchNarRacesByDate).mockResolvedValueOnce([
    {
      hasso_jikoku: "1300",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0512",
      keibajo_code: "55",
      kyosomei_hondai: "Test NAR Race",
      race_bango: "1",
    },
  ] as never);
  vi.mocked(fetchJraRacesByDate).mockResolvedValueOnce([
    {
      hasso_jikoku: "1500",
      kaisai_kai: "02",
      kaisai_nen: "2026",
      kaisai_nichime: "06",
      kaisai_tsukihi: "0512",
      keibajo_code: "08",
      kyosomei_hondai: "Test JRA Race",
      race_bango: "1",
    },
  ] as never);
  vi.mocked(fetchTodayRaceListUrls).mockResolvedValueOnce([
    { babaCode: "30", url: "https://nankan.example/race-list" },
  ] as never);
  vi.mocked(fetchRacePage).mockResolvedValue("<html></html>");
  await handleJob(buildEnv(), { date: "20260512", type: "discover-urls" });
});

it("handleJob discover-urls exercises the inner NAR race-list link processing", async () => {
  const { handleJob } = await import("./worker");
  const { fetchJraRacesByDate, fetchNarRacesByDate } = await import("./postgres");
  const {
    fetchRacePage,
    fetchRaceLinksFromRaceList,
    fetchTodayRaceListUrls,
  } = await import("./keiba-go");
  const { upsertNarRaceSource, upsertJraRaceSource } = await import("./storage");
  vi.mocked(fetchJraRacesByDate).mockResolvedValueOnce([
    {
      hasso_jikoku: "1500",
      kaisai_kai: "02",
      kaisai_nen: "2026",
      kaisai_nichime: "06",
      kaisai_tsukihi: "0512",
      keibajo_code: "08",
      kyosomei_hondai: "JRA",
      race_bango: "1",
    },
  ] as never);
  vi.mocked(fetchNarRacesByDate).mockResolvedValueOnce([
    {
      hasso_jikoku: "1300",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0512",
      keibajo_code: "30",
      kyosomei_hondai: "NAR Local",
      race_bango: "1",
    },
  ] as never);
  vi.mocked(fetchTodayRaceListUrls).mockResolvedValueOnce([
    { babaCode: "36", url: "https://nankan.example/race-list" },
  ] as never);
  vi.mocked(fetchRaceLinksFromRaceList).mockResolvedValueOnce([
    {
      babaCode: "36",
      raceNumber: "1",
      url: "https://nankan.example/race?race_id=1",
    },
    {
      babaCode: "ZZ",
      raceNumber: "2",
      url: "https://nankan.example/race?race_id=2",
    },
  ] as never);
  vi.mocked(fetchRacePage).mockResolvedValue("<html></html>");
  await handleJob(buildEnv(), { date: "20260512", type: "discover-urls" });
  expect(upsertJraRaceSource).toHaveBeenCalled();
  expect(upsertNarRaceSource).toHaveBeenCalledTimes(1);
});

it("handleJob fetch-premium-race-data throws when origin set but no race link discovered", async () => {
  const { handleJob } = await import("./worker");
  await expect(
    handleJob(
      buildEnv({ PREMIUM_RACE_ORIGIN: "https://x.test" } as never),
      { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-race-data" },
    ),
  ).rejects.toThrow("premium race data fetch failed");
});

it("handleJob fetch-premium-paddock with valid link + attempts runs the parse + update path", async () => {
  const { handleJob } = await import("./worker");
  const {
    getRaceSource,
    getPremiumRaceLink,
    updatePremiumPaddockFetchState,
  } = await import("./storage");
  const { fetchPremiumHtmlAttempts } = await import("./premium-race");
  vi.mocked(getRaceSource).mockResolvedValue({
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
  } as never);
  vi.mocked(getPremiumRaceLink).mockResolvedValue({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  vi.mocked(fetchPremiumHtmlAttempts).mockResolvedValue([
    { html: "<table></table>", mode: "direct" },
  ] as never);
  await handleJob(
    buildEnv({
      PREMIUM_RACE_ORIGIN: "https://x.test",
      PREMIUM_RACE_PADDOCK_PATH_TEMPLATE: "/paddock/{sourceRaceId}",
    } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-paddock" },
  );
  expect(updatePremiumPaddockFetchState).toHaveBeenCalled();
});

it("handleJob fetch-premium-paddock skips when current state has future retryAfter", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, getPremiumPaddockFetchState, logFetch } = await import("./storage");
  vi.mocked(getRaceSource).mockResolvedValue({
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
  } as never);
  vi.mocked(getPremiumPaddockFetchState).mockResolvedValue({
    raceKey: "jra:2026:0512:08:01",
    retryAfter: "2099-01-01T00:00:00.000Z",
    status: "failed",
  } as never);
  await handleJob(
    buildEnv({
      PREMIUM_RACE_ORIGIN: "https://x.test",
      PREMIUM_RACE_PADDOCK_PATH_TEMPLATE: "/paddock/{sourceRaceId}",
    } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-paddock" },
  );
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-premium-paddock",
    "ok",
    "jra:2026:0512:08:01",
    null,
  );
});

it("handleJob fetch-premium-race-data with valid link + fetched HTML exercises full ingest path", async () => {
  const { handleJob } = await import("./worker");
  const {
    getRaceSource,
    getPremiumRaceLink,
    updatePremiumRaceDataFetchState,
  } = await import("./storage");
  const { fetchPremiumHtml } = await import("./premium-race");
  vi.mocked(getRaceSource).mockResolvedValue({
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
  } as never);
  vi.mocked(getPremiumRaceLink).mockResolvedValue({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  vi.mocked(fetchPremiumHtml).mockResolvedValue("<table></table>");
  await handleJob(
    buildEnv({
      PREMIUM_RACE_COMMENT_PATH_TEMPLATE: "/comment/{sourceRaceId}",
      PREMIUM_RACE_DATA_TOP_PATH_TEMPLATE: "/data-top/{sourceRaceId}",
      PREMIUM_RACE_ORIGIN: "https://x.test",
      PREMIUM_RACE_WORK_PATH_TEMPLATE: "/work/{sourceRaceId}",
    } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-race-data" },
  );
  expect(updatePremiumRaceDataFetchState).toHaveBeenCalled();
});

it("handleJob fetch-premium-paddock with origin + no race link returns ok early", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch } = await import("./storage");
  await handleJob(
    buildEnv({ PREMIUM_RACE_ORIGIN: "https://x.test" } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-paddock" },
  );
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-premium-paddock",
    "ok",
    "jra:2026:0512:08:01",
    null,
  );
});

it("handleJob fetch-jra-track-condition with successful claim runs the snapshot insert", async () => {
  const { handleJob } = await import("./worker");
  const {
    claimTrackConditionFetch,
    insertJraTrackConditionSnapshot,
    completeTrackConditionFetch,
  } = await import("./storage");
  vi.mocked(claimTrackConditionFetch).mockResolvedValueOnce(true);
  vi.mocked(insertJraTrackConditionSnapshot).mockResolvedValueOnce([
    { raceKey: "jra:2026:0512:08:01", raceStartAtJst: "2026-05-12T13:00:00+09:00" },
  ]);
  await handleJob(buildEnv(), {
    date: "20260512",
    keibajoCode: "08",
    type: "fetch-jra-track-condition",
  });
  expect(completeTrackConditionFetch).toHaveBeenCalledTimes(1);
});

it("handleJob fetch-jra-track-condition with successful claim and empty races falls through to fail", async () => {
  const { handleJob } = await import("./worker");
  const {
    claimTrackConditionFetch,
    insertJraTrackConditionSnapshot,
  } = await import("./storage");
  vi.mocked(claimTrackConditionFetch).mockResolvedValueOnce(true);
  vi.mocked(insertJraTrackConditionSnapshot).mockResolvedValueOnce([]);
  await handleJob(buildEnv(), {
    date: "20260512",
    keibajoCode: "08",
    type: "fetch-jra-track-condition",
  });
});

it("handleJob fetch-results with JRA race source completes when isRaceFinished", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, getRaceSource, completeResultFetch } = await import("./storage");
  vi.mocked(claimResultFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce({
    babaCode: "08",
    debaUrl: "https://www.jra.go.jp/race?race_id=202605120801",
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
  } as never);
  await handleJob(
    buildEnv({ REALTIME_TEST_NOW: "2026-05-12T07:00:00.000Z" } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-results" },
  );
  expect(completeResultFetch).toHaveBeenCalledTimes(1);
});

it("handleJob fetch-weights with JRA race source runs assert + insertHorseWeightSnapshot", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, insertHorseWeightSnapshot, updateLastFetch } = await import("./storage");
  const { parseJraHorseWeights } = await import("./jra");
  vi.mocked(getRaceSource).mockResolvedValueOnce({
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
  } as never);
  vi.mocked(parseJraHorseWeights).mockReturnValueOnce([
    { changeAmount: null, changeSign: null, horseName: null, horseNumber: "1", weight: 480 },
    { changeAmount: null, changeSign: null, horseName: null, horseNumber: "2", weight: 490 },
  ]);
  await handleJob(buildEnv(), {
    raceKey: "jra:2026:0512:08:01",
    type: "fetch-weights",
  });
  expect(insertHorseWeightSnapshot).toHaveBeenCalled();
  expect(updateLastFetch).toHaveBeenCalledWith(
    expect.anything(),
    "jra:2026:0512:08:01",
    "last_weight_fetch_at",
    expect.any(String),
  );
});

it("handleJob fetch-results with not-yet-finished race fails the fetch and returns", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, getRaceSource, failResultFetch } = await import("./storage");
  vi.mocked(claimResultFetch).mockReset();
  vi.mocked(claimResultFetch).mockResolvedValue(true);
  vi.mocked(getRaceSource).mockReset();
  vi.mocked(getRaceSource).mockResolvedValue({
    babaCode: "22",
    debaUrl: "https://nar.example/race",
    discoveredAt: "2099-05-12T00:00:00+09:00",
    kaisaiKai: null,
    kaisaiNen: "2099",
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
    raceKey: "nar:2099:0512:55:01",
    raceName: "T",
    raceStartAtJst: "2099-05-12T18:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "nar",
    updatedAt: "2099-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(failResultFetch).mockReset();
  await handleJob(buildEnv(), {
    raceKey: "nar:2026:0512:55:01",
    type: "fetch-results",
  });
  expect(failResultFetch).toHaveBeenCalled();
});

it("handleJob fetch-results NAR throws when results empty but expectedHorseCount > 0", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, getRaceSource, failResultFetch } = await import("./storage");
  const { parseRaceResults } = await import("./keiba-go");
  vi.mocked(claimResultFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce({
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
    raceName: "T",
    raceStartAtJst: "2026-05-12T18:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "nar",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(parseRaceResults).mockReturnValueOnce([]);
  const { fetchRacePage } = await import("./keiba-go");
  vi.mocked(fetchRacePage).mockImplementation(async (url: string) => {
    if (url.includes("result")) {
      return '<html><tr><td class="num">1</td></tr></html>';
    }
    return '<html><tr><td class="num">1</td></tr></html>';
  });
  vi.spyOn(
    await import("./keiba-go"),
    "parseRaceEntryHorseNumbers",
  ).mockReturnValue(["1", "2"]);
  vi.spyOn(
    await import("./keiba-go"),
    "parseRaceResultExcludedHorseNumbers",
  ).mockReturnValue([]);
  await expect(
    handleJob(buildEnv(), {
      raceKey: "nar:2026:0512:55:01",
      type: "fetch-results",
    }),
  ).rejects.toThrow();
  expect(failResultFetch).toHaveBeenCalled();
});

it("handleJob fetch-weights NAR + sparse weight rows (length 1) clears snapshot and throws", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, insertHorseWeightSnapshot } = await import("./storage");
  const { parseHorseWeights } = await import("./keiba-go");
  vi.mocked(getRaceSource).mockResolvedValueOnce({
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
    raceName: "T",
    raceStartAtJst: "2026-05-12T18:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "nar",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(parseHorseWeights).mockReturnValueOnce([
    { changeAmount: null, changeSign: null, horseName: null, horseNumber: "1", weight: 500 },
  ] as never);
  await expect(
    handleJob(buildEnv(), { raceKey: "nar:2026:0512:55:01", type: "fetch-weights" }),
  ).rejects.toThrow("horse weight rows are unexpectedly sparse");
  expect(insertHorseWeightSnapshot).toHaveBeenCalledWith(
    expect.anything(),
    "nar:2026:0512:55:01",
    expect.any(String),
    [],
  );
});

it("handleJob fetch-results with NAR race source completes when finish-position rows empty", async () => {
  const { handleJob } = await import("./worker");
  const {
    claimResultFetch,
    getRaceSource,
    completeResultFetch,
  } = await import("./storage");
  vi.mocked(claimResultFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce({
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
    raceStartAtJst: "2026-05-12T18:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "nar",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  await handleJob(buildEnv(), {
    raceKey: "nar:2026:0512:55:01",
    type: "fetch-results",
  });
  expect(completeResultFetch).toHaveBeenCalledTimes(1);
});
