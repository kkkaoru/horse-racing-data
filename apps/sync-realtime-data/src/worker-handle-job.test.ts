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
  claimResultCacheBust: vi.fn(async () => null),
  claimReservedWeightFetch: vi.fn(async () => true),
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
  RUNNING_STYLE_INFERENCE_CRON: "*/10 0-14 * * *",
  RUNNING_STYLE_PREWARM_CRON: "0 12 * * *",
  resolveRunningStyleCronDates: vi.fn(() => ["20260512"]),
  exportRunningStyleParquetsForDate: vi.fn(async () => ({
    bytesWritten: 100,
    fileCount: 1,
    rowCount: 3,
  })),
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
vi.mock("./running-style-feature-materialize", () => ({
  materializeRunningStyleFeatureParquetsForDate: vi.fn(async () => ({
    date: "20260602",
    materialized: 3,
    scanned: 3,
    skipped: 0,
  })),
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
    parseRaceResultHorseWeights: vi.fn((html: string) => actual.parseRaceResultHorseWeights(html)),
    parseRaceResults: vi.fn(() => []),
  };
});
vi.mock("./jra", async () => {
  const actual = await vi.importActual<typeof import("./jra")>("./jra");
  return {
    ...actual,
    fetchJraResultHtmlWithPlaywright: vi.fn(async () => "<html></html>"),
    fetchJraResultHtmlWithFallback: vi.fn(async () => "<html></html>"),
    fetchJraOddsWithPlaywright: vi.fn(async () => ({ entryHtml: "", latest: {} })),
    parseJraHorseWeights: vi.fn(() => []),
    parseJraRaceEntries: vi.fn(() => []),
    parseJraRaceResults: vi.fn(() => []),
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
    FINISH_POSITION_CRON: {
      fetch: vi.fn(
        async (): Promise<Response> => Response.json({ claimed: true, ok: true }, { status: 202 }),
      ),
    },
    PREMIUM_RACE_JOBS: { send: vi.fn(async () => {}), sendBatch: vi.fn(async () => {}) },
    REALTIME_DB: {},
    REALTIME_JOBS: { send: vi.fn(async () => {}), sendBatch: vi.fn(async () => {}) },
    REALTIME_TEST_NOW: "2026-05-12T12:00:00.000Z",
    TRIGGER_TOKEN: "secret-token",
    ...overrides,
  } as unknown as Env;
};

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("handleJob disables build-daily-features without running the legacy builder", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch } = await import("./storage");
  const { runDailyFeatureBuildForEnv } = await import("./daily-feature-build");
  await handleJob(buildEnv(), {
    date: "20260512",
    type: "build-daily-features",
  });
  expect(runDailyFeatureBuildForEnv).not.toHaveBeenCalled();
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "build-daily-features",
    "disabled",
    null,
    "Catalog service owns realtime feature builds",
  );
});

it("handleJob skips stale fetch-weights jobs before scraping", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch } = await import("./storage");
  const { fetchRacePage } = await import("./keiba-go");
  await handleJob(buildEnv({ REALTIME_TEST_NOW: "2026-07-02T06:00:00.000Z" }), {
    raceKey: "nar:2026:0630:48:01",
    type: "fetch-weights",
  });
  expect(fetchRacePage).not.toHaveBeenCalled();
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-weights",
    "skip:stale-live-job",
    "nar:2026:0630:48:01",
    JSON.stringify({ raceDate: "20260630", today: "20260702" }),
    undefined,
  );
});

it("handleJob skips stale fetch-results jobs before claiming result fetch", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, logFetch } = await import("./storage");
  await handleJob(buildEnv({ REALTIME_TEST_NOW: "2026-07-02T06:00:00.000Z" }), {
    raceKey: "nar:2026:0630:50:01",
    type: "fetch-results",
  });
  expect(claimResultFetch).not.toHaveBeenCalled();
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-results",
    "skip:stale-live-job",
    "nar:2026:0630:50:01",
    JSON.stringify({ raceDate: "20260630", today: "20260702" }),
    undefined,
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
  const { planRunningStylePredictionsForDate, refreshViewerRunningStyleCachesForDate } =
    await import("./running-style-cron");
  const { logFetch } = await import("./storage");
  vi.mocked(planRunningStylePredictionsForDate).mockRejectedValueOnce(new Error("plan boom"));
  vi.mocked(refreshViewerRunningStyleCachesForDate).mockRejectedValueOnce(new Error("cache boom"));
  await handleJob(buildEnv(), { date: "20260512", type: "plan-running-style-predictions" });
  const args = vi.mocked(logFetch).mock.calls.at(-1);
  expect(args?.[2]).toBe("ok");
  expect(args?.[4]).toContain("plan boom");
  expect(args?.[4]).toContain("cache boom");
});

it("handleJob delegates plan-running-style-predictions to planRunningStylePredictionsForDate", async () => {
  const { handleJob } = await import("./worker");
  const { exportRunningStyleParquetsForDate, planRunningStylePredictionsForDate } =
    await import("./running-style-cron");
  await handleJob(buildEnv(), { date: "20260512", type: "plan-running-style-predictions" });
  expect(planRunningStylePredictionsForDate).toHaveBeenCalledTimes(1);
  expect(exportRunningStyleParquetsForDate).toHaveBeenCalledWith(expect.anything(), "20260512");
});

it("handleJob falls back to the system clock when the injected clock is invalid", async () => {
  vi.useFakeTimers();
  const now = new Date("2026-05-12T12:34:56.000Z");
  vi.setSystemTime(now);
  try {
    const { handleJob } = await import("./worker");
    const { planRunningStylePredictionsForDate } = await import("./running-style-cron");

    await handleJob(buildEnv({ REALTIME_TEST_NOW: "not-a-date" }), {
      date: "20260512",
      type: "plan-running-style-predictions",
    });

    expect(planRunningStylePredictionsForDate).toHaveBeenCalledWith(
      expect.anything(),
      "20260512",
      now,
    );
  } finally {
    vi.useRealTimers();
  }
});

it("handleJob continues running-style planning when feature warm is incomplete", async () => {
  const { handleJob } = await import("./worker");
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { materializeRunningStyleFeatureParquetsForDate } =
    await import("./running-style-feature-materialize");
  const { logFetch } = await import("./storage");
  vi.mocked(materializeRunningStyleFeatureParquetsForDate).mockResolvedValueOnce({
    date: "20260512",
    materializeError: "feature warm failed",
    materialized: 0,
    scanned: 1,
    skipped: 1,
  });

  await handleJob(buildEnv(), { date: "20260512", type: "plan-running-style-predictions" });

  expect(planRunningStylePredictionsForDate).toHaveBeenCalledTimes(1);
  expect(vi.mocked(logFetch).mock.calls.at(-1)?.[4]).toContain("feature warm failed");
});

it("handleJob continues running-style planning when feature warm throws", async () => {
  const { handleJob } = await import("./worker");
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { materializeRunningStyleFeatureParquetsForDate } =
    await import("./running-style-feature-materialize");
  const { logFetch } = await import("./storage");
  vi.mocked(materializeRunningStyleFeatureParquetsForDate).mockRejectedValueOnce(
    new Error("feature warm crashed"),
  );

  await handleJob(buildEnv(), { date: "20260512", type: "plan-running-style-predictions" });

  expect(planRunningStylePredictionsForDate).toHaveBeenCalledTimes(1);
  const detail = vi.mocked(logFetch).mock.calls.at(-1)?.[4];
  expect(detail).toContain("feature warm crashed");
  expect(detail).toContain('"scanned":0');
});

it("handleJob materialize-running-style-features logs the materialize summary on success", async () => {
  const { handleJob } = await import("./worker");
  const { materializeRunningStyleFeatureParquetsForDate } =
    await import("./running-style-feature-materialize");
  const { logFetch } = await import("./storage");
  await handleJob(buildEnv(), { date: "20260602", type: "materialize-running-style-features" });
  expect(materializeRunningStyleFeatureParquetsForDate).toHaveBeenCalledTimes(1);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "materialize-running-style-features",
    "ok",
    null,
    '{"date":"20260602","materialized":3,"scanned":3,"skipped":0}',
  );
});

it("handleJob materialize-running-style-features logs the error shape when materialize rejects", async () => {
  const { handleJob } = await import("./worker");
  const { materializeRunningStyleFeatureParquetsForDate } =
    await import("./running-style-feature-materialize");
  const { logFetch } = await import("./storage");
  vi.mocked(materializeRunningStyleFeatureParquetsForDate).mockRejectedValueOnce(
    new Error("materialize boom"),
  );
  await handleJob(buildEnv(), { date: "20260602", type: "materialize-running-style-features" });
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "materialize-running-style-features",
    "ok",
    null,
    '{"error":"materialize boom"}',
  );
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
  const { handleWin5PredictionJob } = await import("./win5-queue");
  vi.mocked(handleWin5PredictionJob).mockRejectedValueOnce(new Error("boom"));
  await expect(
    handleJob(buildEnv(), {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      predictedAt: "2026-05-12T03:00:00.000Z",
      type: "generate-win5-predictions",
    }),
  ).rejects.toThrow("boom");
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "generate-win5-predictions",
    "error",
    null,
    "boom",
    undefined,
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

it("handleJob fetch-premium-paddock logs skip:non-jra and does not call the fetcher for non-JRA race keys", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch, getRaceSource } = await import("./storage");
  await handleJob(buildEnv(), { raceKey: "nar:2026:0512:06:01", type: "fetch-premium-paddock" });
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-premium-paddock",
    "skip:non-jra",
    "nar:2026:0512:06:01",
    null,
  );
  expect(getRaceSource).not.toHaveBeenCalled();
});

it("handleJob fetch-premium-race-data calls the fetcher for non-Ban-ei nar race keys (NAR re-enabled 2026-07-04)", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch, getRaceSource } = await import("./storage");
  await handleJob(buildEnv(), { raceKey: "nar:2026:0629:44:01", type: "fetch-premium-race-data" });
  expect(getRaceSource).toHaveBeenCalled();
  expect(logFetch).not.toHaveBeenCalledWith(
    expect.anything(),
    "fetch-premium-race-data",
    "skip:non-jra",
    "nar:2026:0629:44:01",
    null,
  );
});

it("handleJob fetch-premium-race-data logs skip:non-jra for Ban-ei nar race keys (keibajo 83)", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch, getRaceSource } = await import("./storage");
  await handleJob(buildEnv(), { raceKey: "nar:2026:0628:83:04", type: "fetch-premium-race-data" });
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-premium-race-data",
    "skip:non-jra",
    "nar:2026:0628:83:04",
    null,
  );
  expect(getRaceSource).not.toHaveBeenCalled();
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
  await handleJob(buildEnv({ PREMIUM_RACE_ORIGIN: "https://x.test" } as never), {
    date: "20260512",
    type: "plan-premium-race-data-fetches",
  });
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
  const { listSchedulableRaceSourcesByDate, upsertPremiumRaceLink } = await import("./storage");
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

it("handleJob plan-realtime-fetches with selfSchedule logs without chaining another plan", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch } = await import("./storage");
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_JOBS: { send, sendBatch: vi.fn(async () => {}) },
  } as never);
  await handleJob(env, {
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
  expect(send).not.toHaveBeenCalled();
});

it("handleJob fetch-weights with NAR race source records pending when no rows are published", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch, getRaceSource, updateLastFetch } = await import("./storage");
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
  expect(updateLastFetch).toHaveBeenCalledWith(
    expect.anything(),
    "nar:2026:0512:55:01",
    "last_weight_fetch_soft_miss_at",
    expect.any(String),
  );
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-weights",
    "pending:weights-unavailable",
    "nar:2026:0512:55:01",
    "count=0 upstream-not-published",
  );
});

it("handleJob fetch-weights binds a failed direct trigger to an exact-generation repair job", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, insertHorseWeightSnapshot, logFetch } = await import("./storage");
  const { parseHorseWeights, parseRaceEntries } = await import("./keiba-go");
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
  vi.mocked(parseHorseWeights).mockReturnValueOnce([
    {
      changeAmount: 4,
      changeSign: "+",
      horseName: "WeightA",
      horseNumber: "1",
      weight: 482,
    },
    {
      changeAmount: 2,
      changeSign: "-",
      horseName: "WeightC",
      horseNumber: "3",
      weight: 510,
    },
  ]);
  vi.mocked(parseRaceEntries).mockReturnValueOnce([
    { horseName: "WeightA", horseNumber: "1", jockeyName: "JockeyA", status: null },
    {
      horseName: "CanceledB",
      horseNumber: "2",
      jockeyName: "JockeyB",
      status: "出走取消",
    },
    { horseName: "WeightC", horseNumber: "3", jockeyName: "JockeyC", status: null },
  ]);
  const repairSend = vi.fn(async () => undefined);
  await handleJob(
    buildEnv({
      FINISH_POSITION_CRON: {
        fetch: vi.fn(async (): Promise<Response> => Response.json({ ok: false }, { status: 503 })),
      },
      REALTIME_JOBS: { send: repairSend, sendBatch: vi.fn(async () => undefined) } as never,
    }),
    {
      raceKey: "nar:2026:0512:55:01",
      type: "fetch-weights",
    },
  );
  expect(insertHorseWeightSnapshot).toHaveBeenCalled();
  expect(repairSend).toHaveBeenCalledWith({
    raceKey: "nar:2026:0512:55:01",
    type: "fetch-weights",
    weightGeneration: {
      activeHorseNumbers: [1, 3],
      entrySnapshotFetchedAt: "2026-05-12T21:00:00+09:00",
      entrySnapshotHash: "00574dee8f89ae6c93ded1fc8187aa58e1a9d460db315e00c1d782296997e5d7",
      excludedHorseNumbers: [2],
      weightSnapshotCount: 2,
      weightSnapshotFetchedAt: "2026-05-12T21:00:00+09:00",
      weightSnapshotHash: "5bdaf5c6c5a84a7efbfae1f4f8e5eceea79123dd8386a0e23c0df05581411bfc",
    },
  });
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-weights",
    "ok",
    "nar:2026:0512:55:01",
    null,
  );
});

// Bug B regression: fetch-weights pushes an entry-only row to the
// race-trend DO so a viewer hitting the next race in the venue card sees
// the pre-result sibling row immediately, without waiting for either the
// 60s alarm self-pull or the first result fetch. Pre-fix race 10 of a
// venue was invisible to the viewer between its entry-fetch lead time and
// its first result-fetch push.
it("handleJob fetch-weights pushes the entry-only row to RACE_TREND_DAILY_TRACK_DO when entries parsed", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource } = await import("./storage");
  const { parseRaceEntries } = await import("./keiba-go");
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
    raceBango: "10",
    raceKey: "nar:2026:0512:55:10",
    raceName: "EntryPush",
    raceStartAtJst: "2026-05-12T18:15:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "nar",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(parseRaceEntries).mockReturnValue([
    { horseName: "EntryA", horseNumber: "1", jockeyName: "JockeyA", status: null },
    { horseName: "EntryB", horseNumber: "2", jockeyName: "JockeyB", status: null },
    { horseName: "EntryC", horseNumber: "3", jockeyName: "JockeyC", status: null },
  ]);
  const stubFetch = vi.fn(
    async (_url: string, _init?: RequestInit): Promise<Response> =>
      new Response(JSON.stringify({ ok: true }), { status: 200 }),
  );
  const idFromName = vi.fn((name: string): string => name);
  const get = vi.fn((_id: string) => ({ fetch: stubFetch }));
  await handleJob(
    buildEnv({
      RACE_TREND_DAILY_TRACK_DO: { get, idFromName } as never,
    }),
    { raceKey: "nar:2026:0512:55:10", type: "fetch-weights" },
  );
  expect(idFromName).toHaveBeenCalledWith("nar:20260512:55");
  expect(stubFetch).toHaveBeenCalledTimes(1);
  expect(stubFetch.mock.calls[0]![0]).toBe("https://race-trend-daily-track-do/push");
  const body = stubFetch.mock.calls[0]![1]!.body;
  if (typeof body !== "string") throw new Error("expected push body to be a JSON string");
  const parsed = JSON.parse(body) as {
    isComplete: boolean;
    raceBango: string;
    raceKey: string;
    starterRows: Array<{ finishPosition: number; umaban: string }>;
  };
  expect(parsed.raceBango).toBe("10");
  expect(parsed.raceKey).toBe("nar:2026:0512:55:10");
  expect(parsed.isComplete).toBe(false);
  expect(parsed.starterRows.map((row) => row.umaban)).toStrictEqual(["1", "2", "3"]);
  expect(parsed.starterRows.map((row) => row.finishPosition)).toStrictEqual([0, 0, 0]);
});

// Defensive: with zero parsed entries (NAR entry HTML parse failure), the
// DO push must NOT fire because an empty starter row list would not help
// the viewer surface a sibling row and would waste a DO write.
it("handleJob fetch-weights skips the entry-only DO push when parseRaceEntries returns []", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource } = await import("./storage");
  const { parseRaceEntries } = await import("./keiba-go");
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
    raceBango: "10",
    raceKey: "nar:2026:0512:55:10",
    raceName: "EntryEmpty",
    raceStartAtJst: "2026-05-12T18:15:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "nar",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(parseRaceEntries).mockReturnValue([]);
  const stubFetch = vi.fn(
    async (_url: string, _init?: RequestInit): Promise<Response> =>
      new Response(JSON.stringify({ ok: true }), { status: 200 }),
  );
  const idFromName = vi.fn((name: string): string => name);
  const get = vi.fn((_id: string) => ({ fetch: stubFetch }));
  await handleJob(
    buildEnv({
      RACE_TREND_DAILY_TRACK_DO: { get, idFromName } as never,
    }),
    { raceKey: "nar:2026:0512:55:10", type: "fetch-weights" },
  );
  expect(stubFetch).toHaveBeenCalledTimes(0);
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

it("upsertDiscoveredUrls reads NAR and JRA Postgres sources serially", async () => {
  const { upsertDiscoveredUrls } = await import("./worker");
  const { fetchNarRacesByDate, fetchJraRacesByDate } = await import("./postgres");
  const { fetchTodayRaceListUrls } = await import("./keiba-go");
  const narRows = Promise.withResolvers<never[]>();
  vi.mocked(fetchTodayRaceListUrls).mockResolvedValueOnce([]);
  vi.mocked(fetchNarRacesByDate).mockReturnValueOnce(narRows.promise);
  vi.mocked(fetchJraRacesByDate).mockResolvedValueOnce([]);

  const discovery = upsertDiscoveredUrls(buildEnv(), "20260512", {
    sleep: async () => undefined,
  });
  await vi.waitFor(() => expect(fetchNarRacesByDate).toHaveBeenCalledTimes(1));
  expect(fetchJraRacesByDate).not.toHaveBeenCalled();

  narRows.resolve([]);
  await discovery;

  expect(fetchJraRacesByDate).toHaveBeenCalledTimes(1);
});

it("handleJob discover-urls exercises the inner NAR race-list link processing", async () => {
  const { handleJob } = await import("./worker");
  const { fetchJraRacesByDate, fetchNarRacesByDate } = await import("./postgres");
  const { fetchRacePage, fetchRaceLinksFromRaceList, fetchTodayRaceListUrls } =
    await import("./keiba-go");
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
  const { getRaceSource, getPremiumRaceLink } = await import("./storage");
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
  vi.mocked(getPremiumRaceLink).mockResolvedValue(null);
  await expect(
    handleJob(buildEnv({ PREMIUM_RACE_ORIGIN: "https://x.test" } as never), {
      raceKey: "jra:2026:0512:08:01",
      type: "fetch-premium-race-data",
    }),
  ).rejects.toThrow("premium race data fetch failed");
});

it("handleJob fetch-premium-paddock with valid link + attempts runs the parse + update path", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, getPremiumRaceLink, updatePremiumPaddockFetchState } =
    await import("./storage");
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

it("handleJob fetch-premium-race-data records auth_required when comment HTML lacks the authenticated marker", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, getPremiumRaceLink, updatePremiumRaceDataFetchState } =
    await import("./storage");
  const { fetchPremiumHtml } = await import("./premium-race");
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
    raceName: "T",
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "jra",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  vi.mocked(fetchPremiumHtml).mockImplementation(async (_config: unknown, url: unknown) => {
    if (typeof url === "string" && url.includes("/c/")) {
      return "<div>unauthenticated comment</div>";
    }
    return "";
  });
  await handleJob(
    buildEnv({
      PREMIUM_RACE_COMMENT_PATH_TEMPLATE: "/c/{sourceRaceId}",
      PREMIUM_RACE_DATA_TOP_PATH_TEMPLATE: "/d/{sourceRaceId}",
      PREMIUM_RACE_ORIGIN: "https://x.test",
      PREMIUM_RACE_WORK_PATH_TEMPLATE: "/w/{sourceRaceId}",
    } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-race-data" },
  );
  expect(vi.mocked(updatePremiumRaceDataFetchState).mock.calls.at(-1)?.[1]).toMatchObject({
    status: "auth_required",
  });
});

it("handleJob fetch-premium-race-data records empty when all fetch results are blank", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, getPremiumRaceLink, updatePremiumRaceDataFetchState } =
    await import("./storage");
  const { fetchPremiumHtml } = await import("./premium-race");
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
    raceName: "T",
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "jra",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  // workHtml="x" (truthy so we don't enter the throw branch) but trainingReviews empty
  vi.mocked(fetchPremiumHtml).mockImplementation(async (_config: unknown, url: unknown) => {
    if (typeof url === "string" && url.includes("/w/")) {
      return "<div>no rows</div>";
    }
    return "";
  });
  await handleJob(
    buildEnv({
      PREMIUM_RACE_COMMENT_PATH_TEMPLATE: "/c/{sourceRaceId}",
      PREMIUM_RACE_DATA_TOP_PATH_TEMPLATE: "/d/{sourceRaceId}",
      PREMIUM_RACE_ORIGIN: "https://x.test",
      PREMIUM_RACE_WORK_PATH_TEMPLATE: "/w/{sourceRaceId}",
    } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-race-data" },
  );
  expect(vi.mocked(updatePremiumRaceDataFetchState).mock.calls.at(-1)?.[1]).toMatchObject({
    status: "empty",
  });
});

// 2026-07-04: fetchAndStorePremiumRaceData must bust the viewer's per-race
// cache once new premium data lands, otherwise a same-day cache pre-warm
// that ran before the premium fetch keeps serving a stale/empty training
// section until race-start+6h. Only fires when hasAnyData is true (see the
// paired "does not bust" test below for the empty-fetch case).
it("handleJob fetch-premium-race-data busts the viewer race cache when the fetch produces data (hasAnyData=true)", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, getPremiumRaceLink } = await import("./storage");
  const { fetchPremiumHtml } = await import("./premium-race");
  const premiumRace = await import("./premium-race");
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
    raceName: "T",
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "jra",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  vi.spyOn(premiumRace, "parsePremiumTrainingReviews").mockReturnValue([
    {
      commentText: "good",
      evaluationGrade: null,
      evaluationText: null,
      horseName: "馬1",
      horseNumber: "1",
      riderName: null,
      trainingDate: "2026-05-10",
    },
  ]);
  vi.mocked(fetchPremiumHtml).mockImplementation(async (_config: unknown, url: unknown) => {
    if (typeof url === "string" && url.includes("/w/")) {
      return "<table>work</table>";
    }
    return "";
  });
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));
  await handleJob(
    buildEnv({
      PC_KEIBA_VIEWER_INTERNAL_TOKEN: "secret-token",
      PREMIUM_RACE_COMMENT_PATH_TEMPLATE: "/c/{sourceRaceId}",
      PREMIUM_RACE_DATA_TOP_PATH_TEMPLATE: "/d/{sourceRaceId}",
      PREMIUM_RACE_ORIGIN: "https://x.test",
      PREMIUM_RACE_WORK_PATH_TEMPLATE: "/w/{sourceRaceId}",
      RUNNING_STYLE_CACHE_ORIGIN: "https://viewer.test",
    } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-race-data" },
  );
  const bustCall = fetchSpy.mock.calls.find(
    (args) => args[0] === "https://viewer.test/api/internal/race-cache-bust",
  );
  expect(bustCall?.[1]?.method).toBe("POST");
  expect(bustCall?.[1]?.body).toBe(
    '{"keibajoCode":"08","mmdd":"0512","raceBango":"01","source":"jra","year":"2026"}',
  );
});

it("handleJob fetch-premium-race-data skips the viewer bust when durable premium content is unchanged", async () => {
  const { handleJob } = await import("./worker");
  const { getPremiumRaceLink, getPremiumRacePayload, getRaceSource } = await import("./storage");
  const { fetchPremiumHtml } = await import("./premium-race");
  const premiumRace = await import("./premium-race");
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
    raceName: "T",
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "jra",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  vi.mocked(getPremiumRacePayload).mockResolvedValueOnce({
    dataTopHorses: [
      {
        fetchedAt: "2026-05-12T10:00:00+09:00",
        horseName: "馬1",
        horseNumber: "1",
        rank: 1,
        reasons: ["好材料"],
      },
    ],
    paddockBulletins: [],
    stableComments: [
      {
        commentText: "順調",
        evaluationGrade: 1,
        evaluationText: "◎",
        fetchedAt: "2026-05-12T10:00:00+09:00",
        frameNumber: "1",
        horseName: "馬1",
        horseNumber: "1",
      },
    ],
    trainingReviews: [
      {
        commentText: "good",
        evaluationGrade: null,
        evaluationText: null,
        fetchedAt: "2026-05-12T10:00:00+09:00",
        horseName: "馬1",
        horseNumber: "1",
        riderName: null,
        trainingDate: "2026-05-10",
      },
    ],
  });
  vi.spyOn(premiumRace, "parsePremiumTrainingReviews").mockReturnValue([
    {
      commentText: "good",
      evaluationGrade: null,
      evaluationText: null,
      horseName: "馬1",
      horseNumber: "1",
      riderName: null,
      trainingDate: "2026-05-10",
    },
  ]);
  vi.spyOn(premiumRace, "parsePremiumStableComments").mockReturnValue([
    {
      commentText: "順調",
      evaluationGrade: 1,
      evaluationText: "◎",
      frameNumber: "1",
      horseName: "馬1",
      horseNumber: "1",
    },
  ]);
  vi.spyOn(premiumRace, "parsePremiumDataTopHorses").mockReturnValue([
    { horseName: "馬1", horseNumber: "1", rank: 1, reasons: ["好材料"] },
  ]);
  vi.mocked(fetchPremiumHtml).mockImplementation(async (_config: unknown, url: unknown) => {
    if (typeof url === "string" && url.includes("/w/")) {
      return "<table>work</table>";
    }
    if (typeof url === "string" && url.includes("/c/")) {
      return '<table class="Comment_Table_Show_All"></table>';
    }
    return '<div class="Icon_Account"></div>';
  });
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));
  await handleJob(
    buildEnv({
      PC_KEIBA_VIEWER_INTERNAL_TOKEN: "secret-token",
      PREMIUM_RACE_COMMENT_PATH_TEMPLATE: "/c/{sourceRaceId}",
      PREMIUM_RACE_DATA_TOP_PATH_TEMPLATE: "/d/{sourceRaceId}",
      PREMIUM_RACE_ORIGIN: "https://x.test",
      PREMIUM_RACE_WORK_PATH_TEMPLATE: "/w/{sourceRaceId}",
      RUNNING_STYLE_CACHE_ORIGIN: "https://viewer.test",
    } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-race-data" },
  );
  expect(
    fetchSpy.mock.calls.some(
      (args) => args[0] === "https://viewer.test/api/internal/race-cache-bust",
    ),
  ).toBe(false);
});

it("handleJob fetch-premium-race-data does not bust the viewer race cache when the fetch produces nothing (hasAnyData=false)", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, getPremiumRaceLink, updatePremiumRaceDataFetchState } =
    await import("./storage");
  const { fetchPremiumHtml } = await import("./premium-race");
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
    raceName: "T",
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "jra",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  // workHtml="x" (truthy so we don't enter the throw branch) but trainingReviews empty
  vi.mocked(fetchPremiumHtml).mockImplementation(async (_config: unknown, url: unknown) => {
    if (typeof url === "string" && url.includes("/w/")) {
      return "<div>no rows</div>";
    }
    return "";
  });
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));
  await handleJob(
    buildEnv({
      PC_KEIBA_VIEWER_INTERNAL_TOKEN: "secret-token",
      PREMIUM_RACE_COMMENT_PATH_TEMPLATE: "/c/{sourceRaceId}",
      PREMIUM_RACE_DATA_TOP_PATH_TEMPLATE: "/d/{sourceRaceId}",
      PREMIUM_RACE_ORIGIN: "https://x.test",
      PREMIUM_RACE_WORK_PATH_TEMPLATE: "/w/{sourceRaceId}",
      RUNNING_STYLE_CACHE_ORIGIN: "https://viewer.test",
    } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-race-data" },
  );
  expect(vi.mocked(updatePremiumRaceDataFetchState).mock.calls.at(-1)?.[1]).toMatchObject({
    status: "empty",
  });
  expect(
    fetchSpy.mock.calls.some(
      (args) => args[0] === "https://viewer.test/api/internal/race-cache-bust",
    ),
  ).toBe(false);
});

it("handleJob fetch-premium-race-data records workError when fetch rejects with a non-Error reason", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, getPremiumRaceLink, updatePremiumRaceDataFetchState } =
    await import("./storage");
  const { fetchPremiumHtml } = await import("./premium-race");
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
    raceName: "T",
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "jra",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  // work rejects with a string (non-Error), comment ok, data-top ok
  vi.mocked(fetchPremiumHtml).mockImplementation(async (_config: unknown, url: unknown) => {
    if (typeof url === "string" && url.includes("/w/")) {
      throw "string-error" as never;
    }
    if (typeof url === "string" && url.includes("/c/")) {
      return "<table></table>";
    }
    return "<table></table>";
  });
  await handleJob(
    buildEnv({
      PREMIUM_RACE_COMMENT_PATH_TEMPLATE: "/c/{sourceRaceId}",
      PREMIUM_RACE_DATA_TOP_PATH_TEMPLATE: "/d/{sourceRaceId}",
      PREMIUM_RACE_ORIGIN: "https://x.test",
      PREMIUM_RACE_WORK_PATH_TEMPLATE: "/w/{sourceRaceId}",
    } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-race-data" },
  );
  const lastMessage = JSON.parse(
    String(vi.mocked(updatePremiumRaceDataFetchState).mock.calls.at(-1)?.[1].message ?? "{}"),
  );
  expect(lastMessage.workError).toBe("string-error");
});

it("handleJob fetch-premium-race-data with non-empty dataTopHorses writes the data-top cache", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, getPremiumRaceLink } = await import("./storage");
  const { fetchPremiumHtml } = await import("./premium-race");
  const premiumRace = await import("./premium-race");
  const { putPremiumDataTopCache } = await import("./premium-data-top-cache");
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
    raceName: "T",
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
  vi.mocked(fetchPremiumHtml).mockResolvedValue(
    '<div class="Icon_Account"></div><table>data</table>',
  );
  vi.spyOn(premiumRace, "parsePremiumDataTopHorses").mockReturnValue([
    { horseName: "馬1", horseNumber: "1", rank: 1, reasons: ["a"] },
  ]);
  await handleJob(
    buildEnv({
      PREMIUM_RACE_COMMENT_PATH_TEMPLATE: "/c/{sourceRaceId}",
      PREMIUM_RACE_DATA_TOP_PATH_TEMPLATE: "/d/{sourceRaceId}",
      PREMIUM_RACE_ORIGIN: "https://x.test",
      PREMIUM_RACE_WORK_PATH_TEMPLATE: "/w/{sourceRaceId}",
    } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-race-data" },
  );
  expect(putPremiumDataTopCache).toHaveBeenCalled();
});

it("handleJob fetch-premium-race-data records auth_required with retryAfter when login prompt is detected", async () => {
  const { handleJob } = await import("./worker");
  const {
    getRaceSource,
    getPremiumRaceLink,
    getPremiumRaceDataFetchState,
    updatePremiumRaceDataFetchState,
    replacePremiumRaceData,
  } = await import("./storage");
  const { fetchPremiumHtml } = await import("./premium-race");
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
    raceName: "T",
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "jra",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  vi.mocked(getPremiumRaceDataFetchState).mockResolvedValueOnce(null);
  vi.mocked(fetchPremiumHtml).mockResolvedValue(
    "<html><body>プレミアムサービス 登録でご覧になれます</body></html>",
  );
  await handleJob(
    buildEnv({
      PREMIUM_RACE_COMMENT_PATH_TEMPLATE: "/c/{sourceRaceId}",
      PREMIUM_RACE_DATA_TOP_PATH_TEMPLATE: "/d/{sourceRaceId}",
      PREMIUM_RACE_ORIGIN: "https://x.test",
      PREMIUM_RACE_WORK_PATH_TEMPLATE: "/w/{sourceRaceId}",
    } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-race-data" },
  );
  const updateCall = vi.mocked(updatePremiumRaceDataFetchState).mock.calls.at(-1)?.[1];
  expect(updateCall?.status).toBe("auth_required");
  expect(typeof updateCall?.retryAfter).toBe("string");
  const replaceCall = vi.mocked(replacePremiumRaceData).mock.calls.at(-1)?.[1];
  expect(replaceCall?.dataTopHorses).toBeUndefined();
  const parsedMessage = JSON.parse(String(updateCall?.message ?? "{}"));
  expect(parsedMessage.loginPromptDetected).toBe(true);
  expect(parsedMessage.authRetryCount).toBe(1);
});

it("handleJob fetch-premium-race-data suppresses the unauthenticated data-top teaser page and marks auth_required", async () => {
  const { handleJob } = await import("./worker");
  const {
    getRaceSource,
    getPremiumRaceLink,
    getPremiumRaceDataFetchState,
    updatePremiumRaceDataFetchState,
    replacePremiumRaceData,
  } = await import("./storage");
  const { fetchPremiumHtml } = await import("./premium-race");
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
    raceName: "T",
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "jra",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  vi.mocked(getPremiumRaceDataFetchState).mockResolvedValueOnce(null);
  vi.mocked(fetchPremiumHtml).mockImplementation(async (_config: unknown, url: unknown) => {
    if (typeof url === "string" && url.includes("/d/")) {
      return `
        <div class="DataPickupHorseArea">
          <dl>
            <dt><span class="Umaban_Num">1</span></dt>
            <dd>
              <a class="data_top_horse_link">テイザー馬</a>
              <dd class="PickupDataBox"><ul><li>偽の理由</li></ul></dd>
            </dd>
          </dl>
        </div>
        <div class="DummyBox"></div>
        <div class="Premium_Regist_Box"></div>
      `;
    }
    return "";
  });
  await handleJob(
    buildEnv({
      PREMIUM_RACE_COMMENT_PATH_TEMPLATE: "/c/{sourceRaceId}",
      PREMIUM_RACE_DATA_TOP_PATH_TEMPLATE: "/d/{sourceRaceId}",
      PREMIUM_RACE_ORIGIN: "https://x.test",
      PREMIUM_RACE_WORK_PATH_TEMPLATE: "/w/{sourceRaceId}",
    } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-race-data" },
  );
  const updateCall = vi.mocked(updatePremiumRaceDataFetchState).mock.calls.at(-1)?.[1];
  expect(updateCall?.status).toBe("auth_required");
  expect(typeof updateCall?.retryAfter).toBe("string");
  const replaceCall = vi.mocked(replacePremiumRaceData).mock.calls.at(-1)?.[1];
  expect(replaceCall?.dataTopHorses).toBeUndefined();
  const parsedMessage = JSON.parse(String(updateCall?.message ?? "{}"));
  expect(parsedMessage.loginPromptDetected).toBe(false);
  expect(parsedMessage.dataTopAuthorized).toBe(false);
  expect(parsedMessage.dataTopPersisted).toBe(false);
  expect(parsedMessage.authRetryCount).toBe(1);
});

it("handleJob fetch-premium-race-data persists data-top horses when the login prompt only fires on the comment page", async () => {
  const { handleJob } = await import("./worker");
  const {
    getRaceSource,
    getPremiumRaceLink,
    getPremiumRaceDataFetchState,
    updatePremiumRaceDataFetchState,
    replacePremiumRaceData,
  } = await import("./storage");
  const { fetchPremiumHtml } = await import("./premium-race");
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
    raceName: "T",
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "jra",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  vi.mocked(getPremiumRaceDataFetchState).mockResolvedValueOnce(null);
  vi.mocked(fetchPremiumHtml).mockImplementation(async (_config: unknown, url: unknown) => {
    if (typeof url === "string" && url.includes("/d/")) {
      return `
        <div class="Icon_Account">user</div>
        <div class="DataPickupHorseArea">
          <dl>
            <dt><span class="Umaban_Num">1</span></dt>
            <dd>
              <a class="data_top_horse_link">本物馬</a>
              <dd class="PickupDataBox"><ul><li>本当の理由</li></ul></dd>
            </dd>
          </dl>
        </div>
      `;
    }
    if (typeof url === "string" && url.includes("/c/")) {
      return "<html><body>プレミアムサービス 登録でご覧になれます</body></html>";
    }
    return "";
  });
  await handleJob(
    buildEnv({
      PREMIUM_RACE_COMMENT_PATH_TEMPLATE: "/c/{sourceRaceId}",
      PREMIUM_RACE_DATA_TOP_PATH_TEMPLATE: "/d/{sourceRaceId}",
      PREMIUM_RACE_ORIGIN: "https://x.test",
      PREMIUM_RACE_WORK_PATH_TEMPLATE: "/w/{sourceRaceId}",
    } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-race-data" },
  );
  const updateCall = vi.mocked(updatePremiumRaceDataFetchState).mock.calls.at(-1)?.[1];
  expect(updateCall?.status).toBe("auth_required");
  expect(typeof updateCall?.retryAfter).toBe("string");
  const replaceCall = vi.mocked(replacePremiumRaceData).mock.calls.at(-1)?.[1];
  expect(replaceCall?.dataTopHorses).toStrictEqual([
    { horseName: "本物馬", horseNumber: "1", rank: 1, reasons: ["本当の理由"] },
  ]);
  const parsedMessage = JSON.parse(String(updateCall?.message ?? "{}"));
  expect(parsedMessage.loginPromptDetected).toBe(true);
  expect(parsedMessage.dataTopAuthorized).toBe(true);
  expect(parsedMessage.dataTopPersisted).toBe(true);
  expect(parsedMessage.authRetryCount).toBe(1);
});

it("handleJob fetch-premium-race-data records ok when login text coexists with authoritative content", async () => {
  const { handleJob } = await import("./worker");
  const {
    getRaceSource,
    getPremiumRaceLink,
    getPremiumRaceDataFetchState,
    updatePremiumRaceDataFetchState,
    replacePremiumRaceData,
  } = await import("./storage");
  const { fetchPremiumHtml } = await import("./premium-race");
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
    raceName: "T",
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "jra",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  vi.mocked(getPremiumRaceDataFetchState).mockResolvedValueOnce(null);
  vi.mocked(fetchPremiumHtml).mockImplementation(async (_config: unknown, url: unknown) => {
    if (typeof url === "string" && url.includes("/d/")) {
      return `
        <div class="Icon_Account">user</div>
        <div>プレミアムサービス 登録でご覧になれます</div>
        <div class="DataPickupHorseArea">
          <dl>
            <dt><span class="Umaban_Num">1</span></dt>
            <dd>
              <a class="data_top_horse_link">本物馬</a>
              <dd class="PickupDataBox"><ul><li>本当の理由</li></ul></dd>
            </dd>
          </dl>
        </div>
      `;
    }
    if (typeof url === "string" && url.includes("/c/")) {
      return `
        <table class="Comment_Table_Show_All"></table>
        <div>プレミアムサービス 登録でご覧になれます</div>
      `;
    }
    if (typeof url === "string" && url.includes("/w/")) {
      return `
        <tr class="OikiriDataHead1 HorseList">
          <td class="Umaban">1</td>
          <td class="Horse_Name">本物馬</td>
          <td class="Training_Critic">好調</td>
          <td class="Rank_好調">A</td>
        </tr>
        <div>プレミアムサービス 登録でご覧になれます</div>
      `;
    }
    return "";
  });
  await handleJob(
    buildEnv({
      PREMIUM_RACE_COMMENT_PATH_TEMPLATE: "/c/{sourceRaceId}",
      PREMIUM_RACE_DATA_TOP_PATH_TEMPLATE: "/d/{sourceRaceId}",
      PREMIUM_RACE_ORIGIN: "https://x.test",
      PREMIUM_RACE_WORK_PATH_TEMPLATE: "/w/{sourceRaceId}",
    } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-race-data" },
  );
  const updateCall = vi.mocked(updatePremiumRaceDataFetchState).mock.calls.at(-1)?.[1];
  expect(updateCall?.status).toBe("ok");
  expect(updateCall?.retryAfter).toBeNull();
  const replaceCall = vi.mocked(replacePremiumRaceData).mock.calls.at(-1)?.[1];
  expect(replaceCall?.dataTopHorses).toStrictEqual([
    { horseName: "本物馬", horseNumber: "1", rank: 1, reasons: ["本当の理由"] },
  ]);
  const parsedMessage = JSON.parse(String(updateCall?.message ?? "{}"));
  expect(parsedMessage.loginPromptDetected).toBe(false);
  expect(parsedMessage.commentAuthRequired).toBe(false);
  expect(parsedMessage.dataTopAuthorized).toBe(true);
  expect(parsedMessage.dataTopPersisted).toBe(true);
  expect(parsedMessage.trainingReviewCount).toBe(1);
  expect(parsedMessage.authRetryCount).toBe(0);
});

it("handleJob fetch-premium-race-data backs off auth_required when retry attempts are exhausted", async () => {
  const { handleJob } = await import("./worker");
  const {
    getRaceSource,
    getPremiumRaceLink,
    getPremiumRaceDataFetchState,
    updatePremiumRaceDataFetchState,
  } = await import("./storage");
  const { fetchPremiumHtml } = await import("./premium-race");
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
    raceName: "T",
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "jra",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  vi.mocked(getPremiumRaceDataFetchState).mockResolvedValueOnce({
    lastFetchAt: "2026-05-12T02:00:00+09:00",
    lastQueuedAt: "2026-05-12T02:00:00+09:00",
    message: '{"authRetryCount":5}',
    retryAfter: null,
    status: "auth_required",
  });
  vi.mocked(fetchPremiumHtml).mockResolvedValue(
    "<html><body>プレミアムサービス 登録でご覧になれます</body></html>",
  );
  await handleJob(
    buildEnv({
      PREMIUM_RACE_COMMENT_PATH_TEMPLATE: "/c/{sourceRaceId}",
      PREMIUM_RACE_DATA_TOP_PATH_TEMPLATE: "/d/{sourceRaceId}",
      PREMIUM_RACE_ORIGIN: "https://x.test",
      PREMIUM_RACE_WORK_PATH_TEMPLATE: "/w/{sourceRaceId}",
    } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-race-data" },
  );
  const updateCall = vi.mocked(updatePremiumRaceDataFetchState).mock.calls.at(-1)?.[1];
  expect(updateCall?.status).toBe("auth_required");
  const parsedMessage = JSON.parse(String(updateCall?.message ?? "{}"));
  expect(parsedMessage.authRetryCount).toBe(6);
});

it("handleJob fetch-premium-race-data persists stable comments normally when no login prompt fires", async () => {
  const { handleJob } = await import("./worker");
  const {
    getRaceSource,
    getPremiumRaceLink,
    getPremiumRaceDataFetchState,
    updatePremiumRaceDataFetchState,
  } = await import("./storage");
  const { fetchPremiumHtml } = await import("./premium-race");
  const premiumRace = await import("./premium-race");
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
    raceName: "T",
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "jra",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  vi.mocked(getPremiumRaceDataFetchState).mockResolvedValueOnce(null);
  vi.mocked(fetchPremiumHtml).mockResolvedValue(
    '<table class="Comment_Table_Show_All"><tr></tr></table><div class="Icon_Account"></div>',
  );
  vi.spyOn(premiumRace, "parsePremiumStableComments").mockReturnValue([
    {
      commentText: "good",
      evaluationGrade: 1,
      evaluationText: null,
      frameNumber: null,
      horseName: null,
      horseNumber: "1",
    },
  ]);
  await handleJob(
    buildEnv({
      PREMIUM_RACE_COMMENT_PATH_TEMPLATE: "/c/{sourceRaceId}",
      PREMIUM_RACE_DATA_TOP_PATH_TEMPLATE: "/d/{sourceRaceId}",
      PREMIUM_RACE_ORIGIN: "https://x.test",
      PREMIUM_RACE_WORK_PATH_TEMPLATE: "/w/{sourceRaceId}",
    } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-race-data" },
  );
  const updateCall = vi.mocked(updatePremiumRaceDataFetchState).mock.calls.at(-1)?.[1];
  expect(updateCall?.status).toBe("ok");
  expect(updateCall?.retryAfter).toBeNull();
  const parsedMessage = JSON.parse(String(updateCall?.message ?? "{}"));
  expect(parsedMessage.authRetryCount).toBe(0);
  expect(parsedMessage.loginPromptDetected).toBe(false);
});

it("handleJob fetch-premium-race-data with valid link + fetched HTML exercises full ingest path", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, getPremiumRaceLink, updatePremiumRaceDataFetchState } =
    await import("./storage");
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
  await handleJob(buildEnv({ PREMIUM_RACE_ORIGIN: "https://x.test" } as never), {
    raceKey: "jra:2026:0512:08:01",
    type: "fetch-premium-paddock",
  });
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
  const { claimTrackConditionFetch, insertJraTrackConditionSnapshot, completeTrackConditionFetch } =
    await import("./storage");
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

it("handleJob fetch-jra-track-condition writes per-race cache when insert returns future races", async () => {
  const { handleJob } = await import("./worker");
  const { claimTrackConditionFetch, insertJraTrackConditionSnapshot, completeTrackConditionFetch } =
    await import("./storage");
  const { writeCachedTrackCondition } = await import("./track-condition-cache");
  vi.mocked(claimTrackConditionFetch).mockResolvedValueOnce(true);
  vi.mocked(insertJraTrackConditionSnapshot).mockResolvedValueOnce([
    { raceKey: "jra:2026:0512:08:01", raceStartAtJst: "2099-05-12T15:00:00+09:00" },
  ] as never);
  await handleJob(buildEnv({ REALTIME_TEST_NOW: "2026-05-12T00:00:00.000Z" } as never), {
    date: "20260512",
    keibajoCode: "08",
    type: "fetch-jra-track-condition",
  });
  expect(completeTrackConditionFetch).toHaveBeenCalled();
  expect(writeCachedTrackCondition).toHaveBeenCalledTimes(1);
});

it("handleJob fetch-jra-track-condition fails the fetch and rethrows when Playwright throws", async () => {
  const { handleJob } = await import("./worker");
  const { claimTrackConditionFetch, failTrackConditionFetch } = await import("./storage");
  const { fetchJraTrackConditionWithPlaywright } = await import("./jra-track-condition");
  vi.mocked(claimTrackConditionFetch).mockResolvedValueOnce(true);
  vi.mocked(fetchJraTrackConditionWithPlaywright).mockRejectedValueOnce(
    new Error("playwright boom"),
  );
  await expect(
    handleJob(buildEnv(), {
      date: "20260512",
      keibajoCode: "08",
      type: "fetch-jra-track-condition",
    }),
  ).rejects.toThrow("playwright boom");
  expect(failTrackConditionFetch).toHaveBeenCalled();
});

it("handleJob fetch-jra-track-condition with successful claim and empty races falls through to fail", async () => {
  const { handleJob } = await import("./worker");
  const { claimTrackConditionFetch, insertJraTrackConditionSnapshot } = await import("./storage");
  vi.mocked(claimTrackConditionFetch).mockResolvedValueOnce(true);
  vi.mocked(insertJraTrackConditionSnapshot).mockResolvedValueOnce([]);
  await handleJob(buildEnv(), {
    date: "20260512",
    keibajoCode: "08",
    type: "fetch-jra-track-condition",
  });
});

it("handleJob fetch-results with JRA race source throws when entry and result both parse empty", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, getRaceSource, completeResultFetch, failResultFetch } =
    await import("./storage");
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
  await expect(
    handleJob(buildEnv({ REALTIME_TEST_NOW: "2026-05-12T07:00:00.000Z" } as never), {
      raceKey: "jra:2026:0512:08:01",
      type: "fetch-results",
    }),
  ).rejects.toThrow("race entry rows are empty: jra:2026:0512:08:01");
  expect(failResultFetch).toHaveBeenCalled();
  expect(completeResultFetch).not.toHaveBeenCalled();
});

it("handleJob fetch-results with JRA race source pushes hot odds to race trend DO", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, getLatestHorseWeights, getRaceSource, insertRaceResultSnapshot } =
    await import("./storage");
  const { parseJraRaceEntries, parseJraRaceResults } = await import("./jra");
  vi.mocked(claimResultFetch).mockResolvedValueOnce(true);
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
  vi.mocked(parseJraRaceEntries).mockReturnValueOnce([
    { horseName: "JraA", horseNumber: "1", jockeyName: "JockeyA", status: null },
    { horseName: "JraB", horseNumber: "2", jockeyName: "JockeyB", status: null },
  ]);
  vi.mocked(parseJraRaceResults).mockReturnValueOnce([
    { finishPosition: "1", horseName: "JraA", horseNumber: "1", time: "1:10.1" },
    { finishPosition: "2", horseName: "JraB", horseNumber: "2", time: "1:10.3" },
  ]);
  vi.mocked(insertRaceResultSnapshot).mockResolvedValueOnce(2);
  vi.mocked(getLatestHorseWeights).mockResolvedValueOnce({
    fetchedAt: "2026-05-12T12:40:00+09:00",
    horses: [
      { changeAmount: null, changeSign: null, horseName: null, horseNumber: "1", weight: 480 },
      { changeAmount: null, changeSign: null, horseName: null, horseNumber: "2", weight: 490 },
    ],
  } as never);
  const stubFetch = vi.fn(
    async (_url: string, _init?: RequestInit): Promise<Response> =>
      new Response(JSON.stringify({ ok: true }), { status: 200 }),
  );
  const idFromName = vi.fn((name: string): string => name);
  const get = vi.fn((_id: string) => ({ fetch: stubFetch }));
  await handleJob(
    buildEnv({
      RACE_TREND_DAILY_TRACK_DO: { get, idFromName } as never,
      REALTIME_HOT: {
        fetch: vi.fn(
          async (): Promise<Response> =>
            Response.json({
              fetchedAt: "2026-05-12T12:50:00+09:00",
              history: [],
              historyByType: {},
              latest: {
                tansho: [
                  { combination: "1", odds: 2.6, rank: 1 },
                  { combination: "2", odds: 4.8, rank: 3 },
                ],
              },
            }),
        ),
      } as never,
      REALTIME_TEST_NOW: "2026-05-12T05:00:00.000Z",
    }),
    {
      raceKey: "jra:2026:0512:08:01",
      type: "fetch-results",
    },
  );
  const body = stubFetch.mock.calls[0]![1]!.body;
  if (typeof body !== "string") throw new Error("expected push body to be a JSON string");
  const parsed = JSON.parse(body) as {
    starterRows: Array<{
      bataiju: string | null;
      finishPosition: number;
      tanshoOdds: string | null;
      tanshoPopularity: string | null;
      umaban: string;
    }>;
  };
  expect(parsed.starterRows).toMatchObject([
    {
      bataiju: "480",
      finishPosition: 1,
      tanshoOdds: "0026",
      tanshoPopularity: "01",
      umaban: "1",
    },
    {
      bataiju: "490",
      finishPosition: 2,
      tanshoOdds: "0048",
      tanshoPopularity: "03",
      umaban: "2",
    },
  ]);
});

it("handleJob fetch-weights with JRA race source runs assert + insertHorseWeightSnapshot", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, insertHorseWeightSnapshot, updateLastFetch } = await import("./storage");
  const { parseJraHorseWeights, parseJraRaceEntries } = await import("./jra");
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
  vi.mocked(parseJraRaceEntries).mockReturnValueOnce([
    { horseName: "JraA", horseNumber: "1", jockeyName: "JockeyA", status: null },
    { horseName: "JraB", horseNumber: "2", jockeyName: "JockeyB", status: null },
  ]);
  const stubFetch = vi.fn(
    async (_url: string, _init?: RequestInit): Promise<Response> =>
      new Response(JSON.stringify({ ok: true }), { status: 200 }),
  );
  const idFromName = vi.fn((name: string): string => name);
  const get = vi.fn((_id: string) => ({ fetch: stubFetch }));
  const finishFetch = vi.fn(
    async (): Promise<Response> => Response.json({ claimed: true, ok: true }, { status: 202 }),
  );
  await handleJob(
    buildEnv({
      FINISH_POSITION_CRON: { fetch: finishFetch },
      RACE_TREND_DAILY_TRACK_DO: { get, idFromName } as never,
      REALTIME_HOT: {
        fetch: vi.fn(
          async (): Promise<Response> =>
            Response.json({
              fetchedAt: "2026-05-12T12:00:00+09:00",
              history: [],
              historyByType: {},
              latest: {
                tansho: [
                  { combination: "1", odds: 3.4, rank: 2 },
                  { combination: "2", odds: 1.8, rank: 1 },
                ],
              },
            }),
        ),
      } as never,
    }),
    {
      raceKey: "jra:2026:0512:08:01",
      type: "fetch-weights",
    },
  );
  expect(insertHorseWeightSnapshot).toHaveBeenCalled();
  expect(updateLastFetch).toHaveBeenCalledWith(
    expect.anything(),
    "jra:2026:0512:08:01",
    "last_weight_fetch_at",
    expect.any(String),
  );
  expect(finishFetch.mock.invocationCallOrder[0]).toBeLessThan(
    vi.mocked(updateLastFetch).mock.invocationCallOrder[0]!,
  );
  expect(stubFetch).toHaveBeenCalledTimes(2);
  const body = stubFetch.mock.calls[1]![1]!.body;
  if (typeof body !== "string") throw new Error("expected push body to be a JSON string");
  const parsed = JSON.parse(body) as {
    starterRows: Array<{
      bataiju: string | null;
      tanshoOdds: string | null;
      tanshoPopularity: string | null;
      umaban: string;
    }>;
  };
  expect(parsed.starterRows).toMatchObject([
    { bataiju: "480", tanshoOdds: "0034", tanshoPopularity: "02", umaban: "1" },
    { bataiju: "490", tanshoOdds: "0018", tanshoPopularity: "01", umaban: "2" },
  ]);
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

it("handleJob fetch-results NAR silently acks when results empty but expectedHorseCount > 0 (2026-06-30 fix)", async () => {
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
  vi.spyOn(await import("./keiba-go"), "parseRaceEntryHorseNumbers").mockReturnValue(["1", "2"]);
  vi.spyOn(await import("./keiba-go"), "parseRaceResultExcludedHorseNumbers").mockReturnValue([]);
  await handleJob(buildEnv(), {
    raceKey: "nar:2026:0512:55:01",
    type: "fetch-results",
  });
  expect(failResultFetch).toHaveBeenCalled();
});

it("handleJob fetch-results completes a same-day NAR race from stored entries when DebaTable is 404", async () => {
  const { handleJob } = await import("./worker");
  const {
    claimResultFetch,
    completeResultFetch,
    getLatestRaceEntries,
    getRaceSource,
    insertRaceEntrySnapshot,
    insertRaceResultSnapshot,
    logFetch,
    recordPartialResultFetch,
  } = await import("./storage");
  const { FetchStatusError, fetchRacePage, parseRaceResults } = await import("./keiba-go");
  vi.mocked(claimResultFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce({
    babaCode: "22",
    debaUrl: "https://www.keiba.go.jp/DebaTable",
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
  vi.mocked(getLatestRaceEntries).mockResolvedValueOnce({
    fetchedAt: "2026-05-12T17:00:00+09:00",
    horses: [
      {
        fetchedAt: "2026-05-12T17:00:00+09:00",
        horseName: "one",
        horseNumber: "1",
        jockeyName: "j1",
        status: null,
      },
      {
        fetchedAt: "2026-05-12T17:00:00+09:00",
        horseName: "two",
        horseNumber: "2",
        jockeyName: "j2",
        status: null,
      },
    ],
  });
  vi.mocked(fetchRacePage)
    .mockRejectedValueOnce(new FetchStatusError("https://www.keiba.go.jp/DebaTable", 404))
    .mockResolvedValueOnce("<html>result</html>");
  vi.mocked(parseRaceResults).mockReturnValueOnce([
    { finishPosition: "01", horseName: "one", horseNumber: "1", time: "1:30.0" },
    { finishPosition: "中止", horseName: "two", horseNumber: "2", time: null },
  ]);
  vi.mocked(insertRaceResultSnapshot).mockResolvedValueOnce(2);

  await handleJob(buildEnv(), {
    raceKey: "nar:2026:0512:55:01",
    type: "fetch-results",
  });

  expect(insertRaceEntrySnapshot).not.toHaveBeenCalled();
  expect(completeResultFetch).toHaveBeenCalledWith(
    expect.anything(),
    "nar:2026:0512:55:01",
    expect.any(String),
    { expectedHorseCount: 2, isComplete: true, savedHorseCount: 2 },
  );
  expect(recordPartialResultFetch).not.toHaveBeenCalled();
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-results",
    "fallback:stored-entry-after-404",
    "nar:2026:0512:55:01",
    "fetched_at=2026-05-12T17:00:00+09:00 horses=2",
    undefined,
  );
});

it("handleJob fetch-results treats a same-day NAR RaceMarkTable 404 as awaiting publish", async () => {
  const { handleJob } = await import("./worker");
  const {
    claimResultFetch,
    completeResultFetch,
    failResultFetch,
    getRaceSource,
    incrementEmptyResultAttempts,
    logFetch,
  } = await import("./storage");
  const { FetchStatusError, fetchRacePage } = await import("./keiba-go");
  vi.mocked(claimResultFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce({
    babaCode: "22",
    debaUrl: "https://www.keiba.go.jp/DebaTable",
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
  vi.mocked(fetchRacePage)
    .mockResolvedValueOnce("<html>entry</html>")
    .mockRejectedValueOnce(new FetchStatusError("https://www.keiba.go.jp/RaceMarkTable", 404));
  vi.spyOn(await import("./keiba-go"), "parseRaceEntryHorseNumbers").mockReturnValue(["1", "2"]);

  await handleJob(buildEnv({ REALTIME_TEST_NOW: "2026-05-12T09:05:00.000Z" }), {
    raceKey: "nar:2026:0512:55:01",
    type: "fetch-results",
  });

  expect(failResultFetch).toHaveBeenCalledWith(expect.anything(), "nar:2026:0512:55:01");
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-results",
    "skip:awaiting-publish",
    "nar:2026:0512:55:01",
    null,
    undefined,
  );
  expect(incrementEmptyResultAttempts).not.toHaveBeenCalled();
  expect(completeResultFetch).not.toHaveBeenCalled();
});

it("handleJob fetch-results safely acks NAR 404 pages when no entry snapshot exists", async () => {
  const { handleJob } = await import("./worker");
  const {
    claimResultFetch,
    completeResultFetch,
    failResultFetch,
    getLatestRaceEntries,
    getRaceSource,
    incrementEmptyResultAttempts,
    insertRaceEntrySnapshot,
    logFetch,
  } = await import("./storage");
  const { FetchStatusError, fetchRacePage } = await import("./keiba-go");
  vi.mocked(claimResultFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce({
    babaCode: "22",
    debaUrl: "https://www.keiba.go.jp/DebaTable",
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
  vi.mocked(getLatestRaceEntries).mockResolvedValueOnce(null);
  vi.mocked(fetchRacePage)
    .mockRejectedValueOnce(new FetchStatusError("https://www.keiba.go.jp/DebaTable", 404))
    .mockRejectedValueOnce(new FetchStatusError("https://www.keiba.go.jp/RaceMarkTable", 404));

  await handleJob(buildEnv({ REALTIME_TEST_NOW: "2026-05-12T09:05:00.000Z" }), {
    raceKey: "nar:2026:0512:55:01",
    type: "fetch-results",
  });

  expect(failResultFetch).toHaveBeenCalledWith(expect.anything(), "nar:2026:0512:55:01");
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-results",
    "skip:awaiting-publish",
    "nar:2026:0512:55:01",
    null,
    undefined,
  );
  expect(incrementEmptyResultAttempts).not.toHaveBeenCalled();
  expect(insertRaceEntrySnapshot).not.toHaveBeenCalled();
  expect(completeResultFetch).not.toHaveBeenCalled();
});

it("handleJob fetch-results keeps NAR RaceMarkTable 503 retryable", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, failResultFetch, getRaceSource } = await import("./storage");
  const { FetchStatusError, fetchRacePage } = await import("./keiba-go");
  vi.mocked(claimResultFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce({
    babaCode: "22",
    debaUrl: "https://www.keiba.go.jp/DebaTable",
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
  vi.mocked(fetchRacePage)
    .mockResolvedValueOnce("<html>entry</html>")
    .mockRejectedValueOnce(new FetchStatusError("https://www.keiba.go.jp/RaceMarkTable", 503));

  await expect(
    handleJob(buildEnv(), {
      raceKey: "nar:2026:0512:55:01",
      type: "fetch-results",
    }),
  ).rejects.toThrow("Failed to fetch https://www.keiba.go.jp/RaceMarkTable: 503");

  expect(failResultFetch).toHaveBeenCalledWith(expect.anything(), "nar:2026:0512:55:01");
});

it("handleJob fetch-results keeps NAR DebaTable network failures retryable", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, failResultFetch, getRaceSource } = await import("./storage");
  const { fetchRacePage } = await import("./keiba-go");
  vi.mocked(claimResultFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce({
    babaCode: "22",
    debaUrl: "https://www.keiba.go.jp/DebaTable",
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
  vi.mocked(fetchRacePage)
    .mockRejectedValueOnce(new Error("NAR entry connection reset"))
    .mockResolvedValueOnce("<html>result</html>");

  await expect(
    handleJob(buildEnv(), {
      raceKey: "nar:2026:0512:55:01",
      type: "fetch-results",
    }),
  ).rejects.toThrow("NAR entry connection reset");

  expect(failResultFetch).toHaveBeenCalledWith(expect.anything(), "nar:2026:0512:55:01");
});

it("handleJob fetch-weights NAR + sparse weight rows (length 1) preserves existing snapshot and logs pending:weights-incomplete", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, insertHorseWeightSnapshot, logFetch, updateLastFetch } =
    await import("./storage");
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
  await handleJob(buildEnv(), { raceKey: "nar:2026:0512:55:01", type: "fetch-weights" });
  expect(insertHorseWeightSnapshot).not.toHaveBeenCalled();
  expect(updateLastFetch).not.toHaveBeenCalledWith(
    expect.anything(),
    "nar:2026:0512:55:01",
    "last_weight_fetch_at",
    expect.anything(),
  );
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-weights",
    "pending:weights-incomplete",
    "nar:2026:0512:55:01",
    "count=1 upstream-incomplete",
  );
  expect(logFetch).not.toHaveBeenCalledWith(
    expect.anything(),
    "fetch-weights",
    "error",
    "nar:2026:0512:55:01",
    expect.anything(),
  );
});

// 2026-07-19..21 production: assertNarHorseWeightsComplete threw on incomplete
// non-empty weight sets (e.g. missing=6) before the soft sparse path, so the
// job hard-errored with no retry and last_weight_fetch_at stayed null forever.
it("handleJob fetch-weights NAR sparse result leaves retry scheduling to the watchdog", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, insertHorseWeightSnapshot, logFetch, updateLastFetch } =
    await import("./storage");
  const { parseHorseWeights, parseRaceEntries, fetchRacePage, parseRaceResultHorseWeights } =
    await import("./keiba-go");
  vi.mocked(getRaceSource).mockResolvedValueOnce({
    babaCode: "22",
    debaUrl: "https://nar.example/DebaTable?race_id=1",
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
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "nar",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(parseRaceEntries).mockReturnValueOnce([
    { horseName: "h1", horseNumber: "1", jockeyName: "j", status: null },
    { horseName: "h2", horseNumber: "2", jockeyName: "j", status: null },
    { horseName: "h3", horseNumber: "3", jockeyName: "j", status: null },
  ] as never);
  vi.mocked(parseHorseWeights).mockReturnValueOnce([
    { changeAmount: null, changeSign: null, horseName: "h1", horseNumber: "1", weight: 500 },
    { changeAmount: null, changeSign: null, horseName: "h2", horseNumber: "2", weight: 510 },
  ] as never);
  // Result page also incomplete — pending path must still not throw.
  vi.mocked(fetchRacePage).mockResolvedValue("<html></html>");
  vi.mocked(parseRaceResultHorseWeights).mockReturnValueOnce([
    { changeAmount: null, changeSign: null, horseName: "h1", horseNumber: "1", weight: 500 },
    { changeAmount: null, changeSign: null, horseName: "h2", horseNumber: "2", weight: 510 },
  ] as never);
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z" });
  const sendSpy = vi.spyOn(env.REALTIME_JOBS, "send");
  await expect(
    handleJob(env, { raceKey: "nar:2026:0512:55:01", type: "fetch-weights" }),
  ).resolves.toBeUndefined();
  expect(insertHorseWeightSnapshot).not.toHaveBeenCalled();
  expect(updateLastFetch).not.toHaveBeenCalledWith(
    expect.anything(),
    "nar:2026:0512:55:01",
    "last_weight_fetch_at",
    expect.anything(),
  );
  expect(sendSpy).not.toHaveBeenCalled();
  expect(logFetch).not.toHaveBeenCalledWith(
    expect.anything(),
    "fetch-weights",
    "queued:weights-empty-retry",
    expect.anything(),
    expect.anything(),
  );
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-weights",
    "pending:weights-incomplete",
    "nar:2026:0512:55:01",
    "count=2 missing=3 upstream-incomplete",
  );
  expect(logFetch).not.toHaveBeenCalledWith(
    expect.anything(),
    "fetch-weights",
    "error",
    expect.anything(),
    expect.anything(),
  );
});

it("handleJob fetch-weights JRA sparse result leaves retry scheduling to the watchdog", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, insertHorseWeightSnapshot, logFetch, updateLastFetch } =
    await import("./storage");
  const { parseJraHorseWeights, parseJraRaceEntries } = await import("./jra");
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
  vi.mocked(parseJraRaceEntries).mockReturnValueOnce([
    { horseName: "JraA", horseNumber: "1", jockeyName: "JockeyA", status: null },
    { horseName: "JraB", horseNumber: "2", jockeyName: "JockeyB", status: null },
    { horseName: "JraC", horseNumber: "3", jockeyName: "JockeyC", status: null },
  ] as never);
  vi.mocked(parseJraHorseWeights).mockReturnValueOnce([
    { changeAmount: null, changeSign: null, horseName: null, horseNumber: "1", weight: 480 },
    { changeAmount: null, changeSign: null, horseName: null, horseNumber: "2", weight: 490 },
  ] as never);
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z" });
  const sendSpy = vi.spyOn(env.REALTIME_JOBS, "send");
  await expect(
    handleJob(env, { raceKey: "jra:2026:0512:08:01", type: "fetch-weights" }),
  ).resolves.toBeUndefined();
  expect(insertHorseWeightSnapshot).not.toHaveBeenCalled();
  expect(updateLastFetch).not.toHaveBeenCalledWith(
    expect.anything(),
    "jra:2026:0512:08:01",
    "last_weight_fetch_at",
    expect.anything(),
  );
  expect(sendSpy).not.toHaveBeenCalled();
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-weights",
    "pending:weights-incomplete",
    "jra:2026:0512:08:01",
    "count=2 missing=3 upstream-incomplete",
  );
});

// Incomplete primary entry-page parse must fall back to the race-result page
// and store the more complete set (fail-closed only when still incomplete).
it("handleJob fetch-weights NAR prefers complete result-page weights over incomplete primary", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, insertHorseWeightSnapshot, logFetch } = await import("./storage");
  const { fetchRacePage, parseHorseWeights, parseRaceEntries, parseRaceResultHorseWeights } =
    await import("./keiba-go");
  vi.mocked(getRaceSource).mockResolvedValueOnce({
    babaCode: "22",
    debaUrl: "https://nar.example/DebaTable?race_id=1",
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
  vi.mocked(parseRaceEntries).mockReturnValueOnce([
    { horseName: "h1", horseNumber: "1", jockeyName: "j", status: null },
    { horseName: "h2", horseNumber: "2", jockeyName: "j", status: null },
  ] as never);
  vi.mocked(parseHorseWeights).mockReturnValueOnce([
    { changeAmount: null, changeSign: null, horseName: "h1", horseNumber: "1", weight: 500 },
  ] as never);
  vi.mocked(fetchRacePage)
    .mockResolvedValueOnce("<html>primary</html>")
    .mockResolvedValueOnce("<html>result</html>");
  const resultWeights = [
    { changeAmount: null, changeSign: null, horseName: "h1", horseNumber: "1", weight: 500 },
    { changeAmount: null, changeSign: null, horseName: "h2", horseNumber: "2", weight: 510 },
  ];
  vi.mocked(parseRaceResultHorseWeights).mockReturnValueOnce(resultWeights as never);
  await handleJob(buildEnv(), { raceKey: "nar:2026:0512:55:01", type: "fetch-weights" });
  expect(fetchRacePage).toHaveBeenCalledTimes(2);
  expect(fetchRacePage).toHaveBeenNthCalledWith(2, "https://nar.example/RaceMarkTable?race_id=1");
  expect(insertHorseWeightSnapshot).toHaveBeenCalledWith(
    expect.anything(),
    "nar:2026:0512:55:01",
    expect.any(String),
    resultWeights,
  );
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-weights",
    "ok",
    "nar:2026:0512:55:01",
    null,
  );
});

it("handleJob fetch-weights NAR complete primary does not fetch result page", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, insertHorseWeightSnapshot } = await import("./storage");
  const { fetchRacePage, parseHorseWeights, parseRaceEntries, parseRaceResultHorseWeights } =
    await import("./keiba-go");
  vi.mocked(getRaceSource).mockResolvedValueOnce({
    babaCode: "22",
    debaUrl: "https://nar.example/DebaTable?race_id=1",
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
  vi.mocked(parseRaceEntries).mockReturnValueOnce([
    { horseName: "h1", horseNumber: "1", jockeyName: "j", status: null },
    { horseName: "h2", horseNumber: "2", jockeyName: "j", status: null },
  ] as never);
  const primaryWeights = [
    { changeAmount: null, changeSign: null, horseName: "h1", horseNumber: "1", weight: 500 },
    { changeAmount: null, changeSign: null, horseName: "h2", horseNumber: "2", weight: 510 },
  ];
  vi.mocked(parseHorseWeights).mockReturnValueOnce(primaryWeights as never);
  vi.mocked(fetchRacePage).mockResolvedValueOnce("<html>primary</html>");
  await handleJob(buildEnv(), { raceKey: "nar:2026:0512:55:01", type: "fetch-weights" });
  expect(fetchRacePage).toHaveBeenCalledTimes(1);
  expect(parseRaceResultHorseWeights).not.toHaveBeenCalled();
  expect(insertHorseWeightSnapshot).toHaveBeenCalledWith(
    expect.anything(),
    "nar:2026:0512:55:01",
    expect.any(String),
    primaryWeights,
  );
});

// A stored snapshot skips the upstream scrape but still reads race timing and
// re-sends the idempotent post-weight rescore trigger. This repairs a previous
// trigger failure on watchdog/Queue redelivery without rewriting weights.
it("handleJob fetch-weights re-triggers rescore without scraping when a snapshot exists", async () => {
  const { handleJob } = await import("./worker");
  const { getLatestHorseWeights, getRaceSource, logFetch, updateLastFetch } =
    await import("./storage");
  const { fetchRacePage } = await import("./keiba-go");
  vi.mocked(getLatestHorseWeights).mockResolvedValueOnce({
    fetchedAt: "2026-05-12T11:00:00+09:00",
    horses: [
      { changeAmount: null, changeSign: null, horseName: null, horseNumber: "1", weight: 480 },
    ],
  } as never);
  vi.mocked(getRaceSource).mockResolvedValueOnce({
    raceKey: "jra:2026:0512:08:01",
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
  } as never);
  const triggerFetch = vi.fn(
    async (_input: RequestInfo | URL): Promise<Response> =>
      Response.json({ claimed: true, ok: true }, { status: 202 }),
  );
  await handleJob(buildEnv({ FINISH_POSITION_CRON: { fetch: triggerFetch } }), {
    raceKey: "jra:2026:0512:08:01",
    type: "fetch-weights",
    weightGeneration: {
      weightSnapshotCount: 1,
      weightSnapshotFetchedAt: "2026-05-12T11:00:00+09:00",
      weightSnapshotHash: "34ccadd1aba2f53450dfcc8b7614e1934d6ca4296a24aa90beef2825ac765a57",
    },
  });
  expect(fetchRacePage).not.toHaveBeenCalled();
  expect(getRaceSource).toHaveBeenCalledWith(expect.anything(), "jra:2026:0512:08:01");
  expect(triggerFetch).toHaveBeenCalledTimes(1);
  expect(updateLastFetch).toHaveBeenCalledWith(
    expect.anything(),
    "jra:2026:0512:08:01",
    "last_weight_fetch_at",
    "2026-05-12T11:00:00+09:00",
  );
  const triggerRequest = triggerFetch.mock.calls[0]![0] as Request;
  expect(await triggerRequest.json()).toStrictEqual({
    category: "jra",
    keibajoCode: "08",
    raceBango: "01",
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    runYmd: "20260512",
    weightSnapshotCount: 1,
    weightSnapshotFetchedAt: "2026-05-12T11:00:00+09:00",
    weightSnapshotHash: "34ccadd1aba2f53450dfcc8b7614e1934d6ca4296a24aa90beef2825ac765a57",
  });
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-weights",
    "skip:weights-already-stored",
    "jra:2026:0512:08:01",
    null,
  );
});

it("handleJob fetch-weights retries a stored snapshot when the rescore trigger fails", async () => {
  const { handleJob } = await import("./worker");
  const { getLatestHorseWeights, getRaceSource, logFetch, updateLastFetch } =
    await import("./storage");
  const { fetchRacePage } = await import("./keiba-go");
  vi.mocked(getLatestHorseWeights).mockResolvedValueOnce({
    fetchedAt: "2026-05-12T11:00:00+09:00",
    horses: [
      { changeAmount: null, changeSign: null, horseName: null, horseNumber: "1", weight: 480 },
    ],
  } as never);
  vi.mocked(getRaceSource).mockResolvedValueOnce({
    raceKey: "jra:2026:0512:08:01",
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
  } as never);
  await expect(
    handleJob(
      buildEnv({
        FINISH_POSITION_CRON: {
          fetch: vi.fn(
            async (): Promise<Response> =>
              Response.json({ error: "queue unavailable", ok: false }, { status: 503 }),
          ),
        },
      }),
      {
        raceKey: "jra:2026:0512:08:01",
        type: "fetch-weights",
        weightGeneration: {
          weightSnapshotCount: 1,
          weightSnapshotFetchedAt: "2026-05-12T11:00:00+09:00",
          weightSnapshotHash: "34ccadd1aba2f53450dfcc8b7614e1934d6ca4296a24aa90beef2825ac765a57",
        },
      },
    ),
  ).rejects.toThrow("weight rescore trigger failed: http 503");
  expect(fetchRacePage).not.toHaveBeenCalled();
  expect(logFetch).not.toHaveBeenCalledWith(
    expect.anything(),
    "fetch-weights",
    "skip:weights-already-stored",
    "jra:2026:0512:08:01",
    null,
  );
  expect(updateLastFetch).not.toHaveBeenCalledWith(
    expect.anything(),
    "jra:2026:0512:08:01",
    "last_weight_fetch_at",
    expect.anything(),
  );
});

it("handleJob fetch-weights repairs a stored marker after its first marker write fails", async () => {
  const { handleJob } = await import("./worker");
  const { getLatestHorseWeights, getRaceSource, updateLastFetch } = await import("./storage");
  const { fetchRacePage } = await import("./keiba-go");
  vi.mocked(getLatestHorseWeights)
    .mockResolvedValueOnce({
      fetchedAt: "2026-05-12T11:00:00+09:00",
      horses: [
        { changeAmount: null, changeSign: null, horseName: null, horseNumber: "1", weight: 480 },
      ],
    } as never)
    .mockResolvedValueOnce({
      fetchedAt: "2026-05-12T11:00:00+09:00",
      horses: [
        { changeAmount: null, changeSign: null, horseName: null, horseNumber: "1", weight: 480 },
      ],
    } as never);
  vi.mocked(getRaceSource)
    .mockResolvedValueOnce({
      raceKey: "jra:2026:0512:08:01",
      raceStartAtJst: "2026-05-12T13:00:00+09:00",
    } as never)
    .mockResolvedValueOnce({
      raceKey: "jra:2026:0512:08:01",
      raceStartAtJst: "2026-05-12T13:00:00+09:00",
    } as never);
  vi.mocked(updateLastFetch)
    .mockRejectedValueOnce(new Error("marker write failed"))
    .mockResolvedValueOnce();
  const triggerFetch = vi.fn(
    async (): Promise<Response> => Response.json({ claimed: true, ok: true }, { status: 202 }),
  );
  const env = buildEnv({ FINISH_POSITION_CRON: { fetch: triggerFetch } });

  await expect(
    handleJob(env, {
      raceKey: "jra:2026:0512:08:01",
      type: "fetch-weights",
      weightGeneration: {
        weightSnapshotCount: 1,
        weightSnapshotFetchedAt: "2026-05-12T11:00:00+09:00",
        weightSnapshotHash: "34ccadd1aba2f53450dfcc8b7614e1934d6ca4296a24aa90beef2825ac765a57",
      },
    }),
  ).rejects.toThrow("marker write failed");
  await expect(
    handleJob(env, {
      raceKey: "jra:2026:0512:08:01",
      type: "fetch-weights",
      weightGeneration: {
        weightSnapshotCount: 1,
        weightSnapshotFetchedAt: "2026-05-12T11:00:00+09:00",
        weightSnapshotHash: "34ccadd1aba2f53450dfcc8b7614e1934d6ca4296a24aa90beef2825ac765a57",
      },
    }),
  ).resolves.toBeUndefined();

  expect(fetchRacePage).not.toHaveBeenCalled();
  expect(triggerFetch).toHaveBeenCalledTimes(2);
  expect(updateLastFetch).toHaveBeenLastCalledWith(
    expect.anything(),
    "jra:2026:0512:08:01",
    "last_weight_fetch_at",
    "2026-05-12T11:00:00+09:00",
  );
});

it("handleJob fetch-weights rejects a stored snapshot without its race source", async () => {
  const { handleJob } = await import("./worker");
  const { getLatestHorseWeights, getRaceSource } = await import("./storage");
  const { fetchRacePage } = await import("./keiba-go");
  vi.mocked(getLatestHorseWeights).mockResolvedValueOnce({
    fetchedAt: "2026-05-12T11:00:00+09:00",
    horses: [
      { changeAmount: null, changeSign: null, horseName: null, horseNumber: "1", weight: 480 },
    ],
  } as never);
  vi.mocked(getRaceSource).mockResolvedValueOnce(null);
  await expect(
    handleJob(buildEnv(), {
      raceKey: "jra:2026:0512:08:01",
      type: "fetch-weights",
      weightGeneration: {
        weightSnapshotCount: 1,
        weightSnapshotFetchedAt: "2026-05-12T11:00:00+09:00",
        weightSnapshotHash: "34ccadd1aba2f53450dfcc8b7614e1934d6ca4296a24aa90beef2825ac765a57",
      },
    }),
  ).rejects.toThrow("race source not found: jra:2026:0512:08:01");
  expect(fetchRacePage).not.toHaveBeenCalled();
});

it("handleJob fetch-weights does not treat an unbound old non-empty snapshot as post-weight success", async () => {
  const { handleJob } = await import("./worker");
  const { getLatestHorseWeights, getRaceSource, updateLastFetch } = await import("./storage");
  const { fetchRacePage } = await import("./keiba-go");
  vi.mocked(getLatestHorseWeights).mockResolvedValueOnce({
    fetchedAt: "2026-05-12T10:00:00+09:00",
    horses: [
      { changeAmount: null, changeSign: null, horseName: null, horseNumber: "1", weight: 470 },
    ],
  } as never);
  vi.mocked(getRaceSource).mockResolvedValueOnce({
    debaUrl: "https://x.test/nar-old-snapshot",
    raceKey: "nar:2026:0512:55:01",
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    source: "nar",
  } as never);
  const triggerFetch = vi.fn(
    async (): Promise<Response> => Response.json({ claimed: true, ok: true }, { status: 202 }),
  );
  await handleJob(buildEnv({ FINISH_POSITION_CRON: { fetch: triggerFetch } }), {
    raceKey: "nar:2026:0512:55:01",
    type: "fetch-weights",
  });
  expect(fetchRacePage).toHaveBeenCalledWith("https://x.test/nar-old-snapshot");
  expect(triggerFetch).not.toHaveBeenCalled();
  expect(updateLastFetch).not.toHaveBeenCalledWith(
    expect.anything(),
    "nar:2026:0512:55:01",
    "last_weight_fetch_at",
    expect.anything(),
  );
});

// 2026-07-03 incident: the attempt timestamp must be written before the HTTP
// The atomic claim writes the attempt timestamp before the fetch, so a thrown
// fetchRacePage call still leaves both a short lease and the watchdog backoff
// marker.
it("handleJob fetch-weights claims before rethrowing when fetchRacePage throws", async () => {
  const { handleJob } = await import("./worker");
  const { claimWeightFetch, getRaceSource, logFetch } = await import("./storage");
  const { fetchRacePage } = await import("./keiba-go");
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
    raceName: "T",
    raceStartAtJst: "2026-05-12T18:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "nar",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(fetchRacePage).mockRejectedValueOnce(new Error("upstream 404"));
  await expect(
    handleJob(buildEnv(), { raceKey: "nar:2026:0512:55:01", type: "fetch-weights" }),
  ).rejects.toThrow("upstream 404");
  expect(claimWeightFetch).toHaveBeenCalledWith(
    expect.anything(),
    "nar:2026:0512:55:01",
    "2026-05-12T21:00:00+09:00",
    "2026-05-12T20:59:15+09:00",
  );
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-weights",
    "error",
    "nar:2026:0512:55:01",
    "upstream 404",
    undefined,
  );
});

it("handleJob fetch-weights skips scraping when another message holds the race lease", async () => {
  const { handleJob } = await import("./worker");
  const { claimWeightFetch, getRaceSource, logFetch } = await import("./storage");
  const { fetchRacePage } = await import("./keiba-go");
  vi.mocked(getRaceSource).mockResolvedValueOnce({
    debaUrl: "https://x.test/race",
    raceKey: "nar:2026:0512:55:01",
  } as never);
  vi.mocked(claimWeightFetch).mockResolvedValueOnce(false);

  await handleJob(buildEnv(), { raceKey: "nar:2026:0512:55:01", type: "fetch-weights" });

  expect(fetchRacePage).not.toHaveBeenCalled();
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-weights",
    "skip:lock-held",
    "nar:2026:0512:55:01",
    "leaseSeconds=45",
  );
});

it("handleJob fetch-weights consumes its watchdog reservation without taking a second lease", async () => {
  const { handleJob } = await import("./worker");
  const { claimReservedWeightFetch, claimWeightFetch, getRaceSource } = await import("./storage");
  const { fetchRacePage } = await import("./keiba-go");
  vi.mocked(getRaceSource).mockResolvedValueOnce({
    debaUrl: "https://x.test/race",
    raceKey: "nar:2026:0512:55:01",
    raceStartAtJst: "2026-05-12T21:30:00+09:00",
    source: "nar",
  } as never);
  vi.mocked(fetchRacePage).mockRejectedValueOnce(new Error("upstream unavailable"));

  await expect(
    handleJob(buildEnv(), {
      raceKey: "nar:2026:0512:55:01",
      type: "fetch-weights",
      watchdogReservedAt: "2026-05-12T21:00:00+09:00",
    }),
  ).rejects.toThrow("upstream unavailable");

  expect(claimReservedWeightFetch).toHaveBeenCalledWith(
    expect.anything(),
    "nar:2026:0512:55:01",
    "2026-05-12T21:00:00+09:00",
    "2026-05-12T21:00:01+09:00",
  );
  expect(claimWeightFetch).not.toHaveBeenCalled();
});

it("handleJob fetch-weights rejects a duplicate watchdog delivery after fencing", async () => {
  const { handleJob } = await import("./worker");
  const { claimReservedWeightFetch, claimWeightFetch, getRaceSource, logFetch } =
    await import("./storage");
  const { fetchRacePage } = await import("./keiba-go");
  vi.mocked(getRaceSource).mockResolvedValueOnce({
    debaUrl: "https://x.test/race",
    raceKey: "nar:2026:0512:55:01",
    raceStartAtJst: "2026-05-12T21:30:00+09:00",
    source: "nar",
  } as never);
  vi.mocked(claimReservedWeightFetch).mockResolvedValueOnce(false);
  vi.mocked(claimWeightFetch).mockResolvedValueOnce(false);

  await handleJob(buildEnv(), {
    raceKey: "nar:2026:0512:55:01",
    type: "fetch-weights",
    watchdogReservedAt: "2026-05-12T21:00:00+09:00",
  });

  expect(fetchRacePage).not.toHaveBeenCalled();
  expect(claimWeightFetch).toHaveBeenCalledWith(
    expect.anything(),
    "nar:2026:0512:55:01",
    "2026-05-12T21:00:01+09:00",
    "2026-05-12T20:59:15+09:00",
  );
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "fetch-weights",
    "skip:lock-held",
    "nar:2026:0512:55:01",
    "leaseSeconds=45",
  );
});

it("handleJob fetch-weights reclaims a failed watchdog delivery after the lease expires", async () => {
  const { handleJob } = await import("./worker");
  const { claimReservedWeightFetch, claimWeightFetch, getRaceSource } = await import("./storage");
  const { fetchRacePage } = await import("./keiba-go");
  vi.mocked(getRaceSource).mockResolvedValueOnce({
    debaUrl: "https://x.test/race",
    raceKey: "nar:2026:0512:55:01",
    raceStartAtJst: "2026-05-12T21:30:00+09:00",
    source: "nar",
  } as never);
  vi.mocked(claimReservedWeightFetch).mockResolvedValueOnce(false);
  vi.mocked(claimWeightFetch).mockResolvedValueOnce(true);
  vi.mocked(fetchRacePage).mockRejectedValueOnce(new Error("second upstream failure"));

  await expect(
    handleJob(buildEnv({ REALTIME_TEST_NOW: "2026-05-12T12:01:00.000Z" }), {
      raceKey: "nar:2026:0512:55:01",
      type: "fetch-weights",
      watchdogReservedAt: "2026-05-12T21:00:00+09:00",
    }),
  ).rejects.toThrow("second upstream failure");

  expect(claimReservedWeightFetch).toHaveBeenCalledWith(
    expect.anything(),
    "nar:2026:0512:55:01",
    "2026-05-12T21:00:00+09:00",
    "2026-05-12T21:01:00+09:00",
  );
  expect(claimWeightFetch).toHaveBeenCalledWith(
    expect.anything(),
    "nar:2026:0512:55:01",
    "2026-05-12T21:01:00+09:00",
    "2026-05-12T21:00:15+09:00",
  );
});

it("handleJob fetch-results with NAR race source throws when entry and result both parse empty", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, getRaceSource, completeResultFetch, failResultFetch } =
    await import("./storage");
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
  await expect(
    handleJob(buildEnv(), {
      raceKey: "nar:2026:0512:55:01",
      type: "fetch-results",
    }),
  ).rejects.toThrow("race entry rows are empty: nar:2026:0512:55:01");
  expect(failResultFetch).toHaveBeenCalled();
  expect(completeResultFetch).not.toHaveBeenCalled();
});

it("isD1OverloadError returns true for D1 DB is overloaded message", async () => {
  const { isD1OverloadError } = await import("./worker");
  expect(isD1OverloadError(new Error("D1_ERROR: D1 DB is overloaded"))).toBe(true);
});

it("isD1OverloadError returns true for Too many requests queued message", async () => {
  const { isD1OverloadError } = await import("./worker");
  expect(isD1OverloadError(new Error("Too many requests queued for this D1"))).toBe(true);
});

it("isD1OverloadError returns false for a non-overload Error", async () => {
  const { isD1OverloadError } = await import("./worker");
  expect(isD1OverloadError(new Error("kaboom"))).toBe(false);
});

it("isD1OverloadError returns false when value is not an Error instance", async () => {
  const { isD1OverloadError } = await import("./worker");
  expect(isD1OverloadError("D1 DB is overloaded")).toBe(false);
});

it("isConnectionPressureError classifies Neon admission errors separately from D1 overload", async () => {
  const { isConnectionPressureError } = await import("./worker");
  expect(isConnectionPressureError(new Error("remaining connection slots are reserved"))).toBe(
    true,
  );
  expect(
    isConnectionPressureError(new Error("Timed out while creating a new server connection.")),
  ).toBe(true);
  expect(
    isConnectionPressureError(
      new Error(
        "Failed to acquire permit to connect to the database. Too many database connection attempts are currently ongoing.",
      ),
    ),
  ).toBe(true);
  expect(isConnectionPressureError(new Error("D1_ERROR: D1 DB is overloaded"))).toBe(false);
});

it("isConnectionPressureError rejects unrelated and non-Error values", async () => {
  const { isConnectionPressureError } = await import("./worker");
  expect(isConnectionPressureError(new Error("upstream returned 503"))).toBe(false);
  expect(isConnectionPressureError(new Error("D1_ERROR: Network connection lost."))).toBe(false);
  expect(isConnectionPressureError("too many connections")).toBe(false);
});

it("isPlanRealtimeCircuitBreakerOpen returns false when KV binding is absent", async () => {
  const { isPlanRealtimeCircuitBreakerOpen } = await import("./worker");
  const env = buildEnv();
  expect(await isPlanRealtimeCircuitBreakerOpen(env)).toBe(false);
});

it("isPlanRealtimeCircuitBreakerOpen returns true when KV value is open", async () => {
  const { isPlanRealtimeCircuitBreakerOpen } = await import("./worker");
  const get = vi.fn(async () => "open");
  const put = vi.fn(async () => {});
  const env = buildEnv({
    DETAIL_SECTION_CACHE_KV: { get, put } as unknown as KVNamespace,
  });
  expect(await isPlanRealtimeCircuitBreakerOpen(env)).toBe(true);
});

it("isPlanRealtimeCircuitBreakerOpen returns false when KV value is null", async () => {
  const { isPlanRealtimeCircuitBreakerOpen } = await import("./worker");
  const get = vi.fn(async () => null);
  const put = vi.fn(async () => {});
  const env = buildEnv({
    DETAIL_SECTION_CACHE_KV: { get, put } as unknown as KVNamespace,
  });
  expect(await isPlanRealtimeCircuitBreakerOpen(env)).toBe(false);
});

it("tripPlanRealtimeCircuitBreaker no-ops when KV binding is absent", async () => {
  const { tripPlanRealtimeCircuitBreaker } = await import("./worker");
  const env = buildEnv();
  await tripPlanRealtimeCircuitBreaker(env);
  expect(env.DETAIL_SECTION_CACHE_KV).toBeUndefined();
});

it("tripPlanRealtimeCircuitBreaker writes open with TTL 120s", async () => {
  const { tripPlanRealtimeCircuitBreaker } = await import("./worker");
  const get = vi.fn(async () => null);
  const put = vi.fn(async () => {});
  const env = buildEnv({
    DETAIL_SECTION_CACHE_KV: { get, put } as unknown as KVNamespace,
  });
  await tripPlanRealtimeCircuitBreaker(env);
  expect(put).toHaveBeenCalledWith("plan-realtime-fetches:circuit-breaker", "open", {
    expirationTtl: 120,
  });
});

it("buildPlanRealtimeOverloadRetryDelaySeconds returns delay within 60..179 range", async () => {
  const { buildPlanRealtimeOverloadRetryDelaySeconds } = await import("./worker");
  const value = buildPlanRealtimeOverloadRetryDelaySeconds();
  expect(value >= 60).toBe(true);
  expect(value < 180).toBe(true);
});

it("buildConnectionPressureRecoveryDelaySeconds avoids :01 with a bounded 120..239 delay", async () => {
  const { buildConnectionPressureRecoveryDelaySeconds } = await import("./worker");
  const value = buildConnectionPressureRecoveryDelaySeconds();
  expect(value >= 120).toBe(true);
  expect(value < 240).toBe(true);
});

it("handleJob plan-realtime-fetches short-circuits when circuit breaker is open", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch, listSchedulableRaceSourcesByDate } = await import("./storage");
  const get = vi.fn(async () => "open");
  const put = vi.fn(async () => {});
  const env = buildEnv({
    DETAIL_SECTION_CACHE_KV: { get, put } as unknown as KVNamespace,
  });
  await handleJob(env, { date: "20260512", type: "plan-realtime-fetches" });
  expect(listSchedulableRaceSourcesByDate).not.toHaveBeenCalled();
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-realtime-fetches",
    "skipped",
    null,
    "circuit breaker open",
  );
});

it("handleJob plan-realtime-fetches trips circuit breaker when D1 overload error escapes", async () => {
  const { handleJob } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockRejectedValueOnce(
    new Error("D1_ERROR: D1 DB is overloaded. Please try again later."),
  );
  const get = vi.fn(async () => null);
  const put = vi.fn(async () => {});
  const env = buildEnv({
    DETAIL_SECTION_CACHE_KV: { get, put } as unknown as KVNamespace,
    REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z",
  });
  await expect(handleJob(env, { date: "20260512", type: "plan-realtime-fetches" })).rejects.toThrow(
    "D1 DB is overloaded",
  );
  expect(put).toHaveBeenCalledWith("plan-realtime-fetches:circuit-breaker", "open", {
    expirationTtl: 120,
  });
});

it("handleJob plan-realtime-fetches does not trip breaker for non-overload errors", async () => {
  const { handleJob } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockRejectedValueOnce(new Error("kaboom"));
  const get = vi.fn(async () => null);
  const put = vi.fn(async () => {});
  const env = buildEnv({
    DETAIL_SECTION_CACHE_KV: { get, put } as unknown as KVNamespace,
    REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z",
  });
  await expect(handleJob(env, { date: "20260512", type: "plan-realtime-fetches" })).rejects.toThrow(
    "kaboom",
  );
  expect(put).not.toHaveBeenCalled();
});

it("handleJob plan-realtime-fetches swallows KV put error inside trip path", async () => {
  const { handleJob } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockRejectedValueOnce(
    new Error("Too many requests queued"),
  );
  const get = vi.fn(async () => null);
  const put = vi.fn(async () => {
    throw new Error("kv down");
  });
  const env = buildEnv({
    DETAIL_SECTION_CACHE_KV: { get, put } as unknown as KVNamespace,
    REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z",
  });
  await expect(handleJob(env, { date: "20260512", type: "plan-realtime-fetches" })).rejects.toThrow(
    "Too many requests queued",
  );
});

it("handleJob plan-realtime-fetches swallows logFetch error in skipped path", async () => {
  const { handleJob } = await import("./worker");
  const { logFetch } = await import("./storage");
  vi.mocked(logFetch).mockRejectedValueOnce(new Error("log down"));
  const get = vi.fn(async () => "open");
  const put = vi.fn(async () => {});
  const env = buildEnv({
    DETAIL_SECTION_CACHE_KV: { get, put } as unknown as KVNamespace,
  });
  await handleJob(env, { date: "20260512", type: "plan-realtime-fetches" });
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-realtime-fetches",
    "skipped",
    null,
    "circuit breaker open",
  );
});
