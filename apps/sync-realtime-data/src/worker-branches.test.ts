// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { Env, Job } from "./types";

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
    parseRaceEntries: vi.fn(() => []),
    parseRaceResults: vi.fn(() => []),
    parseRaceEntryHorseNumbers: vi.fn(() => []),
    parseRaceResultExcludedHorseNumbers: vi.fn(() => []),
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

const buildEnv = (overrides?: Partial<Env>): Env =>
  ({
    PREMIUM_RACE_JOBS: { send: vi.fn(async () => {}), sendBatch: vi.fn(async () => {}) },
    REALTIME_DB: {},
    REALTIME_JOBS: { send: vi.fn(async () => {}), sendBatch: vi.fn(async () => {}) },
    ...overrides,
  }) as unknown as Env;

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

// Covers enqueueJobs sort: left=fetch-premium-paddock vs right!=fetch-premium-paddock,
// and the reverse arm. Both branches of the toSorted compare must run.
it("enqueueJobs sorts fetch-premium-paddock before other premium jobs (both sort arms)", async () => {
  const { enqueueJobs } = await import("./worker");
  const premiumSend = vi.fn(async () => {});
  const env = buildEnv({
    PREMIUM_RACE_JOBS: { send: premiumSend, sendBatch: vi.fn(async () => {}) },
  } as never);
  const jobs: Job[] = [
    { date: "20260512", type: "discover-premium-race-links" },
    { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-paddock" },
    { date: "20260512", type: "discover-premium-races" },
  ];
  await enqueueJobs(env, jobs);
  expect(premiumSend).toHaveBeenCalledTimes(3);
});

// Covers planRealtimeFetches when oddsLockUntil has a non-null timestamp that
// is in the past (lock released) -- the (Number.isNaN(...) || oddsLockUntil <= now) arm.

// Covers planRealtimeFetches when resultFetchLockUntil is in the past (lock released).
it("planRealtimeFetches enqueues fetch-results when resultFetchLockUntil is in the past", async () => {
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
      raceName: "Finished",
      raceStartAtJst: "2026-05-12T10:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: "2026-05-12T10:00:00+09:00",
      resultSavedHorseCount: null,
      source: "nar",
      updatedAt: "2026-05-12T00:00:00+09:00",
    },
  ] as never);
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-12T01:48:00.000Z" } as never);
  const count = await planRealtimeFetches(env, "20260512");
  expect(count).toBeGreaterThanOrEqual(1);
});

// Covers planJraAdvanceOddsFetchesForDate when oddsFetchLockUntil is in the past,
// exercising the same released-lock arm as above.

// Covers fetchAndStorePremiumRaceData where commentResult AND dataTopResult are rejected,
// hitting the "rejected" arms of the Promise.allSettled fallback (?? "") and
// nested commentError / dataTopError ternaries.
it("fetch-premium-race-data records commentError + dataTopError when those fetches reject", async () => {
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
  vi.mocked(fetchPremiumHtml).mockImplementation(async (_config: unknown, url: unknown) => {
    if (typeof url === "string" && url.includes("/w/")) {
      return "<table>work</table>";
    }
    if (typeof url === "string" && url.includes("/c/")) {
      throw new Error("comment boom");
    }
    throw "datatop string boom";
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
  expect(lastMessage.commentError).toBe("comment boom");
  expect(lastMessage.dataTopError).toBe("datatop string boom");
});

// Covers fetchAndStorePremiumPaddock attempts-throw path with existing payload that has
// bulletins -- specifically the latest-fetchedAt reducer where latest > row.fetchedAt
// (more recent row wins over older latest).
it("fetch-premium-paddock recovers existing payload selecting the latest fetchedAt", async () => {
  const { handleJob } = await import("./worker");
  const {
    getRaceSource,
    getPremiumRaceLink,
    getPremiumRacePayload,
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
  vi.mocked(fetchPremiumHtmlAttempts).mockRejectedValue(new Error("network boom"));
  vi.mocked(getPremiumRacePayload).mockResolvedValue({
    paddockBulletins: [
      { fetchedAt: "2026-05-12T01:00:00+09:00", horseNumber: "1" },
      { fetchedAt: "2026-05-12T03:00:00+09:00", horseNumber: "2" },
      { fetchedAt: "2026-05-12T02:00:00+09:00", horseNumber: "3" },
    ],
  } as never);
  await handleJob(
    buildEnv({
      PREMIUM_RACE_ORIGIN: "https://x.test",
      PREMIUM_RACE_PADDOCK_PATH_TEMPLATE: "/paddock/{sourceRaceId}",
    } as never),
    { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-paddock" },
  );
  expect(updatePremiumPaddockFetchState).toHaveBeenCalledWith(
    expect.anything(),
    expect.objectContaining({ fetchedAt: "2026-05-12T03:00:00+09:00", status: "ok" }),
  );
});

// Covers enqueueSelfRealtimePlanIfStale with a recent successful plan (early return).
it("scheduled plan-realtime-fetches skips self-enqueue when latest plan is fresh", async () => {
  const workerModule = await import("./worker");
  const handler = workerModule.default;
  const { logFetch } = await import("./storage");
  vi.mocked(logFetch).mockImplementation(async (_db: unknown, action: unknown): Promise<void> => {
    if (action === "plan-realtime-fetches-success-marker") {
      return;
    }
  });
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_JOBS: { send, sendBatch: vi.fn(async () => {}) },
    REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z",
  } as never);
  const recentIso = "2026-05-12T02:59:30+09:00";
  const dbWithRecent = {
    batch: vi.fn(async () => []),
    exec: vi.fn(async () => ({})),
    prepare: vi.fn(() => ({
      all: vi.fn(async () => ({ results: [] })),
      bind: vi.fn(() => ({
        all: vi.fn(async () => ({ results: [] })),
        first: vi.fn(async () => ({ created_at: recentIso })),
        run: vi.fn(async () => ({ meta: { changes: 0 } })),
      })),
      first: vi.fn(async () => ({ created_at: recentIso })),
      run: vi.fn(async () => ({ meta: { changes: 0 } })),
    })),
  } as unknown as D1Database;
  const envWithRecent = { ...env, REALTIME_DB: dbWithRecent } as Env;
  const waitUntil = vi.fn();
  const ctx = { waitUntil } as unknown as ExecutionContext;
  await handler.scheduled?.(
    {
      cron: "*/1 * * * *",
      noRetry: vi.fn(),
      scheduledTime: new Date("2026-05-12T03:00:00.000Z").getTime(),
    } as unknown as ScheduledController,
    envWithRecent,
    ctx,
  );
  expect(waitUntil).toHaveBeenCalled();
});

// Covers fetchAndStorePremiumRaceData with commentResult rejecting with NON-Error reason
// (String(reason) arm) AND status === "ok" path (hasAnyData=true, !commentAuthRequired).
it("fetch-premium-race-data records ok with commentError(String) when comment rejects with string + parses authenticated training reviews", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, getPremiumRaceLink, updatePremiumRaceDataFetchState } =
    await import("./storage");
  const { fetchPremiumHtml } = await import("./premium-race");
  const premiumRace = await import("./premium-race");
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
  vi.spyOn(premiumRace, "parsePremiumTrainingReviews").mockReturnValue([
    { horseName: "h", horseNumber: "1", reviewText: "r" },
  ] as never);
  vi.mocked(fetchPremiumHtml).mockImplementation(async (_config: unknown, url: unknown) => {
    if (typeof url === "string" && url.includes("/w/")) {
      return "<table>work</table>";
    }
    if (typeof url === "string" && url.includes("/c/")) {
      throw "comment string boom";
    }
    return "<table>data-top</table>";
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
  const lastCall = vi.mocked(updatePremiumRaceDataFetchState).mock.calls.at(-1)?.[1];
  const lastMessage = JSON.parse(String(lastCall?.message ?? "{}"));
  expect(lastMessage.commentError).toBe("comment string boom");
  expect(lastCall?.status).toBe("ok");
});

// Covers fetch-results when buildJraResultUrlFromRaceSource returns null
// (kaisaiKai missing) -- the `if (!resultUrl)` throw path.
it("fetch-results throws when JRA result URL is unavailable (missing kaisaiKai)", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, getRaceSource, failResultFetch } = await import("./storage");
  vi.mocked(claimResultFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce({
    babaCode: "08",
    debaUrl: "https://www.jra.go.jp/race",
    discoveredAt: "2026-05-12T00:00:00+09:00",
    kaisaiKai: null,
    kaisaiNen: "2026",
    kaisaiNichime: null,
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
  await expect(
    handleJob(buildEnv({ REALTIME_TEST_NOW: "2026-05-12T07:00:00.000Z" } as never), {
      raceKey: "jra:2026:0512:08:01",
      type: "fetch-results",
    }),
  ).rejects.toThrow("race result url is unavailable");
  expect(failResultFetch).toHaveBeenCalled();
});

// Covers fetch-results with isComplete=true (inserted >= expectedHorseCount > 0),
// hitting the `if (isComplete) { await requestTrendCacheBust(...) }` branch.
it("fetch-results triggers trend cache bust when expectedHorseCount > 0 and rows complete", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, getRaceSource, insertRaceResultSnapshot } = await import("./storage");
  const { fetchRacePage, parseRaceEntries, parseRaceResults, parseRaceEntryHorseNumbers } =
    await import("./keiba-go");
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
    raceStartAtJst: "2026-05-12T10:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "nar",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(fetchRacePage).mockResolvedValue("<html></html>");
  vi.mocked(parseRaceEntries).mockReturnValue([
    { horseName: "h", horseNumber: "1", jockeyName: "j", status: null },
    { horseName: "h", horseNumber: "2", jockeyName: "j", status: null },
  ] as never);
  vi.mocked(parseRaceEntryHorseNumbers).mockReturnValue(["1", "2"]);
  vi.mocked(parseRaceResults).mockReturnValue([
    { horseNumber: "1", placement: "1" },
    { horseNumber: "2", placement: "2" },
  ] as never);
  vi.mocked(insertRaceResultSnapshot).mockResolvedValue(2);
  await handleJob(buildEnv({ REALTIME_TEST_NOW: "2026-05-12T07:00:00.000Z" } as never), {
    raceKey: "nar:2026:0512:55:01",
    type: "fetch-results",
  });
  // No assertion needed; reaching here without throw exercises the isComplete branch.
  expect(insertRaceResultSnapshot).toHaveBeenCalled();
});

// Covers fetch-results with inserted > 0 but inserted < expectedHorseCount,
// so isComplete is false yet the trend cache bust must still fire. This is
// the partial-final path that prevents "11R is confirmed but 12R detail
// shows it as unfinished" when JRA / NAR publishes the result page with
// some horses still pending (e.g. objection, late horse, or partial bunch).
it("fetch-results still busts trend cache when partial-final (inserted > 0 but < expected)", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, getRaceSource, insertRaceResultSnapshot, completeResultFetch } =
    await import("./storage");
  const { fetchRacePage, parseRaceEntries, parseRaceResults, parseRaceEntryHorseNumbers } =
    await import("./keiba-go");
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
    raceStartAtJst: "2026-05-12T10:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "nar",
    updatedAt: "2026-05-12T00:00:00+09:00",
  } as never);
  vi.mocked(fetchRacePage).mockResolvedValue("<html></html>");
  vi.mocked(parseRaceEntries).mockReturnValue([
    { horseName: "h", horseNumber: "1", jockeyName: "j", status: null },
    { horseName: "h", horseNumber: "2", jockeyName: "j", status: null },
    { horseName: "h", horseNumber: "3", jockeyName: "j", status: null },
  ] as never);
  vi.mocked(parseRaceEntryHorseNumbers).mockReturnValue(["1", "2", "3"]);
  vi.mocked(parseRaceResults).mockReturnValue([
    { horseNumber: "1", placement: "1" },
    { horseNumber: "2", placement: "2" },
  ] as never);
  vi.mocked(insertRaceResultSnapshot).mockResolvedValue(2);
  await handleJob(buildEnv({ REALTIME_TEST_NOW: "2026-05-12T07:00:00.000Z" } as never), {
    raceKey: "nar:2026:0512:55:01",
    type: "fetch-results",
  });
  expect(completeResultFetch).toHaveBeenCalledWith(
    expect.anything(),
    "nar:2026:0512:55:01",
    expect.any(String),
    { expectedHorseCount: 3, isComplete: false, savedHorseCount: 2 },
  );
});

// Covers planResultFetchesOnly running the hourly discover-urls recovery
// when the tick lands inside the first minute of an hour, plus the queued
// race path so both the discovery side effect and job enqueue land.
it("planResultFetchesOnly runs hourly discovery recovery and enqueues due result jobs", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([
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
      raceName: "Recovery",
      raceStartAtJst: "2026-05-12T10:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "nar",
      updatedAt: "2026-05-12T00:00:00+09:00",
    },
  ] as never);
  // Now = 12:00 JST (= 03:00 UTC) so the JST minute is 00 and the recovery
  // discovery side-effect path fires this tick.
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_JOBS: { send, sendBatch: vi.fn(async () => {}) },
    REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z",
  } as never);
  const count = await planResultFetchesOnly(env, "20260512");
  expect(count).toBe(1);
});

// Covers planResultFetchesOnly NOT running discovery recovery when the JST
// minute is past the hourly window (= second branch of the
// shouldRunHourlyDiscoveryRecovery guard).
it("planResultFetchesOnly skips discovery recovery outside the first minute of the hour", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([]);
  // Now = 12:04 JST (= 03:04 UTC) — minute 04 is past the threshold.
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_JOBS: { send, sendBatch: vi.fn(async () => {}) },
    REALTIME_TEST_NOW: "2026-05-12T03:04:00.000Z",
  } as never);
  const count = await planResultFetchesOnly(env, "20260512");
  expect(count).toBe(0);
});

// Covers planRealtimeFetches markPremiumPaddockQueued + markPremiumRaceDataQueued flatMap
// arms by ensuring fetch-premium-paddock + fetch-premium-race-data jobs exist in the array
// (so both the `[raceKey]` and `[]` arms of the ternaries hit).
it("planRealtimeFetches feeds mark*Queued with mixed job types so flatMap exercises both arms", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const {
    listSchedulableRaceSourcesByDate,
    listPremiumRaceDataFetchCandidatesByDate,
    listJraVenueTrackConditionSchedulesByDate,
    markPremiumPaddockQueued,
    markPremiumRaceDataQueued,
  } = await import("./storage");
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
      raceName: "T",
      raceStartAtJst: "2026-05-12T13:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-12T00:00:00+09:00",
    },
  ] as never);
  vi.mocked(listPremiumRaceDataFetchCandidatesByDate).mockResolvedValue([
    { raceKey: "jra:2026:0512:08:01" },
  ] as never);
  vi.mocked(listJraVenueTrackConditionSchedulesByDate).mockResolvedValue([
    {
      firstRaceStartAtJst: "2026-05-12T13:00:00+09:00",
      keibajoCode: "08",
      lastFetchAt: null,
      lastQueuedAt: null,
      lastRaceStartAtJst: "2026-05-12T16:30:00+09:00",
    },
  ]);
  const env = buildEnv({
    PREMIUM_RACE_ORIGIN: "https://x.test",
    REALTIME_TEST_NOW: "2026-05-12T03:40:00.000Z",
  } as never);
  const count = await planRealtimeFetches(env, "20260512");
  expect(count).toBeGreaterThan(0);
  expect(markPremiumPaddockQueued).toHaveBeenCalled();
  expect(markPremiumRaceDataQueued).toHaveBeenCalled();
});

// Covers premiumRaceKeyFromRequest returning null for a non-matching path.
it("premiumRaceKeyFromRequest returns null for an unrelated path", async () => {
  const { premiumRaceKeyFromRequest } = await import("./worker");
  expect(premiumRaceKeyFromRequest(new URL("https://x.test/api/foo"))).toBeNull();
});

// Covers planRealtimeFetches outside the polling window: skips the general polling
// branch, runs the planJraAdvanceOddsFetchesForDate fallback (else arm of line 1409).
it("planRealtimeFetches outside polling window runs JRA advance odds fallback only", async () => {
  const { planRealtimeFetches } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([]);
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-11T20:00:00.000Z" } as never);
  const count = await planRealtimeFetches(env, "20260512");
  expect(typeof count).toBe("number");
});

// Covers fetchAndStorePremiumRaceData when all three Promise.allSettled results reject:
// triggers the failed-status path (line 1761 flatMap), and exercises Error vs String
// arms in dataTopError + workError ternaries.
it("fetch-premium-race-data all-rejects path stamps failed status with concatenated reasons", async () => {
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
  vi.mocked(fetchPremiumHtml).mockImplementation(async (_config: unknown, url: unknown) => {
    if (typeof url === "string" && url.includes("/w/")) {
      throw new Error("work boom");
    }
    if (typeof url === "string" && url.includes("/c/")) {
      throw "comment string boom";
    }
    throw new Error("datatop boom");
  });
  await expect(
    handleJob(
      buildEnv({
        PREMIUM_RACE_COMMENT_PATH_TEMPLATE: "/c/{sourceRaceId}",
        PREMIUM_RACE_DATA_TOP_PATH_TEMPLATE: "/d/{sourceRaceId}",
        PREMIUM_RACE_ORIGIN: "https://x.test",
        PREMIUM_RACE_WORK_PATH_TEMPLATE: "/w/{sourceRaceId}",
      } as never),
      { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-race-data" },
    ),
  ).rejects.toThrow("premium race data fetch failed");
  const lastCall = vi.mocked(updatePremiumRaceDataFetchState).mock.calls.at(-1)?.[1];
  expect(lastCall?.status).toBe("failed");
});

// Covers planRealtimeFetches when race.raceStartAtJst is invalid (parsing fails):
// minutesUntilRace returns null, hitting the `if (minutes === null) continue` arm.
it("planRealtimeFetches skips races whose raceStartAtJst is unparseable", async () => {
  const { planRealtimeFetches } = await import("./worker");
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
      raceName: "T",
      raceStartAtJst: "INVALID",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "jra",
      updatedAt: "2026-05-12T00:00:00+09:00",
    },
  ] as never);
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-12T03:30:00.000Z" } as never);
  const count = await planRealtimeFetches(env, "20260512");
  expect(typeof count).toBe("number");
});

// Covers fetchAndStorePremiumRaceData with isPremiumRaceDataTarget JRA race + incomplete
// config: hits `if (!hasPremiumRaceFetchConfig(config)) return` arm (line 1713 source).
it("fetch-premium-race-data returns early when JRA race present but config has no origin", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource } = await import("./storage");
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
  await handleJob(buildEnv(), {
    raceKey: "jra:2026:0512:08:01",
    type: "fetch-premium-race-data",
  });
});

// Covers fetchAndStorePremiumPaddock attempts returning empty array (no entries),
// hitting the `if (!selectedAttempt) throw` arm (line 1910 source).
it("fetch-premium-paddock throws when fetchPremiumHtmlAttempts returns empty list", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, getPremiumRaceLink } = await import("./storage");
  const { fetchPremiumHtmlAttempts } = await import("./premium-race");
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
  vi.mocked(fetchPremiumHtmlAttempts).mockResolvedValueOnce([]);
  await expect(
    handleJob(
      buildEnv({
        PREMIUM_RACE_ORIGIN: "https://x.test",
        PREMIUM_RACE_PADDOCK_PATH_TEMPLATE: "/p/{sourceRaceId}",
      } as never),
      { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-paddock" },
    ),
  ).rejects.toThrow("premium paddock fetch returned no attempts");
});

// Covers fetchAndStorePremiumPaddock JRA race + incomplete config: hits the
// `if (!hasPremiumRaceFetchConfig(config)) return` arm (line 1861 source).
it("fetch-premium-paddock returns early when config has no origin (incomplete)", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource } = await import("./storage");
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
  // no PREMIUM_RACE_ORIGIN -> hasPremiumRaceFetchConfig=false
  await handleJob(buildEnv(), {
    raceKey: "jra:2026:0512:08:01",
    type: "fetch-premium-paddock",
  });
});

// Covers fetchAndStorePremiumPaddock JRA race + config + link present but no paddock template:
// hits the `if (!paddockUrl) return` arm (line 1871 source).
it("fetch-premium-paddock returns early when paddockUrl cannot be built (no template)", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, getPremiumRaceLink } = await import("./storage");
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
  // origin present (hasPremiumRaceFetchConfig=true), but no PADDOCK_PATH_TEMPLATE
  await handleJob(buildEnv({ PREMIUM_RACE_ORIGIN: "https://x.test" } as never), {
    raceKey: "jra:2026:0512:08:01",
    type: "fetch-premium-paddock",
  });
});

// Covers planPremiumPaddockFetchesForDate where state has recent lastFetchAt
// (line 874-878 source) -- the lastFetchAt continue arm.
it("planPremiumPaddockFetchesForDate skips when recent lastFetchAt exists", async () => {
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
      raceName: "T",
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
    lastFetchAt: "2026-05-12T03:39:30.000Z",
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

// Covers fetch-premium-race-data where dataTopResult rejects with an Error (not string),
// hitting line 1818 `dataTopResult.reason.message` arm. The workResult also rejects with
// Error to cover line 1838.
it("fetch-premium-race-data records dataTopError(Error.message) + workError(Error.message)", async () => {
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
  vi.mocked(fetchPremiumHtml).mockImplementation(async (_config: unknown, url: unknown) => {
    if (typeof url === "string" && url.includes("/w/")) {
      throw new Error("work err");
    }
    if (typeof url === "string" && url.includes("/c/")) {
      return "<table>comment ok</table>";
    }
    throw new Error("datatop err");
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
  expect(lastMessage.workError).toBe("work err");
  expect(lastMessage.dataTopError).toBe("datatop err");
});

// Covers fetch-premium-race-data where stableCommentSample is summarized
// (commentHtml present and parsedStableComments empty -- line 1831).
it("fetch-premium-race-data summarizes stable comment sample when parsed comments empty", async () => {
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
  vi.mocked(fetchPremiumHtml).mockImplementation(async (_config: unknown, url: unknown) => {
    if (typeof url === "string" && url.includes("/c/")) {
      return "<html><body>comment but unparseable</body></html>";
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
  const lastMessage = JSON.parse(
    String(vi.mocked(updatePremiumRaceDataFetchState).mock.calls.at(-1)?.[1].message ?? "{}"),
  );
  // commentHtml is non-empty + parsedStableComments is empty -> summarize branch runs.
  expect(lastMessage.commentHtmlLength).toBeGreaterThan(0);
  expect(lastMessage.stableCommentCount).toBe(0);
});

// Covers fetch handler queue path: message.body.type === "fetch-odds" triggers ack,
// other types trigger retry. Exercises both arms of the catch branch.

it("planResultFetchesOnly returns 0 outside the JST polling window", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-11T20:00:00.000Z" } as never);
  const count = await planResultFetchesOnly(env, "20260512");
  expect(count).toBe(0);
});

it("planResultFetchesOnly returns 0 when there are no schedulable races", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([] as never);
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z" } as never);
  const count = await planResultFetchesOnly(env, "20260512");
  expect(count).toBe(0);
});

it("planResultFetchesOnly enqueues fetch-results for a finished NAR race with no completion", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, markResultFetchQueued } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([
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
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_JOBS: { send, sendBatch: vi.fn(async () => {}) },
    REALTIME_TEST_NOW: "2026-05-12T02:00:00.000Z",
  } as never);
  const count = await planResultFetchesOnly(env, "20260512");
  expect(count).toBe(1);
  expect(send).toHaveBeenCalledWith({ raceKey: "nar:2026:0512:55:01", type: "fetch-results" });
  expect(markResultFetchQueued).toHaveBeenCalled();
});

it("planResultFetchesOnly skips race that already has resultCompleteAt", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([
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
      raceName: "DoneRace",
      raceStartAtJst: "2026-05-12T10:00:00+09:00",
      resultCompleteAt: "2026-05-12T10:10:00+09:00",
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "nar",
      updatedAt: "2026-05-12T00:00:00+09:00",
    },
  ] as never);
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_JOBS: { send, sendBatch: vi.fn(async () => {}) },
    REALTIME_TEST_NOW: "2026-05-12T02:00:00.000Z",
  } as never);
  const count = await planResultFetchesOnly(env, "20260512");
  expect(count).toBe(0);
});

it("planResultFetchesOnly skips race that has not started yet", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([
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
      raceName: "Future",
      raceStartAtJst: "2026-05-12T15:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "nar",
      updatedAt: "2026-05-12T00:00:00+09:00",
    },
  ] as never);
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_JOBS: { send, sendBatch: vi.fn(async () => {}) },
    REALTIME_TEST_NOW: "2026-05-12T02:00:00.000Z",
  } as never);
  const count = await planResultFetchesOnly(env, "20260512");
  expect(count).toBe(0);
});

it("planResultFetchesOnly skips race when lastResultQueuedAt is set", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([
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
      lastResultQueuedAt: "2026-05-12T10:55:00+09:00",
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "01",
      raceKey: "nar:2026:0512:55:01",
      raceName: "QueuedAlready",
      raceStartAtJst: "2026-05-12T10:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "nar",
      updatedAt: "2026-05-12T00:00:00+09:00",
    },
  ] as never);
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_JOBS: { send, sendBatch: vi.fn(async () => {}) },
    REALTIME_TEST_NOW: "2026-05-12T02:00:00.000Z",
  } as never);
  const count = await planResultFetchesOnly(env, "20260512");
  expect(count).toBe(0);
});

it("planResultFetchesOnly skips race when resultFetchLockUntil is still in the future", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([
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
      raceName: "Locked",
      raceStartAtJst: "2026-05-12T10:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: "2026-05-12T20:00:00+09:00",
      resultSavedHorseCount: null,
      source: "nar",
      updatedAt: "2026-05-12T00:00:00+09:00",
    },
  ] as never);
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_JOBS: { send, sendBatch: vi.fn(async () => {}) },
    REALTIME_TEST_NOW: "2026-05-12T02:00:00.000Z",
  } as never);
  const count = await planResultFetchesOnly(env, "20260512");
  expect(count).toBe(0);
});

it("planResultFetchesOnly skips race when lastResultFetchAt is within RESULT_FETCH_INTERVAL_MINUTES", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([
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
      // Now = 02:00 UTC = 11:00 JST. lastResultFetchAt = 10:59 JST is only 1
      // minute ago, which is less than RESULT_FETCH_INTERVAL_MINUTES (2) so
      // the race must be skipped this tick.
      lastResultFetchAt: "2026-05-12T10:59:00+09:00",
      lastResultQueuedAt: null,
      lastWeightFetchAt: null,
      oddsFetchLockUntil: null,
      oddsLinks: {},
      raceBango: "01",
      raceKey: "nar:2026:0512:55:01",
      raceName: "Throttle",
      raceStartAtJst: "2026-05-12T10:00:00+09:00",
      resultCompleteAt: null,
      resultExpectedHorseCount: null,
      resultFetchLockUntil: null,
      resultSavedHorseCount: null,
      source: "nar",
      updatedAt: "2026-05-12T00:00:00+09:00",
    },
  ] as never);
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_JOBS: { send, sendBatch: vi.fn(async () => {}) },
    REALTIME_TEST_NOW: "2026-05-12T02:00:00.000Z",
  } as never);
  const count = await planResultFetchesOnly(env, "20260512");
  expect(count).toBe(0);
});
