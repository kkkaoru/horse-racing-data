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
  recordPartialResultFetch: vi.fn(async () => {}),
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
  DAILY_FEATURE_BUILD_CRON: "0 0 * * *",
  runDailyFeatureBuildForEnv: vi.fn(async () => ({})),
  listDailyRaceEntriesForRace: vi.fn(async () => []),
}));
vi.mock("./win5-queue", () => ({ handleWin5PredictionJob: vi.fn(async () => ({})) }));
vi.mock("./win5-cron", () => ({
  WIN5_DISCOVER_CRON: "0 0 * * *",
  logWin5CronResult: vi.fn(async () => {}),
}));
vi.mock("./running-style-cron", () => ({
  RUNNING_STYLE_INFERENCE_CRON: "*/10 * * * *",
  RUNNING_STYLE_PREWARM_CRON: "0 12 * * *",
  planRunningStylePredictionsForDate: vi.fn(async () => ({})),
  refreshViewerRunningStyleCachesForDate: vi.fn(async () => ({})),
  refreshViewerRunningStyleCacheForRace: vi.fn(async () => false),
  runRunningStyleCronTick: vi.fn(async () => ({})),
  formatTomorrowYYYYMMDDInJst: vi.fn(() => "20260513"),
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
vi.mock("./jra-track-condition", () => ({ fetchJraTrackConditionWithPlaywright: vi.fn() }));
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

const buildPremiumPaddockRaceSource = () =>
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
    raceName: "Test",
    raceStartAtJst: "2026-05-12T13:00:00+09:00",
    resultCompleteAt: null,
    resultExpectedHorseCount: null,
    resultFetchLockUntil: null,
    resultSavedHorseCount: null,
    source: "jra",
    updatedAt: "2026-05-12T00:00:00+09:00",
  }) as never;

const buildPaddockEnv = (overrides?: Partial<Env>): Env =>
  ({
    PREMIUM_RACE_ORIGIN: "https://x.test",
    PREMIUM_RACE_PADDOCK_PATH_TEMPLATE: "/paddock/{sourceRaceId}",
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

it("fetch-premium-paddock auth_required branch updates state and records event", async () => {
  const { handleJob } = await import("./worker");
  const {
    getRaceSource,
    getPremiumRaceLink,
    updatePremiumPaddockFetchState,
    recordPremiumPaddockNotificationEvent,
  } = await import("./storage");
  const { fetchPremiumHtmlAttempts, parsePremiumPaddockBulletins } = await import("./premium-race");
  vi.mocked(getRaceSource).mockResolvedValueOnce(buildPremiumPaddockRaceSource());
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  vi.mocked(fetchPremiumHtmlAttempts).mockResolvedValueOnce([
    { html: "<div>auth</div>", mode: "proxy" },
  ] as never);
  vi.mocked(parsePremiumPaddockBulletins).mockReturnValueOnce({
    authRequired: true,
    bulletins: [],
    pending: false,
    unavailable: false,
  });
  await handleJob(buildPaddockEnv(), {
    raceKey: "jra:2026:0512:08:01",
    type: "fetch-premium-paddock",
  });
  expect(vi.mocked(updatePremiumPaddockFetchState).mock.calls.at(-1)?.[1]).toMatchObject({
    status: "auth_required",
  });
  expect(recordPremiumPaddockNotificationEvent).toHaveBeenCalled();
});

it("fetch-premium-paddock unavailable branch updates state with status unavailable", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, getPremiumRaceLink, updatePremiumPaddockFetchState } =
    await import("./storage");
  const { fetchPremiumHtmlAttempts, parsePremiumPaddockBulletins } = await import("./premium-race");
  vi.mocked(getRaceSource).mockResolvedValueOnce(buildPremiumPaddockRaceSource());
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  vi.mocked(fetchPremiumHtmlAttempts).mockResolvedValueOnce([
    { html: "<div>404</div>", mode: "direct" },
  ] as never);
  vi.mocked(parsePremiumPaddockBulletins).mockReturnValueOnce({
    authRequired: false,
    bulletins: [],
    pending: false,
    unavailable: true,
  });
  await handleJob(buildPaddockEnv(), {
    raceKey: "jra:2026:0512:08:01",
    type: "fetch-premium-paddock",
  });
  expect(vi.mocked(updatePremiumPaddockFetchState).mock.calls.at(-1)?.[1]).toMatchObject({
    status: "unavailable",
  });
});

it("fetch-premium-paddock pending branch updates state with status pending", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, getPremiumRaceLink, updatePremiumPaddockFetchState } =
    await import("./storage");
  const { fetchPremiumHtmlAttempts, parsePremiumPaddockBulletins } = await import("./premium-race");
  vi.mocked(getRaceSource).mockResolvedValueOnce(buildPremiumPaddockRaceSource());
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  vi.mocked(fetchPremiumHtmlAttempts).mockResolvedValueOnce([
    { html: "<div>pending</div>", mode: "direct" },
  ] as never);
  vi.mocked(parsePremiumPaddockBulletins).mockReturnValueOnce({
    authRequired: false,
    bulletins: [],
    pending: true,
    unavailable: false,
  });
  await handleJob(buildPaddockEnv(), {
    raceKey: "jra:2026:0512:08:01",
    type: "fetch-premium-paddock",
  });
  expect(vi.mocked(updatePremiumPaddockFetchState).mock.calls.at(-1)?.[1]).toMatchObject({
    status: "pending",
  });
});

it("fetch-premium-paddock falls back through ensurePremiumRaceLink when no existing link", async () => {
  const { handleJob } = await import("./worker");
  const {
    getRaceSource,
    getPremiumRaceLink,
    upsertPremiumRaceLink,
    updatePremiumPaddockFetchState,
  } = await import("./storage");
  const { fetchPremiumHtmlAttempts } = await import("./premium-race");
  vi.mocked(getRaceSource).mockResolvedValueOnce(buildPremiumPaddockRaceSource());
  vi.mocked(getPremiumRaceLink).mockReset();
  vi.mocked(getPremiumRaceLink).mockResolvedValue(null);
  vi.mocked(fetchPremiumHtmlAttempts).mockResolvedValueOnce([
    { html: "<table></table>", mode: "direct" },
  ] as never);
  await handleJob(buildPaddockEnv(), {
    raceKey: "jra:2026:0512:08:01",
    type: "fetch-premium-paddock",
  });
  expect(upsertPremiumRaceLink).toHaveBeenCalled();
  expect(updatePremiumPaddockFetchState).toHaveBeenCalled();
});

it("fetch-premium-paddock empty branch updates state with status empty", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource, getPremiumRaceLink, updatePremiumPaddockFetchState } =
    await import("./storage");
  const { fetchPremiumHtmlAttempts } = await import("./premium-race");
  vi.mocked(getRaceSource).mockResolvedValueOnce(buildPremiumPaddockRaceSource());
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  vi.mocked(fetchPremiumHtmlAttempts).mockResolvedValueOnce([
    { html: "<table></table>", mode: "direct" },
  ] as never);
  await handleJob(buildPaddockEnv(), {
    raceKey: "jra:2026:0512:08:01",
    type: "fetch-premium-paddock",
  });
  expect(vi.mocked(updatePremiumPaddockFetchState).mock.calls.at(-1)?.[1]).toMatchObject({
    status: "empty",
  });
});

it("fetch-premium-paddock attempts throw + no existing payload throws and writes failed status", async () => {
  const { handleJob } = await import("./worker");
  const {
    getRaceSource,
    getPremiumRaceLink,
    getPremiumRacePayload,
    updatePremiumPaddockFetchState,
  } = await import("./storage");
  const { fetchPremiumHtmlAttempts } = await import("./premium-race");
  vi.mocked(getRaceSource).mockResolvedValueOnce(buildPremiumPaddockRaceSource());
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  vi.mocked(getPremiumRacePayload).mockResolvedValueOnce(null as never);
  vi.mocked(fetchPremiumHtmlAttempts).mockRejectedValueOnce(new Error("network down"));
  await expect(
    handleJob(buildPaddockEnv(), {
      raceKey: "jra:2026:0512:08:01",
      type: "fetch-premium-paddock",
    }),
  ).rejects.toThrow("network down");
  expect(vi.mocked(updatePremiumPaddockFetchState).mock.calls.at(-1)?.[1]).toMatchObject({
    status: "failed",
  });
});

it("fetch-premium-paddock success path with non-empty bulletins replaces data and updates ok", async () => {
  const { handleJob } = await import("./worker");
  const {
    getRaceSource,
    getPremiumRaceLink,
    getPremiumRacePayload,
    replacePremiumRaceData,
    updatePremiumPaddockFetchState,
  } = await import("./storage");
  const { fetchPremiumHtmlAttempts, parsePremiumPaddockBulletins } = await import("./premium-race");
  vi.mocked(getRaceSource).mockResolvedValueOnce(buildPremiumPaddockRaceSource());
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  vi.mocked(fetchPremiumHtmlAttempts).mockResolvedValueOnce([
    { html: "<table><tr>data</tr></table>", mode: "direct" },
  ] as never);
  vi.mocked(parsePremiumPaddockBulletins).mockReturnValueOnce({
    authRequired: false,
    bulletins: [
      {
        commentText: "好調",
        evaluationText: "A",
        frameNumber: "1",
        groupKey: "favorite",
        horseName: "馬1",
        horseNumber: "1",
      },
    ],
    pending: false,
    unavailable: false,
  });
  vi.mocked(getPremiumRacePayload).mockResolvedValueOnce({
    dataTopHorses: [],
    paddockBulletins: [
      {
        commentText: "好調",
        evaluationText: "A",
        fetchedAt: "2026-05-12T11:00:00+09:00",
        frameNumber: "1",
        groupKey: "favorite",
        horseName: "馬1",
        horseNumber: "1",
      },
    ],
    stableComments: [],
    trainingReviews: [],
  } as never);
  await handleJob(buildPaddockEnv(), {
    raceKey: "jra:2026:0512:08:01",
    type: "fetch-premium-paddock",
  });
  expect(replacePremiumRaceData).toHaveBeenCalledTimes(1);
  expect(vi.mocked(updatePremiumPaddockFetchState).mock.calls.at(-1)?.[1]).toMatchObject({
    status: "ok",
  });
});

it("fetch-premium-paddock attempts throw + existing payload with bulletins writes ok and returns", async () => {
  const { handleJob } = await import("./worker");
  const {
    getRaceSource,
    getPremiumRaceLink,
    getPremiumRacePayload,
    updatePremiumPaddockFetchState,
  } = await import("./storage");
  const { fetchPremiumHtmlAttempts } = await import("./premium-race");
  vi.mocked(getRaceSource).mockResolvedValueOnce(buildPremiumPaddockRaceSource());
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce({
    entryUrl: "https://x.test/race?race_id=202605120801",
    sourceRaceId: "202605120801",
  } as never);
  vi.mocked(getPremiumRacePayload).mockResolvedValueOnce({
    dataTopHorses: [],
    paddockBulletins: [
      {
        commentText: "x",
        evaluationText: null,
        fetchedAt: "2026-05-12T12:30:00+09:00",
        frameNumber: "1",
        groupKey: "favorite",
        horseName: "馬",
        horseNumber: "1",
      },
    ],
    stableComments: [],
    trainingReviews: [],
  } as never);
  vi.mocked(fetchPremiumHtmlAttempts).mockRejectedValueOnce(new Error("network down"));
  await handleJob(buildPaddockEnv(), {
    raceKey: "jra:2026:0512:08:01",
    type: "fetch-premium-paddock",
  });
  expect(vi.mocked(updatePremiumPaddockFetchState).mock.calls.at(-1)?.[1]).toMatchObject({
    status: "ok",
  });
});
