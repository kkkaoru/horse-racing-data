// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { Env, NarRaceSource } from "./types";
import type { PremiumPaddockBulletin } from "./premium-race";

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
  };
});

const buildRace = (overrides?: Partial<NarRaceSource>): NarRaceSource =>
  ({
    babaCode: "06",
    debaUrl: "https://example.com/race/1",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "06",
    lastOddsFetchAt: null,
    lastWeightFetchAt: null,
    oddsLinks: {},
    raceBango: "11",
    raceKey: "nar:20260512:06:11",
    raceName: "テストレース",
    raceStartAtJst: "2026-05-12T15:00:00+09:00",
    source: "nar",
    ...overrides,
  }) as NarRaceSource;

const buildBulletins = (): PremiumPaddockBulletin[] => [
  {
    commentText: "好調",
    evaluationText: "A",
    frameNumber: "1",
    groupKey: "favorite",
    horseName: "ウマ太郎",
    horseNumber: "1",
  },
];

const buildEnv = (overrides?: Partial<Env>): Env =>
  ({
    REALTIME_DB: {},
    REALTIME_JOBS: { send: vi.fn(async () => {}), sendBatch: vi.fn(async () => {}) },
    REALTIME_TEST_NOW: "2026-05-12T14:00:00+09:00",
    ...overrides,
  }) as unknown as Env;

beforeEach(() => {
  vi.clearAllMocks();
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response("ok", { status: 200 })),
  );
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

it("notifyPremiumPaddockIfNeeded marks skipped_started when race already started", async () => {
  const { notifyPremiumPaddockIfNeeded } = await import("./worker");
  const storage = await import("./storage");
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-12T16:00:00+09:00" } as never);
  await notifyPremiumPaddockIfNeeded(
    env,
    buildRace(),
    buildBulletins(),
    "2026-05-12T15:30:00+09:00",
  );
  expect(storage.recordPremiumPaddockNotificationEvent).toHaveBeenCalledTimes(1);
  expect(storage.updatePremiumPaddockNotificationState).toHaveBeenCalledTimes(1);
  expect(vi.mocked(storage.recordPremiumPaddockNotificationEvent).mock.calls[0]?.[1]).toMatchObject(
    {
      skipReason: "race_started",
      status: "skipped_started",
    },
  );
});

it("notifyPremiumPaddockIfNeeded marks skipped_empty when bulletins are empty", async () => {
  const { notifyPremiumPaddockIfNeeded } = await import("./worker");
  const storage = await import("./storage");
  await notifyPremiumPaddockIfNeeded(buildEnv(), buildRace(), [], "2026-05-12T13:00:00+09:00");
  expect(vi.mocked(storage.recordPremiumPaddockNotificationEvent).mock.calls[0]?.[1]).toMatchObject(
    {
      skipReason: "empty",
      status: "skipped_empty",
    },
  );
});

it("notifyPremiumPaddockIfNeeded marks skipped_unconfigured when webhook URL is missing", async () => {
  const { notifyPremiumPaddockIfNeeded } = await import("./worker");
  const storage = await import("./storage");
  await notifyPremiumPaddockIfNeeded(
    buildEnv(),
    buildRace(),
    buildBulletins(),
    "2026-05-12T13:00:00+09:00",
  );
  expect(vi.mocked(storage.recordPremiumPaddockNotificationEvent).mock.calls[0]?.[1]).toMatchObject(
    {
      skipReason: "webhook_not_configured",
      status: "skipped_unconfigured",
    },
  );
});

it("notifyPremiumPaddockIfNeeded skips re-notification when already notified at the same fetchedAt", async () => {
  const { notifyPremiumPaddockIfNeeded } = await import("./worker");
  const storage = await import("./storage");
  vi.mocked(storage.getPremiumPaddockNotificationState).mockResolvedValueOnce({
    lastNotifiedAt: "2026-05-12T13:00:00+09:00",
  } as never);
  await notifyPremiumPaddockIfNeeded(
    buildEnv({ PREMIUM_PADDOCK_DISCORD_WEBHOOK_URL: "https://discord.example/webhook" } as never),
    buildRace(),
    buildBulletins(),
    "2026-05-12T13:00:00+09:00",
  );
  expect(storage.recordPremiumPaddockNotificationEvent).not.toHaveBeenCalled();
  expect(vi.mocked(storage.updatePremiumPaddockNotificationState).mock.calls[0]?.[1]).toMatchObject(
    {
      skipReason: "already_notified",
      status: "skipped_duplicate",
    },
  );
});

it("notifyPremiumPaddockIfNeeded records duplicate event when already notified at different fetchedAt", async () => {
  const { notifyPremiumPaddockIfNeeded } = await import("./worker");
  const storage = await import("./storage");
  vi.mocked(storage.getPremiumPaddockNotificationState).mockResolvedValueOnce({
    lastNotifiedAt: "2026-05-12T12:30:00+09:00",
  } as never);
  await notifyPremiumPaddockIfNeeded(
    buildEnv({ PREMIUM_PADDOCK_DISCORD_WEBHOOK_URL: "https://discord.example/webhook" } as never),
    buildRace(),
    buildBulletins(),
    "2026-05-12T13:00:00+09:00",
  );
  expect(storage.recordPremiumPaddockNotificationEvent).toHaveBeenCalledTimes(1);
  expect(vi.mocked(storage.recordPremiumPaddockNotificationEvent).mock.calls[0]?.[1]).toMatchObject(
    {
      skipReason: "already_notified",
      status: "skipped_duplicate",
    },
  );
});

it("notifyPremiumPaddockIfNeeded returns early when claim fails", async () => {
  const { notifyPremiumPaddockIfNeeded } = await import("./worker");
  const storage = await import("./storage");
  vi.mocked(storage.claimPremiumPaddockNotificationSend).mockResolvedValueOnce(false);
  await notifyPremiumPaddockIfNeeded(
    buildEnv({ PREMIUM_PADDOCK_DISCORD_WEBHOOK_URL: "https://discord.example/webhook" } as never),
    buildRace(),
    buildBulletins(),
    "2026-05-12T13:00:00+09:00",
  );
  expect(storage.recordPremiumPaddockNotificationEvent).not.toHaveBeenCalled();
  expect(fetch).not.toHaveBeenCalled();
});

it("notifyPremiumPaddockIfNeeded posts to Discord and records ok event on 200", async () => {
  const { notifyPremiumPaddockIfNeeded } = await import("./worker");
  const storage = await import("./storage");
  await notifyPremiumPaddockIfNeeded(
    buildEnv({
      PREMIUM_PADDOCK_DISCORD_BOT_NAME: "テスト Bot",
      PREMIUM_PADDOCK_DISCORD_WEBHOOK_URL: "https://discord.example/webhook",
    } as never),
    buildRace({ keibajoCode: "99" }),
    buildBulletins(),
    "2026-05-12T13:00:00+09:00",
  );
  expect(fetch).toHaveBeenCalledTimes(1);
  expect(vi.mocked(storage.recordPremiumPaddockNotificationEvent).mock.calls[0]?.[1]).toMatchObject(
    {
      status: "ok",
    },
  );
  expect(vi.mocked(storage.updatePremiumPaddockNotificationState).mock.calls[0]?.[1]).toMatchObject(
    {
      status: "ok",
    },
  );
});

it("notifyPremiumPaddockIfNeeded falls back to placeholder labels when raceName/keibajoCode missing", async () => {
  const { notifyPremiumPaddockIfNeeded } = await import("./worker");
  await notifyPremiumPaddockIfNeeded(
    buildEnv({ PREMIUM_PADDOCK_DISCORD_WEBHOOK_URL: "https://discord.example/webhook" } as never),
    buildRace({ keibajoCode: "ZZ", raceName: null }),
    buildBulletins(),
    "2026-05-12T13:00:00+09:00",
  );
  expect(fetch).toHaveBeenCalledTimes(1);
  const rawBody = vi.mocked(fetch).mock.calls[0]?.[1]?.body;
  const body = JSON.parse(typeof rawBody === "string" ? rawBody : "");
  expect(body.embeds[0].description).toContain("レース名未取得");
});

it("notifyPremiumPaddockIfNeeded records failed event and throws when Discord returns non-OK", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response("err", { status: 500 })),
  );
  const { notifyPremiumPaddockIfNeeded } = await import("./worker");
  const storage = await import("./storage");
  await expect(
    notifyPremiumPaddockIfNeeded(
      buildEnv({ PREMIUM_PADDOCK_DISCORD_WEBHOOK_URL: "https://discord.example/webhook" } as never),
      buildRace(),
      buildBulletins(),
      "2026-05-12T13:00:00+09:00",
    ),
  ).rejects.toThrow("premium paddock notification failed: 500");
  expect(vi.mocked(storage.recordPremiumPaddockNotificationEvent).mock.calls[0]?.[1]).toMatchObject(
    {
      status: "failed",
    },
  );
});
