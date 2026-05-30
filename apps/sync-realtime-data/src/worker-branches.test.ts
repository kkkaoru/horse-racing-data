// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { Env, Job, NarRaceSource, RaceEntry, RaceResult } from "./types";
import type { PremiumRacePayload, SchedulableRaceSource } from "./storage";
import type { PremiumRaceLink, PremiumTrainingReview } from "./premium-race";

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
  buildRealtimePayload: vi.fn(async () => ({
    horseWeights: null,
    odds: null,
    raceKey: "",
    raceResults: null,
    source: null,
  })),
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

// Mock D1Database / DurableObjectNamespace / R2Bucket / Queue stubs are scoped
// to this test file. The Env type pulls in abstract framework classes that we
// cannot satisfy structurally without a one-time bridge cast inside the
// `buildEnv` factory — every test body uses `buildEnv()` and never touches
// `as unknown as Env` directly (typescript rule 28).
interface BuildEnvOverrides extends Partial<
  Omit<Env, "REALTIME_DB" | "RACE_TREND_DAILY_TRACK_DO">
> {
  REALTIME_DB?: object;
  RACE_TREND_DAILY_TRACK_DO?: object;
}

const buildEnv = (overrides?: BuildEnvOverrides): Env => {
  const baseQueue: Queue<Job> = {
    send: vi.fn(async (_message: Job) => {}),
    sendBatch: vi.fn(async () => {}),
  };
  const base = {
    PREMIUM_RACE_JOBS: baseQueue,
    REALTIME_DB: {},
    REALTIME_JOBS: baseQueue,
    ...overrides,
  };
  return base satisfies BuildEnvOverrides as unknown as Env;
};

// Build a SchedulableRaceSource fixture without leaking dead-test-only fields
// (`discoveredAt`, `resultExpectedHorseCount`, `resultSavedHorseCount`,
// `updatedAt`) that used to require `as never` to satisfy the production
// interface (= dropped excess properties).
const JRA_RACE_BASE = {
  babaCode: "08",
  debaUrl: "https://www.jra.go.jp/race",
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
  resultFetchLockUntil: null,
  source: "jra",
} satisfies SchedulableRaceSource;

const NAR_RACE_BASE = {
  babaCode: "22",
  debaUrl: "https://nar.example/race",
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
  resultFetchLockUntil: null,
  source: "nar",
} satisfies SchedulableRaceSource;

const buildJraSchedulableRaceSource = (
  overrides?: Partial<SchedulableRaceSource>,
): SchedulableRaceSource => ({ ...JRA_RACE_BASE, ...overrides });

const buildNarSchedulableRaceSource = (
  overrides?: Partial<SchedulableRaceSource>,
): SchedulableRaceSource => ({ ...NAR_RACE_BASE, ...overrides });

const buildJraNarRaceSource = (overrides?: Partial<NarRaceSource>): NarRaceSource => ({
  babaCode: JRA_RACE_BASE.babaCode,
  debaUrl: JRA_RACE_BASE.debaUrl,
  kaisaiKai: JRA_RACE_BASE.kaisaiKai,
  kaisaiNen: JRA_RACE_BASE.kaisaiNen,
  kaisaiNichime: JRA_RACE_BASE.kaisaiNichime,
  kaisaiTsukihi: JRA_RACE_BASE.kaisaiTsukihi,
  keibajoCode: JRA_RACE_BASE.keibajoCode,
  lastOddsFetchAt: JRA_RACE_BASE.lastOddsFetchAt,
  lastWeightFetchAt: JRA_RACE_BASE.lastWeightFetchAt,
  oddsLinks: JRA_RACE_BASE.oddsLinks,
  raceBango: JRA_RACE_BASE.raceBango,
  raceKey: JRA_RACE_BASE.raceKey,
  raceName: JRA_RACE_BASE.raceName,
  raceStartAtJst: JRA_RACE_BASE.raceStartAtJst,
  source: JRA_RACE_BASE.source,
  ...overrides,
});

const buildNarNarRaceSource = (overrides?: Partial<NarRaceSource>): NarRaceSource => ({
  babaCode: NAR_RACE_BASE.babaCode,
  debaUrl: NAR_RACE_BASE.debaUrl,
  kaisaiKai: NAR_RACE_BASE.kaisaiKai,
  kaisaiNen: NAR_RACE_BASE.kaisaiNen,
  kaisaiNichime: NAR_RACE_BASE.kaisaiNichime,
  kaisaiTsukihi: NAR_RACE_BASE.kaisaiTsukihi,
  keibajoCode: NAR_RACE_BASE.keibajoCode,
  lastOddsFetchAt: NAR_RACE_BASE.lastOddsFetchAt,
  lastWeightFetchAt: NAR_RACE_BASE.lastWeightFetchAt,
  oddsLinks: NAR_RACE_BASE.oddsLinks,
  raceBango: NAR_RACE_BASE.raceBango,
  raceKey: NAR_RACE_BASE.raceKey,
  raceName: NAR_RACE_BASE.raceName,
  raceStartAtJst: NAR_RACE_BASE.raceStartAtJst,
  source: NAR_RACE_BASE.source,
  ...overrides,
});

const PREMIUM_RACE_LINK: PremiumRaceLink = {
  entryUrl: "https://x.test/race?race_id=202605120801",
  sourceRaceId: "202605120801",
};

const buildPremiumRaceLink = (overrides?: Partial<PremiumRaceLink>): PremiumRaceLink => ({
  ...PREMIUM_RACE_LINK,
  ...overrides,
});

type RaceEntryFixture = Omit<RaceEntry, "fetchedAt">;
type RaceResultFixture = Omit<RaceResult, "fetchedAt">;

const buildRaceEntry = (overrides?: Partial<RaceEntryFixture>): RaceEntryFixture => ({
  horseName: "h",
  horseNumber: "1",
  jockeyName: "j",
  status: null,
  ...overrides,
});

const buildRaceResult = (overrides?: Partial<RaceResultFixture>): RaceResultFixture => ({
  finishPosition: "1",
  horseName: null,
  horseNumber: "1",
  time: null,
  ...overrides,
});

const buildPremiumTrainingReview = (
  overrides?: Partial<PremiumTrainingReview>,
): PremiumTrainingReview => ({
  commentText: "r",
  evaluationGrade: null,
  evaluationText: null,
  horseName: "h",
  horseNumber: "1",
  riderName: null,
  trainingDate: "2026-05-12",
  ...overrides,
});

type PaddockBulletinFixture = PremiumRacePayload["paddockBulletins"][number];

const buildPaddockBulletinFixture = (
  overrides: Partial<PaddockBulletinFixture> &
    Pick<PaddockBulletinFixture, "fetchedAt" | "horseNumber">,
): PaddockBulletinFixture => ({
  commentText: null,
  evaluationText: null,
  frameNumber: null,
  groupKey: "favorite",
  horseName: null,
  ...overrides,
});

const buildEmptyPremiumRacePayload = (
  overrides?: Partial<PremiumRacePayload>,
): PremiumRacePayload => ({
  dataTopHorses: [],
  paddockBulletins: [],
  stableComments: [],
  trainingReviews: [],
  ...overrides,
});

interface ExecutionContextOverrides {
  waitUntil?: ExecutionContext["waitUntil"];
}

// ExecutionContext is a 3-member interface but `props` is `unknown` and
// `passThroughOnException` is a void no-op; we only assert on `waitUntil`
// across the tests, so this builder fills the rest with no-op fakes.
const buildExecutionContext = (overrides?: ExecutionContextOverrides): ExecutionContext => ({
  passThroughOnException: vi.fn(),
  props: undefined,
  waitUntil: overrides?.waitUntil ?? vi.fn(),
});

interface ScheduledControllerOverrides {
  cron?: string;
  scheduledTime?: number;
}

const buildScheduledController = (
  overrides?: ScheduledControllerOverrides,
): ScheduledController => ({
  cron: overrides?.cron ?? "*/1 * * * *",
  noRetry: vi.fn(),
  scheduledTime: overrides?.scheduledTime ?? 0,
});

// D1Database is an abstract framework class; the cast lives once inside this
// helper so test bodies do not repeat `as unknown as D1Database`. The shape
// matches what the scheduled-cron test exercises (prepare/bind/all/first/run).
const buildFakeD1WithRecentMarker = (recentIso: string): D1Database => {
  const db = {
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
  };
  return db satisfies object as unknown as D1Database;
};

// Env override that swaps REALTIME_DB without re-introducing `as Env` at the
// call site (the buildEnv helper already widens to Env). The spread preserves
// every other binding from the original Env unchanged.
const replaceRealtimeDb = (env: Env, db: D1Database): Env => ({ ...env, REALTIME_DB: db });

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
  });
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
    buildNarSchedulableRaceSource({
      raceName: "Finished",
      resultFetchLockUntil: "2026-05-12T10:00:00+09:00",
    }),
  ]);
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-12T01:48:00.000Z" });
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
  vi.mocked(getRaceSource).mockResolvedValue(buildJraNarRaceSource());
  vi.mocked(getPremiumRaceLink).mockResolvedValue(buildPremiumRaceLink());
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
    }),
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
  vi.mocked(getRaceSource).mockResolvedValue(buildJraNarRaceSource());
  vi.mocked(getPremiumRaceLink).mockResolvedValue(buildPremiumRaceLink());
  vi.mocked(fetchPremiumHtmlAttempts).mockRejectedValue(new Error("network boom"));
  vi.mocked(getPremiumRacePayload).mockResolvedValue(
    buildEmptyPremiumRacePayload({
      paddockBulletins: [
        buildPaddockBulletinFixture({ fetchedAt: "2026-05-12T01:00:00+09:00", horseNumber: "1" }),
        buildPaddockBulletinFixture({ fetchedAt: "2026-05-12T03:00:00+09:00", horseNumber: "2" }),
        buildPaddockBulletinFixture({ fetchedAt: "2026-05-12T02:00:00+09:00", horseNumber: "3" }),
      ],
    }),
  );
  await handleJob(
    buildEnv({
      PREMIUM_RACE_ORIGIN: "https://x.test",
      PREMIUM_RACE_PADDOCK_PATH_TEMPLATE: "/paddock/{sourceRaceId}",
    }),
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
  });
  const recentIso = "2026-05-12T02:59:30+09:00";
  const dbWithRecent = buildFakeD1WithRecentMarker(recentIso);
  const envWithRecent = replaceRealtimeDb(env, dbWithRecent);
  const waitUntil = vi.fn();
  const ctx = buildExecutionContext({ waitUntil });
  await handler.scheduled?.(
    buildScheduledController({
      scheduledTime: new Date("2026-05-12T03:00:00.000Z").getTime(),
    }),
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
  vi.mocked(getRaceSource).mockResolvedValue(buildJraNarRaceSource());
  vi.mocked(getPremiumRaceLink).mockResolvedValue(buildPremiumRaceLink());
  vi.spyOn(premiumRace, "parsePremiumTrainingReviews").mockReturnValue([
    buildPremiumTrainingReview(),
  ]);
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
    }),
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
  vi.mocked(getRaceSource).mockResolvedValueOnce(
    buildJraNarRaceSource({ kaisaiKai: null, kaisaiNichime: null }),
  );
  await expect(
    handleJob(buildEnv({ REALTIME_TEST_NOW: "2026-05-12T07:00:00.000Z" }), {
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
  vi.mocked(getRaceSource).mockResolvedValueOnce(buildNarNarRaceSource());
  vi.mocked(fetchRacePage).mockResolvedValue("<html></html>");
  vi.mocked(parseRaceEntries).mockReturnValue([
    buildRaceEntry({ horseNumber: "1" }),
    buildRaceEntry({ horseNumber: "2" }),
  ]);
  vi.mocked(parseRaceEntryHorseNumbers).mockReturnValue(["1", "2"]);
  vi.mocked(parseRaceResults).mockReturnValue([
    buildRaceResult({ finishPosition: "1", horseNumber: "1" }),
    buildRaceResult({ finishPosition: "2", horseNumber: "2" }),
  ]);
  vi.mocked(insertRaceResultSnapshot).mockResolvedValue(2);
  await handleJob(buildEnv({ REALTIME_TEST_NOW: "2026-05-12T07:00:00.000Z" }), {
    raceKey: "nar:2026:0512:55:01",
    type: "fetch-results",
  });
  // No assertion needed; reaching here without throw exercises the isComplete branch.
  expect(insertRaceResultSnapshot).toHaveBeenCalled();
});

// Covers the RACE_TREND_DAILY_TRACK_DO push helper hitting the bound stub fetch
// with a fully-built row in the fetch-results happy path. Without this case
// the binding access in pushResultsToRaceTrendDO never resolves to a real
// stub.fetch and the line stays branch-uncovered.
it("fetch-results pushes the freshly built row to the RACE_TREND_DAILY_TRACK_DO stub", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, getRaceSource, insertRaceResultSnapshot } = await import("./storage");
  const { fetchRacePage, parseRaceEntries, parseRaceResults, parseRaceEntryHorseNumbers } =
    await import("./keiba-go");
  vi.mocked(claimResultFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce(buildNarNarRaceSource());
  vi.mocked(fetchRacePage).mockResolvedValue("<html></html>");
  vi.mocked(parseRaceEntries).mockReturnValue([
    buildRaceEntry({ horseNumber: "1" }),
    buildRaceEntry({ horseNumber: "2" }),
  ]);
  vi.mocked(parseRaceEntryHorseNumbers).mockReturnValue(["1", "2"]);
  vi.mocked(parseRaceResults).mockReturnValue([
    buildRaceResult({ finishPosition: "1", horseNumber: "1" }),
    buildRaceResult({ finishPosition: "2", horseNumber: "2" }),
  ]);
  vi.mocked(insertRaceResultSnapshot).mockResolvedValue(2);
  const stubFetch = vi.fn(
    async (_url: string, _init?: RequestInit): Promise<Response> =>
      new Response(JSON.stringify({ ok: true }), { status: 200 }),
  );
  const idFromName = vi.fn((name: string): string => name);
  const get = vi.fn((_id: string) => ({ fetch: stubFetch }));
  await handleJob(
    buildEnv({
      RACE_TREND_DAILY_TRACK_DO: { get, idFromName },
      REALTIME_TEST_NOW: "2026-05-12T07:00:00.000Z",
    }),
    { raceKey: "nar:2026:0512:55:01", type: "fetch-results" },
  );
  expect(idFromName).toHaveBeenCalledTimes(1);
  expect(idFromName.mock.calls[0]![0]).toBe("nar:20260512:55");
  expect(stubFetch).toHaveBeenCalledTimes(1);
  expect(stubFetch.mock.calls[0]![0]).toBe("https://race-trend-daily-track-do/push");
  expect(stubFetch.mock.calls[0]![1]!.method).toBe("POST");
});

// BUG-2 regression: a 5xx Response from the RACE_TREND_DAILY_TRACK_DO push
// must surface via logFetch so the standard fetch_logs telemetry catches a
// silently unhealthy DO. Pre-fix, the Response was discarded and a 5xx looked
// identical to a 200.
it("fetch-results logs a non-2xx job entry when RACE_TREND_DAILY_TRACK_DO push returns 5xx", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, getRaceSource, insertRaceResultSnapshot, logFetch } =
    await import("./storage");
  const { fetchRacePage, parseRaceEntries, parseRaceResults, parseRaceEntryHorseNumbers } =
    await import("./keiba-go");
  vi.mocked(claimResultFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce(buildNarNarRaceSource());
  vi.mocked(fetchRacePage).mockResolvedValue("<html></html>");
  vi.mocked(parseRaceEntries).mockReturnValue([
    buildRaceEntry({ horseNumber: "1" }),
    buildRaceEntry({ horseNumber: "2" }),
  ]);
  vi.mocked(parseRaceEntryHorseNumbers).mockReturnValue(["1", "2"]);
  vi.mocked(parseRaceResults).mockReturnValue([
    buildRaceResult({ finishPosition: "1", horseNumber: "1" }),
    buildRaceResult({ finishPosition: "2", horseNumber: "2" }),
  ]);
  vi.mocked(insertRaceResultSnapshot).mockResolvedValue(2);
  const stubFetch = vi.fn(
    async (_url: string, _init?: RequestInit): Promise<Response> =>
      new Response("internal error", { status: 503 }),
  );
  const idFromName = vi.fn((name: string): string => name);
  const get = vi.fn((_id: string) => ({ fetch: stubFetch }));
  await handleJob(
    buildEnv({
      RACE_TREND_DAILY_TRACK_DO: { get, idFromName },
      REALTIME_TEST_NOW: "2026-05-12T07:00:00.000Z",
    }),
    { raceKey: "nar:2026:0512:55:01", type: "fetch-results" },
  );
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "race-trend-daily-track-do-push",
    "non-2xx",
    "nar:2026:0512:55:01",
    "HTTP 503",
  );
});

// BUG-2 regression: when the DO push throws (network failure / binding miss)
// the existing catch arm must still log via logFetch with the error message.
it("fetch-results logs an error entry when RACE_TREND_DAILY_TRACK_DO push throws", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, getRaceSource, insertRaceResultSnapshot, logFetch } =
    await import("./storage");
  const { fetchRacePage, parseRaceEntries, parseRaceResults, parseRaceEntryHorseNumbers } =
    await import("./keiba-go");
  vi.mocked(claimResultFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce(buildNarNarRaceSource());
  vi.mocked(fetchRacePage).mockResolvedValue("<html></html>");
  vi.mocked(parseRaceEntries).mockReturnValue([
    buildRaceEntry({ horseNumber: "1" }),
    buildRaceEntry({ horseNumber: "2" }),
  ]);
  vi.mocked(parseRaceEntryHorseNumbers).mockReturnValue(["1", "2"]);
  vi.mocked(parseRaceResults).mockReturnValue([
    buildRaceResult({ finishPosition: "1", horseNumber: "1" }),
    buildRaceResult({ finishPosition: "2", horseNumber: "2" }),
  ]);
  vi.mocked(insertRaceResultSnapshot).mockResolvedValue(2);
  const stubFetch = vi.fn(async (_url: string, _init?: RequestInit): Promise<Response> => {
    throw new Error("do unreachable");
  });
  const idFromName = vi.fn((name: string): string => name);
  const get = vi.fn((_id: string) => ({ fetch: stubFetch }));
  await handleJob(
    buildEnv({
      RACE_TREND_DAILY_TRACK_DO: { get, idFromName },
      REALTIME_TEST_NOW: "2026-05-12T07:00:00.000Z",
    }),
    { raceKey: "nar:2026:0512:55:01", type: "fetch-results" },
  );
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "race-trend-daily-track-do-push",
    "error",
    "nar:2026:0512:55:01",
    "do unreachable",
  );
});

// BUG-3 regression: a 5xx from the viewer trend cache bust must surface via
// logFetch so a long stretch of "1R-11R confirmed but 12R detail stale" can
// be diagnosed without trawling viewer logs.
it("fetch-results logs a trend-cache-bust error entry when the viewer bust returns 5xx", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, getRaceSource, insertRaceResultSnapshot, logFetch } =
    await import("./storage");
  const { fetchRacePage, parseRaceEntries, parseRaceResults, parseRaceEntryHorseNumbers } =
    await import("./keiba-go");
  vi.mocked(claimResultFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce(buildNarNarRaceSource());
  vi.mocked(fetchRacePage).mockResolvedValue("<html></html>");
  vi.mocked(parseRaceEntries).mockReturnValue([
    buildRaceEntry({ horseNumber: "1" }),
    buildRaceEntry({ horseNumber: "2" }),
  ]);
  vi.mocked(parseRaceEntryHorseNumbers).mockReturnValue(["1", "2"]);
  vi.mocked(parseRaceResults).mockReturnValue([
    buildRaceResult({ finishPosition: "1", horseNumber: "1" }),
    buildRaceResult({ finishPosition: "2", horseNumber: "2" }),
  ]);
  vi.mocked(insertRaceResultSnapshot).mockResolvedValue(2);
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response("nope", { status: 502 }));
  await handleJob(
    buildEnv({
      PC_KEIBA_VIEWER_INTERNAL_TOKEN: "secret-token",
      REALTIME_TEST_NOW: "2026-05-12T07:00:00.000Z",
      RUNNING_STYLE_CACHE_ORIGIN: "https://viewer.test",
    }),
    { raceKey: "nar:2026:0512:55:01", type: "fetch-results" },
  );
  expect(fetchSpy).toHaveBeenCalled();
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "trend-cache-bust",
    "error",
    "nar:2026:0512:55:01",
    "HTTP 502",
  );
});

// BUG-3 regression: a "skipped" outcome (viewer internal token missing) must
// still surface so a long no-token stretch is not silent.
it("fetch-results logs a trend-cache-bust skipped entry when no internal token is configured", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, getRaceSource, insertRaceResultSnapshot, logFetch } =
    await import("./storage");
  const { fetchRacePage, parseRaceEntries, parseRaceResults, parseRaceEntryHorseNumbers } =
    await import("./keiba-go");
  vi.mocked(claimResultFetch).mockResolvedValueOnce(true);
  vi.mocked(getRaceSource).mockResolvedValueOnce(buildNarNarRaceSource());
  vi.mocked(fetchRacePage).mockResolvedValue("<html></html>");
  vi.mocked(parseRaceEntries).mockReturnValue([
    buildRaceEntry({ horseNumber: "1" }),
    buildRaceEntry({ horseNumber: "2" }),
  ]);
  vi.mocked(parseRaceEntryHorseNumbers).mockReturnValue(["1", "2"]);
  vi.mocked(parseRaceResults).mockReturnValue([
    buildRaceResult({ finishPosition: "1", horseNumber: "1" }),
    buildRaceResult({ finishPosition: "2", horseNumber: "2" }),
  ]);
  vi.mocked(insertRaceResultSnapshot).mockResolvedValue(2);
  await handleJob(
    buildEnv({
      PC_KEIBA_VIEWER_INTERNAL_TOKEN: undefined,
      REALTIME_TEST_NOW: "2026-05-12T07:00:00.000Z",
    }),
    { raceKey: "nar:2026:0512:55:01", type: "fetch-results" },
  );
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "trend-cache-bust",
    "skipped",
    "nar:2026:0512:55:01",
    "PC_KEIBA_VIEWER_INTERNAL_TOKEN not configured",
  );
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
  vi.mocked(getRaceSource).mockResolvedValueOnce(buildNarNarRaceSource());
  vi.mocked(fetchRacePage).mockResolvedValue("<html></html>");
  vi.mocked(parseRaceEntries).mockReturnValue([
    buildRaceEntry({ horseNumber: "1" }),
    buildRaceEntry({ horseNumber: "2" }),
    buildRaceEntry({ horseNumber: "3" }),
  ]);
  vi.mocked(parseRaceEntryHorseNumbers).mockReturnValue(["1", "2", "3"]);
  vi.mocked(parseRaceResults).mockReturnValue([
    buildRaceResult({ finishPosition: "1", horseNumber: "1" }),
    buildRaceResult({ finishPosition: "2", horseNumber: "2" }),
  ]);
  vi.mocked(insertRaceResultSnapshot).mockResolvedValue(2);
  // 01:05 UTC = 10:05 JST = 5 min after the 10:00 JST race start, so the
  // NAR_RESULT_COMPLETION_BACKSTOP_MINUTES (10) window has not yet elapsed
  // and the partial-final isComplete=false semantics this test pins down
  // are preserved end-to-end.
  await handleJob(buildEnv({ REALTIME_TEST_NOW: "2026-05-12T01:05:00.000Z" }), {
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
    buildNarSchedulableRaceSource({ raceName: "Recovery" }),
  ]);
  // Now = 12:00 JST (= 03:00 UTC) so the JST minute is 00 and the recovery
  // discovery side-effect path fires this tick.
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_JOBS: { send, sendBatch: vi.fn(async () => {}) },
    REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z",
  });
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
  });
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
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([buildJraSchedulableRaceSource()]);
  vi.mocked(listPremiumRaceDataFetchCandidatesByDate).mockResolvedValue([
    { raceKey: "jra:2026:0512:08:01" },
  ]);
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
  });
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
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-11T20:00:00.000Z" });
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
  vi.mocked(getRaceSource).mockResolvedValue(buildJraNarRaceSource());
  vi.mocked(getPremiumRaceLink).mockResolvedValue(buildPremiumRaceLink());
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
      }),
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
    buildJraSchedulableRaceSource({ raceStartAtJst: "INVALID" }),
  ]);
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-12T03:30:00.000Z" });
  const count = await planRealtimeFetches(env, "20260512");
  expect(typeof count).toBe("number");
});

// Covers fetchAndStorePremiumRaceData with isPremiumRaceDataTarget JRA race + incomplete
// config: hits `if (!hasPremiumRaceFetchConfig(config)) return` arm (line 1713 source).
it("fetch-premium-race-data returns early when JRA race present but config has no origin", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource } = await import("./storage");
  vi.mocked(getRaceSource).mockResolvedValueOnce(buildJraNarRaceSource());
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
  vi.mocked(getRaceSource).mockResolvedValueOnce(buildJraNarRaceSource());
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce(buildPremiumRaceLink());
  vi.mocked(fetchPremiumHtmlAttempts).mockResolvedValueOnce([]);
  await expect(
    handleJob(
      buildEnv({
        PREMIUM_RACE_ORIGIN: "https://x.test",
        PREMIUM_RACE_PADDOCK_PATH_TEMPLATE: "/p/{sourceRaceId}",
      }),
      { raceKey: "jra:2026:0512:08:01", type: "fetch-premium-paddock" },
    ),
  ).rejects.toThrow("premium paddock fetch returned no attempts");
});

// Covers fetchAndStorePremiumPaddock JRA race + incomplete config: hits the
// `if (!hasPremiumRaceFetchConfig(config)) return` arm (line 1861 source).
it("fetch-premium-paddock returns early when config has no origin (incomplete)", async () => {
  const { handleJob } = await import("./worker");
  const { getRaceSource } = await import("./storage");
  vi.mocked(getRaceSource).mockResolvedValueOnce(buildJraNarRaceSource());
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
  vi.mocked(getRaceSource).mockResolvedValueOnce(buildJraNarRaceSource());
  vi.mocked(getPremiumRaceLink).mockResolvedValueOnce(buildPremiumRaceLink());
  // origin present (hasPremiumRaceFetchConfig=true), but no PADDOCK_PATH_TEMPLATE
  await handleJob(buildEnv({ PREMIUM_RACE_ORIGIN: "https://x.test" }), {
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
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([buildJraSchedulableRaceSource()]);
  vi.mocked(getPremiumPaddockFetchState).mockResolvedValue({
    lastFetchAt: "2026-05-12T03:39:30.000Z",
    lastQueuedAt: null,
    retryAfter: null,
    status: "ok",
  });
  const env = buildEnv({ PREMIUM_RACE_ORIGIN: "https://x.test" });
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
  vi.mocked(getRaceSource).mockResolvedValue(buildJraNarRaceSource());
  vi.mocked(getPremiumRaceLink).mockResolvedValue(buildPremiumRaceLink());
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
    }),
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
  vi.mocked(getRaceSource).mockResolvedValue(buildJraNarRaceSource());
  vi.mocked(getPremiumRaceLink).mockResolvedValue(buildPremiumRaceLink());
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
    }),
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
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-11T20:00:00.000Z" });
  const count = await planResultFetchesOnly(env, "20260512");
  expect(count).toBe(0);
});

it("planResultFetchesOnly returns 0 when there are no schedulable races", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([]);
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z" });
  const count = await planResultFetchesOnly(env, "20260512");
  expect(count).toBe(0);
});

it("planResultFetchesOnly enqueues fetch-results for a finished NAR race with no completion", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, markResultFetchQueued } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([
    buildNarSchedulableRaceSource({ raceName: "Finished" }),
  ]);
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_JOBS: { send, sendBatch: vi.fn(async () => {}) },
    REALTIME_TEST_NOW: "2026-05-12T02:00:00.000Z",
  });
  const count = await planResultFetchesOnly(env, "20260512");
  expect(count).toBe(1);
  expect(send).toHaveBeenCalledWith({ raceKey: "nar:2026:0512:55:01", type: "fetch-results" });
  expect(markResultFetchQueued).toHaveBeenCalled();
});

it("planResultFetchesOnly skips race that already has resultCompleteAt", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([
    buildNarSchedulableRaceSource({
      raceName: "DoneRace",
      resultCompleteAt: "2026-05-12T10:10:00+09:00",
    }),
  ]);
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_JOBS: { send, sendBatch: vi.fn(async () => {}) },
    REALTIME_TEST_NOW: "2026-05-12T02:00:00.000Z",
  });
  const count = await planResultFetchesOnly(env, "20260512");
  expect(count).toBe(0);
});

it("planResultFetchesOnly skips race that has not started yet", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([
    buildNarSchedulableRaceSource({
      raceName: "Future",
      raceStartAtJst: "2026-05-12T15:00:00+09:00",
    }),
  ]);
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_JOBS: { send, sendBatch: vi.fn(async () => {}) },
    REALTIME_TEST_NOW: "2026-05-12T02:00:00.000Z",
  });
  const count = await planResultFetchesOnly(env, "20260512");
  expect(count).toBe(0);
});

it("planResultFetchesOnly skips race when lastResultQueuedAt is set", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([
    buildNarSchedulableRaceSource({
      lastResultQueuedAt: "2026-05-12T10:55:00+09:00",
      raceName: "QueuedAlready",
    }),
  ]);
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_JOBS: { send, sendBatch: vi.fn(async () => {}) },
    REALTIME_TEST_NOW: "2026-05-12T02:00:00.000Z",
  });
  const count = await planResultFetchesOnly(env, "20260512");
  expect(count).toBe(0);
});

it("planResultFetchesOnly skips race when resultFetchLockUntil is still in the future", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([
    buildNarSchedulableRaceSource({
      raceName: "Locked",
      resultFetchLockUntil: "2026-05-12T20:00:00+09:00",
    }),
  ]);
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_JOBS: { send, sendBatch: vi.fn(async () => {}) },
    REALTIME_TEST_NOW: "2026-05-12T02:00:00.000Z",
  });
  const count = await planResultFetchesOnly(env, "20260512");
  expect(count).toBe(0);
});

it("planResultFetchesOnly skips race when lastResultFetchAt is within RESULT_FETCH_INTERVAL_MINUTES", async () => {
  const { planResultFetchesOnly } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([
    buildNarSchedulableRaceSource({
      // Now = 02:00 UTC = 11:00 JST. lastResultFetchAt = 10:59 JST is only 1
      // minute ago, which is less than RESULT_FETCH_INTERVAL_MINUTES (2) so
      // the race must be skipped this tick.
      lastResultFetchAt: "2026-05-12T10:59:00+09:00",
      raceName: "Throttle",
    }),
  ]);
  const send = vi.fn(async () => {});
  const env = buildEnv({
    REALTIME_JOBS: { send, sendBatch: vi.fn(async () => {}) },
    REALTIME_TEST_NOW: "2026-05-12T02:00:00.000Z",
  });
  const count = await planResultFetchesOnly(env, "20260512");
  expect(count).toBe(0);
});

// shouldApplyNarResultCompletionBackstop: every NAR backstop condition is met
// (all available result rows persisted, expectedHorseCount over-counted by 1
// due to a parser miss on a 取消 row, and 10 minutes have elapsed after race
// start), so the backstop fires to force isComplete=true.
it("shouldApplyNarResultCompletionBackstop returns true for NAR when every condition holds", async () => {
  const { shouldApplyNarResultCompletionBackstop } = await import("./worker");
  const result = shouldApplyNarResultCompletionBackstop({
    expectedHorseCount: 5,
    inserted: 4,
    minutesAfterRaceStart: 11,
    resultCount: 4,
    source: "nar",
  });
  expect(result).toBe(true);
});

// JRA results trust the entry-derived expectedHorseCount because JRA HTML
// exposes 取消 reliably, so the backstop must never fire for JRA even when the
// same numeric pattern holds.
it("shouldApplyNarResultCompletionBackstop returns false for JRA source", async () => {
  const { shouldApplyNarResultCompletionBackstop } = await import("./worker");
  const result = shouldApplyNarResultCompletionBackstop({
    expectedHorseCount: 5,
    inserted: 4,
    minutesAfterRaceStart: 11,
    resultCount: 4,
    source: "jra",
  });
  expect(result).toBe(false);
});

// No result rows at all means the result HTML is still empty, so the existing
// "empty rows" error path should handle it; backstop must not fire here.
it("shouldApplyNarResultCompletionBackstop returns false when resultCount is 0", async () => {
  const { shouldApplyNarResultCompletionBackstop } = await import("./worker");
  const result = shouldApplyNarResultCompletionBackstop({
    expectedHorseCount: 5,
    inserted: 0,
    minutesAfterRaceStart: 11,
    resultCount: 0,
    source: "nar",
  });
  expect(result).toBe(false);
});

// inserted < resultCount means D1 write itself dropped rows (deadlock, conflict,
// etc.), which is a real data loss case that should not be papered over by the
// backstop.
it("shouldApplyNarResultCompletionBackstop returns false when inserted is below resultCount", async () => {
  const { shouldApplyNarResultCompletionBackstop } = await import("./worker");
  const result = shouldApplyNarResultCompletionBackstop({
    expectedHorseCount: 5,
    inserted: 3,
    minutesAfterRaceStart: 11,
    resultCount: 4,
    source: "nar",
  });
  expect(result).toBe(false);
});

// inserted >= expectedHorseCount means the base completion already fires, so
// the backstop is redundant. Returning false keeps the boolean clean (the
// caller OR-combines it with baseComplete anyway).
it("shouldApplyNarResultCompletionBackstop returns false when inserted reaches expectedHorseCount", async () => {
  const { shouldApplyNarResultCompletionBackstop } = await import("./worker");
  const result = shouldApplyNarResultCompletionBackstop({
    expectedHorseCount: 4,
    inserted: 4,
    minutesAfterRaceStart: 11,
    resultCount: 4,
    source: "nar",
  });
  expect(result).toBe(false);
});

// minutesAfterRaceStart === null means raceStartAtJst could not be parsed, so
// we cannot make any time-based judgement; backstop must abstain.
it("shouldApplyNarResultCompletionBackstop returns false when minutesAfterRaceStart is null", async () => {
  const { shouldApplyNarResultCompletionBackstop } = await import("./worker");
  const result = shouldApplyNarResultCompletionBackstop({
    expectedHorseCount: 5,
    inserted: 4,
    minutesAfterRaceStart: null,
    resultCount: 4,
    source: "nar",
  });
  expect(result).toBe(false);
});

// 5 minutes after race start is below the 10-minute threshold, so a late row
// might still arrive and we must not prematurely declare the race complete.
it("shouldApplyNarResultCompletionBackstop returns false when minutesAfterRaceStart is below threshold", async () => {
  const { shouldApplyNarResultCompletionBackstop } = await import("./worker");
  const result = shouldApplyNarResultCompletionBackstop({
    expectedHorseCount: 5,
    inserted: 4,
    minutesAfterRaceStart: 5,
    resultCount: 4,
    source: "nar",
  });
  expect(result).toBe(false);
});

// Integration: NAR race where parser missed a 取消 row so expectedHorseCount=5
// over-counts the real 4 runners, every available result row is in D1, and
// 11 minutes have passed since race start. The backstop must force the
// completeResultFetch call with isComplete=true.
it("fetch-results forces NAR isComplete=true via backstop after 11min when inserted equals resultCount below expectedHorseCount", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, getRaceSource, insertRaceResultSnapshot, completeResultFetch } =
    await import("./storage");
  const {
    fetchRacePage,
    parseRaceEntries,
    parseRaceResults,
    parseRaceEntryHorseNumbers,
    parseRaceResultExcludedHorseNumbers,
  } = await import("./keiba-go");
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
    raceName: "Backstop",
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
    { horseName: "h", horseNumber: "4", jockeyName: "j", status: null },
    { horseName: "h", horseNumber: "5", jockeyName: "j", status: null },
  ] as never);
  vi.mocked(parseRaceEntryHorseNumbers).mockReturnValue(["1", "2", "3", "4", "5"]);
  vi.mocked(parseRaceResultExcludedHorseNumbers).mockReturnValue([]);
  vi.mocked(parseRaceResults).mockReturnValue([
    { finishPosition: "1", horseName: null, horseNumber: "1", time: null },
    { finishPosition: "2", horseName: null, horseNumber: "2", time: null },
    { finishPosition: "3", horseName: null, horseNumber: "3", time: null },
    { finishPosition: "4", horseName: null, horseNumber: "4", time: null },
  ] as never);
  vi.mocked(insertRaceResultSnapshot).mockResolvedValue(4);
  await handleJob(buildEnv({ REALTIME_TEST_NOW: "2026-05-12T01:11:00.000Z" } as never), {
    raceKey: "nar:2026:0512:55:01",
    type: "fetch-results",
  });
  expect(completeResultFetch).toHaveBeenCalledWith(
    expect.anything(),
    "nar:2026:0512:55:01",
    expect.any(String),
    {
      expectedHorseCount: 5,
      isComplete: true,
      savedHorseCount: 4,
    },
  );
});

// Integration: same NAR race shape as above but only 5 minutes elapsed since
// race start, so the backstop must NOT fire and completeResultFetch must be
// called with isComplete=false (existing behaviour).
it("fetch-results keeps NAR isComplete=false when backstop window not yet elapsed", async () => {
  const { handleJob } = await import("./worker");
  const { claimResultFetch, getRaceSource, insertRaceResultSnapshot, completeResultFetch } =
    await import("./storage");
  const {
    fetchRacePage,
    parseRaceEntries,
    parseRaceResults,
    parseRaceEntryHorseNumbers,
    parseRaceResultExcludedHorseNumbers,
  } = await import("./keiba-go");
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
    raceName: "BackstopEarly",
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
    { horseName: "h", horseNumber: "4", jockeyName: "j", status: null },
    { horseName: "h", horseNumber: "5", jockeyName: "j", status: null },
  ] as never);
  vi.mocked(parseRaceEntryHorseNumbers).mockReturnValue(["1", "2", "3", "4", "5"]);
  vi.mocked(parseRaceResultExcludedHorseNumbers).mockReturnValue([]);
  vi.mocked(parseRaceResults).mockReturnValue([
    { finishPosition: "1", horseName: null, horseNumber: "1", time: null },
    { finishPosition: "2", horseName: null, horseNumber: "2", time: null },
    { finishPosition: "3", horseName: null, horseNumber: "3", time: null },
    { finishPosition: "4", horseName: null, horseNumber: "4", time: null },
  ] as never);
  vi.mocked(insertRaceResultSnapshot).mockResolvedValue(4);
  await handleJob(buildEnv({ REALTIME_TEST_NOW: "2026-05-12T01:05:00.000Z" } as never), {
    raceKey: "nar:2026:0512:55:01",
    type: "fetch-results",
  });
  expect(completeResultFetch).toHaveBeenCalledWith(
    expect.anything(),
    "nar:2026:0512:55:01",
    expect.any(String),
    {
      expectedHorseCount: 5,
      isComplete: false,
      savedHorseCount: 4,
    },
  );
});
