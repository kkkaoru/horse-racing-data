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
  addDaysToYYYYMMDDInJst: vi.fn((d: string, days: number) =>
    new Date(Date.parse(`${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}T00:00:00Z`) + days * 86_400_000)
      .toISOString()
      .slice(0, 10)
      .replace(/-/g, ""),
  ),
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
    REALTIME_DB: buildDb(),
    REALTIME_JOBS: { send: vi.fn(async () => {}), sendBatch: vi.fn(async () => {}) },
    ...overrides,
  } as unknown as Env;
};

const buildCtx = () => {
  const waits: Promise<unknown>[] = [];
  const ctx = {
    passThroughOnException: () => {},
    waitUntil: (promise: Promise<unknown>): void => {
      waits.push(promise);
    },
  } as unknown as ExecutionContext;
  return { ctx, waits };
};

const flushWaits = async (waits: Promise<unknown>[]): Promise<void> => {
  await Promise.all(waits.map((promise) => promise.catch(() => undefined)));
};

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("scheduled triggers logRunningStylePlanResult for the inference cron", async () => {
  const { default: worker } = await import("./worker");
  const { runRunningStyleCronTick } = await import("./running-style-cron");
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    { cron: "*/10 * * * *", scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"), noRetry: () => {} } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(runRunningStyleCronTick).toHaveBeenCalled();
});

it("scheduled triggers prewarm path for the prewarm cron", async () => {
  const { default: worker } = await import("./worker");
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    { cron: "0 12 * * *", scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"), noRetry: () => {} } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
});

it("scheduled triggers logWin5CronResult for the WIN5 cron", async () => {
  const { default: worker } = await import("./worker");
  const { logWin5CronResult } = await import("./win5-cron");
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    { cron: "30 12 * * *", scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"), noRetry: () => {} } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(logWin5CronResult).toHaveBeenCalledTimes(1);
});

it("scheduled defaults to handleJob via getCronJob for unknown cron", async () => {
  const { default: worker } = await import("./worker");
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    { cron: "* 1-12 * * *", scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"), noRetry: () => {} } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
});

it("queue acks a message after successful handleJob", async () => {
  const { default: worker } = await import("./worker");
  const { runDailyFeatureBuildForEnv } = await import("./daily-feature-build");
  vi.mocked(runDailyFeatureBuildForEnv).mockResolvedValueOnce({} as never);
  const ack = vi.fn();
  const retry = vi.fn();
  const message = {
    ack,
    body: { date: "20260512", type: "build-daily-features" } satisfies Job,
    retry,
  };
  await worker.queue(
    { messages: [message], queue: "q", retryAll: () => {}, ackAll: () => {} } as never,
    buildEnv(),
    {} as never,
  );
  expect(ack).toHaveBeenCalledTimes(1);
  expect(retry).not.toHaveBeenCalled();
});

it("queue retries a failing non-fetch-odds message", async () => {
  const { default: worker } = await import("./worker");
  const { runDailyFeatureBuildForEnv } = await import("./daily-feature-build");
  vi.mocked(runDailyFeatureBuildForEnv).mockRejectedValueOnce(new Error("boom"));
  const ack = vi.fn();
  const retry = vi.fn();
  const message = {
    ack,
    body: { date: "20260512", type: "build-daily-features" } satisfies Job,
    retry,
  };
  await worker.queue(
    { messages: [message], queue: "q", retryAll: () => {}, ackAll: () => {} } as never,
    buildEnv(),
    {} as never,
  );
  expect(retry).toHaveBeenCalledTimes(1);
  expect(ack).not.toHaveBeenCalled();
});

it("queue acks (instead of retrying) a failing fetch-odds message", async () => {
  const { default: worker } = await import("./worker");
  const { claimOddsFetch } = await import("./storage");
  vi.mocked(claimOddsFetch).mockRejectedValueOnce(new Error("boom"));
  const ack = vi.fn();
  const retry = vi.fn();
  const message = {
    ack,
    body: { raceKey: "k", type: "fetch-odds" } satisfies Job,
    retry,
  };
  await worker.queue(
    { messages: [message], queue: "q", retryAll: () => {}, ackAll: () => {} } as never,
    buildEnv(),
    {} as never,
  );
  expect(ack).toHaveBeenCalledTimes(1);
  expect(retry).not.toHaveBeenCalled();
});

it("scheduled triggers runDailyFeatureBuildForEnv for the daily-feature-build cron", async () => {
  const { default: worker } = await import("./worker");
  const { runDailyFeatureBuildForEnv } = await import("./daily-feature-build");
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    { cron: "0 19 * * *", scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"), noRetry: () => {} } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(runDailyFeatureBuildForEnv).toHaveBeenCalledTimes(1);
});

it("scheduled triggers runD1Retention for the D1 retention cron", async () => {
  const { default: worker } = await import("./worker");
  const { runD1Retention } = await import("./storage");
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    { cron: "30 18 * * *", scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"), noRetry: () => {} } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(runD1Retention).toHaveBeenCalledTimes(1);
});
