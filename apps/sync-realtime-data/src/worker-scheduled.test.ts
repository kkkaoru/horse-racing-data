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
  DAILY_FEATURE_BUILD_CRON: "0 19 * * *",
  runDailyFeatureBuildForEnv: vi.fn(async () => ({})),
  listDailyRaceEntriesForRace: vi.fn(async () => []),
  probeDailyRaceEntriesFreshness: vi.fn(async () => ({ latestUpdatedAt: null, rowCount: 1 })),
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
    new Date(
      Date.parse(`${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}T00:00:00Z`) +
        days * 86_400_000,
    )
      .toISOString()
      .slice(0, 10)
      .replace(/-/g, ""),
  ),
  planRunningStylePredictionsForDate: vi.fn(async () => ({})),
  refreshViewerRunningStyleCachesForDate: vi.fn(async () => ({})),
  refreshViewerRunningStyleCacheForRace: vi.fn(async () => false),
  runRunningStyleCronTick: vi.fn(async () => ({})),
}));
vi.mock("./running-style-feature-materialize", () => ({
  materializeRunningStyleFeatureParquetsForDate: vi.fn(async () => ({
    date: "20260513",
    materialized: 0,
    scanned: 0,
    skipped: 0,
  })),
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

it("scheduled logs error when runRunningStyleCronTick rejects", async () => {
  const { default: worker } = await import("./worker");
  const { runRunningStyleCronTick } = await import("./running-style-cron");
  const { logFetch } = await import("./storage");
  vi.mocked(runRunningStyleCronTick).mockRejectedValueOnce(new Error("inference boom"));
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "*/10 * * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-running-style-predictions",
    "error",
    null,
    "inference boom",
  );
});

it("scheduled triggers logRunningStylePlanResult for the inference cron", async () => {
  const { default: worker } = await import("./worker");
  const { runRunningStyleCronTick } = await import("./running-style-cron");
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "*/10 * * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(runRunningStyleCronTick).toHaveBeenCalled();
});

it("scheduled prewarm path stringifies non-Error rejections from postgres", async () => {
  const { default: worker } = await import("./worker");
  const { fetchJraRacesByDate } = await import("./postgres");
  const { logFetch } = await import("./storage");
  vi.mocked(fetchJraRacesByDate).mockImplementationOnce(async () => {
    throw "raw-string-error" as never;
  });
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "0 12 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "discover-urls",
    "error",
    null,
    "raw-string-error",
    undefined,
  );
});

it("scheduled prewarm path logs discover-urls error when upsertDiscoveredUrls throws", async () => {
  const { default: worker } = await import("./worker");
  const { fetchJraRacesByDate } = await import("./postgres");
  const { logFetch } = await import("./storage");
  vi.mocked(fetchJraRacesByDate).mockRejectedValueOnce(new Error("postgres boom"));
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "0 12 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "discover-urls",
    "error",
    null,
    "postgres boom",
    undefined,
  );
});

it("scheduled triggers prewarm path for the prewarm cron", async () => {
  const { default: worker } = await import("./worker");
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "0 12 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
});

it("scheduled prewarm path logs materialize-running-style-features ok on success", async () => {
  const { default: worker } = await import("./worker");
  const { materializeRunningStyleFeatureParquetsForDate } =
    await import("./running-style-feature-materialize");
  const { logFetch } = await import("./storage");
  vi.mocked(materializeRunningStyleFeatureParquetsForDate).mockResolvedValueOnce({
    date: "20260513",
    materialized: 3,
    scanned: 3,
    skipped: 0,
  });
  const env = buildEnv();
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "0 12 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    env,
    ctx,
  );
  await flushWaits(waits);
  expect(materializeRunningStyleFeatureParquetsForDate).toHaveBeenCalledWith(env, "20260513");
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "materialize-running-style-features",
    "ok",
    null,
    JSON.stringify({ date: "20260513", materialized: 3, scanned: 3, skipped: 0, mode: "prewarm" }),
  );
});

it("scheduled prewarm path logs materialize-running-style-features error when a race fails", async () => {
  const { default: worker } = await import("./worker");
  const { materializeRunningStyleFeatureParquetsForDate } =
    await import("./running-style-feature-materialize");
  const { logFetch } = await import("./storage");
  vi.mocked(materializeRunningStyleFeatureParquetsForDate).mockResolvedValueOnce({
    date: "20260513",
    materialized: 0,
    scanned: 1,
    skipped: 1,
    materializeError: "boom",
  });
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "0 12 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "materialize-running-style-features",
    "error",
    null,
    JSON.stringify({
      date: "20260513",
      materialized: 0,
      scanned: 1,
      skipped: 1,
      materializeError: "boom",
      mode: "prewarm",
    }),
  );
});

it("scheduled prewarm path skips materialize when D1 daily_race_entries is empty", async () => {
  const { default: worker } = await import("./worker");
  const { materializeRunningStyleFeatureParquetsForDate } =
    await import("./running-style-feature-materialize");
  const { probeDailyRaceEntriesFreshness } = await import("./daily-feature-build");
  const { logFetch } = await import("./storage");
  vi.mocked(probeDailyRaceEntriesFreshness).mockResolvedValueOnce({
    latestUpdatedAt: null,
    rowCount: 0,
  });
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "0 12 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(materializeRunningStyleFeatureParquetsForDate).not.toHaveBeenCalled();
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "materialize-running-style-features",
    "skipped",
    null,
    JSON.stringify({
      date: "20260513",
      materialized: 0,
      materializeError:
        "build-daily-features produced 0 D1 rows for 20260513; deferring materialize to next cron tick",
      scanned: 0,
      skipped: 0,
      mode: "prewarm",
    }),
  );
});

it("scheduled triggers logWin5CronResult for the WIN5 cron", async () => {
  const { default: worker } = await import("./worker");
  const { logWin5CronResult } = await import("./win5-cron");
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "30 12 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(logWin5CronResult).toHaveBeenCalledTimes(1);
});

it("scheduled triggers self-schedule planner during polling window", async () => {
  const { default: worker } = await import("./worker");
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "* 1-12 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv({ REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z" } as never),
    ctx,
  );
  await flushWaits(waits);
  expect(waits.length).toBeGreaterThanOrEqual(2);
});

it("scheduled defaults scheduledAt to new Date() when controller.scheduledTime is not a number", async () => {
  const { default: worker } = await import("./worker");
  const { logWin5CronResult } = await import("./win5-cron");
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "30 12 * * *",
      scheduledTime: undefined as unknown as number,
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(logWin5CronResult).toHaveBeenCalled();
});

it("scheduled defaults to handleJob via getCronJob for unknown cron", async () => {
  const { default: worker } = await import("./worker");
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "* 1-12 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
});

it("scheduled multi-day-prep cron fans out feature build to next 1-3 JST dates", async () => {
  const { default: worker } = await import("./worker");
  const { runDailyFeatureBuildForEnv } = await import("./daily-feature-build");
  vi.mocked(runDailyFeatureBuildForEnv).mockResolvedValue({} as never);
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "5 11 * * *",
      scheduledTime: Date.parse("2026-05-12T11:05:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  const calls = vi.mocked(runDailyFeatureBuildForEnv).mock.calls;
  const fromDates = calls.map(([, options]) => options?.fromDate);
  expect(fromDates).toStrictEqual(["20260513", "20260514", "20260515"]);
});

it("scheduled multi-day-prep cron logs build-daily-features error when runDailyFeatureBuildForEnv throws", async () => {
  const { default: worker } = await import("./worker");
  const { runDailyFeatureBuildForEnv } = await import("./daily-feature-build");
  const { logFetch } = await import("./storage");
  vi.mocked(runDailyFeatureBuildForEnv).mockRejectedValueOnce(new Error("build boom"));
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "5 11 * * *",
      scheduledTime: Date.parse("2026-05-12T11:05:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "build-daily-features",
    "error",
    null,
    "build boom",
    undefined,
  );
});

it("scheduled multi-day-prep cron stringifies non-Error rejection from runDailyFeatureBuildForEnv", async () => {
  const { default: worker } = await import("./worker");
  const { runDailyFeatureBuildForEnv } = await import("./daily-feature-build");
  const { logFetch } = await import("./storage");
  vi.mocked(runDailyFeatureBuildForEnv).mockRejectedValueOnce("raw-string-build-error");
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "5 11 * * *",
      scheduledTime: Date.parse("2026-05-12T11:05:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "build-daily-features",
    "error",
    null,
    "raw-string-build-error",
    undefined,
  );
});

it("scheduled today-backfill cron builds features for today only", async () => {
  const { default: worker } = await import("./worker");
  const { runDailyFeatureBuildForEnv } = await import("./daily-feature-build");
  vi.mocked(runDailyFeatureBuildForEnv).mockResolvedValue({} as never);
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "10 0 * * *",
      scheduledTime: Date.parse("2026-05-12T00:10:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  const calls = vi.mocked(runDailyFeatureBuildForEnv).mock.calls;
  const fromDates = calls.map(([, options]) => options?.fromDate);
  expect(fromDates).toStrictEqual(["20260512"]);
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
  );
  expect(retry).toHaveBeenCalledTimes(1);
  expect(ack).not.toHaveBeenCalled();
});

it("scheduled enqueues a build-daily-features job for the daily-feature-build cron", async () => {
  const { default: worker } = await import("./worker");
  const env = buildEnv();
  const sendSpy = vi.spyOn(env.REALTIME_JOBS, "send");
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "0 19 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    env,
    ctx,
  );
  await flushWaits(waits);
  expect(sendSpy).toHaveBeenCalledWith({
    date: "20260512",
    sourceScope: "all",
    type: "build-daily-features",
  });
});

it("scheduled logs queued status when daily-feature-build cron enqueue succeeds", async () => {
  const { default: worker } = await import("./worker");
  const { logFetch } = await import("./storage");
  const env = buildEnv();
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "0 19 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    env,
    ctx,
  );
  await flushWaits(waits);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "build-daily-features",
    "queued",
    null,
    JSON.stringify({ date: "20260512", sourceScope: "all" }),
  );
});

it("scheduled logs error when daily-feature-build cron enqueue rejects", async () => {
  const { default: worker } = await import("./worker");
  const { logFetch } = await import("./storage");
  const env = buildEnv();
  const sendSpy = vi.spyOn(env.REALTIME_JOBS, "send");
  sendSpy.mockRejectedValueOnce(new Error("queue boom"));
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "0 19 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    env,
    ctx,
  );
  await flushWaits(waits);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "build-daily-features",
    "error",
    null,
    "queue boom",
    undefined,
  );
});

it("scheduled logs error when prewarm running-style cron rejects", async () => {
  const { default: worker } = await import("./worker");
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { logFetch } = await import("./storage");
  vi.mocked(planRunningStylePredictionsForDate).mockRejectedValueOnce(new Error("prewarm boom"));
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "0 12 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-running-style-predictions",
    "error",
    null,
    "prewarm boom",
    undefined,
  );
});

it("scheduled logs error when D1 retention cron rejects", async () => {
  const { default: worker } = await import("./worker");
  const { runD1Retention, logFetch } = await import("./storage");
  vi.mocked(runD1Retention).mockRejectedValueOnce(new Error("retention boom"));
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "30 18 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "d1-retention",
    "error",
    null,
    "retention boom",
    undefined,
  );
});

it("scheduled triggers runD1Retention for the D1 retention cron", async () => {
  const { default: worker } = await import("./worker");
  const { runD1Retention } = await import("./storage");
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "30 18 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(runD1Retention).toHaveBeenCalledTimes(1);
});

it("scheduled result-poll cron logs plan-result-fetches ok", async () => {
  const { default: worker } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, logFetch } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([] as never);
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "*/2 0-13 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv({ REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z" } as never),
    ctx,
  );
  await flushWaits(waits);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-result-fetches",
    "ok",
    null,
    "0 jobs queued",
  );
});

it("scheduled result-poll cron skips running-style inference path", async () => {
  const { default: worker } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  const { runRunningStyleCronTick } = await import("./running-style-cron");
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValueOnce([] as never);
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "*/2 0-13 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv({ REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z" } as never),
    ctx,
  );
  await flushWaits(waits);
  expect(runRunningStyleCronTick).not.toHaveBeenCalled();
});

it("scheduled result-poll cron logs plan-result-fetches error when planner rejects", async () => {
  const { default: worker } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, logFetch } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockRejectedValueOnce(new Error("planner boom"));
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "*/2 0-13 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv({ REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z" } as never),
    ctx,
  );
  await flushWaits(waits);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-result-fetches",
    "error",
    null,
    "planner boom",
    undefined,
  );
});

it("scheduled result-poll cron also logs plan-premium-paddock ok alongside plan-result-fetches", async () => {
  const { default: worker } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, logFetch } = await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockReset();
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([] as never);
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "*/2 0-13 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv({
      PREMIUM_RACE_ORIGIN: "https://x.test",
      REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z",
    } as never),
    ctx,
  );
  await flushWaits(waits);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-premium-paddock",
    "ok",
    null,
    "0 jobs queued",
  );
});

it("scheduled result-poll cron logs plan-premium-paddock error when paddock planner rejects", async () => {
  const { default: worker } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, markPremiumPaddockQueued, logFetch } =
    await import("./storage");
  vi.mocked(listSchedulableRaceSourcesByDate).mockReset();
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([] as never);
  vi.mocked(markPremiumPaddockQueued).mockReset();
  vi.mocked(markPremiumPaddockQueued).mockRejectedValueOnce(new Error("paddock planner boom"));
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "*/2 0-13 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv({
      PREMIUM_RACE_ORIGIN: "https://x.test",
      REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z",
    } as never),
    ctx,
  );
  await flushWaits(waits);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-premium-paddock",
    "error",
    null,
    "paddock planner boom",
    undefined,
  );
});
it("queue retries with long delay when handleJob throws a D1 overload error", async () => {
  const { default: worker } = await import("./worker");
  const { runDailyFeatureBuildForEnv } = await import("./daily-feature-build");
  vi.mocked(runDailyFeatureBuildForEnv).mockRejectedValueOnce(
    new Error("D1_ERROR: D1 DB is overloaded. Please try again later."),
  );
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
  );
  expect(retry).toHaveBeenCalledTimes(1);
  const callArg = retry.mock.calls[0]?.[0] as { delaySeconds?: number } | undefined;
  expect(callArg?.delaySeconds !== undefined).toBe(true);
  expect((callArg?.delaySeconds ?? 0) >= 60).toBe(true);
  expect((callArg?.delaySeconds ?? 0) < 180).toBe(true);
});

it("queue retries with standard delay when handleJob throws a non-overload error", async () => {
  const { default: worker } = await import("./worker");
  const { runDailyFeatureBuildForEnv } = await import("./daily-feature-build");
  vi.mocked(runDailyFeatureBuildForEnv).mockRejectedValueOnce(new Error("unrelated boom"));
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
  );
  expect(retry).toHaveBeenCalledWith({ delaySeconds: 60 });
});

it("scheduled triggers the weight watchdog for the every-minute cron", async () => {
  const { default: worker } = await import("./worker");
  const { logFetch } = await import("./storage");
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "* * * * *",
      scheduledTime: Date.parse("2026-06-07T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "weight-watchdog",
    "ok",
    null,
    "no stale weight races",
  );
});
