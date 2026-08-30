// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { Env, Job } from "./types";

const queueSendResponse = (): QueueSendResponse => ({
  metadata: { metrics: { backlogCount: 0, backlogBytes: 0 } },
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
  resolveRunningStyleCronDates: vi.fn(() => ["20260512"]),
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
  const run = vi.fn(async () => ({ meta: { changes: 1 } }));
  const bind = vi.fn(() => ({ all, first, run, bind: vi.fn() }));
  const prepare = vi.fn(() => ({ all, bind, first, run }));
  const batch = vi.fn(async () => []);
  const exec = vi.fn(async () => ({}));
  return { batch, exec, prepare } as unknown as D1Database;
};

const buildAtomicRecoveryDb = (): D1Database => {
  const state = { ownerToken: "", owned: false };
  const prepare = vi.fn((sql: string) => ({
    bind: vi.fn((...values: unknown[]) => ({
      run: vi.fn(async () => {
        if (sql.includes("insert into realtime_plan_recovery_claims")) {
          if (state.owned) return { meta: { changes: 0 } };
          state.owned = true;
          state.ownerToken = String(values[1]);
          return { meta: { changes: 1 } };
        }
        if (
          sql.includes("delete from realtime_plan_recovery_claims") &&
          state.ownerToken === String(values[1])
        ) {
          state.owned = false;
          state.ownerToken = "";
          return { meta: { changes: 1 } };
        }
        return { meta: { changes: 0 } };
      }),
    })),
    first: vi.fn(async () => null),
  }));
  return { prepare } as unknown as D1Database;
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
      cron: "*/10 0-14 * * *",
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
      cron: "*/10 0-14 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(runRunningStyleCronTick).toHaveBeenCalled();
});

it("scheduled inference cron still plans races when batch feature warm is incomplete", async () => {
  const { default: worker } = await import("./worker");
  const { runRunningStyleCronTick } = await import("./running-style-cron");
  const { materializeRunningStyleFeatureParquetsForDate } =
    await import("./running-style-feature-materialize");
  vi.mocked(materializeRunningStyleFeatureParquetsForDate).mockResolvedValueOnce({
    date: "20260512",
    materializeError: "feature warm failed",
    materialized: 0,
    scanned: 1,
    skipped: 1,
  });
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "*/10 0-14 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(runRunningStyleCronTick).toHaveBeenCalled();
});

it("scheduled inference cron still plans races when batch feature warm rejects", async () => {
  const { default: worker } = await import("./worker");
  const { runRunningStyleCronTick } = await import("./running-style-cron");
  const { materializeRunningStyleFeatureParquetsForDate } =
    await import("./running-style-feature-materialize");
  const { logFetch } = await import("./storage");
  vi.mocked(materializeRunningStyleFeatureParquetsForDate).mockRejectedValueOnce(
    new Error("Catalog unavailable"),
  );
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "*/10 0-14 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);
  expect(runRunningStyleCronTick).toHaveBeenCalled();
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "materialize-running-style-features",
    "error",
    null,
    JSON.stringify({
      date: "20260512",
      materializeError: "Catalog unavailable",
      mode: "inference-cron",
    }),
  );
});

it("scheduled inference cron still plans when materialize result logging rejects", async () => {
  const { default: worker } = await import("./worker");
  const { runRunningStyleCronTick } = await import("./running-style-cron");
  const { logFetch } = await import("./storage");
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  vi.mocked(logFetch).mockRejectedValueOnce(new Error("D1 log unavailable"));
  const { ctx, waits } = buildCtx();

  await worker.scheduled(
    {
      cron: "*/10 0-14 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    buildEnv(),
    ctx,
  );
  await flushWaits(waits);

  expect(runRunningStyleCronTick).toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    "[sync-realtime-data] failed to log running-style materialize result",
    new Error("D1 log unavailable"),
  );
  errorSpy.mockRestore();
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

it("scheduled prewarm path fails closed when Catalog materialize rejects", async () => {
  const { default: worker } = await import("./worker");
  const { materializeRunningStyleFeatureParquetsForDate } =
    await import("./running-style-feature-materialize");
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { logFetch } = await import("./storage");
  vi.mocked(materializeRunningStyleFeatureParquetsForDate).mockRejectedValueOnce(
    new Error("Catalog unavailable"),
  );
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
  expect(planRunningStylePredictionsForDate).not.toHaveBeenCalled();
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-running-style-predictions",
    "error",
    null,
    "Catalog unavailable",
    undefined,
  );
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
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
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
  expect(planRunningStylePredictionsForDate).not.toHaveBeenCalled();
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
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-running-style-predictions",
    "error",
    null,
    "boom",
    undefined,
  );
});

it("scheduled prewarm path materializes from Catalog without D1 readiness", async () => {
  const { default: worker } = await import("./worker");
  const { runDailyFeatureBuildForEnv } = await import("./daily-feature-build");
  const { materializeRunningStyleFeatureParquetsForDate } =
    await import("./running-style-feature-materialize");
  const { logFetch } = await import("./storage");
  vi.mocked(materializeRunningStyleFeatureParquetsForDate).mockResolvedValueOnce({
    date: "20260513",
    materialized: 2,
    scanned: 2,
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
  expect(runDailyFeatureBuildForEnv).not.toHaveBeenCalled();
  expect(materializeRunningStyleFeatureParquetsForDate).toHaveBeenCalledWith(env, "20260513");
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "materialize-running-style-features",
    "ok",
    null,
    JSON.stringify({
      date: "20260513",
      materialized: 2,
      scanned: 2,
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

it("scheduled waits for the normal realtime planner and does not self-enqueue after success", async () => {
  const { default: worker } = await import("./worker");
  const { logFetch } = await import("./storage");
  const order: string[] = [];
  vi.mocked(logFetch).mockImplementation(async (_db, jobType, status) => {
    if (jobType === "plan-realtime-fetches" && status === "ok") {
      order.push("normal-ok");
    }
  });
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z" } as never);
  const send = vi.spyOn(env.REALTIME_JOBS, "send");
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "* 1-12 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    env,
    ctx,
  );
  expect(waits).toHaveLength(1);
  await expect(waits[0]).resolves.toBeUndefined();
  expect(order).toStrictEqual(["normal-ok"]);
  expect(send).not.toHaveBeenCalled();
  expect(logFetch).not.toHaveBeenCalledWith(
    expect.anything(),
    "plan-realtime-fetches-self",
    "queued",
    expect.anything(),
    expect.anything(),
  );
});

it("scheduled self-enqueues recovery after realtime planner failure and keeps waitUntil rejected", async () => {
  const { default: worker } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, logFetch } = await import("./storage");
  const plannerError = new Error("scheduled planner failed");
  vi.mocked(listSchedulableRaceSourcesByDate).mockRejectedValueOnce(plannerError);
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z" } as never);
  const send = vi.spyOn(env.REALTIME_JOBS, "send");
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "* 1-12 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    env,
    ctx,
  );
  expect(waits).toHaveLength(1);
  await expect(waits[0]).rejects.toBe(plannerError);
  expect(send).toHaveBeenCalledWith(
    { date: "20260512", selfSchedule: true, type: "plan-realtime-fetches" },
    { delaySeconds: 60 },
  );
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-realtime-fetches-self",
    "queued",
    null,
    "20260512",
  );
});

it("scheduled uses a bounded jittered single recovery for Hyperdrive connection pressure", async () => {
  const { default: worker } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, logFetch } = await import("./storage");
  const plannerError = new Error("remaining connection slots are reserved");
  vi.spyOn(Math, "random").mockReturnValue(0.5);
  vi.mocked(listSchedulableRaceSourcesByDate).mockRejectedValueOnce(plannerError);
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-12T03:06:00.000Z" } as never);
  const send = vi.spyOn(env.REALTIME_JOBS, "send");
  const { ctx, waits } = buildCtx();

  await worker.scheduled(
    {
      cron: "6-59/15 0-14 * * *",
      scheduledTime: Date.parse("2026-05-12T03:06:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    env,
    ctx,
  );

  await expect(waits[0]).rejects.toBe(plannerError);
  expect(send).toHaveBeenCalledWith(
    { date: "20260512", selfSchedule: true, type: "plan-realtime-fetches" },
    { delaySeconds: 180 },
  );
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-realtime-fetches-recovery",
    "queued",
    null,
    '{"delaySeconds":180,"failureClass":"connection_pressure","stage":"scheduled.enqueue-single-recovery"}',
    undefined,
  );
});

it("scheduled keeps the planner rejection visible when self-recovery enqueue also fails", async () => {
  const { default: worker } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  const plannerError = new Error("scheduled planner failed");
  const recoveryError = new Error("self recovery queue failed");
  vi.spyOn(console, "error").mockImplementation(() => {});
  vi.mocked(listSchedulableRaceSourcesByDate).mockRejectedValueOnce(plannerError);
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-12T03:00:00.000Z" } as never);
  vi.spyOn(env.REALTIME_JOBS, "send").mockRejectedValueOnce(recoveryError);
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "* 1-12 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    env,
    ctx,
  );
  expect(waits).toHaveLength(1);
  await expect(waits[0]).rejects.toBe(plannerError);
  expect(vi.mocked(console.error).mock.calls[0]?.[0]).toBe(
    `Scheduled realtime planner self-recovery failed date=20260512 name=Error message=self recovery queue failed stack=${recoveryError.stack}`,
  );
});

it("scheduled non-plan cron keeps its single waitUntil path without self-recovery", async () => {
  const { default: worker } = await import("./worker");
  const { logWin5CronResult } = await import("./win5-cron");
  const env = buildEnv();
  const send = vi.spyOn(env.REALTIME_JOBS, "send");
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "30 12 * * *",
      scheduledTime: Date.parse("2026-05-12T03:00:00.000Z"),
      noRetry: () => {},
    } as unknown as ScheduledController,
    env,
    ctx,
  );
  expect(waits).toHaveLength(1);
  await expect(waits[0]).resolves.toBeUndefined();
  expect(logWin5CronResult).toHaveBeenCalledTimes(1);
  expect(send).not.toHaveBeenCalled();
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

it("scheduled multi-day-prep cron reserves the FIFO inference queue for the next JST date", async () => {
  const { default: worker } = await import("./worker");
  const { runDailyFeatureBuildForEnv } = await import("./daily-feature-build");
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
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
  expect(runDailyFeatureBuildForEnv).not.toHaveBeenCalled();
  const dates = vi.mocked(planRunningStylePredictionsForDate).mock.calls.map(([, date]) => date);
  expect(dates).toStrictEqual(["20260513"]);
});

it("scheduled multi-day prep does not plan when the next-day feature warm fails", async () => {
  const { default: worker } = await import("./worker");
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { materializeRunningStyleFeatureParquetsForDate } =
    await import("./running-style-feature-materialize");
  vi.mocked(materializeRunningStyleFeatureParquetsForDate).mockResolvedValueOnce({
    date: "20260513",
    materializeError: "feature warm failed",
    materialized: 0,
    scanned: 1,
    skipped: 1,
  });
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
  expect(planRunningStylePredictionsForDate).not.toHaveBeenCalled();
});

it("scheduled today-backfill cron plans today without the legacy feature build", async () => {
  const { default: worker } = await import("./worker");
  const { runDailyFeatureBuildForEnv } = await import("./daily-feature-build");
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
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
  expect(runDailyFeatureBuildForEnv).not.toHaveBeenCalled();
  expect(planRunningStylePredictionsForDate).toHaveBeenCalledWith(
    expect.anything(),
    "20260512",
    new Date("2026-05-12T00:10:00.000Z"),
  );
});

it("queue acks a disabled build-daily-features message without running the legacy builder", async () => {
  const { default: worker } = await import("./worker");
  const { runDailyFeatureBuildForEnv } = await import("./daily-feature-build");
  const { logFetch } = await import("./storage");
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
  expect(runDailyFeatureBuildForEnv).not.toHaveBeenCalled();
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "build-daily-features",
    "disabled",
    null,
    "Catalog service owns realtime feature builds",
  );
  expect(ack).toHaveBeenCalledTimes(1);
  expect(retry).not.toHaveBeenCalled();
});

it("scheduled disables the legacy daily-feature-build cron without enqueueing", async () => {
  const { default: worker } = await import("./worker");
  const { runDailyFeatureBuildForEnv } = await import("./daily-feature-build");
  const { logFetch } = await import("./storage");
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
  expect(sendSpy).not.toHaveBeenCalled();
  expect(runDailyFeatureBuildForEnv).not.toHaveBeenCalled();
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "build-daily-features",
    "disabled",
    null,
    "Catalog service owns realtime feature builds",
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
  expect(vi.mocked(listSchedulableRaceSourcesByDate).mock.calls.map((call) => call[1])).toEqual(
    expect.arrayContaining(["20260512", "20260511"]),
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
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-premium-paddock",
    "ok",
    null,
    "0 jobs queued",
  );
});

it("scheduled still runs paddock when the successful result-plan log rejects", async () => {
  const { default: worker } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, logFetch } = await import("./storage");
  const resultLogError = new Error("result success log unavailable");
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([] as never);
  vi.mocked(logFetch).mockImplementation(async (_db, jobType, status) => {
    if (jobType === "plan-result-fetches" && status === "ok") throw resultLogError;
  });
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

  await expect(waits[0]).resolves.toBeUndefined();
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-premium-paddock",
    "ok",
    null,
    "0 jobs queued",
  );
  expect(console.error).toHaveBeenCalledTimes(1);
});

it("scheduled still runs paddock when result planning and its error log both reject", async () => {
  const { default: worker } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, logFetch } = await import("./storage");
  const plannerError = new Error("result planner unavailable");
  const resultLogError = new Error("result error log unavailable");
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  vi.mocked(listSchedulableRaceSourcesByDate)
    .mockRejectedValueOnce(plannerError)
    .mockResolvedValue([] as never);
  vi.mocked(logFetch).mockImplementation(async (_db, jobType, status) => {
    if (jobType === "plan-result-fetches" && status === "error") throw resultLogError;
  });
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

  await expect(waits[0]).resolves.toBeUndefined();
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-premium-paddock",
    "ok",
    null,
    "0 jobs queued",
  );
  expect(console.error).toHaveBeenCalledTimes(1);
});

it("scheduled result-poll cron runs premium paddock after result planning in one waitUntil", async () => {
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
  expect(waits).toHaveLength(1);
  await flushWaits(waits);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-premium-paddock",
    "ok",
    null,
    "0 jobs queued",
  );
  expect(
    vi.mocked(listSchedulableRaceSourcesByDate).mock.calls.map((call) => call[1]),
  ).toStrictEqual(["20260512", "20260511", "20260512", "20260513"]);
});

it("scheduled closes a premium-paddock outcome log failure without rejecting waitUntil", async () => {
  const { default: worker } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, logFetch } = await import("./storage");
  const paddockLogError = new Error("paddock success log unavailable");
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  vi.mocked(listSchedulableRaceSourcesByDate).mockResolvedValue([] as never);
  vi.mocked(logFetch).mockImplementation(async (_db, jobType, status) => {
    if (jobType === "plan-premium-paddock" && status === "ok") throw paddockLogError;
  });
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

  await expect(waits[0]).resolves.toBeUndefined();
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-result-fetches",
    "ok",
    null,
    "0 jobs queued",
  );
  expect(console.error).toHaveBeenCalledTimes(1);
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
  const { handleWin5PredictionJob } = await import("./win5-queue");
  vi.spyOn(console, "error").mockImplementation(() => {});
  vi.mocked(handleWin5PredictionJob).mockRejectedValueOnce(
    new Error("D1_ERROR: D1 DB is overloaded. Please try again later."),
  );
  const ack = vi.fn();
  const retry = vi.fn();
  const message = {
    ack,
    body: {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      predictedAt: "2026-05-12T03:00:00.000Z",
      type: "generate-win5-predictions",
    } satisfies Job,
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
  const { handleWin5PredictionJob } = await import("./win5-queue");
  const boom = new Error("unrelated boom");
  vi.spyOn(console, "error").mockImplementation(() => {});
  vi.mocked(handleWin5PredictionJob).mockRejectedValueOnce(boom);
  const ack = vi.fn();
  const retry = vi.fn();
  const message = {
    ack,
    body: {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      predictedAt: "2026-05-12T03:00:00.000Z",
      type: "generate-win5-predictions",
    } satisfies Job,
    retry,
  };
  await worker.queue(
    { messages: [message], queue: "q", retryAll: () => {}, ackAll: () => {} } as never,
    buildEnv(),
  );
  expect(retry).toHaveBeenCalledWith({ delaySeconds: 60 });
  expect(vi.mocked(console.error).mock.calls[0]?.[0]).toBe(
    `Queue job failed type=generate-win5-predictions name=Error message=unrelated boom stack=${boom.stack}`,
  );
});

it("queue converts planner connection pressure into one delayed recovery and acks the original", async () => {
  const { default: worker } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  vi.spyOn(Math, "random").mockReturnValue(0);
  vi.mocked(listSchedulableRaceSourcesByDate).mockRejectedValueOnce(
    new Error("MaxClientsInSessionMode: too many connections"),
  );
  const ack = vi.fn();
  const retry = vi.fn();
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-12T03:06:00.000Z" } as never);
  const send = vi.spyOn(env.REALTIME_JOBS, "send");
  const message = {
    ack,
    body: { date: "20260512", type: "plan-realtime-fetches" } satisfies Job,
    retry,
  };

  await worker.queue(
    { messages: [message], queue: "q", retryAll: () => {}, ackAll: () => {} } as never,
    env,
  );

  expect(send).toHaveBeenCalledWith(
    { date: "20260512", selfSchedule: true, type: "plan-realtime-fetches" },
    { delaySeconds: 120 },
  );
  expect(ack).toHaveBeenCalledTimes(1);
  expect(retry).not.toHaveBeenCalled();
});

it("queue acks a failed connection-pressure recovery without creating another retry", async () => {
  const { default: worker } = await import("./worker");
  const { listSchedulableRaceSourcesByDate, logFetch } = await import("./storage");
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  vi.mocked(listSchedulableRaceSourcesByDate).mockRejectedValueOnce(
    new Error("connection pool exhausted"),
  );
  const ack = vi.fn();
  const retry = vi.fn();
  const env = buildEnv({ REALTIME_TEST_NOW: "2026-05-12T03:10:00.000Z" } as never);
  const send = vi.spyOn(env.REALTIME_JOBS, "send");
  const message = {
    ack,
    body: {
      date: "20260512",
      selfSchedule: true,
      type: "plan-realtime-fetches",
    } satisfies Job,
    retry,
  };

  await worker.queue(
    { messages: [message], queue: "q", retryAll: () => {}, ackAll: () => {} } as never,
    env,
  );

  expect(send).not.toHaveBeenCalled();
  expect(ack).toHaveBeenCalledTimes(1);
  expect(retry).not.toHaveBeenCalled();
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "plan-realtime-fetches-recovery",
    "exhausted",
    null,
    '{"failureClass":"connection_pressure","stage":"queue.single-recovery-exhausted"}',
    undefined,
  );
});

it("concurrent planner failures acquire one atomic recovery claim and send one recovery", async () => {
  const { default: worker } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  vi.mocked(listSchedulableRaceSourcesByDate).mockRejectedValue(
    new Error("Timed out while creating a new server connection."),
  );
  const firstAck = vi.fn();
  const firstRetry = vi.fn();
  const secondAck = vi.fn();
  const secondRetry = vi.fn();
  const env = buildEnv({
    REALTIME_DB: buildAtomicRecoveryDb(),
    REALTIME_TEST_NOW: "2026-05-12T03:06:00.000Z",
  });
  const send = vi.spyOn(env.REALTIME_JOBS, "send");

  await Promise.all([
    worker.queue(
      {
        ackAll: () => {},
        messages: [
          {
            ack: firstAck,
            body: { date: "20260512", type: "plan-realtime-fetches" } satisfies Job,
            retry: firstRetry,
          },
        ],
        queue: "q",
        retryAll: () => {},
      } as never,
      env,
    ),
    worker.queue(
      {
        ackAll: () => {},
        messages: [
          {
            ack: secondAck,
            body: { date: "20260512", type: "plan-realtime-fetches" } satisfies Job,
            retry: secondRetry,
          },
        ],
        queue: "q",
        retryAll: () => {},
      } as never,
      env,
    ),
  ]);

  expect(send).toHaveBeenCalledTimes(1);
  expect(firstAck).toHaveBeenCalledTimes(1);
  expect(secondAck).toHaveBeenCalledTimes(1);
  expect(firstRetry).not.toHaveBeenCalled();
  expect(secondRetry).not.toHaveBeenCalled();
});

it("a failed recovery send rolls back its claim so the next delivery can acquire it", async () => {
  const { default: worker } = await import("./worker");
  const { listSchedulableRaceSourcesByDate } = await import("./storage");
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  vi.mocked(listSchedulableRaceSourcesByDate).mockRejectedValue(
    new Error("Failed to acquire permit to connect to the database."),
  );
  const firstAck = vi.fn();
  const firstRetry = vi.fn();
  const secondAck = vi.fn();
  const secondRetry = vi.fn();
  const env = buildEnv({
    REALTIME_DB: buildAtomicRecoveryDb(),
    REALTIME_TEST_NOW: "2026-05-12T03:06:00.000Z",
  });
  const sendError = new Error("recovery queue unavailable");
  const send = vi
    .spyOn(env.REALTIME_JOBS, "send")
    .mockRejectedValueOnce(sendError)
    .mockResolvedValueOnce(queueSendResponse());

  await worker.queue(
    {
      ackAll: () => {},
      messages: [
        {
          ack: firstAck,
          body: { date: "20260512", type: "plan-realtime-fetches" } satisfies Job,
          retry: firstRetry,
        },
      ],
      queue: "q",
      retryAll: () => {},
    } as never,
    env,
  );
  await worker.queue(
    {
      ackAll: () => {},
      messages: [
        {
          ack: secondAck,
          body: { date: "20260512", type: "plan-realtime-fetches" } satisfies Job,
          retry: secondRetry,
        },
      ],
      queue: "q",
      retryAll: () => {},
    } as never,
    env,
  );

  expect(send).toHaveBeenCalledTimes(2);
  expect(firstAck).toHaveBeenCalledTimes(1);
  expect(secondAck).toHaveBeenCalledTimes(1);
  expect(firstRetry).not.toHaveBeenCalled();
  expect(secondRetry).not.toHaveBeenCalled();
});

it("queue persists the running-style start attempt before prediction and then acknowledges success", async () => {
  const { default: worker } = await import("./worker");
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { logFetch } = await import("./storage");
  const order: string[] = [];
  vi.mocked(logFetch).mockImplementationOnce(async () => {
    order.push("start");
  });
  vi.mocked(handleRunningStylePredictionJob).mockImplementationOnce(async () => {
    order.push("prediction");
    return null;
  });
  const ack = vi.fn(() => {
    order.push("ack");
  });
  const retry = vi.fn();
  const message = {
    ack,
    attempts: 3,
    body: {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      predictedAt: "2026-05-12T11:00:00.000Z",
      raceBango: "01",
      raceKey: "jra:20260512:08:01",
      source: "jra",
      type: "generate-running-style-predictions",
    } satisfies Job,
    retry,
  };
  await worker.queue(
    { messages: [message], queue: "q", retryAll: () => {}, ackAll: () => {} } as never,
    buildEnv(),
  );
  expect(order).toStrictEqual(["start", "prediction", "ack"]);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "generate-running-style-predictions",
    "started",
    "jra:20260512:08:01",
    "queue=running-style-predictions attempts=3",
    undefined,
  );
  expect(retry).not.toHaveBeenCalled();
});

it("queue logs catalog 502 details and retries running-style prediction failures", async () => {
  const { default: worker } = await import("./worker");
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { logFetch } = await import("./storage");
  const catalogError = new Error(
    "PC_KEIBA_R2_CATALOG /v1/running-style-features failed with HTTP 502: r2_sql_unavailable",
  );
  vi.spyOn(console, "error").mockImplementation(() => {});
  vi.mocked(handleRunningStylePredictionJob).mockRejectedValueOnce(catalogError);
  const ack = vi.fn();
  const retry = vi.fn();
  const message = {
    ack,
    attempts: 2,
    body: {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      predictedAt: "2026-05-12T11:00:00.000Z",
      raceBango: "01",
      raceKey: "jra:20260512:08:01",
      source: "jra",
      type: "generate-running-style-predictions",
    } satisfies Job,
    retry,
  };
  await worker.queue(
    { messages: [message], queue: "q", retryAll: () => {}, ackAll: () => {} } as never,
    buildEnv(),
  );
  expect(ack).not.toHaveBeenCalled();
  expect(retry).toHaveBeenCalledWith({ delaySeconds: 60 });
  expect(logFetch).toHaveBeenNthCalledWith(
    1,
    expect.anything(),
    "generate-running-style-predictions",
    "started",
    "jra:20260512:08:01",
    "queue=running-style-predictions attempts=2",
    undefined,
  );
  expect(logFetch).toHaveBeenNthCalledWith(
    2,
    expect.anything(),
    "generate-running-style-predictions",
    "error",
    "jra:20260512:08:01",
    "queue=running-style-predictions attempts=2 error=PC_KEIBA_R2_CATALOG /v1/running-style-features failed with HTTP 502: r2_sql_unavailable",
    undefined,
  );
  expect(vi.mocked(console.error).mock.calls[0]?.[0]).toBe(
    `Queue job failed attempts=2 raceKey=jra:20260512:08:01 type=generate-running-style-predictions name=Error message=PC_KEIBA_R2_CATALOG /v1/running-style-features failed with HTTP 502: r2_sql_unavailable stack=${catalogError.stack}`,
  );
});

it("queue continues running-style prediction when the awaited start log fails", async () => {
  const { default: worker } = await import("./worker");
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { logFetch } = await import("./storage");
  const logError = new Error("D1 start log unavailable");
  vi.spyOn(console, "warn").mockImplementation(() => {});
  vi.mocked(logFetch).mockRejectedValueOnce(logError);
  const ack = vi.fn();
  const retry = vi.fn();
  const message = {
    ack,
    attempts: 5,
    body: {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      predictedAt: "2026-05-12T11:00:00.000Z",
      raceBango: "01",
      raceKey: "jra:20260512:08:01",
      source: "jra",
      type: "generate-running-style-predictions",
    } satisfies Job,
    retry,
  };
  await worker.queue(
    { messages: [message], queue: "q", retryAll: () => {}, ackAll: () => {} } as never,
    buildEnv(),
  );
  expect(handleRunningStylePredictionJob).toHaveBeenCalledTimes(1);
  expect(ack).toHaveBeenCalledTimes(1);
  expect(retry).not.toHaveBeenCalled();
  expect(vi.mocked(console.warn).mock.calls[0]?.[0]).toBe(
    `Running-style queue start log failed attempts=5 raceKey=jra:20260512:08:01 name=Error message=D1 start log unavailable stack=${logError.stack}`,
  );
});

it("queue retries the original running-style failure when the error log also fails", async () => {
  const { default: worker } = await import("./worker");
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { logFetch } = await import("./storage");
  const predictionError = new Error("prediction failed");
  const logError = new Error("D1 error log unavailable");
  vi.spyOn(console, "error").mockImplementation(() => {});
  vi.spyOn(console, "warn").mockImplementation(() => {});
  vi.mocked(handleRunningStylePredictionJob).mockRejectedValueOnce(predictionError);
  vi.mocked(logFetch).mockResolvedValueOnce(undefined).mockRejectedValueOnce(logError);
  const ack = vi.fn();
  const retry = vi.fn();
  const message = {
    ack,
    attempts: 6,
    body: {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      predictedAt: "2026-05-12T11:00:00.000Z",
      raceBango: "01",
      raceKey: "jra:20260512:08:01",
      source: "jra",
      type: "generate-running-style-predictions",
    } satisfies Job,
    retry,
  };
  await worker.queue(
    { messages: [message], queue: "q", retryAll: () => {}, ackAll: () => {} } as never,
    buildEnv(),
  );
  expect(ack).not.toHaveBeenCalled();
  expect(retry).toHaveBeenCalledWith({ delaySeconds: 60 });
  expect(vi.mocked(console.warn).mock.calls[0]?.[0]).toBe(
    `Running-style queue error log failed attempts=6 raceKey=jra:20260512:08:01 name=Error message=D1 error log unavailable stack=${logError.stack}`,
  );
  expect(vi.mocked(console.error).mock.calls[0]?.[0]).toBe(
    `Queue job failed attempts=6 raceKey=jra:20260512:08:01 type=generate-running-style-predictions name=Error message=prediction failed stack=${predictionError.stack}`,
  );
});

it("queue falls back to implicit retry when delayed retry throws", async () => {
  const { default: worker } = await import("./worker");
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const catalogError = new Error(
    "PC_KEIBA_R2_CATALOG /v1/running-style-features failed with HTTP 502: r2_sql_unavailable",
  );
  const retryError = new Error("retry delay not configured");
  vi.spyOn(console, "error").mockImplementation(() => {});
  vi.mocked(handleRunningStylePredictionJob).mockRejectedValueOnce(catalogError);
  const ack = vi.fn();
  const retry = vi
    .fn()
    .mockImplementationOnce(() => {
      throw retryError;
    })
    .mockImplementationOnce(() => undefined);
  const message = {
    ack,
    attempts: 4,
    body: {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      predictedAt: "2026-05-12T11:00:00.000Z",
      raceBango: "01",
      raceKey: "jra:20260512:08:01",
      source: "jra",
      type: "generate-running-style-predictions",
    } satisfies Job,
    retry,
  };
  await worker.queue(
    { messages: [message], queue: "q", retryAll: () => {}, ackAll: () => {} } as never,
    buildEnv(),
  );
  expect(ack).not.toHaveBeenCalled();
  expect(retry).toHaveBeenCalledTimes(2);
  expect(retry.mock.calls[0]?.[0]).toStrictEqual({ delaySeconds: 60 });
  expect(retry.mock.calls[1]?.[0]).toBeUndefined();
  expect(vi.mocked(console.error).mock.calls[0]?.[0]).toBe(
    `Queue job failed attempts=4 raceKey=jra:20260512:08:01 type=generate-running-style-predictions name=Error message=PC_KEIBA_R2_CATALOG /v1/running-style-features failed with HTTP 502: r2_sql_unavailable stack=${catalogError.stack}`,
  );
  expect(vi.mocked(console.error).mock.calls[1]?.[0]).toBe(
    `Queue delayed retry failed attempts=4 raceKey=jra:20260512:08:01 type=generate-running-style-predictions name=Error message=retry delay not configured stack=${retryError.stack}`,
  );
});

it("scheduled triggers the weight watchdog for the every-two-minute cron", async () => {
  const { default: worker } = await import("./worker");
  const { logFetch } = await import("./storage");
  const { ctx, waits } = buildCtx();
  await worker.scheduled(
    {
      cron: "1-59/2 * * * *",
      scheduledTime: Date.parse("2026-06-07T03:01:00.000Z"),
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
    undefined,
    3600,
  );
});
