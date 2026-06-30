// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { Env } from "./types";

const queueSendOk = async (): Promise<QueueSendResponse> => ({
  metadata: { metrics: { backlogCount: 0, backlogBytes: 0 } },
});

const queueMetricsOk = async (): Promise<QueueMetrics> => ({
  backlogCount: 0,
  backlogBytes: 0,
});

vi.mock("./finish-position-lite-pool", () => ({
  getFinishPositionPool: vi.fn(() => ({ query: vi.fn(async () => ({ rows: [] })) })),
}));
vi.mock("./running-style-d1", () => ({
  listRaceRunningStyleCounts: vi.fn(async () => new Map()),
  listRaceRunningStylesForRace: vi.fn(async () => []),
  listRunningStyleInferenceStates: vi.fn(async () => new Map()),
  upsertRunningStylePendingStates: vi.fn(async () => {}),
}));
vi.mock("./running-style-expected-horses", () => ({
  listRunningStyleExpectedHorseCounts: vi.fn(async () => new Map()),
}));
vi.mock("./running-style-neon", () => ({
  listRaceRunningStylePredictionCountsByDate: vi.fn(async () => new Map()),
}));
vi.mock("./viewer-running-style-cache", () => ({
  putViewerRunningStyleRaceCache: vi.fn(async () => true),
}));
vi.mock("./running-style-race-list", () => ({
  listRunningStyleRacesByDate: vi.fn(async () => ({ races: [], source: "d1" })),
}));

const buildEnv = (overrides?: Partial<Env>): Env => {
  return {
    REALTIME_DB: {},
    REALTIME_JOBS: { send: vi.fn(async () => {}), sendBatch: vi.fn(async () => {}) },
    RUNNING_STYLE_D1_WRITE_ENABLED: "1",
    ...overrides,
  } as unknown as Env;
};

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("planRunningStylePredictionsForDate returns empty summary when no races registered", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const env = buildEnv();
  const summary = await planRunningStylePredictionsForDate(
    env,
    "20260512",
    new Date("2026-05-12T12:00:00.000Z"),
  );
  expect(summary.scanned).toBe(0);
  expect(summary.enqueued).toBe(0);
});

it("planRunningStylePredictionsForDate skips enqueueing when inference disabled", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        race_bango: "01",
        source: "jra",
      },
    ],
    source: "d1",
  });
  const env = buildEnv({ RUNNING_STYLE_D1_WRITE_ENABLED: "0" });
  const summary = await planRunningStylePredictionsForDate(
    env,
    "20260512",
    new Date("2026-05-12T12:00:00.000Z"),
  );
  expect(summary.scanned).toBe(1);
  expect(summary.missingFeatures).toBe(1);
});

it("planRunningStylePredictionsForDate enqueues jobs when races need running-style predictions", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        race_bango: "01",
        source: "jra",
      },
    ],
    source: "d1",
  });
  const metrics = vi.fn(queueMetricsOk);
  const send = vi.fn(queueSendOk);
  const sendBatch = vi.fn(queueSendOk);
  const env = buildEnv({
    RUNNING_STYLE_JOBS: { metrics, send, sendBatch },
  });
  const summary = await planRunningStylePredictionsForDate(
    env,
    "20260512",
    new Date("2026-05-12T12:00:00.000Z"),
  );
  expect(summary.enqueued).toBe(1);
});

it("refreshViewerRunningStyleCacheForRace returns false on malformed race key", async () => {
  const { refreshViewerRunningStyleCacheForRace } = await import("./running-style-cron");
  expect(await refreshViewerRunningStyleCacheForRace(buildEnv(), "broken-key")).toBe(false);
});

it("refreshViewerRunningStyleCacheForRace returns false when D1 has no rows", async () => {
  const { refreshViewerRunningStyleCacheForRace } = await import("./running-style-cron");
  const { listRaceRunningStylesForRace } = await import("./running-style-d1");
  vi.mocked(listRaceRunningStylesForRace).mockResolvedValue([]);
  expect(await refreshViewerRunningStyleCacheForRace(buildEnv(), "jra:20260512:08:01")).toBe(false);
});

it("refreshViewerRunningStyleCacheForRace writes cache when rows present", async () => {
  const { refreshViewerRunningStyleCacheForRace } = await import("./running-style-cron");
  const { listRaceRunningStylesForRace } = await import("./running-style-d1");
  const { putViewerRunningStyleRaceCache } = await import("./viewer-running-style-cache");
  vi.mocked(listRaceRunningStylesForRace).mockResolvedValue([
    {
      bamei: null,
      category: "jra",
      horseNumber: 1,
      kaisaiNen: "2026",
      kettoTorokuBango: "ktb",
      modelVersion: "v7",
      pNige: 0,
      pOikomi: 0,
      pSashi: 0,
      pSenkou: 1,
      predictedAt: "x",
      predictedLabel: "nige",
      raceKey: "jra:20260512:08:01",
    },
  ]);
  vi.mocked(putViewerRunningStyleRaceCache).mockResolvedValue(true);
  expect(await refreshViewerRunningStyleCacheForRace(buildEnv(), "jra:20260512:08:01")).toBe(true);
});

it("refreshViewerRunningStyleCachesForDate skips when inference is disabled", async () => {
  const { refreshViewerRunningStyleCachesForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        race_bango: "01",
        source: "jra",
      },
    ],
    source: "d1",
  });
  const env = buildEnv({ RUNNING_STYLE_D1_WRITE_ENABLED: "0" });
  const result = await refreshViewerRunningStyleCachesForDate(env, "20260512");
  expect(result.refreshed).toBe(0);
  expect(result.skipped).toBe(1);
});

it("refreshViewerRunningStyleCachesForDate refreshes only races with predictions", async () => {
  const { refreshViewerRunningStyleCachesForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { listRaceRunningStyleCounts, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
  const { putViewerRunningStyleRaceCache } = await import("./viewer-running-style-cache");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        race_bango: "01",
        source: "jra",
      },
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        race_bango: "02",
        source: "jra",
      },
    ],
    source: "d1",
  });
  vi.mocked(listRaceRunningStyleCounts).mockResolvedValue(
    new Map([
      ["jra:20260512:08:01", 16],
      ["jra:20260512:08:02", 0],
    ]),
  );
  vi.mocked(listRaceRunningStylesForRace).mockResolvedValue([
    {
      bamei: null,
      category: "jra",
      horseNumber: 1,
      kaisaiNen: "2026",
      kettoTorokuBango: "ktb",
      modelVersion: "v7",
      pNige: 0,
      pOikomi: 0,
      pSashi: 0,
      pSenkou: 1,
      predictedAt: "x",
      predictedLabel: "nige",
      raceKey: "jra:20260512:08:01",
    },
  ]);
  vi.mocked(putViewerRunningStyleRaceCache).mockResolvedValue(true);
  const result = await refreshViewerRunningStyleCachesForDate(buildEnv(), "20260512");
  expect(result.scanned).toBe(2);
  expect(result.refreshed).toBe(1);
  expect(result.skipped).toBe(1);
});

it("planRunningStylePredictionsForDate sends a batch when more than one race is enqueued", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        race_bango: "01",
        source: "jra",
      },
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        race_bango: "02",
        source: "jra",
      },
    ],
    source: "d1",
  });
  const metrics = vi.fn(queueMetricsOk);
  const send = vi.fn(queueSendOk);
  const sendBatch = vi.fn(queueSendOk);
  const env = buildEnv({
    RUNNING_STYLE_JOBS: { metrics, send, sendBatch },
  });
  const summary = await planRunningStylePredictionsForDate(
    env,
    "20260512",
    new Date("2026-05-12T12:00:00.000Z"),
  );
  expect(summary.enqueued).toBe(2);
  expect(sendBatch).toHaveBeenCalledTimes(1);
  expect(send).not.toHaveBeenCalled();
});

it("refreshViewerRunningStyleCachesForDate counts rows length zero in the per-race loop as skipped", async () => {
  const { refreshViewerRunningStyleCachesForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { listRaceRunningStyleCounts, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        race_bango: "01",
        source: "jra",
      },
    ],
    source: "d1",
  });
  vi.mocked(listRaceRunningStyleCounts).mockResolvedValue(new Map([["jra:20260512:08:01", 12]]));
  vi.mocked(listRaceRunningStylesForRace).mockResolvedValue([]);
  const result = await refreshViewerRunningStyleCachesForDate(buildEnv(), "20260512");
  expect(result.scanned).toBe(1);
  expect(result.refreshed).toBe(0);
  expect(result.skipped).toBe(1);
});

it("refreshViewerRunningStyleCachesForDate counts a failed cache write as skipped", async () => {
  const { refreshViewerRunningStyleCachesForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { listRaceRunningStyleCounts, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
  const { putViewerRunningStyleRaceCache } = await import("./viewer-running-style-cache");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        race_bango: "01",
        source: "jra",
      },
    ],
    source: "d1",
  });
  vi.mocked(listRaceRunningStyleCounts).mockResolvedValue(new Map([["jra:20260512:08:01", 12]]));
  vi.mocked(listRaceRunningStylesForRace).mockResolvedValue([
    {
      bamei: null,
      category: "jra",
      horseNumber: 1,
      kaisaiNen: "2026",
      kettoTorokuBango: "ktb",
      modelVersion: "v7",
      pNige: 0,
      pOikomi: 0,
      pSashi: 0,
      pSenkou: 1,
      predictedAt: "x",
      predictedLabel: "nige",
      raceKey: "jra:20260512:08:01",
    },
  ]);
  vi.mocked(putViewerRunningStyleRaceCache).mockResolvedValue(false);
  const result = await refreshViewerRunningStyleCachesForDate(buildEnv(), "20260512");
  expect(result.refreshed).toBe(0);
  expect(result.skipped).toBe(1);
});

it("refreshViewerRunningStyleCachesForDate treats a race missing from predictionCounts as skipped via the ?? 0 fallback", async () => {
  const { refreshViewerRunningStyleCachesForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { listRaceRunningStyleCounts } = await import("./running-style-d1");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        race_bango: "07",
        source: "jra",
      },
    ],
    source: "d1",
  });
  vi.mocked(listRaceRunningStyleCounts).mockResolvedValue(new Map());
  const result = await refreshViewerRunningStyleCachesForDate(buildEnv(), "20260512");
  expect(result.skipped).toBe(1);
  expect(result.refreshed).toBe(0);
});

it("runRunningStyleCronTick captures plan error as planError on summary", async () => {
  const { runRunningStyleCronTick } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  vi.mocked(listRunningStyleRacesByDate).mockRejectedValue(new Error("boom"));
  const summary = await runRunningStyleCronTick(buildEnv(), new Date("2026-05-12T12:00:00.000Z"));
  expect(summary.planError).toBe("boom");
});

it("planRunningStylePredictionsForDate skips enqueueing when every completed race has a Neon mirror", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { listRunningStyleInferenceStates } = await import("./running-style-d1");
  const { getFinishPositionPool } = await import("./finish-position-lite-pool");
  const { listRaceRunningStylePredictionCountsByDate } = await import("./running-style-neon");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        race_bango: "01",
        source: "jra",
      },
    ],
    source: "d1",
  });
  vi.mocked(listRunningStyleInferenceStates).mockResolvedValue(
    new Map([
      [
        "jra:20260512:08:01",
        {
          attemptedAt: "2026-05-12T11:00:00.000Z",
          cellModelKey: null,
          cellVariantId: null,
          completedAt: "2026-05-12T11:05:00.000Z",
          expectedHorseCount: 16,
          featuresR2Key: null,
          modelVersion: "v7",
          raceKey: "jra:20260512:08:01",
          status: "completed",
          writtenHorseCount: 16,
        },
      ],
    ]),
  );
  vi.mocked(listRaceRunningStylePredictionCountsByDate).mockResolvedValue(
    new Map([["jra:20260512:08:01", new Map([["v7", 16]])]]),
  );
  const summary = await planRunningStylePredictionsForDate(
    buildEnv(),
    "20260512",
    new Date("2026-05-12T12:00:00.000Z"),
  );
  expect(summary.completed).toBe(1);
  expect(summary.enqueued).toBe(0);
  expect(summary.scanned).toBe(1);
  expect(getFinishPositionPool).toHaveBeenCalledTimes(1);
  expect(listRaceRunningStylePredictionCountsByDate).toHaveBeenCalledTimes(1);
});

it("planRunningStylePredictionsForDate requeues completed races when Neon mirror is missing", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { listRunningStyleInferenceStates, upsertRunningStylePendingStates } =
    await import("./running-style-d1");
  const { listRaceRunningStylePredictionCountsByDate } = await import("./running-style-neon");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        race_bango: "01",
        source: "jra",
      },
    ],
    source: "d1",
  });
  vi.mocked(listRunningStyleInferenceStates).mockResolvedValue(
    new Map([
      [
        "jra:20260512:08:01",
        {
          attemptedAt: "2026-05-12T11:00:00.000Z",
          cellModelKey: null,
          cellVariantId: null,
          completedAt: "2026-05-12T11:05:00.000Z",
          expectedHorseCount: 16,
          featuresR2Key: "features.parquet",
          modelVersion: "v7",
          raceKey: "jra:20260512:08:01",
          status: "completed",
          writtenHorseCount: 16,
        },
      ],
    ]),
  );
  vi.mocked(listRaceRunningStylePredictionCountsByDate).mockResolvedValue(new Map());
  const send = vi.fn(queueSendOk);
  const env = buildEnv({
    RUNNING_STYLE_JOBS: {
      metrics: vi.fn(queueMetricsOk),
      send,
      sendBatch: vi.fn(queueSendOk),
    },
  });
  const summary = await planRunningStylePredictionsForDate(
    env,
    "20260512",
    new Date("2026-05-12T12:00:00.000Z"),
  );
  expect(summary.completed).toBe(0);
  expect(summary.enqueued).toBe(1);
  expect(send).toHaveBeenCalledWith(
    expect.objectContaining({
      raceKey: "jra:20260512:08:01",
      type: "generate-running-style-predictions",
    }),
  );
  expect(upsertRunningStylePendingStates).not.toHaveBeenCalled();
});

it("planRunningStylePredictionsForDate requeues completed races when Neon mirror is incomplete", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { listRunningStyleInferenceStates } = await import("./running-style-d1");
  const { listRaceRunningStylePredictionCountsByDate } = await import("./running-style-neon");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        race_bango: "01",
        source: "jra",
      },
    ],
    source: "d1",
  });
  vi.mocked(listRunningStyleInferenceStates).mockResolvedValue(
    new Map([
      [
        "jra:20260512:08:01",
        {
          attemptedAt: "2026-05-12T11:00:00.000Z",
          cellModelKey: null,
          cellVariantId: null,
          completedAt: "2026-05-12T11:05:00.000Z",
          expectedHorseCount: 16,
          featuresR2Key: "features.parquet",
          modelVersion: "v7",
          raceKey: "jra:20260512:08:01",
          status: "completed",
          writtenHorseCount: 16,
        },
      ],
    ]),
  );
  vi.mocked(listRaceRunningStylePredictionCountsByDate).mockResolvedValue(
    new Map([["jra:20260512:08:01", new Map([["v7", 15]])]]),
  );
  const send = vi.fn(queueSendOk);
  const summary = await planRunningStylePredictionsForDate(
    buildEnv({
      RUNNING_STYLE_JOBS: {
        metrics: vi.fn(queueMetricsOk),
        send,
        sendBatch: vi.fn(queueSendOk),
      },
    }),
    "20260512",
    new Date("2026-05-12T12:00:00.000Z"),
  );
  expect(summary.completed).toBe(0);
  expect(summary.enqueued).toBe(1);
  expect(send).toHaveBeenCalledTimes(1);
});

it("planRunningStylePredictionsForDate still queries Neon when only some races are completed", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { listRunningStyleInferenceStates } = await import("./running-style-d1");
  const { getFinishPositionPool } = await import("./finish-position-lite-pool");
  const { listRaceRunningStylePredictionCountsByDate } = await import("./running-style-neon");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        race_bango: "01",
        source: "jra",
      },
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        race_bango: "02",
        source: "jra",
      },
    ],
    source: "d1",
  });
  vi.mocked(listRunningStyleInferenceStates).mockResolvedValue(
    new Map([
      [
        "jra:20260512:08:01",
        {
          attemptedAt: "2026-05-12T11:00:00.000Z",
          cellModelKey: null,
          cellVariantId: null,
          completedAt: "2026-05-12T11:05:00.000Z",
          expectedHorseCount: 16,
          featuresR2Key: null,
          modelVersion: "v7",
          raceKey: "jra:20260512:08:01",
          status: "completed",
          writtenHorseCount: 16,
        },
      ],
    ]),
  );
  vi.mocked(listRaceRunningStylePredictionCountsByDate).mockResolvedValue(
    new Map([["jra:20260512:08:01", new Map([["v7", 16]])]]),
  );
  const metrics = vi.fn(queueMetricsOk);
  const send = vi.fn(queueSendOk);
  const sendBatch = vi.fn(queueSendOk);
  const summary = await planRunningStylePredictionsForDate(
    buildEnv({ RUNNING_STYLE_JOBS: { metrics, send, sendBatch } }),
    "20260512",
    new Date("2026-05-12T12:00:00.000Z"),
  );
  expect(summary.completed).toBe(1);
  expect(summary.enqueued).toBe(1);
  expect(getFinishPositionPool).toHaveBeenCalledTimes(1);
});
