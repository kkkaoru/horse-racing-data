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
vi.mock("./running-style-catalog-client", () => ({
  fetchRunningStyleFeatureCountsFromCatalog: vi.fn(async () => new Map()),
}));
vi.mock("./running-style-d1", () => ({
  listRaceRunningStyleCounts: vi.fn(async () => new Map()),
  listRaceRunningStylesForRace: vi.fn(async () => []),
  listRunningStyleInferenceStates: vi.fn(async () => new Map()),
  markRunningStyleInferenceEnqueueFailed: vi.fn(async () => {}),
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
    DETAIL_SECTION_CACHE_KV: {
      get: vi.fn(async () => null),
      put: vi.fn(async () => {}),
    },
    FEATURES_ARCHIVE: {
      head: vi.fn(async () => ({
        customMetadata: {
          "max-data-sakusei-nengappi": "20260512090000",
          "row-count": "12",
          "rs-predicted-at-max": "none",
          "rs-row-count": "0",
        },
        etag: "foundation-etag",
        size: 1024,
      })),
    },
    FINISH_POSITION_CRON: {
      fetch: vi.fn(async () => new Response(null, { status: 202 })),
    },
    REALTIME_DB: {},
    REALTIME_JOBS: {
      metrics: vi.fn(queueMetricsOk),
      send: vi.fn(async () => {}),
      sendBatch: vi.fn(async () => {}),
    },
    RUNNING_STYLE_D1_WRITE_ENABLED: "1",
    TRIGGER_TOKEN: "trigger-token",
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

it("planRunningStylePredictionsForDate rejects dates beyond tomorrow before reading races", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const env = buildEnv();
  const summary = await planRunningStylePredictionsForDate(
    env,
    "20260827",
    new Date("2026-08-25T12:00:00.000Z"),
  );
  expect(summary).toMatchObject({ enqueued: 0, scanned: 0 });
  expect(summary.planError).toContain("latest allowed date is 20260826");
  expect(listRunningStyleRacesByDate).not.toHaveBeenCalled();
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

it("defers dispatch while the dedicated queue has backlog", async () => {
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
  const metrics = vi.fn(
    async (): Promise<QueueMetrics> => ({
      backlogCount: 256,
      backlogBytes: 512,
    }),
  );
  const send = vi.fn(queueSendOk);
  const sendBatch = vi.fn(queueSendOk);
  const summary = await planRunningStylePredictionsForDate(
    buildEnv({ RUNNING_STYLE_JOBS: { metrics, send, sendBatch } }),
    "20260512",
    new Date("2026-05-12T12:00:00.000Z"),
  );
  expect(summary.enqueued).toBe(0);
  expect(summary.planError).toContain("queue projected backlog=257");
  expect(send).not.toHaveBeenCalled();
  expect(sendBatch).not.toHaveBeenCalled();
});

it("allows one bounded multi-race recovery batch above the queue cap", async () => {
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
  const metrics = vi.fn(
    async (): Promise<QueueMetrics> => ({
      backlogCount: 256,
      backlogBytes: 512,
    }),
  );
  const send = vi.fn(queueSendOk);
  const sendBatch = vi.fn(queueSendOk);
  const summary = await planRunningStylePredictionsForDate(
    buildEnv({ RUNNING_STYLE_JOBS: { metrics, send, sendBatch } }),
    "20260512",
    new Date("2026-05-12T12:00:00.000Z"),
  );
  expect(summary.enqueued).toBe(2);
  expect(summary.planError).toBeUndefined();
  expect(sendBatch).toHaveBeenCalledTimes(1);
});

it("fails closed when queue depth cannot be inspected", async () => {
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
  const metrics = vi.fn(async (): Promise<QueueMetrics> => {
    throw new Error("metrics unavailable");
  });
  const send = vi.fn(queueSendOk);
  const sendBatch = vi.fn(queueSendOk);
  const summary = await planRunningStylePredictionsForDate(
    buildEnv({ RUNNING_STYLE_JOBS: { metrics, send, sendBatch } }),
    "20260512",
    new Date("2026-05-12T12:00:00.000Z"),
  );
  expect(summary.enqueued).toBe(0);
  expect(summary.planError).toContain("metrics unavailable");
  expect(send).not.toHaveBeenCalled();
  expect(sendBatch).not.toHaveBeenCalled();
});

it("gates only the missing category foundation and triggers one forced prewarm", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { upsertRunningStylePendingStates } = await import("./running-style-d1");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0824",
        keibajo_code: "43",
        race_bango: "01",
        source: "nar",
      },
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0824",
        keibajo_code: "83",
        race_bango: "01",
        source: "nar",
      },
    ],
    source: "d1",
  });
  const head = vi.fn(async (key: string) =>
    key === "feat-running-style-base/catalog-v1/nar/20260824/features.parquet"
      ? {
          customMetadata: {
            "max-data-sakusei-nengappi": "20260824090000",
            "row-count": "12",
            "rs-predicted-at-max": "none",
            "rs-row-count": "0",
          },
          etag: "nar-foundation",
          size: 1024,
        }
      : null,
  );
  const fetch = vi.fn<typeof globalThis.fetch>(
    async (_input) => new Response(null, { status: 202 }),
  );
  const markerGet = vi.fn(async () => null);
  const markerPut = vi.fn(async () => {});
  const send = vi.fn(queueSendOk);
  const summary = await planRunningStylePredictionsForDate(
    buildEnv({
      DETAIL_SECTION_CACHE_KV: { get: markerGet, put: markerPut } as unknown as KVNamespace,
      FEATURES_ARCHIVE: { head } as unknown as R2Bucket,
      FINISH_POSITION_CRON: { fetch },
      RUNNING_STYLE_JOBS: {
        metrics: vi.fn(queueMetricsOk),
        send,
        sendBatch: vi.fn(queueSendOk),
      },
    }),
    "20260824",
    new Date("2026-08-23T17:42:34.492Z"),
  );
  const request = fetch.mock.calls[0]![0] as Request;
  expect(summary.enqueued).toBe(1);
  expect(summary.planError).toBeUndefined();
  expect(head.mock.calls.map(([key]) => key)).toStrictEqual([
    "feat-running-style-base/catalog-v1/nar/20260824/features.parquet",
    "feat-running-style-base/catalog-v1/ban-ei/20260824/features.parquet",
  ]);
  expect(send).toHaveBeenCalledWith({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0824",
    keibajoCode: "43",
    predictedAt: "2026-08-23T17:42:34.492Z",
    raceBango: "01",
    raceKey: "nar:20260824:43:01",
    source: "nar",
    type: "generate-running-style-predictions",
  });
  expect(
    vi.mocked(upsertRunningStylePendingStates).mock.calls[0]?.[1].map((race) => race.raceKey),
  ).toStrictEqual(["nar:20260824:43:01"]);
  expect(request.url).toBe("https://finish-position-cron.internal/api/admin/prewarm-day-base");
  expect(request.headers.get("authorization")).toBe("Bearer trigger-token");
  expect(await request.json()).toStrictEqual({
    category: "ban-ei",
    force: true,
    generatePredictionsAfterHit: true,
    runYmd: "20260824",
  });
  expect(markerGet).toHaveBeenCalledWith(
    "control:running-style-foundation-prewarm:v1:ban-ei:20260824",
  );
  expect(markerPut).toHaveBeenCalledWith(
    "control:running-style-foundation-prewarm:v1:ban-ei:20260824",
    expect.any(String),
    { expirationTtl: 900 },
  );
});

it("suppresses another forced prewarm while the category build marker is active", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0824",
        keibajo_code: "08",
        race_bango: "01",
        source: "jra",
      },
    ],
    source: "d1",
  });
  const fetch = vi.fn<typeof globalThis.fetch>(
    async (_input) => new Response(null, { status: 202 }),
  );
  const send = vi.fn(queueSendOk);
  const summary = await planRunningStylePredictionsForDate(
    buildEnv({
      DETAIL_SECTION_CACHE_KV: {
        get: vi.fn(async () => "2026-08-23T17:42:00.000Z"),
        put: vi.fn(async () => {}),
      } as unknown as KVNamespace,
      FEATURES_ARCHIVE: { head: vi.fn(async () => null) } as unknown as R2Bucket,
      FINISH_POSITION_CRON: { fetch },
      RUNNING_STYLE_JOBS: {
        metrics: vi.fn(queueMetricsOk),
        send,
        sendBatch: vi.fn(queueSendOk),
      },
    }),
    "20260824",
    new Date("2026-08-23T17:42:34.492Z"),
  );
  expect(summary.enqueued).toBe(0);
  expect(summary.planError).toBeUndefined();
  expect(fetch).not.toHaveBeenCalled();
  expect(send).not.toHaveBeenCalled();
});

it("fails closed and reports a rejected foundation prewarm", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0824",
        keibajo_code: "08",
        race_bango: "01",
        source: "jra",
      },
    ],
    source: "d1",
  });
  const fetch = vi.fn<typeof globalThis.fetch>(
    async (_input) => new Response(null, { status: 503 }),
  );
  const send = vi.fn(queueSendOk);
  vi.spyOn(console, "error").mockImplementation(() => {});
  const summary = await planRunningStylePredictionsForDate(
    buildEnv({
      FEATURES_ARCHIVE: { head: vi.fn(async () => null) } as unknown as R2Bucket,
      FINISH_POSITION_CRON: { fetch },
      RUNNING_STYLE_JOBS: {
        metrics: vi.fn(queueMetricsOk),
        send,
        sendBatch: vi.fn(queueSendOk),
      },
    }),
    "20260824",
    new Date("2026-08-23T17:42:34.492Z"),
  );
  expect(summary.enqueued).toBe(0);
  expect(summary.planError).toBe("Foundation prewarm failed for jra:20260824: HTTP 503");
  expect(send).not.toHaveBeenCalled();
});

it("recovers a foundation HEAD exception through prewarm without enqueueing the race", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0824",
        keibajo_code: "08",
        race_bango: "01",
        source: "jra",
      },
    ],
    source: "d1",
  });
  const fetch = vi.fn<typeof globalThis.fetch>(
    async (_input) => new Response(null, { status: 202 }),
  );
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  const summary = await planRunningStylePredictionsForDate(
    buildEnv({
      FEATURES_ARCHIVE: {
        head: vi.fn(async () => {
          throw new Error("R2 unavailable");
        }),
      } as unknown as R2Bucket,
      FINISH_POSITION_CRON: { fetch },
    }),
    "20260824",
    new Date("2026-08-23T17:42:34.492Z"),
  );
  expect(summary.enqueued).toBe(0);
  expect(summary.planError).toBeUndefined();
  expect(fetch).toHaveBeenCalledTimes(1);
  expect(errorSpy).toHaveBeenCalledTimes(1);
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
      predictedCornerFrontScore: 1,
      predictedCornerRank: 1,
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
      predictedCornerFrontScore: 1,
      predictedCornerRank: 1,
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
      predictedCornerFrontScore: 1,
      predictedCornerRank: 1,
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

it("planRunningStylePredictionsForDate does not infer a Neon gap from missing D1 rows", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { listRunningStyleInferenceStates } = await import("./running-style-d1");
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
  const summary = await planRunningStylePredictionsForDate(
    buildEnv(),
    "20260512",
    new Date("2026-05-12T12:00:00.000Z"),
  );
  expect(summary.completed).toBe(1);
  expect(summary.enqueued).toBe(0);
  expect(summary.scanned).toBe(1);
});

it("requeues an August 29 sync-failed mirror on August 28 even when D1 rows are complete", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const {
    listRaceRunningStyleCounts,
    listRunningStyleInferenceStates,
    upsertRunningStylePendingStates,
  } = await import("./running-style-d1");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0829",
        keibajo_code: "50",
        race_bango: "07",
        source: "nar",
      },
    ],
    source: "d1",
  });
  vi.mocked(listRunningStyleInferenceStates).mockResolvedValue(
    new Map([
      [
        "nar:20260829:50:07",
        {
          attemptedAt: "2026-08-28T11:00:00.000Z",
          cellModelKey: null,
          cellVariantId: null,
          completedAt: null,
          expectedHorseCount: 8,
          featuresR2Key: "features.parquet",
          modelVersion: "v7",
          raceKey: "nar:20260829:50:07",
          status: "sync-failed",
          writtenHorseCount: 8,
        },
      ],
    ]),
  );
  vi.mocked(listRaceRunningStyleCounts).mockResolvedValue(new Map([["nar:20260829:50:07", 8]]));
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
    "20260829",
    new Date("2026-08-28T12:00:00.000Z"),
  );
  expect(summary.completed).toBe(0);
  expect(summary.enqueued).toBe(1);
  expect(send).toHaveBeenCalledWith(
    expect.objectContaining({
      raceKey: "nar:20260829:50:07",
      type: "generate-running-style-predictions",
    }),
  );
  expect(upsertRunningStylePendingStates).not.toHaveBeenCalled();
});

it("planRunningStylePredictionsForDate does not requeue completed from an unrelated count map", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { listRaceRunningStyleCounts, listRunningStyleInferenceStates } =
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
  vi.mocked(listRaceRunningStyleCounts).mockResolvedValue(new Map([["jra:20260512:08:01", 15]]));
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
  expect(summary.completed).toBe(1);
  expect(summary.enqueued).toBe(0);
  expect(send).not.toHaveBeenCalled();
});

it("planRunningStylePredictionsForDate does not requeue a completed race with all expected rows", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { listRaceRunningStyleCounts, listRunningStyleInferenceStates } =
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
  vi.mocked(listRaceRunningStyleCounts).mockResolvedValueOnce(
    new Map([["jra:20260512:08:01", 16]]),
  );
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
  expect(summary.completed).toBe(1);
  expect(summary.enqueued).toBe(0);
  expect(send).not.toHaveBeenCalled();
});

it("planRunningStylePredictionsForDate fails closed for completed races when D1 counts timeout", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { listRaceRunningStyleCounts, listRunningStyleInferenceStates } =
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
  vi.mocked(listRaceRunningStyleCounts).mockRejectedValueOnce(new Error("D1 count timeout"));
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
  expect(summary.completed).toBe(1);
  expect(summary.enqueued).toBe(0);
  expect(summary.planError).toContain("D1 count timeout");
  expect(send).not.toHaveBeenCalled();
});

it("planRunningStylePredictionsForDate uses Catalog counts when only some races are completed", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { listRunningStyleInferenceStates } = await import("./running-style-d1");
  const { fetchRunningStyleFeatureCountsFromCatalog } =
    await import("./running-style-catalog-client");
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
  vi.mocked(fetchRunningStyleFeatureCountsFromCatalog).mockResolvedValue(
    new Map([
      ["jra:20260512:08:01", 16],
      ["jra:20260512:08:02", 16],
    ]),
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
  expect(fetchRunningStyleFeatureCountsFromCatalog).toHaveBeenCalledTimes(1);
});

it("planRunningStylePredictionsForDate falls back to individual sends after sendBatch error 15000", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { markRunningStyleInferenceEnqueueFailed } = await import("./running-style-d1");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0824",
        keibajo_code: "48",
        race_bango: "04",
        source: "nar",
      },
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0824",
        keibajo_code: "48",
        race_bango: "05",
        source: "nar",
      },
    ],
    source: "d1",
  });
  const send = vi.fn(queueSendOk);
  const sendBatch = vi.fn(async () => {
    throw new Error("Unknown Internal Error (15000)");
  });
  vi.spyOn(console, "error").mockImplementation(() => {});
  const summary = await planRunningStylePredictionsForDate(
    buildEnv({
      RUNNING_STYLE_JOBS: { metrics: vi.fn(queueMetricsOk), send, sendBatch },
    }),
    "20260824",
    new Date("2026-08-23T17:42:34.492Z"),
  );
  expect(summary.enqueued).toBe(2);
  expect(summary.planError).toBeUndefined();
  expect(sendBatch).toHaveBeenCalledTimes(1);
  expect(send).toHaveBeenCalledTimes(2);
  expect(markRunningStyleInferenceEnqueueFailed).toHaveBeenCalledWith(
    expect.anything(),
    [],
    "2026-08-23T17:42:34.492Z",
  );
});

it("planRunningStylePredictionsForDate restores only jobs that individual fallback cannot enqueue", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { markRunningStyleInferenceEnqueueFailed } = await import("./running-style-d1");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0824",
        keibajo_code: "83",
        race_bango: "01",
        source: "nar",
      },
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0824",
        keibajo_code: "83",
        race_bango: "02",
        source: "nar",
      },
    ],
    source: "d1",
  });
  const send = vi
    .fn()
    .mockResolvedValueOnce(queueSendOk())
    .mockRejectedValueOnce(new Error("individual send failed"));
  const sendBatch = vi.fn(async () => {
    throw new Error("Unknown Internal Error (15000)");
  });
  vi.spyOn(console, "error").mockImplementation(() => {});
  const summary = await planRunningStylePredictionsForDate(
    buildEnv({
      RUNNING_STYLE_JOBS: { metrics: vi.fn(queueMetricsOk), send, sendBatch },
    }),
    "20260824",
    new Date("2026-08-23T17:42:34.492Z"),
  );
  expect(summary.enqueued).toBe(1);
  expect(summary.planError).toBe(
    "Queue send failed for nar:20260824:83:02: individual send failed",
  );
  expect(markRunningStyleInferenceEnqueueFailed).toHaveBeenCalledWith(
    expect.anything(),
    [
      {
        error: expect.objectContaining({ message: "individual send failed" }),
        raceKey: "nar:20260824:83:02",
      },
    ],
    "2026-08-23T17:42:34.492Z",
  );
});

it("planRunningStylePredictionsForDate preserves sync-failed state when a single mirror retry cannot enqueue", async () => {
  const { planRunningStylePredictionsForDate } = await import("./running-style-cron");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { listRunningStyleInferenceStates, markRunningStyleInferenceEnqueueFailed } =
    await import("./running-style-d1");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0824",
        keibajo_code: "83",
        race_bango: "12",
        source: "nar",
      },
    ],
    source: "d1",
  });
  vi.mocked(listRunningStyleInferenceStates).mockResolvedValue(
    new Map([
      [
        "nar:20260824:83:12",
        {
          attemptedAt: "2026-08-23T17:30:00.000Z",
          cellModelKey: "running-style/models/nar/latest.flatbin",
          cellVariantId: "latest",
          completedAt: null,
          expectedHorseCount: 10,
          featuresR2Key: "features.parquet",
          modelVersion: "v3",
          raceKey: "nar:20260824:83:12",
          status: "sync-failed",
          writtenHorseCount: 10,
        },
      ],
    ]),
  );
  const send = vi.fn(async () => {
    throw new Error("individual send failed");
  });
  const summary = await planRunningStylePredictionsForDate(
    buildEnv({
      RUNNING_STYLE_JOBS: {
        metrics: vi.fn(queueMetricsOk),
        send,
        sendBatch: vi.fn(queueSendOk),
      },
    }),
    "20260824",
    new Date("2026-08-23T17:42:34.492Z"),
  );
  expect(summary.enqueued).toBe(0);
  expect(summary.planError).toBe(
    "Queue send failed for nar:20260824:83:12: individual send failed",
  );
  expect(markRunningStyleInferenceEnqueueFailed).toHaveBeenCalledWith(
    expect.anything(),
    [
      {
        error: expect.objectContaining({ message: "individual send failed" }),
        raceKey: "nar:20260824:83:12",
      },
    ],
    "2026-08-23T17:42:34.492Z",
  );
});
