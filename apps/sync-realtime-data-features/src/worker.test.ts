// Run with: bun run --filter sync-realtime-data-features test
import { beforeEach, expect, it, vi } from "vitest";

vi.mock("./features/build", () => ({
  buildRaceFeatures: vi.fn(async () => []),
  fetchAllRaceFeatures: vi.fn(async () => []),
}));
vi.mock("./features/parquet", () => ({
  encodeRaceFeaturesParquet: vi.fn(async () => new Uint8Array([1, 2, 3])),
  decodeRaceFeaturesParquet: vi.fn(async () => []),
}));
vi.mock("./running-style/inference", () => ({
  handleRunningStylePredictionJob: vi.fn(async () => ({ raceKey: "r", writtenCount: 0 })),
}));
vi.mock("./finish-position/inference", () => ({
  handleFinishPositionPredictionJob: vi.fn(async () => ({ predictionsCount: 0, raceKey: "r" })),
}));

import { buildRaceFeatures } from "./features/build";
import { encodeRaceFeaturesParquet } from "./features/parquet";
import {
  buildAndPersistRaceFeatures,
  handleFetchRequest,
  handleGetFinishPositions,
  handleGetRunningStyles,
  handleMigrationStateGet,
  handleMigrationStatePost,
  handleQueue,
  handleRaceTrendStub,
  handleRecomputeRequest,
  handleRoot,
  handleScheduled,
  runScheduledFeaturesPlan,
} from "./worker";
import type { DailyRaceEntryRow, Env, Job } from "./types";

const buildDb = (firstResult: unknown = null, allResults: unknown[] = []) => {
  const run = vi.fn().mockResolvedValue({});
  const all = vi.fn().mockResolvedValue({ results: allResults });
  const first = vi.fn().mockResolvedValue(firstResult);
  const bind = vi.fn(() => ({ run, all, first }));
  const prepare = vi.fn(() => ({ bind }));
  return { prepare } as unknown as D1Database;
};

const buildKv = () => {
  const kv = {
    get: vi.fn().mockResolvedValue(null),
    put: vi.fn().mockResolvedValue(undefined),
    delete: vi.fn().mockResolvedValue(undefined),
  };
  return kv as unknown as KVNamespace;
};

const buildR2 = () =>
  ({
    put: vi.fn().mockResolvedValue(undefined),
  }) as unknown as R2Bucket;

const buildEnv = (overrides: Partial<Env> = {}): Env =>
  ({
    REALTIME_FEATURES_DB: buildDb(),
    FEATURES_KV: buildKv(),
    FEATURES_ARCHIVE: buildR2(),
    PC_KEIBA_VIEWER_INTERNAL_TOKEN: "secret",
    ...overrides,
  }) as unknown as Env;

beforeEach(() => {
  vi.unstubAllGlobals();
  vi.mocked(buildRaceFeatures).mockReset();
  vi.mocked(buildRaceFeatures).mockResolvedValue([]);
  vi.mocked(encodeRaceFeaturesParquet).mockReset();
  vi.mocked(encodeRaceFeaturesParquet).mockResolvedValue(new Uint8Array([1, 2, 3]));
});

it("handleRoot returns ok payload", async () => {
  const response = handleRoot();
  await expect(response.json()).resolves.toStrictEqual({
    name: "sync-realtime-data-features",
    ok: true,
  });
});

it("handleFetchRequest routes root", async () => {
  const env = buildEnv();
  const response = await handleFetchRequest(env, new Request("https://x/", { method: "GET" }));
  expect(response.status).toBe(200);
});

it("returns 404 for unknown path", async () => {
  const env = buildEnv();
  const response = await handleFetchRequest(
    env,
    new Request("https://x/api/unknown", { method: "GET" }),
  );
  expect(response.status).toBe(404);
});

it("handleRaceTrendStub returns empty aggregate", async () => {
  const response = handleRaceTrendStub();
  await expect(response.json()).resolves.toStrictEqual({
    byJockey: {},
    byWaku: {},
    raceCount: 0,
    starterCount: 0,
  });
});

it("routes /api/features/race-trend to stub", async () => {
  const env = buildEnv();
  const response = await handleFetchRequest(
    env,
    new Request("https://x/api/features/race-trend", { method: "GET" }),
  );
  expect(response.status).toBe(200);
});

it("returns 400 when race_key missing on running-styles", async () => {
  const env = buildEnv();
  const response = await handleFetchRequest(
    env,
    new Request("https://x/api/running-styles", { method: "GET" }),
  );
  expect(response.status).toBe(400);
});

it("returns rows when race_key present", async () => {
  const env = buildEnv();
  const response = await handleFetchRequest(
    env,
    new Request("https://x/api/running-styles?race_key=r", { method: "GET" }),
  );
  expect(response.status).toBe(200);
});

it("handleGetRunningStyles serialises rows and state", async () => {
  const env = buildEnv();
  const response = await handleGetRunningStyles(env, "r");
  await expect(response.json()).resolves.toStrictEqual({ rows: [], state: null });
});

it("returns 400 when race_key missing on finish-positions", async () => {
  const env = buildEnv();
  const response = await handleFetchRequest(
    env,
    new Request("https://x/api/finish-positions", { method: "GET" }),
  );
  expect(response.status).toBe(400);
});

it("handleGetFinishPositions returns null row when miss", async () => {
  const env = buildEnv();
  const response = await handleGetFinishPositions(env, "r");
  await expect(response.json()).resolves.toStrictEqual({ row: null });
});

it("routes /api/finish-positions when race_key present", async () => {
  const env = buildEnv();
  const response = await handleFetchRequest(
    env,
    new Request("https://x/api/finish-positions?race_key=r", { method: "GET" }),
  );
  expect(response.status).toBe(200);
});

it("rejects unauthorized migration-state POST", async () => {
  const env = buildEnv({ PC_KEIBA_VIEWER_INTERNAL_TOKEN: undefined });
  const response = await handleMigrationStatePost(
    env,
    new Request("https://x/api/internal/migration-state", { method: "POST" }),
  );
  expect(response.status).toBe(401);
});

it("rejects migration-state POST when token does not match", async () => {
  const env = buildEnv();
  const response = await handleMigrationStatePost(
    env,
    new Request("https://x/api/internal/migration-state", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "wrong" },
    }),
  );
  expect(response.status).toBe(401);
});

it("accepts migration-state POST with valid token", async () => {
  const env = buildEnv();
  const response = await handleMigrationStatePost(
    env,
    new Request("https://x/api/internal/migration-state", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ key: "k", value: "v" }),
    }),
  );
  expect(response.status).toBe(200);
  expect(env.FEATURES_KV.put).toHaveBeenCalledWith("features:migration:k", "v");
});

it("rejects unauthorized migration-state GET", async () => {
  const env = buildEnv({ PC_KEIBA_VIEWER_INTERNAL_TOKEN: undefined });
  const response = await handleMigrationStateGet(
    env,
    new Request("https://x/api/internal/migration-state?key=k"),
  );
  expect(response.status).toBe(401);
});

it("returns 400 when migration-state GET key missing", async () => {
  const env = buildEnv();
  const response = await handleMigrationStateGet(
    env,
    new Request("https://x/api/internal/migration-state", {
      method: "GET",
      headers: { "x-pc-keiba-internal-token": "secret" },
    }),
  );
  expect(response.status).toBe(400);
});

it("returns key/value on migration-state GET", async () => {
  const env = buildEnv();
  (env.FEATURES_KV.get as ReturnType<typeof vi.fn>).mockResolvedValueOnce("value-1");
  const response = await handleMigrationStateGet(
    env,
    new Request("https://x/api/internal/migration-state?key=k", {
      method: "GET",
      headers: { "x-pc-keiba-internal-token": "secret" },
    }),
  );
  await expect(response.json()).resolves.toStrictEqual({ key: "k", value: "value-1" });
});

it("rejects unauthorized recompute request", async () => {
  const env = buildEnv({ PC_KEIBA_VIEWER_INTERNAL_TOKEN: undefined });
  const response = await handleRecomputeRequest(
    env,
    new Request("https://x/api/internal/recompute-and-build-parquet", { method: "POST" }),
  );
  expect(response.status).toBe(401);
});

it("accepts recompute request, builds Parquet, PUTs to R2, and writes KV", async () => {
  const env = buildEnv();
  const rows: DailyRaceEntryRow[] = [];
  vi.mocked(buildRaceFeatures).mockResolvedValueOnce(rows);
  const response = await handleRecomputeRequest(
    env,
    new Request("https://x/api/internal/recompute-and-build-parquet", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({
        raceKey: "nar:20260529:30:08",
        source: "nar",
        kaisaiNen: "2026",
        kaisaiTsukihi: "0529",
        keibajoCode: "30",
        raceBango: "08",
      }),
    }),
  );
  expect(response.status).toBe(200);
  expect(env.FEATURES_ARCHIVE.put).toHaveBeenCalledWith(
    "features/by-race/2026/05/29/nar/30/08.parquet",
    new Uint8Array([1, 2, 3]),
  );
  const body = (await response.json()) as {
    raceKey: string;
    rowCount: number;
    r2Key: string;
    builtAt: string;
  };
  expect(body.raceKey).toBe("nar:20260529:30:08");
  expect(body.rowCount).toBe(0);
  expect(body.r2Key).toBe("features/by-race/2026/05/29/nar/30/08.parquet");
  expect(typeof body.builtAt).toBe("string");
});

it("buildAndPersistRaceFeatures writes build-state KV and latest features KV", async () => {
  const env = buildEnv();
  vi.mocked(buildRaceFeatures).mockResolvedValueOnce([]);
  const result = await buildAndPersistRaceFeatures(env, {
    raceKey: "nar:20260529:30:08",
    source: "nar",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
  });
  expect(result.rowCount).toBe(0);
  expect(result.r2Key).toBe("features/by-race/2026/05/29/nar/30/08.parquet");
  expect(env.FEATURES_KV.put).toHaveBeenCalledWith(
    "features:build-state:nar:20260529:30:08",
    expect.any(String),
    { expirationTtl: 86_400 },
  );
  expect(env.FEATURES_KV.put).toHaveBeenCalledWith("features:latest:nar:20260529:30:08", "[]", {
    expirationTtl: 600,
  });
});

it("routes POST /api/internal/recompute-and-build-parquet", async () => {
  const env = buildEnv();
  const response = await handleFetchRequest(
    env,
    new Request("https://x/api/internal/recompute-and-build-parquet", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({
        raceKey: "r",
        source: "nar",
        kaisaiNen: "2026",
        kaisaiTsukihi: "0529",
        keibajoCode: "30",
        raceBango: "08",
      }),
    }),
  );
  expect(response.status).toBe(200);
});

it("routes POST /api/internal/migration-state", async () => {
  const env = buildEnv();
  const response = await handleFetchRequest(
    env,
    new Request("https://x/api/internal/migration-state", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ key: "k", value: "v" }),
    }),
  );
  expect(response.status).toBe(200);
});

it("routes GET /api/internal/migration-state", async () => {
  const env = buildEnv();
  const response = await handleFetchRequest(
    env,
    new Request("https://x/api/internal/migration-state?key=k", {
      method: "GET",
      headers: { "x-pc-keiba-internal-token": "secret" },
    }),
  );
  expect(response.status).toBe(200);
});

it("runScheduledFeaturesPlan skips outside polling window", async () => {
  const env = buildEnv();
  await expect(
    runScheduledFeaturesPlan(env, new Date("2026-05-29T20:00:00Z")),
  ).resolves.toStrictEqual({ enqueuedRaceCount: 0, ran: false });
});

it("runScheduledFeaturesPlan runs inside polling window without REALTIME_OLD", async () => {
  const env = buildEnv();
  await expect(
    runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z")),
  ).resolves.toStrictEqual({ enqueuedRaceCount: 0, ran: true });
});

it("runScheduledFeaturesPlan enqueues per-race jobs from REALTIME_OLD response", async () => {
  const queueSend = vi.fn(async () => {});
  const oldFetch = vi.fn(
    async () =>
      new Response(
        JSON.stringify({
          rows: [{ race_key: "nar:2026:0529:30:08" }, { race_key: "jra:2026:0529:08:01" }],
        }),
      ),
  );
  const env = buildEnv({
    REALTIME_FEATURES_JOBS: { send: queueSend } as unknown as Queue<Job>,
    REALTIME_OLD: { fetch: oldFetch } as never,
    REALTIME_OLD_ADMIN_TOKEN: "old-secret",
  });
  const result = await runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z"));
  expect(result.ran).toBe(true);
  expect(result.enqueuedRaceCount).toBe(2);
  expect(queueSend).toHaveBeenCalledTimes(4);
});

it("runScheduledFeaturesPlan skips enqueue when lock is held", async () => {
  const queueSend = vi.fn(async () => {});
  const oldFetch = vi.fn(
    async () => new Response(JSON.stringify({ rows: [{ race_key: "nar:2026:0529:30:08" }] })),
  );
  const kvGet = vi.fn().mockResolvedValue("1");
  const env = buildEnv({
    FEATURES_KV: {
      delete: vi.fn(),
      get: kvGet,
      put: vi.fn(),
    } as unknown as KVNamespace,
    REALTIME_FEATURES_JOBS: { send: queueSend } as unknown as Queue<Job>,
    REALTIME_OLD: { fetch: oldFetch } as never,
    REALTIME_OLD_ADMIN_TOKEN: "old-secret",
  });
  await runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z"));
  expect(queueSend).not.toHaveBeenCalled();
});

it("runScheduledFeaturesPlan ignores invalid race_keys", async () => {
  const queueSend = vi.fn(async () => {});
  const oldFetch = vi.fn(
    async () =>
      new Response(JSON.stringify({ rows: [{ race_key: "garbage" }, { race_key: "x:y:z:w:v" }] })),
  );
  const env = buildEnv({
    REALTIME_FEATURES_JOBS: { send: queueSend } as unknown as Queue<Job>,
    REALTIME_OLD: { fetch: oldFetch } as never,
    REALTIME_OLD_ADMIN_TOKEN: "old-secret",
  });
  const result = await runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z"));
  expect(result.enqueuedRaceCount).toBe(0);
  expect(queueSend).not.toHaveBeenCalled();
});

it("runScheduledFeaturesPlan returns empty when REALTIME_OLD response is not ok", async () => {
  const queueSend = vi.fn(async () => {});
  const oldFetch = vi.fn(async () => new Response("", { status: 500 }));
  const env = buildEnv({
    REALTIME_FEATURES_JOBS: { send: queueSend } as unknown as Queue<Job>,
    REALTIME_OLD: { fetch: oldFetch } as never,
    REALTIME_OLD_ADMIN_TOKEN: "old-secret",
  });
  const result = await runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z"));
  expect(result.enqueuedRaceCount).toBe(0);
});

it("handleScheduled dispatches scheduled tick", async () => {
  const env = buildEnv();
  await expect(
    handleScheduled({ scheduledTime: Date.parse("2026-05-29T20:00:00Z") } as ScheduledEvent, env),
  ).resolves.toBeUndefined();
});

const buildMessage = (job: Job) => ({
  body: job,
  ack: vi.fn(),
  retry: vi.fn(),
  id: "id",
  timestamp: new Date(),
  attempts: 1,
});

it("handleQueue dispatches build-race-features job by building Parquet and PUT to R2", async () => {
  const env = buildEnv();
  vi.mocked(buildRaceFeatures).mockResolvedValueOnce([]);
  const message = buildMessage({
    type: "build-race-features",
    raceKey: "r",
    source: "nar",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
  });
  await handleQueue({ messages: [message] } as unknown as MessageBatch<Job>, env);
  expect(message.ack).toHaveBeenCalled();
  expect(env.FEATURES_ARCHIVE.put).toHaveBeenCalledWith(
    "features/by-race/2026/05/29/nar/30/08.parquet",
    new Uint8Array([1, 2, 3]),
  );
  expect(env.FEATURES_KV.put).toHaveBeenCalledWith("features:build-state:r", expect.any(String), {
    expirationTtl: 86_400,
  });
});

it("handleQueue dispatches predict-running-style job", async () => {
  const env = buildEnv();
  const message = buildMessage({
    type: "predict-running-style",
    raceKey: "r",
    source: "nar",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
    predictedAt: "t",
  });
  await handleQueue({ messages: [message] } as unknown as MessageBatch<Job>, env);
  expect(message.ack).toHaveBeenCalled();
});

it("handleQueue dispatches predict-finish-position job", async () => {
  const env = buildEnv();
  const message = buildMessage({
    type: "predict-finish-position",
    raceKey: "r",
    source: "nar",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
    predictedAt: "t",
  });
  await handleQueue({ messages: [message] } as unknown as MessageBatch<Job>, env);
  expect(message.ack).toHaveBeenCalled();
});

it("handleQueue acknowledges unknown job types", async () => {
  const env = buildEnv();
  const message = buildMessage({ type: "archive-features-to-r2", date: "20260529" });
  await handleQueue({ messages: [message] } as unknown as MessageBatch<Job>, env);
  expect(message.ack).toHaveBeenCalled();
});

it("default worker.fetch dispatches handleFetchRequest", async () => {
  const { default: worker } = await import("./worker");
  const env = buildEnv();
  const response = await worker.fetch(new Request("https://x/", { method: "GET" }), env);
  expect(response.status).toBe(200);
});

it("default worker.scheduled dispatches handleScheduled", async () => {
  const { default: worker } = await import("./worker");
  const env = buildEnv();
  await expect(
    worker.scheduled({ scheduledTime: Date.parse("2026-05-29T20:00:00Z") } as ScheduledEvent, env),
  ).resolves.toBeUndefined();
});

it("default worker.queue dispatches handleQueue", async () => {
  const { default: worker } = await import("./worker");
  const env = buildEnv();
  const message = buildMessage({ type: "archive-features-to-r2", date: "20260529" });
  await worker.queue({ messages: [message] } as unknown as MessageBatch<Job>, env);
  expect(message.ack).toHaveBeenCalled();
});
