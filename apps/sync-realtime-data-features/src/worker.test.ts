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
vi.mock("./features/race-trend", () => ({
  handleRaceTrend: vi.fn(async () => new Response('{"ok":true}', { status: 200 })),
}));
vi.mock("./running-style/inference", () => ({
  handleRunningStylePredictionJob: vi.fn(async () => ({ raceKey: "r", writtenCount: 0 })),
}));
vi.mock("./finish-position/inference", () => ({
  handleFinishPositionPredictionJob: vi.fn(async () => ({ predictionsCount: 0, raceKey: "r" })),
}));
vi.mock("./scheduled-race-list", async () => {
  const actual =
    await vi.importActual<typeof import("./scheduled-race-list")>("./scheduled-race-list");
  return {
    ...actual,
    listTodayRaceKeysFromHyperdrive: vi.fn(async () => []),
    listTomorrowRaceKeysFromHyperdrive: vi.fn(async () => []),
  };
});
vi.mock("./gates/adaptive-batch-kv", () => ({
  readNextBatchSize: vi.fn(async () => 5),
  recordRecomputeOutcome: vi.fn(async () => {}),
}));

import { buildRaceFeatures } from "./features/build";
import { encodeRaceFeaturesParquet } from "./features/parquet";
import { handleRaceTrend } from "./features/race-trend";
import { readNextBatchSize, recordRecomputeOutcome } from "./gates/adaptive-batch-kv";
import {
  listTodayRaceKeysFromHyperdrive,
  listTomorrowRaceKeysFromHyperdrive,
} from "./scheduled-race-list";
import {
  buildAndPersistRaceFeatures,
  handleFetchRequest,
  handleGetFinishPositions,
  handleGetRunningStyles,
  handleMigrationStateGet,
  handleMigrationStatePost,
  handleQueue,
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
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockReset();
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValue([]);
  vi.mocked(listTomorrowRaceKeysFromHyperdrive).mockReset();
  vi.mocked(listTomorrowRaceKeysFromHyperdrive).mockResolvedValue([]);
  vi.mocked(readNextBatchSize).mockReset();
  vi.mocked(readNextBatchSize).mockResolvedValue(5);
  vi.mocked(recordRecomputeOutcome).mockReset();
  vi.mocked(recordRecomputeOutcome).mockResolvedValue();
  vi.mocked(handleRaceTrend).mockReset();
  vi.mocked(handleRaceTrend).mockResolvedValue(new Response('{"ok":true}', { status: 200 }));
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

it("routes /api/features/race-trend to handleRaceTrend", async () => {
  const env = buildEnv();
  const response = await handleFetchRequest(
    env,
    new Request("https://x/api/features/race-trend?source=nar&keibajoCode=30&raceBango=08", {
      method: "GET",
    }),
  );
  expect(response.status).toBe(200);
  expect(handleRaceTrend).toHaveBeenCalledTimes(1);
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

it("accepts recompute request with raceKey-only body by parsing the 5-part string", async () => {
  const env = buildEnv();
  vi.mocked(buildRaceFeatures).mockResolvedValueOnce([]);
  const response = await handleRecomputeRequest(
    env,
    new Request("https://x/api/internal/recompute-and-build-parquet", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ raceKey: "nar:2026:0529:42:01" }),
    }),
  );
  expect(response.status).toBe(200);
  const callArg = vi.mocked(buildRaceFeatures).mock.calls[0]![0];
  expect(callArg.kaisaiNen).toBe("2026");
  expect(callArg.kaisaiTsukihi).toBe("0529");
  expect(callArg.keibajoCode).toBe("42");
  expect(callArg.raceBango).toBe("01");
  expect(callArg.source).toBe("nar");
  expect(env.FEATURES_ARCHIVE.put).toHaveBeenCalledWith(
    "features/by-race/2026/05/29/nar/42/01.parquet",
    new Uint8Array([1, 2, 3]),
  );
});

it("handleRecomputeRequest returns 500 JSON when buildAndPersistRaceFeatures throws", async () => {
  const env = buildEnv();
  vi.spyOn(console, "error").mockImplementation(() => {});
  vi.mocked(buildRaceFeatures).mockRejectedValueOnce(new Error("hyperdrive socket dead"));
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
  expect(response.status).toBe(500);
  await expect(response.json()).resolves.toStrictEqual({
    error: "hyperdrive socket dead",
    raceKey: "nar:20260529:30:08",
  });
});

it("handleRecomputeRequest returns 500 JSON stringifying non-Error throwables", async () => {
  const env = buildEnv();
  vi.spyOn(console, "error").mockImplementation(() => {});
  vi.mocked(buildRaceFeatures).mockRejectedValueOnce("plain string failure");
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
  expect(response.status).toBe(500);
  await expect(response.json()).resolves.toStrictEqual({
    error: "plain string failure",
    raceKey: "nar:20260529:30:08",
  });
});

it("rejects recompute request with malformed raceKey-only body", async () => {
  const env = buildEnv();
  const response = await handleRecomputeRequest(
    env,
    new Request("https://x/api/internal/recompute-and-build-parquet", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ raceKey: "garbage" }),
    }),
  );
  expect(response.status).toBe(400);
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
  ).resolves.toStrictEqual({
    batchSize: 0,
    enqueuedRaceCount: 0,
    past14Count: 0,
    ran: false,
    todayCount: 0,
    tomorrowCount: 0,
  });
});

it("runScheduledFeaturesPlan runs inside polling window with empty hyperdrive result", async () => {
  const env = buildEnv();
  await expect(
    runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z")),
  ).resolves.toStrictEqual({
    batchSize: 5,
    enqueuedRaceCount: 0,
    past14Count: 0,
    ran: true,
    todayCount: 0,
    tomorrowCount: 0,
  });
});

it("runScheduledFeaturesPlan enqueues today builds plus today inference jobs (batchSize=2 cap)", async () => {
  const queueSend = vi.fn(async () => {});
  vi.mocked(readNextBatchSize).mockResolvedValueOnce(2);
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      raceBango: "08",
      raceKey: "nar:2026:0529:30:08",
      source: "nar",
    },
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "08",
      raceBango: "01",
      raceKey: "jra:2026:0529:08:01",
      source: "jra",
    },
  ]);
  const env = buildEnv({
    REALTIME_FEATURES_JOBS: { send: queueSend } as unknown as Queue<Job>,
  });
  const result = await runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z"));
  expect(result.ran).toBe(true);
  expect(result.enqueuedRaceCount).toBe(2);
  expect(result.todayCount).toBe(2);
  expect(result.tomorrowCount).toBe(0);
  expect(result.past14Count).toBe(0);
  expect(result.batchSize).toBe(2);
  expect(queueSend).toHaveBeenCalledTimes(6);
});

it("runScheduledFeaturesPlan skips enqueue when lock is held for every candidate", async () => {
  const queueSend = vi.fn(async () => {});
  vi.mocked(readNextBatchSize).mockResolvedValueOnce(5);
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      raceBango: "08",
      raceKey: "nar:2026:0529:30:08",
      source: "nar",
    },
  ]);
  const kvGet = vi.fn().mockResolvedValue("1");
  const env = buildEnv({
    FEATURES_KV: {
      delete: vi.fn(),
      get: kvGet,
      put: vi.fn(),
    } as unknown as KVNamespace,
    REALTIME_FEATURES_JOBS: { send: queueSend } as unknown as Queue<Job>,
  });
  await runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z"));
  expect(queueSend).not.toHaveBeenCalled();
});

it("runScheduledFeaturesPlan enqueues build-race-features when no build-state KV", async () => {
  const queueSend = vi.fn(async () => {});
  vi.mocked(readNextBatchSize).mockResolvedValueOnce(1);
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      raceBango: "08",
      raceKey: "nar:2026:0529:30:08",
      source: "nar",
    },
  ]);
  const kvGet = vi.fn().mockResolvedValue(null);
  const env = buildEnv({
    FEATURES_KV: {
      delete: vi.fn(),
      get: kvGet,
      put: vi.fn(),
    } as unknown as KVNamespace,
    REALTIME_FEATURES_JOBS: { send: queueSend } as unknown as Queue<Job>,
  });
  await runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z"));
  expect(queueSend).toHaveBeenCalledWith({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
    raceKey: "nar:2026:0529:30:08",
    source: "nar",
    type: "build-race-features",
  });
});

it("runScheduledFeaturesPlan skips build-race-features when build-state fresh within 10 min", async () => {
  const queueSend = vi.fn(async () => {});
  vi.mocked(readNextBatchSize).mockResolvedValueOnce(1);
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      raceBango: "08",
      raceKey: "nar:2026:0529:30:08",
      source: "nar",
    },
  ]);
  const kvGet = vi
    .fn()
    .mockResolvedValueOnce(null)
    .mockResolvedValueOnce(
      JSON.stringify({ lastBuiltAt: "2026-05-29T02:55:00.000Z", rowCount: 12 }),
    )
    .mockResolvedValue("1");
  const env = buildEnv({
    FEATURES_KV: {
      delete: vi.fn(),
      get: kvGet,
      put: vi.fn(),
    } as unknown as KVNamespace,
    REALTIME_FEATURES_JOBS: { send: queueSend } as unknown as Queue<Job>,
  });
  await runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z"));
  expect(queueSend).not.toHaveBeenCalledWith({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
    raceKey: "nar:2026:0529:30:08",
    source: "nar",
    type: "build-race-features",
  });
});

it("runScheduledFeaturesPlan re-enqueues build-race-features when build-state older than 10 min", async () => {
  const queueSend = vi.fn(async () => {});
  vi.mocked(readNextBatchSize).mockResolvedValueOnce(1);
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      raceBango: "08",
      raceKey: "nar:2026:0529:30:08",
      source: "nar",
    },
  ]);
  const kvGet = vi
    .fn()
    .mockResolvedValueOnce(null)
    .mockResolvedValueOnce(
      JSON.stringify({ lastBuiltAt: "2026-05-29T02:40:00.000Z", rowCount: 12 }),
    )
    .mockResolvedValue(null);
  const env = buildEnv({
    FEATURES_KV: {
      delete: vi.fn(),
      get: kvGet,
      put: vi.fn(),
    } as unknown as KVNamespace,
    REALTIME_FEATURES_JOBS: { send: queueSend } as unknown as Queue<Job>,
  });
  await runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z"));
  expect(queueSend).toHaveBeenCalledWith({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
    raceKey: "nar:2026:0529:30:08",
    source: "nar",
    type: "build-race-features",
  });
});

it("runScheduledFeaturesPlan re-enqueues build-race-features when prior rowCount was 0", async () => {
  const queueSend = vi.fn(async () => {});
  vi.mocked(readNextBatchSize).mockResolvedValueOnce(1);
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      raceBango: "08",
      raceKey: "nar:2026:0529:30:08",
      source: "nar",
    },
  ]);
  const kvGet = vi
    .fn()
    .mockResolvedValueOnce(null)
    .mockResolvedValueOnce(JSON.stringify({ lastBuiltAt: "2026-05-29T02:59:00.000Z", rowCount: 0 }))
    .mockResolvedValue(null);
  const env = buildEnv({
    FEATURES_KV: {
      delete: vi.fn(),
      get: kvGet,
      put: vi.fn(),
    } as unknown as KVNamespace,
    REALTIME_FEATURES_JOBS: { send: queueSend } as unknown as Queue<Job>,
  });
  await runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z"));
  expect(queueSend).toHaveBeenCalledWith({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
    raceKey: "nar:2026:0529:30:08",
    source: "nar",
    type: "build-race-features",
  });
});

it("runScheduledFeaturesPlan skips build-race-features when build enqueue-lock active", async () => {
  const queueSend = vi.fn(async () => {});
  vi.mocked(readNextBatchSize).mockResolvedValueOnce(1);
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      raceBango: "08",
      raceKey: "nar:2026:0529:30:08",
      source: "nar",
    },
  ]);
  const kvGet = vi.fn().mockResolvedValueOnce("1").mockResolvedValue("1");
  const env = buildEnv({
    FEATURES_KV: {
      delete: vi.fn(),
      get: kvGet,
      put: vi.fn(),
    } as unknown as KVNamespace,
    REALTIME_FEATURES_JOBS: { send: queueSend } as unknown as Queue<Job>,
  });
  await runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z"));
  expect(queueSend).not.toHaveBeenCalledWith({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
    raceKey: "nar:2026:0529:30:08",
    source: "nar",
    type: "build-race-features",
  });
});

it("runScheduledFeaturesPlan enqueues tomorrow builds tracked in tomorrowCount", async () => {
  const queueSend = vi.fn(async () => {});
  vi.mocked(readNextBatchSize).mockResolvedValueOnce(1);
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([]);
  vi.mocked(listTomorrowRaceKeysFromHyperdrive).mockResolvedValueOnce([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0530",
      keibajoCode: "06",
      raceBango: "11",
      raceKey: "jra:2026:0530:06:11",
      source: "jra",
    },
  ]);
  const env = buildEnv({
    REALTIME_FEATURES_JOBS: { send: queueSend } as unknown as Queue<Job>,
  });
  const result = await runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z"));
  expect(result.tomorrowCount).toBe(1);
  expect(result.todayCount).toBe(0);
  expect(result.past14Count).toBe(0);
  expect(queueSend).toHaveBeenCalledWith({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0530",
    keibajoCode: "06",
    raceBango: "11",
    raceKey: "jra:2026:0530:06:11",
    source: "jra",
    type: "build-race-features",
  });
});

it("runScheduledFeaturesPlan enqueues past14 builds tracked in past14Count", async () => {
  const queueSend = vi.fn(async () => {});
  vi.mocked(readNextBatchSize).mockResolvedValueOnce(3);
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      raceBango: "08",
      raceKey: "nar:2026:0529:30:08",
      source: "nar",
    },
  ]);
  const env = buildEnv({
    REALTIME_FEATURES_JOBS: { send: queueSend } as unknown as Queue<Job>,
  });
  const result = await runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z"));
  expect(result.todayCount).toBe(1);
  expect(result.past14Count).toBe(2);
  expect(result.tomorrowCount).toBe(0);
  expect(result.enqueuedRaceCount).toBe(3);
});

it("runScheduledFeaturesPlan skips tomorrow when freshness 6h is current", async () => {
  const queueSend = vi.fn(async () => {});
  vi.mocked(readNextBatchSize).mockResolvedValueOnce(1);
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([]);
  vi.mocked(listTomorrowRaceKeysFromHyperdrive).mockResolvedValueOnce([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0530",
      keibajoCode: "06",
      raceBango: "11",
      raceKey: "jra:2026:0530:06:11",
      source: "jra",
    },
  ]);
  const kvGet = vi
    .fn()
    .mockResolvedValueOnce(null)
    .mockResolvedValueOnce(
      JSON.stringify({ lastBuiltAt: "2026-05-29T01:00:00.000Z", rowCount: 14 }),
    )
    .mockResolvedValue("1");
  const env = buildEnv({
    FEATURES_KV: {
      delete: vi.fn(),
      get: kvGet,
      put: vi.fn(),
    } as unknown as KVNamespace,
    REALTIME_FEATURES_JOBS: { send: queueSend } as unknown as Queue<Job>,
  });
  await runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z"));
  expect(queueSend).not.toHaveBeenCalledWith({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0530",
    keibajoCode: "06",
    raceBango: "11",
    raceKey: "jra:2026:0530:06:11",
    source: "jra",
    type: "build-race-features",
  });
});

it("runScheduledFeaturesPlan skips past14 builds when 7d freshness state present", async () => {
  const queueSend = vi.fn(async () => {});
  vi.mocked(readNextBatchSize).mockResolvedValueOnce(1);
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      raceBango: "08",
      raceKey: "nar:2026:0529:30:08",
      source: "nar",
    },
  ]);
  const kvGet = vi
    .fn()
    .mockResolvedValueOnce("1")
    .mockResolvedValueOnce(null)
    .mockResolvedValueOnce(
      JSON.stringify({ lastBuiltAt: "2026-05-28T00:00:00.000Z", rowCount: 14 }),
    )
    .mockResolvedValue("1");
  const env = buildEnv({
    FEATURES_KV: {
      delete: vi.fn(),
      get: kvGet,
      put: vi.fn(),
    } as unknown as KVNamespace,
    REALTIME_FEATURES_JOBS: { send: queueSend } as unknown as Queue<Job>,
  });
  await runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z"));
  expect(queueSend).not.toHaveBeenCalledWith({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0528",
    keibajoCode: "30",
    raceBango: "08",
    raceKey: "nar:2026:0528:30:08",
    source: "nar",
    type: "build-race-features",
  });
});

it("runScheduledFeaturesPlan honors adaptive batchSize cap of 0", async () => {
  const queueSend = vi.fn(async () => {});
  vi.mocked(readNextBatchSize).mockResolvedValueOnce(0);
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      raceBango: "08",
      raceKey: "nar:2026:0529:30:08",
      source: "nar",
    },
  ]);
  const env = buildEnv({
    REALTIME_FEATURES_JOBS: { send: queueSend } as unknown as Queue<Job>,
  });
  const result = await runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z"));
  expect(result.enqueuedRaceCount).toBe(0);
  expect(result.todayCount).toBe(0);
  expect(queueSend).not.toHaveBeenCalledWith({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
    raceKey: "nar:2026:0529:30:08",
    source: "nar",
    type: "build-race-features",
  });
});

it("shouldRebuildRaceFeatures returns true when state is null", async () => {
  const { shouldRebuildRaceFeatures } = await import("./worker");
  expect(shouldRebuildRaceFeatures(null, new Date("2026-05-29T03:00:00Z"))).toBe(true);
});

it("shouldRebuildRaceFeatures returns true when rowCount is 0", async () => {
  const { shouldRebuildRaceFeatures } = await import("./worker");
  expect(
    shouldRebuildRaceFeatures(
      { lastBuiltAt: "2026-05-29T02:59:00.000Z", rowCount: 0 },
      new Date("2026-05-29T03:00:00Z"),
    ),
  ).toBe(true);
});

it("shouldRebuildRaceFeatures returns true when lastBuiltAt is not parseable", async () => {
  const { shouldRebuildRaceFeatures } = await import("./worker");
  expect(
    shouldRebuildRaceFeatures(
      { lastBuiltAt: "not-a-date", rowCount: 5 },
      new Date("2026-05-29T03:00:00Z"),
    ),
  ).toBe(true);
});

it("shouldRebuildRaceFeatures returns false when fresh within 10 min", async () => {
  const { shouldRebuildRaceFeatures } = await import("./worker");
  expect(
    shouldRebuildRaceFeatures(
      { lastBuiltAt: "2026-05-29T02:55:00.000Z", rowCount: 5 },
      new Date("2026-05-29T03:00:00Z"),
    ),
  ).toBe(false);
});

it("shouldRebuildRaceFeatures returns true when last build older than 10 min", async () => {
  const { shouldRebuildRaceFeatures } = await import("./worker");
  expect(
    shouldRebuildRaceFeatures(
      { lastBuiltAt: "2026-05-29T02:40:00.000Z", rowCount: 5 },
      new Date("2026-05-29T03:00:00Z"),
    ),
  ).toBe(true);
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

it("handleQueue records success outcome after build-race-features job", async () => {
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
  expect(recordRecomputeOutcome).toHaveBeenCalledWith(env, true);
});

it("handleQueue records failure outcome and rethrows when build-race-features throws", async () => {
  const env = buildEnv();
  vi.mocked(buildRaceFeatures).mockRejectedValueOnce(new Error("hyperdrive down"));
  const message = buildMessage({
    type: "build-race-features",
    raceKey: "r",
    source: "nar",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
  });
  await expect(
    handleQueue({ messages: [message] } as unknown as MessageBatch<Job>, env),
  ).rejects.toThrow("hyperdrive down");
  expect(recordRecomputeOutcome).toHaveBeenCalledWith(env, false);
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
