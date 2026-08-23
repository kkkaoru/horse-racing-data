// Run with: bun run --filter sync-realtime-data-features test
import { beforeEach, expect, it, vi } from "vitest";

vi.mock("./admin-predict-for-day", () => ({
  runPredictionsForDay: vi.fn(async () => ({
    enqueuedFinishPosition: [],
    enqueuedRunningStyle: [],
    skippedReasons: [],
  })),
}));
vi.mock("./features/build", () => ({
  buildRaceFeatures: vi.fn(async () => []),
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
    listTodayRaceKeysWithKvCache: vi.fn(async () => []),
    listTomorrowRaceKeysWithKvCache: vi.fn(async () => []),
  };
});
vi.mock("./gates/adaptive-batch-kv", () => ({
  readNextBatchSize: vi.fn(async () => 5),
  recordRecomputeOutcome: vi.fn(async () => {}),
}));

import { runPredictionsForDay } from "./admin-predict-for-day";
import { buildRaceFeatures } from "./features/build";
import { encodeRaceFeaturesParquet } from "./features/parquet";
import { handleRaceTrend } from "./features/race-trend";
import { handleFinishPositionPredictionJob } from "./finish-position/inference";
import { readNextBatchSize, recordRecomputeOutcome } from "./gates/adaptive-batch-kv";
import { handleRunningStylePredictionJob } from "./running-style/inference";
import {
  listTodayRaceKeysWithKvCache,
  listTomorrowRaceKeysWithKvCache,
} from "./scheduled-race-list";
import {
  buildAndPersistRaceFeatures,
  computeNextPast14Cursor,
  handleEnqueueRecomputeRequest,
  handleFetchRequest,
  handleGetFinishPositions,
  handleGetRunningStyles,
  handleMigrationStateGet,
  handleMigrationStatePost,
  handlePredictFinishPositionRequest,
  handlePredictForDayRequest,
  handlePredictRunningStyleRequest,
  handleQueue,
  handleRecomputeRequest,
  handleRoot,
  handleScheduled,
  runScheduledFeaturesPlan,
  sliceWithWraparound,
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
    REALTIME_FEATURES_JOBS: { send: vi.fn().mockResolvedValue(undefined) },
    ...overrides,
  }) as unknown as Env;

// KV mock whose `get` resolves per-key from `valueByKey`, falling back to
// null (miss) for everything else — needed once a test must tell the
// enqueue-lock key apart from the build-state key for the same race.
const buildKeyedKv = (valueByKey: Record<string, string>) => {
  const kv = buildKv();
  (kv.get as ReturnType<typeof vi.fn>).mockImplementation(async (key: string) =>
    Object.hasOwn(valueByKey, key) ? valueByKey[key] : null,
  );
  return kv;
};

const buildDailyRow = (overrides: Partial<DailyRaceEntryRow> = {}): DailyRaceEntryRow => ({
  babajotai_code_dirt: null,
  babajotai_code_shiba: null,
  bamei: null,
  banushimei: null,
  barei: null,
  bataiju: null,
  chokyoshimei_ryakusho: null,
  corner1_norm: null,
  corner2_norm: null,
  corner3_norm: null,
  corner4_norm: null,
  corner_1: null,
  corner_2: null,
  corner_3: null,
  corner_4: null,
  finish_norm: null,
  finish_position: null,
  futan_juryo: null,
  grade_code: null,
  hasso_jikoku: null,
  juryo_shubetsu_code: null,
  kaisai_nen: "2026",
  kaisai_tsukihi: "0529",
  keibajo_code: "30",
  ketto_toroku_bango: "2020100099",
  kishumei_ryakusho: null,
  kohan_3f: null,
  kyori: null,
  kyoso_joken_code: null,
  kyoso_shubetsu_code: null,
  race_bango: "08",
  race_date: "2026-05-29",
  race_name: null,
  seibetsu_code: null,
  shusso_tosu: null,
  soha_time: null,
  source: "nar",
  tansho_ninkijun: null,
  tansho_odds: null,
  time_sa: null,
  track_code: null,
  umaban: 1,
  wakuban: null,
  zogen_fugo: null,
  zogen_sa: null,
  ...overrides,
});

beforeEach(() => {
  vi.unstubAllGlobals();
  vi.mocked(buildRaceFeatures).mockReset();
  vi.mocked(buildRaceFeatures).mockResolvedValue([]);
  vi.mocked(encodeRaceFeaturesParquet).mockReset();
  vi.mocked(encodeRaceFeaturesParquet).mockResolvedValue(new Uint8Array([1, 2, 3]));
  vi.mocked(listTodayRaceKeysWithKvCache).mockReset();
  vi.mocked(listTodayRaceKeysWithKvCache).mockResolvedValue([]);
  vi.mocked(listTomorrowRaceKeysWithKvCache).mockReset();
  vi.mocked(listTomorrowRaceKeysWithKvCache).mockResolvedValue([]);
  vi.mocked(readNextBatchSize).mockReset();
  vi.mocked(readNextBatchSize).mockResolvedValue(5);
  vi.mocked(recordRecomputeOutcome).mockReset();
  vi.mocked(recordRecomputeOutcome).mockResolvedValue();
  vi.mocked(handleRaceTrend).mockReset();
  vi.mocked(handleRaceTrend).mockResolvedValue(new Response('{"ok":true}', { status: 200 }));
  vi.mocked(handleRunningStylePredictionJob).mockReset();
  vi.mocked(handleRunningStylePredictionJob).mockResolvedValue({ raceKey: "r", writtenCount: 0 });
  vi.mocked(handleFinishPositionPredictionJob).mockReset();
  vi.mocked(handleFinishPositionPredictionJob).mockResolvedValue({
    predictionsCount: 0,
    raceKey: "r",
  });
  vi.mocked(runPredictionsForDay).mockReset();
  vi.mocked(runPredictionsForDay).mockResolvedValue({
    enqueuedFinishPosition: [],
    enqueuedRunningStyle: [],
    skippedReasons: [],
  });
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

it("rejects an unauthorized enqueue-recompute request", async () => {
  const env = buildEnv({ PC_KEIBA_VIEWER_INTERNAL_TOKEN: undefined });
  const response = await handleEnqueueRecomputeRequest(
    env,
    new Request("https://x/api/internal/enqueue-recompute", { method: "POST" }),
  );
  expect(response.status).toBe(401);
});

it("enqueues recompute work and responds before building features", async () => {
  const env = buildEnv();
  const response = await handleEnqueueRecomputeRequest(
    env,
    new Request("https://x/api/internal/enqueue-recompute", {
      body: JSON.stringify({ raceKey: "nar:2026:0823:35:02" }),
      headers: { "x-pc-keiba-internal-token": "secret" },
      method: "POST",
    }),
  );

  expect(response.status).toBe(202);
  await expect(response.json()).resolves.toStrictEqual({
    queued: true,
    raceKey: "nar:2026:0823:35:02",
  });
  expect(env.REALTIME_FEATURES_JOBS.send).toHaveBeenCalledWith({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0823",
    keibajoCode: "35",
    raceBango: "02",
    raceKey: "nar:2026:0823:35:02",
    source: "nar",
    type: "build-race-features",
  });
  expect(env.FEATURES_KV.put).toHaveBeenCalledWith(
    "features:enqueue-lock:build-race-features:nar:2026:0823:35:02",
    "1",
    { expirationTtl: 60 },
  );
  expect(buildRaceFeatures).not.toHaveBeenCalled();
});

it("acknowledges a duplicate enqueue-recompute request without sending another job", async () => {
  const env = buildEnv({
    FEATURES_KV: buildKeyedKv({
      "features:enqueue-lock:build-race-features:nar:2026:0823:35:02": "1",
    }),
  });
  const response = await handleEnqueueRecomputeRequest(
    env,
    new Request("https://x/api/internal/enqueue-recompute", {
      body: JSON.stringify({ raceKey: "nar:2026:0823:35:02" }),
      headers: { "x-pc-keiba-internal-token": "secret" },
      method: "POST",
    }),
  );

  expect(response.status).toBe(202);
  await expect(response.json()).resolves.toStrictEqual({
    queued: false,
    raceKey: "nar:2026:0823:35:02",
    reason: "enqueue-locked",
  });
  expect(env.REALTIME_FEATURES_JOBS.send).not.toHaveBeenCalled();
});

it("returns 400 when enqueue-recompute receives a malformed race key", async () => {
  const env = buildEnv();
  const response = await handleEnqueueRecomputeRequest(
    env,
    new Request("https://x/api/internal/enqueue-recompute", {
      body: JSON.stringify({ raceKey: "invalid" }),
      headers: { "x-pc-keiba-internal-token": "secret" },
      method: "POST",
    }),
  );
  expect(response.status).toBe(400);
  expect(env.REALTIME_FEATURES_JOBS.send).not.toHaveBeenCalled();
});

it("returns 500 when enqueue-recompute cannot durably send the queue job", async () => {
  const env = buildEnv();
  vi.spyOn(console, "error").mockImplementation(() => {});
  vi.mocked(env.REALTIME_FEATURES_JOBS.send).mockRejectedValue(new Error("queue unavailable"));
  const response = await handleEnqueueRecomputeRequest(
    env,
    new Request("https://x/api/internal/enqueue-recompute", {
      body: JSON.stringify({ raceKey: "nar:2026:0823:35:02" }),
      headers: { "x-pc-keiba-internal-token": "secret" },
      method: "POST",
    }),
  );

  expect(response.status).toBe(500);
  await expect(response.json()).resolves.toStrictEqual({
    error: "queue unavailable",
    raceKey: "nar:2026:0823:35:02",
  });
  expect(env.FEATURES_KV.put).not.toHaveBeenCalled();
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
    "features/catalog-v1/by-race/2026/05/29/nar/30/08.parquet",
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
  expect(body.r2Key).toBe("features/catalog-v1/by-race/2026/05/29/nar/30/08.parquet");
  expect(typeof body.builtAt).toBe("string");
});

it("accepts recompute request for Ban'ei (keibajoCode=83) and forwards the raw job key", async () => {
  // Ban'ei routing: build.ts resolveScope maps source="nar" + keibajoCode="83"
  // to the "ban-ei" SQL scope so the `<> '83'` filter does not strip the rows.
  // worker.test mocks build.ts, so this test only asserts the keibajoCode is
  // preserved end-to-end through handleRecomputeRequest into buildRaceFeatures.
  const env = buildEnv();
  vi.mocked(buildRaceFeatures).mockResolvedValueOnce([]);
  const response = await handleRecomputeRequest(
    env,
    new Request("https://x/api/internal/recompute-and-build-parquet", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ raceKey: "nar:2026:0530:83:01" }),
    }),
  );
  expect(response.status).toBe(200);
  const callArg = vi.mocked(buildRaceFeatures).mock.calls[0]![0];
  expect(callArg.source).toBe("nar");
  expect(callArg.keibajoCode).toBe("83");
  expect(callArg.kaisaiNen).toBe("2026");
  expect(callArg.kaisaiTsukihi).toBe("0530");
  expect(callArg.raceBango).toBe("01");
  expect(env.FEATURES_ARCHIVE.put).toHaveBeenCalledWith(
    "features/catalog-v1/by-race/2026/05/30/nar/83/01.parquet",
    new Uint8Array([1, 2, 3]),
  );
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
    "features/catalog-v1/by-race/2026/05/29/nar/42/01.parquet",
    new Uint8Array([1, 2, 3]),
  );
});

it("handleRecomputeRequest returns 500 JSON when buildAndPersistRaceFeatures throws", async () => {
  const env = buildEnv();
  vi.spyOn(console, "error").mockImplementation(() => {});
  vi.mocked(buildRaceFeatures).mockRejectedValueOnce(new Error("catalog request failed"));
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
    error: "catalog request failed",
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

it("predict-running-style returns 401 without token", async () => {
  const env = buildEnv({ PC_KEIBA_VIEWER_INTERNAL_TOKEN: undefined });
  const response = await handlePredictRunningStyleRequest(
    env,
    new Request("https://x/api/internal/predict-running-style", { method: "POST" }),
  );
  expect(response.status).toBe(401);
});

it("predict-running-style returns 400 for invalid body", async () => {
  const env = buildEnv();
  const response = await handlePredictRunningStyleRequest(
    env,
    new Request("https://x/api/internal/predict-running-style", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ raceKey: "garbage" }),
    }),
  );
  expect(response.status).toBe(400);
});

it("predict-running-style invokes handler and returns 200 with writtenCount", async () => {
  const env = buildEnv();
  vi.mocked(handleRunningStylePredictionJob).mockResolvedValueOnce({
    raceKey: "nar:2026:0529:30:08",
    writtenCount: 12,
  });
  const response = await handlePredictRunningStyleRequest(
    env,
    new Request("https://x/api/internal/predict-running-style", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ raceKey: "nar:2026:0529:30:08" }),
    }),
  );
  expect(response.status).toBe(200);
  await expect(response.json()).resolves.toStrictEqual({
    raceKey: "nar:2026:0529:30:08",
    writtenCount: 12,
  });
  expect(handleRunningStylePredictionJob).toHaveBeenCalledTimes(1);
});

it("predict-running-style returns 500 when handler throws", async () => {
  const env = buildEnv();
  vi.spyOn(console, "error").mockImplementation(() => {});
  vi.mocked(handleRunningStylePredictionJob).mockRejectedValueOnce(new Error("r2 fetch failed"));
  const response = await handlePredictRunningStyleRequest(
    env,
    new Request("https://x/api/internal/predict-running-style", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ raceKey: "nar:2026:0529:30:08" }),
    }),
  );
  expect(response.status).toBe(500);
  await expect(response.json()).resolves.toStrictEqual({
    error: "r2 fetch failed",
    raceKey: "nar:2026:0529:30:08",
  });
});

it("predict-running-style returns 500 stringifying non-Error throwables", async () => {
  const env = buildEnv();
  vi.spyOn(console, "error").mockImplementation(() => {});
  vi.mocked(handleRunningStylePredictionJob).mockRejectedValueOnce("plain fail");
  const response = await handlePredictRunningStyleRequest(
    env,
    new Request("https://x/api/internal/predict-running-style", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ raceKey: "nar:2026:0529:30:08" }),
    }),
  );
  expect(response.status).toBe(500);
  await expect(response.json()).resolves.toStrictEqual({
    error: "plain fail",
    raceKey: "nar:2026:0529:30:08",
  });
});

it("predict-finish-position returns 401 without token", async () => {
  const env = buildEnv({ PC_KEIBA_VIEWER_INTERNAL_TOKEN: undefined });
  const response = await handlePredictFinishPositionRequest(
    env,
    new Request("https://x/api/internal/predict-finish-position", { method: "POST" }),
  );
  expect(response.status).toBe(401);
});

it("predict-finish-position returns 400 for invalid body", async () => {
  const env = buildEnv();
  const response = await handlePredictFinishPositionRequest(
    env,
    new Request("https://x/api/internal/predict-finish-position", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ raceKey: "garbage" }),
    }),
  );
  expect(response.status).toBe(400);
});

it("predict-finish-position invokes handler and returns 200 with predictionsCount", async () => {
  const env = buildEnv();
  vi.mocked(handleFinishPositionPredictionJob).mockResolvedValueOnce({
    predictionsCount: 14,
    raceKey: "nar:2026:0529:30:08",
  });
  const response = await handlePredictFinishPositionRequest(
    env,
    new Request("https://x/api/internal/predict-finish-position", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ raceKey: "nar:2026:0529:30:08" }),
    }),
  );
  expect(response.status).toBe(200);
  await expect(response.json()).resolves.toStrictEqual({
    predictionsCount: 14,
    raceKey: "nar:2026:0529:30:08",
  });
  expect(handleFinishPositionPredictionJob).toHaveBeenCalledTimes(1);
});

it("predict-finish-position returns 500 when handler throws", async () => {
  const env = buildEnv();
  vi.spyOn(console, "error").mockImplementation(() => {});
  vi.mocked(handleFinishPositionPredictionJob).mockRejectedValueOnce(new Error("d1 down"));
  const response = await handlePredictFinishPositionRequest(
    env,
    new Request("https://x/api/internal/predict-finish-position", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ raceKey: "nar:2026:0529:30:08" }),
    }),
  );
  expect(response.status).toBe(500);
  await expect(response.json()).resolves.toStrictEqual({
    error: "d1 down",
    raceKey: "nar:2026:0529:30:08",
  });
});

it("predict-finish-position returns 500 stringifying non-Error throwables", async () => {
  const env = buildEnv();
  vi.spyOn(console, "error").mockImplementation(() => {});
  vi.mocked(handleFinishPositionPredictionJob).mockRejectedValueOnce("plain fail");
  const response = await handlePredictFinishPositionRequest(
    env,
    new Request("https://x/api/internal/predict-finish-position", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ raceKey: "nar:2026:0529:30:08" }),
    }),
  );
  expect(response.status).toBe(500);
  await expect(response.json()).resolves.toStrictEqual({
    error: "plain fail",
    raceKey: "nar:2026:0529:30:08",
  });
});

it("predict-for-day returns 401 without token", async () => {
  const env = buildEnv({ PC_KEIBA_VIEWER_INTERNAL_TOKEN: undefined });
  const response = await handlePredictForDayRequest(
    env,
    new Request("https://x/api/internal/predict-for-day", { method: "POST" }),
  );
  expect(response.status).toBe(401);
});

it("predict-for-day returns 401 when token does not match", async () => {
  const env = buildEnv();
  const response = await handlePredictForDayRequest(
    env,
    new Request("https://x/api/internal/predict-for-day", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "wrong" },
      body: JSON.stringify({ source: "all", targetYmd: "20260531" }),
    }),
  );
  expect(response.status).toBe(401);
});

it("predict-for-day returns 400 when source is missing", async () => {
  const env = buildEnv();
  const response = await handlePredictForDayRequest(
    env,
    new Request("https://x/api/internal/predict-for-day", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ targetYmd: "20260531" }),
    }),
  );
  expect(response.status).toBe(400);
});

it("predict-for-day returns 400 when source is not in the enum", async () => {
  const env = buildEnv();
  const response = await handlePredictForDayRequest(
    env,
    new Request("https://x/api/internal/predict-for-day", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ source: "banei", targetYmd: "20260531" }),
    }),
  );
  expect(response.status).toBe(400);
});

it("predict-for-day returns 400 when targetYmd is not 8 digits", async () => {
  const env = buildEnv();
  const response = await handlePredictForDayRequest(
    env,
    new Request("https://x/api/internal/predict-for-day", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ source: "all", targetYmd: "2026-05-31" }),
    }),
  );
  expect(response.status).toBe(400);
});

it("predict-for-day returns 400 when body is not an object", async () => {
  const env = buildEnv();
  const response = await handlePredictForDayRequest(
    env,
    new Request("https://x/api/internal/predict-for-day", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify("garbage"),
    }),
  );
  expect(response.status).toBe(400);
});

it("predict-for-day source=jra returns 200 with helper result", async () => {
  const env = buildEnv();
  vi.mocked(runPredictionsForDay).mockResolvedValueOnce({
    enqueuedFinishPosition: ["jra:2026:0531:05:01"],
    enqueuedRunningStyle: ["jra:2026:0531:05:01"],
    skippedReasons: [],
  });
  const response = await handlePredictForDayRequest(
    env,
    new Request("https://x/api/internal/predict-for-day", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ source: "jra", targetYmd: "20260531" }),
    }),
  );
  expect(response.status).toBe(200);
  await expect(response.json()).resolves.toStrictEqual({
    enqueuedFinishPosition: ["jra:2026:0531:05:01"],
    enqueuedRunningStyle: ["jra:2026:0531:05:01"],
    skippedReasons: [],
  });
  expect(runPredictionsForDay).toHaveBeenCalledWith(env, {
    skipCompleted: false,
    source: "jra",
    targetYmd: "20260531",
  });
});

it("predict-for-day source=nar normalises and forwards to helper", async () => {
  const env = buildEnv();
  const response = await handlePredictForDayRequest(
    env,
    new Request("https://x/api/internal/predict-for-day", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ source: "nar", targetYmd: "20260601" }),
    }),
  );
  expect(response.status).toBe(200);
  expect(runPredictionsForDay).toHaveBeenCalledWith(env, {
    skipCompleted: false,
    source: "nar",
    targetYmd: "20260601",
  });
});

it("predict-for-day source=all normalises and forwards to helper", async () => {
  const env = buildEnv();
  const response = await handlePredictForDayRequest(
    env,
    new Request("https://x/api/internal/predict-for-day", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ source: "all", targetYmd: "20260531" }),
    }),
  );
  expect(response.status).toBe(200);
  expect(runPredictionsForDay).toHaveBeenCalledWith(env, {
    skipCompleted: false,
    source: "all",
    targetYmd: "20260531",
  });
});

it("predict-for-day forwards skipCompleted=true when present in body", async () => {
  const env = buildEnv();
  const response = await handlePredictForDayRequest(
    env,
    new Request("https://x/api/internal/predict-for-day", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ skipCompleted: true, source: "all", targetYmd: "20260531" }),
    }),
  );
  expect(response.status).toBe(200);
  expect(runPredictionsForDay).toHaveBeenCalledWith(env, {
    skipCompleted: true,
    source: "all",
    targetYmd: "20260531",
  });
});

it("predict-for-day forwards skipCompleted=false when present in body", async () => {
  const env = buildEnv();
  const response = await handlePredictForDayRequest(
    env,
    new Request("https://x/api/internal/predict-for-day", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ skipCompleted: false, source: "jra", targetYmd: "20260531" }),
    }),
  );
  expect(response.status).toBe(200);
  expect(runPredictionsForDay).toHaveBeenCalledWith(env, {
    skipCompleted: false,
    source: "jra",
    targetYmd: "20260531",
  });
});

it("predict-for-day defaults skipCompleted=false when absent", async () => {
  const env = buildEnv();
  const response = await handlePredictForDayRequest(
    env,
    new Request("https://x/api/internal/predict-for-day", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ source: "nar", targetYmd: "20260601" }),
    }),
  );
  expect(response.status).toBe(200);
  expect(runPredictionsForDay).toHaveBeenCalledWith(env, {
    skipCompleted: false,
    source: "nar",
    targetYmd: "20260601",
  });
});

it("predict-for-day defaults skipCompleted=false when value is non-boolean", async () => {
  const env = buildEnv();
  const response = await handlePredictForDayRequest(
    env,
    new Request("https://x/api/internal/predict-for-day", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ skipCompleted: "yes", source: "all", targetYmd: "20260531" }),
    }),
  );
  expect(response.status).toBe(200);
  expect(runPredictionsForDay).toHaveBeenCalledWith(env, {
    skipCompleted: false,
    source: "all",
    targetYmd: "20260531",
  });
});

it("predict-for-day returns 200 with skip-aware result envelope", async () => {
  const env = buildEnv();
  vi.mocked(runPredictionsForDay).mockResolvedValueOnce({
    enqueuedFinishPosition: ["jra:2026:0531:05:01"],
    enqueuedRunningStyle: ["jra:2026:0531:05:01"],
    skippedReasons: [{ raceKey: "jra:2026:0531:05:02", reason: "already-completed" }],
  });
  const response = await handlePredictForDayRequest(
    env,
    new Request("https://x/api/internal/predict-for-day", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ skipCompleted: true, source: "jra", targetYmd: "20260531" }),
    }),
  );
  expect(response.status).toBe(200);
  await expect(response.json()).resolves.toStrictEqual({
    enqueuedFinishPosition: ["jra:2026:0531:05:01"],
    enqueuedRunningStyle: ["jra:2026:0531:05:01"],
    skippedReasons: [{ raceKey: "jra:2026:0531:05:02", reason: "already-completed" }],
  });
});

it("predict-for-day returns 500 when runPredictionsForDay throws", async () => {
  const env = buildEnv();
  vi.spyOn(console, "error").mockImplementation(() => {});
  vi.mocked(runPredictionsForDay).mockRejectedValueOnce(new Error("catalog unavailable"));
  const response = await handlePredictForDayRequest(
    env,
    new Request("https://x/api/internal/predict-for-day", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ source: "all", targetYmd: "20260531" }),
    }),
  );
  expect(response.status).toBe(500);
  await expect(response.json()).resolves.toStrictEqual({
    error: "catalog unavailable",
    targetYmd: "20260531",
  });
});

it("predict-for-day returns 500 stringifying non-Error throwables", async () => {
  const env = buildEnv();
  vi.spyOn(console, "error").mockImplementation(() => {});
  vi.mocked(runPredictionsForDay).mockRejectedValueOnce("plain fail");
  const response = await handlePredictForDayRequest(
    env,
    new Request("https://x/api/internal/predict-for-day", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ source: "all", targetYmd: "20260531" }),
    }),
  );
  expect(response.status).toBe(500);
  await expect(response.json()).resolves.toStrictEqual({
    error: "plain fail",
    targetYmd: "20260531",
  });
});

it("routes POST /api/internal/predict-for-day", async () => {
  const env = buildEnv();
  const response = await handleFetchRequest(
    env,
    new Request("https://x/api/internal/predict-for-day", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ source: "all", targetYmd: "20260531" }),
    }),
  );
  expect(response.status).toBe(200);
});

it("routes POST /api/internal/predict-running-style", async () => {
  const env = buildEnv();
  const response = await handleFetchRequest(
    env,
    new Request("https://x/api/internal/predict-running-style", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ raceKey: "nar:2026:0529:30:08" }),
    }),
  );
  expect(response.status).toBe(200);
});

it("routes POST /api/internal/predict-finish-position", async () => {
  const env = buildEnv();
  const response = await handleFetchRequest(
    env,
    new Request("https://x/api/internal/predict-finish-position", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ raceKey: "nar:2026:0529:30:08" }),
    }),
  );
  expect(response.status).toBe(200);
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
  expect(result.r2Key).toBe("features/catalog-v1/by-race/2026/05/29/nar/30/08.parquet");
  expect(env.FEATURES_KV.put).toHaveBeenCalledWith(
    "features:build-state:catalog-v1:nar:20260529:30:08",
    expect.any(String),
    { expirationTtl: 86_400 },
  );
  expect(env.FEATURES_KV.put).toHaveBeenCalledWith(
    "features:latest:catalog-v1:nar:20260529:30:08",
    "[]",
    { expirationTtl: 600 },
  );
});

it("buildAndPersistRaceFeatures counts only rows with a resolved finish_position into rankedRowCount", async () => {
  const env = buildEnv();
  vi.mocked(buildRaceFeatures).mockResolvedValueOnce([
    buildDailyRow({ finish_position: 1, umaban: 1 }),
    buildDailyRow({ finish_position: 2, umaban: 2 }),
    buildDailyRow({ finish_position: null, umaban: 3 }),
  ]);
  await buildAndPersistRaceFeatures(env, {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "30",
    raceBango: "08",
    raceKey: "nar:2026:0529:30:08",
    source: "nar",
  });
  expect(env.FEATURES_KV.put).toHaveBeenCalledWith(
    "features:build-state:catalog-v1:nar:2026:0529:30:08",
    expect.stringContaining('"rankedRowCount":2'),
    { expirationTtl: 86_400 },
  );
  expect(env.FEATURES_KV.put).toHaveBeenCalledWith(
    "features:build-state:catalog-v1:nar:2026:0529:30:08",
    expect.stringContaining('"rowCount":3'),
    { expirationTtl: 86_400 },
  );
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

it("routes POST /api/internal/enqueue-recompute", async () => {
  const env = buildEnv();
  const response = await handleFetchRequest(
    env,
    new Request("https://x/api/internal/enqueue-recompute", {
      method: "POST",
      headers: { "x-pc-keiba-internal-token": "secret" },
      body: JSON.stringify({ raceKey: "nar:2026:0529:30:08" }),
    }),
  );
  expect(response.status).toBe(202);
  expect(env.REALTIME_FEATURES_JOBS.send).toHaveBeenCalledTimes(1);
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

it("runScheduledFeaturesPlan runs inside polling window with empty catalog result", async () => {
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
  vi.mocked(listTodayRaceKeysWithKvCache).mockResolvedValueOnce([
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
  // past14-starvation fix: past14 now has its own reserved quota (25) that is
  // NOT shared with the adaptive batchSize (2) above. These 2 today keys
  // explode into 2 tuples x 14 past days = 28 past14 candidates, capped to
  // the reserved quota of 25 (all unlocked/no-state under the default KV
  // mock, so all 25 get enqueued). Previously past14Count was 0 here because
  // today alone exhausted the shared batchSize=2.
  expect(result.enqueuedRaceCount).toBe(27);
  expect(result.todayCount).toBe(2);
  expect(result.tomorrowCount).toBe(0);
  expect(result.past14Count).toBe(25);
  expect(result.batchSize).toBe(2);
  expect(queueSend).toHaveBeenCalledTimes(31);
});

it("runScheduledFeaturesPlan skips enqueue when lock is held for every candidate", async () => {
  const queueSend = vi.fn(async () => {});
  vi.mocked(readNextBatchSize).mockResolvedValueOnce(5);
  vi.mocked(listTodayRaceKeysWithKvCache).mockResolvedValueOnce([
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
  vi.mocked(listTodayRaceKeysWithKvCache).mockResolvedValueOnce([
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
  vi.mocked(listTodayRaceKeysWithKvCache).mockResolvedValueOnce([
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
  vi.mocked(listTodayRaceKeysWithKvCache).mockResolvedValueOnce([
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
  vi.mocked(listTodayRaceKeysWithKvCache).mockResolvedValueOnce([
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
  vi.mocked(listTodayRaceKeysWithKvCache).mockResolvedValueOnce([
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
  vi.mocked(listTodayRaceKeysWithKvCache).mockResolvedValueOnce([]);
  vi.mocked(listTomorrowRaceKeysWithKvCache).mockResolvedValueOnce([
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
  // past14-starvation fix: past14 has its own reserved quota independent of
  // the batchSize=1 that tomorrow's single build already exhausted. The
  // tomorrow tuple explodes into 14 past14 candidates, all enqueued under the
  // default (unlocked/no-state) KV mock. Previously this was 0 because
  // past14 shared the same already-exhausted batchSize as tomorrow.
  expect(result.past14Count).toBe(14);
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
  vi.mocked(listTodayRaceKeysWithKvCache).mockResolvedValueOnce([
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
  // past14-starvation fix: past14 has its own reserved quota (25), separate
  // from the batchSize=3 above. The single today tuple explodes into 14
  // past14 candidates, all enqueued under the default (unlocked/no-state) KV
  // mock. Previously past14Count was 2 because past14 shared the leftover of
  // the same batchSize=3 that today's single candidate had already consumed.
  expect(result.todayCount).toBe(1);
  expect(result.past14Count).toBe(14);
  expect(result.tomorrowCount).toBe(0);
  expect(result.enqueuedRaceCount).toBe(15);
});

it("runScheduledFeaturesPlan includes a venue that raced yesterday but is absent from today/tomorrow in past14 targets", async () => {
  const queueSend = vi.fn(async () => {});
  vi.mocked(readNextBatchSize).mockResolvedValueOnce(1);
  vi.mocked(listTodayRaceKeysWithKvCache)
    .mockResolvedValueOnce([
      {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0529",
        keibajoCode: "30",
        raceBango: "08",
        raceKey: "nar:2026:0529:30:08",
        source: "nar",
      },
    ])
    .mockResolvedValueOnce([
      {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0528",
        keibajoCode: "44",
        raceBango: "03",
        raceKey: "nar:2026:0528:44:03",
        source: "nar",
      },
    ]);
  vi.mocked(listTomorrowRaceKeysWithKvCache).mockResolvedValueOnce([]);
  const env = buildEnv({
    REALTIME_FEATURES_JOBS: { send: queueSend } as unknown as Queue<Job>,
  });
  const result = await runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z"));
  // Venue 44/03 raced yesterday (2026-05-28) but has no race today or
  // tomorrow, so it would never have become a past14 rebuild candidate before
  // this fix. Its own tuple now explodes across the past-14-day window
  // (2026-05-15..2026-05-28) same as today's tuple, giving 2 tuples * 14 = 28
  // raw candidates capped to the reserved quota of 25.
  expect(result.past14Count).toBe(25);
  expect(queueSend).toHaveBeenCalledWith({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0528",
    keibajoCode: "44",
    raceBango: "03",
    raceKey: "nar:2026:0528:44:03",
    source: "nar",
    type: "build-race-features",
  });
});

it("runScheduledFeaturesPlan skips tomorrow when freshness 6h is current", async () => {
  const queueSend = vi.fn(async () => {});
  vi.mocked(readNextBatchSize).mockResolvedValueOnce(1);
  vi.mocked(listTodayRaceKeysWithKvCache).mockResolvedValueOnce([]);
  vi.mocked(listTomorrowRaceKeysWithKvCache).mockResolvedValueOnce([
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

it("runScheduledFeaturesPlan skips a past14 candidate whose build was recorded after race day end with ranked results", async () => {
  const queueSend = vi.fn(async () => {});
  vi.mocked(readNextBatchSize).mockResolvedValueOnce(1);
  vi.mocked(listTodayRaceKeysWithKvCache).mockResolvedValueOnce([
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
    FEATURES_KV: buildKeyedKv({
      "features:build-state:catalog-v1:nar:2026:0528:30:08": JSON.stringify({
        lastBuiltAt: "2026-05-29T01:00:00.000Z",
        rankedRowCount: 14,
        rowCount: 14,
      }),
    }),
    REALTIME_FEATURES_JOBS: { send: queueSend } as unknown as Queue<Job>,
  });
  const result = await runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z"));
  // The single today tuple explodes into 14 past14 candidates spanning
  // 2026-05-15..2026-05-28. Only 2026-05-28's build-state satisfies the new
  // past14 freshness predicate (recorded AFTER its race day ended, with
  // ranked rows), so it alone is skipped; the other 13 have no KV state and
  // get enqueued.
  expect(result.todayCount).toBe(1);
  expect(result.past14Count).toBe(13);
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

it("runScheduledFeaturesPlan still rebuilds a past14 candidate whose build was recorded during race day despite rowCount > 0 (past14 starvation regression)", async () => {
  const queueSend = vi.fn(async () => {});
  vi.mocked(readNextBatchSize).mockResolvedValueOnce(1);
  vi.mocked(listTodayRaceKeysWithKvCache).mockResolvedValueOnce([
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
    FEATURES_KV: buildKeyedKv({
      "features:build-state:catalog-v1:nar:2026:0528:30:08": JSON.stringify({
        lastBuiltAt: "2026-05-28T08:41:00.000Z",
        rowCount: 14,
      }),
    }),
    REALTIME_FEATURES_JOBS: { send: queueSend } as unknown as Queue<Job>,
  });
  const result = await runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z"));
  // Before this fix, isBuildStateFresh would have treated this record (built
  // during race day, well within the old 7-day freshness window) as fresh
  // forever, so the race's Parquet would never be rebuilt with finish
  // positions. The new predicate requires both a post-race-day-end
  // lastBuiltAt AND ranked rows, so this candidate correctly rebuilds.
  expect(result.past14Count).toBe(14);
  expect(queueSend).toHaveBeenCalledWith({
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
  vi.mocked(listTodayRaceKeysWithKvCache).mockResolvedValueOnce([
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
  // The adaptive batchSize cap of 0 only governs the today/tomorrow loop
  // (guard `counts.total(0) >= batchSize(0)` trips immediately, so today's
  // candidate lock/state is never even checked) — this part is unchanged.
  // past14 is decoupled from that batchSize (intentional: past14 must make
  // guaranteed progress regardless of today/tomorrow congestion), so its 14
  // candidates (1 tuple) still get processed and enqueued via the default
  // (unlocked/no-state) KV mock.
  expect(result.enqueuedRaceCount).toBe(14);
  expect(result.past14Count).toBe(14);
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

it("runScheduledFeaturesPlan no longer starves past14 when today+tomorrow alone exceed the reserved past14 quota", async () => {
  const queueSend = vi.fn(async () => {});
  vi.mocked(readNextBatchSize).mockResolvedValueOnce(1);
  vi.mocked(listTodayRaceKeysWithKvCache).mockResolvedValueOnce([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      raceBango: "01",
      raceKey: "nar:2026:0529:30:01",
      source: "nar",
    },
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      raceBango: "02",
      raceKey: "nar:2026:0529:30:02",
      source: "nar",
    },
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0529",
      keibajoCode: "30",
      raceBango: "03",
      raceKey: "nar:2026:0529:30:03",
      source: "nar",
    },
  ]);
  vi.mocked(listTomorrowRaceKeysWithKvCache).mockResolvedValueOnce([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0530",
      keibajoCode: "42",
      raceBango: "01",
      raceKey: "nar:2026:0530:42:01",
      source: "nar",
    },
  ]);
  const env = buildEnv({
    REALTIME_FEATURES_JOBS: { send: queueSend } as unknown as Queue<Job>,
  });
  const result = await runScheduledFeaturesPlan(env, new Date("2026-05-29T03:00:00Z"));
  // batchSize=1 is fully consumed by the today/tomorrow loop: 4 combined
  // candidates (3 today + 1 tomorrow) far exceed batchSize=1, so only the
  // very first candidate is enqueued and the loop stops there.
  expect(result.todayCount).toBe(1);
  expect(result.tomorrowCount).toBe(0);
  expect(result.batchSize).toBe(1);
  // This is the core regression check: past14 no longer shares that
  // exhausted batchSize. It has its own reserved quota (25), so it still
  // makes guaranteed progress even though today+tomorrow already consumed
  // the entire shared batchSize. The 4 venue tuples (3 today + 1 tomorrow)
  // explode to 4*14=56 past14 candidates, capped to the reserved quota.
  // Before the fix this was always 0 in this scenario.
  expect(result.past14Count).toBe(25);
});

it("sliceWithWraparound returns [] for an empty list", async () => {
  expect(sliceWithWraparound([], 0, 5)).toStrictEqual([]);
});

it("sliceWithWraparound returns the first N items in order when cursor is 0", async () => {
  expect(sliceWithWraparound(["a", "b", "c", "d", "e"], 0, 3)).toStrictEqual(["a", "b", "c"]);
});

it("sliceWithWraparound wraps mid-list returning the tail then the head with no duplicates", async () => {
  expect(sliceWithWraparound(["a", "b", "c", "d", "e"], 3, 3)).toStrictEqual(["d", "e", "a"]);
});

it("sliceWithWraparound with quota >= length returns all items exactly once starting at the normalized cursor", async () => {
  const result = sliceWithWraparound(["a", "b", "c", "d", "e"], 2, 20);
  expect(result).toStrictEqual(["c", "d", "e", "a", "b"]);
  expect(result.length).toBe(5);
});

it("sliceWithWraparound normalizes a negative cursor correctly", async () => {
  expect(sliceWithWraparound(["a", "b", "c"], -1, 2)).toStrictEqual(["c", "a"]);
});

it("computeNextPast14Cursor returns 0 when totalCount is 0", async () => {
  expect(computeNextPast14Cursor(5, 25, 0)).toBe(0);
});

it("computeNextPast14Cursor wraps via modulo in the normal case", async () => {
  expect(computeNextPast14Cursor(3, 5, 10)).toBe(8);
});

it("computeNextPast14Cursor wraps to 0 when cursor + quota exactly equals totalCount", async () => {
  expect(computeNextPast14Cursor(10, 15, 25)).toBe(0);
});

it("handleScheduled dispatches scheduled tick", async () => {
  const env = buildEnv();
  await expect(
    handleScheduled({ scheduledTime: Date.parse("2026-05-29T20:00:00Z") } as ScheduledEvent, env),
  ).resolves.toBeUndefined();
});

it("handleScheduled dispatches to runPredictionsForDay when event.cron matches PREDICT_FOR_DAY_CRON", async () => {
  const env = buildEnv();
  await handleScheduled(
    {
      cron: "5 2,6,10,14 * * *",
      scheduledTime: Date.parse("2026-05-31T02:05:00Z"),
    } as ScheduledEvent,
    env,
  );
  expect(runPredictionsForDay).toHaveBeenCalledTimes(1);
});

it("handleScheduled calls runPredictionsForDay with source=all and skipCompleted=true", async () => {
  const env = buildEnv();
  await handleScheduled(
    {
      cron: "5 2,6,10,14 * * *",
      scheduledTime: Date.parse("2026-05-31T02:05:00Z"),
    } as ScheduledEvent,
    env,
  );
  expect(runPredictionsForDay).toHaveBeenCalledWith(env, {
    skipCompleted: true,
    source: "all",
    targetYmd: "20260531",
  });
});

it("handleScheduled passes today YMD in JST when scheduledTime is in UTC", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-05-30T17:05:00Z"));
  const env = buildEnv();
  await handleScheduled(
    {
      cron: "5 2,6,10,14 * * *",
      scheduledTime: Date.parse("2026-05-30T17:05:00Z"),
    } as ScheduledEvent,
    env,
  );
  expect(runPredictionsForDay).toHaveBeenCalledWith(env, {
    skipCompleted: true,
    source: "all",
    targetYmd: "20260531",
  });
  vi.useRealTimers();
});

it("handleScheduled dispatches to runScheduledFeaturesPlan for unrecognized cron", async () => {
  const env = buildEnv();
  await handleScheduled(
    {
      cron: "*/10 * * * *",
      scheduledTime: Date.parse("2026-05-29T20:00:00Z"),
    } as ScheduledEvent,
    env,
  );
  expect(runPredictionsForDay).not.toHaveBeenCalled();
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
    "features/catalog-v1/by-race/2026/05/29/nar/30/08.parquet",
    new Uint8Array([1, 2, 3]),
  );
  expect(env.FEATURES_KV.put).toHaveBeenCalledWith(
    "features:build-state:catalog-v1:r",
    expect.any(String),
    { expirationTtl: 86_400 },
  );
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
  vi.mocked(buildRaceFeatures).mockRejectedValueOnce(new Error("catalog down"));
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
  ).rejects.toThrow("catalog down");
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
