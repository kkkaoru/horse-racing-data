// Run with bun. Fetch / scheduled / queue handlers for sync-realtime-data-features.

import { buildRaceFeatures } from "./features/build";
import { encodeRaceFeaturesParquet } from "./features/parquet";
import { buildRaceParquetR2Key } from "./features/r2-key";
import { putBuildStateToKv } from "./gates/build-state-kv";
import { writeLatestFeaturesToKv } from "./gates/latest-features-kv-mirror";
import { shouldRunFeaturesCron } from "./gates/polling-window-gate";
import { jsonResponse } from "./http";
import {
  getFinishPositionPredictions,
  getRunningStyleInferenceState,
  listRaceRunningStyles,
  upsertFinishPositionPredictions,
} from "./storage";
import type { DailyRaceEntryRow, Env, Job, RaceJobKey } from "./types";

const MIGRATION_STATE_KV_PREFIX = "features:migration";

interface MigrationStateRequest {
  key: string;
  value: string;
}

const isAuthorizedInternalRequest = (request: Request, env: Env): boolean => {
  const token = env.PC_KEIBA_VIEWER_INTERNAL_TOKEN;
  if (!token) {
    return false;
  }
  return request.headers.get("x-pc-keiba-internal-token") === token;
};

export const handleRoot = (): Response =>
  jsonResponse({ name: "sync-realtime-data-features", ok: true });

export const handleGetRunningStyles = async (env: Env, raceKey: string): Promise<Response> => {
  const [rows, state] = await Promise.all([
    listRaceRunningStyles(env.REALTIME_FEATURES_DB, raceKey),
    getRunningStyleInferenceState(env.REALTIME_FEATURES_DB, raceKey),
  ]);
  return jsonResponse({ rows, state });
};

export const handleGetFinishPositions = async (env: Env, raceKey: string): Promise<Response> => {
  const row = await getFinishPositionPredictions(env.REALTIME_FEATURES_DB, raceKey);
  return jsonResponse({ row });
};

export const handleRaceTrendStub = (): Response =>
  jsonResponse({
    byJockey: {},
    byWaku: {},
    raceCount: 0,
    starterCount: 0,
  });

export const handleMigrationStatePost = async (env: Env, request: Request): Promise<Response> => {
  if (!isAuthorizedInternalRequest(request, env)) {
    return jsonResponse({ error: "unauthorized" }, { status: 401 });
  }
  const body = (await request.json()) as MigrationStateRequest;
  await env.FEATURES_KV.put(`${MIGRATION_STATE_KV_PREFIX}:${body.key}`, body.value);
  return jsonResponse({ ok: true });
};

export const handleMigrationStateGet = async (env: Env, request: Request): Promise<Response> => {
  if (!isAuthorizedInternalRequest(request, env)) {
    return jsonResponse({ error: "unauthorized" }, { status: 401 });
  }
  const url = new URL(request.url);
  const key = url.searchParams.get("key");
  if (!key) {
    return jsonResponse({ error: "key is required" }, { status: 400 });
  }
  const value = await env.FEATURES_KV.get(`${MIGRATION_STATE_KV_PREFIX}:${key}`);
  return jsonResponse({ key, value });
};

interface RecomputeRequestBody {
  raceKey: string;
  source: "jra" | "nar";
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

interface BuildRaceFeaturesResult {
  raceKey: string;
  rowCount: number;
  r2Key: string;
  builtAt: string;
}

const toRaceJobKeyFromBody = (body: RecomputeRequestBody): RaceJobKey => ({
  raceKey: body.raceKey,
  source: body.source,
  kaisaiNen: body.kaisaiNen,
  kaisaiTsukihi: body.kaisaiTsukihi,
  keibajoCode: body.keibajoCode,
  raceBango: body.raceBango,
});

// Reads Hyperdrive (Postgres) via buildRaceFeatures, encodes Parquet bytes,
// PUTs to R2, then mirrors the build into KV (state + latest features).
// Legacy D1 daily_race_entries is NEVER touched (Phase 0 rule 3).
export const buildAndPersistRaceFeatures = async (
  env: Env,
  raceJobKey: RaceJobKey,
): Promise<BuildRaceFeaturesResult> => {
  const rows: DailyRaceEntryRow[] = await buildRaceFeatures(raceJobKey, env);
  const parquetBytes = await encodeRaceFeaturesParquet(rows);
  const r2Key = buildRaceParquetR2Key({
    source: raceJobKey.source,
    kaisaiNen: raceJobKey.kaisaiNen,
    kaisaiTsukihi: raceJobKey.kaisaiTsukihi,
    keibajoCode: raceJobKey.keibajoCode,
    raceBango: raceJobKey.raceBango,
  });
  await env.FEATURES_ARCHIVE.put(r2Key, parquetBytes);
  const builtAt = new Date().toISOString();
  await putBuildStateToKv(env, raceJobKey.raceKey, {
    lastBuiltAt: builtAt,
    rowCount: rows.length,
  });
  await writeLatestFeaturesToKv(env, raceJobKey.raceKey, rows);
  return { builtAt, r2Key, raceKey: raceJobKey.raceKey, rowCount: rows.length };
};

export const handleRecomputeRequest = async (env: Env, request: Request): Promise<Response> => {
  if (!isAuthorizedInternalRequest(request, env)) {
    return jsonResponse({ error: "unauthorized" }, { status: 401 });
  }
  const body = (await request.json()) as RecomputeRequestBody;
  const result = await buildAndPersistRaceFeatures(env, toRaceJobKeyFromBody(body));
  return jsonResponse(result);
};

const parseRaceKeyFromUrl = (url: URL): string | null => {
  const value = url.searchParams.get("race_key");
  return value ? decodeURIComponent(value) : null;
};

export const handleFetchRequest = async (env: Env, request: Request): Promise<Response> => {
  const url = new URL(request.url);
  if (url.pathname === "/") {
    return handleRoot();
  }
  if (request.method === "GET" && url.pathname === "/api/features/race-trend") {
    return handleRaceTrendStub();
  }
  if (request.method === "GET" && url.pathname === "/api/running-styles") {
    const raceKey = parseRaceKeyFromUrl(url);
    if (!raceKey) {
      return jsonResponse({ error: "race_key is required" }, { status: 400 });
    }
    return handleGetRunningStyles(env, raceKey);
  }
  if (request.method === "GET" && url.pathname === "/api/finish-positions") {
    const raceKey = parseRaceKeyFromUrl(url);
    if (!raceKey) {
      return jsonResponse({ error: "race_key is required" }, { status: 400 });
    }
    return handleGetFinishPositions(env, raceKey);
  }
  if (request.method === "POST" && url.pathname === "/api/internal/recompute-and-build-parquet") {
    return handleRecomputeRequest(env, request);
  }
  if (request.method === "POST" && url.pathname === "/api/internal/migration-state") {
    return handleMigrationStatePost(env, request);
  }
  if (request.method === "GET" && url.pathname === "/api/internal/migration-state") {
    return handleMigrationStateGet(env, request);
  }
  return jsonResponse({ error: "not found" }, { status: 404 });
};

export const runScheduledFeaturesPlan = async (now: Date): Promise<boolean> => {
  if (!shouldRunFeaturesCron(now)) {
    return false;
  }
  // TODO(next phase): enqueue per-race predict-running-style and
  // predict-finish-position jobs for today's NAR/JRA races.
  return true;
};

export const handleScheduled = async (event: ScheduledEvent): Promise<void> => {
  await runScheduledFeaturesPlan(new Date(event.scheduledTime));
};

const toRaceJobKey = (job: Extract<Job, { type: "build-race-features" }>): RaceJobKey => ({
  raceKey: job.raceKey,
  source: job.source,
  kaisaiNen: job.kaisaiNen,
  kaisaiTsukihi: job.kaisaiTsukihi,
  keibajoCode: job.keibajoCode,
  raceBango: job.raceBango,
});

const handleBuildRaceFeaturesJob = async (
  env: Env,
  job: Extract<Job, { type: "build-race-features" }>,
): Promise<void> => {
  await buildAndPersistRaceFeatures(env, toRaceJobKey(job));
};

const handlePredictRunningStyleJob = async (
  env: Env,
  job: Extract<Job, { type: "predict-running-style" }>,
): Promise<void> => {
  await env.FEATURES_KV.put(
    `features:running-style-requested:${job.raceKey}`,
    JSON.stringify({ predictedAt: job.predictedAt }),
    { expirationTtl: 3600 },
  );
};

const handlePredictFinishPositionJob = async (
  env: Env,
  job: Extract<Job, { type: "predict-finish-position" }>,
): Promise<void> => {
  await upsertFinishPositionPredictions(env.REALTIME_FEATURES_DB, {
    raceKey: job.raceKey,
    source: job.source,
    predictionsJson: "[]",
    predictedAt: job.predictedAt,
    predictorVersion: "skeleton-v0",
  });
};

export const handleQueue = async (batch: MessageBatch<Job>, env: Env): Promise<void> => {
  for (const message of batch.messages) {
    const job = message.body;
    if (job.type === "build-race-features") {
      await handleBuildRaceFeaturesJob(env, job);
      message.ack();
      continue;
    }
    if (job.type === "predict-running-style") {
      await handlePredictRunningStyleJob(env, job);
      message.ack();
      continue;
    }
    if (job.type === "predict-finish-position") {
      await handlePredictFinishPositionJob(env, job);
      message.ack();
      continue;
    }
    message.ack();
  }
};

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    return handleFetchRequest(env, request);
  },
  async scheduled(event: ScheduledEvent): Promise<void> {
    await handleScheduled(event);
  },
  async queue(batch: MessageBatch<Job>, env: Env): Promise<void> {
    await handleQueue(batch, env);
  },
};
