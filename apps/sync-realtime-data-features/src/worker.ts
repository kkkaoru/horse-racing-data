// Run with bun. Fetch / scheduled / queue handlers for sync-realtime-data-features.

import { buildRaceFeatures } from "./features/build";
import { encodeRaceFeaturesParquet } from "./features/parquet";
import { tryParseRaceKey } from "./features/race-key";
import { handleRaceTrend } from "./features/race-trend";
import { buildRaceParquetR2Key } from "./features/r2-key";
import { handleFinishPositionPredictionJob } from "./finish-position/inference";
import {
  type BuildStateRecord,
  getBuildStateFromKv,
  putBuildStateToKv,
} from "./gates/build-state-kv";
import { acquireEnqueueLock, isEnqueueLocked } from "./gates/enqueue-lock-kv";
import { writeLatestFeaturesToKv } from "./gates/latest-features-kv-mirror";
import { shouldRunFeaturesCron } from "./gates/polling-window-gate";
import { jsonResponse } from "./http";
import { handleRunningStylePredictionJob } from "./running-style/inference";
import {
  listTodayRaceKeysFromHyperdrive,
  toRaceJobKeyFromTodayRaceKey,
} from "./scheduled-race-list";
import {
  getFinishPositionPredictions,
  getRunningStyleInferenceState,
  listRaceRunningStyles,
} from "./storage";
import { getTodayJst } from "./time";
import type { DailyRaceEntryRow, Env, Job, RaceJobKey } from "./types";

const MIGRATION_STATE_KV_PREFIX = "features:migration";
const BUILD_STATE_FRESHNESS_MS = 10 * 60 * 1000;
const BUILD_RACE_FEATURES_JOB_TYPE = "build-race-features";

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
  source?: "jra" | "nar";
  kaisaiNen?: string;
  kaisaiTsukihi?: string;
  keibajoCode?: string;
  raceBango?: string;
}

interface BuildRaceFeaturesResult {
  raceKey: string;
  rowCount: number;
  r2Key: string;
  builtAt: string;
}

const hasExplicitRaceFields = (
  body: RecomputeRequestBody,
): body is Required<RecomputeRequestBody> =>
  body.source !== undefined &&
  body.kaisaiNen !== undefined &&
  body.kaisaiTsukihi !== undefined &&
  body.keibajoCode !== undefined &&
  body.raceBango !== undefined;

// Accept both shapes from upstream callers:
//   1. { raceKey } only — parse the 5-part race_key string.
//   2. { raceKey, source, kaisaiNen, kaisaiTsukihi, keibajoCode, raceBango }
//      — trust the explicit fields (legacy forwardRaceForFeatures payload).
const toRaceJobKeyFromBody = (body: RecomputeRequestBody): RaceJobKey | null => {
  if (hasExplicitRaceFields(body)) {
    return {
      raceKey: body.raceKey,
      source: body.source,
      kaisaiNen: body.kaisaiNen,
      kaisaiTsukihi: body.kaisaiTsukihi,
      keibajoCode: body.keibajoCode,
      raceBango: body.raceBango,
    };
  }
  return tryParseRaceKey(body.raceKey);
};

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
  const raceJobKey = toRaceJobKeyFromBody(body);
  if (!raceJobKey) {
    return jsonResponse(
      {
        error: "raceKey must match {source}:{kaisaiNen}:{kaisaiTsukihi}:{keibajoCode}:{raceBango}",
      },
      { status: 400 },
    );
  }
  const result = await buildAndPersistRaceFeatures(env, raceJobKey);
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
    return handleRaceTrend(env, request);
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

// Conditional re-build gate for the scheduled */10 * * * * tick.
// Returns true when we should enqueue a build-race-features job.
// - state null (no Parquet yet for this race today) -> build
// - state.rowCount > 0 AND lastBuiltAt within freshness window -> SKIP
// - otherwise (stale OR rowCount === 0) -> re-build to refresh finish_position
//   after results land in old D1 via Phase A2 5-minute poller.
export const shouldRebuildRaceFeatures = (state: BuildStateRecord | null, now: Date): boolean => {
  if (!state) {
    return true;
  }
  if (state.rowCount <= 0) {
    return true;
  }
  const lastMs = Date.parse(state.lastBuiltAt);
  if (!Number.isFinite(lastMs)) {
    return true;
  }
  return now.getTime() - lastMs >= BUILD_STATE_FRESHNESS_MS;
};

const enqueueRaceBuildJobIfNeeded = async (
  env: Env,
  raceJobKey: RaceJobKey,
  now: Date,
): Promise<void> => {
  const locked = await isEnqueueLocked(env, raceJobKey.raceKey, BUILD_RACE_FEATURES_JOB_TYPE);
  if (locked) {
    return;
  }
  const state = await getBuildStateFromKv(env, raceJobKey.raceKey);
  if (!shouldRebuildRaceFeatures(state, now)) {
    return;
  }
  await env.REALTIME_FEATURES_JOBS.send({
    kaisaiNen: raceJobKey.kaisaiNen,
    kaisaiTsukihi: raceJobKey.kaisaiTsukihi,
    keibajoCode: raceJobKey.keibajoCode,
    raceBango: raceJobKey.raceBango,
    raceKey: raceJobKey.raceKey,
    source: raceJobKey.source,
    type: "build-race-features",
  });
  await acquireEnqueueLock(env, raceJobKey.raceKey, BUILD_RACE_FEATURES_JOB_TYPE);
};

const enqueueRaceInferenceJobs = async (
  env: Env,
  raceJobKey: RaceJobKey,
  predictedAt: string,
): Promise<void> => {
  const runningStyleLocked = await isEnqueueLocked(
    env,
    raceJobKey.raceKey,
    "predict-running-style",
  );
  if (!runningStyleLocked) {
    await env.REALTIME_FEATURES_JOBS.send({
      kaisaiNen: raceJobKey.kaisaiNen,
      kaisaiTsukihi: raceJobKey.kaisaiTsukihi,
      keibajoCode: raceJobKey.keibajoCode,
      predictedAt,
      raceBango: raceJobKey.raceBango,
      raceKey: raceJobKey.raceKey,
      source: raceJobKey.source,
      type: "predict-running-style",
    });
    await acquireEnqueueLock(env, raceJobKey.raceKey, "predict-running-style");
  }
  const finishPositionLocked = await isEnqueueLocked(
    env,
    raceJobKey.raceKey,
    "predict-finish-position",
  );
  if (!finishPositionLocked) {
    await env.REALTIME_FEATURES_JOBS.send({
      kaisaiNen: raceJobKey.kaisaiNen,
      kaisaiTsukihi: raceJobKey.kaisaiTsukihi,
      keibajoCode: raceJobKey.keibajoCode,
      predictedAt,
      raceBango: raceJobKey.raceBango,
      raceKey: raceJobKey.raceKey,
      source: raceJobKey.source,
      type: "predict-finish-position",
    });
    await acquireEnqueueLock(env, raceJobKey.raceKey, "predict-finish-position");
  }
};

export interface ScheduledFeaturesPlanResult {
  ran: boolean;
  enqueuedRaceCount: number;
}

export const runScheduledFeaturesPlan = async (
  env: Env,
  now: Date,
): Promise<ScheduledFeaturesPlanResult> => {
  if (!shouldRunFeaturesCron(now)) {
    return { enqueuedRaceCount: 0, ran: false };
  }
  const yyyymmdd = getTodayJst(now);
  const todayRaceKeys = await listTodayRaceKeysFromHyperdrive(env, yyyymmdd);
  const predictedAt = now.toISOString();
  const raceJobKeys = todayRaceKeys.map(toRaceJobKeyFromTodayRaceKey);
  for (const raceJobKey of raceJobKeys) {
    await enqueueRaceBuildJobIfNeeded(env, raceJobKey, now);
    await enqueueRaceInferenceJobs(env, raceJobKey, predictedAt);
  }
  return { enqueuedRaceCount: raceJobKeys.length, ran: true };
};

export const handleScheduled = async (event: ScheduledEvent, env: Env): Promise<void> => {
  await runScheduledFeaturesPlan(env, new Date(event.scheduledTime));
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
  await handleRunningStylePredictionJob(
    {
      kaisaiNen: job.kaisaiNen,
      kaisaiTsukihi: job.kaisaiTsukihi,
      keibajoCode: job.keibajoCode,
      predictedAt: job.predictedAt,
      raceBango: job.raceBango,
      raceKey: job.raceKey,
      source: job.source,
    },
    env,
  );
};

const handlePredictFinishPositionJob = async (
  env: Env,
  job: Extract<Job, { type: "predict-finish-position" }>,
): Promise<void> => {
  await handleFinishPositionPredictionJob(
    {
      kaisaiNen: job.kaisaiNen,
      kaisaiTsukihi: job.kaisaiTsukihi,
      keibajoCode: job.keibajoCode,
      predictedAt: job.predictedAt,
      raceBango: job.raceBango,
      raceKey: job.raceKey,
      source: job.source,
    },
    env,
  );
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
  async scheduled(event: ScheduledEvent, env: Env): Promise<void> {
    await handleScheduled(event, env);
  },
  async queue(batch: MessageBatch<Job>, env: Env): Promise<void> {
    await handleQueue(batch, env);
  },
};
