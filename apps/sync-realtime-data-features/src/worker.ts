// Run with bun. Fetch / scheduled / queue handlers for sync-realtime-data-features.

import {
  type PredictForDaySource,
  type RunPredictionsForDayResult,
  runPredictionsForDay,
} from "./admin-predict-for-day";
import { buildRaceFeatures } from "./features/build";
import { encodeRaceFeaturesParquet } from "./features/parquet";
import { tryParseRaceKey } from "./features/race-key";
import { handleRaceTrend } from "./features/race-trend";
import { buildRaceParquetR2Key } from "./features/r2-key";
import { handleFinishPositionPredictionJob } from "./finish-position/inference";
import { readNextBatchSize, recordRecomputeOutcome } from "./gates/adaptive-batch-kv";
import {
  type BuildStateRecord,
  getBuildStateFromKv,
  isBuildStateFresh,
  putBuildStateToKv,
} from "./gates/build-state-kv";
import { acquireEnqueueLock, isEnqueueLocked } from "./gates/enqueue-lock-kv";
import { writeLatestFeaturesToKv } from "./gates/latest-features-kv-mirror";
import { shouldRunFeaturesCron } from "./gates/polling-window-gate";
import { jsonResponse } from "./http";
import { handleRunningStylePredictionJob } from "./running-style/inference";
import { buildPast14Targets } from "./scheduled-past14-targets";
import {
  listTodayRaceKeysFromHyperdrive,
  listTomorrowRaceKeysFromHyperdrive,
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
const BUILD_STATE_FRESHNESS_FUTURE_MS = 6 * 60 * 60 * 1000;
const BUILD_STATE_FRESHNESS_PAST14_MS = 7 * 24 * 60 * 60 * 1000;
const BUILD_RACE_FEATURES_JOB_TYPE = "build-race-features";
const YMD_PATTERN = /^\d{8}$/u;
const VALID_PREDICT_FOR_DAY_SOURCES = new Set<string>(["jra", "nar", "all"]);
const PREDICT_FOR_DAY_CRON = "5 2,6,10,14 * * *";

interface MigrationStateRequest {
  key: string;
  value: string;
}

interface PredictForDayParsedBody {
  source: PredictForDaySource;
  targetYmd: string;
  skipCompleted: boolean;
}

// Guard-style normaliser so the source narrowing stays a single linear chain
// instead of a nested ternary (rule 27 / 58).
const normalizePredictForDaySource = (value: string): PredictForDaySource => {
  if (value === "jra") {
    return "jra";
  }
  if (value === "nar") {
    return "nar";
  }
  return "all";
};

const parsePredictForDayBody = (raw: unknown): PredictForDayParsedBody | null => {
  if (typeof raw !== "object" || raw === null) {
    return null;
  }
  const source = Reflect.get(raw, "source");
  const targetYmd = Reflect.get(raw, "targetYmd");
  const skipCompletedRaw = Reflect.get(raw, "skipCompleted");
  if (typeof source !== "string" || !VALID_PREDICT_FOR_DAY_SOURCES.has(source)) {
    return null;
  }
  if (typeof targetYmd !== "string" || !YMD_PATTERN.test(targetYmd)) {
    return null;
  }
  const skipCompleted = typeof skipCompletedRaw === "boolean" ? skipCompletedRaw : false;
  return { skipCompleted, source: normalizePredictForDaySource(source), targetYmd };
};

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
  try {
    const result = await buildAndPersistRaceFeatures(env, raceJobKey);
    return jsonResponse(result);
  } catch (error) {
    console.error("[features] recompute failed", error);
    return jsonResponse(
      {
        error: error instanceof Error ? error.message : String(error),
        raceKey: raceJobKey.raceKey,
      },
      { status: 500 },
    );
  }
};

// Direct (queue-bypassing) running-style inference endpoint for force-recovery.
// Mirrors the `as RecomputeRequestBody` cast pattern already used above —
// this is a pre-existing accepted style in this file.
export const handlePredictRunningStyleRequest = async (
  env: Env,
  request: Request,
): Promise<Response> => {
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
  try {
    const result = await handleRunningStylePredictionJob(
      { ...raceJobKey, predictedAt: new Date().toISOString() },
      env,
    );
    return jsonResponse(result);
  } catch (error) {
    console.error("[features] predict-running-style failed", error);
    return jsonResponse(
      {
        error: error instanceof Error ? error.message : String(error),
        raceKey: raceJobKey.raceKey,
      },
      { status: 500 },
    );
  }
};

// Direct (queue-bypassing) finish-position inference endpoint for force-recovery.
// Mirrors the `as RecomputeRequestBody` cast pattern already used above —
// this is a pre-existing accepted style in this file.
export const handlePredictFinishPositionRequest = async (
  env: Env,
  request: Request,
): Promise<Response> => {
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
  try {
    const result = await handleFinishPositionPredictionJob(
      { ...raceJobKey, predictedAt: new Date().toISOString() },
      env,
    );
    return jsonResponse(result);
  } catch (error) {
    console.error("[features] predict-finish-position failed", error);
    return jsonResponse(
      {
        error: error instanceof Error ? error.message : String(error),
        raceKey: raceJobKey.raceKey,
      },
      { status: 500 },
    );
  }
};

// Enumerates all of today's race_keys from Hyperdrive and enqueues
// predict-running-style + predict-finish-position jobs for each via the
// existing REALTIME_FEATURES_JOBS queue producer. Force-recovery / backfill
// endpoint for the admin "predict for day" workflow.
export const handlePredictForDayRequest = async (env: Env, request: Request): Promise<Response> => {
  if (!isAuthorizedInternalRequest(request, env)) {
    return jsonResponse({ error: "unauthorized" }, { status: 401 });
  }
  const rawBody = (await request.json()) as unknown;
  const parsed = parsePredictForDayBody(rawBody);
  if (!parsed) {
    return jsonResponse(
      {
        error: "body must be { source: 'jra' | 'nar' | 'all', targetYmd: 'YYYYMMDD' (8 digits) }",
      },
      { status: 400 },
    );
  }
  try {
    const result: RunPredictionsForDayResult = await runPredictionsForDay(env, parsed);
    return jsonResponse(result);
  } catch (error) {
    console.error("[features] predict-for-day failed", error);
    return jsonResponse(
      {
        error: error instanceof Error ? error.message : String(error),
        targetYmd: parsed.targetYmd,
      },
      { status: 500 },
    );
  }
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
  if (request.method === "POST" && url.pathname === "/api/internal/predict-running-style") {
    return handlePredictRunningStyleRequest(env, request);
  }
  if (request.method === "POST" && url.pathname === "/api/internal/predict-finish-position") {
    return handlePredictFinishPositionRequest(env, request);
  }
  if (request.method === "POST" && url.pathname === "/api/internal/predict-for-day") {
    return handlePredictForDayRequest(env, request);
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

interface BuildCandidate {
  key: RaceJobKey;
  freshnessMs: number;
}

const toBuildJobMessage = (
  raceJobKey: RaceJobKey,
): Extract<Job, { type: "build-race-features" }> => ({
  kaisaiNen: raceJobKey.kaisaiNen,
  kaisaiTsukihi: raceJobKey.kaisaiTsukihi,
  keibajoCode: raceJobKey.keibajoCode,
  raceBango: raceJobKey.raceBango,
  raceKey: raceJobKey.raceKey,
  source: raceJobKey.source,
  type: "build-race-features",
});

const tryEnqueueBuildCandidate = async (
  env: Env,
  candidate: BuildCandidate,
  now: Date,
): Promise<boolean> => {
  const locked = await isEnqueueLocked(env, candidate.key.raceKey, BUILD_RACE_FEATURES_JOB_TYPE);
  if (locked) {
    return false;
  }
  const state = await getBuildStateFromKv(env, candidate.key.raceKey);
  if (isBuildStateFresh(state, candidate.freshnessMs, now)) {
    return false;
  }
  await env.REALTIME_FEATURES_JOBS.send(toBuildJobMessage(candidate.key));
  await acquireEnqueueLock(env, candidate.key.raceKey, BUILD_RACE_FEATURES_JOB_TYPE);
  return true;
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
  todayCount: number;
  tomorrowCount: number;
  past14Count: number;
  batchSize: number;
}

const emptyPlanResult = (ran: boolean): ScheduledFeaturesPlanResult => ({
  batchSize: 0,
  enqueuedRaceCount: 0,
  past14Count: 0,
  ran,
  todayCount: 0,
  tomorrowCount: 0,
});

const buildCandidateList = (
  todayKeys: RaceJobKey[],
  tomorrowKeys: RaceJobKey[],
  past14Keys: RaceJobKey[],
): BuildCandidate[] => [
  ...todayKeys.map((key) => ({ freshnessMs: BUILD_STATE_FRESHNESS_MS, key })),
  ...tomorrowKeys.map((key) => ({ freshnessMs: BUILD_STATE_FRESHNESS_FUTURE_MS, key })),
  ...past14Keys.map((key) => ({ freshnessMs: BUILD_STATE_FRESHNESS_PAST14_MS, key })),
];

interface BuildEnqueueCounts {
  total: number;
  today: number;
  tomorrow: number;
  past14: number;
}

const classifyEnqueue = (
  candidateIndex: number,
  todayCount: number,
  tomorrowCount: number,
): "today" | "tomorrow" | "past14" => {
  if (candidateIndex < todayCount) {
    return "today";
  }
  if (candidateIndex < todayCount + tomorrowCount) {
    return "tomorrow";
  }
  return "past14";
};

const stepBuildEnqueueLoop = async (
  env: Env,
  candidates: BuildCandidate[],
  todayCount: number,
  tomorrowCount: number,
  batchSize: number,
  now: Date,
  index: number,
  counts: BuildEnqueueCounts,
): Promise<BuildEnqueueCounts> => {
  if (index >= candidates.length || counts.total >= batchSize) {
    return counts;
  }
  const enqueued = await tryEnqueueBuildCandidate(env, candidates[index]!, now);
  const bucket = classifyEnqueue(index, todayCount, tomorrowCount);
  const nextCounts: BuildEnqueueCounts = enqueued
    ? {
        past14: bucket === "past14" ? counts.past14 + 1 : counts.past14,
        today: bucket === "today" ? counts.today + 1 : counts.today,
        tomorrow: bucket === "tomorrow" ? counts.tomorrow + 1 : counts.tomorrow,
        total: counts.total + 1,
      }
    : counts;
  return stepBuildEnqueueLoop(
    env,
    candidates,
    todayCount,
    tomorrowCount,
    batchSize,
    now,
    index + 1,
    nextCounts,
  );
};

const runBuildEnqueueLoop = (
  env: Env,
  candidates: BuildCandidate[],
  todayCount: number,
  tomorrowCount: number,
  batchSize: number,
  now: Date,
): Promise<BuildEnqueueCounts> =>
  stepBuildEnqueueLoop(env, candidates, todayCount, tomorrowCount, batchSize, now, 0, {
    past14: 0,
    today: 0,
    tomorrow: 0,
    total: 0,
  });

const runTodayInferenceEnqueueLoop = async (
  env: Env,
  todayJobs: RaceJobKey[],
  predictedAt: string,
): Promise<void> => {
  await todayJobs.reduce<Promise<void>>(
    (prev, job) => prev.then(() => enqueueRaceInferenceJobs(env, job, predictedAt)),
    Promise.resolve(),
  );
};

export const runScheduledFeaturesPlan = async (
  env: Env,
  now: Date,
): Promise<ScheduledFeaturesPlanResult> => {
  if (!shouldRunFeaturesCron(now)) {
    return emptyPlanResult(false);
  }
  const todayJst = getTodayJst(now);
  const batchSize = await readNextBatchSize(env);
  const todayRaceKeys = await listTodayRaceKeysFromHyperdrive(env, todayJst);
  const tomorrowRaceKeys = await listTomorrowRaceKeysFromHyperdrive(env, now);
  const todayJobs = todayRaceKeys.map(toRaceJobKeyFromTodayRaceKey);
  const tomorrowJobs = tomorrowRaceKeys.map(toRaceJobKeyFromTodayRaceKey);
  const past14Jobs = buildPast14Targets({
    todayJst,
    todayKeys: todayRaceKeys,
    tomorrowKeys: tomorrowRaceKeys,
  });
  const candidates = buildCandidateList(todayJobs, tomorrowJobs, past14Jobs);
  const counts = await runBuildEnqueueLoop(
    env,
    candidates,
    todayJobs.length,
    tomorrowJobs.length,
    batchSize,
    now,
  );
  await runTodayInferenceEnqueueLoop(env, todayJobs, now.toISOString());
  return {
    batchSize,
    enqueuedRaceCount: counts.total,
    past14Count: counts.past14,
    ran: true,
    todayCount: counts.today,
    tomorrowCount: counts.tomorrow,
  };
};

const runScheduledPredictForDay = async (env: Env, scheduledAt: Date): Promise<void> => {
  const targetYmd = getTodayJst(scheduledAt);
  await runPredictionsForDay(env, {
    skipCompleted: true,
    source: "all",
    targetYmd,
  });
};

export const handleScheduled = async (event: ScheduledEvent, env: Env): Promise<void> => {
  const scheduledAt = new Date(event.scheduledTime);
  if (event.cron === PREDICT_FOR_DAY_CRON) {
    await runScheduledPredictForDay(env, scheduledAt);
    return;
  }
  await runScheduledFeaturesPlan(env, scheduledAt);
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
  try {
    await buildAndPersistRaceFeatures(env, toRaceJobKey(job));
    await recordRecomputeOutcome(env, true);
  } catch (error) {
    await recordRecomputeOutcome(env, false);
    throw error;
  }
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
