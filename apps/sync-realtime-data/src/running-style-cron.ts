// Run with bun. Cron planner for running-style prediction generation.
// It inspects Cloudflare D1 race registrations for a date, checks whether
// race_running_styles already has all runners, and queues per-race Worker
// jobs for missing predictions.

import { formatError, formatErrorLogLine } from "./format-error";
import { fetchRunningStyleFeatureCountsFromCatalog } from "./running-style-catalog-client";
import {
  listRaceRunningStyleCounts,
  listRaceRunningStylesForRace,
  listRunningStyleInferenceStates,
  markRunningStyleInferenceEnqueueFailed,
  upsertRunningStylePendingStates,
  type RunningStyleInferenceStateDetail,
  type RunningStylePendingRace,
} from "./running-style-d1";
import { listRunningStyleExpectedHorseCounts } from "./running-style-expected-horses";
import {
  exportRunningStyleParquetForDay,
  type ExportRunningStyleParquetResult,
} from "./running-style-parquet-export";
import { putViewerRunningStyleRaceCache } from "./viewer-running-style-cache";
import {
  deriveRunningStyleCategory,
  type RunningStyleCellCategory,
} from "./running-style-cell-router";
import {
  buildRunningStyleRaceKey,
  normalizeKeibajoCode,
  normalizeRaceBango,
  parseRunningStyleRaceKey,
  type RunningStyleSource,
} from "./running-style-features";
import { listRunningStyleRacesByDate } from "./running-style-race-list";
import type { Env, RunningStylePredictionJob } from "./types";

export const RUNNING_STYLE_INFERENCE_CRON = "*/10 0-14 * * *";
export const RUNNING_STYLE_PREWARM_CRON = "0 12 * * *";

const ENABLED_FLAG = "1";
const DATE_PAD_WIDTH = 2;
const QUEUE_SEND_BATCH_SIZE = 100;
// Existing work must not starve a newly published race day. Keep a bounded
// projected backlog instead of refusing every dispatch whenever any message
// is in flight; inference-state deduplication already prevents duplicates.
const MAX_RUNNING_STYLE_PROJECTED_BACKLOG = 256;
// A stale queue can contain acknowledged/duplicate work while D1 still has
// several pending races.  Permit one bounded multi-race recovery batch above
// the projected cap; state leases and per-race idempotency still prevent a
// duplicate inference storm.  A single new race remains fail-closed at the
// normal cap so this is not a general backlog bypass.
const MAX_RUNNING_STYLE_RECOVERY_BATCH = 32;
// Queue delivery can legitimately take several minutes while a container is
// cold-starting.  A five-minute lease expired before that delivery completed,
// so every planner tick could enqueue a duplicate job.  Keep the reservation
// alive across the queue's retry window; failed sends are explicitly restored
// to an enqueue-failed state and remain eligible on the next tick.
const ACTIVE_STATE_TTL_MS = 15 * 60 * 1000;
const ACTIVE_STATUSES = new Set(["pending", "processing"]);
const FINISH_POSITION_DAY_BASE_URL =
  "https://finish-position-cron.internal/api/admin/prewarm-day-base";
const RUNNING_STYLE_FOUNDATION_PREFIX = "feat-running-style-base/catalog-v1";
const RUNNING_STYLE_FOUNDATION_FILE = "features.parquet";
const FOUNDATION_NONE_WATERMARK = "none";
const FOUNDATION_PREWARM_MARKER_PREFIX = "control:running-style-foundation-prewarm:v1";
const FOUNDATION_PREWARM_MARKER_TTL_SECONDS = 15 * 60;
// 2026-06-04 incident: before JST midnight rolled over, the cron derived
// today=06-03 and so never retried the stalled 06-04 races. Sweeping the last
// 6 hours of yesterday-JST keeps post-midnight races eligible for retry
// without re-processing dates far enough in the past that all states are
// settled.
const YESTERDAY_SWEEP_HOURS = 6;
const JST_OFFSET_HOURS = 9;

export interface RegisteredRaceRow {
  source: RunningStyleSource;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  grade_code?: string | null;
}

export interface RunningStylePlanRace extends RunningStylePendingRace {
  existingHorseCount: number;
}

export interface RunningStyleParquetExportSummary {
  bytesWritten: number;
  exportError?: string;
  fileCount: number;
  rowCount: number;
}

export interface RunningStylePlanSummary {
  alreadyQueued: number;
  cacheRefresh?: ViewerRunningStyleCacheRefreshSummary;
  completed: number;
  date: string;
  enqueued: number;
  featureReady: number;
  missingFeatures: number;
  parquetExport?: RunningStyleParquetExportSummary;
  planError?: string;
  scanned: number;
}

interface SettledPlanSummaryInput {
  completed: number;
  date: string;
  missingFeatures: number;
  scanned: number;
}

interface PredictionJobSendFailure {
  error: unknown;
  job: RunningStylePredictionJob;
}

interface PredictionJobSendResult {
  failed: ReadonlyArray<PredictionJobSendFailure>;
  sentCount: number;
}

interface RunningStyleFoundationGateResult {
  errors: ReadonlyArray<string>;
  readyCategories: ReadonlySet<RunningStyleCellCategory>;
}

interface RunningStyleFoundationInspection {
  ready: boolean;
}

interface RunningStyleFoundationIdentity {
  category: RunningStyleCellCategory;
  date: string;
  env: Env;
}

interface RunningStyleFoundationGateParams {
  date: string;
  env: Env;
  races: ReadonlyArray<RunningStylePlanRace>;
}

interface RunningStyleQueueDispatchGate {
  blocked: boolean;
  error?: string;
}

const inspectRunningStyleQueueDispatch = async (
  queue: Queue,
  requestedCount: number,
  allowRecoveryOverflow = false,
): Promise<RunningStyleQueueDispatchGate> => {
  try {
    const metrics = await queue.metrics();
    if (metrics.backlogCount + requestedCount <= MAX_RUNNING_STYLE_PROJECTED_BACKLOG) {
      return { blocked: false };
    }
    if (allowRecoveryOverflow && requestedCount <= MAX_RUNNING_STYLE_RECOVERY_BATCH) {
      console.warn(
        `Running-style queue recovery overflow allowed backlog=${metrics.backlogCount} requested=${requestedCount}`,
      );
      return { blocked: false };
    }
    return {
      blocked: true,
      error: `Running-style queue projected backlog=${metrics.backlogCount + requestedCount}; dispatch deferred`,
    };
  } catch (error) {
    // Do not enqueue blindly when the authoritative queue depth is unknown.
    // A closed gate is retried by the next cron tick and prevents a metrics
    // outage from turning into an unbounded duplicate backlog.
    return {
      blocked: true,
      error: `Running-style queue metrics unavailable; dispatch deferred: ${formatError(error)}`,
    };
  }
};

const padDatePart = (value: number): string => String(value).padStart(DATE_PAD_WIDTH, "0");

export const formatYYYYMMDDInJst = (now: Date): string => {
  const utcMillis = now.getTime();
  const jstOffsetMinutes = 9 * 60;
  const jst = new Date(utcMillis + jstOffsetMinutes * 60 * 1000);
  return `${jst.getUTCFullYear()}${padDatePart(jst.getUTCMonth() + 1)}${padDatePart(jst.getUTCDate())}`;
};

export const addDaysToYYYYMMDDInJst = (yyyymmdd: string, days: number): string => {
  const date = new Date(
    `${yyyymmdd.slice(0, 4)}-${yyyymmdd.slice(4, 6)}-${yyyymmdd.slice(6, 8)}T00:00:00+09:00`,
  );
  date.setUTCDate(date.getUTCDate() + days);
  return formatYYYYMMDDInJst(date);
};

export const formatTomorrowYYYYMMDDInJst = (now: Date): string =>
  addDaysToYYYYMMDDInJst(formatYYYYMMDDInJst(now), 1);

export const isRunningStylePlanDateAllowed = (date: string, now: Date): boolean =>
  date <= formatTomorrowYYYYMMDDInJst(now);

const isInferenceEnabled = (env: Env): boolean =>
  env.RUNNING_STYLE_D1_WRITE_ENABLED === ENABLED_FLAG;

const toRunningStylePendingRace = (
  row: RegisteredRaceRow,
  expectedHorseCount: number,
): RunningStylePendingRace => {
  const race = {
    kaisaiNen: row.kaisai_nen,
    kaisaiTsukihi: row.kaisai_tsukihi,
    keibajoCode: normalizeKeibajoCode(row.keibajo_code),
    raceBango: normalizeRaceBango(row.race_bango),
    source: row.source,
  };
  return {
    ...race,
    expectedHorseCount,
    raceKey: buildRunningStyleRaceKey(race),
  };
};

const isActiveState = (state: RunningStyleInferenceStateDetail | undefined, now: Date): boolean => {
  if (state === undefined || !ACTIVE_STATUSES.has(state.status)) return false;
  if (state.attemptedAt === null) return true;
  const attemptedAt = new Date(state.attemptedAt).getTime();
  if (Number.isNaN(attemptedAt)) return false;
  return now.getTime() - attemptedAt <= ACTIVE_STATE_TTL_MS;
};

const toRunningStyleRaceKey = (row: RegisteredRaceRow): string =>
  buildRunningStyleRaceKey({
    kaisaiNen: row.kaisai_nen,
    kaisaiTsukihi: row.kaisai_tsukihi,
    keibajoCode: row.keibajo_code,
    raceBango: row.race_bango,
    source: row.source,
  });

const toRunningStyleCategory = (
  race: Pick<RunningStylePendingRace, "keibajoCode" | "source">,
): RunningStyleCellCategory => deriveRunningStyleCategory(race);

export const buildRunningStyleFoundationKeyForCategory = (
  category: RunningStyleCellCategory,
  date: string,
): string =>
  `${RUNNING_STYLE_FOUNDATION_PREFIX}/${category}/${date}/${RUNNING_STYLE_FOUNDATION_FILE}`;

const isNonNegativeIntegerMetadata = (value: string | undefined): boolean => {
  if (value === undefined || value.trim().length === 0) return false;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0;
};

export const isRunningStyleFoundationReady = (object: R2Object | null): boolean => {
  if (object === null || object.size <= 0 || object.customMetadata === undefined) return false;
  const metadata = object.customMetadata;
  const maxSourceUpdated = metadata["max-data-sakusei-nengappi"];
  const rowCount = metadata["row-count"];
  return (
    maxSourceUpdated !== undefined &&
    maxSourceUpdated.trim().length > 0 &&
    isNonNegativeIntegerMetadata(rowCount) &&
    Number(rowCount) > 0 &&
    metadata["rs-predicted-at-max"] === FOUNDATION_NONE_WATERMARK &&
    metadata["rs-row-count"] === "0"
  );
};

const inspectRunningStyleFoundation = async (
  params: RunningStyleFoundationIdentity,
): Promise<RunningStyleFoundationInspection> => {
  if (params.env.FEATURES_ARCHIVE === undefined) return { ready: false };
  const object = await params.env.FEATURES_ARCHIVE.head(
    buildRunningStyleFoundationKeyForCategory(params.category, params.date),
  );
  const ready = isRunningStyleFoundationReady(object);
  return { ready };
};

const buildRunningStyleFoundationPrewarmMarkerKey = (
  category: RunningStyleCellCategory,
  date: string,
): string => `${FOUNDATION_PREWARM_MARKER_PREFIX}:${category}:${date}`;

const hasRecentRunningStyleFoundationPrewarm = async (
  params: RunningStyleFoundationIdentity,
): Promise<boolean> => {
  if (params.env.DETAIL_SECTION_CACHE_KV === undefined) return false;
  try {
    return (
      (await params.env.DETAIL_SECTION_CACHE_KV.get(
        buildRunningStyleFoundationPrewarmMarkerKey(params.category, params.date),
      )) !== null
    );
  } catch (error) {
    console.error(
      formatErrorLogLine(
        "Running-style foundation prewarm marker read failed",
        { category: params.category, date: params.date },
        error,
      ),
    );
    return false;
  }
};

const rememberRunningStyleFoundationPrewarm = async (
  params: RunningStyleFoundationIdentity,
): Promise<void> => {
  if (params.env.DETAIL_SECTION_CACHE_KV === undefined) return;
  await params.env.DETAIL_SECTION_CACHE_KV.put(
    buildRunningStyleFoundationPrewarmMarkerKey(params.category, params.date),
    new Date().toISOString(),
    { expirationTtl: FOUNDATION_PREWARM_MARKER_TTL_SECONDS },
  );
};

const triggerRunningStyleFoundationPrewarm = async (
  params: RunningStyleFoundationIdentity,
): Promise<string | null> => {
  if (await hasRecentRunningStyleFoundationPrewarm(params)) return null;
  if (params.env.FINISH_POSITION_CRON === undefined) return "missing FINISH_POSITION_CRON binding";
  if (params.env.TRIGGER_TOKEN === undefined || params.env.TRIGGER_TOKEN.length === 0)
    return "missing TRIGGER_TOKEN";
  try {
    const response = await params.env.FINISH_POSITION_CRON.fetch(
      new Request(FINISH_POSITION_DAY_BASE_URL, {
        body: JSON.stringify({
          category: params.category,
          // A final day-base can be fresh while this earlier foundation is
          // absent. Force is required so that prewarm rebuilds the base and
          // cannot short-circuit on that later-stage object.
          force: true,
          generatePredictionsAfterHit: true,
          runYmd: params.date,
        }),
        headers: {
          Authorization: `Bearer ${params.env.TRIGGER_TOKEN}`,
          "Content-Type": "application/json",
        },
        method: "POST",
      }),
    );
    if (!response.ok) return `HTTP ${response.status}`;
    await rememberRunningStyleFoundationPrewarm(params).catch((error: unknown) =>
      console.error(
        formatErrorLogLine(
          "Running-style foundation prewarm marker write failed",
          { category: params.category, date: params.date },
          error,
        ),
      ),
    );
    return null;
  } catch (error) {
    return formatError(error);
  }
};

const gateRunningStyleFoundations = async (
  params: RunningStyleFoundationGateParams,
): Promise<RunningStyleFoundationGateResult> => {
  const categories = [...new Set(params.races.map(toRunningStyleCategory))];
  const results = await Promise.all(
    categories.map(async (category) => {
      const inspection = await inspectRunningStyleFoundation({
        category,
        date: params.date,
        env: params.env,
      }).catch((error: unknown) => {
        console.error(
          formatErrorLogLine(
            "Running-style foundation HEAD failed",
            { category, date: params.date },
            error,
          ),
        );
        return { ready: false };
      });
      if (inspection.ready) return { category, error: null, ready: true };
      const error = await triggerRunningStyleFoundationPrewarm({
        category,
        date: params.date,
        env: params.env,
      });
      if (error !== null) {
        console.error(
          `Running-style foundation prewarm failed category=${category} date=${params.date}: ${error}`,
        );
      }
      return { category, error, ready: false };
    }),
  );
  return {
    errors: results.flatMap(({ category, error }) =>
      error === null ? [] : [`Foundation prewarm failed for ${category}:${params.date}: ${error}`],
    ),
    readyCategories: new Set(results.flatMap(({ category, ready }) => (ready ? [category] : []))),
  };
};

const isRunningStyleStateGenerationComplete = (
  state: RunningStyleInferenceStateDetail | undefined,
): boolean =>
  (state?.status === "completed" || state?.status === "sync-failed") &&
  state.writtenHorseCount !== null &&
  state.expectedHorseCount !== null &&
  state.writtenHorseCount >= state.expectedHorseCount;

// D1-only generation probe. A sync-failed race has a complete recovery
// payload, but is not serving-complete until the Queue retry commits to Neon.
// An empty race list trivially satisfies the probe.
const allRegisteredRacesCompleted = (
  registeredRaces: ReadonlyArray<RegisteredRaceRow>,
  states: ReadonlyMap<string, RunningStyleInferenceStateDetail>,
): boolean =>
  registeredRaces.every((row) =>
    isRunningStyleStateGenerationComplete(states.get(toRunningStyleRaceKey(row))),
  );

const selectSyncFailedRacesNeedingRunningStyleMirror = (
  registeredRaces: ReadonlyArray<RegisteredRaceRow>,
  states: ReadonlyMap<string, RunningStyleInferenceStateDetail>,
): RunningStylePlanRace[] => {
  const needed: RunningStylePlanRace[] = [];
  registeredRaces.forEach((row) => {
    const raceKey = toRunningStyleRaceKey(row);
    const state = states.get(raceKey);
    // sync-failed means D1 holds a complete recovery payload but Neon has not
    // acknowledged the mirror transaction. D1 row counts cannot prove Neon
    // completeness, so every such state must re-enter the idempotent mirror
    // Queue path. The consumer promotes it to completed only after the Neon
    // transaction succeeds and reports every expected row written.
    if (state?.status !== "sync-failed" || !isRunningStyleStateGenerationComplete(state)) return;
    const expectedHorseCount = state.expectedHorseCount ?? 0;
    needed.push({
      ...toRunningStylePendingRace(row, expectedHorseCount),
      existingHorseCount: state.writtenHorseCount ?? 0,
    });
  });
  return needed;
};

const buildSettledPlanSummary = (input: SettledPlanSummaryInput): RunningStylePlanSummary => ({
  alreadyQueued: 0,
  completed: input.completed,
  date: input.date,
  enqueued: 0,
  featureReady: 0,
  missingFeatures: input.missingFeatures,
  scanned: input.scanned,
});

// Planner does not gate on race_entry_corner_features anymore: today's races
// often arrive in realtime_race_sources before their derived feature cache is
// built. The per-race worker `handleRunningStylePredictionJob` builds features
// from upstream nvd_se/jvd_se directly, so we enqueue any registered race that
// lacks predictions and let the worker resolve features at inference time.
export const selectRacesNeedingRunningStyleInference = (
  registeredRaces: ReadonlyArray<RegisteredRaceRow>,
  featureCounts: ReadonlyMap<string, number>,
  expectedHorseCounts: ReadonlyMap<string, number>,
  predictionCounts: ReadonlyMap<string, number>,
  states: ReadonlyMap<string, RunningStyleInferenceStateDetail>,
  now = new Date(),
): {
  alreadyQueued: number;
  completed: number;
  featureReady: number;
  missingFeatures: number;
  needed: RunningStylePlanRace[];
} => {
  let alreadyQueued = 0;
  let completed = 0;
  let featureReady = 0;
  let missingFeatures = 0;
  const needed: RunningStylePlanRace[] = [];

  registeredRaces.forEach((row) => {
    const race = toRunningStylePendingRace(row, 0);
    const featureCount = featureCounts.get(race.raceKey) ?? 0;
    if (featureCount > 0) {
      featureReady += 1;
    } else {
      missingFeatures += 1;
    }
    const expectedHorseCount = expectedHorseCounts.get(race.raceKey) ?? featureCount;
    const existingHorseCount = predictionCounts.get(race.raceKey) ?? 0;
    const state = states.get(race.raceKey);
    const stateCompleted = isRunningStyleStateGenerationComplete(state);
    if (stateCompleted) {
      completed += 1;
      return;
    }
    if (isActiveState(state, now)) {
      alreadyQueued += 1;
      return;
    }
    needed.push({
      ...race,
      existingHorseCount,
      expectedHorseCount,
    });
  });

  return { alreadyQueued, completed, featureReady, missingFeatures, needed };
};

const toPredictionJob = (
  row: RunningStylePlanRace,
  predictedAt: string,
): RunningStylePredictionJob => ({
  kaisaiNen: row.kaisaiNen,
  kaisaiTsukihi: row.kaisaiTsukihi,
  keibajoCode: row.keibajoCode,
  predictedAt,
  raceBango: row.raceBango,
  raceKey: row.raceKey,
  source: row.source,
  type: "generate-running-style-predictions",
});

const sendPredictionJob = async (
  queue: Queue,
  job: RunningStylePredictionJob,
): Promise<PredictionJobSendFailure | null> => {
  try {
    await queue.send(job);
    return null;
  } catch (error) {
    return { error, job };
  }
};

const sendPredictionJobChunk = async (
  queue: Queue,
  jobs: ReadonlyArray<RunningStylePredictionJob>,
): Promise<PredictionJobSendResult> => {
  if (jobs.length === 1) {
    const failure = await sendPredictionJob(queue, jobs[0]!);
    return failure === null ? { failed: [], sentCount: 1 } : { failed: [failure], sentCount: 0 };
  }
  try {
    await queue.sendBatch(jobs.map((body) => ({ body })));
    return { failed: [], sentCount: jobs.length };
  } catch (error) {
    console.error(
      formatErrorLogLine(
        "Running-style Queue sendBatch failed; retrying jobs individually",
        { jobCount: String(jobs.length) },
        error,
      ),
    );
  }
  const failed: PredictionJobSendFailure[] = [];
  const sent: RunningStylePredictionJob[] = [];
  for (const job of jobs) {
    const failure = await sendPredictionJob(queue, job);
    if (failure === null) {
      sent.push(job);
    } else {
      failed.push(failure);
    }
  }
  return { failed, sentCount: sent.length };
};

const sendPredictionJobs = async (
  queue: Queue,
  jobs: ReadonlyArray<RunningStylePredictionJob>,
): Promise<PredictionJobSendResult> => {
  const failed: PredictionJobSendFailure[] = [];
  const sent: RunningStylePredictionJob[] = [];
  for (let index = 0; index < jobs.length; index += QUEUE_SEND_BATCH_SIZE) {
    const chunk = jobs.slice(index, index + QUEUE_SEND_BATCH_SIZE);
    const result = await sendPredictionJobChunk(queue, chunk);
    failed.push(...result.failed);
    sent.push(...chunk.slice(0, result.sentCount));
  }
  return { failed, sentCount: sent.length };
};

const formatPredictionJobSendFailures = (
  failures: ReadonlyArray<PredictionJobSendFailure>,
): string | undefined =>
  failures.length === 0
    ? undefined
    : `Queue send failed for ${failures.map(({ job }) => job.raceKey).join(",")}: ${failures
        .map(({ error }) => formatError(error))
        .join("; ")}`;

const restoreFailedPendingStates = async (
  env: Env,
  pendingRaces: ReadonlyArray<RunningStylePendingRace>,
  failures: ReadonlyArray<PredictionJobSendFailure>,
  attemptedAt: string,
): Promise<void> => {
  const pendingRaceKeys = new Set(pendingRaces.map(({ raceKey }) => raceKey));
  await markRunningStyleInferenceEnqueueFailed(
    env.REALTIME_DB,
    failures
      .filter(({ job }) => pendingRaceKeys.has(job.raceKey))
      .map(({ error, job }) => ({ error, raceKey: job.raceKey })),
    attemptedAt,
  );
};

export const planRunningStylePredictionsForDate = async (
  env: Env,
  date: string,
  now: Date,
): Promise<RunningStylePlanSummary> => {
  if (!isRunningStylePlanDateAllowed(date, now)) {
    return {
      alreadyQueued: 0,
      completed: 0,
      date,
      enqueued: 0,
      featureReady: 0,
      missingFeatures: 0,
      planError: `running-style planning rejected future date ${date}; latest allowed date is ${formatTomorrowYYYYMMDDInJst(now)}`,
      scanned: 0,
    };
  }
  const { races: registeredRaces } = await listRunningStyleRacesByDate(env, date);
  if (!isInferenceEnabled(env)) {
    return buildSettledPlanSummary({
      completed: 0,
      date,
      missingFeatures: registeredRaces.length,
      scanned: registeredRaces.length,
    });
  }
  const raceKeys = registeredRaces.map(toRunningStyleRaceKey);
  const [predictionCountsResult, statesResult] = await Promise.allSettled([
    listRaceRunningStyleCounts(env.REALTIME_DB, raceKeys, { bypassCache: true }),
    listRunningStyleInferenceStates(env.REALTIME_DB, raceKeys),
  ]);
  if (statesResult.status === "rejected") {
    throw statesResult.reason;
  }
  const states = statesResult.value;
  // A transient D1 count failure must never turn a completed race into a
  // full re-inference. Treat completed rows as mirrored for this tick and let
  // the next scheduled tick retry the mirror audit once counts are available.
  // Non-completed rows remain eligible because their state is the source of
  // truth for generation, independent of the count query.
  const predictionCountError =
    predictionCountsResult.status === "rejected"
      ? formatError(predictionCountsResult.reason)
      : undefined;
  const predictionCounts =
    predictionCountsResult.status === "fulfilled"
      ? predictionCountsResult.value
      : new Map(
          registeredRaces.flatMap((row) => {
            const state = states.get(toRunningStyleRaceKey(row));
            return isRunningStyleStateGenerationComplete(state)
              ? [[toRunningStyleRaceKey(row), state?.expectedHorseCount ?? 0] as const]
              : [];
          }),
        );
  const mirrorNeeded = selectSyncFailedRacesNeedingRunningStyleMirror(registeredRaces, states);
  const predictedAt = now.toISOString();
  const runningStyleQueue = env.RUNNING_STYLE_JOBS ?? env.REALTIME_JOBS;
  if (allRegisteredRacesCompleted(registeredRaces, states)) {
    const mirrorJobs = mirrorNeeded.map((row) => toPredictionJob(row, predictedAt));
    const dispatchGate: RunningStyleQueueDispatchGate =
      mirrorJobs.length === 0
        ? { blocked: false }
        : await inspectRunningStyleQueueDispatch(runningStyleQueue, mirrorJobs.length);
    const sendResult = dispatchGate.blocked
      ? { failed: [], sentCount: 0 }
      : await sendPredictionJobs(runningStyleQueue, mirrorJobs);
    await restoreFailedPendingStates(env, mirrorNeeded, sendResult.failed, predictedAt);
    return {
      alreadyQueued: 0,
      completed: registeredRaces.length - mirrorNeeded.length,
      date,
      enqueued: sendResult.sentCount,
      featureReady: 0,
      missingFeatures: 0,
      planError:
        [
          predictionCountError,
          dispatchGate.error,
          formatPredictionJobSendFailures(sendResult.failed),
        ]
          .filter((error): error is string => error !== undefined)
          .join("; ") || undefined,
      scanned: registeredRaces.length,
    };
  }
  const featureCounts = await fetchRunningStyleFeatureCountsFromCatalog(
    env.PC_KEIBA_R2_CATALOG,
    date,
  );
  const expectedHorseCounts = await listRunningStyleExpectedHorseCounts(
    env.REALTIME_DB,
    raceKeys,
    featureCounts,
  );
  const selected = selectRacesNeedingRunningStyleInference(
    registeredRaces,
    featureCounts,
    expectedHorseCounts,
    predictionCounts,
    states,
    now,
  );
  const foundationGate = await gateRunningStyleFoundations({
    date,
    env,
    races: selected.needed,
  });
  const foundationReadyRaces = selected.needed.filter((race) =>
    foundationGate.readyCategories.has(toRunningStyleCategory(race)),
  );
  const predictionJobs = [...foundationReadyRaces, ...mirrorNeeded].map((row) =>
    toPredictionJob(row, predictedAt),
  );
  // A multi-race batch is either stale pending work or a newly published
  // date.  Let one bounded batch through when an old queue backlog is above
  // the cap; otherwise the planner can leave the date pending forever because
  // its own messages never enter the queue.  The one-race case stays behind
  // the normal cap and the queue-metrics failure gate remains fail-closed.
  const allowRecoveryOverflow = predictionJobs.length > 1;
  const dispatchGate: RunningStyleQueueDispatchGate =
    predictionJobs.length === 0
      ? { blocked: false }
      : await inspectRunningStyleQueueDispatch(
          runningStyleQueue,
          predictionJobs.length,
          allowRecoveryOverflow,
        );
  if (dispatchGate.blocked) {
    return {
      alreadyQueued: selected.alreadyQueued,
      completed: selected.completed - mirrorNeeded.length,
      date,
      enqueued: 0,
      featureReady: selected.featureReady,
      missingFeatures: selected.missingFeatures,
      planError:
        [predictionCountError, ...foundationGate.errors, dispatchGate.error]
          .filter((error): error is string => error !== undefined)
          .join("; ") || undefined,
      scanned: registeredRaces.length,
    };
  }
  // Never reset sync-failed rows to pending: their D1 predictions are
  // complete and the fast path in handleRunningStylePredictionJob retries
  // only the Neon mirror. Resetting would wipe written_horse_count and
  // force a full re-inference (and re-classify the race as never-run).
  const pendingUpsertRaces = foundationReadyRaces.filter(
    (row) => states.get(row.raceKey)?.status !== "sync-failed",
  );
  await upsertRunningStylePendingStates(env.REALTIME_DB, pendingUpsertRaces, predictedAt);
  const sendResult = await sendPredictionJobs(runningStyleQueue, predictionJobs);
  await restoreFailedPendingStates(env, pendingUpsertRaces, sendResult.failed, predictedAt);
  const queueError = formatPredictionJobSendFailures(sendResult.failed);
  return {
    alreadyQueued: selected.alreadyQueued,
    completed: selected.completed - mirrorNeeded.length,
    date,
    enqueued: sendResult.sentCount,
    featureReady: selected.featureReady,
    missingFeatures: selected.missingFeatures,
    planError:
      [
        predictionCountError,
        ...foundationGate.errors,
        ...(queueError === undefined ? [] : [queueError]),
      ]
        .filter((error): error is string => error !== undefined)
        .join("; ") || undefined,
    scanned: registeredRaces.length,
  };
};

const EMPTY_PLAN_SUMMARY = (date: string, error: string): RunningStylePlanSummary => ({
  alreadyQueued: 0,
  completed: 0,
  date,
  enqueued: 0,
  featureReady: 0,
  missingFeatures: 0,
  planError: error,
  scanned: 0,
});

const isWithinYesterdaySweepWindow = (now: Date): boolean => {
  const jstHour = (now.getUTCHours() + JST_OFFSET_HOURS) % 24;
  return jstHour < YESTERDAY_SWEEP_HOURS;
};

export const resolveRunningStyleCronDates = (now: Date): string[] => {
  const today = formatYYYYMMDDInJst(now);
  const tomorrow = addDaysToYYYYMMDDInJst(today, 1);
  // Re-audit tomorrow on every inference tick, not only during the nightly
  // prewarm. This gives a Neon mirror failure a bounded ten-minute recovery
  // cadence before race day while the date fence still rejects day+2 onward.
  if (!isWithinYesterdaySweepWindow(now)) {
    return [today, tomorrow];
  }
  const yesterday = addDaysToYYYYMMDDInJst(today, -1);
  return [yesterday, today, tomorrow];
};

const planRunningStyleForDateSafe = async (
  env: Env,
  date: string,
  now: Date,
): Promise<RunningStylePlanSummary> =>
  planRunningStylePredictionsForDate(env, date, now).catch((error: unknown) =>
    EMPTY_PLAN_SUMMARY(date, formatError(error)),
  );

const refreshViewerCacheSafe = async (
  env: Env,
  date: string,
  ctx: ExecutionContext | undefined,
): Promise<ViewerRunningStyleCacheRefreshSummary> =>
  refreshViewerRunningStyleCachesForDate(env, date, ctx).catch((error: unknown) => ({
    date,
    refreshed: 0,
    refreshError: formatError(error),
    scanned: 0,
    skipped: 0,
  }));

const mergeCacheRefresh = (
  current: ViewerRunningStyleCacheRefreshSummary | undefined,
  next: ViewerRunningStyleCacheRefreshSummary,
): ViewerRunningStyleCacheRefreshSummary => {
  if (current === undefined) return next;
  return {
    date: next.date,
    refreshed: current.refreshed + next.refreshed,
    refreshError: next.refreshError ?? current.refreshError,
    scanned: current.scanned + next.scanned,
    skipped: current.skipped + next.skipped,
  };
};

const SOURCES_FOR_PARQUET_EXPORT: ReadonlyArray<"jra" | "nar"> = ["jra", "nar"];

export const exportRunningStyleParquetsForDate = async (
  env: Env,
  date: string,
): Promise<RunningStyleParquetExportSummary> => {
  const exportOne = async (
    source: "jra" | "nar",
  ): Promise<ExportRunningStyleParquetResult | { exportError: string }> =>
    exportRunningStyleParquetForDay({ dateYmd: date, env, source }).catch((error: unknown) => ({
      exportError: formatError(error),
    }));
  const results = await Promise.all(SOURCES_FOR_PARQUET_EXPORT.map(exportOne));
  const ok = results.filter((r): r is ExportRunningStyleParquetResult => !("exportError" in r));
  const firstError = results.find((r): r is { exportError: string } => "exportError" in r);
  return {
    bytesWritten: ok.reduce((sum, r) => sum + r.bytesWritten, 0),
    exportError: firstError?.exportError,
    fileCount: ok.reduce((sum, r) => sum + r.fileCount, 0),
    rowCount: ok.reduce((sum, r) => sum + r.rowCount, 0),
  };
};

const mergeParquetExport = (
  current: RunningStyleParquetExportSummary | undefined,
  next: RunningStyleParquetExportSummary,
): RunningStyleParquetExportSummary => {
  if (current === undefined) return next;
  return {
    bytesWritten: current.bytesWritten + next.bytesWritten,
    exportError: next.exportError ?? current.exportError,
    fileCount: current.fileCount + next.fileCount,
    rowCount: current.rowCount + next.rowCount,
  };
};

const resolveMergedParquetExport = (
  current: RunningStyleParquetExportSummary | undefined,
  next: RunningStyleParquetExportSummary | undefined,
): RunningStyleParquetExportSummary | undefined => {
  if (current === undefined && next === undefined) return undefined;
  if (current === undefined) return next;
  if (next === undefined) return current;
  return mergeParquetExport(current, next);
};

const mergeRunningStylePlan = (
  current: RunningStylePlanSummary | undefined,
  next: RunningStylePlanSummary,
): RunningStylePlanSummary => {
  if (current === undefined) return next;
  return {
    alreadyQueued: current.alreadyQueued + next.alreadyQueued,
    completed: current.completed + next.completed,
    date: next.date,
    enqueued: current.enqueued + next.enqueued,
    featureReady: current.featureReady + next.featureReady,
    missingFeatures: current.missingFeatures + next.missingFeatures,
    parquetExport: resolveMergedParquetExport(current.parquetExport, next.parquetExport),
    planError: next.planError ?? current.planError,
    scanned: current.scanned + next.scanned,
  };
};

interface RunningStyleCronAccumulator {
  cacheRefresh: ViewerRunningStyleCacheRefreshSummary | undefined;
  plan: RunningStylePlanSummary | undefined;
}

const planAndRefreshForDate = async (
  env: Env,
  date: string,
  now: Date,
  ctx: ExecutionContext | undefined,
  acc: RunningStyleCronAccumulator,
): Promise<RunningStyleCronAccumulator> => {
  const plan = await planRunningStyleForDateSafe(env, date, now);
  const cacheRefresh = await refreshViewerCacheSafe(env, date, ctx);
  const parquetExport = isInferenceEnabled(env)
    ? await exportRunningStyleParquetsForDate(env, date)
    : undefined;
  const planWithExport: RunningStylePlanSummary =
    parquetExport === undefined ? plan : { ...plan, parquetExport };
  return {
    cacheRefresh: mergeCacheRefresh(acc.cacheRefresh, cacheRefresh),
    plan: mergeRunningStylePlan(acc.plan, planWithExport),
  };
};

export const runRunningStyleCronTick = async (
  env: Env,
  now: Date,
  ctx?: ExecutionContext,
): Promise<RunningStylePlanSummary> => {
  const dates = resolveRunningStyleCronDates(now);
  const aggregated = await dates.reduce<Promise<RunningStyleCronAccumulator>>(
    async (accPromise, date) => {
      const acc = await accPromise;
      return planAndRefreshForDate(env, date, now, ctx, acc);
    },
    Promise.resolve({ cacheRefresh: undefined, plan: undefined }),
  );
  // resolveRunningStyleCronDates always yields at least today, so plan +
  // cacheRefresh are guaranteed to be populated after the reduce above.
  return { ...aggregated.plan!, cacheRefresh: aggregated.cacheRefresh! };
};

export const refreshViewerRunningStyleCacheForRace = async (
  env: Env,
  raceKey: string,
  ctx?: ExecutionContext,
): Promise<boolean> => {
  const race = parseRunningStyleRaceKey(raceKey);
  if (race === null) {
    return false;
  }
  const rows = await listRaceRunningStylesForRace(env.REALTIME_DB, raceKey, {
    bypassCache: true,
    ctx,
  });
  if (rows.length === 0) {
    return false;
  }
  return putViewerRunningStyleRaceCache({
    ctx,
    env,
    race,
    rows,
  });
};

export interface ViewerRunningStyleCacheRefreshSummary {
  date: string;
  refreshed: number;
  refreshError?: string;
  scanned: number;
  skipped: number;
}

export const refreshViewerRunningStyleCachesForDate = async (
  env: Env,
  date: string,
  ctx?: ExecutionContext,
): Promise<ViewerRunningStyleCacheRefreshSummary> => {
  const { races: registeredRaces } = await listRunningStyleRacesByDate(env, date);
  if (!isInferenceEnabled(env) || registeredRaces.length === 0) {
    return {
      date,
      refreshed: 0,
      scanned: registeredRaces.length,
      skipped: registeredRaces.length,
    };
  }

  const raceKeys = registeredRaces.map((row) =>
    buildRunningStyleRaceKey({
      kaisaiNen: row.kaisai_nen,
      kaisaiTsukihi: row.kaisai_tsukihi,
      keibajoCode: row.keibajo_code,
      raceBango: row.race_bango,
      source: row.source,
    }),
  );
  const predictionCounts = await listRaceRunningStyleCounts(env.REALTIME_DB, raceKeys, {
    bypassCache: true,
    ctx,
  });
  let refreshed = 0;
  let skipped = 0;

  for (const row of registeredRaces) {
    const race = {
      kaisaiNen: row.kaisai_nen,
      kaisaiTsukihi: row.kaisai_tsukihi,
      keibajoCode: normalizeKeibajoCode(row.keibajo_code),
      raceBango: normalizeRaceBango(row.race_bango),
      source: row.source,
    };
    const raceKey = buildRunningStyleRaceKey(race);
    const existingHorseCount = predictionCounts.get(raceKey) ?? 0;
    if (existingHorseCount === 0) {
      skipped += 1;
      continue;
    }
    const rows = await listRaceRunningStylesForRace(env.REALTIME_DB, raceKey, {
      bypassCache: true,
      ctx,
    });
    if (rows.length === 0) {
      skipped += 1;
      continue;
    }
    const cacheWritten = await putViewerRunningStyleRaceCache({
      ctx,
      env,
      race: { ...race, raceKey },
      rows,
    });
    if (cacheWritten) {
      refreshed += 1;
    } else {
      skipped += 1;
    }
  }

  return {
    date,
    refreshed,
    scanned: registeredRaces.length,
    skipped,
  };
};
