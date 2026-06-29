// Run with bun. Queue consumer for per-race running-style inference.
// The Worker reads the per-race feature Parquet from R2 first and only rebuilds
// it from PostgreSQL on a miss, then writes flatbin model predictions to D1
// and mirrors them to the Neon race_running_style_model_predictions table so
// the viewer can read predictions without a separate sync step.

import { markFinishPositionFeaturesCached } from "./finish-position-d1";
import { formatError } from "./format-error";
import { putFinishPositionInputsCache } from "./finish-position-inputs-cache";
import { getFinishPositionPool, getFinishPositionWritePool } from "./finish-position-lite-pool";
import {
  filterRunningStyleFeatureRowsByActiveEntries,
  resolveRunningStyleExpectedHorseCount,
} from "./running-style-expected-horses";
import { putViewerRunningStyleRaceCache } from "./viewer-running-style-cache";
import {
  getRunningStyleInferenceState,
  listRaceRunningStylesForRace,
  markRunningStyleInferenceCompleted,
  markRunningStyleInferenceFailed,
  markRunningStyleInferenceProcessing,
} from "./running-style-d1";
import { loadOrBuildRunningStyleFeatureParquet } from "./running-style-feature-materialize";
import {
  buildRealtimeRaceKeyFromRunningStyle,
  buildRunningStyleRaceKey,
  normalizeKeibajoCode,
  normalizeRaceBango,
} from "./running-style-features";
import { runRunningStyleInferenceRowsWithFlatModel } from "./running-style-inference";
import { loadFlatLightGBMModelFromR2 } from "./running-style-model-binary";
import {
  buildCalibrationR2Key,
  loadCalibratorsFromR2,
  type RunningStyleCalibrationTable,
} from "./running-style-calibration";
import {
  resolveRunningStyleCellRoute,
  type RunningStyleCellCategory,
  type RunningStyleCellRoute,
  type RunningStyleCellRoutingConfig,
} from "./running-style-cell-router";
import { upsertRunningStylePredictionsToNeon } from "./running-style-neon";
import type { RaceHorseFeatureRow } from "./running-style-r2";
import { getLatestRaceEntries } from "./storage";
import type { Env, RunningStylePredictionJob } from "./types";

const ENABLED_FLAG = "1";
const FINISH_POSITION_CRON_RUN_URL = "https://finish-position-cron.internal/run";

const tryLoadCalibrators = async (
  bucket: R2Bucket,
  source: "jra" | "nar",
): Promise<RunningStyleCalibrationTable | undefined> => {
  try {
    return await loadCalibratorsFromR2(bucket, buildCalibrationR2Key(source));
  } catch {
    console.error("Failed to load running-style calibrators, falling back to uncalibrated");
    return undefined;
  }
};

export interface RunningStylePredictionJobSummary {
  cellModelKey?: string;
  cellVariantId?: string;
  raceKey: string;
  cacheError?: string;
  cacheWritten?: boolean;
  featuresR2Key: string;
  horseCount: number;
  modelVersion: string;
  neonError?: string;
  neonWrittenCount?: number;
  skipped?: boolean;
  writtenCount: number;
}

interface CacheAndSyncRunningStylesResult {
  cacheError?: string;
  cacheWritten: boolean;
  neonError?: string;
  neonWrittenCount: number;
}

const runningStyleCellRoutingConfig = (env: Env): RunningStyleCellRoutingConfig => {
  if (env.RUNNING_STYLE_CELL_ROUTING_JSON === undefined) return {};
  return JSON.parse(env.RUNNING_STYLE_CELL_ROUTING_JSON) as RunningStyleCellRoutingConfig;
};

const routeInputFromFeatureRow = (job: RunningStylePredictionJob, row: RaceHorseFeatureRow) => ({
  category: row.category,
  gradeCode: row.gradeCode,
  kaisaiNen: job.kaisaiNen,
  kaisaiTsukihi: job.kaisaiTsukihi,
  keibajoCode: job.keibajoCode,
  kyori: row.kyori,
  kyosoJokenCode: row.kyosoJokenCode,
  narSubClass: row.narSubClass,
  raceBango: job.raceBango,
  shussoTosu: row.shussoTosu,
  source: job.source,
  trackCode: row.trackCode,
});

const routeInputFromJob = (job: RunningStylePredictionJob) => ({
  kaisaiNen: job.kaisaiNen,
  kaisaiTsukihi: job.kaisaiTsukihi,
  keibajoCode: job.keibajoCode,
  raceBango: job.raceBango,
  source: job.source,
});

const resolveRouteFromRows = (
  job: RunningStylePredictionJob,
  rows: ReadonlyArray<RaceHorseFeatureRow>,
  config: RunningStyleCellRoutingConfig,
): RunningStyleCellRoute => {
  const firstRow = rows[0];
  if (firstRow === undefined) return resolveRunningStyleCellRoute(routeInputFromJob(job), config);
  return resolveRunningStyleCellRoute(routeInputFromFeatureRow(job, firstRow), config);
};

const triggerFinishPositionFullRun = async (
  env: Env,
  job: RunningStylePredictionJob,
  category: RunningStyleCellCategory,
): Promise<void> => {
  const binding = env.FINISH_POSITION_CRON;
  const token = env.TRIGGER_TOKEN;
  if (binding === undefined) {
    console.error(
      `Finish-position full trigger not sent for ${buildRunningStyleRaceKey(job)}: missing FINISH_POSITION_CRON binding`,
    );
    return;
  }
  if (token === undefined) {
    console.error(
      `Finish-position full trigger not sent for ${buildRunningStyleRaceKey(job)}: missing TRIGGER_TOKEN`,
    );
    return;
  }
  if (token.length === 0) {
    console.error(
      `Finish-position full trigger not sent for ${buildRunningStyleRaceKey(job)}: empty TRIGGER_TOKEN`,
    );
    return;
  }
  const body = {
    category,
    keibajoCode: normalizeKeibajoCode(job.keibajoCode),
    mode: "full",
    raceBango: normalizeRaceBango(job.raceBango),
    runDate: `${job.kaisaiNen}${job.kaisaiTsukihi}`,
    skipDedup: true,
  };
  try {
    const response = await binding.fetch(
      new Request(FINISH_POSITION_CRON_RUN_URL, {
        body: JSON.stringify(body),
        headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
        method: "POST",
      }),
    );
    if (!response.ok) {
      console.error(
        `Finish-position full trigger failed for ${buildRunningStyleRaceKey(job)}: ${response.status}`,
      );
    }
  } catch (error) {
    console.error(
      `Finish-position full trigger threw for ${buildRunningStyleRaceKey(job)}: ${formatError(error)}`,
    );
  }
};

const finishPositionTriggerSkipReason = (
  cacheResult: CacheAndSyncRunningStylesResult,
  expectedHorseCount: number,
  writtenHorseCount: number,
): string | null => {
  if (writtenHorseCount < expectedHorseCount) {
    return `written count ${writtenHorseCount} is below expected horse count ${expectedHorseCount}`;
  }
  if (cacheResult.neonError !== undefined) {
    return `Neon sync failed: ${cacheResult.neonError}`;
  }
  if (cacheResult.neonWrittenCount < expectedHorseCount) {
    return `Neon written count ${cacheResult.neonWrittenCount} is below expected horse count ${expectedHorseCount}`;
  }
  return null;
};

const triggerFinishPositionFullRunWhenReady = async (
  env: Env,
  job: RunningStylePredictionJob,
  category: RunningStyleCellCategory,
  cacheResult: CacheAndSyncRunningStylesResult,
  expectedHorseCount: number,
  writtenHorseCount: number,
): Promise<void> => {
  const reason = finishPositionTriggerSkipReason(
    cacheResult,
    expectedHorseCount,
    writtenHorseCount,
  );
  if (reason !== null) {
    console.log(`finish-position trigger skipped for ${buildRunningStyleRaceKey(job)}: ${reason}`);
    return;
  }
  await triggerFinishPositionFullRun(env, job, category);
};

const cacheAndSyncCompletedRunningStyles = async (
  env: Env,
  job: RunningStylePredictionJob,
): Promise<CacheAndSyncRunningStylesResult> => {
  try {
    const rows = await listRaceRunningStylesForRace(env.REALTIME_DB, buildRunningStyleRaceKey(job));
    if (rows.length === 0) {
      return { cacheWritten: false, neonWrittenCount: 0 };
    }
    const syncRunningStylesToNeon = async () =>
      upsertRunningStylePredictionsToNeon(getFinishPositionWritePool(env), rows);
    const [cacheWritten, neonResult] = await Promise.all([
      putViewerRunningStyleRaceCache({ env, race: job, rows }).catch((error: unknown) => {
        console.error("Running-style cache write failed:", formatError(error));
        return false;
      }),
      syncRunningStylesToNeon().catch((error: unknown) => {
        console.error("Running-style Neon write failed:", formatError(error));
        return formatError(error);
      }),
    ]);
    const neonFailed = typeof neonResult === "string";
    return {
      cacheWritten,
      neonError: neonFailed ? neonResult : undefined,
      neonWrittenCount: neonFailed ? 0 : neonResult,
    };
  } catch (error) {
    return {
      cacheError: formatError(error),
      cacheWritten: false,
      neonWrittenCount: 0,
    };
  }
};

export const handleRunningStylePredictionJob = async (
  env: Env,
  job: RunningStylePredictionJob,
): Promise<RunningStylePredictionJobSummary | null> => {
  if (env.RUNNING_STYLE_D1_WRITE_ENABLED !== ENABLED_FLAG) {
    return null;
  }
  const raceKey = buildRunningStyleRaceKey(job);
  const state = await getRunningStyleInferenceState(env.REALTIME_DB, raceKey);
  if (
    state?.status === "completed" &&
    state.expectedHorseCount !== null &&
    state.writtenHorseCount !== null &&
    state.writtenHorseCount >= state.expectedHorseCount
  ) {
    const cacheResult = await cacheAndSyncCompletedRunningStyles(env, job);
    const route = resolveRunningStyleCellRoute(
      routeInputFromJob(job),
      runningStyleCellRoutingConfig(env),
    );
    await triggerFinishPositionFullRunWhenReady(
      env,
      job,
      route.cell.category,
      cacheResult,
      state.expectedHorseCount,
      state.writtenHorseCount,
    );
    return {
      ...cacheResult,
      cellModelKey: route.modelKey,
      cellVariantId: route.variantId,
      featuresR2Key: state.featuresR2Key ?? "",
      horseCount: state.expectedHorseCount,
      modelVersion: state.modelVersion ?? "completed",
      raceKey,
      skipped: true,
      writtenCount: state.writtenHorseCount,
    };
  }
  await markRunningStyleInferenceProcessing(env.REALTIME_DB, job, new Date().toISOString());
  try {
    const pool = getFinishPositionPool(env);
    const routingConfig = runningStyleCellRoutingConfig(env);
    const latestEntries = await getLatestRaceEntries(
      env.REALTIME_DB,
      buildRealtimeRaceKeyFromRunningStyle(job),
    );
    let selectedRoute = resolveRunningStyleCellRoute(routeInputFromJob(job), routingConfig);
    let model = await loadFlatLightGBMModelFromR2(env.RUNNING_STYLE_MODELS, selectedRoute.modelKey);
    const calibrators = await tryLoadCalibrators(env.RUNNING_STYLE_MODELS, job.source);
    let featureNames = model.header.feature_names;
    let loadOrBuild = await loadOrBuildRunningStyleFeatureParquet({
      env,
      featureNames,
      pool,
      race: job,
    });
    const routeFromRows = resolveRouteFromRows(job, loadOrBuild.rows, routingConfig);
    if (routeFromRows.modelKey !== selectedRoute.modelKey) {
      selectedRoute = routeFromRows;
      model = await loadFlatLightGBMModelFromR2(env.RUNNING_STYLE_MODELS, selectedRoute.modelKey);
      featureNames = model.header.feature_names;
      loadOrBuild = await loadOrBuildRunningStyleFeatureParquet({
        env,
        featureNames,
        pool,
        race: job,
      });
    }
    const inferenceRows = filterRunningStyleFeatureRowsByActiveEntries(
      loadOrBuild.rows,
      latestEntries,
    );
    if (inferenceRows.length === 0) {
      throw new Error(`no active running-style feature rows found for race ${raceKey}`);
    }
    const expectedHorseCount = resolveRunningStyleExpectedHorseCount(
      inferenceRows.length,
      latestEntries,
    );
    const completedAt = new Date().toISOString();
    await markFinishPositionFeaturesCached(env.REALTIME_DB, job, {
      attemptedAt: job.predictedAt,
      completedAt,
      featuresR2Key: loadOrBuild.featuresR2Key,
      modelVersion: model.header.model_version,
    });
    await putFinishPositionInputsCache({
      env,
      payload: {
        featuresR2Key: loadOrBuild.featuresR2Key,
        modelVersion: model.header.model_version,
        raceKey,
      },
      race: job,
    });
    const summary = await runRunningStyleInferenceRowsWithFlatModel(env.REALTIME_DB, {
      calibrators,
      model,
      predictedAt: job.predictedAt,
      rows: inferenceRows,
    });
    await markRunningStyleInferenceCompleted(env.REALTIME_DB, {
      completedAt: new Date().toISOString(),
      expectedHorseCount,
      featuresR2Key: loadOrBuild.featuresR2Key,
      modelVersion: summary.modelVersion,
      raceKey,
      writtenHorseCount: summary.writtenCount,
    });
    const cacheResult =
      summary.writtenCount >= expectedHorseCount
        ? await cacheAndSyncCompletedRunningStyles(env, job)
        : { cacheWritten: false, neonWrittenCount: 0 };
    await triggerFinishPositionFullRunWhenReady(
      env,
      job,
      selectedRoute.cell.category,
      cacheResult,
      expectedHorseCount,
      summary.writtenCount,
    );
    return {
      ...cacheResult,
      cellModelKey: selectedRoute.modelKey,
      cellVariantId: selectedRoute.variantId,
      featuresR2Key: loadOrBuild.featuresR2Key,
      horseCount: inferenceRows.length,
      modelVersion: summary.modelVersion,
      raceKey,
      writtenCount: summary.writtenCount,
    };
  } catch (error) {
    await markRunningStyleInferenceFailed(env.REALTIME_DB, raceKey, error);
    throw error;
  }
};
