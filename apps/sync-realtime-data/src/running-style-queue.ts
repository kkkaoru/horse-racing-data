// Run with bun. Queue consumer for per-race running-style inference.
// The Worker reads the per-race feature Parquet from R2 first and only rebuilds
// it from PostgreSQL on a miss, then writes flatbin model predictions to D1
// and mirrors them to the Neon race_running_style_model_predictions table so
// the viewer can read predictions without a separate sync step.

import { markFinishPositionFeaturesCached } from "./finish-position-d1";
import { formatError } from "./format-error";
import { putFinishPositionInputsCache } from "./finish-position-inputs-cache";
import { getFinishPositionPool } from "./finish-position-lite-pool";
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
} from "./running-style-features";
import { runRunningStyleInferenceRowsWithFlatModel } from "./running-style-inference";
import {
  buildRunningStyleFlatModelKey,
  loadFlatLightGBMModelFromR2,
} from "./running-style-model-binary";
import {
  buildCalibrationR2Key,
  loadCalibratorsFromR2,
  type RunningStyleCalibrationTable,
} from "./running-style-calibration";
import { upsertRunningStylePredictionsToNeon } from "./running-style-neon";
import { getLatestRaceEntries } from "./storage";
import type { Env, RunningStylePredictionJob } from "./types";

const ENABLED_FLAG = "1";

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

const cacheAndSyncCompletedRunningStyles = async (
  env: Env,
  job: RunningStylePredictionJob,
): Promise<{
  cacheError?: string;
  cacheWritten: boolean;
  neonError?: string;
  neonWrittenCount: number;
}> => {
  const pool = getFinishPositionPool(env);
  try {
    const rows = await listRaceRunningStylesForRace(env.REALTIME_DB, buildRunningStyleRaceKey(job));
    if (rows.length === 0) {
      return { cacheWritten: false, neonWrittenCount: 0 };
    }
    const [cacheWritten, neonResult] = await Promise.all([
      putViewerRunningStyleRaceCache({ env, race: job, rows }).catch((error: unknown) => {
        console.error("Running-style cache write failed:", formatError(error));
        return false;
      }),
      upsertRunningStylePredictionsToNeon(pool, rows).catch((error: unknown) => {
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
    return {
      ...cacheResult,
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
    const latestEntries = await getLatestRaceEntries(
      env.REALTIME_DB,
      buildRealtimeRaceKeyFromRunningStyle(job),
    );
    const modelKey = buildRunningStyleFlatModelKey(job.source);
    const model = await loadFlatLightGBMModelFromR2(env.RUNNING_STYLE_MODELS, modelKey);
    const calibrators = await tryLoadCalibrators(env.RUNNING_STYLE_MODELS, job.source);
    const featureNames = model.header.feature_names;
    const loadOrBuild = await loadOrBuildRunningStyleFeatureParquet({
      env,
      featureNames,
      pool,
      race: job,
    });
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
    return {
      ...cacheResult,
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
