// Run with bun. Queue consumer for per-race running-style generation.
// The Worker builds one-race 117-feature Parquet files, stores them in R2,
// reads the stored file back, then writes flatbin model predictions to D1.

import { markFinishPositionFeaturesCached } from "./finish-position-d1";
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
import {
  buildRunningStyleFeatureParquetKey,
  loadRunningStyleFeatureParquet,
  putRunningStyleFeatureParquet,
  validateFeatureCoverage,
} from "./running-style-feature-parquet";
import { listDailyRaceEntriesForRace } from "./daily-feature-build";
import { buildRunningStyleFeaturesForRaceFromD1Target } from "./running-style-feature-sql";
import {
  buildRealtimeRaceKeyFromRunningStyle,
  buildRunningStyleRaceKey,
} from "./running-style-features";
import { runRunningStyleInferenceRowsWithFlatModel } from "./running-style-inference";
import {
  buildRunningStyleFlatModelKey,
  loadFlatLightGBMModelFromR2,
} from "./running-style-model-binary";
import { getLatestRaceEntries } from "./storage";
import type { Env, RunningStylePredictionJob } from "./types";

const ENABLED_FLAG = "1";

export interface RunningStylePredictionJobSummary {
  raceKey: string;
  cacheError?: string;
  cacheWritten?: boolean;
  featuresR2Key: string;
  horseCount: number;
  modelVersion: string;
  skipped?: boolean;
  writtenCount: number;
}

const cacheCompletedRunningStyles = async (
  env: Env,
  job: RunningStylePredictionJob,
): Promise<{ cacheError?: string; cacheWritten: boolean }> => {
  try {
    const rows = await listRaceRunningStylesForRace(env.REALTIME_DB, buildRunningStyleRaceKey(job));
    if (rows.length === 0) {
      return { cacheWritten: false };
    }
    return {
      cacheWritten: await putViewerRunningStyleRaceCache({
        env,
        race: job,
        rows,
      }),
    };
  } catch (error) {
    return {
      cacheError: error instanceof Error ? error.message : String(error),
      cacheWritten: false,
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
    const cacheResult = await cacheCompletedRunningStyles(env, job);
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
    const featureNames = model.header.feature_names;
    const dailyTargetRows = await listDailyRaceEntriesForRace(env.REALTIME_DB, job);
    if (dailyTargetRows.length === 0) {
      throw new Error(
        `no D1 daily_race_entries rows for race ${raceKey}; run build-daily-features`,
      );
    }
    const built = await buildRunningStyleFeaturesForRaceFromD1Target(
      pool,
      job,
      featureNames,
      dailyTargetRows,
    );
    if (built.rows.length === 0) {
      throw new Error(`no running-style feature rows found for race ${raceKey}`);
    }
    const inferenceRows = filterRunningStyleFeatureRowsByActiveEntries(built.rows, latestEntries);
    if (inferenceRows.length === 0) {
      throw new Error(`no active running-style feature rows found for race ${raceKey}`);
    }
    const expectedHorseCount = resolveRunningStyleExpectedHorseCount(
      inferenceRows.length,
      latestEntries,
    );
    const coverage = validateFeatureCoverage(inferenceRows, featureNames);
    if (coverage.missingFeatureNames.length > 0) {
      throw new Error(
        `PostgreSQL feature build missing model features: ${coverage.missingFeatureNames.join(", ")}`,
      );
    }
    const featuresR2Key = buildRunningStyleFeatureParquetKey(job);
    await putRunningStyleFeatureParquet(
      env.RUNNING_STYLE_MODELS,
      featuresR2Key,
      inferenceRows,
      featureNames,
    );
    const completedAt = new Date().toISOString();
    await markFinishPositionFeaturesCached(env.REALTIME_DB, job, {
      attemptedAt: job.predictedAt,
      completedAt,
      featuresR2Key,
      modelVersion: model.header.model_version,
    });
    await putFinishPositionInputsCache({
      env,
      payload: {
        featuresR2Key,
        modelVersion: model.header.model_version,
        raceKey,
      },
      race: job,
    });
    const rows = await loadRunningStyleFeatureParquet(
      env.RUNNING_STYLE_MODELS,
      featuresR2Key,
      featureNames,
    );
    const summary = await runRunningStyleInferenceRowsWithFlatModel(env.REALTIME_DB, {
      model,
      predictedAt: job.predictedAt,
      rows,
    });
    await markRunningStyleInferenceCompleted(env.REALTIME_DB, {
      completedAt: new Date().toISOString(),
      expectedHorseCount,
      featuresR2Key,
      modelVersion: summary.modelVersion,
      raceKey,
      writtenHorseCount: summary.writtenCount,
    });
    const cacheResult =
      summary.writtenCount >= expectedHorseCount
        ? await cacheCompletedRunningStyles(env, job)
        : { cacheWritten: false };
    return {
      ...cacheResult,
      featuresR2Key,
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
