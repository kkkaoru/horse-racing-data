// Run with bun. Queue consumer for per-race running-style generation.
// The Worker builds one-race 117-feature Parquet files, stores them in R2,
// reads the stored file back, then writes flatbin model predictions to D1.

import { getFinishPositionPool } from "./finish-position-lite-pool";
import { putRunningStyleCache } from "./running-style-cache";
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
import { buildRunningStyleFeaturesForRaceFromPostgres } from "./running-style-feature-sql";
import { buildRunningStyleRaceKey } from "./running-style-features";
import { runRunningStyleInferenceRowsWithFlatModel } from "./running-style-inference";
import {
  buildRunningStyleFlatModelKey,
  loadFlatLightGBMModelFromR2,
} from "./running-style-model-binary";
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
  expectedHorseCount: number,
): Promise<{ cacheError?: string; cacheWritten: boolean }> => {
  try {
    const rows = await listRaceRunningStylesForRace(env.REALTIME_DB, buildRunningStyleRaceKey(job));
    if (rows.length !== expectedHorseCount) {
      return { cacheWritten: false };
    }
    return {
      cacheWritten: await putRunningStyleCache({
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
    const cacheResult = await cacheCompletedRunningStyles(env, job, state.expectedHorseCount);
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
    const modelKey = buildRunningStyleFlatModelKey(job.source);
    const model = await loadFlatLightGBMModelFromR2(env.RUNNING_STYLE_MODELS, modelKey);
    const featureNames = model.header.feature_names;
    const built = await buildRunningStyleFeaturesForRaceFromPostgres(pool, job, featureNames);
    if (built.rows.length === 0) {
      throw new Error(`no running-style feature rows found for race ${raceKey}`);
    }
    const coverage = validateFeatureCoverage(built.rows, featureNames);
    if (coverage.missingFeatureNames.length > 0) {
      throw new Error(
        `PostgreSQL feature build missing model features: ${coverage.missingFeatureNames.join(", ")}`,
      );
    }
    const featuresR2Key = buildRunningStyleFeatureParquetKey(job);
    await putRunningStyleFeatureParquet(
      env.RUNNING_STYLE_MODELS,
      featuresR2Key,
      built.rows,
      featureNames,
    );
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
      expectedHorseCount: rows.length,
      featuresR2Key,
      modelVersion: summary.modelVersion,
      raceKey,
      writtenHorseCount: summary.writtenCount,
    });
    const cacheResult =
      summary.writtenCount === rows.length
        ? await cacheCompletedRunningStyles(env, job, rows.length)
        : { cacheWritten: false };
    return {
      ...cacheResult,
      featuresR2Key,
      horseCount: rows.length,
      modelVersion: summary.modelVersion,
      raceKey,
      writtenCount: summary.writtenCount,
    };
  } catch (error) {
    await markRunningStyleInferenceFailed(env.REALTIME_DB, raceKey, error);
    throw error;
  }
};
