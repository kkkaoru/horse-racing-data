// Run with bun. Queue consumer for per-race running-style generation.
// The Worker builds the feature JSONL file, stores it in R2, then writes
// the prediction rows to D1.

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
  buildRunningStyleFeaturesKey,
  buildRunningStyleModelKey,
  buildRunningStyleRaceKey,
  loadRunningStyleFeaturesForRace,
  writeRunningStyleFeaturesToR2,
} from "./running-style-features";
import { runRunningStyleInferenceForRows } from "./running-style-inference";
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
    const rows = await loadRunningStyleFeaturesForRace(pool, job);
    if (rows.length === 0) {
      throw new Error(`no running-style feature rows found for race ${raceKey}`);
    }
    const featuresR2Key = buildRunningStyleFeaturesKey(job);
    await writeRunningStyleFeaturesToR2(env.RUNNING_STYLE_MODELS, featuresR2Key, rows);
    const summary = await runRunningStyleInferenceForRows(
      env.RUNNING_STYLE_MODELS,
      env.REALTIME_DB,
      {
        modelKey: buildRunningStyleModelKey(job.source),
        predictedAt: job.predictedAt,
        rows,
      },
    );
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
