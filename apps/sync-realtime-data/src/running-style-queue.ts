// Run with bun. Queue consumer for per-race running-style generation.
// The Worker builds the feature JSONL file, stores it in R2, then writes
// the prediction rows to D1.

import { getFinishPositionPool } from "./finish-position-lite-pool";
import {
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
  featuresR2Key: string;
  horseCount: number;
  modelVersion: string;
  writtenCount: number;
}

export const handleRunningStylePredictionJob = async (
  env: Env,
  job: RunningStylePredictionJob,
): Promise<RunningStylePredictionJobSummary | null> => {
  if (env.RUNNING_STYLE_D1_WRITE_ENABLED !== ENABLED_FLAG) {
    return null;
  }
  const raceKey = buildRunningStyleRaceKey(job);
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
    return {
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
