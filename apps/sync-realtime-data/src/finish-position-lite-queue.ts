// Run with bun. Queue consumer for the finish-position lite inference jobs
// enqueued by the cron scanner. One job = one race; the consumer loads
// features from Postgres via Hyperdrive, runs the LightGBM evaluator,
// writes the JSONL to R2, and flips the D1 state row to completed (or
// failed with an error message).

import { runFinishPositionLiteInference } from "./finish-position-lite-inference";
import { getFinishPositionPool } from "./finish-position-lite-pool";
import type { Env, FinishPositionLiteJob } from "./types";

const PROCESSING_STATUS = "processing";
const COMPLETED_STATUS = "completed";
const FAILED_STATUS = "failed";

const buildRaceKey = (job: FinishPositionLiteJob): string =>
  `${job.source}:${job.kaisaiNen}${job.kaisaiTsukihi}:${job.keibajoCode}:${job.raceBango}`;

const markProcessing = async (db: D1Database, raceKey: string): Promise<void> => {
  await db
    .prepare(
      `update finish_position_inference_state
       set status = ?, attempted_at = ?, error_message = null
       where race_key = ?`,
    )
    .bind(PROCESSING_STATUS, new Date().toISOString(), raceKey)
    .run();
};

const markCompleted = async (
  db: D1Database,
  raceKey: string,
  predictionsR2Key: string,
): Promise<void> => {
  await db
    .prepare(
      `update finish_position_inference_state
       set status = ?, predictions_r2_key = ?, completed_at = ?, error_message = null
       where race_key = ?`,
    )
    .bind(COMPLETED_STATUS, predictionsR2Key, new Date().toISOString(), raceKey)
    .run();
};

const markFailed = async (db: D1Database, raceKey: string, error: unknown): Promise<void> => {
  const message = error instanceof Error ? error.message : String(error);
  await db
    .prepare(
      `update finish_position_inference_state
       set status = ?, error_message = ?
       where race_key = ?`,
    )
    .bind(FAILED_STATUS, message, raceKey)
    .run();
};

export const handleFinishPositionLiteJob = async (
  env: Env,
  job: FinishPositionLiteJob,
): Promise<void> => {
  const raceKey = buildRaceKey(job);
  await markProcessing(env.REALTIME_DB, raceKey);
  try {
    const pool = getFinishPositionPool(env);
    const result = await runFinishPositionLiteInference(pool, env.RUNNING_STYLE_MODELS, {
      kaisaiNen: job.kaisaiNen,
      kaisaiTsukihi: job.kaisaiTsukihi,
      keibajoCode: job.keibajoCode,
      modelVersion: job.modelVersion,
      predictedAt: job.predictedAt,
      raceBango: job.raceBango,
      source: job.source,
    });
    await markCompleted(env.REALTIME_DB, raceKey, result.predictionsR2Key);
  } catch (error) {
    await markFailed(env.REALTIME_DB, raceKey, error);
    throw error;
  }
};
