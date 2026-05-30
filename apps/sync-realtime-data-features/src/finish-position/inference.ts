// Run with bun. Queue handler skeleton for finish-position prediction.
// TODO(next phase): replace the placeholder predictor with the real LightGBM
// flat-model inference. For now we use umaban-as-predicted-position so the
// downstream persistence path is exercised end-to-end.

import { decodeRaceFeaturesParquet } from "../features/parquet";
import { buildRaceParquetR2Key } from "../features/r2-key";
import { upsertFinishPositionInferenceState, upsertFinishPositionPredictions } from "../storage";
import type {
  DailyRaceEntryRow,
  Env,
  FinishPositionPredictionEntry,
  Job,
  RaceJobKey,
} from "../types";

const PREDICTOR_VERSION = "skeleton-v0";
const DEFAULT_PROBABILITY = 0.1;

// Auto-recovery helper: enqueue a build-race-features job so the next
// inference attempt finds a parquet in R2. Shape mirrors toBuildJobMessage
// in worker.ts exactly.
const enqueueBuildRaceFeaturesJob = async (job: RaceJobKey, env: Env): Promise<void> => {
  const message: Extract<Job, { type: "build-race-features" }> = {
    kaisaiNen: job.kaisaiNen,
    kaisaiTsukihi: job.kaisaiTsukihi,
    keibajoCode: job.keibajoCode,
    raceBango: job.raceBango,
    raceKey: job.raceKey,
    source: job.source,
    type: "build-race-features",
  };
  await env.REALTIME_FEATURES_JOBS.send(message);
};

// Use loose `==` against null so both `null` AND runtime `undefined` are
// treated as "no value". TypeScript types `umaban` as `number | null`, but
// hyparquet can leak `undefined` for optional int32 columns, which then
// flows into JSON.stringify (where undefined is silently dropped) — not a
// D1 type error in this handler, but we still skip the entry to avoid
// emitting incomplete predictions arrays downstream.
const toPredictionEntry = (row: DailyRaceEntryRow): FinishPositionPredictionEntry | null => {
  if (row.umaban == null) {
    return null;
  }
  return {
    horse_number: row.umaban,
    predicted_position: row.umaban,
    probability: DEFAULT_PROBABILITY,
  };
};

export interface FinishPositionPredictionResult {
  raceKey: string;
  predictionsCount: number;
}

export const handleFinishPositionPredictionJob = async (
  job: RaceJobKey & { predictedAt: string },
  env: Env,
): Promise<FinishPositionPredictionResult> => {
  const r2Key = buildRaceParquetR2Key(job);
  const object = await env.FEATURES_ARCHIVE.get(r2Key);
  if (!object) {
    await upsertFinishPositionInferenceState(env.REALTIME_FEATURES_DB, {
      raceKey: job.raceKey,
      source: job.source,
      kaisaiNen: job.kaisaiNen,
      kaisaiTsukihi: job.kaisaiTsukihi,
      keibajoCode: job.keibajoCode,
      raceBango: job.raceBango,
      status: "missing-parquet",
      predictionsR2Key: r2Key,
      modelVersion: PREDICTOR_VERSION,
      attemptedAt: job.predictedAt,
      completedAt: null,
      errorMessage: "features parquet not found in R2",
    });
    // Auto-recovery: trigger a feature build so the next inference attempt
    // has a parquet ready in R2.
    await enqueueBuildRaceFeaturesJob(job, env);
    return { predictionsCount: 0, raceKey: job.raceKey };
  }
  const bytes = new Uint8Array(await object.arrayBuffer());
  const rows = await decodeRaceFeaturesParquet(bytes);
  const predictions = rows
    .map(toPredictionEntry)
    .filter((entry): entry is FinishPositionPredictionEntry => entry !== null);
  await upsertFinishPositionPredictions(env.REALTIME_FEATURES_DB, {
    raceKey: job.raceKey,
    source: job.source,
    predictionsJson: JSON.stringify(predictions),
    predictedAt: job.predictedAt,
    predictorVersion: PREDICTOR_VERSION,
  });
  await upsertFinishPositionInferenceState(env.REALTIME_FEATURES_DB, {
    raceKey: job.raceKey,
    source: job.source,
    kaisaiNen: job.kaisaiNen,
    kaisaiTsukihi: job.kaisaiTsukihi,
    keibajoCode: job.keibajoCode,
    raceBango: job.raceBango,
    status: "completed",
    predictionsR2Key: r2Key,
    modelVersion: PREDICTOR_VERSION,
    attemptedAt: job.predictedAt,
    completedAt: new Date().toISOString(),
    errorMessage: null,
  });
  return { predictionsCount: predictions.length, raceKey: job.raceKey };
};
