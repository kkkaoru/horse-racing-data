// Run with bun. Queue handler stub for running-style prediction.
// Real model loading + flat LightGBM inference will be ported in a later
// Phase. Until then this handler MUST NOT write predicted rows: the previous
// skeleton hard-coded predicted_label="senkou" for every horse, and the
// viewer was reading those rows in preference to the real inference output
// from sync-realtime-data's REALTIME_DB. The viewer side now reads REALTIME_DB
// first, and this writer is disabled so features-db stops accumulating
// all-senkou stub rows. Existing stub rows are NOT deleted
// (feedback_no_data_delete).

import { decodeRaceFeaturesParquet } from "../features/parquet";
import { buildRaceParquetR2Key } from "../features/r2-key";
import { upsertRunningStyleInferenceState } from "../storage";
import type { Env, Job, RaceJobKey } from "../types";

const RUNNING_STYLE_MODEL_VERSION = "skeleton-v0";
const SKELETON_DISABLED_STATUS = "skeleton-disabled";
const SKELETON_DISABLED_MESSAGE =
  "skeleton inference disabled to prevent all-senkou stub leak (real LightGBM inference not yet ported)";

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

export interface RunningStylePredictionResult {
  raceKey: string;
  writtenCount: number;
}

export const handleRunningStylePredictionJob = async (
  job: RaceJobKey & { predictedAt: string },
  env: Env,
): Promise<RunningStylePredictionResult> => {
  const r2Key = buildRaceParquetR2Key(job);
  const object = await env.FEATURES_ARCHIVE.get(r2Key);
  if (!object) {
    await upsertRunningStyleInferenceState(env.REALTIME_FEATURES_DB, {
      raceKey: job.raceKey,
      source: job.source,
      kaisaiNen: job.kaisaiNen,
      kaisaiTsukihi: job.kaisaiTsukihi,
      keibajoCode: job.keibajoCode,
      raceBango: job.raceBango,
      status: "missing-parquet",
      featuresR2Key: r2Key,
      modelVersion: RUNNING_STYLE_MODEL_VERSION,
      expectedHorseCount: null,
      writtenHorseCount: 0,
      attemptedAt: job.predictedAt,
      completedAt: null,
      errorMessage: "features parquet not found in R2",
    });
    // Auto-recovery: trigger a feature build so the next inference attempt
    // has a parquet ready in R2.
    await enqueueBuildRaceFeaturesJob(job, env);
    return { raceKey: job.raceKey, writtenCount: 0 };
  }
  // The parquet is present, but the real LightGBM inference path is not
  // ported yet. We deliberately skip `upsertRunningStyle` so that
  // features-db.race_running_styles stops receiving all-senkou stub rows.
  // The inference_state row is still written with a dedicated
  // "skeleton-disabled" status so the orchestration layer can observe that
  // the job ran (and did not silently no-op).
  const bytes = new Uint8Array(await object.arrayBuffer());
  const rows = await decodeRaceFeaturesParquet(bytes);
  await upsertRunningStyleInferenceState(env.REALTIME_FEATURES_DB, {
    raceKey: job.raceKey,
    source: job.source,
    kaisaiNen: job.kaisaiNen,
    kaisaiTsukihi: job.kaisaiTsukihi,
    keibajoCode: job.keibajoCode,
    raceBango: job.raceBango,
    status: SKELETON_DISABLED_STATUS,
    featuresR2Key: r2Key,
    modelVersion: RUNNING_STYLE_MODEL_VERSION,
    expectedHorseCount: rows.length,
    writtenHorseCount: 0,
    attemptedAt: job.predictedAt,
    completedAt: new Date().toISOString(),
    errorMessage: SKELETON_DISABLED_MESSAGE,
  });
  return { raceKey: job.raceKey, writtenCount: 0 };
};
