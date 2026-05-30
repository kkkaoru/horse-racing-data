// Run with bun. Queue handler skeleton for running-style prediction.
// Real model loading + flat LightGBM inference will be ported in a later Phase;
// this stub demonstrates the persist path and DailyRaceEntryRow → row mapping.

import { decodeRaceFeaturesParquet } from "../features/parquet";
import { buildRaceParquetR2Key } from "../features/r2-key";
import { upsertRunningStyle, upsertRunningStyleInferenceState } from "../storage";
import type { DailyRaceEntryRow, Env, Job, RaceJobKey, RunningStyleRow } from "../types";

const RUNNING_STYLE_MODEL_VERSION = "skeleton-v0";
const DEFAULT_PROBABILITY = 0.25;
const DEFAULT_LABEL = "senkou";

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
// treated as "no value". TypeScript types the parquet row fields as
// `T | null`, but hyparquet can leak `undefined` for optional columns,
// which D1's prepared-statement bind rejects with D1_TYPE_ERROR.
const toRunningStyleRow = (
  row: DailyRaceEntryRow,
  job: RaceJobKey,
  predictedAt: string,
): RunningStyleRow | null => {
  if (row.umaban == null) {
    return null;
  }
  if (row.ketto_toroku_bango == null) {
    return null;
  }
  return {
    raceKey: job.raceKey,
    horseNumber: row.umaban,
    kettoTorokuBango: row.ketto_toroku_bango,
    bamei: row.bamei ?? null,
    category: job.source,
    kaisaiNen: job.kaisaiNen,
    modelVersion: RUNNING_STYLE_MODEL_VERSION,
    pNige: DEFAULT_PROBABILITY,
    pSenkou: DEFAULT_PROBABILITY,
    pSashi: DEFAULT_PROBABILITY,
    pOikomi: DEFAULT_PROBABILITY,
    predictedLabel: DEFAULT_LABEL,
    predictedAt,
  };
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
  const bytes = new Uint8Array(await object.arrayBuffer());
  const rows = await decodeRaceFeaturesParquet(bytes);
  const styleRows = rows
    .map((row) => toRunningStyleRow(row, job, job.predictedAt))
    .filter((row): row is RunningStyleRow => row !== null);
  for (const row of styleRows) {
    await upsertRunningStyle(env.REALTIME_FEATURES_DB, row);
  }
  await upsertRunningStyleInferenceState(env.REALTIME_FEATURES_DB, {
    raceKey: job.raceKey,
    source: job.source,
    kaisaiNen: job.kaisaiNen,
    kaisaiTsukihi: job.kaisaiTsukihi,
    keibajoCode: job.keibajoCode,
    raceBango: job.raceBango,
    status: "completed",
    featuresR2Key: r2Key,
    modelVersion: RUNNING_STYLE_MODEL_VERSION,
    expectedHorseCount: rows.length,
    writtenHorseCount: styleRows.length,
    attemptedAt: job.predictedAt,
    completedAt: new Date().toISOString(),
    errorMessage: null,
  });
  return { raceKey: job.raceKey, writtenCount: styleRows.length };
};
