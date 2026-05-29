// Run with bun. Queue handler skeleton for running-style prediction.
// Real model loading + flat LightGBM inference will be ported in a later Phase;
// this stub demonstrates the persist path and DailyRaceEntryRow → row mapping.

import { decodeRaceFeaturesParquet } from "../features/parquet";
import { buildRaceParquetR2Key } from "../features/r2-key";
import { upsertRunningStyle, upsertRunningStyleInferenceState } from "../storage";
import type { DailyRaceEntryRow, Env, RaceJobKey, RunningStyleRow } from "../types";

const RUNNING_STYLE_MODEL_VERSION = "skeleton-v0";
const DEFAULT_PROBABILITY = 0.25;
const DEFAULT_LABEL = "senkou";

const toRunningStyleRow = (
  row: DailyRaceEntryRow,
  job: RaceJobKey,
  predictedAt: string,
): RunningStyleRow | null => {
  if (row.umaban === null) {
    return null;
  }
  return {
    raceKey: job.raceKey,
    horseNumber: row.umaban,
    kettoTorokuBango: row.ketto_toroku_bango,
    bamei: row.bamei,
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
