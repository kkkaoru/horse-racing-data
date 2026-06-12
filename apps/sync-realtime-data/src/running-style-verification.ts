// Run with bun. Admin-only verification flow for Worker-side 117 feature
// Parquet generation and D1 persistence.

import { buildRunningStyleRaceKey, type RunningStyleRaceParams } from "./running-style-features";
import {
  loadRunningStyleFeatureParquet,
  putRunningStyleFeatureParquet,
  runningStyleParquetVerificationKey,
  validateFeatureCoverage,
} from "./running-style-feature-parquet";
import {
  buildRunningStyleFeaturesForRaceFromD1Target,
  buildRunningStyleFeaturesForRaceFromPostgres,
} from "./running-style-feature-sql";
import { listDailyRaceEntriesForRace } from "./daily-feature-build";
import { runRunningStyleInferenceRowsWithFlatModel } from "./running-style-inference";
import { getFinishPositionPool } from "./finish-position-lite-pool";
import {
  buildRunningStyleFlatModelKey,
  loadFlatLightGBMModelFromR2,
} from "./running-style-model-binary";
import {
  buildCalibrationR2Key,
  loadCalibratorsFromR2,
  type RunningStyleCalibrationTable,
} from "./running-style-calibration";
import type { Env } from "./types";

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

export interface RunningStyleVerificationSummary {
  featureBuildMs?: number;
  featureCount: number;
  inputFeaturesKey: string;
  missingCells: number;
  missingFeatureNames: string[];
  modelKey: string;
  modelVersion: string;
  parquetBytes: number;
  parquetKey: string;
  raceKey: string;
  readBackRows: number;
  writtenCount: number;
}

export const parseRunningStylePostgresVerificationParams = (
  url: URL,
): RunningStyleRaceParams | null => {
  const match = url.pathname.match(
    /^\/admin\/running-style\/verify-postgres\/(jra|nar)\/(\d{4})\/(\d{2})\/(\d{2})\/([0-9A-Z]{2})\/(\d{2})$/u,
  );
  if (!match?.[1] || !match[2] || !match[3] || !match[4] || !match[5] || !match[6]) {
    return null;
  }
  return {
    kaisaiNen: match[2],
    kaisaiTsukihi: `${match[3]}${match[4]}`,
    keibajoCode: match[5],
    raceBango: match[6],
    source: match[1] as RunningStyleRaceParams["source"],
  };
};

export const runRunningStyleWorkerPostgresVerification = async (
  env: Env,
  params: RunningStyleRaceParams,
  predictedAt = new Date().toISOString(),
): Promise<RunningStyleVerificationSummary> => {
  const raceKey = buildRunningStyleRaceKey(params);
  const raceDate = `${params.kaisaiNen}${params.kaisaiTsukihi}`;
  const modelKey = buildRunningStyleFlatModelKey(params.source);
  const model = await loadFlatLightGBMModelFromR2(env.RUNNING_STYLE_MODELS, modelKey);
  const calibrators = await tryLoadCalibrators(env.RUNNING_STYLE_MODELS, params.source);
  const pool = getFinishPositionPool(env);
  // Prefer the D1 daily-target path (mirrors the production queue): today's
  // races have D1 race-day entries before they land in nvd_se, so building from
  // Postgres alone would return zero rows. Fall back to Postgres only when D1
  // has no entries (historical-race verification use case).
  const dailyTargetRows = await listDailyRaceEntriesForRace(env.REALTIME_DB, params);
  const built =
    dailyTargetRows.length > 0
      ? await buildRunningStyleFeaturesForRaceFromD1Target(
          pool,
          params,
          model.header.feature_names,
          dailyTargetRows,
        )
      : await buildRunningStyleFeaturesForRaceFromPostgres(
          pool,
          params,
          model.header.feature_names,
        );
  const coverage = validateFeatureCoverage(built.rows, model.header.feature_names);
  if (coverage.missingFeatureNames.length > 0) {
    throw new Error(
      `PostgreSQL feature build missing model features: ${coverage.missingFeatureNames.join(", ")}`,
    );
  }
  const parquetKey = runningStyleParquetVerificationKey(
    params.source,
    raceDate,
    `${raceKey}.postgres`,
  );
  const parquetBytes = await putRunningStyleFeatureParquet(
    env.RUNNING_STYLE_MODELS,
    parquetKey,
    built.rows,
    model.header.feature_names,
  );
  const rows = await loadRunningStyleFeatureParquet(
    env.RUNNING_STYLE_MODELS,
    parquetKey,
    model.header.feature_names,
  );
  const inference = await runRunningStyleInferenceRowsWithFlatModel(env.REALTIME_DB, {
    calibrators,
    model,
    predictedAt,
    rows,
  });
  return {
    featureBuildMs: built.elapsedMs,
    featureCount: model.header.feature_names.length,
    inputFeaturesKey: "postgres",
    missingCells: coverage.missingCells,
    missingFeatureNames: coverage.missingFeatureNames,
    modelKey,
    modelVersion: inference.modelVersion,
    parquetBytes,
    parquetKey,
    raceKey,
    readBackRows: rows.length,
    writtenCount: inference.writtenCount,
  };
};
