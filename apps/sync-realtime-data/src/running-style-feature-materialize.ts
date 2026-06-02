// Run with bun. Makes the per-race R2 feature Parquet the source of truth for
// running-style (脚質) inference. The consumer helper reads R2 first and only
// rebuilds from PostgreSQL on a miss, an empty object, or a coverage gap; the
// producer pre-materializes Parquets at prewarm so the queue path is a pure R2
// read. All comments and logs are intentionally in English.

import type { Pool } from "pg";

import { listDailyRaceEntriesForRace } from "./daily-feature-build";
import { formatError } from "./format-error";
import { getFinishPositionPool } from "./finish-position-lite-pool";
import {
  buildRunningStyleFeatureParquetKey,
  loadRunningStyleFeatureParquet,
  putRunningStyleFeatureParquet,
  validateFeatureCoverage,
} from "./running-style-feature-parquet";
import { buildRunningStyleFeaturesForRaceFromD1Target } from "./running-style-feature-sql";
import { buildRunningStyleRaceKey, type RunningStyleRaceParams } from "./running-style-features";
import {
  buildRunningStyleFlatModelKey,
  loadFlatLightGBMModelFromR2,
} from "./running-style-model-binary";
import type { RegisteredRaceRow } from "./running-style-cron";
import type { RaceHorseFeatureRow } from "./running-style-r2";
import { listRunningStyleRacesByDate } from "./running-style-race-list";
import type { Env } from "./types";

const R2_OBJECT_NOT_FOUND_PREFIX = "R2 object not found:";
const ENABLED_FLAG = "1";

export interface MaterializeRunningStyleFeatureParquetParams {
  env: Env;
  featureNames: ReadonlyArray<string>;
  pool: Pool;
  race: RunningStyleRaceParams;
}

export interface MaterializeRunningStyleFeatureParquetResult {
  builtRowCount: number;
  bytesWritten: number;
  featuresR2Key: string;
}

export interface LoadOrBuildRunningStyleFeatureParquetParams {
  env: Env;
  featureNames: ReadonlyArray<string>;
  pool: Pool;
  race: RunningStyleRaceParams;
}

export interface LoadOrBuildRunningStyleFeatureParquetResult {
  featuresR2Key: string;
  rebuilt: boolean;
  rows: RaceHorseFeatureRow[];
}

export interface MaterializeRunningStyleFeaturesForDateResult {
  date: string;
  materialized: number;
  scanned: number;
  skipped: number;
  materializeError?: string;
}

const buildAndPutRunningStyleFeatureParquet = async (
  params: MaterializeRunningStyleFeatureParquetParams,
): Promise<MaterializeRunningStyleFeatureParquetResult> => {
  const raceKey = buildRunningStyleRaceKey(params.race);
  const dailyTargetRows = await listDailyRaceEntriesForRace(params.env.REALTIME_DB, params.race);
  if (dailyTargetRows.length === 0) {
    throw new Error(`no D1 daily_race_entries rows for race ${raceKey}; run build-daily-features`);
  }
  const built = await buildRunningStyleFeaturesForRaceFromD1Target(
    params.pool,
    params.race,
    params.featureNames,
    dailyTargetRows,
  );
  if (built.rows.length === 0) {
    throw new Error(`no running-style feature rows found for race ${raceKey}`);
  }
  const coverage = validateFeatureCoverage(built.rows, params.featureNames);
  if (coverage.missingFeatureNames.length > 0) {
    throw new Error(
      `PostgreSQL feature build missing model features: ${coverage.missingFeatureNames.join(", ")}`,
    );
  }
  const featuresR2Key = buildRunningStyleFeatureParquetKey(params.race);
  const bytesWritten = await putRunningStyleFeatureParquet(
    params.env.RUNNING_STYLE_MODELS,
    featuresR2Key,
    built.rows,
    params.featureNames,
  );
  return { builtRowCount: built.rows.length, bytesWritten, featuresR2Key };
};

const isR2NotFoundError = (error: unknown): boolean =>
  error instanceof Error && error.message.startsWith(R2_OBJECT_NOT_FOUND_PREFIX);

export const loadOrBuildRunningStyleFeatureParquet = async (
  params: LoadOrBuildRunningStyleFeatureParquetParams,
): Promise<LoadOrBuildRunningStyleFeatureParquetResult> => {
  const featuresR2Key = buildRunningStyleFeatureParquetKey(params.race);
  const loaded = await loadRunningStyleFeatureParquet(
    params.env.RUNNING_STYLE_MODELS,
    featuresR2Key,
    params.featureNames,
  ).catch((error: unknown) => {
    if (isR2NotFoundError(error)) return null;
    throw error;
  });
  const coverageMissing =
    loaded !== null &&
    validateFeatureCoverage(loaded, params.featureNames).missingFeatureNames.length > 0;
  if (loaded !== null && loaded.length > 0 && !coverageMissing) {
    return { featuresR2Key, rebuilt: false, rows: loaded };
  }
  const built = await buildAndPutRunningStyleFeatureParquet({
    env: params.env,
    featureNames: params.featureNames,
    pool: params.pool,
    race: params.race,
  });
  const rows = await loadRunningStyleFeatureParquet(
    params.env.RUNNING_STYLE_MODELS,
    built.featuresR2Key,
    params.featureNames,
  );
  return { featuresR2Key: built.featuresR2Key, rebuilt: true, rows };
};

export const materializeRunningStyleFeatureParquetForRace = async (
  params: MaterializeRunningStyleFeatureParquetParams,
): Promise<MaterializeRunningStyleFeatureParquetResult> =>
  buildAndPutRunningStyleFeatureParquet(params);

const buildRaceParamsFromRegisteredRow = (row: RegisteredRaceRow): RunningStyleRaceParams => ({
  kaisaiNen: row.kaisai_nen,
  kaisaiTsukihi: row.kaisai_tsukihi,
  keibajoCode: row.keibajo_code,
  raceBango: row.race_bango,
  source: row.source,
});

const materializeRegisteredRace = async (
  env: Env,
  pool: Pool,
  row: RegisteredRaceRow,
  acc: MaterializeRunningStyleFeaturesForDateResult,
): Promise<MaterializeRunningStyleFeaturesForDateResult> => {
  const race = buildRaceParamsFromRegisteredRow(row);
  const model = await loadFlatLightGBMModelFromR2(
    env.RUNNING_STYLE_MODELS,
    buildRunningStyleFlatModelKey(race.source),
  );
  await materializeRunningStyleFeatureParquetForRace({
    env,
    featureNames: model.header.feature_names,
    pool,
    race,
  });
  return { ...acc, materialized: acc.materialized + 1, scanned: acc.scanned + 1 };
};

export const materializeRunningStyleFeatureParquetsForDate = async (
  env: Env,
  date: string,
): Promise<MaterializeRunningStyleFeaturesForDateResult> => {
  if (env.RUNNING_STYLE_D1_WRITE_ENABLED !== ENABLED_FLAG) {
    return { date, materialized: 0, scanned: 0, skipped: 0 };
  }
  const { races } = await listRunningStyleRacesByDate(env, date);
  const pool = getFinishPositionPool(env);
  return races.reduce<Promise<MaterializeRunningStyleFeaturesForDateResult>>(
    async (accPromise, row) => {
      const acc = await accPromise;
      return materializeRegisteredRace(env, pool, row, acc).catch((error: unknown) => ({
        ...acc,
        materializeError: formatError(error),
        scanned: acc.scanned + 1,
        skipped: acc.skipped + 1,
      }));
    },
    Promise.resolve({ date, materialized: 0, scanned: 0, skipped: 0 }),
  );
};
