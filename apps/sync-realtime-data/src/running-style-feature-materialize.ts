// Run with bun. Materializes a versioned R2 Parquet cache from the authoritative
// local-PostgreSQL-sourced raw Iceberg tables exposed by the catalog Worker.

import { formatError } from "./format-error";
import {
  fetchRunningStyleFeaturesFromCatalog,
  isCatalogUnavailableError,
} from "./running-style-catalog-client";
import {
  buildRunningStyleFeatureParquetKey,
  loadRunningStyleFeatureParquet,
  putRunningStyleFeatureParquet,
  validateFeatureCoverage,
} from "./running-style-feature-parquet";
import { buildRunningStyleRaceKey, type RunningStyleRaceParams } from "./running-style-features";
import {
  buildRunningStyleFlatModelKey,
  loadFlatLightGBMModelFromR2,
} from "./running-style-model-binary";
import type { RegisteredRaceRow } from "./running-style-cron";
import type { RaceHorseFeatureRow } from "./running-style-r2";
import { listRunningStyleRacesByDate } from "./running-style-race-list";
import type { Env } from "./types";

const ENABLED_FLAG = "1";

export interface MaterializeRunningStyleFeatureParquetParams {
  env: Env;
  featureNames: ReadonlyArray<string>;
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
  race: RunningStyleRaceParams;
}

export interface LoadOrBuildRunningStyleFeatureParquetResult {
  featuresR2Key: string;
  rebuilt: boolean;
  rows: ReadonlyArray<RaceHorseFeatureRow>;
}

export interface MaterializeRunningStyleFeaturesForDateResult {
  date: string;
  materialized: number;
  scanned: number;
  skipped: number;
  materializeError?: string;
}

interface BuildAndPutRunningStyleFeatureParquetInternalResult extends MaterializeRunningStyleFeatureParquetResult {
  rows: ReadonlyArray<RaceHorseFeatureRow>;
}

const buildAndPutRunningStyleFeatureParquetInternal = async (
  params: MaterializeRunningStyleFeatureParquetParams,
): Promise<BuildAndPutRunningStyleFeatureParquetInternalResult> => {
  const raceKey = buildRunningStyleRaceKey(params.race);
  const rows = await fetchRunningStyleFeaturesFromCatalog(
    params.env.PC_KEIBA_R2_CATALOG,
    params.race,
    params.featureNames,
  );
  if (rows.length === 0) {
    throw new Error(`no running-style feature rows found for race ${raceKey}`);
  }
  const coverage = validateFeatureCoverage(rows, params.featureNames);
  if (coverage.missingFeatureNames.length > 0) {
    throw new Error(
      `catalog feature build missing model features: ${coverage.missingFeatureNames.join(", ")}`,
    );
  }
  const featuresR2Key = buildRunningStyleFeatureParquetKey(params.race);
  const bytesWritten = await putRunningStyleFeatureParquet(
    params.env.RUNNING_STYLE_MODELS,
    featuresR2Key,
    rows,
    params.featureNames,
  );
  return { builtRowCount: rows.length, bytesWritten, featuresR2Key, rows };
};

const tryLoadCachedRunningStyleFeatureParquet = async (
  params: LoadOrBuildRunningStyleFeatureParquetParams,
): Promise<LoadOrBuildRunningStyleFeatureParquetResult | null> => {
  const featuresR2Key = buildRunningStyleFeatureParquetKey(params.race);
  try {
    const rows = await loadRunningStyleFeatureParquet(
      params.env.RUNNING_STYLE_MODELS,
      featuresR2Key,
      params.featureNames,
    );
    if (rows.length === 0) return null;
    const coverage = validateFeatureCoverage(rows, params.featureNames);
    if (coverage.missingFeatureNames.length > 0) return null;
    return { featuresR2Key, rebuilt: false, rows };
  } catch {
    return null;
  }
};

export const loadOrBuildRunningStyleFeatureParquet = async (
  params: LoadOrBuildRunningStyleFeatureParquetParams,
): Promise<LoadOrBuildRunningStyleFeatureParquetResult> => {
  // Memory mitigation (2026-06-09): the previous implementation re-fetched the
  // freshly-uploaded Parquet from R2 here, which doubled peak ArrayBuffer +
  // Buffer + decoded-row residency on the rebuild path inside the 128 MiB
  // isolate. The internal builder now hands the in-memory rows back so the
  // round-trip is skipped — the file in R2 is identical to the rows we just
  // assembled, so the second load was pure overhead.
  try {
    const built = await buildAndPutRunningStyleFeatureParquetInternal({
      env: params.env,
      featureNames: params.featureNames,
      race: params.race,
    });
    return { featuresR2Key: built.featuresR2Key, rebuilt: true, rows: built.rows };
  } catch (error) {
    if (!isCatalogUnavailableError(error)) throw error;
    const cached = await tryLoadCachedRunningStyleFeatureParquet(params);
    if (cached === null) throw error;
    console.error(
      `Running-style features catalog unavailable, using R2 parquet fallback ${cached.featuresR2Key}: ${formatError(error)}`,
    );
    return cached;
  }
};

export const materializeRunningStyleFeatureParquetForRace = async (
  params: MaterializeRunningStyleFeatureParquetParams,
): Promise<MaterializeRunningStyleFeatureParquetResult> => {
  const built = await buildAndPutRunningStyleFeatureParquetInternal(params);
  return {
    builtRowCount: built.builtRowCount,
    bytesWritten: built.bytesWritten,
    featuresR2Key: built.featuresR2Key,
  };
};

const buildRaceParamsFromRegisteredRow = (row: RegisteredRaceRow): RunningStyleRaceParams => ({
  kaisaiNen: row.kaisai_nen,
  kaisaiTsukihi: row.kaisai_tsukihi,
  keibajoCode: row.keibajo_code,
  raceBango: row.race_bango,
  source: row.source,
});

const materializeRegisteredRace = async (
  env: Env,
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
  return races.reduce<Promise<MaterializeRunningStyleFeaturesForDateResult>>(
    async (accPromise, row) => {
      const acc = await accPromise;
      return materializeRegisteredRace(env, row, acc).catch((error: unknown) => ({
        ...acc,
        materializeError: formatError(error),
        scanned: acc.scanned + 1,
        skipped: acc.skipped + 1,
      }));
    },
    Promise.resolve({ date, materialized: 0, scanned: 0, skipped: 0 }),
  );
};
