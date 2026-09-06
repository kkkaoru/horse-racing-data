// Run with bun. Materializes a versioned R2 Parquet cache from the authoritative
// local-PostgreSQL-sourced raw Iceberg tables exposed by the catalog Worker.

import { formatError } from "./format-error";
import { getFinishPositionPool } from "./finish-position-lite-pool";
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
import { buildRunningStyleFeaturesForRaceFromPostgres } from "./running-style-feature-sql";
import { loadRunningStyleFeaturesFromFinishPositionDayBase } from "./running-style-finish-feature-hit";
import { buildRunningStyleRaceKey, type RunningStyleRaceParams } from "./running-style-features";
import {
  buildRunningStyleFlatModelKey,
  loadFlatLightGBMHeaderFromR2,
} from "./running-style-model-binary";
import type { RegisteredRaceRow } from "./running-style-cron";
import type { RaceHorseFeatureRow } from "./running-style-r2";
import { listRunningStyleRacesByDate } from "./running-style-race-list";
import type { Env } from "./types";

const ENABLED_FLAG = "1";
const REQUIRE_DAY_BASE_CACHE_HIT_FLAG = "1";
// R2 SQL can spend over a minute planning a cold multi-year history query.
// PostgreSQL is the indexed mirror and should get enough time to complete the
// one-race fallback; keeping this bounded still prevents a Queue attempt from
// hanging indefinitely.
const POSTGRES_FALLBACK_TIMEOUT_MS = 120_000;
const R2_SQL_EXECUTION_RESOURCE_ERROR_PATTERN =
  /r2_sql_unavailable(?:\s+code=)?(?:60104|70200)|code=(?:60104|70200)\b/;

// A 70200 response means the Catalog query itself exhausted R2 SQL resources.
// Falling back to the decade-wide PostgreSQL mirror in that case only creates a
// second expensive scan, which is what left queue slots occupied until timeout.
// Let the queue retry the bounded Catalog path instead.  PostgreSQL remains a
// useful fallback for transport/time-out failures where the query was never
// executed by R2 SQL.
const isCatalogExecutionResourceError = (error: unknown): boolean =>
  R2_SQL_EXECUTION_RESOURCE_ERROR_PATTERN.test(
    error instanceof Error ? error.message : String(error),
  );

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
  publishedSources?: ReadonlyArray<"jra" | "nar">;
}

interface BuildAndPutRunningStyleFeatureParquetInternalResult extends MaterializeRunningStyleFeatureParquetResult {
  rows: ReadonlyArray<RaceHorseFeatureRow>;
}

interface PostgresFallbackResult {
  elapsedMs: number;
  rows: ReadonlyArray<RaceHorseFeatureRow>;
}

const loadPostgresFallback = async (
  params: MaterializeRunningStyleFeatureParquetParams,
): Promise<PostgresFallbackResult | null> => {
  try {
    const postgres = await Promise.race([
      buildRunningStyleFeaturesForRaceFromPostgres(
        getFinishPositionPool(params.env),
        params.race,
        params.featureNames,
      ),
      new Promise<never>((_, reject) =>
        setTimeout(
          () =>
            reject(
              new Error(
                `PostgreSQL running-style fallback timed out after ${POSTGRES_FALLBACK_TIMEOUT_MS}ms`,
              ),
            ),
          POSTGRES_FALLBACK_TIMEOUT_MS,
        ),
      ),
    ]);
    return postgres.rows.length > 0 ? postgres : null;
  } catch (error) {
    console.error(`Running-style PostgreSQL fallback failed: ${formatError(error)}`);
    return null;
  }
};

interface CachedFoundation {
  etag: string;
  rows: Promise<ReadonlyArray<RaceHorseFeatureRow>>;
}

const foundationCache = new Map<string, CachedFoundation>();

export const buildRunningStyleDayFoundationKey = (race: RunningStyleRaceParams): string =>
  `running-style/features-day/raw-iceberg-v1/${race.source}/${race.kaisaiNen}${race.kaisaiTsukihi}/features.parquet`;

export const clearRunningStyleFoundationCache = (): void => {
  foundationCache.clear();
};

const loadRunningStyleFoundationRows = async (
  params: MaterializeRunningStyleFeatureParquetParams,
): Promise<ReadonlyArray<RaceHorseFeatureRow> | null> => {
  const bucket = params.env.FEATURES_ARCHIVE;
  if (bucket === undefined) return null;
  const key = buildRunningStyleDayFoundationKey(params.race);
  const cacheKey = `${key}\u0000${params.featureNames.join("\u0000")}`;
  const object = await bucket.head(key);
  if (object === null) {
    foundationCache.delete(cacheKey);
    return null;
  }
  let cached = foundationCache.get(cacheKey);
  if (cached === undefined || cached.etag !== object.etag) {
    cached = {
      etag: object.etag,
      rows: loadRunningStyleFeatureParquet(bucket, key, params.featureNames),
    };
    foundationCache.set(cacheKey, cached);
  }
  try {
    const raceKey = buildRunningStyleRaceKey(params.race);
    const rows = (await cached.rows).filter((row) => row.raceKey === raceKey);
    if (rows.length === 0) return null;
    const coverage = validateFeatureCoverage(rows, params.featureNames);
    return coverage.missingFeatureNames.length === 0 ? rows : null;
  } catch (error) {
    foundationCache.delete(cacheKey);
    if (error instanceof Error && error.message.startsWith("R2 object not found:")) return null;
    throw error;
  }
};

const loadAuthoritativeFeatureRows = async (
  params: MaterializeRunningStyleFeatureParquetParams,
): Promise<ReadonlyArray<RaceHorseFeatureRow>> => {
  try {
    const foundation = await loadRunningStyleFoundationRows(params);
    if (foundation !== null) {
      console.log(
        `Running-style features HIT day foundation for ${buildRunningStyleRaceKey(params.race)}`,
      );
      return foundation;
    }
  } catch (error) {
    console.warn(
      `Running-style day foundation MISS for ${buildRunningStyleRaceKey(params.race)}: ${formatError(error)}`,
    );
  }
  try {
    const hit = await loadRunningStyleFeaturesFromFinishPositionDayBase({
      bucket: params.env.FEATURES_ARCHIVE,
      featureNames: params.featureNames,
      race: params.race,
    });
    if (hit !== null && hit.length > 0) {
      console.log(
        `Running-style features HIT finish-position day-base for ${buildRunningStyleRaceKey(params.race)}`,
      );
      return hit;
    }
    if (params.env.RUNNING_STYLE_REQUIRE_DAY_BASE_CACHE_HIT === REQUIRE_DAY_BASE_CACHE_HIT_FLAG) {
      throw new Error(
        `Running-style day-base cache MISS for ${buildRunningStyleRaceKey(params.race)}`,
      );
    }
  } catch (error) {
    if (params.env.RUNNING_STYLE_REQUIRE_DAY_BASE_CACHE_HIT === REQUIRE_DAY_BASE_CACHE_HIT_FLAG) {
      throw error;
    }
    console.warn(
      `Running-style finish-position day-base MISS for ${buildRunningStyleRaceKey(params.race)}: ${formatError(error)}`,
    );
  }
  try {
    const catalogRows = await fetchRunningStyleFeaturesFromCatalog(
      params.env.PC_KEIBA_R2_CATALOG,
      params.race,
      params.featureNames,
    );
    if (catalogRows.length > 0) return catalogRows;
    // An empty Catalog result means the source race is not present in the
    // materialized scan (rather than a valid zero-row prediction). Fall back
    // to the indexed PostgreSQL mirror so a single missing Catalog slice does
    // not permanently fail the race.
    return (await loadPostgresFallback(params))?.rows ?? [];
  } catch (catalogError) {
    if (!isCatalogUnavailableError(catalogError) || isCatalogExecutionResourceError(catalogError)) {
      throw catalogError;
    }
    const postgresRows = await loadPostgresFallback(params);
    if (postgresRows !== null) {
      console.error(
        `Running-style Catalog unavailable, rebuilt from PostgreSQL mirror in ${String(postgresRows.elapsedMs)}ms: ${formatError(catalogError)}`,
      );
      return postgresRows.rows;
    }
    throw catalogError;
  }
};

const buildAndPutRunningStyleFeatureParquetInternal = async (
  params: MaterializeRunningStyleFeatureParquetParams,
): Promise<BuildAndPutRunningStyleFeatureParquetInternalResult> => {
  const raceKey = buildRunningStyleRaceKey(params.race);
  const rows = await loadAuthoritativeFeatureRows(params);
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
  if (params.env.RUNNING_STYLE_REQUIRE_DAY_BASE_CACHE_HIT === REQUIRE_DAY_BASE_CACHE_HIT_FLAG) {
    return {
      featuresR2Key: buildRunningStyleFeatureParquetKey(params.race),
      rebuilt: false,
      rows: await loadAuthoritativeFeatureRows(params),
    };
  }
  const cached = await tryLoadCachedRunningStyleFeatureParquet(params);
  if (cached !== null) return cached;
  // Memory mitigation (2026-06-09): the previous implementation re-fetched the
  // freshly-uploaded Parquet from R2 here, which doubled peak ArrayBuffer +
  // Buffer + decoded-row residency on the rebuild path inside the 128 MiB
  // isolate. The internal builder now hands the in-memory rows back so the
  // round-trip is skipped — the file in R2 is identical to the rows we just
  // assembled, so the second load was pure overhead.
  const built = await buildAndPutRunningStyleFeatureParquetInternal({
    env: params.env,
    featureNames: params.featureNames,
    race: params.race,
  });
  return { featuresR2Key: built.featuresR2Key, rebuilt: true, rows: built.rows };
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
  gradeCode: row.grade_code,
});

const loadOrBuildFoundationRace = async (
  env: Env,
  row: RegisteredRaceRow,
  featureNames: ReadonlyArray<string>,
): Promise<{ rebuilt: boolean; rows: ReadonlyArray<RaceHorseFeatureRow> }> => {
  const race = buildRaceParamsFromRegisteredRow(row);
  const key = buildRunningStyleFeatureParquetKey(race);
  try {
    const cached = await loadRunningStyleFeatureParquet(
      env.RUNNING_STYLE_MODELS,
      key,
      featureNames,
    );
    const coverage = validateFeatureCoverage(cached, featureNames);
    if (cached.length > 0 && coverage.missingFeatureNames.length === 0) {
      return { rebuilt: false, rows: cached };
    }
  } catch {
    // A missing or stale per-race cache is rebuilt from the Catalog below.
  }
  // Reuse the attested day foundation instead of rebuilding history per race.
  const rows = await loadAuthoritativeFeatureRows({ env, race, featureNames });
  if (rows.length === 0)
    throw new Error(
      `no running-style feature rows found for race ${buildRunningStyleRaceKey(race)}`,
    );
  const coverage = validateFeatureCoverage(rows, featureNames);
  if (coverage.missingFeatureNames.length > 0) {
    throw new Error(
      `catalog feature build missing model features: ${coverage.missingFeatureNames.join(", ")}`,
    );
  }
  await putRunningStyleFeatureParquet(env.RUNNING_STYLE_MODELS, key, rows, featureNames);
  return { rebuilt: true, rows };
};

export const materializeRunningStyleFeatureParquetsForDate = async (
  env: Env,
  date: string,
): Promise<MaterializeRunningStyleFeaturesForDateResult> => {
  if (env.RUNNING_STYLE_D1_WRITE_ENABLED !== ENABLED_FLAG) {
    return { date, materialized: 0, scanned: 0, skipped: 0 };
  }
  if (env.FEATURES_ARCHIVE === undefined) {
    return {
      date,
      materialized: 0,
      materializeError: "FEATURES_ARCHIVE binding is missing",
      scanned: 0,
      skipped: 0,
    };
  }
  const { races } = await listRunningStyleRacesByDate(env, date);
  let materialized = 0;
  let scanned = 0;
  let skipped = 0;
  const publishedSources: Array<"jra" | "nar"> = [];
  try {
    for (const source of ["jra", "nar"] as const) {
      const sourceRaces = races.filter((race) => race.source === source);
      if (sourceRaces.length === 0) continue;
      const header = await loadFlatLightGBMHeaderFromR2(
        env.RUNNING_STYLE_MODELS,
        buildRunningStyleFlatModelKey(source),
      );
      const rows: RaceHorseFeatureRow[] = [];
      for (const race of sourceRaces) {
        const result = await loadOrBuildFoundationRace(env, race, header.feature_names);
        rows.push(...result.rows);
        materialized += result.rebuilt ? 1 : 0;
        skipped += result.rebuilt ? 0 : 1;
        scanned += 1;
      }
      const key = buildRunningStyleDayFoundationKey(
        buildRaceParamsFromRegisteredRow(sourceRaces[0]!),
      );
      await putRunningStyleFeatureParquet(env.FEATURES_ARCHIVE, key, rows, header.feature_names);
      foundationCache.delete(`${key}\u0000${header.feature_names.join("\u0000")}`);
      publishedSources.push(source);
    }
    return { date, materialized, scanned, skipped };
  } catch (error) {
    return {
      date,
      materializeError: formatError(error),
      materialized,
      scanned,
      skipped,
      ...(publishedSources.length > 0 ? { publishedSources } : {}),
    };
  }
};
