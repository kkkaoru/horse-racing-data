// Run with bun. Queue consumer for per-race running-style inference.
// The Worker reads the raw-Iceberg-derived per-race feature Parquet from R2 and
// rebuilds it through the catalog Worker on a miss, then writes predictions to D1
// and mirrors them to the Neon race_running_style_model_predictions table so
// the viewer can read predictions without a separate sync step.

import { markFinishPositionFeaturesCached } from "./finish-position-d1";
import { formatError, formatErrorLogLine } from "./format-error";
import { putFinishPositionInputsCache } from "./finish-position-inputs-cache";
import { getFinishPositionWritePool } from "./finish-position-lite-pool";
import {
  filterRunningStyleFeatureRowsByActiveEntries,
  resolveRunningStyleExpectedHorseCount,
} from "./running-style-expected-horses";
import { putViewerRunningStyleRaceCache } from "./viewer-running-style-cache";
import {
  getRunningStyleInferenceState,
  listRaceRunningStylesForRace,
  listRunningStyleInferenceStates,
  markRunningStyleInferenceCompleted,
  markRunningStyleInferenceFailed,
  markRunningStyleInferenceProcessing,
  markRunningStyleInferenceSyncFailed,
  type RaceRunningStyleRow,
  upsertRaceRunningStyles,
} from "./running-style-d1";
import { loadOrBuildRunningStyleFeatureParquet } from "./running-style-feature-materialize";
import {
  buildRealtimeRaceKeyFromRunningStyle,
  buildRunningStyleRaceKey,
} from "./running-style-features";
import { runRunningStyleInferenceRowsWithFlatModel } from "./running-style-inference";
import { loadFlatLightGBMModelFromR2 } from "./running-style-model-binary";
import {
  buildCalibrationR2Key,
  loadCalibratorsFromR2,
  type RunningStyleCalibrationTable,
} from "./running-style-calibration";
import {
  deriveRunningStyleCategory,
  resolveRunningStyleCellRoute,
  type RunningStyleCellCategory,
  type RunningStyleCellRoute,
  type RunningStyleCellRoutingConfig,
} from "./running-style-cell-router";
import type { RegisteredRaceRow } from "./running-style-cron";
import { upsertRunningStylePredictionsToNeon } from "./running-style-neon";
import { exportRunningStyleParquetForDay } from "./running-style-parquet-export";
import type { RaceHorseFeatureRow } from "./running-style-r2";
import { listRunningStyleRacesByDate } from "./running-style-race-list";
import { getLatestRaceEntries } from "./storage";
import type { Env, RunningStylePredictionJob } from "./types";

const ENABLED_FLAG = "1";
const FINISH_POSITION_DAY_BASE_URL =
  "https://finish-position-cron.internal/api/admin/prewarm-day-base";
const NEON_SYNC_MAX_ATTEMPTS = 3;
const NEON_SYNC_RETRY_DELAY_MS = 200;

const buildFinishPositionRunYmd = (job: RunningStylePredictionJob): string =>
  `${job.kaisaiNen}${job.kaisaiTsukihi}`;

const tryLoadCalibrators = async (
  bucket: R2Bucket,
  source: "jra" | "nar",
): Promise<RunningStyleCalibrationTable | undefined> => {
  try {
    return await loadCalibratorsFromR2(bucket, buildCalibrationR2Key(source));
  } catch (error) {
    console.error(
      formatErrorLogLine(
        "Failed to load running-style calibrators, falling back to uncalibrated",
        { source },
        error,
      ),
    );
    return undefined;
  }
};

export interface RunningStylePredictionJobSummary {
  cellModelKey?: string;
  cellVariantId?: string;
  raceKey: string;
  cacheError?: string;
  cacheWritten?: boolean;
  featuresR2Key: string;
  finishPositionTriggerError?: string;
  finishPositionTriggerMode?: "service-binding" | "skipped";
  horseCount: number;
  modelVersion: string;
  neonError?: string;
  neonWrittenCount?: number;
  parquetExportError?: string;
  parquetExportedRows?: number;
  skipped?: boolean;
  writtenCount: number;
}

interface CacheAndSyncRunningStylesResult {
  cacheError?: string;
  cacheWritten: boolean;
  neonError?: string;
  neonWrittenCount: number;
  parquetExportError?: string;
  parquetExportedRows: number;
}

interface FinishPositionTriggerResult {
  finishPositionTriggerError?: string;
  finishPositionTriggerMode: "service-binding" | "skipped";
}

interface FinishPositionDayBarrierResult extends FinishPositionTriggerResult {
  parquetExportError?: string;
  parquetExportedRows: number;
}

const waitForNeonSyncRetry = (attemptIndex: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, NEON_SYNC_RETRY_DELAY_MS * attemptIndex));

const upsertRunningStylesToNeonWithRetry = async (
  env: Env,
  rows: ReadonlyArray<RaceRunningStyleRow>,
): Promise<number> => {
  let lastError: unknown;
  for (let attempt = 1; attempt <= NEON_SYNC_MAX_ATTEMPTS; attempt += 1) {
    try {
      return await upsertRunningStylePredictionsToNeon(getFinishPositionWritePool(env), rows);
    } catch (error) {
      lastError = error;
      if (attempt < NEON_SYNC_MAX_ATTEMPTS) {
        await waitForNeonSyncRetry(attempt);
      }
    }
  }
  throw lastError;
};

const exportRunningStylesToR2 = async (
  env: Env,
  job: RunningStylePredictionJob,
): Promise<number | string> => {
  try {
    const result = await exportRunningStyleParquetForDay({
      dateYmd: buildFinishPositionRunYmd(job),
      env,
      source: job.source,
    });
    if (result.skipped) {
      return result.skippedReason ?? "running-style Parquet export was skipped";
    }
    return result.rowCount;
  } catch (error) {
    return formatError(error);
  }
};

const runningStyleCellRoutingConfig = (env: Env): RunningStyleCellRoutingConfig => {
  if (env.RUNNING_STYLE_CELL_ROUTING_JSON === undefined) return {};
  return JSON.parse(env.RUNNING_STYLE_CELL_ROUTING_JSON) as RunningStyleCellRoutingConfig;
};

const routeInputFromFeatureRow = (job: RunningStylePredictionJob, row: RaceHorseFeatureRow) => ({
  category: row.category,
  gradeCode: row.gradeCode,
  kaisaiNen: job.kaisaiNen,
  kaisaiTsukihi: job.kaisaiTsukihi,
  keibajoCode: job.keibajoCode,
  kyori: row.kyori,
  kyosoJokenCode: row.kyosoJokenCode,
  narSubClass: row.narSubClass,
  raceBango: job.raceBango,
  shussoTosu: row.shussoTosu,
  source: job.source,
  trackCode: row.trackCode,
});

const routeInputFromJob = (job: RunningStylePredictionJob) => ({
  kaisaiNen: job.kaisaiNen,
  kaisaiTsukihi: job.kaisaiTsukihi,
  keibajoCode: job.keibajoCode,
  raceBango: job.raceBango,
  source: job.source,
});

const resolveRouteFromRows = (
  job: RunningStylePredictionJob,
  rows: ReadonlyArray<RaceHorseFeatureRow>,
  config: RunningStyleCellRoutingConfig,
): RunningStyleCellRoute => {
  const firstRow = rows[0];
  if (firstRow === undefined) return resolveRunningStyleCellRoute(routeInputFromJob(job), config);
  return resolveRunningStyleCellRoute(routeInputFromFeatureRow(job, firstRow), config);
};

const triggerFinishPositionAfterDayBaseHit = async (
  env: Env,
  job: RunningStylePredictionJob,
  category: RunningStyleCellCategory,
): Promise<FinishPositionTriggerResult> => {
  const binding = env.FINISH_POSITION_CRON;
  const token = env.TRIGGER_TOKEN;
  if (binding === undefined) {
    const message = "missing FINISH_POSITION_CRON binding";
    console.error(
      `Finish-position feature-hit trigger not sent for ${category}:${buildFinishPositionRunYmd(job)}: ${message}`,
    );
    return { finishPositionTriggerError: message, finishPositionTriggerMode: "skipped" };
  }
  if (token === undefined) {
    const message = "missing TRIGGER_TOKEN";
    console.error(
      `Finish-position feature-hit trigger not sent for ${category}:${buildFinishPositionRunYmd(job)}: ${message}`,
    );
    return { finishPositionTriggerError: message, finishPositionTriggerMode: "skipped" };
  }
  if (token.length === 0) {
    const message = "empty TRIGGER_TOKEN";
    console.error(
      `Finish-position feature-hit trigger not sent for ${category}:${buildFinishPositionRunYmd(job)}: ${message}`,
    );
    return { finishPositionTriggerError: message, finishPositionTriggerMode: "skipped" };
  }
  const body = {
    category,
    generatePredictionsAfterHit: true,
    runYmd: buildFinishPositionRunYmd(job),
  };
  try {
    const response = await binding.fetch(
      new Request(FINISH_POSITION_DAY_BASE_URL, {
        body: JSON.stringify(body),
        headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
        method: "POST",
      }),
    );
    if (!response.ok) {
      const message = `HTTP ${response.status}`;
      console.error(
        `Finish-position feature-hit trigger failed for ${category}:${buildFinishPositionRunYmd(job)}: ${response.status}`,
      );
      return {
        finishPositionTriggerError: message,
        finishPositionTriggerMode: "service-binding",
      };
    }
    return { finishPositionTriggerMode: "service-binding" };
  } catch (error) {
    const message = formatError(error);
    console.error(
      `Finish-position feature-hit trigger threw for ${category}:${buildFinishPositionRunYmd(job)}: ${message}`,
    );
    return {
      finishPositionTriggerError: message,
      finishPositionTriggerMode: "service-binding",
    };
  }
};

const buildPredictionJobFromRegisteredRace = (
  race: RegisteredRaceRow,
  predictedAt: string,
): RunningStylePredictionJob => {
  const job = {
    kaisaiNen: race.kaisai_nen,
    kaisaiTsukihi: race.kaisai_tsukihi,
    keibajoCode: race.keibajo_code,
    predictedAt,
    raceBango: race.race_bango,
    source: race.source,
    type: "generate-running-style-predictions" as const,
  };
  return { ...job, raceKey: buildRunningStyleRaceKey(job) };
};

const triggerFinishPositionDayWhenReady = async (
  env: Env,
  job: RunningStylePredictionJob,
  cacheResult: CacheAndSyncRunningStylesResult,
  expectedHorseCount: number,
  writtenHorseCount: number,
): Promise<FinishPositionDayBarrierResult> => {
  const raceKey = buildRunningStyleRaceKey(job);
  const currentRaceNotReadyReason =
    writtenHorseCount < expectedHorseCount
      ? `written count ${writtenHorseCount} is below expected horse count ${expectedHorseCount}`
      : cacheResult.neonError !== undefined
        ? `Neon sync failed: ${cacheResult.neonError}`
        : cacheResult.neonWrittenCount < expectedHorseCount
          ? `Neon written count ${cacheResult.neonWrittenCount} is below expected horse count ${expectedHorseCount}`
          : null;
  if (currentRaceNotReadyReason !== null) {
    console.log(`finish-position trigger skipped for ${raceKey}: ${currentRaceNotReadyReason}`);
    return {
      finishPositionTriggerError: currentRaceNotReadyReason,
      finishPositionTriggerMode: "skipped",
      parquetExportedRows: 0,
    };
  }
  try {
    const raceList = await listRunningStyleRacesByDate(env, buildFinishPositionRunYmd(job));
    // JRA and NAR use different shared daily Parquet objects. Ban-ei is part of
    // the NAR object, so wait for the whole source-day rather than allowing one
    // model category to publish a partial object that another category later
    // overwrites.
    const sourceRaces = raceList.races.filter((race) => race.source === job.source);
    if (sourceRaces.length === 0) {
      const reason = `no authoritative ${job.source} races registered for ${buildFinishPositionRunYmd(job)}`;
      console.log(`finish-position trigger skipped for ${raceKey}: ${reason}`);
      return {
        finishPositionTriggerError: reason,
        finishPositionTriggerMode: "skipped",
        parquetExportedRows: 0,
      };
    }
    const jobs = sourceRaces.map((race) =>
      buildPredictionJobFromRegisteredRace(race, job.predictedAt),
    );
    const states = await listRunningStyleInferenceStates(
      env.REALTIME_DB,
      jobs.map((registeredJob) => registeredJob.raceKey),
    );
    // Publish the currently available source-day rows after every completed
    // race.  The finish-position day-base can safely consume this immutable
    // snapshot for completed races while later races remain pending.  Waiting
    // for every race here made one failed state suppress the by-day shard for
    // the whole category, which in turn caused repeated standard-4 rebuilds
    // with an absent running-style watermark.
    const parquetExportResult = await exportRunningStylesToR2(env, job);
    if (typeof parquetExportResult === "string") {
      const reason = `R2 Parquet export failed: ${parquetExportResult}`;
      console.log(`finish-position trigger skipped for ${raceKey}: ${reason}`);
      return {
        finishPositionTriggerError: reason,
        finishPositionTriggerMode: "skipped",
        parquetExportError: parquetExportResult,
        parquetExportedRows: 0,
      };
    }
    const incompleteRace = jobs.find((registeredJob) => {
      const state = states.get(registeredJob.raceKey);
      return (
        state?.status !== "completed" ||
        state.expectedHorseCount === null ||
        state.writtenHorseCount === null ||
        state.writtenHorseCount < state.expectedHorseCount
      );
    });
    if (incompleteRace !== undefined) {
      const reason = `running-style source-day is incomplete; waiting for ${incompleteRace.raceKey}`;
      console.log(`finish-position trigger skipped for ${raceKey}: ${reason}`);
      return {
        finishPositionTriggerError: reason,
        finishPositionTriggerMode: "skipped",
        parquetExportedRows: parquetExportResult,
      };
    }

    const expectedRows = jobs.reduce(
      (total, registeredJob) =>
        total + (states.get(registeredJob.raceKey)?.expectedHorseCount ?? 0),
      0,
    );
    if (parquetExportResult < expectedRows) {
      const reason = `R2 Parquet export row count ${parquetExportResult} is below expected source-day horse count ${expectedRows}`;
      console.log(`finish-position trigger skipped for ${raceKey}: ${reason}`);
      return {
        finishPositionTriggerError: reason,
        finishPositionTriggerMode: "skipped",
        parquetExportedRows: parquetExportResult,
      };
    }

    const categories = [...new Set(jobs.map(deriveRunningStyleCategory))];
    const triggerResults = await Promise.all(
      categories.map((category) => triggerFinishPositionAfterDayBaseHit(env, job, category)),
    );
    const failedTrigger = triggerResults.find(
      (result) => result.finishPositionTriggerError !== undefined,
    );
    return {
      ...failedTrigger,
      finishPositionTriggerMode:
        failedTrigger?.finishPositionTriggerMode ??
        triggerResults[0]?.finishPositionTriggerMode ??
        "skipped",
      parquetExportedRows: parquetExportResult,
    };
  } catch (error) {
    const reason = formatError(error);
    console.error(formatErrorLogLine("Running-style day barrier failed", { raceKey }, error));
    return {
      finishPositionTriggerError: reason,
      finishPositionTriggerMode: "skipped",
      parquetExportedRows: 0,
    };
  }
};

const cacheAndSyncCompletedRunningStyles = async (
  env: Env,
  job: RunningStylePredictionJob,
): Promise<CacheAndSyncRunningStylesResult> => {
  const raceKey = buildRunningStyleRaceKey(job);
  try {
    const rows = await listRaceRunningStylesForRace(env.REALTIME_DB, raceKey, {
      bypassCache: true,
    });
    if (rows.length === 0) {
      return { cacheWritten: false, neonWrittenCount: 0, parquetExportedRows: 0 };
    }
    await upsertRaceRunningStyles(env.REALTIME_DB, rows);
    const [cacheWritten, neonResult] = await Promise.all([
      putViewerRunningStyleRaceCache({ env, race: job, rows }).catch((error: unknown) => {
        console.error(formatErrorLogLine("Running-style cache write failed", { raceKey }, error));
        return false;
      }),
      upsertRunningStylesToNeonWithRetry(env, rows).catch((error: unknown) => {
        console.error(formatErrorLogLine("Running-style Neon write failed", { raceKey }, error));
        return formatError(error);
      }),
    ]);
    const neonFailed = typeof neonResult === "string";
    return {
      cacheWritten,
      neonError: neonFailed ? neonResult : undefined,
      neonWrittenCount: neonFailed ? 0 : neonResult,
      parquetExportedRows: 0,
    };
  } catch (error) {
    console.error(formatErrorLogLine("Running-style cache/sync failed", { raceKey }, error));
    return {
      cacheError: formatError(error),
      cacheWritten: false,
      neonWrittenCount: 0,
      parquetExportedRows: 0,
    };
  }
};

export const handleRunningStylePredictionJob = async (
  env: Env,
  job: RunningStylePredictionJob,
): Promise<RunningStylePredictionJobSummary | null> => {
  if (env.RUNNING_STYLE_D1_WRITE_ENABLED !== ENABLED_FLAG) {
    return null;
  }
  const raceKey = buildRunningStyleRaceKey(job);
  try {
    const state = await getRunningStyleInferenceState(env.REALTIME_DB, raceKey);
    if (
      (state?.status === "completed" || state?.status === "sync-failed") &&
      state.expectedHorseCount !== null &&
      state.writtenHorseCount !== null &&
      state.writtenHorseCount >= state.expectedHorseCount
    ) {
      const cacheResult = await cacheAndSyncCompletedRunningStyles(env, job);
      // Keep the state honest (2026-08-16 incident: Neon write failures were
      // recorded as completed, leaving races with zero Neon rows while every
      // state-based monitor reported success). Neon success upgrades
      // sync-failed back to completed; Neon failure downgrades completed to
      // sync-failed so the planner keeps re-enqueuing the mirror retry.
      const expectedHorseCount = state.expectedHorseCount;
      const neonSyncOk =
        cacheResult.neonError === undefined && cacheResult.neonWrittenCount >= expectedHorseCount;
      const stateMetadata = {
        cellModelKey: state.cellModelKey ?? null,
        cellVariantId: state.cellVariantId ?? null,
        expectedHorseCount,
        featuresR2Key: state.featuresR2Key ?? "",
        modelVersion: state.modelVersion ?? "completed",
        raceKey,
        writtenHorseCount: state.writtenHorseCount,
      };
      if (neonSyncOk) {
        if (state.status === "sync-failed") {
          await markRunningStyleInferenceCompleted(env.REALTIME_DB, {
            ...stateMetadata,
            completedAt: new Date().toISOString(),
          });
        }
      } else {
        await markRunningStyleInferenceSyncFailed(env.REALTIME_DB, {
          ...stateMetadata,
          attemptedAt: new Date().toISOString(),
          errorMessage:
            cacheResult.neonError ??
            `Neon sync wrote ${cacheResult.neonWrittenCount}/${expectedHorseCount} rows`,
        });
      }
      const route = resolveRunningStyleCellRoute(
        routeInputFromJob(job),
        runningStyleCellRoutingConfig(env),
      );
      const finishPositionTrigger =
        state.status === "completed"
          ? {
              finishPositionTriggerMode: "skipped" as const,
              parquetExportedRows: 0,
            }
          : await triggerFinishPositionDayWhenReady(
              env,
              job,
              cacheResult,
              state.expectedHorseCount,
              state.writtenHorseCount,
            );
      return {
        ...cacheResult,
        cellModelKey: state.cellModelKey ?? route.modelKey,
        cellVariantId: state.cellVariantId ?? route.variantId,
        featuresR2Key: state.featuresR2Key ?? "",
        ...finishPositionTrigger,
        horseCount: state.expectedHorseCount,
        modelVersion: state.modelVersion ?? "completed",
        raceKey,
        skipped: true,
        writtenCount: state.writtenHorseCount,
      };
    }
    await markRunningStyleInferenceProcessing(env.REALTIME_DB, job, new Date().toISOString());
    const routingConfig = runningStyleCellRoutingConfig(env);
    const latestEntries = await getLatestRaceEntries(
      env.REALTIME_DB,
      buildRealtimeRaceKeyFromRunningStyle(job),
    );
    let selectedRoute = resolveRunningStyleCellRoute(routeInputFromJob(job), routingConfig);
    let model = await loadFlatLightGBMModelFromR2(env.RUNNING_STYLE_MODELS, selectedRoute.modelKey);
    const calibrators = await tryLoadCalibrators(env.RUNNING_STYLE_MODELS, job.source);
    let featureNames = model.header.feature_names;
    let loadOrBuild = await loadOrBuildRunningStyleFeatureParquet({
      env,
      featureNames,
      race: job,
    });
    const routeFromRows = resolveRouteFromRows(job, loadOrBuild.rows, routingConfig);
    if (routeFromRows.modelKey !== selectedRoute.modelKey) {
      selectedRoute = routeFromRows;
      model = await loadFlatLightGBMModelFromR2(env.RUNNING_STYLE_MODELS, selectedRoute.modelKey);
      featureNames = model.header.feature_names;
      loadOrBuild = await loadOrBuildRunningStyleFeatureParquet({
        env,
        featureNames,
        race: job,
      });
    }
    const inferenceRows = filterRunningStyleFeatureRowsByActiveEntries(
      loadOrBuild.rows,
      latestEntries,
    );
    if (inferenceRows.length === 0) {
      throw new Error(`no active running-style feature rows found for race ${raceKey}`);
    }
    const expectedHorseCount = resolveRunningStyleExpectedHorseCount(
      inferenceRows.length,
      latestEntries,
    );
    const completedAt = new Date().toISOString();
    await markFinishPositionFeaturesCached(env.REALTIME_DB, job, {
      attemptedAt: job.predictedAt,
      completedAt,
      featuresR2Key: loadOrBuild.featuresR2Key,
      modelVersion: model.header.model_version,
    });
    await putFinishPositionInputsCache({
      env,
      payload: {
        featuresR2Key: loadOrBuild.featuresR2Key,
        modelVersion: model.header.model_version,
        raceKey,
      },
      race: job,
    });
    const summary = await runRunningStyleInferenceRowsWithFlatModel(env.REALTIME_DB, {
      calibrators,
      cellModelKey: selectedRoute.modelKey,
      cellVariantId: selectedRoute.variantId,
      model,
      predictedAt: job.predictedAt,
      rows: inferenceRows,
    });
    const completionInput = {
      cellModelKey: selectedRoute.modelKey,
      cellVariantId: selectedRoute.variantId,
      expectedHorseCount,
      featuresR2Key: loadOrBuild.featuresR2Key,
      modelVersion: summary.modelVersion,
      raceKey,
      writtenHorseCount: summary.writtenCount,
    };
    const cacheResult =
      summary.writtenCount >= expectedHorseCount
        ? await cacheAndSyncCompletedRunningStyles(env, job)
        : { cacheWritten: false, neonWrittenCount: 0, parquetExportedRows: 0 };
    // Record completed ONLY when the Neon mirror actually has every row
    // (2026-08-16 incident: completed was written before the Neon sync, so
    // Neon failures left a completed state with zero Neon rows that no
    // state-based monitor could detect). A failed Neon sync is recorded as
    // sync-failed instead; the planner re-enqueues it and the fast path
    // above retries just the sync until it upgrades to completed.
    if (
      summary.writtenCount >= expectedHorseCount &&
      (cacheResult.neonError !== undefined || cacheResult.neonWrittenCount < expectedHorseCount)
    ) {
      await markRunningStyleInferenceSyncFailed(env.REALTIME_DB, {
        ...completionInput,
        attemptedAt: new Date().toISOString(),
        errorMessage:
          cacheResult.neonError ??
          `Neon sync wrote ${cacheResult.neonWrittenCount}/${expectedHorseCount} rows`,
      });
    } else {
      await markRunningStyleInferenceCompleted(env.REALTIME_DB, {
        ...completionInput,
        completedAt: new Date().toISOString(),
      });
    }
    const finishPositionTrigger = await triggerFinishPositionDayWhenReady(
      env,
      job,
      cacheResult,
      expectedHorseCount,
      summary.writtenCount,
    );
    return {
      ...cacheResult,
      cellModelKey: selectedRoute.modelKey,
      cellVariantId: selectedRoute.variantId,
      featuresR2Key: loadOrBuild.featuresR2Key,
      ...finishPositionTrigger,
      horseCount: inferenceRows.length,
      modelVersion: summary.modelVersion,
      raceKey,
      writtenCount: summary.writtenCount,
    };
  } catch (error) {
    console.error(formatErrorLogLine("Running-style prediction failed", { raceKey }, error));
    try {
      await markRunningStyleInferenceFailed(env.REALTIME_DB, raceKey, error);
    } catch (stateUpdateError) {
      console.error(
        formatErrorLogLine(
          "Running-style inference state update failed",
          { raceKey },
          stateUpdateError,
        ),
      );
    }
    throw error;
  }
};
