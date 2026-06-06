// Run with bun. Cron planner for running-style prediction generation.
// It inspects Cloudflare D1 race registrations for a date, checks whether
// race_running_styles already has all runners, and queues per-race Worker
// jobs for missing predictions.

import type { Pool } from "pg";

import { formatError } from "./format-error";
import { getFinishPositionPool } from "./finish-position-lite-pool";
import {
  listRaceRunningStyleCounts,
  listRaceRunningStylesForRace,
  listRunningStyleInferenceStates,
  upsertRunningStylePendingStates,
  type RunningStyleInferenceStateDetail,
  type RunningStylePendingRace,
} from "./running-style-d1";
import { listRunningStyleExpectedHorseCounts } from "./running-style-expected-horses";
import {
  exportRunningStyleParquetForDay,
  type ExportRunningStyleParquetResult,
} from "./running-style-parquet-export";
import { putViewerRunningStyleRaceCache } from "./viewer-running-style-cache";
import {
  buildRunningStyleRaceKey,
  normalizeKeibajoCode,
  normalizeRaceBango,
  parseRunningStyleRaceKey,
  type RunningStyleSource,
} from "./running-style-features";
import { listRunningStyleRacesByDate } from "./running-style-race-list";
import type { Env, RunningStylePredictionJob } from "./types";

export const RUNNING_STYLE_INFERENCE_CRON = "*/10 * * * *";
export const RUNNING_STYLE_PREWARM_CRON = "0 12 * * *";

const ENABLED_FLAG = "1";
const DATE_PAD_WIDTH = 2;
const QUEUE_SEND_BATCH_SIZE = 100;
const ACTIVE_STATE_TTL_MS = 5 * 60 * 1000;
const ACTIVE_STATUSES = new Set(["pending", "processing"]);
// 2026-06-04 incident: before JST midnight rolled over, the cron derived
// today=06-03 and so never retried the stalled 06-04 races. Sweeping the last
// 6 hours of yesterday-JST keeps post-midnight races eligible for retry
// without re-processing dates far enough in the past that all states are
// settled.
const YESTERDAY_SWEEP_HOURS = 6;
const JST_OFFSET_HOURS = 9;

export interface RegisteredRaceRow {
  source: RunningStyleSource;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
}

interface FeatureCountRow {
  race_key: string;
  count: string;
}

export interface RunningStylePlanRace extends RunningStylePendingRace {
  existingHorseCount: number;
}

export interface RunningStyleParquetExportSummary {
  bytesWritten: number;
  exportError?: string;
  fileCount: number;
  rowCount: number;
}

export interface RunningStylePlanSummary {
  alreadyQueued: number;
  cacheRefresh?: ViewerRunningStyleCacheRefreshSummary;
  completed: number;
  date: string;
  enqueued: number;
  featureReady: number;
  missingFeatures: number;
  parquetExport?: RunningStyleParquetExportSummary;
  planError?: string;
  scanned: number;
}

const padDatePart = (value: number): string => String(value).padStart(DATE_PAD_WIDTH, "0");

export const formatYYYYMMDDInJst = (now: Date): string => {
  const utcMillis = now.getTime();
  const jstOffsetMinutes = 9 * 60;
  const jst = new Date(utcMillis + jstOffsetMinutes * 60 * 1000);
  return `${jst.getUTCFullYear()}${padDatePart(jst.getUTCMonth() + 1)}${padDatePart(jst.getUTCDate())}`;
};

export const addDaysToYYYYMMDDInJst = (yyyymmdd: string, days: number): string => {
  const date = new Date(
    `${yyyymmdd.slice(0, 4)}-${yyyymmdd.slice(4, 6)}-${yyyymmdd.slice(6, 8)}T00:00:00+09:00`,
  );
  date.setUTCDate(date.getUTCDate() + days);
  return formatYYYYMMDDInJst(date);
};

export const formatTomorrowYYYYMMDDInJst = (now: Date): string =>
  addDaysToYYYYMMDDInJst(formatYYYYMMDDInJst(now), 1);

const isInferenceEnabled = (env: Env): boolean =>
  env.RUNNING_STYLE_D1_WRITE_ENABLED === ENABLED_FLAG;

const toRunningStylePendingRace = (
  row: RegisteredRaceRow,
  expectedHorseCount: number,
): RunningStylePendingRace => {
  const race = {
    kaisaiNen: row.kaisai_nen,
    kaisaiTsukihi: row.kaisai_tsukihi,
    keibajoCode: normalizeKeibajoCode(row.keibajo_code),
    raceBango: normalizeRaceBango(row.race_bango),
    source: row.source,
  };
  return {
    ...race,
    expectedHorseCount,
    raceKey: buildRunningStyleRaceKey(race),
  };
};

const listFeatureCountsByDate = async (pool: Pool, date: string): Promise<Map<string, number>> => {
  const featureResult = await pool.query<FeatureCountRow>(
    `
      select
        source || ':' || kaisai_nen || kaisai_tsukihi || ':' ||
          lpad(keibajo_code::text, 2, '0') || ':' ||
          lpad(race_bango::text, 2, '0') as race_key,
        count(*)::text as count
      from race_entry_corner_features
      where source in ('jra', 'nar')
        and race_date = $1
      group by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
    `,
    [date],
  );
  const counts = new Map<string, number>();
  featureResult.rows.forEach((row) => counts.set(row.race_key, Number(row.count)));
  return counts;
};

const isActiveState = (state: RunningStyleInferenceStateDetail | undefined, now: Date): boolean => {
  if (state === undefined || !ACTIVE_STATUSES.has(state.status)) return false;
  if (state.attemptedAt === null) return true;
  const attemptedAt = new Date(state.attemptedAt).getTime();
  if (Number.isNaN(attemptedAt)) return false;
  return now.getTime() - attemptedAt <= ACTIVE_STATE_TTL_MS;
};

// Planner does not gate on race_entry_corner_features anymore: today's races
// often arrive in realtime_race_sources before their derived feature cache is
// built. The per-race worker `handleRunningStylePredictionJob` builds features
// from upstream nvd_se/jvd_se directly, so we enqueue any registered race that
// lacks predictions and let the worker resolve features at inference time.
export const selectRacesNeedingRunningStyleInference = (
  registeredRaces: ReadonlyArray<RegisteredRaceRow>,
  featureCounts: ReadonlyMap<string, number>,
  expectedHorseCounts: ReadonlyMap<string, number>,
  predictionCounts: ReadonlyMap<string, number>,
  states: ReadonlyMap<string, RunningStyleInferenceStateDetail>,
  now = new Date(),
): {
  alreadyQueued: number;
  completed: number;
  featureReady: number;
  missingFeatures: number;
  needed: RunningStylePlanRace[];
} => {
  let alreadyQueued = 0;
  let completed = 0;
  let featureReady = 0;
  let missingFeatures = 0;
  const needed: RunningStylePlanRace[] = [];

  registeredRaces.forEach((row) => {
    const race = toRunningStylePendingRace(row, 0);
    const featureCount = featureCounts.get(race.raceKey) ?? 0;
    if (featureCount > 0) {
      featureReady += 1;
    } else {
      missingFeatures += 1;
    }
    const expectedHorseCount = expectedHorseCounts.get(race.raceKey) ?? featureCount;
    const existingHorseCount = predictionCounts.get(race.raceKey) ?? 0;
    const state = states.get(race.raceKey);
    const stateCompleted =
      state?.status === "completed" &&
      state.writtenHorseCount !== null &&
      state.expectedHorseCount !== null &&
      state.writtenHorseCount >= state.expectedHorseCount;
    const predictionsMeetExpected =
      expectedHorseCount > 0 && existingHorseCount >= expectedHorseCount;
    if (stateCompleted || predictionsMeetExpected) {
      completed += 1;
      return;
    }
    if (isActiveState(state, now)) {
      alreadyQueued += 1;
      return;
    }
    needed.push({
      ...race,
      existingHorseCount,
      expectedHorseCount,
    });
  });

  return { alreadyQueued, completed, featureReady, missingFeatures, needed };
};

const toPredictionJob = (
  row: RunningStylePlanRace,
  predictedAt: string,
): RunningStylePredictionJob => ({
  kaisaiNen: row.kaisaiNen,
  kaisaiTsukihi: row.kaisaiTsukihi,
  keibajoCode: row.keibajoCode,
  predictedAt,
  raceBango: row.raceBango,
  raceKey: row.raceKey,
  source: row.source,
  type: "generate-running-style-predictions",
});

const sendPredictionJobs = async (
  queue: Queue,
  jobs: ReadonlyArray<RunningStylePredictionJob>,
): Promise<void> => {
  for (let index = 0; index < jobs.length; index += QUEUE_SEND_BATCH_SIZE) {
    const chunk = jobs.slice(index, index + QUEUE_SEND_BATCH_SIZE);
    if (chunk.length === 1) {
      await queue.send(chunk[0]);
      continue;
    }
    await queue.sendBatch(chunk.map((body) => ({ body })));
  }
};

export const planRunningStylePredictionsForDate = async (
  env: Env,
  date: string,
  now: Date,
): Promise<RunningStylePlanSummary> => {
  const { races: registeredRaces } = await listRunningStyleRacesByDate(env, date);
  if (!isInferenceEnabled(env)) {
    return {
      alreadyQueued: 0,
      completed: 0,
      date,
      enqueued: 0,
      featureReady: 0,
      missingFeatures: registeredRaces.length,
      scanned: registeredRaces.length,
    };
  }
  const pool = getFinishPositionPool(env);
  const featureCounts = await listFeatureCountsByDate(pool, date);
  const raceKeys = registeredRaces.map((row) =>
    buildRunningStyleRaceKey({
      kaisaiNen: row.kaisai_nen,
      kaisaiTsukihi: row.kaisai_tsukihi,
      keibajoCode: row.keibajo_code,
      raceBango: row.race_bango,
      source: row.source,
    }),
  );
  const [predictionCounts, states, expectedHorseCounts] = await Promise.all([
    listRaceRunningStyleCounts(env.REALTIME_DB, raceKeys, { bypassCache: true }),
    listRunningStyleInferenceStates(env.REALTIME_DB, raceKeys),
    listRunningStyleExpectedHorseCounts(env.REALTIME_DB, raceKeys, featureCounts),
  ]);
  const selected = selectRacesNeedingRunningStyleInference(
    registeredRaces,
    featureCounts,
    expectedHorseCounts,
    predictionCounts,
    states,
    now,
  );
  const predictedAt = now.toISOString();
  await upsertRunningStylePendingStates(env.REALTIME_DB, selected.needed, predictedAt);
  await sendPredictionJobs(
    env.RUNNING_STYLE_JOBS ?? env.REALTIME_JOBS,
    selected.needed.map((row) => toPredictionJob(row, predictedAt)),
  );
  return {
    alreadyQueued: selected.alreadyQueued,
    completed: selected.completed,
    date,
    enqueued: selected.needed.length,
    featureReady: selected.featureReady,
    missingFeatures: selected.missingFeatures,
    scanned: registeredRaces.length,
  };
};

const EMPTY_PLAN_SUMMARY = (date: string, error: string): RunningStylePlanSummary => ({
  alreadyQueued: 0,
  completed: 0,
  date,
  enqueued: 0,
  featureReady: 0,
  missingFeatures: 0,
  planError: error,
  scanned: 0,
});

const isWithinYesterdaySweepWindow = (now: Date): boolean => {
  const jstHour = (now.getUTCHours() + JST_OFFSET_HOURS) % 24;
  return jstHour < YESTERDAY_SWEEP_HOURS;
};

export const resolveRunningStyleCronDates = (now: Date): string[] => {
  const today = formatYYYYMMDDInJst(now);
  if (!isWithinYesterdaySweepWindow(now)) {
    return [today];
  }
  const yesterday = addDaysToYYYYMMDDInJst(today, -1);
  return [yesterday, today];
};

const planRunningStyleForDateSafe = async (
  env: Env,
  date: string,
  now: Date,
): Promise<RunningStylePlanSummary> =>
  planRunningStylePredictionsForDate(env, date, now).catch((error: unknown) =>
    EMPTY_PLAN_SUMMARY(date, formatError(error)),
  );

const refreshViewerCacheSafe = async (
  env: Env,
  date: string,
  ctx: ExecutionContext | undefined,
): Promise<ViewerRunningStyleCacheRefreshSummary> =>
  refreshViewerRunningStyleCachesForDate(env, date, ctx).catch((error: unknown) => ({
    date,
    refreshed: 0,
    refreshError: formatError(error),
    scanned: 0,
    skipped: 0,
  }));

const mergeCacheRefresh = (
  current: ViewerRunningStyleCacheRefreshSummary | undefined,
  next: ViewerRunningStyleCacheRefreshSummary,
): ViewerRunningStyleCacheRefreshSummary => {
  if (current === undefined) return next;
  return {
    date: next.date,
    refreshed: current.refreshed + next.refreshed,
    refreshError: next.refreshError ?? current.refreshError,
    scanned: current.scanned + next.scanned,
    skipped: current.skipped + next.skipped,
  };
};

const SOURCES_FOR_PARQUET_EXPORT: ReadonlyArray<"jra" | "nar"> = ["jra", "nar"];

const exportParquetSafe = async (
  env: Env,
  date: string,
): Promise<RunningStyleParquetExportSummary> => {
  const exportOne = async (
    source: "jra" | "nar",
  ): Promise<ExportRunningStyleParquetResult | { exportError: string }> =>
    exportRunningStyleParquetForDay({ dateYmd: date, env, source }).catch((error: unknown) => ({
      exportError: formatError(error),
    }));
  const results = await Promise.all(SOURCES_FOR_PARQUET_EXPORT.map(exportOne));
  const ok = results.filter((r): r is ExportRunningStyleParquetResult => !("exportError" in r));
  const firstError = results.find((r): r is { exportError: string } => "exportError" in r);
  return {
    bytesWritten: ok.reduce((sum, r) => sum + r.bytesWritten, 0),
    exportError: firstError?.exportError,
    fileCount: ok.reduce((sum, r) => sum + r.fileCount, 0),
    rowCount: ok.reduce((sum, r) => sum + r.rowCount, 0),
  };
};

const mergeParquetExport = (
  current: RunningStyleParquetExportSummary | undefined,
  next: RunningStyleParquetExportSummary,
): RunningStyleParquetExportSummary => {
  if (current === undefined) return next;
  return {
    bytesWritten: current.bytesWritten + next.bytesWritten,
    exportError: next.exportError ?? current.exportError,
    fileCount: current.fileCount + next.fileCount,
    rowCount: current.rowCount + next.rowCount,
  };
};

const resolveMergedParquetExport = (
  current: RunningStyleParquetExportSummary | undefined,
  next: RunningStyleParquetExportSummary | undefined,
): RunningStyleParquetExportSummary | undefined => {
  if (current === undefined && next === undefined) return undefined;
  if (current === undefined) return next;
  if (next === undefined) return current;
  return mergeParquetExport(current, next);
};

const mergeRunningStylePlan = (
  current: RunningStylePlanSummary | undefined,
  next: RunningStylePlanSummary,
): RunningStylePlanSummary => {
  if (current === undefined) return next;
  return {
    alreadyQueued: current.alreadyQueued + next.alreadyQueued,
    completed: current.completed + next.completed,
    date: next.date,
    enqueued: current.enqueued + next.enqueued,
    featureReady: current.featureReady + next.featureReady,
    missingFeatures: current.missingFeatures + next.missingFeatures,
    parquetExport: resolveMergedParquetExport(current.parquetExport, next.parquetExport),
    planError: next.planError ?? current.planError,
    scanned: current.scanned + next.scanned,
  };
};

interface RunningStyleCronAccumulator {
  cacheRefresh: ViewerRunningStyleCacheRefreshSummary | undefined;
  plan: RunningStylePlanSummary | undefined;
}

const planAndRefreshForDate = async (
  env: Env,
  date: string,
  now: Date,
  ctx: ExecutionContext | undefined,
  acc: RunningStyleCronAccumulator,
): Promise<RunningStyleCronAccumulator> => {
  const plan = await planRunningStyleForDateSafe(env, date, now);
  const cacheRefresh = await refreshViewerCacheSafe(env, date, ctx);
  const parquetExport = isInferenceEnabled(env) ? await exportParquetSafe(env, date) : undefined;
  const planWithExport: RunningStylePlanSummary =
    parquetExport === undefined ? plan : { ...plan, parquetExport };
  return {
    cacheRefresh: mergeCacheRefresh(acc.cacheRefresh, cacheRefresh),
    plan: mergeRunningStylePlan(acc.plan, planWithExport),
  };
};

export const runRunningStyleCronTick = async (
  env: Env,
  now: Date,
  ctx?: ExecutionContext,
): Promise<RunningStylePlanSummary> => {
  const dates = resolveRunningStyleCronDates(now);
  const aggregated = await dates.reduce<Promise<RunningStyleCronAccumulator>>(
    async (accPromise, date) => {
      const acc = await accPromise;
      return planAndRefreshForDate(env, date, now, ctx, acc);
    },
    Promise.resolve({ cacheRefresh: undefined, plan: undefined }),
  );
  // resolveRunningStyleCronDates always yields at least today, so plan +
  // cacheRefresh are guaranteed to be populated after the reduce above.
  return { ...aggregated.plan!, cacheRefresh: aggregated.cacheRefresh! };
};

export const refreshViewerRunningStyleCacheForRace = async (
  env: Env,
  raceKey: string,
  ctx?: ExecutionContext,
): Promise<boolean> => {
  const race = parseRunningStyleRaceKey(raceKey);
  if (race === null) {
    return false;
  }
  const rows = await listRaceRunningStylesForRace(env.REALTIME_DB, raceKey, {
    bypassCache: true,
    ctx,
  });
  if (rows.length === 0) {
    return false;
  }
  return putViewerRunningStyleRaceCache({
    ctx,
    env,
    race,
    rows,
  });
};

export interface ViewerRunningStyleCacheRefreshSummary {
  date: string;
  refreshed: number;
  refreshError?: string;
  scanned: number;
  skipped: number;
}

export const refreshViewerRunningStyleCachesForDate = async (
  env: Env,
  date: string,
  ctx?: ExecutionContext,
): Promise<ViewerRunningStyleCacheRefreshSummary> => {
  const { races: registeredRaces } = await listRunningStyleRacesByDate(env, date);
  if (!isInferenceEnabled(env) || registeredRaces.length === 0) {
    return {
      date,
      refreshed: 0,
      scanned: registeredRaces.length,
      skipped: registeredRaces.length,
    };
  }

  const raceKeys = registeredRaces.map((row) =>
    buildRunningStyleRaceKey({
      kaisaiNen: row.kaisai_nen,
      kaisaiTsukihi: row.kaisai_tsukihi,
      keibajoCode: row.keibajo_code,
      raceBango: row.race_bango,
      source: row.source,
    }),
  );
  const predictionCounts = await listRaceRunningStyleCounts(env.REALTIME_DB, raceKeys, {
    bypassCache: true,
    ctx,
  });
  let refreshed = 0;
  let skipped = 0;

  for (const row of registeredRaces) {
    const race = {
      kaisaiNen: row.kaisai_nen,
      kaisaiTsukihi: row.kaisai_tsukihi,
      keibajoCode: normalizeKeibajoCode(row.keibajo_code),
      raceBango: normalizeRaceBango(row.race_bango),
      source: row.source,
    };
    const raceKey = buildRunningStyleRaceKey(race);
    const existingHorseCount = predictionCounts.get(raceKey) ?? 0;
    if (existingHorseCount === 0) {
      skipped += 1;
      continue;
    }
    const rows = await listRaceRunningStylesForRace(env.REALTIME_DB, raceKey, {
      bypassCache: true,
      ctx,
    });
    if (rows.length === 0) {
      skipped += 1;
      continue;
    }
    const cacheWritten = await putViewerRunningStyleRaceCache({
      ctx,
      env,
      race: { ...race, raceKey },
      rows,
    });
    if (cacheWritten) {
      refreshed += 1;
    } else {
      skipped += 1;
    }
  }

  return {
    date,
    refreshed,
    scanned: registeredRaces.length,
    skipped,
  };
};
