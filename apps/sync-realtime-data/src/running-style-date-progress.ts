// Run with bun. Progress helpers for per-date running-style batch runs.

import { getFinishPositionPool } from "./finish-position-lite-pool";
import {
  getRunningStyleInferenceState,
  listRaceRunningStyleCounts,
  type RunningStyleInferenceStatus,
} from "./running-style-d1";
import { evaluateRunningStyleCacheCoverage } from "./running-style-entry-coverage";
import { isViewerRunningStyleRaceCacheReady } from "./viewer-running-style-cache-probe";
import {
  buildRunningStyleRaceKey,
  normalizeKeibajoCode,
  normalizeRaceBango,
  type RunningStyleSource,
} from "./running-style-features";
import { formatYYYYMMDDInJst } from "./running-style-cron";
import { listRunningStyleRacesByDate } from "./running-style-race-list";
import { getLatestRaceEntries } from "./storage";
import type { Env } from "./types";

export interface RunningStyleDateProgressRow {
  cacheReady: boolean;
  d1Count: number;
  displayReady: boolean;
  expectedHorses: number;
  featuresReady: boolean;
  inferenceStatus: RunningStyleInferenceStatus | "missing";
  parquetReady: boolean;
  raceKey: string;
  source: RunningStyleSource;
}

export interface RunningStyleDateProgressSummary {
  cacheReady: number;
  d1Ready: number;
  displayReady: number;
  expectedHorses: number;
  featureReady: number;
  incomplete: number;
  parquetReady: number;
  scanned: number;
}

const listFeatureCountsByDate = async (env: Env, date: string): Promise<Map<string, number>> => {
  const pool = getFinishPositionPool(env);
  const featureResult = await pool.query<{ count: string; race_key: string }>(
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

export const isRunningStyleDateProgressRowComplete = (
  row: RunningStyleDateProgressRow,
): boolean =>
  row.featuresReady &&
  row.inferenceStatus === "completed" &&
  row.d1Count >= row.expectedHorses &&
  row.parquetReady &&
  row.cacheReady;

export const isRunningStyleDateProgressRowDisplayReady = (
  row: RunningStyleDateProgressRow,
): boolean => row.cacheReady;

export const summarizeRunningStyleDateProgress = (
  rows: ReadonlyArray<RunningStyleDateProgressRow>,
): RunningStyleDateProgressSummary => {
  let cacheReady = 0;
  let d1Ready = 0;
  let displayReady = 0;
  let expectedHorses = 0;
  let featureReady = 0;
  let incomplete = 0;
  let parquetReady = 0;
  rows.forEach((row) => {
    if (row.expectedHorses > 0) {
      featureReady += 1;
      expectedHorses += row.expectedHorses;
    }
    if (row.d1Count >= row.expectedHorses && row.expectedHorses > 0) {
      d1Ready += 1;
    }
    if (row.parquetReady) {
      parquetReady += 1;
    }
    if (row.cacheReady) {
      cacheReady += 1;
    }
    if (row.displayReady) {
      displayReady += 1;
    }
    if (!isRunningStyleDateProgressRowDisplayReady(row)) {
      incomplete += 1;
    }
  });
  return {
    cacheReady,
    d1Ready,
    displayReady,
    expectedHorses,
    featureReady,
    incomplete,
    parquetReady,
    scanned: rows.length,
  };
};

export const collectRunningStyleDateProgress = async (
  env: Env,
  date: string,
): Promise<RunningStyleDateProgressRow[]> => {
  const { races: registeredRaces } = await listRunningStyleRacesByDate(env, date);
  if (registeredRaces.length === 0) {
    return [];
  }
  const featureCounts = await listFeatureCountsByDate(env, date);
  const raceKeys = registeredRaces.map((row) =>
    buildRunningStyleRaceKey({
      kaisaiNen: row.kaisai_nen,
      kaisaiTsukihi: row.kaisai_tsukihi,
      keibajoCode: row.keibajo_code,
      raceBango: row.race_bango,
      source: row.source,
    }),
  );
  const predictionCounts = await listRaceRunningStyleCounts(env.REALTIME_DB, raceKeys);
  const rows: RunningStyleDateProgressRow[] = [];
  for (const row of registeredRaces) {
    const race = {
      kaisaiNen: row.kaisai_nen,
      kaisaiTsukihi: row.kaisai_tsukihi,
      keibajoCode: normalizeKeibajoCode(row.keibajo_code),
      raceBango: normalizeRaceBango(row.race_bango),
      source: row.source,
    };
    const raceKey = buildRunningStyleRaceKey(race);
    const latestEntries = await getLatestRaceEntries(env.REALTIME_DB, raceKey);
    const activeHorseCount =
      latestEntries === null
        ? null
        : evaluateRunningStyleCacheCoverage(latestEntries.horses, []).activeHorseCount;
    const expectedHorses = activeHorseCount ?? featureCounts.get(raceKey) ?? 0;
    const d1Count = predictionCounts.get(raceKey) ?? 0;
    const state = await getRunningStyleInferenceState(env.REALTIME_DB, raceKey);
    const featuresR2Key = state?.featuresR2Key ?? null;
    const parquetReady =
      featuresR2Key !== null && featuresR2Key.length > 0
        ? (await env.RUNNING_STYLE_MODELS.head(featuresR2Key)) !== null
        : false;
    const inferenceStatus = state?.status ?? "missing";
    const cacheReady = await isViewerRunningStyleRaceCacheReady(env, { ...race, raceKey });
    const displayReady = cacheReady;
    rows.push({
      cacheReady,
      d1Count,
      displayReady,
      expectedHorses,
      featuresReady: expectedHorses > 0,
      inferenceStatus,
      parquetReady,
      raceKey,
      source: race.source,
    });
  }
  return rows;
};

export const resolveRunningStyleDateYmd = (date: string, year?: number, now = new Date()): string => {
  if (/^\d{8}$/u.test(date)) {
    return date;
  }
  const match = date.match(/^(\d{1,2})-(\d{1,2})$/u);
  if (!match) {
    throw new Error(`Invalid --date value "${date}". Use YYYYMMDD or MM-DD.`);
  }
  const resolvedYear = year ?? Number(formatYYYYMMDDInJst(now).slice(0, 4));
  const month = match[1]?.padStart(2, "0") ?? "01";
  const day = match[2]?.padStart(2, "0") ?? "01";
  return `${resolvedYear}${month}${day}`;
};
