// Run with bun. Progress helpers for per-date running-style batch runs.

import { fetchRunningStyleFeatureCountsFromCatalog } from "./running-style-catalog-client";
import {
  getRunningStyleInferenceState,
  listRaceRunningStyleCounts,
  type RunningStyleInferenceStatus,
} from "./running-style-d1";
import { evaluateRunningStyleCacheCoverage } from "./running-style-entry-coverage";
import { isViewerRunningStyleRaceCacheReady } from "./viewer-running-style-cache-probe";
import {
  buildRealtimeRaceKeyFromRunningStyle,
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

export const isRunningStyleDateProgressRowComplete = (row: RunningStyleDateProgressRow): boolean =>
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
  const featureCounts = await fetchRunningStyleFeatureCountsFromCatalog(
    env.PC_KEIBA_R2_CATALOG,
    date,
  );
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
    const latestEntries = await getLatestRaceEntries(
      env.REALTIME_DB,
      buildRealtimeRaceKeyFromRunningStyle(race),
    );
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

export const resolveRunningStyleDateYmd = (
  date: string,
  year?: number,
  now = new Date(),
): string => {
  if (/^\d{8}$/u.test(date)) {
    return date;
  }
  const match = date.match(/^(\d{1,2})-(\d{1,2})$/u);
  if (!match) {
    throw new Error(`Invalid --date value "${date}". Use YYYYMMDD or MM-DD.`);
  }
  const resolvedYear = year ?? Number(formatYYYYMMDDInJst(now).slice(0, 4));
  const month = match[1]!.padStart(2, "0");
  const day = match[2]!.padStart(2, "0");
  return `${resolvedYear}${month}${day}`;
};
