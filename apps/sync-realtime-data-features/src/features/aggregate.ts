// Run with bun. Pure aggregation helpers for the race-trend endpoint.
// Ported from apps/pc-keiba-viewer/src/lib/race-trend-aggregate.ts (browser-safe
// pure helpers). Worker side keeps a minimal subset focused on byJockey /
// byWaku tallies plus DailyRaceEntryRow -> StarterRow conversion. The viewer
// retains its richer aggregateForTargets path; this module duplicates only the
// normalisation helpers that need exact parity with the viewer for golden
// cases (parseStoredPopularity / parseStoredWinOdds / runningStyleFromCorners
// / buildRaceKey).

import type { DailyRaceEntryRow } from "../types";

const POPULARITY_EMPTY = "00";
const ODDS_EMPTY = "0000";
const ODDS_TENTH_DIVISOR = 10;
const CORNER_EMPTY = "00";
const RACE_BANGO_PAD_WIDTH = 2;
const KEIBAJO_PAD_WIDTH = 2;
const NIGE_CORNER_THRESHOLD = 1;
const SENKOU_RATIO_THRESHOLD = 0.35;
const SASHI_RATIO_THRESHOLD = 0.7;
const SENKOU_CORNER_FALLBACK = 4;
const SASHI_CORNER_FALLBACK = 8;
const WIN_FINISH = 1;
const QUINELLA_FINISH = 2;
const SHOW_FINISH = 3;

export type RaceTrendRunningStyle = "nige" | "senkou" | "sashi" | "oikomi";

export interface RaceTrendStarterRow {
  source: "jra" | "nar";
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  raceName: string | null;
  hassoJikoku: string | null;
  runnerCount: string | null;
  wakuban: string | null;
  umaban: string | null;
  bamei: string | null;
  jockeyName: string | null;
  tanshoOdds: string | null;
  tanshoPopularity: string | null;
  finishPosition: number;
  sohaTime: string | null;
  corner1: string | null;
  corner2: string | null;
  corner3: string | null;
  corner4: string | null;
  bataiju: string | null;
  zogenFugo: string | null;
  zogenSa: string | null;
}

export interface RaceTrendBucketStats {
  runs: number;
  wins: number;
  shows: number;
  quinellas: number;
}

export interface RaceLookupKeys {
  source: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

export const normalizeText = (value: string | null | undefined): string | null => {
  const normalized = value?.trim();
  return normalized ? normalized : null;
};

export const normalizeNumberText = (value: string | null | undefined): string | null => {
  const normalized = normalizeText(value);
  if (!normalized) return null;
  return normalized.replace(/^0+(?=\d)/, "");
};

export const parseStoredInteger = (
  value: string | null | undefined,
  emptyValue: string,
): number | null => {
  const normalized = normalizeText(value);
  if (!normalized || normalized === emptyValue) return null;
  const parsed = Number(normalized.replace(/[^\d]/g, ""));
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
};

export const parseStoredPopularity = (value: string | null | undefined): number | null =>
  parseStoredInteger(value, POPULARITY_EMPTY);

export const parseStoredWinOdds = (value: string | null | undefined): number | null => {
  const odds = parseStoredInteger(value, ODDS_EMPTY);
  return odds === null ? null : odds / ODDS_TENTH_DIVISOR;
};

export const parseCornerPosition = (value: string | null | undefined): number | null =>
  parseStoredInteger(value, CORNER_EMPTY);

interface RunningStyleCornerInput {
  corner1: string | null | undefined;
  corner2: string | null | undefined;
  corner3: string | null | undefined;
  corner4: string | null | undefined;
  runnerCount: string | null | undefined;
}

const resolveRunningStyleFromRatio = (
  corner: number,
  runnerCount: number,
): RaceTrendRunningStyle => {
  const ratio = (corner - NIGE_CORNER_THRESHOLD) / Math.max(runnerCount - NIGE_CORNER_THRESHOLD, 1);
  if (ratio <= SENKOU_RATIO_THRESHOLD) return "senkou";
  if (ratio <= SASHI_RATIO_THRESHOLD) return "sashi";
  return "oikomi";
};

const resolveRunningStyleFromCorner = (corner: number): RaceTrendRunningStyle => {
  if (corner <= SENKOU_CORNER_FALLBACK) return "senkou";
  if (corner <= SASHI_CORNER_FALLBACK) return "sashi";
  return "oikomi";
};

export const runningStyleFromCorners = (
  input: RunningStyleCornerInput,
): RaceTrendRunningStyle | null => {
  const corner =
    parseCornerPosition(input.corner1) ??
    parseCornerPosition(input.corner2) ??
    parseCornerPosition(input.corner3) ??
    parseCornerPosition(input.corner4);
  if (corner === null) return null;
  if (corner <= NIGE_CORNER_THRESHOLD) return "nige";
  const parsedRunnerCount = parseStoredInteger(input.runnerCount, POPULARITY_EMPTY);
  if (parsedRunnerCount === null || parsedRunnerCount <= NIGE_CORNER_THRESHOLD) {
    return resolveRunningStyleFromCorner(corner);
  }
  return resolveRunningStyleFromRatio(corner, parsedRunnerCount);
};

export const buildRaceKey = (keys: RaceLookupKeys): string =>
  `${keys.source}:${keys.kaisaiNen}${keys.kaisaiTsukihi}:${keys.keibajoCode.padStart(KEIBAJO_PAD_WIDTH, "0")}:${keys.raceBango.padStart(RACE_BANGO_PAD_WIDTH, "0")}`;

const numberOrNull = (value: number | null): string | null =>
  value === null ? null : String(value);

const padNumber = (value: number | null, width: number): string | null =>
  value === null ? null : String(value).padStart(width, "0");

const oddsToStored = (value: number | null): string | null =>
  value === null ? null : padNumber(Math.round(value * ODDS_TENTH_DIVISOR), 4);

const popularityToStored = (value: number | null): string | null => padNumber(value, 2);

export const dailyRowToStarterRow = (row: DailyRaceEntryRow): RaceTrendStarterRow => ({
  source: row.source,
  kaisaiNen: row.kaisai_nen,
  kaisaiTsukihi: row.kaisai_tsukihi,
  keibajoCode: row.keibajo_code,
  raceBango: row.race_bango,
  raceName: row.race_name,
  hassoJikoku: row.hasso_jikoku,
  runnerCount: numberOrNull(row.shusso_tosu),
  wakuban: row.wakuban,
  umaban: numberOrNull(row.umaban),
  bamei: row.bamei,
  jockeyName: row.kishumei_ryakusho,
  tanshoOdds: oddsToStored(row.tansho_odds),
  tanshoPopularity: popularityToStored(row.tansho_ninkijun),
  finishPosition: row.finish_position ?? 0,
  sohaTime: numberOrNull(row.soha_time),
  corner1: numberOrNull(row.corner_1),
  corner2: numberOrNull(row.corner_2),
  corner3: numberOrNull(row.corner_3),
  corner4: numberOrNull(row.corner_4),
  bataiju: numberOrNull(row.bataiju),
  zogenFugo: row.zogen_fugo,
  zogenSa: numberOrNull(row.zogen_sa),
});

const emptyBucket = (): RaceTrendBucketStats => ({ quinellas: 0, runs: 0, shows: 0, wins: 0 });

const incrementBucket = (bucket: RaceTrendBucketStats, finishPosition: number): void => {
  bucket.runs += 1;
  if (finishPosition === WIN_FINISH) bucket.wins += 1;
  if (finishPosition <= QUINELLA_FINISH) bucket.quinellas += 1;
  if (finishPosition <= SHOW_FINISH) bucket.shows += 1;
};

const getOrInsertBucket = (
  buckets: Record<string, RaceTrendBucketStats>,
  key: string,
): RaceTrendBucketStats => {
  const existing = buckets[key];
  if (existing) return existing;
  const created = emptyBucket();
  buckets[key] = created;
  return created;
};

interface AggregationAccumulator {
  byJockey: Record<string, RaceTrendBucketStats>;
  byWaku: Record<string, RaceTrendBucketStats>;
  raceKeys: Set<string>;
}

const buildAccumulator = (): AggregationAccumulator => ({
  byJockey: {},
  byWaku: {},
  raceKeys: new Set<string>(),
});

const accumulateRow = (acc: AggregationAccumulator, row: DailyRaceEntryRow): void => {
  acc.raceKeys.add(
    buildRaceKey({
      source: row.source,
      kaisaiNen: row.kaisai_nen,
      kaisaiTsukihi: row.kaisai_tsukihi,
      keibajoCode: row.keibajo_code,
      raceBango: row.race_bango,
    }),
  );
  // Skip rows without a finish_position to avoid skewing wins / shows /
  // quinellas. Unfinished races are still counted in raceKeys and starterRows.
  if (row.finish_position === null) return;
  const jockey = normalizeText(row.kishumei_ryakusho);
  if (jockey !== null) {
    incrementBucket(getOrInsertBucket(acc.byJockey, jockey), row.finish_position);
  }
  const waku = normalizeText(row.wakuban);
  if (waku !== null) {
    incrementBucket(getOrInsertBucket(acc.byWaku, waku), row.finish_position);
  }
};

export interface RaceTrendAggregateResult {
  starterRows: RaceTrendStarterRow[];
  raceCount: number;
  starterCount: number;
  byJockey: Record<string, RaceTrendBucketStats>;
  byWaku: Record<string, RaceTrendBucketStats>;
}

export const aggregateRaceTrendRows = (rows: DailyRaceEntryRow[]): RaceTrendAggregateResult => {
  const acc = buildAccumulator();
  for (const row of rows) accumulateRow(acc, row);
  return {
    byJockey: acc.byJockey,
    byWaku: acc.byWaku,
    raceCount: acc.raceKeys.size,
    starterCount: rows.length,
    starterRows: rows.map(dailyRowToStarterRow),
  };
};
