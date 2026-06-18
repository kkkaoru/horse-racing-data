// Run with bun. Read the Stage-1 R2 feature-cache parquet and group rows into
// per-race entry records for Stage-2 rescore. The cache is a single parquet per
// (category, runDate) put by the container's _try_r2_put — key format
// ``feat-cache/{category}/{runDate}/features.parquet`` (serve.build_r2_feat_cache_key).
// Each row carries the full early-binding feature set + a ``race_id`` column
// (``source:kaisai_nen:kaisai_tsukihi:keibajo_code:race_bango``) so the day's
// rows split into races exactly like pipeline_runner's frame.groupby(race_id).
//
// FORMAT DECISION: the cache stays parquet (no Stage-1 change needed). We decode
// it in-Worker with hyparquet (pure JS, Workers-compatible — already used by
// sync-realtime-data-features/features/parquet.ts). parquetReadObjects yields
// plain row objects whose keys are the parquet column names = the model
// feature_names, which projectFeatureRow consumes positionally.

import { parquetReadObjects } from "hyparquet";

import { computeLateBindingColumns, type LateBindingCategory } from "./late-binding";
import type { FeatureEntry } from "./feature-projection";
import type { JraRaceEntry } from "./jra-scorer";

const FEAT_CACHE_PREFIX = "feat-cache";
const CACHE_FILE_NAME = "features.parquet";
const RACE_ID_FIELD = "race_id";
const KETTO_FIELD = "ketto_toroku_bango";
const UMABAN_FIELD = "umaban";
const RUNNER_COUNT_FIELD = "shusso_tosu";
const WEIGHT_AVG_5_FIELD = "weight_avg_5";
const ODDS_SCORE_FIELD = "odds_score";
const POPULARITY_SCORE_FIELD = "popularity_score";
const TANSHO_ODDS_FIELD = "tansho_odds";
const TANSHO_NINKIJUN_FIELD = "tansho_ninkijun";
const WEIGHT_DIFF_FIELD = "weight_diff_from_avg";

// feat-cache/{category}/{runDate}/features.parquet — mirrors serve.py.
export const buildFeatCacheKey = (category: string, runDate: string): string =>
  `${FEAT_CACHE_PREFIX}/${category}/${runDate}/${CACHE_FILE_NAME}`;

export interface CachedRaceRows {
  raceId: string;
  rows: FeatureEntry[];
}

const asUint8ArrayBuffer = (bytes: Uint8Array): ArrayBuffer =>
  bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;

// Decode the whole-day cache parquet into plain row objects (column name -> cell).
export const decodeCacheParquet = async (bytes: Uint8Array): Promise<FeatureEntry[]> => {
  const rows = await parquetReadObjects({ file: asUint8ArrayBuffer(bytes) });
  return rows as FeatureEntry[];
};

// Group decoded rows by race_id, preserving row order within each race (which is
// the parquet's entry order — the same order pipeline_runner.groupby yields).
export const groupRowsByRace = (rows: ReadonlyArray<FeatureEntry>): CachedRaceRows[] => {
  const byRace = new Map<string, FeatureEntry[]>();
  rows.forEach((row) => {
    const raceId = String(row[RACE_ID_FIELD]);
    const existing = byRace.get(raceId);
    if (existing === undefined) {
      byRace.set(raceId, [row]);
      return;
    }
    existing.push(row);
  });
  return [...byRace.entries()].map(([raceId, raceRows]) => ({ raceId, rows: raceRows }));
};

const numberOrNull = (value: unknown): number | null => {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return Number.isNaN(value) ? null : value;
  if (typeof value === "bigint") return Number(value);
  if (typeof value !== "string") return null;
  const text = value.trim();
  if (text === "") return null;
  const parsed = Number(text);
  return Number.isNaN(parsed) ? null : parsed;
};

export interface RefreshLateBindingInput {
  row: FeatureEntry;
  category: LateBindingCategory;
  // Latest odds/weight for THIS horse, fetched from /api/odds + /api/horse-weight.
  // null fields fall back to the cached row values (no realtime update yet).
  tanshoOdds: number | null;
  tanshoNinkijun: number | null;
  currentBataiju: number | null;
}

// Overwrite the 5 late-binding columns on a cached row with values recomputed
// from the freshest odds + bataiju, keeping every early-binding column intact.
// weight_avg_5 is read from the cache (history-derived); runner_count from the
// cached shusso_tosu. Returns a new row object (does not mutate the input).
export const refreshLateBindingColumns = (input: RefreshLateBindingInput): FeatureEntry => {
  const cachedOdds = numberOrNull(input.row[TANSHO_ODDS_FIELD]);
  const cachedNinkijun = numberOrNull(input.row[TANSHO_NINKIJUN_FIELD]);
  const late = computeLateBindingColumns({
    category: input.category,
    odds: {
      runnerCount: numberOrNull(input.row[RUNNER_COUNT_FIELD]),
      tanshoNinkijun: input.tanshoNinkijun ?? cachedNinkijun,
      tanshoOdds: input.tanshoOdds ?? cachedOdds,
    },
    weight: {
      currentBataiju: input.currentBataiju,
      weightAvg5: numberOrNull(input.row[WEIGHT_AVG_5_FIELD]),
    },
  });
  return {
    ...input.row,
    [ODDS_SCORE_FIELD]: late.oddsScore,
    [POPULARITY_SCORE_FIELD]: late.popularityScore,
    [TANSHO_NINKIJUN_FIELD]: late.tanshoNinkijun,
    [TANSHO_ODDS_FIELD]: late.tanshoOdds,
    [WEIGHT_DIFF_FIELD]: late.weightDiffFromAvg,
  };
};

// Convert a cache row into a scorer entry (features + identity). umaban / ketto
// are read with the same coercion the projection uses (string ketto, int umaban).
export const toJraRaceEntry = (row: FeatureEntry): JraRaceEntry => ({
  features: row,
  kettoTorokuBango: String(row[KETTO_FIELD]),
  umaban: numberOrNull(row[UMABAN_FIELD]) ?? 0,
});
