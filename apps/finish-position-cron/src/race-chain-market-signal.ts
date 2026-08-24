// Run with bun. Worker-native, fail-closed port of the focused-race branch in
// add-market-signal-features.py. This module only transforms an already
// validated per-race day-base foundation; it never publishes to the canonical
// final feature-cache namespace. Callers must retain the Container fallback
// whenever this function returns status=fallback.

import { computeLateBindingColumns } from "./scoring/late-binding";
import type { RealtimeOdds } from "./scoring/rescore-realtime";

const RACE_ID_FIELD: string = "race_id";
const HORSE_NUMBER_FIELD: string = "umaban";
const ODDS_FIELD: string = "tansho_odds";
const POPULARITY_FIELD: string = "tansho_ninkijun";
const ODDS_SCORE_FIELD: string = "odds_score";
const POPULARITY_SCORE_FIELD: string = "popularity_score";
const CAREER_WIN_RATE_FIELD: string = "career_win_rate";
const RAW_ODDS_FIELD: string = "tansho_odds_raw";
const RAW_POPULARITY_FIELD: string = "tansho_ninkijun_raw";
const IMPLIED_PROBABILITY_FIELD: string = "inverse_odds_implied_prob";
const MARKET_SHARE_FIELD: string = "inverse_odds_market_share";
const ODDS_RANK_FIELD: string = "inverse_odds_rank_in_race";
const POPULARITY_RANK_FIELD: string = "popularity_rank_in_race";
const ODDS_SCORE_DIFF_FIELD: string = "odds_score_diff_from_race_avg";
const POPULARITY_SCORE_DIFF_FIELD: string = "popularity_score_diff_from_race_avg";
const DISAGREEMENT_FIELD: string = "popularity_odds_disagreement";
const FORM_MARKET_EDGE_FIELD: string = "form_market_edge";
const MIN_HORSE_NUMBER: number = 1;
const MAX_HORSE_NUMBER: number = 32;

export type MarketSignalCell = boolean | number | string | null;
export type MarketSignalFoundationRow = Readonly<Record<string, MarketSignalCell>>;
export type MarketSignalOutputRow = Record<string, MarketSignalCell>;

export type MarketSignalFallbackReason =
  | "duplicate-horse-number"
  | "empty-foundation"
  | "invalid-cached-input"
  | "invalid-horse-number"
  | "invalid-live-odds"
  | "partial-live-odds"
  | "race-contract-mismatch"
  | "runner-limit";

export interface MaterializeMarketSignalInput {
  liveOddsByHorseNumber: ReadonlyMap<number, RealtimeOdds>;
  raceId: string;
  rows: ReadonlyArray<MarketSignalFoundationRow>;
}

interface MarketSignalReadyResult {
  rows: MarketSignalOutputRow[];
  status: "ready";
}

interface MarketSignalFallbackResult {
  reason: MarketSignalFallbackReason;
  status: "fallback";
}

export type MarketSignalMaterializeResult = MarketSignalFallbackResult | MarketSignalReadyResult;

interface NullableNumberResult {
  valid: boolean;
  value: number | null;
}

interface PreparedMarketRow {
  careerWinRate: number | null;
  horseNumber: number;
  odds: number | null;
  oddsScore: number | null;
  popularity: number | null;
  popularityScore: number | null;
  row: MarketSignalFoundationRow;
}

interface PreparedMarketRowsReadyResult {
  rows: PreparedMarketRow[];
  status: "ready";
}

interface PreparedMarketRowsFallbackResult {
  reason: MarketSignalFallbackReason;
  status: "fallback";
}

type PreparedMarketRowsResult = PreparedMarketRowsFallbackResult | PreparedMarketRowsReadyResult;

interface HorseRow {
  horseNumber: number | null;
  row: MarketSignalFoundationRow;
}

interface ValidHorseRow {
  horseNumber: number;
  row: MarketSignalFoundationRow;
}

const finiteNumber = (value: MarketSignalCell | undefined): number | null => {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value !== "string" || value.trim() === "") return null;
  const parsed: number = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const nullableNumber = (value: MarketSignalCell | undefined): NullableNumberResult => {
  if (value === null || value === undefined) return { valid: true, value: null };
  const parsed: number | null = finiteNumber(value);
  return parsed === null ? { valid: false, value: null } : { valid: true, value: parsed };
};

const positiveHorseNumber = (value: MarketSignalCell | undefined): number | null => {
  const parsed: number | null = finiteNumber(value);
  if (parsed === null || !Number.isInteger(parsed)) return null;
  return parsed >= MIN_HORSE_NUMBER && parsed <= MAX_HORSE_NUMBER ? parsed : null;
};

const validRaceRows = (raceId: string, rows: ReadonlyArray<MarketSignalFoundationRow>): boolean =>
  raceId.length > 0 &&
  rows.every((row) => typeof row[RACE_ID_FIELD] === "string" && row[RACE_ID_FIELD] === raceId);

const validLiveOdds = (odds: RealtimeOdds): boolean =>
  Number.isFinite(odds.tanshoOdds) &&
  odds.tanshoOdds > 0 &&
  Number.isSafeInteger(odds.tanshoNinkijun) &&
  odds.tanshoNinkijun > 0;

const average = (values: ReadonlyArray<number | null>): number | null => {
  const finite: number[] = values.filter((value): value is number => value !== null);
  return finite.length === 0
    ? null
    : finite.reduce((total, value) => total + value, 0) / finite.length;
};

const differenceFromAverage = (value: number | null, mean: number | null): number | null =>
  value === null || mean === null ? null : value - mean;

const competitionRankDescending = (
  values: ReadonlyArray<number | null>,
  value: number | null,
): number =>
  value === null
    ? values.filter((candidate) => candidate !== null).length + 1
    : values.filter((candidate) => candidate !== null && candidate > value).length + 1;

const competitionRankAscending = (
  values: ReadonlyArray<number | null>,
  value: number | null,
): number =>
  value === null
    ? values.filter((candidate) => candidate !== null).length + 1
    : values.filter((candidate) => candidate !== null && candidate < value).length + 1;

const prepareCachedRow = (
  row: MarketSignalFoundationRow,
  horseNumber: number,
): PreparedMarketRowsResult => {
  const odds: NullableNumberResult = nullableNumber(row[ODDS_FIELD]);
  const popularity: NullableNumberResult = nullableNumber(row[POPULARITY_FIELD]);
  const oddsScore: NullableNumberResult = nullableNumber(row[ODDS_SCORE_FIELD]);
  const popularityScore: NullableNumberResult = nullableNumber(row[POPULARITY_SCORE_FIELD]);
  const careerWinRate: NullableNumberResult = nullableNumber(row[CAREER_WIN_RATE_FIELD]);
  if (
    !odds.valid ||
    !popularity.valid ||
    !oddsScore.valid ||
    !popularityScore.valid ||
    !careerWinRate.valid
  ) {
    return { reason: "invalid-cached-input", status: "fallback" };
  }
  return {
    status: "ready",
    rows: [
      {
        careerWinRate: careerWinRate.value,
        horseNumber,
        odds: odds.value,
        oddsScore: oddsScore.value,
        popularity: odds.value === null ? null : popularity.value,
        popularityScore: popularityScore.value,
        row,
      },
    ],
  };
};

const prepareLiveRow = (
  row: MarketSignalFoundationRow,
  horseNumber: number,
  odds: RealtimeOdds,
  runnerCount: number,
): PreparedMarketRowsResult => {
  const careerWinRate: NullableNumberResult = nullableNumber(row[CAREER_WIN_RATE_FIELD]);
  if (!careerWinRate.valid || !validLiveOdds(odds)) {
    return { reason: "invalid-live-odds", status: "fallback" };
  }
  const late = computeLateBindingColumns({
    category: "jra",
    odds: {
      runnerCount,
      tanshoNinkijun: odds.tanshoNinkijun,
      tanshoOdds: odds.tanshoOdds,
    },
    weight: { currentBataiju: null, weightAvg5: null },
  });
  return {
    status: "ready",
    rows: [
      {
        careerWinRate: careerWinRate.value,
        horseNumber,
        odds: odds.tanshoOdds,
        oddsScore: late.oddsScore,
        popularity: odds.tanshoNinkijun,
        popularityScore: late.popularityScore,
        row,
      },
    ],
  };
};

const prepareRow = (
  row: MarketSignalFoundationRow,
  horseNumber: number,
  liveOddsByHorseNumber: ReadonlyMap<number, RealtimeOdds>,
  runnerCount: number,
): PreparedMarketRowsResult => {
  if (liveOddsByHorseNumber.size === 0) return prepareCachedRow(row, horseNumber);
  const liveOdds: RealtimeOdds | undefined = liveOddsByHorseNumber.get(horseNumber);
  return liveOdds === undefined
    ? { reason: "partial-live-odds", status: "fallback" }
    : prepareLiveRow(row, horseNumber, liveOdds, runnerCount);
};

const prepareRows = (
  rows: ReadonlyArray<MarketSignalFoundationRow>,
  liveOddsByHorseNumber: ReadonlyMap<number, RealtimeOdds>,
): PreparedMarketRowsResult => {
  const horseRows: HorseRow[] = rows.map((row) => ({
    horseNumber: positiveHorseNumber(row[HORSE_NUMBER_FIELD]),
    row,
  }));
  if (horseRows.some(({ horseNumber }) => horseNumber === null)) {
    return { reason: "invalid-horse-number", status: "fallback" };
  }
  const validHorseRows: ValidHorseRow[] = horseRows.filter(
    (entry): entry is ValidHorseRow => entry.horseNumber !== null,
  );
  const horseNumbers: number[] = validHorseRows.map(({ horseNumber }) => horseNumber);
  if (new Set(horseNumbers).size !== horseNumbers.length) {
    return { reason: "duplicate-horse-number", status: "fallback" };
  }
  if (liveOddsByHorseNumber.size !== 0 && liveOddsByHorseNumber.size !== horseNumbers.length) {
    return { reason: "partial-live-odds", status: "fallback" };
  }
  const prepared: PreparedMarketRowsResult[] = validHorseRows.map(({ horseNumber, row }) =>
    prepareRow(row, horseNumber, liveOddsByHorseNumber, horseNumbers.length),
  );
  const failed: PreparedMarketRowsResult | undefined = prepared.find(
    (result) => result.status === "fallback",
  );
  if (failed?.status === "fallback") return failed;
  const ready = prepared.filter(
    (result): result is PreparedMarketRowsReadyResult => result.status === "ready",
  );
  return { rows: ready.flatMap(({ rows: readyRows }) => readyRows), status: "ready" };
};

const appendMarketSignals = (
  prepared: ReadonlyArray<PreparedMarketRow>,
): MarketSignalOutputRow[] => {
  const impliedProbabilities: Array<number | null> = prepared.map(({ odds }) =>
    odds !== null && odds > 0 ? 1 / odds : null,
  );
  const totalImpliedProbability: number = impliedProbabilities
    .map((value) => value ?? 0)
    .reduce((total, value) => total + value, 0);
  const popularities: Array<number | null> = prepared.map(({ popularity }) => popularity);
  const oddsScoreAverage: number | null = average(prepared.map(({ oddsScore }) => oddsScore));
  const popularityScoreAverage: number | null = average(
    prepared.map(({ popularityScore }) => popularityScore),
  );
  return prepared.map((entry, index) => {
    const impliedProbability: number | null = impliedProbabilities[index] ?? null;
    const marketShare: number | null =
      impliedProbability === null || totalImpliedProbability === 0
        ? null
        : impliedProbability / totalImpliedProbability;
    return {
      ...entry.row,
      [CAREER_WIN_RATE_FIELD]: entry.careerWinRate,
      [DISAGREEMENT_FIELD]:
        entry.popularityScore === null || entry.oddsScore === null
          ? null
          : Math.abs(entry.popularityScore - entry.oddsScore),
      [FORM_MARKET_EDGE_FIELD]:
        entry.careerWinRate === null || impliedProbability === null
          ? null
          : entry.careerWinRate - impliedProbability,
      [IMPLIED_PROBABILITY_FIELD]: impliedProbability,
      [MARKET_SHARE_FIELD]: marketShare,
      [ODDS_FIELD]: entry.odds,
      [ODDS_RANK_FIELD]: competitionRankDescending(impliedProbabilities, impliedProbability),
      [ODDS_SCORE_DIFF_FIELD]: differenceFromAverage(entry.oddsScore, oddsScoreAverage),
      [ODDS_SCORE_FIELD]: entry.oddsScore,
      [POPULARITY_FIELD]: entry.popularity,
      [POPULARITY_RANK_FIELD]: competitionRankAscending(popularities, entry.popularity),
      [POPULARITY_SCORE_DIFF_FIELD]: differenceFromAverage(
        entry.popularityScore,
        popularityScoreAverage,
      ),
      [POPULARITY_SCORE_FIELD]: entry.popularityScore,
      [RAW_ODDS_FIELD]: entry.odds,
      [RAW_POPULARITY_FIELD]: entry.popularity,
    };
  });
};

export const materializeRaceMarketSignals = (
  input: MaterializeMarketSignalInput,
): MarketSignalMaterializeResult => {
  if (input.rows.length === 0) return { reason: "empty-foundation", status: "fallback" };
  if (input.rows.length > MAX_HORSE_NUMBER) {
    return { reason: "runner-limit", status: "fallback" };
  }
  if (!validRaceRows(input.raceId, input.rows)) {
    return { reason: "race-contract-mismatch", status: "fallback" };
  }
  const prepared: PreparedMarketRowsResult = prepareRows(input.rows, input.liveOddsByHorseNumber);
  if (prepared.status === "fallback") return prepared;
  return { rows: appendMarketSignals(prepared.rows), status: "ready" };
};
