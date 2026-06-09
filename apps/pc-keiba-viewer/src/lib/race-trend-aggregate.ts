// Run with bun (browser-safe pure helpers). Extracted from trends route.ts so
// the client can aggregate raw starter rows in a useMemo on checkbox change.
import type {
  RaceTrendRunningStyleCache,
  RaceTrendStarterRow,
} from "horse-racing-realtime/race-trend-daily-track-types";

import type { RaceSource } from "./codes";
import { isSameJockeyName, normalizeJockeyNameForComparison } from "./jockey-name";
import type {
  RaceTrendCurrentRunningStyle,
  RaceTrendDetail,
  RaceTrendRunningStyle,
  RaceTrendRunningStyleRow,
} from "./race-types";
import { buildRaceKey } from "./running-style-cache";

export interface RaceTrendTargets {
  frame: boolean;
  jockey: boolean;
  trainer: boolean;
  raceNumber: boolean;
  runningStyle: boolean;
}

export interface RaceTrendAggregateRaceContext {
  keibajoCode: string;
  raceBango: string;
  source: RaceSource;
}

export interface RaceTrendAggregateOptions {
  endYmd: string;
  ignoreFrame: boolean;
  ignoreJockey: boolean;
  ignoreTrainer: boolean;
  ignoreRaceNumber: boolean;
  ignoreRunningStyle: boolean;
  jockeySameVenue: boolean;
  keibajoCode: string;
  startYmd: string;
}

export interface RaceTrendTodaySiblingTarget {
  keibajoCode: string;
  raceBango: string;
  source: RaceSource;
  targetYmd: string;
}

interface RaceTrendRunningStyleTarget {
  frameNumber: string | null;
  horseNumber: string | null;
  jockeyKey: string | null;
  jockeyName: string | null;
  trainerKey: string | null;
  trainerName: string | null;
  raceNumber: string | null;
  runningStyle: RaceTrendRunningStyle | null;
}

const RACE_BANGO_FALLBACK_LOCALE = "ja";
const RACE_BANGO_LOCALE_OPTIONS = {
  numeric: true,
} satisfies Intl.CollatorOptions;

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
  parseStoredInteger(value, "00");

export const parseStoredWinOdds = (value: string | null | undefined): number | null => {
  const odds = parseStoredInteger(value, "0000");
  return odds === null ? null : odds / 10;
};

export const parseCornerPosition = (value: string | null | undefined): number | null =>
  parseStoredInteger(value, "00");

const JOCKEY_DEMURO_ALIASES = ["デムーロ", "Ｍ．デム", "M.デム"];

export const normalizeRaceTrendJockeyName = (value: string | null | undefined): string | null => {
  const normalized = normalizeJockeyNameForComparison(value);
  return normalized === "" ? null : normalized;
};

export const getJockeyNameAliases = (value: string): string[] => {
  if (normalizeRaceTrendJockeyName(value) !== "デムーロ") return [value];
  return [value, ...JOCKEY_DEMURO_ALIASES];
};

// Map a recorded corner position to a running-style label. Returns null when
// the data is insufficient for a confident call so the trend display falls
// through to the prediction cache instead of leaking a heuristic guess.
//
// Without a runnerCount we can only safely identify nige (corner position 1):
// the recorded corner number alone cannot distinguish a mid-pack horse in a
// 6-horse field from a tail-end horse in a 12-horse field. Returning a
// hard-coded label here used to leak "senkou" for every corner-2..4 row whose
// runnerCount the upstream DO had not yet populated, which surfaced as the
// race-trend section showing every past-race entry as senkou.
export const runningStyleFromCorners = ({
  corner1,
  corner2,
  corner3,
  corner4,
  runnerCount,
}: {
  corner1: string | null | undefined;
  corner2: string | null | undefined;
  corner3: string | null | undefined;
  corner4: string | null | undefined;
  runnerCount: string | null | undefined;
}): RaceTrendRunningStyle | null => {
  const corner =
    parseCornerPosition(corner1) ??
    parseCornerPosition(corner2) ??
    parseCornerPosition(corner3) ??
    parseCornerPosition(corner4);
  if (corner === null) return null;
  if (corner <= 1) return "nige";
  const parsedRunnerCount = parseStoredInteger(runnerCount, "00");
  if (parsedRunnerCount === null || parsedRunnerCount <= 1) return null;
  const ratio = (corner - 1) / Math.max(parsedRunnerCount - 1, 1);
  if (ratio <= 0.35) return "senkou";
  if (ratio <= 0.7) return "sashi";
  return "oikomi";
};

const toYmd = (year: string, monthDay: string): string => `${year}${monthDay}`;

const toIsoDate = (ymd: string): string =>
  `${ymd.slice(0, 4)}-${ymd.slice(4, 6)}-${ymd.slice(6, 8)}`;

const isYmdInRange = (ymd: string, startYmd: string, endYmd: string): boolean =>
  ymd >= startYmd && ymd <= endYmd;

export const starterKey = (
  row: Pick<
    RaceTrendStarterRow,
    "source" | "kaisaiNen" | "kaisaiTsukihi" | "keibajoCode" | "raceBango" | "umaban"
  >,
): string =>
  [
    row.source,
    row.kaisaiNen,
    row.kaisaiTsukihi,
    row.keibajoCode,
    row.raceBango,
    row.umaban ?? "",
  ].join(":");

export const starterRaceKey = (
  row: Pick<
    RaceTrendStarterRow,
    "source" | "kaisaiNen" | "kaisaiTsukihi" | "keibajoCode" | "raceBango"
  >,
): string =>
  buildRaceKey({
    kaisaiNen: row.kaisaiNen,
    kaisaiTsukihi: row.kaisaiTsukihi,
    keibajoCode: row.keibajoCode,
    raceBango: row.raceBango,
    source: row.source,
  });

export const starterRunningStyleKey = (
  row: Pick<
    RaceTrendStarterRow,
    "source" | "kaisaiNen" | "kaisaiTsukihi" | "keibajoCode" | "raceBango" | "umaban"
  >,
): string => `${starterRaceKey(row)}:${normalizeNumberText(row.umaban) ?? ""}`;

// Canonical race-number comparator shared by trend aggregation paths. Numeric
// values get an integer comparison so "10" sorts AFTER "9" instead of the
// lexical "10" < "9" trap a plain `<` on the stored text would hit. When
// either side is non-numeric (rare alphabetic codes) the function falls back
// to locale-aware comparison so callers always get a stable ordering.
export const compareRaceBango = (left: string, right: string): number => {
  const leftNumber = Number(left);
  const rightNumber = Number(right);
  if (Number.isFinite(leftNumber) && Number.isFinite(rightNumber)) {
    return leftNumber - rightNumber;
  }
  return left.localeCompare(right, RACE_BANGO_FALLBACK_LOCALE, RACE_BANGO_LOCALE_OPTIONS);
};

// Narrow a today-cache batch to the sibling rows of one target race. Today
// cache returns every completed starter row for the day across all venues so
// multiple races can share one upstream D1 round trip — this helper trims it
// down to the current race's siblings (same source, same date, same venue,
// strictly smaller race number). Race rows missing required dimensions are
// dropped instead of silently passing because they would otherwise corrupt
// the merge step downstream.
export const filterTodaySiblingRows = (
  rows: ReadonlyArray<RaceTrendStarterRow>,
  target: RaceTrendTodaySiblingTarget,
): RaceTrendStarterRow[] =>
  rows.filter((row) => {
    if (row.source !== target.source) return false;
    if (`${row.kaisaiNen}${row.kaisaiTsukihi}` !== target.targetYmd) return false;
    if (row.keibajoCode !== target.keibajoCode) return false;
    if (!row.raceBango) return false;
    return compareRaceBango(row.raceBango, target.raceBango) < 0;
  });

// Merge raw starter rows from multiple sources into a single deduplicated
// list keyed by (source, ymd, keibajoCode, raceBango, umaban). Later sources
// override earlier sources on non-empty fields via `mergeStarterRowPair` so
// today's snapshot can fill in fields the past-14 aggregate left blank
// without clobbering data the historical row already had. Partial rows are
// pass-through preserved so DO snapshots that have not yet captured a
// finishPosition still surface in the trend section.
export const mergeStarterRows = (
  ...rowGroups: ReadonlyArray<ReadonlyArray<RaceTrendStarterRow>>
): RaceTrendStarterRow[] => {
  const merged = new Map<string, RaceTrendStarterRow>();
  const flatRows = rowGroups.flat();
  for (const row of flatRows) {
    const key = starterKey(row);
    const existing = merged.get(key);
    merged.set(key, existing ? mergeStarterRowPair(existing, row) : row);
  }
  return Array.from(merged.values());
};

const pickNonEmptyValue = <T>(...values: ReadonlyArray<T | null | undefined>): T | null => {
  for (const value of values) {
    if (value !== null && value !== undefined && value !== "") return value;
  }
  return null;
};

const pickFinishPosition = (a: number, b: number): number => (a > 0 ? a : b);

// Per-pair merge primitive. `b` is the newer source (today snapshot,
// realtime), so non-empty fields from `b` win, falling back to `a`. The
// finishPosition rule treats `> 0` as confirmed and otherwise lets the newer
// row's value through, so a snapshot's completed finishPosition still gets
// merged into an earlier partial row from the same key.
export const mergeStarterRowPair = (
  a: RaceTrendStarterRow,
  b: RaceTrendStarterRow,
): RaceTrendStarterRow => ({
  ...a,
  raceName: pickNonEmptyValue(b.raceName, a.raceName),
  hassoJikoku: pickNonEmptyValue(b.hassoJikoku, a.hassoJikoku),
  runnerCount: pickNonEmptyValue(b.runnerCount, a.runnerCount),
  wakuban: pickNonEmptyValue(b.wakuban, a.wakuban),
  bamei: pickNonEmptyValue(b.bamei, a.bamei),
  jockeyName: pickNonEmptyValue(b.jockeyName, a.jockeyName),
  chokyoshiName: pickNonEmptyValue(b.chokyoshiName, a.chokyoshiName),
  tanshoOdds: pickNonEmptyValue(b.tanshoOdds, a.tanshoOdds),
  tanshoPopularity: pickNonEmptyValue(b.tanshoPopularity, a.tanshoPopularity),
  finishPosition: pickFinishPosition(b.finishPosition, a.finishPosition),
  sohaTime: pickNonEmptyValue(b.sohaTime, a.sohaTime),
  corner1: pickNonEmptyValue(b.corner1, a.corner1),
  corner2: pickNonEmptyValue(b.corner2, a.corner2),
  corner3: pickNonEmptyValue(b.corner3, a.corner3),
  corner4: pickNonEmptyValue(b.corner4, a.corner4),
  bataiju: pickNonEmptyValue(b.bataiju, a.bataiju),
  zogenFugo: pickNonEmptyValue(b.zogenFugo, a.zogenFugo),
  zogenSa: pickNonEmptyValue(b.zogenSa, a.zogenSa),
});

// Resolve a jockey display name to its normalized comparison key. Extracted so
// that callers (e.g. race-trend-score.ts) can build identical lookup keys
// without rebuilding a target-table resolver.
export const resolveRowJockeyKey = (jockeyName: string | null | undefined): string | null => {
  if (!jockeyName) return null;
  return normalizeRaceTrendJockeyName(jockeyName);
};

// Trainer comparison key. Whitespace-trimmed name suffices since the trainer
// snapshot column already holds a normalized 略称. Returns null when missing
// so the score / aggregate paths treat trainer as unknown.
export const resolveRowTrainerKey = (trainerName: string | null | undefined): string | null => {
  if (!trainerName) return null;
  const trimmed = trainerName.trim();
  return trimmed === "" ? null : trimmed;
};

const parseHorseWeightDelta = (zogenFugo: string | null, zogenSa: string | null): number | null => {
  const magnitude = parseStoredInteger(zogenSa, "000");
  if (magnitude === null) return zogenSa === "0" ? 0 : null;
  const sign = zogenFugo === "-" ? -1 : 1;
  return sign * magnitude;
};

const parseHorseWeight = (bataiju: string | null): number | null =>
  parseStoredInteger(bataiju, "000");

// Pull the optional trainer name off a starter row. The optional field plus
// the row's `extends Record<string, unknown>` index signature widens the
// inferred type to `{} | unknown`, so this guard narrows back to string | null
// without an `as` cast (forbidden by the TypeScript rules).
const pickStarterChokyoshiName = (row: RaceTrendStarterRow): string | null => {
  const value = row.chokyoshiName;
  return typeof value === "string" ? value : null;
};

export const detailFromStarter = (row: RaceTrendStarterRow): RaceTrendDetail => ({
  source: row.source,
  date: toIsoDate(toYmd(row.kaisaiNen, row.kaisaiTsukihi)),
  keibajoCode: row.keibajoCode,
  raceNumber: row.raceBango,
  raceName: row.raceName,
  runningStyle: runningStyleFromCorners(row),
  frameNumber: row.wakuban,
  horseNumber: row.umaban,
  horseName: row.bamei,
  jockeyName: row.jockeyName,
  trainerName: pickStarterChokyoshiName(row),
  popularity: parseStoredPopularity(row.tanshoPopularity),
  winOdds: parseStoredWinOdds(row.tanshoOdds),
  finishPosition: row.finishPosition,
  time: row.sohaTime,
  horseWeight: parseHorseWeight(row.bataiju),
  horseWeightDelta: parseHorseWeightDelta(row.zogenFugo, row.zogenSa),
});

const calculateMedian = (values: number[]): number | null => {
  if (values.length === 0) return null;
  const sorted = values.toSorted((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[middle] ?? null;
  const left = sorted[middle - 1];
  const right = sorted[middle];
  return left === undefined || right === undefined ? null : (left + right) / 2;
};

interface RunningStyleTargetKeyValue {
  frameNumber: string | null;
  jockeyKey: string | null;
  trainerKey: string | null;
  raceNumber: string | null;
  runningStyle: RaceTrendRunningStyle | null;
}

interface RunningStyleTargetKeyOptions {
  ignoreFrame: boolean;
  ignoreJockey: boolean;
  ignoreTrainer: boolean;
  ignoreRaceNumber: boolean;
  ignoreRunningStyle: boolean;
}

const runningStyleTargetKey = (
  value: RunningStyleTargetKeyValue,
  options: RunningStyleTargetKeyOptions,
): string | null => {
  if (!options.ignoreFrame && !value.frameNumber) return null;
  if (!options.ignoreJockey && !value.jockeyKey) return null;
  if (!options.ignoreTrainer && !value.trainerKey) return null;
  if (!options.ignoreRaceNumber && !value.raceNumber) return null;
  return [
    options.ignoreRunningStyle || !value.runningStyle ? "*" : value.runningStyle,
    options.ignoreFrame ? "*" : value.frameNumber,
    options.ignoreJockey ? "*" : value.jockeyKey,
    options.ignoreTrainer ? "*" : value.trainerKey,
    options.ignoreRaceNumber ? "*" : value.raceNumber,
  ].join(":");
};

const average = (values: number[]): number | null =>
  values.length === 0 ? null : values.reduce((sum, value) => sum + value, 0) / values.length;

export const compareTrendDetails = (a: RaceTrendDetail, b: RaceTrendDetail): number => {
  const dateOrder = b.date.localeCompare(a.date);
  if (dateOrder !== 0) return dateOrder;
  const raceOrder = b.raceNumber.localeCompare(
    a.raceNumber,
    RACE_BANGO_FALLBACK_LOCALE,
    RACE_BANGO_LOCALE_OPTIONS,
  );
  if (raceOrder !== 0) return raceOrder;
  return (a.horseNumber ?? "").localeCompare(
    b.horseNumber ?? "",
    RACE_BANGO_FALLBACK_LOCALE,
    RACE_BANGO_LOCALE_OPTIONS,
  );
};

const sortTrendDetails = (details: RaceTrendDetail[]): RaceTrendDetail[] =>
  details.toSorted(compareTrendDetails);

export const compareAggregatedRows = (
  a: RaceTrendRunningStyleRow,
  b: RaceTrendRunningStyleRow,
): number => {
  const showOrder = b.showRate - a.showRate;
  if (showOrder !== 0) return showOrder;
  const quinellaOrder = b.quinellaRate - a.quinellaRate;
  if (quinellaOrder !== 0) return quinellaOrder;
  const winOrder = b.winRate - a.winRate;
  if (winOrder !== 0) return winOrder;
  const startsOrder = b.starts - a.starts;
  if (startsOrder !== 0) return startsOrder;
  const horseOrder = (a.targetHorseNumbers[0] ?? "").localeCompare(
    b.targetHorseNumbers[0] ?? "",
    RACE_BANGO_FALLBACK_LOCALE,
    RACE_BANGO_LOCALE_OPTIONS,
  );
  if (horseOrder !== 0) return horseOrder;
  const frameOrder = (a.frameNumber ?? "").localeCompare(
    b.frameNumber ?? "",
    RACE_BANGO_FALLBACK_LOCALE,
    RACE_BANGO_LOCALE_OPTIONS,
  );
  if (frameOrder !== 0) return frameOrder;
  const jockeyOrder = (a.jockeyName ?? "").localeCompare(
    b.jockeyName ?? "",
    RACE_BANGO_FALLBACK_LOCALE,
  );
  if (jockeyOrder !== 0) return jockeyOrder;
  return (a.raceNumber ?? "").localeCompare(
    b.raceNumber ?? "",
    RACE_BANGO_FALLBACK_LOCALE,
    RACE_BANGO_LOCALE_OPTIONS,
  );
};

const sortAggregatedRows = (rows: RaceTrendRunningStyleRow[]): RaceTrendRunningStyleRow[] =>
  rows.toSorted(compareAggregatedRows);

const compareTargetEntries = (
  a: { target: RaceTrendRunningStyleTarget },
  b: { target: RaceTrendRunningStyleTarget },
): number =>
  (a.target.horseNumber ?? "").localeCompare(
    b.target.horseNumber ?? "",
    RACE_BANGO_FALLBACK_LOCALE,
    RACE_BANGO_LOCALE_OPTIONS,
  );

const buildRowJockeyKeyResolver = (
  targets: ReadonlyArray<RaceTrendRunningStyleTarget>,
): ((jockeyName: string | null | undefined) => string | null) => {
  const matchableTargets = targets.filter(
    (target): target is RaceTrendRunningStyleTarget & { jockeyKey: string; jockeyName: string } =>
      target.jockeyKey !== null && target.jockeyName !== null,
  );
  return (jockeyName) => {
    if (!jockeyName) return null;
    for (const target of matchableTargets) {
      if (isSameJockeyName(jockeyName, target.jockeyName)) {
        return target.jockeyKey;
      }
    }
    return resolveRowJockeyKey(jockeyName);
  };
};

const aggregateRunningStyleRows = (
  rows: RaceTrendStarterRow[],
  runningStyleByStarterKey: Map<string, RaceTrendRunningStyle>,
  targets: RaceTrendRunningStyleTarget[],
  options: RaceTrendAggregateOptions,
): RaceTrendRunningStyleRow[] => {
  const targetEntries = targets
    .map((target, index) => ({
      index,
      key: runningStyleTargetKey(target, options),
      target,
    }))
    .filter((entry): entry is { index: number; key: string; target: RaceTrendRunningStyleTarget } =>
      Boolean(entry.key),
    )
    .toSorted(compareTargetEntries);
  const resolveTargetJockeyKey = buildRowJockeyKeyResolver(targets);
  const groupedRowsByTargetKey = new Map<string, RaceTrendStarterRow[]>();
  for (const row of rows) {
    const ymd = toYmd(row.kaisaiNen, row.kaisaiTsukihi);
    if (!isYmdInRange(ymd, options.startYmd, options.endYmd)) continue;
    if (options.jockeySameVenue && row.keibajoCode !== options.keibajoCode) continue;
    const key = runningStyleTargetKey(
      {
        frameNumber: normalizeNumberText(row.wakuban),
        jockeyKey: resolveTargetJockeyKey(row.jockeyName),
        trainerKey: resolveRowTrainerKey(pickStarterChokyoshiName(row)),
        raceNumber: normalizeNumberText(row.raceBango),
        runningStyle:
          runningStyleByStarterKey.get(starterRunningStyleKey(row)) ?? runningStyleFromCorners(row),
      },
      options,
    );
    if (!key) continue;
    const groupedRows = groupedRowsByTargetKey.get(key);
    if (groupedRows) {
      groupedRows.push(row);
    } else {
      groupedRowsByTargetKey.set(key, [row]);
    }
  }

  const aggregated = targetEntries.map(({ index, key, target }) => {
    const groupRows = groupedRowsByTargetKey.get(key) ?? [];
    // finishPosition === 0 is the sentinel for "no result yet" (entry-only
    // row, eg. today sibling whose top-3 didn't include this horse). It
    // must NOT count toward quinella / show / win — otherwise the
    // `<= 2` / `<= 3` filters would silently lift the rate by every
    // unranked starter in the group. `starts` still counts these rows so
    // frame participation reflects the actual field size.
    const rankedRows = groupRows.filter((row) => row.finishPosition >= 1);
    const finishPositions = rankedRows.map((row) => row.finishPosition);
    const winCount = rankedRows.filter((row) => row.finishPosition === 1).length;
    const quinellaCount = rankedRows.filter((row) => row.finishPosition <= 2).length;
    const showCount = rankedRows.filter((row) => row.finishPosition <= 3).length;
    const popularities = groupRows
      .map((row) => parseStoredPopularity(row.tanshoPopularity))
      .filter((value): value is number => value !== null);
    const winOdds = groupRows
      .map((row) => parseStoredWinOdds(row.tanshoOdds))
      .filter((value): value is number => value !== null);
    const details = sortTrendDetails(
      groupRows.map((row) => ({
        ...detailFromStarter(row),
        runningStyle:
          runningStyleByStarterKey.get(starterRunningStyleKey(row)) ?? runningStyleFromCorners(row),
      })),
    );
    return {
      key: `${key}:${target.horseNumber ?? index}`,
      targetHorseNumbers: target.horseNumber ? [target.horseNumber] : [],
      runningStyle: target.runningStyle,
      frameNumber: options.ignoreFrame ? null : target.frameNumber,
      jockeyName: options.ignoreJockey ? null : target.jockeyName,
      // trainerName is rendered in the column regardless of the trainer
      // grouping target. The win-rate aggregation above already honors
      // ignoreTrainer when computing rates, so showing the runner's
      // trainer name here gives the user context without affecting math.
      trainerName: target.trainerName,
      raceNumber: options.ignoreRaceNumber ? null : target.raceNumber,
      starts: groupRows.length,
      showRate: groupRows.length > 0 ? (showCount / groupRows.length) * 100 : 0,
      quinellaRate: groupRows.length > 0 ? (quinellaCount / groupRows.length) * 100 : 0,
      winRate: groupRows.length > 0 ? (winCount / groupRows.length) * 100 : 0,
      finishPositionAverage: average(finishPositions),
      popularityMedian: calculateMedian(popularities),
      winOddsMedian: calculateMedian(winOdds),
      finishPositionMedian: calculateMedian(finishPositions),
      details,
    };
  });
  return sortAggregatedRows(aggregated);
};

export const countDistinctRunningStyleDetailRaces = (rows: RaceTrendRunningStyleRow[]): number =>
  new Set(
    rows.flatMap((row) =>
      row.details.map((detail) =>
        [detail.source, detail.date, detail.keibajoCode, detail.raceNumber].join(":"),
      ),
    ),
  ).size;

export interface RaceTrendAggregateRunnerInput {
  frameNumber: string | null;
  horseNumber: string | null;
  jockeyName: string | null;
  // Optional: when missing, the trainer column renders "-" and trainer
  // filtering treats the row as unfilterable (skips it under the trainer
  // grouping target).
  trainerName?: string | null;
}

export interface RaceTrendAggregateInput {
  currentRunningStyles: RaceTrendCurrentRunningStyle[];
  historicalRunningStyles: RaceTrendRunningStyleCache[];
  raceContext: RaceTrendAggregateRaceContext;
  runners: ReadonlyArray<RaceTrendAggregateRunnerInput>;
  starterRows: RaceTrendStarterRow[];
}

const buildRunningStyleTargets = (
  input: RaceTrendAggregateInput,
): RaceTrendRunningStyleTarget[] => {
  const currentByHorse = new Map(
    input.currentRunningStyles.map((row) => [row.horseNumber, row.predictedLabel]),
  );
  return input.runners.map((runner) => {
    const horseNumber = normalizeNumberText(runner.horseNumber);
    return {
      frameNumber: normalizeNumberText(runner.frameNumber),
      horseNumber,
      jockeyKey: normalizeRaceTrendJockeyName(runner.jockeyName),
      jockeyName: normalizeText(runner.jockeyName),
      trainerKey: resolveRowTrainerKey(runner.trainerName),
      trainerName: normalizeText(runner.trainerName),
      raceNumber: normalizeNumberText(input.raceContext.raceBango),
      runningStyle: horseNumber ? (currentByHorse.get(horseNumber) ?? null) : null,
    };
  });
};

const buildRunningStyleByStarterKey = (
  input: RaceTrendAggregateInput,
): Map<string, RaceTrendRunningStyle> =>
  new Map(
    input.historicalRunningStyles.map((row) => [
      `${row.raceKey}:${normalizeNumberText(row.horseNumber) ?? ""}`,
      row.predictedLabel,
    ]),
  );

export const aggregateForTargets = (
  input: RaceTrendAggregateInput,
  trendTargets: RaceTrendTargets,
  jockeySameVenue: boolean,
  startYmd: string,
  endYmd: string,
): { raceCount: number; runningStyleRows: RaceTrendRunningStyleRow[] } => {
  const targets = buildRunningStyleTargets(input);
  const runningStyleByStarterKey = buildRunningStyleByStarterKey(input);
  const aggregateOptions: RaceTrendAggregateOptions = {
    startYmd,
    endYmd,
    ignoreFrame: !trendTargets.frame,
    ignoreJockey: !trendTargets.jockey,
    ignoreTrainer: !trendTargets.trainer,
    ignoreRaceNumber: !trendTargets.raceNumber,
    ignoreRunningStyle: !trendTargets.runningStyle,
    jockeySameVenue,
    keibajoCode: input.raceContext.keibajoCode,
  };
  const runningStyleRows = aggregateRunningStyleRows(
    input.starterRows,
    runningStyleByStarterKey,
    targets,
    aggregateOptions,
  );
  return {
    raceCount: countDistinctRunningStyleDetailRaces(runningStyleRows),
    runningStyleRows,
  };
};
