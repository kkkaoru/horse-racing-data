// Run with bun (browser-safe pure helpers). Extracted from trends route.ts so
// the client can aggregate raw starter rows in a useMemo on checkbox change.
import type { RaceSource } from "./codes";
import { normalizeJockeyNameForComparison } from "./jockey-name";
import { buildRaceKey } from "./running-style-cache";
import type {
  RaceTrendCurrentRunningStyle,
  RaceTrendDetail,
  RaceTrendRunningStyle,
  RaceTrendRunningStyleCache,
  RaceTrendRunningStyleRow,
  RaceTrendStarterRow,
} from "./race-types";

export interface RaceTrendTargets {
  frame: boolean;
  jockey: boolean;
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
  ignoreRaceNumber: boolean;
  ignoreRunningStyle: boolean;
  jockeySameVenue: boolean;
  keibajoCode: string;
  startYmd: string;
}

interface RaceTrendRunningStyleTarget {
  frameNumber: string | null;
  horseNumber: string | null;
  jockeyKey: string | null;
  jockeyName: string | null;
  raceNumber: string | null;
  runningStyle: RaceTrendRunningStyle | null;
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
  parseStoredInteger(value, "00");

export const parseStoredWinOdds = (value: string | null | undefined): number | null => {
  const odds = parseStoredInteger(value, "0000");
  return odds === null ? null : odds / 10;
};

export const parseCornerPosition = (value: string | null | undefined): number | null =>
  parseStoredInteger(value, "00");

const JOCKEY_DEMURO_ALIASES = ["デムーロ", "Ｍ．デム", "M.デム"];

export const normalizeRaceTrendJockeyName = (
  value: string | null | undefined,
): string | null => {
  const normalized = normalizeJockeyNameForComparison(value);
  return normalized === "" ? null : normalized;
};

export const getJockeyNameAliases = (value: string): string[] => {
  if (normalizeRaceTrendJockeyName(value) !== "デムーロ") return [value];
  return [value, ...JOCKEY_DEMURO_ALIASES];
};

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
  if (parsedRunnerCount === null || parsedRunnerCount <= 1) {
    if (corner <= 4) return "senkou";
    if (corner <= 8) return "sashi";
    return "oikomi";
  }
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

const starterRunningStyleKey = (
  row: Pick<
    RaceTrendStarterRow,
    "source" | "kaisaiNen" | "kaisaiTsukihi" | "keibajoCode" | "raceBango" | "umaban"
  >,
): string => `${starterRaceKey(row)}:${normalizeNumberText(row.umaban) ?? ""}`;

const parseHorseWeightDelta = (
  zogenFugo: string | null,
  zogenSa: string | null,
): number | null => {
  const magnitude = parseStoredInteger(zogenSa, "000");
  if (magnitude === null) return zogenSa === "0" ? 0 : null;
  const sign = zogenFugo === "-" ? -1 : 1;
  return sign * magnitude;
};

const parseHorseWeight = (bataiju: string | null): number | null =>
  parseStoredInteger(bataiju, "000");

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

const runningStyleTargetKey = (
  value: {
    frameNumber: string | null;
    jockeyKey: string | null;
    raceNumber: string | null;
    runningStyle: RaceTrendRunningStyle | null;
  },
  options: {
    ignoreFrame: boolean;
    ignoreJockey: boolean;
    ignoreRaceNumber: boolean;
    ignoreRunningStyle: boolean;
  },
): string | null => {
  if (!options.ignoreFrame && !value.frameNumber) return null;
  if (!options.ignoreJockey && !value.jockeyKey) return null;
  if (!options.ignoreRaceNumber && !value.raceNumber) return null;
  return [
    options.ignoreRunningStyle || !value.runningStyle ? "*" : value.runningStyle,
    options.ignoreFrame ? "*" : value.frameNumber,
    options.ignoreJockey ? "*" : value.jockeyKey,
    options.ignoreRaceNumber ? "*" : value.raceNumber,
  ].join(":");
};

const average = (values: number[]): number | null =>
  values.length === 0 ? null : values.reduce((sum, value) => sum + value, 0) / values.length;

const sortTrendDetails = (details: RaceTrendDetail[]): RaceTrendDetail[] =>
  details.toSorted((a, b) => {
    const dateOrder = b.date.localeCompare(a.date);
    if (dateOrder !== 0) return dateOrder;
    const raceOrder = b.raceNumber.localeCompare(a.raceNumber, "ja", { numeric: true });
    if (raceOrder !== 0) return raceOrder;
    return (a.horseNumber ?? "").localeCompare(b.horseNumber ?? "", "ja", { numeric: true });
  });

const sortAggregatedRows = (rows: RaceTrendRunningStyleRow[]): RaceTrendRunningStyleRow[] =>
  rows.toSorted(
    (a, b) =>
      b.showRate - a.showRate ||
      b.quinellaRate - a.quinellaRate ||
      b.winRate - a.winRate ||
      b.starts - a.starts ||
      (a.targetHorseNumbers[0] ?? "").localeCompare(b.targetHorseNumbers[0] ?? "", "ja", {
        numeric: true,
      }) ||
      (a.frameNumber ?? "").localeCompare(b.frameNumber ?? "", "ja", { numeric: true }) ||
      (a.jockeyName ?? "").localeCompare(b.jockeyName ?? "", "ja") ||
      (a.raceNumber ?? "").localeCompare(b.raceNumber ?? "", "ja", { numeric: true }),
  );

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
    .toSorted((a, b) =>
      (a.target.horseNumber ?? "").localeCompare(b.target.horseNumber ?? "", "ja", {
        numeric: true,
      }),
    );
  const groupedRowsByTargetKey = new Map<string, RaceTrendStarterRow[]>();
  for (const row of rows) {
    const ymd = toYmd(row.kaisaiNen, row.kaisaiTsukihi);
    if (!isYmdInRange(ymd, options.startYmd, options.endYmd)) continue;
    if (options.jockeySameVenue && row.keibajoCode !== options.keibajoCode) continue;
    const key = runningStyleTargetKey(
      {
        frameNumber: normalizeNumberText(row.wakuban),
        jockeyKey: normalizeRaceTrendJockeyName(row.jockeyName),
        raceNumber: normalizeNumberText(row.raceBango),
        runningStyle: runningStyleByStarterKey.get(starterRunningStyleKey(row)) ?? null,
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
    const finishPositions = groupRows.map((row) => row.finishPosition);
    const winCount = groupRows.filter((row) => row.finishPosition === 1).length;
    const quinellaCount = groupRows.filter((row) => row.finishPosition <= 2).length;
    const showCount = groupRows.filter((row) => row.finishPosition <= 3).length;
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
          runningStyleByStarterKey.get(starterRunningStyleKey(row)) ??
          runningStyleFromCorners(row),
      })),
    );
    return {
      key: `${key}:${target.horseNumber ?? index}`,
      targetHorseNumbers: target.horseNumber ? [target.horseNumber] : [],
      runningStyle: target.runningStyle,
      frameNumber: options.ignoreFrame ? null : target.frameNumber,
      jockeyName: options.ignoreJockey ? null : target.jockeyName,
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

export const countDistinctRunningStyleDetailRaces = (
  rows: RaceTrendRunningStyleRow[],
): number =>
  new Set(
    rows.flatMap((row) =>
      row.details.map((detail) =>
        [detail.source, detail.date, detail.keibajoCode, detail.raceNumber].join(":"),
      ),
    ),
  ).size;

export interface RaceTrendAggregateInput {
  currentRunningStyles: RaceTrendCurrentRunningStyle[];
  historicalRunningStyles: RaceTrendRunningStyleCache[];
  raceContext: RaceTrendAggregateRaceContext;
  runners: ReadonlyArray<{
    frameNumber: string | null;
    horseNumber: string | null;
    jockeyName: string | null;
  }>;
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
