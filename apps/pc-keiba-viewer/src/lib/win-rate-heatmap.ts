// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)
import { cleanText } from "./format";
import {
  getHorseWeightClass,
  parseHorseWeightKg,
  resolveCurrentHorseWeightKg,
} from "./horse-weight-class";
import type {
  BloodlineStatsRow,
  FrameStatsRow,
  HorseRaceResult,
  Runner,
  SimilarRaceStatsRow,
  StatsDetail,
} from "./race-types";
import { getRunnerDisplayNames } from "./runner-display";
import { formatRunnerNumber, isOverseasKeibajoCode } from "./runner-format";

export type WinRateHeatmapMetricKey =
  | "damSire"
  | "frame"
  | "horse"
  | "jockey"
  | "sire"
  | "sireSire"
  | "trainer"
  | "weight";

export type WinRateHeatmapRateKey = "quinellaRate" | "showRate" | "winRate";

export interface WinRateHeatmapCell {
  name: string | null;
  quinellaCount: number | null;
  quinellaRate: number | null;
  showCount: number | null;
  showRate: number | null;
  starts: number | null;
  winCount: number | null;
  winRate: number | null;
}

export interface WinRateHeatmapColumn {
  key: WinRateHeatmapMetricKey;
  label: string;
}

export interface WinRateHeatmapRateMetric {
  countKey: "quinellaCount" | "showCount" | "winCount";
  hue: number;
  key: WinRateHeatmapRateKey;
  label: string;
  shortLabel: string;
}

export interface WinRateHeatmapRow {
  cells: Record<WinRateHeatmapMetricKey, WinRateHeatmapCell>;
  frameNumber: string;
  horseName: string;
  horseNumber: string;
}

export interface BuildWinRateHeatmapRowsInput {
  bloodlineRows: BloodlineStatsRow[];
  frameStats: FrameStatsRow[];
  horseResults: HorseRaceResult[];
  keibajoCode: string;
  liveWeightKgByHorse: Map<string, number>;
  runners: Runner[];
  similarRows: SimilarRaceStatsRow[];
}

export interface ShouldShowWinRateHeatmapWeightColumnInput {
  keibajoCode: string;
  liveWeightKgByHorse: Map<string, number>;
  runners: Runner[];
}

interface WeightClassRateCounts {
  quinellaCount: number;
  showCount: number;
  starts: number;
  winCount: number;
}

export type WinRateHeatmapViewMode = "all" | WinRateHeatmapRateKey;

export interface WinRateHeatmapViewModeOption {
  key: WinRateHeatmapViewMode;
  label: string;
}

interface BuildRateCellInput {
  name: string;
  quinellaCount: number;
  showCount: number;
  starts: number;
  winCount: number;
}

export const WIN_RATE_HEATMAP_COLUMNS: readonly WinRateHeatmapColumn[] = [
  { key: "frame", label: "枠" },
  { key: "weight", label: "馬体重" },
  { key: "horse", label: "馬" },
  { key: "jockey", label: "騎手" },
  { key: "trainer", label: "調教師" },
  { key: "sire", label: "父" },
  { key: "damSire", label: "母父" },
  { key: "sireSire", label: "父父" },
];

export const WIN_RATE_HEATMAP_RATE_METRICS: readonly WinRateHeatmapRateMetric[] = [
  { countKey: "winCount", hue: 8, key: "winRate", label: "勝率", shortLabel: "勝" },
  { countKey: "quinellaCount", hue: 36, key: "quinellaRate", label: "連対率", shortLabel: "連" },
  { countKey: "showCount", hue: 196, key: "showRate", label: "複勝率", shortLabel: "複" },
];

export const WIN_RATE_HEATMAP_VIEW_MODES: readonly WinRateHeatmapViewModeOption[] = [
  { key: "winRate", label: "勝率" },
  { key: "quinellaRate", label: "連対率" },
  { key: "showRate", label: "複勝率" },
  { key: "all", label: "勝率+連対率+複勝率" },
];

export const DEFAULT_WIN_RATE_HEATMAP_VIEW_MODE: WinRateHeatmapViewMode = "winRate";

const WIN_RATE_HEATMAP_VIEW_MODE_METRICS: Record<
  WinRateHeatmapViewMode,
  readonly WinRateHeatmapRateMetric[]
> = {
  all: WIN_RATE_HEATMAP_RATE_METRICS,
  quinellaRate: WIN_RATE_HEATMAP_RATE_METRICS.filter((metric) => metric.key === "quinellaRate"),
  showRate: WIN_RATE_HEATMAP_RATE_METRICS.filter((metric) => metric.key === "showRate"),
  winRate: WIN_RATE_HEATMAP_RATE_METRICS.filter((metric) => metric.key === "winRate"),
};

export const EMPTY_WIN_RATE_HEATMAP_CELL: WinRateHeatmapCell = {
  name: null,
  quinellaCount: null,
  quinellaRate: null,
  showCount: null,
  showRate: null,
  starts: null,
  winCount: null,
  winRate: null,
};

const WIN_RATE_HEATMAP_SATURATION = 72;
const WIN_RATE_HEATMAP_MAX_LIGHTNESS = 94;
const WIN_RATE_HEATMAP_MIN_LIGHTNESS = 42;
const WIN_RATE_HEATMAP_LIGHTNESS_RANGE =
  WIN_RATE_HEATMAP_MAX_LIGHTNESS - WIN_RATE_HEATMAP_MIN_LIGHTNESS;
const EMPTY_CELL_BACKGROUND = "hsl(0, 0%, 96%)";
const MAX_WIN_RATE = 100;
const RATE_DECIMAL_FACTOR = 10;
const EMPTY_RATE_COLUMN_SPAN = 1;

const splitHorseNumbers = (value: string): string[] =>
  cleanText(value, "")
    .split(",")
    .map((horseNumber) => formatRunnerNumber(cleanText(horseNumber, "")))
    .filter((horseNumber) => horseNumber !== "-");

const parseFinishPosition = (value: string | null): number | null => {
  const cleaned = cleanText(value, "");
  if (cleaned === "" || /^0+$/u.test(cleaned)) {
    return null;
  }
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
};

const toRate = (count: number, starts: number): number =>
  Math.round((count * MAX_WIN_RATE * RATE_DECIMAL_FACTOR) / starts) / RATE_DECIMAL_FACTOR;

const toHeatmapNumber = (value: number | null | undefined): number | null =>
  typeof value === "number" && Number.isFinite(value) ? value : null;

const ranksFromDetails = (details: StatsDetail[]): number[] =>
  details
    .map((detail) => parseFinishPosition(detail.rank))
    .filter((rank): rank is number => rank !== null);

const buildRateCell = (input: BuildRateCellInput): WinRateHeatmapCell => ({
  name: input.name,
  quinellaCount: input.quinellaCount,
  quinellaRate: toRate(input.quinellaCount, input.starts),
  showCount: input.showCount,
  showRate: toRate(input.showCount, input.starts),
  starts: input.starts,
  winCount: input.winCount,
  winRate: toRate(input.winCount, input.starts),
});

const toHeatmapCell = (
  row: BloodlineStatsRow | SimilarRaceStatsRow | undefined,
): WinRateHeatmapCell => {
  if (row === undefined) {
    return EMPTY_WIN_RATE_HEATMAP_CELL;
  }
  return {
    name: row.name,
    quinellaCount: toHeatmapNumber(row.quinellaCount),
    quinellaRate: toHeatmapNumber(row.quinellaRate),
    showCount: toHeatmapNumber(row.showCount),
    showRate: toHeatmapNumber(row.showRate),
    starts: toHeatmapNumber(row.starts),
    winCount: toHeatmapNumber(row.winCount),
    winRate: toHeatmapNumber(row.winRate),
  };
};

const toFrameHeatmapCell = (
  row: FrameStatsRow | undefined,
  frameNumber: string,
): WinRateHeatmapCell => {
  if (row === undefined) {
    return EMPTY_WIN_RATE_HEATMAP_CELL;
  }
  const name = `枠${frameNumber}`;
  const starts = toHeatmapNumber(row.count);
  const winCount = toHeatmapNumber(row.winCount);
  const quinellaCount = toHeatmapNumber(row.quinellaCount);
  const showCount = toHeatmapNumber(row.showCount);
  const winRate = toHeatmapNumber(row.winRate);
  const quinellaRate = toHeatmapNumber(row.quinellaRate);
  const showRate = toHeatmapNumber(row.showRate);
  if (
    starts !== null &&
    starts > 0 &&
    winCount !== null &&
    quinellaCount !== null &&
    showCount !== null
  ) {
    return {
      name,
      quinellaCount,
      quinellaRate: quinellaRate ?? toRate(quinellaCount, starts),
      showCount,
      showRate: showRate ?? toRate(showCount, starts),
      starts,
      winCount,
      winRate: winRate ?? toRate(winCount, starts),
    };
  }
  const ranks = ranksFromDetails(row.details);
  if (ranks.length === 0) {
    return { ...EMPTY_WIN_RATE_HEATMAP_CELL, name };
  }
  return buildRateCell({
    name,
    quinellaCount: ranks.filter((rank) => rank <= 2).length,
    showCount: ranks.filter((rank) => rank <= 3).length,
    starts: ranks.length,
    winCount: ranks.filter((rank) => rank === 1).length,
  });
};

const buildHorseRateCell = (horseName: string, results: HorseRaceResult[]): WinRateHeatmapCell => {
  const ranks = results
    .map((result) => parseFinishPosition(result.kakuteiChakujun))
    .filter((rank): rank is number => rank !== null);
  if (ranks.length === 0) {
    return { ...EMPTY_WIN_RATE_HEATMAP_CELL, name: horseName };
  }
  return buildRateCell({
    name: horseName,
    quinellaCount: ranks.filter((rank) => rank <= 2).length,
    showCount: ranks.filter((rank) => rank <= 3).length,
    starts: ranks.length,
    winCount: ranks.filter((rank) => rank === 1).length,
  });
};

const indexRowsByHorse = <Row extends { category: string; currentHorseNumbers: string }>(
  rows: Row[],
): Map<string, Map<string, Row>> =>
  rows.reduce((index, row) => {
    splitHorseNumbers(row.currentHorseNumbers).forEach((horseNumber) => {
      const current = index.get(horseNumber) ?? new Map<string, Row>();
      current.set(row.category, row);
      index.set(horseNumber, current);
    });
    return index;
  }, new Map<string, Map<string, Row>>());

const indexHorseResultsByNumber = (
  horseResults: HorseRaceResult[],
): Map<string, HorseRaceResult[]> =>
  horseResults.reduce((index, result) => {
    const horseNumber = formatRunnerNumber(result.currentUmaban);
    if (horseNumber === "-") {
      return index;
    }
    const current = index.get(horseNumber) ?? [];
    index.set(horseNumber, [...current, result]);
    return index;
  }, new Map<string, HorseRaceResult[]>());

const indexFrameStatsByNumber = (rows: FrameStatsRow[]): Map<string, FrameStatsRow> =>
  rows.reduce((index, row) => {
    const frameNumber = formatRunnerNumber(row.frameNumber);
    if (frameNumber === "-") {
      return index;
    }
    index.set(frameNumber, row);
    return index;
  }, new Map<string, FrameStatsRow>());

const addFinishToWeightClassCounts = (
  counts: WeightClassRateCounts,
  rank: number,
): WeightClassRateCounts => ({
  quinellaCount: counts.quinellaCount + (rank <= 2 ? 1 : 0),
  showCount: counts.showCount + (rank <= 3 ? 1 : 0),
  starts: counts.starts + 1,
  winCount: counts.winCount + (rank === 1 ? 1 : 0),
});

const indexWeightClassRates = (
  horseResults: HorseRaceResult[],
): Map<string, WeightClassRateCounts> =>
  horseResults.reduce((index, result) => {
    const kg = parseHorseWeightKg({
      bataiju: result.bataiju,
      keibajoCode: result.keibajoCode,
    });
    const rank = parseFinishPosition(result.kakuteiChakujun);
    if (kg === null || rank === null) {
      return index;
    }
    const weightClass = getHorseWeightClass(kg);
    const current = index.get(weightClass.key) ?? {
      quinellaCount: 0,
      showCount: 0,
      starts: 0,
      winCount: 0,
    };
    return new Map(index).set(weightClass.key, addFinishToWeightClassCounts(current, rank));
  }, new Map<string, WeightClassRateCounts>());

const toWeightHeatmapCell = (
  kg: number | null,
  ratesByClass: Map<string, WeightClassRateCounts>,
): WinRateHeatmapCell => {
  if (kg === null) {
    return EMPTY_WIN_RATE_HEATMAP_CELL;
  }
  const weightClass = getHorseWeightClass(kg);
  const counts = ratesByClass.get(weightClass.key);
  if (counts === undefined || counts.starts === 0) {
    return { ...EMPTY_WIN_RATE_HEATMAP_CELL, name: weightClass.label };
  }
  return buildRateCell({
    name: weightClass.label,
    quinellaCount: counts.quinellaCount,
    showCount: counts.showCount,
    starts: counts.starts,
    winCount: counts.winCount,
  });
};

const compareHeatmapRows = (left: WinRateHeatmapRow, right: WinRateHeatmapRow): number => {
  const leftNumber = Number(left.horseNumber);
  const rightNumber = Number(right.horseNumber);
  if (leftNumber !== rightNumber) {
    return leftNumber - rightNumber;
  }
  return left.horseNumber.localeCompare(right.horseNumber);
};

export const buildWinRateHeatmapRows = (
  input: BuildWinRateHeatmapRowsInput,
): WinRateHeatmapRow[] => {
  const similarByHorse = indexRowsByHorse(input.similarRows);
  const bloodlineByHorse = indexRowsByHorse(input.bloodlineRows);
  const horseResultsByNumber = indexHorseResultsByNumber(input.horseResults);
  const frameStatsByNumber = indexFrameStatsByNumber(input.frameStats);
  const weightClassRates = indexWeightClassRates(input.horseResults);
  return input.runners
    .map((runner) => {
      const horseNumber = formatRunnerNumber(runner.umaban);
      const frameNumber = formatRunnerNumber(runner.wakuban);
      const horseName = getRunnerDisplayNames(runner).horse || "-";
      const similar = similarByHorse.get(horseNumber);
      const bloodline = bloodlineByHorse.get(horseNumber);
      const currentWeightKg = resolveCurrentHorseWeightKg({
        bataiju: runner.bataiju,
        horseNumber,
        keibajoCode: input.keibajoCode,
        liveWeightKgByHorse: input.liveWeightKgByHorse,
      });
      return {
        cells: {
          damSire: toHeatmapCell(bloodline?.get("damSire")),
          frame: toFrameHeatmapCell(frameStatsByNumber.get(frameNumber), frameNumber),
          horse: buildHorseRateCell(horseName, horseResultsByNumber.get(horseNumber) ?? []),
          jockey: toHeatmapCell(similar?.get("jockey")),
          sire: toHeatmapCell(bloodline?.get("sire")),
          sireSire: toHeatmapCell(bloodline?.get("sireSire")),
          trainer: toHeatmapCell(similar?.get("trainer")),
          weight: toWeightHeatmapCell(currentWeightKg, weightClassRates),
        },
        frameNumber,
        horseName,
        horseNumber,
      };
    })
    .filter((row) => row.horseNumber !== "-")
    .toSorted(compareHeatmapRows);
};

export const shouldShowWinRateHeatmapWeightColumn = (
  input: ShouldShowWinRateHeatmapWeightColumnInput,
): boolean => {
  if (isOverseasKeibajoCode(input.keibajoCode)) {
    return false;
  }
  return input.runners.some(
    (runner) =>
      resolveCurrentHorseWeightKg({
        bataiju: runner.bataiju,
        horseNumber: formatRunnerNumber(runner.umaban),
        keibajoCode: input.keibajoCode,
        liveWeightKgByHorse: input.liveWeightKgByHorse,
      }) !== null,
  );
};

export const getVisibleWinRateHeatmapColumns = (
  showWeight: boolean,
): readonly WinRateHeatmapColumn[] =>
  showWeight
    ? WIN_RATE_HEATMAP_COLUMNS
    : WIN_RATE_HEATMAP_COLUMNS.filter((column) => column.key !== "weight");

export const winRateHeatmapBackground = (rate: number | null | undefined, hue: number): string => {
  const numericRate = toHeatmapNumber(rate);
  if (numericRate === null) {
    return EMPTY_CELL_BACKGROUND;
  }
  const ratio = Math.min(1, Math.max(0, numericRate / MAX_WIN_RATE));
  const lightness = WIN_RATE_HEATMAP_MAX_LIGHTNESS - WIN_RATE_HEATMAP_LIGHTNESS_RANGE * ratio;
  return `hsl(${hue}, ${WIN_RATE_HEATMAP_SATURATION}%, ${lightness}%)`;
};

export const formatWinRateHeatmapValue = (rate: number | null | undefined): string => {
  const numericRate = toHeatmapNumber(rate);
  return numericRate === null ? "-" : `${numericRate.toFixed(1)}%`;
};

export const getVisibleWinRateHeatmapRateMetrics = (
  mode: WinRateHeatmapViewMode,
): readonly WinRateHeatmapRateMetric[] => WIN_RATE_HEATMAP_VIEW_MODE_METRICS[mode];

export const getWinRateHeatmapTooltipName = (cell: WinRateHeatmapCell): string =>
  cell.name === null ? "-" : cell.name;

export const winRateHeatmapEntityColSpan = (visibleRateCount: number): number =>
  visibleRateCount === 0 ? EMPTY_RATE_COLUMN_SPAN : visibleRateCount;
