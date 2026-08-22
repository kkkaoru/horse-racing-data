import { getCarriedWeightClass, parseCarriedWeightKg } from "./carried-weight-class";
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
  WeightClassStatsRow,
} from "./race-types";
import { getRunnerDisplayNames } from "./runner-display";
import { formatRunnerNumber, isBanEiKeibajoCode, isOverseasKeibajoCode } from "./runner-format";

export type WinRateHeatmapMetricKey =
  | "carriedWeight"
  | "damDamSire"
  | "damSire"
  | "damSireSire"
  | "frame"
  | "horse"
  | "jockey"
  | "jockeyFrame"
  | "sire"
  | "sireDamSire"
  | "sireSire"
  | "sireSireSire"
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
  carriedWeightClassStats?: readonly WeightClassStatsRow[];
  weightClassStats?: readonly WeightClassStatsRow[];
}

export interface ShouldShowWinRateHeatmapWeightColumnInput {
  keibajoCode: string;
  liveWeightKgByHorse: Map<string, number>;
  runners: Runner[];
}

export interface ShouldShowWinRateHeatmapCarriedWeightColumnInput {
  keibajoCode: string;
  runners: Runner[];
}

export interface GetVisibleWinRateHeatmapColumnsInput {
  keibajoCode: string;
  showCarriedWeight: boolean;
  showWeight: boolean;
}

export interface WinRateHeatmapColorScale {
  maxRate: number;
  minRate: number;
  ticks: readonly number[];
}

export interface WinRateHeatmapColorScales {
  quinellaRate: WinRateHeatmapColorScale;
  showRate: WinRateHeatmapColorScale;
  winRate: WinRateHeatmapColorScale;
}

export interface GetWinRateHeatmapColorScaleInput {
  keibajoCode: string;
  metricKey: WinRateHeatmapRateKey;
}

export interface ResolveWinRateHeatmapColorScaleInput {
  keibajoCode: string;
  metricKey: WinRateHeatmapRateKey;
  rates: readonly number[];
}

export interface WinRateHeatmapFillInput {
  hue: number;
  maxRate: number;
  minRate: number;
  rate: number | null | undefined;
}

export interface WinRateHeatmapColorScaleGradientInput {
  hue: number;
  maxRate: number;
  minRate: number;
  ticks: readonly number[];
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

interface HslColor {
  hue: number;
  lightness: number;
  saturation: number;
}

interface RgbColor {
  blue: number;
  green: number;
  red: number;
}

export const WIN_RATE_HEATMAP_COLUMNS: readonly WinRateHeatmapColumn[] = [
  { key: "frame", label: "枠" },
  { key: "weight", label: "馬体重" },
  { key: "carriedWeight", label: "斤量" },
  { key: "horse", label: "馬" },
  { key: "jockeyFrame", label: "騎手枠別" },
  { key: "jockey", label: "騎手" },
  { key: "trainer", label: "調教師" },
  { key: "sire", label: "父" },
  { key: "damSire", label: "母父" },
  { key: "sireSire", label: "父父" },
  { key: "sireDamSire", label: "父母父" },
  { key: "sireSireSire", label: "父父父" },
  { key: "damSireSire", label: "母父父" },
  { key: "damDamSire", label: "母母父" },
];

const WIN_RATE_HEATMAP_WIN_RATE_METRIC: WinRateHeatmapRateMetric = {
  countKey: "winCount",
  hue: 8,
  key: "winRate",
  label: "勝率",
  shortLabel: "勝",
};
const WIN_RATE_HEATMAP_QUINELLA_RATE_METRIC: WinRateHeatmapRateMetric = {
  countKey: "quinellaCount",
  hue: 36,
  key: "quinellaRate",
  label: "連対率",
  shortLabel: "連",
};
const WIN_RATE_HEATMAP_SHOW_RATE_METRIC: WinRateHeatmapRateMetric = {
  countKey: "showCount",
  hue: 196,
  key: "showRate",
  label: "複勝率",
  shortLabel: "複",
};

export const WIN_RATE_HEATMAP_COMBINED_HUE: number = 272;

export const WIN_RATE_HEATMAP_RATE_METRICS: readonly WinRateHeatmapRateMetric[] = [
  WIN_RATE_HEATMAP_WIN_RATE_METRIC,
  WIN_RATE_HEATMAP_QUINELLA_RATE_METRIC,
  WIN_RATE_HEATMAP_SHOW_RATE_METRIC,
];

export const WIN_RATE_HEATMAP_VIEW_MODES: readonly WinRateHeatmapViewModeOption[] = [
  { key: "winRate", label: "勝率" },
  { key: "quinellaRate", label: "連対率" },
  { key: "showRate", label: "複勝率" },
  { key: "all", label: "勝率+連対率+複勝率" },
];

export const DEFAULT_WIN_RATE_HEATMAP_VIEW_MODE: WinRateHeatmapViewMode = "winRate";
export const DEFAULT_WIN_RATE_HEATMAP_SHOW_STARTS: boolean = false;

const WIN_RATE_HEATMAP_VIEW_MODE_METRICS: Record<
  WinRateHeatmapViewMode,
  readonly WinRateHeatmapRateMetric[]
> = {
  all: WIN_RATE_HEATMAP_RATE_METRICS.map((metric) => ({
    countKey: metric.countKey,
    hue: WIN_RATE_HEATMAP_COMBINED_HUE,
    key: metric.key,
    label: metric.label,
    shortLabel: metric.shortLabel,
  })),
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

export const WIN_RATE_HEATMAP_COLOR_SCALE_MAX_RATE: number = 40;
export const WIN_RATE_HEATMAP_COLOR_SCALE_TICKS: readonly number[] = [0, 10, 20, 30, 40];
// Ban-ei 8-10 horse fields cluster tightly (win ~8-16%, show ~25-40%).
// Live cells stretch from the table's min rate to max rate; these caps are
// fallbacks when a metric has no numeric cells.
export const BAN_EI_WIN_RATE_HEATMAP_WIN_MAX_RATE: number = 20;
export const BAN_EI_WIN_RATE_HEATMAP_QUINELLA_MAX_RATE: number = 40;
export const BAN_EI_WIN_RATE_HEATMAP_SHOW_MAX_RATE: number = 50;
export const BAN_EI_WIN_RATE_HEATMAP_WIN_TICKS: readonly number[] = [0, 5, 10, 15, 20];
export const BAN_EI_WIN_RATE_HEATMAP_QUINELLA_TICKS: readonly number[] = [0, 10, 20, 30, 40];
export const BAN_EI_WIN_RATE_HEATMAP_SHOW_TICKS: readonly number[] = [0, 12.5, 25, 37.5, 50];
const BAN_EI_WIN_RATE_HEATMAP_COLOR_SCALES: Record<
  WinRateHeatmapRateKey,
  WinRateHeatmapColorScale
> = {
  quinellaRate: {
    maxRate: BAN_EI_WIN_RATE_HEATMAP_QUINELLA_MAX_RATE,
    minRate: 0,
    ticks: BAN_EI_WIN_RATE_HEATMAP_QUINELLA_TICKS,
  },
  showRate: {
    maxRate: BAN_EI_WIN_RATE_HEATMAP_SHOW_MAX_RATE,
    minRate: 0,
    ticks: BAN_EI_WIN_RATE_HEATMAP_SHOW_TICKS,
  },
  winRate: {
    maxRate: BAN_EI_WIN_RATE_HEATMAP_WIN_MAX_RATE,
    minRate: 0,
    ticks: BAN_EI_WIN_RATE_HEATMAP_WIN_TICKS,
  },
};
const DEFAULT_WIN_RATE_HEATMAP_COLOR_SCALE: WinRateHeatmapColorScale = {
  maxRate: WIN_RATE_HEATMAP_COLOR_SCALE_MAX_RATE,
  minRate: 0,
  ticks: WIN_RATE_HEATMAP_COLOR_SCALE_TICKS,
};
const HEATMAP_COLOR_SCALE_TICK_INDEXES: readonly number[] = [0, 1, 2, 3, 4];
const HEATMAP_COLOR_SCALE_TICK_LAST_INDEX: number = 4;
const HEATMAP_RATE_TICK_DECIMALS: number = 10;
const BAN_EI_EQUAL_RATE_PAD: number = 1;
const BAN_EI_WIN_RATE_HEATMAP_SATURATION_MIN = 45;
const BAN_EI_WIN_RATE_HEATMAP_SATURATION_MAX = 100;
const BAN_EI_WIN_RATE_HEATMAP_SATURATION_RANGE =
  BAN_EI_WIN_RATE_HEATMAP_SATURATION_MAX - BAN_EI_WIN_RATE_HEATMAP_SATURATION_MIN;
const BAN_EI_WIN_RATE_HEATMAP_MAX_LIGHTNESS = 86;
const BAN_EI_WIN_RATE_HEATMAP_MIN_LIGHTNESS = 14;
const BAN_EI_WIN_RATE_HEATMAP_LIGHTNESS_RANGE =
  BAN_EI_WIN_RATE_HEATMAP_MAX_LIGHTNESS - BAN_EI_WIN_RATE_HEATMAP_MIN_LIGHTNESS;
// JV Ban-ei horse master (nvd_nu) only fills 父 (ketto_joho_01b) and 母父 (ketto_joho_05b).
const BAN_EI_HIDDEN_HEATMAP_COLUMN_KEYS: ReadonlySet<WinRateHeatmapMetricKey> = new Set([
  "damDamSire",
  "damSireSire",
  "sireDamSire",
  "sireSire",
  "sireSireSire",
]);
const WIN_RATE_HEATMAP_SATURATION_MIN = 22;
const WIN_RATE_HEATMAP_SATURATION_MAX = 95;
const WIN_RATE_HEATMAP_SATURATION_RANGE =
  WIN_RATE_HEATMAP_SATURATION_MAX - WIN_RATE_HEATMAP_SATURATION_MIN;
const WIN_RATE_HEATMAP_MAX_LIGHTNESS = 96;
const WIN_RATE_HEATMAP_MIN_LIGHTNESS = 28;
const WIN_RATE_HEATMAP_LIGHTNESS_RANGE =
  WIN_RATE_HEATMAP_MAX_LIGHTNESS - WIN_RATE_HEATMAP_MIN_LIGHTNESS;
const EMPTY_CELL_HSL: HslColor = { hue: 0, lightness: 96, saturation: 0 };
const HEATMAP_FOREGROUND_INK = "var(--ink)";
const HEATMAP_FOREGROUND_SURFACE = "var(--surface)";
const HEATMAP_INK_RGB: RgbColor = { blue: 28, green: 33, red: 23 };
const HEATMAP_SURFACE_RGB: RgbColor = { blue: 255, green: 255, red: 255 };
const RELATIVE_LUMINANCE_RED = 0.2126;
const RELATIVE_LUMINANCE_GREEN = 0.7152;
const RELATIVE_LUMINANCE_BLUE = 0.0722;
const SRGB_LINEAR_THRESHOLD = 0.04045;
const SRGB_LINEAR_DIVISOR = 12.92;
const SRGB_GAMMA_OFFSET = 0.055;
const SRGB_GAMMA_SCALE = 1.055;
const SRGB_GAMMA = 2.4;
const CONTRAST_OFFSET = 0.05;
const RGB_MAX = 255;
const HSL_SATURATION_SCALE = 100;
const HSL_HUE_CIRCLE = 360;
const HSL_HUE_SECTOR = 60;
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

const indexWeightClassStats = (
  rows: readonly WeightClassStatsRow[],
): Map<string, WeightClassRateCounts> =>
  rows.reduce((index, row) => {
    if (row.starts <= 0) {
      return index;
    }
    return new Map(index).set(row.key, {
      quinellaCount: row.quinellaCount,
      showCount: row.showCount,
      starts: row.starts,
      winCount: row.winCount,
    });
  }, new Map<string, WeightClassRateCounts>());

const resolveWeightClassRates = (
  horseResults: HorseRaceResult[],
  weightClassStats: readonly WeightClassStatsRow[] | undefined,
): Map<string, WeightClassRateCounts> => {
  if (weightClassStats !== undefined && weightClassStats.length > 0) {
    return indexWeightClassStats(weightClassStats);
  }
  return indexWeightClassRatesFromHorseResults(horseResults);
};

const indexWeightClassRatesFromHorseResults = (
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

const toClassHeatmapCell = (
  classInfo: { key: string; label: string } | null,
  ratesByClass: Map<string, WeightClassRateCounts>,
): WinRateHeatmapCell => {
  if (classInfo === null) {
    return EMPTY_WIN_RATE_HEATMAP_CELL;
  }
  const counts = ratesByClass.get(classInfo.key);
  if (counts === undefined || counts.starts === 0) {
    return { ...EMPTY_WIN_RATE_HEATMAP_CELL, name: classInfo.label };
  }
  return buildRateCell({
    name: classInfo.label,
    quinellaCount: counts.quinellaCount,
    showCount: counts.showCount,
    starts: counts.starts,
    winCount: counts.winCount,
  });
};

const toWeightHeatmapCell = (
  kg: number | null,
  ratesByClass: Map<string, WeightClassRateCounts>,
): WinRateHeatmapCell =>
  kg === null
    ? EMPTY_WIN_RATE_HEATMAP_CELL
    : toClassHeatmapCell(getHorseWeightClass(kg), ratesByClass);

const toCarriedWeightHeatmapCell = (
  kg: number | null,
  ratesByClass: Map<string, WeightClassRateCounts>,
): WinRateHeatmapCell =>
  kg === null
    ? EMPTY_WIN_RATE_HEATMAP_CELL
    : toClassHeatmapCell(getCarriedWeightClass(kg), ratesByClass);

const indexCarriedWeightClassRatesFromHorseResults = (
  horseResults: HorseRaceResult[],
): Map<string, WeightClassRateCounts> =>
  horseResults.reduce((index, result) => {
    const kg = parseCarriedWeightKg(result.futanJuryo);
    const rank = parseFinishPosition(result.kakuteiChakujun);
    if (kg === null || rank === null) {
      return index;
    }
    const carriedClass = getCarriedWeightClass(kg);
    const current = index.get(carriedClass.key) ?? {
      quinellaCount: 0,
      showCount: 0,
      starts: 0,
      winCount: 0,
    };
    return new Map(index).set(carriedClass.key, addFinishToWeightClassCounts(current, rank));
  }, new Map<string, WeightClassRateCounts>());

const resolveCarriedWeightClassRates = (
  horseResults: HorseRaceResult[],
  carriedWeightClassStats: readonly WeightClassStatsRow[] | undefined,
): Map<string, WeightClassRateCounts> => {
  if (carriedWeightClassStats !== undefined && carriedWeightClassStats.length > 0) {
    return indexWeightClassStats(carriedWeightClassStats);
  }
  return indexCarriedWeightClassRatesFromHorseResults(horseResults);
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
  const weightClassRates = resolveWeightClassRates(input.horseResults, input.weightClassStats);
  const carriedWeightClassRates = resolveCarriedWeightClassRates(
    input.horseResults,
    input.carriedWeightClassStats,
  );
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
      const currentCarriedWeightKg = parseCarriedWeightKg(runner.futanJuryo);
      return {
        cells: {
          carriedWeight: toCarriedWeightHeatmapCell(
            currentCarriedWeightKg,
            carriedWeightClassRates,
          ),
          damDamSire: toHeatmapCell(bloodline?.get("damDamSire")),
          damSire: toHeatmapCell(bloodline?.get("damSire")),
          damSireSire: toHeatmapCell(bloodline?.get("damSireSire")),
          frame: toFrameHeatmapCell(frameStatsByNumber.get(frameNumber), frameNumber),
          horse: buildHorseRateCell(horseName, horseResultsByNumber.get(horseNumber) ?? []),
          jockey: toHeatmapCell(similar?.get("jockey")),
          jockeyFrame: toHeatmapCell(similar?.get("jockeyFrame")),
          sire: toHeatmapCell(bloodline?.get("sire")),
          sireDamSire: toHeatmapCell(bloodline?.get("sireDamSire")),
          sireSire: toHeatmapCell(bloodline?.get("sireSire")),
          sireSireSire: toHeatmapCell(bloodline?.get("sireSireSire")),
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

export const shouldShowWinRateHeatmapCarriedWeightColumn = (
  input: ShouldShowWinRateHeatmapCarriedWeightColumnInput,
): boolean => {
  if (isOverseasKeibajoCode(input.keibajoCode) || isBanEiKeibajoCode(input.keibajoCode)) {
    return false;
  }
  return input.runners.some((runner) => parseCarriedWeightKg(runner.futanJuryo) !== null);
};

export const getWinRateHeatmapColorScale = (
  input: GetWinRateHeatmapColorScaleInput,
): WinRateHeatmapColorScale =>
  isBanEiKeibajoCode(input.keibajoCode)
    ? BAN_EI_WIN_RATE_HEATMAP_COLOR_SCALES[input.metricKey]
    : DEFAULT_WIN_RATE_HEATMAP_COLOR_SCALE;

const roundHeatmapRateTick = (rate: number): number =>
  Math.round(rate * HEATMAP_RATE_TICK_DECIMALS) / HEATMAP_RATE_TICK_DECIMALS;

const heatmapRateBounds = (
  rates: readonly number[],
): { maxRate: number; minRate: number } | null => {
  const first = rates[0];
  if (first === undefined) {
    return null;
  }
  return rates.reduce(
    (bounds, rate) => ({
      maxRate: rate > bounds.maxRate ? rate : bounds.maxRate,
      minRate: rate < bounds.minRate ? rate : bounds.minRate,
    }),
    { maxRate: first, minRate: first },
  );
};

const buildHeatmapColorScaleTicks = (minRate: number, maxRate: number): readonly number[] =>
  HEATMAP_COLOR_SCALE_TICK_INDEXES.map((index) =>
    roundHeatmapRateTick(
      minRate + ((maxRate - minRate) * index) / HEATMAP_COLOR_SCALE_TICK_LAST_INDEX,
    ),
  );

const buildDynamicHeatmapColorScale = (
  rates: readonly number[],
  fallback: WinRateHeatmapColorScale,
): WinRateHeatmapColorScale => {
  const bounds = heatmapRateBounds(rates);
  if (bounds === null) {
    return fallback;
  }
  if (bounds.maxRate - bounds.minRate < BAN_EI_EQUAL_RATE_PAD) {
    const minRate = roundHeatmapRateTick(bounds.minRate - BAN_EI_EQUAL_RATE_PAD);
    const maxRate = roundHeatmapRateTick(bounds.maxRate + BAN_EI_EQUAL_RATE_PAD);
    return { maxRate, minRate, ticks: buildHeatmapColorScaleTicks(minRate, maxRate) };
  }
  const minRate = roundHeatmapRateTick(bounds.minRate);
  const maxRate = roundHeatmapRateTick(bounds.maxRate);
  return { maxRate, minRate, ticks: buildHeatmapColorScaleTicks(minRate, maxRate) };
};

export const resolveWinRateHeatmapColorScale = (
  input: ResolveWinRateHeatmapColorScaleInput,
): WinRateHeatmapColorScale => {
  const fallback = getWinRateHeatmapColorScale({
    keibajoCode: input.keibajoCode,
    metricKey: input.metricKey,
  });
  if (!isBanEiKeibajoCode(input.keibajoCode)) {
    return fallback;
  }
  return buildDynamicHeatmapColorScale(input.rates, fallback);
};

const collectVisibleMetricRates = (
  rows: readonly WinRateHeatmapRow[],
  columns: readonly WinRateHeatmapColumn[],
  metricKey: WinRateHeatmapRateKey,
): readonly number[] =>
  rows.flatMap((row) =>
    columns
      .map((column) => toHeatmapNumber(row.cells[column.key][metricKey]))
      .filter((rate): rate is number => rate !== null),
  );

export const getVisibleWinRateHeatmapColumns = (
  input: GetVisibleWinRateHeatmapColumnsInput,
): readonly WinRateHeatmapColumn[] =>
  WIN_RATE_HEATMAP_COLUMNS.filter((column) => {
    if (column.key === "weight") {
      return input.showWeight;
    }
    if (column.key === "carriedWeight") {
      return input.showCarriedWeight;
    }
    return !(
      isBanEiKeibajoCode(input.keibajoCode) && BAN_EI_HIDDEN_HEATMAP_COLUMN_KEYS.has(column.key)
    );
  });

const formatHeatmapHsl = (color: HslColor): string =>
  `hsl(${color.hue}, ${color.saturation}%, ${color.lightness}%)`;

const heatmapFillRatio = (input: WinRateHeatmapFillInput, numericRate: number): number => {
  const span = input.maxRate - input.minRate;
  if (span <= 0) {
    return 0.5;
  }
  return Math.min(1, Math.max(0, (numericRate - input.minRate) / span));
};

const heatmapFillHsl = (input: WinRateHeatmapFillInput): HslColor => {
  const numericRate = toHeatmapNumber(input.rate);
  if (numericRate === null) {
    return EMPTY_CELL_HSL;
  }
  const ratio = heatmapFillRatio(input, numericRate);
  const useDynamicContrast =
    input.minRate !== 0 || input.maxRate !== WIN_RATE_HEATMAP_COLOR_SCALE_MAX_RATE;
  if (useDynamicContrast) {
    return {
      hue: input.hue,
      lightness: Math.round(
        BAN_EI_WIN_RATE_HEATMAP_MAX_LIGHTNESS - BAN_EI_WIN_RATE_HEATMAP_LIGHTNESS_RANGE * ratio,
      ),
      saturation: Math.round(
        BAN_EI_WIN_RATE_HEATMAP_SATURATION_MIN + BAN_EI_WIN_RATE_HEATMAP_SATURATION_RANGE * ratio,
      ),
    };
  }
  return {
    hue: input.hue,
    lightness: Math.round(
      WIN_RATE_HEATMAP_MAX_LIGHTNESS - WIN_RATE_HEATMAP_LIGHTNESS_RANGE * ratio,
    ),
    saturation: Math.round(
      WIN_RATE_HEATMAP_SATURATION_MIN + WIN_RATE_HEATMAP_SATURATION_RANGE * ratio,
    ),
  };
};

const hslSectorRgb = (huePrime: number, chroma: number, x: number): RgbColor => {
  if (huePrime < 1) {
    return { blue: 0, green: x, red: chroma };
  }
  if (huePrime < 2) {
    return { blue: 0, green: chroma, red: x };
  }
  if (huePrime < 3) {
    return { blue: x, green: chroma, red: 0 };
  }
  if (huePrime < 4) {
    return { blue: chroma, green: x, red: 0 };
  }
  if (huePrime < 5) {
    return { blue: chroma, green: 0, red: x };
  }
  return { blue: x, green: 0, red: chroma };
};

const hslToRgb = (color: HslColor): RgbColor => {
  const hue = ((color.hue % HSL_HUE_CIRCLE) + HSL_HUE_CIRCLE) % HSL_HUE_CIRCLE;
  const saturation = color.saturation / HSL_SATURATION_SCALE;
  const lightness = color.lightness / HSL_SATURATION_SCALE;
  const chroma = (1 - Math.abs(2 * lightness - 1)) * saturation;
  const huePrime = hue / HSL_HUE_SECTOR;
  const x = chroma * (1 - Math.abs((huePrime % 2) - 1));
  const match = lightness - chroma / 2;
  const sector = hslSectorRgb(huePrime, chroma, x);
  return {
    blue: (sector.blue + match) * RGB_MAX,
    green: (sector.green + match) * RGB_MAX,
    red: (sector.red + match) * RGB_MAX,
  };
};

const linearizeSrgbChannel = (channel: number): number => {
  const ratio = channel / RGB_MAX;
  return ratio <= SRGB_LINEAR_THRESHOLD
    ? ratio / SRGB_LINEAR_DIVISOR
    : ((ratio + SRGB_GAMMA_OFFSET) / SRGB_GAMMA_SCALE) ** SRGB_GAMMA;
};

const relativeLuminance = (color: RgbColor): number =>
  RELATIVE_LUMINANCE_RED * linearizeSrgbChannel(color.red) +
  RELATIVE_LUMINANCE_GREEN * linearizeSrgbChannel(color.green) +
  RELATIVE_LUMINANCE_BLUE * linearizeSrgbChannel(color.blue);

const contrastRatio = (left: number, right: number): number => {
  const brighter = Math.max(left, right);
  const darker = Math.min(left, right);
  return (brighter + CONTRAST_OFFSET) / (darker + CONTRAST_OFFSET);
};

export const winRateHeatmapBackground = (input: WinRateHeatmapFillInput): string =>
  formatHeatmapHsl(heatmapFillHsl(input));

export const winRateHeatmapForeground = (input: WinRateHeatmapFillInput): string => {
  const fillLuminance = relativeLuminance(hslToRgb(heatmapFillHsl(input)));
  const inkContrast = contrastRatio(relativeLuminance(HEATMAP_INK_RGB), fillLuminance);
  const surfaceContrast = contrastRatio(relativeLuminance(HEATMAP_SURFACE_RGB), fillLuminance);
  return inkContrast >= surfaceContrast ? HEATMAP_FOREGROUND_INK : HEATMAP_FOREGROUND_SURFACE;
};

export const formatWinRateHeatmapColorScaleTick = (rate: number, maxRate: number): string =>
  rate >= maxRate ? `${rate}%以上` : `${rate}%`;

export const buildWinRateHeatmapColorScaleGradient = (
  input: WinRateHeatmapColorScaleGradientInput,
): string => {
  const span = input.maxRate - input.minRate;
  const stops = input.ticks.map((rate) => {
    const percent = span <= 0 ? 50 : ((rate - input.minRate) / span) * 100;
    return `${winRateHeatmapBackground({
      hue: input.hue,
      maxRate: input.maxRate,
      minRate: input.minRate,
      rate,
    })} ${percent}%`;
  });
  return `linear-gradient(to right, ${stops.join(", ")})`;
};

export const formatWinRateHeatmapColorScaleCaption = (
  metrics: readonly WinRateHeatmapRateMetric[],
): string => metrics.map((metric) => metric.label).join("+");

export const formatWinRateHeatmapColorScaleAriaLabel = (
  metrics: readonly WinRateHeatmapRateMetric[],
  scales: WinRateHeatmapColorScales,
): string => {
  const names = metrics.map((metric) => metric.label).join("、");
  if (metrics.length === 0) {
    return `${names}の色は0%から${WIN_RATE_HEATMAP_COLOR_SCALE_MAX_RATE}%以上まで濃くなります`;
  }
  const uniqueRanges = [
    ...new Set(
      metrics.map((metric) => {
        const scale = scales[metric.key];
        return `${scale.minRate}-${scale.maxRate}`;
      }),
    ),
  ];
  const firstMetric = metrics[0];
  if (uniqueRanges.length === 1 && firstMetric !== undefined) {
    const scale = scales[firstMetric.key];
    return `${names}の色は${scale.minRate}%から${scale.maxRate}%以上まで濃くなります`;
  }
  const metricRanges = metrics
    .map((metric) => {
      const scale = scales[metric.key];
      return `${metric.label}は${scale.minRate}%から${scale.maxRate}%以上`;
    })
    .join("、");
  return `${metricRanges}まで濃くなります`;
};

export const formatWinRateHeatmapValue = (rate: number | null | undefined): string => {
  // Never include "%". Combined 勝率+連対率+複勝率 stacks three rates in one cell,
  // and a percent sign there overflows. heatmapSwatchValueSuffix adds "%" only
  // for single-metric views (勝率 / 連対率 / 複勝率).
  const numericRate = toHeatmapNumber(rate);
  if (numericRate === null) {
    return "-";
  }
  return numericRate.toFixed(1);
};

export const formatWinRateHeatmapStarts = (starts: number | null | undefined): string => {
  const numericStarts = toHeatmapNumber(starts);
  return numericStarts === null ? "-" : String(Math.floor(numericStarts));
};

export const formatWinRateHeatmapTooltipStarts = (
  starts: number | null | undefined,
): string | null => {
  const formattedStarts = formatWinRateHeatmapStarts(starts);
  return formattedStarts === "-" ? null : `(${formattedStarts})`;
};

export const formatWinRateHeatmapGraphStarts = (
  starts: number | null | undefined,
): string | null => {
  const formattedStarts = formatWinRateHeatmapStarts(starts);
  if (formattedStarts === "-") {
    return null;
  }
  return `(${formattedStarts})`;
};

export const getVisibleWinRateHeatmapRateMetrics = (
  mode: WinRateHeatmapViewMode,
): readonly WinRateHeatmapRateMetric[] => WIN_RATE_HEATMAP_VIEW_MODE_METRICS[mode];

export const getWinRateHeatmapColorScaleTracks = (
  metrics: readonly WinRateHeatmapRateMetric[],
  scales: WinRateHeatmapColorScales,
): readonly WinRateHeatmapRateMetric[] => {
  if (metrics.length === 0) {
    return [];
  }
  const hues = new Set(metrics.map((metric) => metric.hue));
  const domains = new Set(
    metrics.map((metric) => {
      const scale = scales[metric.key];
      return `${scale.minRate}-${scale.maxRate}`;
    }),
  );
  if (hues.size > 1 || domains.size > 1) {
    return metrics;
  }
  return metrics.slice(0, 1);
};

export const getWinRateHeatmapTooltipName = (cell: WinRateHeatmapCell): string =>
  cell.name === null ? "-" : cell.name;

export const winRateHeatmapEntityColSpan = (visibleRateCount: number): number =>
  visibleRateCount === 0 ? EMPTY_RATE_COLUMN_SPAN : visibleRateCount;

export interface WinRateHeatmapDisplaySwatch {
  background: string;
  columnKey: WinRateHeatmapMetricKey;
  columnLabel: string;
  foreground: string;
  graphStartsLabel: string | null;
  isZeroGraphStarts: boolean;
  isZeroValue: boolean;
  metricKey: WinRateHeatmapRateKey;
  metricLabel: string;
  name: string;
  startsLabel: string | null;
  valueLabel: string;
}

export interface WinRateHeatmapDisplayRow {
  frameNumber: string;
  horseName: string;
  horseNumber: string;
  swatches: WinRateHeatmapDisplaySwatch[];
}

export interface WinRateHeatmapDisplayModel {
  colorScales: WinRateHeatmapColorScales;
  empty: boolean;
  entityColSpan: number;
  rows: WinRateHeatmapDisplayRow[];
  showCarriedWeight: boolean;
  showWeight: boolean;
  viewMode: WinRateHeatmapViewMode;
  visibleColumns: readonly WinRateHeatmapColumn[];
  visibleRateMetrics: readonly WinRateHeatmapRateMetric[];
}

export interface BuildWinRateHeatmapDisplayInput extends BuildWinRateHeatmapRowsInput {
  showStarts: boolean;
  viewMode: WinRateHeatmapViewMode;
}

export const buildWinRateHeatmapDisplay = (
  input: BuildWinRateHeatmapDisplayInput,
): WinRateHeatmapDisplayModel => {
  const showWeight = shouldShowWinRateHeatmapWeightColumn({
    keibajoCode: input.keibajoCode,
    liveWeightKgByHorse: input.liveWeightKgByHorse,
    runners: input.runners,
  });
  const showCarriedWeight = shouldShowWinRateHeatmapCarriedWeightColumn({
    keibajoCode: input.keibajoCode,
    runners: input.runners,
  });
  const visibleColumns = getVisibleWinRateHeatmapColumns({
    keibajoCode: input.keibajoCode,
    showCarriedWeight,
    showWeight,
  });
  const visibleRateMetrics = getVisibleWinRateHeatmapRateMetrics(input.viewMode);
  const rows = buildWinRateHeatmapRows(input);
  const colorScales: WinRateHeatmapColorScales = {
    quinellaRate: resolveWinRateHeatmapColorScale({
      keibajoCode: input.keibajoCode,
      metricKey: "quinellaRate",
      rates: collectVisibleMetricRates(rows, visibleColumns, "quinellaRate"),
    }),
    showRate: resolveWinRateHeatmapColorScale({
      keibajoCode: input.keibajoCode,
      metricKey: "showRate",
      rates: collectVisibleMetricRates(rows, visibleColumns, "showRate"),
    }),
    winRate: resolveWinRateHeatmapColorScale({
      keibajoCode: input.keibajoCode,
      metricKey: "winRate",
      rates: collectVisibleMetricRates(rows, visibleColumns, "winRate"),
    }),
  };
  return {
    colorScales,
    empty: rows.length === 0,
    entityColSpan: winRateHeatmapEntityColSpan(visibleRateMetrics.length),
    rows: rows.map((row) => ({
      frameNumber: row.frameNumber,
      horseName: row.horseName,
      horseNumber: row.horseNumber,
      swatches: visibleColumns.flatMap((column) => {
        const cell = row.cells[column.key] ?? EMPTY_WIN_RATE_HEATMAP_CELL;
        const startsLabel = formatWinRateHeatmapTooltipStarts(cell.starts);
        const graphStartsLabel = input.showStarts
          ? formatWinRateHeatmapGraphStarts(cell.starts)
          : null;
        return visibleRateMetrics.map((metric) => {
          const rate = cell[metric.key];
          const isZeroValue = toHeatmapNumber(rate) === 0;
          const scale = colorScales[metric.key];
          const fill = {
            hue: metric.hue,
            maxRate: scale.maxRate,
            minRate: scale.minRate,
            rate,
          };
          return {
            background: winRateHeatmapBackground(fill),
            columnKey: column.key,
            columnLabel: column.label,
            foreground: winRateHeatmapForeground(fill),
            graphStartsLabel,
            isZeroGraphStarts:
              graphStartsLabel !== null && (isZeroValue || toHeatmapNumber(cell.starts) === 0),
            isZeroValue,
            metricKey: metric.key,
            metricLabel: metric.shortLabel,
            name: getWinRateHeatmapTooltipName(cell),
            startsLabel,
            valueLabel: formatWinRateHeatmapValue(rate),
          };
        });
      }),
    })),
    showCarriedWeight,
    showWeight,
    viewMode: input.viewMode,
    visibleColumns,
    visibleRateMetrics,
  };
};
