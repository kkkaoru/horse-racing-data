// Run with bun (exercised via `bunx vitest run`).
// Pure data helpers for the race-detail 競走成績グラフ section: they turn the
// `results` section payload (HorseRaceResult rows) into per-horse chart series.
import { cleanText } from "./format";
import type { HorseRaceResult } from "./race-types";
import { isBanEiKeibajoCode } from "./runner-format";

export type HorseRaceChartMetric = "finish" | "popularity" | "weight" | "weightDelta";

export interface HorseRaceChartPoint {
  dateValue: number; // Date.UTC(yyyy, mm - 1, dd) milliseconds
  raceDate: string; // "YYYYMMDD"
  value: number;
}

export interface HorseRaceChartSeries {
  bamei: string;
  color: string;
  kettoTorokuBango: string;
  points: HorseRaceChartPoint[]; // ascending by dateValue, tie by raceBango asc
  umaban: number | null; // numeric currentUmaban
}

// One race of a single horse with all four chart metrics parsed side by side,
// so the correlation view can plot them against a shared X axis.
export interface HorseRaceCorrelationRow {
  dateValue: number; // Date.UTC(yyyy, mm - 1, dd) milliseconds
  finish: number | null;
  popularity: number | null;
  raceDate: string; // "YYYYMMDD"
  weight: number | null;
  weightDelta: number | null;
}

interface HorseRaceChartSeriesDraft {
  bamei: string;
  kettoTorokuBango: string;
  results: HorseRaceResult[];
  umaban: number | null;
}

interface UmabanKeyedDraft extends HorseRaceChartSeriesDraft {
  umaban: number;
}

interface RaceDateOrderKey {
  dateValue: number;
  raceBango: string;
}

interface HorseRaceChartPointSource extends RaceDateOrderKey {
  point: HorseRaceChartPoint;
}

interface HorseRaceCorrelationRowSource extends RaceDateOrderKey {
  row: HorseRaceCorrelationRow;
}

interface SeriesColorInput {
  seriesIndex: number;
  umaban: number | null;
  umabanKeyedCount: number;
  unusedColors: string[];
}

const RACE_DATE_PATTERN = /^\d{8}$/;
const ALL_ZERO_PATTERN = /^0+$/;
const RACE_DATE_YEAR_LENGTH = 4;
const RACE_DATE_MONTH_END = 6;
const RACE_DATE_DAY_END = 8;
const INVALID_WEIGHT_ZERO = "000";
const INVALID_WEIGHT_FFF = "FFF";
// Non-Ban-ei decimal weight 999 means the horse could not be weighed; plotting
// it would distort the Y axis, so the chart treats it as missing.
const NON_BANEI_UNMEASURED_WEIGHT = 999;
const HEX_RADIX = 16;
const NEGATIVE_SIGN = "-";
const UNKNOWN_BAMEI = "不明";
const DATE_PART_LENGTH = 2;
const DATE_PAD_CHAR = "0";
// 18 visually distinct hues that stay readable on a white chart background.
const HORSE_RACE_CHART_COLORS: string[] = [
  "#d62728",
  "#1f77b4",
  "#2ca02c",
  "#ff7f0e",
  "#9467bd",
  "#8c564b",
  "#e377c2",
  "#17becf",
  "#bcbd22",
  "#7f7f7f",
  "#393b79",
  "#637939",
  "#8c6d31",
  "#843c39",
  "#7b4173",
  "#5254a3",
  "#0f766e",
  "#b45309",
];
// Used only when the palette index falls outside the array (e.g. a malformed
// negative or fractional umaban); keeps the series renderable instead of blank.
const FALLBACK_CHART_COLOR = "#52525b";

export const HORSE_RACE_CHART_METRICS: HorseRaceChartMetric[] = [
  "finish",
  "popularity",
  "weight",
  "weightDelta",
];

export const HORSE_RACE_CHART_METRIC_LABELS: Record<HorseRaceChartMetric, string> = {
  finish: "着順",
  popularity: "人気",
  weight: "馬体重",
  weightDelta: "馬体重増減",
};

export const HORSE_RACE_CHART_METRIC_UNITS: Record<HorseRaceChartMetric, string> = {
  finish: "着",
  popularity: "番人気",
  weight: "kg",
  weightDelta: "kg",
};

const toRaceDate = (result: HorseRaceResult): string => result.kaisaiNen + result.kaisaiTsukihi;

const hasValidRaceDate = (result: HorseRaceResult): boolean =>
  RACE_DATE_PATTERN.test(toRaceDate(result));

const laterRaceDate = (left: string, right: string): string => (left >= right ? left : right);

export const filterHorseRaceResultsToRecentYears = (
  results: HorseRaceResult[],
  years: number,
): HorseRaceResult[] => {
  const validResults = results.filter(hasValidRaceDate);
  if (validResults.length === 0) {
    return [];
  }
  const newestDate = validResults.map(toRaceDate).reduce(laterRaceDate);
  const cutoff =
    String(Number(newestDate.slice(0, RACE_DATE_YEAR_LENGTH)) - years) +
    newestDate.slice(RACE_DATE_YEAR_LENGTH);
  return validResults.filter((result) => toRaceDate(result) >= cutoff);
};

const parseNumber = (value: string | null | undefined): number | null => {
  const cleaned = cleanText(value, "");
  if (!cleaned || ALL_ZERO_PATTERN.test(cleaned)) {
    return null;
  }
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
};

const isInvalidWeightText = (cleaned: string): boolean =>
  !cleaned || cleaned === INVALID_WEIGHT_ZERO || cleaned.toUpperCase() === INVALID_WEIGHT_FFF;

const decodeWeightValue = (cleaned: string, decodeHex: boolean): number =>
  decodeHex ? Number.parseInt(cleaned, HEX_RADIX) : Number(cleaned);

const parseWeight = (result: HorseRaceResult): number | null => {
  const cleaned = cleanText(result.bataiju, "");
  if (isInvalidWeightText(cleaned)) {
    return null;
  }
  const decodeHex = isBanEiKeibajoCode(result.keibajoCode);
  const parsed = decodeWeightValue(cleaned, decodeHex);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return !decodeHex && parsed === NON_BANEI_UNMEASURED_WEIGHT ? null : parsed;
};

const parseWeightDelta = (result: HorseRaceResult): number | null => {
  if (parseWeight(result) === null) {
    return null;
  }
  const cleaned = cleanText(result.zogenSa, "");
  if (isInvalidWeightText(cleaned)) {
    return null;
  }
  const parsed = decodeWeightValue(cleaned, isBanEiKeibajoCode(result.keibajoCode));
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return cleanText(result.zogenFugo, "") === NEGATIVE_SIGN ? -parsed : parsed;
};

const METRIC_VALUE_EXTRACTORS: Record<
  HorseRaceChartMetric,
  (result: HorseRaceResult) => number | null
> = {
  finish: (result) => parseNumber(result.kakuteiChakujun),
  popularity: (result) => parseNumber(result.tanshoNinkijun),
  weight: (result) => parseWeight(result),
  weightDelta: (result) => parseWeightDelta(result),
};

export const getHorseRaceChartMetricValue = (
  result: HorseRaceResult,
  metric: HorseRaceChartMetric,
): number | null => METRIC_VALUE_EXTRACTORS[metric](result);

const compareText = (left: string, right: string): number => {
  if (left === right) {
    return 0;
  }
  return left < right ? -1 : 1;
};

const toDateValue = (raceDate: string): number =>
  Date.UTC(
    Number(raceDate.slice(0, RACE_DATE_YEAR_LENGTH)),
    Number(raceDate.slice(RACE_DATE_YEAR_LENGTH, RACE_DATE_MONTH_END)) - 1,
    Number(raceDate.slice(RACE_DATE_MONTH_END, RACE_DATE_DAY_END)),
  );

const toChartPointSource = (result: HorseRaceResult, value: number): HorseRaceChartPointSource => {
  const raceDate = toRaceDate(result);
  const dateValue = toDateValue(raceDate);
  return {
    dateValue,
    point: { dateValue, raceDate, value },
    raceBango: result.raceBango,
  };
};

const compareRaceDateOrderKeys = (left: RaceDateOrderKey, right: RaceDateOrderKey): number => {
  if (left.dateValue !== right.dateValue) {
    return left.dateValue - right.dateValue;
  }
  return compareText(left.raceBango, right.raceBango);
};

const buildSeriesPoints = (
  results: HorseRaceResult[],
  metric: HorseRaceChartMetric,
): HorseRaceChartPoint[] =>
  results
    .filter(hasValidRaceDate)
    .flatMap((result) => {
      const value = getHorseRaceChartMetricValue(result, metric);
      return value === null ? [] : [toChartPointSource(result, value)];
    })
    .toSorted(compareRaceDateOrderKeys)
    .map((source) => source.point);

const buildSeriesDrafts = (results: HorseRaceResult[]): HorseRaceChartSeriesDraft[] => {
  const drafts = new Map<string, HorseRaceChartSeriesDraft>();
  results.forEach((result) => {
    const ketto = cleanText(result.kettoTorokuBango, "");
    if (!ketto) {
      return;
    }
    const existing = drafts.get(ketto);
    if (existing) {
      existing.results.push(result);
      return;
    }
    drafts.set(ketto, {
      bamei: cleanText(result.bamei, UNKNOWN_BAMEI),
      kettoTorokuBango: ketto,
      results: [result],
      umaban: parseNumber(result.currentUmaban),
    });
  });
  return [...drafts.values()];
};

const compareSeriesDrafts = (
  left: HorseRaceChartSeriesDraft,
  right: HorseRaceChartSeriesDraft,
): number => {
  if (left.umaban === null && right.umaban === null) {
    return compareText(left.kettoTorokuBango, right.kettoTorokuBango);
  }
  if (left.umaban === null) {
    return 1;
  }
  if (right.umaban === null) {
    return -1;
  }
  if (left.umaban !== right.umaban) {
    return left.umaban - right.umaban;
  }
  return compareText(left.kettoTorokuBango, right.kettoTorokuBango);
};

const pickChartColor = (paletteIndex: number): string =>
  HORSE_RACE_CHART_COLORS[paletteIndex] ?? FALLBACK_CHART_COLOR;

const resolveUmabanColor = (umaban: number): string =>
  pickChartColor((umaban - 1) % HORSE_RACE_CHART_COLORS.length);

const isUmabanKeyedDraft = (draft: HorseRaceChartSeriesDraft): draft is UmabanKeyedDraft =>
  draft.umaban !== null;

const collectUnusedColors = (drafts: HorseRaceChartSeriesDraft[]): string[] => {
  const usedColors = new Set(
    drafts.filter(isUmabanKeyedDraft).map((draft) => resolveUmabanColor(draft.umaban)),
  );
  return HORSE_RACE_CHART_COLORS.filter((color) => !usedColors.has(color));
};

// Review NIT #1 fix: a null-umaban series must not collide with an
// umaban-keyed color. Fallback series draw from the palette entries left
// unused by umaban-keyed series in this build, indexed by fallback order
// (null-umaban drafts sort last, so seriesIndex minus the umaban-keyed count
// is that order) and cycling modulo the unused-list length. When every
// palette entry is used the lookup misses (an empty list yields a NaN index)
// and the legacy seriesIndex-based pick is kept.
const resolveFallbackColor = (input: SeriesColorInput): string => {
  const fallbackIndex = input.seriesIndex - input.umabanKeyedCount;
  const unusedPick = input.unusedColors[fallbackIndex % input.unusedColors.length];
  return unusedPick ?? pickChartColor(input.seriesIndex % HORSE_RACE_CHART_COLORS.length);
};

const resolveSeriesColor = (input: SeriesColorInput): string =>
  input.umaban === null ? resolveFallbackColor(input) : resolveUmabanColor(input.umaban);

export const buildHorseRaceChartSeriesList = (
  results: HorseRaceResult[],
  metric: HorseRaceChartMetric,
): HorseRaceChartSeries[] => {
  const drafts = buildSeriesDrafts(results).toSorted(compareSeriesDrafts);
  const umabanKeyedCount = drafts.filter(isUmabanKeyedDraft).length;
  const unusedColors = collectUnusedColors(drafts);
  return drafts.map((draft, index) => ({
    bamei: draft.bamei,
    color: resolveSeriesColor({
      seriesIndex: index,
      umaban: draft.umaban,
      umabanKeyedCount,
      unusedColors,
    }),
    kettoTorokuBango: draft.kettoTorokuBango,
    points: buildSeriesPoints(draft.results, metric),
    umaban: draft.umaban,
  }));
};

const toCorrelationRowSource = (result: HorseRaceResult): HorseRaceCorrelationRowSource => {
  const raceDate = toRaceDate(result);
  const dateValue = toDateValue(raceDate);
  return {
    dateValue,
    raceBango: result.raceBango,
    row: {
      dateValue,
      finish: getHorseRaceChartMetricValue(result, "finish"),
      popularity: getHorseRaceChartMetricValue(result, "popularity"),
      raceDate,
      weight: getHorseRaceChartMetricValue(result, "weight"),
      weightDelta: getHorseRaceChartMetricValue(result, "weightDelta"),
    },
  };
};

// Rows are kept even when every metric is null so the correlation view still
// shows that the race happened; sentinel and Ban-ei hex handling is shared
// with the per-metric series via getHorseRaceChartMetricValue.
export const buildHorseRaceCorrelationRows = (
  results: HorseRaceResult[],
  kettoTorokuBango: string,
): HorseRaceCorrelationRow[] => {
  const targetKetto = cleanText(kettoTorokuBango, "");
  if (!targetKetto) {
    return [];
  }
  return results
    .filter((result) => cleanText(result.kettoTorokuBango, "") === targetKetto)
    .filter(hasValidRaceDate)
    .map(toCorrelationRowSource)
    .toSorted(compareRaceDateOrderKeys)
    .map((source) => source.row);
};

export const formatHorseRaceChartDate = (dateValue: number): string => {
  const date = new Date(dateValue);
  const month = String(date.getUTCMonth() + 1).padStart(DATE_PART_LENGTH, DATE_PAD_CHAR);
  const day = String(date.getUTCDate()).padStart(DATE_PART_LENGTH, DATE_PAD_CHAR);
  return `${date.getUTCFullYear()}/${month}/${day}`;
};
