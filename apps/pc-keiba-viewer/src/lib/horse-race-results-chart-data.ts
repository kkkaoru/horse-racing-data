// Run with bun (exercised via `bunx vitest run`).
// Pure data helpers for the race-detail 競走成績グラフ section: they turn the
// `results` section payload (HorseRaceResult rows) into per-horse chart series.
import { cleanText } from "./format";
import type { HorseRaceResult } from "./race-types";
import { formatRunnerNumber, isBanEiKeibajoCode } from "./runner-format";

export type HorseRaceChartMetric = "finish" | "popularity" | "weight" | "weightDelta" | "futan";

export interface HorseRaceChartPoint {
  dateValue: number; // Date.UTC(yyyy, mm - 1, dd) milliseconds
  // Race distance ("kyori"); populated only for finish & popularity metrics so
  // their tooltips can show distance. Weight/delta points leave it null.
  jockey?: string | null;
  // Jockey short name ("kishumeiRyakusho"); populated only for finish &
  // popularity metrics. Weight/delta points leave it null.
  kyori?: string | null;
  // True for the synthetic upcoming-race point appended for weight & weightDelta
  // metrics. Omitted/false for every past point.
  isUpcoming?: boolean;
  raceDate: string; // "YYYYMMDD"
  value: number;
}

// One entered runner of the upcoming (target) race; supplies its frame and the
// to-be-weighed values that seed the upcoming chart point / correlation row.
export interface HorseRaceChartRunner {
  bataiju: string | null;
  kettoTorokuBango: string | null;
  umaban: string | null;
  wakuban: string | null;
  zogenFugo: string | null;
  zogenSa: string | null;
}

// One horse's realtime upcoming-race weight, already decoded to numbers (kg) by
// the realtime weight stream. `weightDelta` carries the sign already applied
// (changeSign === "-" ? -changeAmount : changeAmount). Keyed back to a series by
// `umaban`. When `weight` is a finite number it overrides the static-runner
// bataiju path; otherwise the static path is kept as the fallback.
export interface UpcomingWeightOverride {
  umaban: string | null;
  weight: number | null;
  weightDelta: number | null;
}

// Resolved numeric weight/delta for one horse's upcoming point/row.
interface UpcomingWeightValues {
  weight: number;
  weightDelta: number | null;
}

export interface HorseRaceChartSeries {
  bamei: string;
  color: string;
  // Entered-race wakuban for this horse (matched by trimmed kettoTorokuBango),
  // or null when the horse is not in the upcoming race / no runners supplied.
  // The UI resolves the actual stroke color from this frame; `color` stays as a
  // fallback.
  frame: string | null;
  kettoTorokuBango: string;
  points: HorseRaceChartPoint[]; // ascending by dateValue, tie by raceBango asc
  umaban: number | null; // numeric currentUmaban
}

// One race of a single horse with all four chart metrics parsed side by side,
// so the correlation view can plot them against a shared X axis.
export interface HorseRaceCorrelationRow {
  dateValue: number; // Date.UTC(yyyy, mm - 1, dd) milliseconds
  finish: number | null;
  futan: number | null;
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

// Options for buildHorseRaceChartSeriesList; runners + target context are
// optional so callers that only need past points can omit them. `upcomingWeights`
// carries the realtime numeric weight/delta that takes precedence over the
// static-runner bataiju path when present for a matching umaban.
export interface BuildHorseRaceChartSeriesListOptions {
  // When true, the past "weight" points plot body weight plus carried weight
  // (馬体重 + 斤量) summed; ignored by every other metric. Defaults to off.
  combineFutan?: boolean;
  metric: HorseRaceChartMetric;
  results: HorseRaceResult[];
  runners?: HorseRaceChartRunner[];
  targetKeibajoCode?: string | null;
  targetRaceDate?: string | null; // "YYYYMMDD"
  upcomingWeights?: UpcomingWeightOverride[];
}

// Options for buildHorseRaceCorrelationRows; same optional target context so the
// upcoming race can be appended as the newest row, plus the realtime numeric
// weight override applied identically to the series builder.
export interface BuildHorseRaceCorrelationRowsOptions {
  kettoTorokuBango: string;
  results: HorseRaceResult[];
  runners?: HorseRaceChartRunner[];
  targetKeibajoCode?: string | null;
  targetRaceDate?: string | null; // "YYYYMMDD"
  upcomingWeights?: UpcomingWeightOverride[];
}

// Resolved upcoming-race context: a matched runner, the target race date, and the
// optional realtime numeric weight override for the same horse (null when no
// realtime entry matched its umaban). Shared by the series-point and
// correlation-row builders.
interface UpcomingRaceContext {
  dateValue: number;
  keibajoCode: string | null;
  raceDate: string;
  runner: HorseRaceChartRunner;
  weightOverride: UpcomingWeightOverride | null;
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
// Non-Ban-ei futanJuryo is stored in 0.1kg units; divide to recover kilograms.
const FUTAN_DECIGRAM_DIVISOR = 10;
const MONTHS_PER_YEAR = 12;
const NEGATIVE_SIGN = "-";
// formatRunnerNumber returns this sentinel for a blank / non-positive umaban; an
// override or runner umaban that normalizes to it must never key the map.
const EMPTY_UMABAN_KEY = "-";
const UNKNOWN_BAMEI = "不明";
const DATE_PART_LENGTH = 2;
const DATE_PAD_CHAR = "0";
// 18 maximally-distinguishable categorical colors for per-horse lines, ordered so
// adjacent umaban land on well-separated hues. Every entry is saturated and dark
// enough to stay legible against the white chart background; the previous set was
// replaced because its grays/olive/brown tones were too muddy and low-contrast to
// tell apart. Hues sweep the wheel (red→green→blue→orange→purple→cyan→magenta→
// gold...) with no near-duplicate pairs, so 18 horses remain individually readable.
const HORSE_RACE_CHART_COLORS: string[] = [
  "#e6194b",
  "#3cb44b",
  "#4363d8",
  "#f58231",
  "#911eb4",
  "#42d4f4",
  "#f032e6",
  "#bfa600",
  "#9a6324",
  "#469990",
  "#800000",
  "#000075",
  "#e60073",
  "#808000",
  "#3d8b00",
  "#7a3cff",
  "#d35400",
  "#0089a3",
];
// Used only when the palette index falls outside the array (e.g. a malformed
// negative or fractional umaban); keeps the series renderable instead of blank.
const FALLBACK_CHART_COLOR = "#52525b";

export const HORSE_RACE_CHART_METRICS: HorseRaceChartMetric[] = [
  "finish",
  "popularity",
  "weight",
  "weightDelta",
  "futan",
];

export const HORSE_RACE_CHART_METRIC_LABELS: Record<HorseRaceChartMetric, string> = {
  finish: "着順",
  popularity: "人気",
  weight: "馬体重",
  weightDelta: "馬体重増減",
  futan: "斤量",
};

export const HORSE_RACE_CHART_METRIC_UNITS: Record<HorseRaceChartMetric, string> = {
  finish: "着",
  popularity: "番人気",
  weight: "kg",
  weightDelta: "kg",
  futan: "kg",
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

// Convert a "YYYYMMDD" string into a flat calendar-month index so spans and
// cutoffs are plain integer arithmetic regardless of year boundaries.
const raceDateMonthIndex = (raceDate: string): number => {
  const year = Number(raceDate.slice(0, RACE_DATE_YEAR_LENGTH));
  const month = Number(raceDate.slice(RACE_DATE_YEAR_LENGTH, RACE_DATE_MONTH_END));
  return year * MONTHS_PER_YEAR + (month - 1);
};

export const countHorseRaceResultsSpanMonths = (results: HorseRaceResult[]): number => {
  const monthIndices = results
    .filter(hasValidRaceDate)
    .map((result) => raceDateMonthIndex(toRaceDate(result)));
  if (monthIndices.length === 0) {
    return 0;
  }
  return Math.max(...monthIndices) - Math.min(...monthIndices) + 1;
};

export const filterHorseRaceResultsToRecentMonths = (
  results: HorseRaceResult[],
  months: number,
): HorseRaceResult[] => {
  const validResults = results.filter(hasValidRaceDate);
  if (validResults.length === 0) {
    return [];
  }
  const newestMonthIndex = Math.max(
    ...validResults.map((result) => raceDateMonthIndex(toRaceDate(result))),
  );
  const cutoffMonthIndex = newestMonthIndex - (months - 1);
  return validResults.filter(
    (result) => raceDateMonthIndex(toRaceDate(result)) >= cutoffMonthIndex,
  );
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

// Parse a raw bataiju field with the keibajo's encoding (Ban-ei hex vs decimal),
// rejecting the 000/FFF/999 sentinels. Shared by past-result rows and the
// upcoming-race runner so both plot identically.
const parseWeightFields = (bataiju: string | null, keibajoCode: string | null): number | null => {
  const cleaned = cleanText(bataiju, "");
  if (isInvalidWeightText(cleaned)) {
    return null;
  }
  const decodeHex = isBanEiKeibajoCode(keibajoCode);
  const parsed = decodeWeightValue(cleaned, decodeHex);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return !decodeHex && parsed === NON_BANEI_UNMEASURED_WEIGHT ? null : parsed;
};

interface WeightDeltaFields {
  bataiju: string | null;
  keibajoCode: string | null;
  zogenFugo: string | null;
  zogenSa: string | null;
}

// Parse a raw zogenSa/zogenFugo delta with the keibajo's encoding; null unless
// the matching weight is also valid. Shared by past rows and the upcoming runner.
const parseWeightDeltaFields = (fields: WeightDeltaFields): number | null => {
  if (parseWeightFields(fields.bataiju, fields.keibajoCode) === null) {
    return null;
  }
  const cleaned = cleanText(fields.zogenSa, "");
  if (isInvalidWeightText(cleaned)) {
    return null;
  }
  const parsed = decodeWeightValue(cleaned, isBanEiKeibajoCode(fields.keibajoCode));
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return cleanText(fields.zogenFugo, "") === NEGATIVE_SIGN ? -parsed : parsed;
};

const parseWeight = (result: HorseRaceResult): number | null =>
  parseWeightFields(result.bataiju, result.keibajoCode);

const parseWeightDelta = (result: HorseRaceResult): number | null =>
  parseWeightDeltaFields({
    bataiju: result.bataiju,
    keibajoCode: result.keibajoCode,
    zogenFugo: result.zogenFugo,
    zogenSa: result.zogenSa,
  });

// Parse the carried weight (futanJuryo) to a number in kilograms, mirroring the
// formatCarriedWeight decode in runner-format.ts: Ban-ei stores hex kilograms,
// other keibajo store 0.1kg decigrams. 000/FFF/blank are missing-value sentinels.
const parseFutan = (result: HorseRaceResult): number | null => {
  const cleaned = cleanText(result.futanJuryo, "");
  if (!cleaned || cleaned.toUpperCase() === INVALID_WEIGHT_FFF || ALL_ZERO_PATTERN.test(cleaned)) {
    return null;
  }
  const decodeHex = isBanEiKeibajoCode(result.keibajoCode);
  const parsed = decodeHex ? Number.parseInt(cleaned, HEX_RADIX) : Number(cleaned);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return decodeHex ? parsed : parsed / FUTAN_DECIGRAM_DIVISOR;
};

// Body weight optionally combined with carried weight (馬体重 + 斤量). When
// combineFutan is false this is the plain parsed body weight. When true and the
// body weight is present, the carried weight is added (treated as 0 kg when its
// own field is missing); a missing body weight yields null in either mode.
export const getCombinedWeightValue = (
  result: HorseRaceResult,
  combineFutan: boolean,
): number | null => {
  const weight = parseWeight(result);
  if (!combineFutan || weight === null) {
    return weight;
  }
  return weight + (parseFutan(result) ?? 0);
};

const METRIC_VALUE_EXTRACTORS: Record<
  HorseRaceChartMetric,
  (result: HorseRaceResult) => number | null
> = {
  finish: (result) => parseNumber(result.kakuteiChakujun),
  popularity: (result) => parseNumber(result.tanshoNinkijun),
  weight: (result) => parseWeight(result),
  weightDelta: (result) => parseWeightDelta(result),
  futan: (result) => parseFutan(result),
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

// Metrics whose past points carry distance + jockey for the tooltip. Weight and
// weightDelta points leave those fields null.
const METADATA_METRICS = new Set<HorseRaceChartMetric>(["finish", "popularity"]);

// Metrics that gain a synthetic upcoming-race point. Finish & popularity have no
// result yet for the target race, so they get none.
const UPCOMING_POINT_METRICS = new Set<HorseRaceChartMetric>(["weight", "weightDelta"]);

const toChartPointSource = (
  result: HorseRaceResult,
  value: number,
  metric: HorseRaceChartMetric,
): HorseRaceChartPointSource => {
  const raceDate = toRaceDate(result);
  const dateValue = toDateValue(raceDate);
  const carriesMetadata = METADATA_METRICS.has(metric);
  return {
    dateValue,
    point: {
      dateValue,
      jockey: carriesMetadata ? cleanText(result.kishumeiRyakusho, "") || null : null,
      kyori: carriesMetadata ? cleanText(result.kyori, "") || null : null,
      raceDate,
      value,
    },
    raceBango: result.raceBango,
  };
};

const UPCOMING_RACE_BANGO = "99";

// Read the static-runner weight/delta from the bataiju/zogen fields, used as the
// fallback when no realtime numeric weight matched the horse.
const staticUpcomingWeight = (context: UpcomingRaceContext): number | null =>
  parseWeightFields(context.runner.bataiju, context.keibajoCode);

const staticUpcomingWeightDelta = (context: UpcomingRaceContext): number | null =>
  parseWeightDeltaFields({
    bataiju: context.runner.bataiju,
    keibajoCode: context.keibajoCode,
    zogenFugo: context.runner.zogenFugo,
    zogenSa: context.runner.zogenSa,
  });

// Resolve the upcoming weight/delta for one horse: prefer the realtime numeric
// override (already decoded kg + signed delta) when its weight is a finite
// number, otherwise fall back to parsing the static-runner bataiju fields.
// Returns null when neither source yields a usable weight (so no point/row is
// appended).
const resolveUpcomingWeightValues = (context: UpcomingRaceContext): UpcomingWeightValues | null => {
  const override = context.weightOverride;
  if (override !== null && override.weight !== null && Number.isFinite(override.weight)) {
    return { weight: override.weight, weightDelta: override.weightDelta };
  }
  const staticWeight = staticUpcomingWeight(context);
  if (staticWeight === null) {
    return null;
  }
  return { weight: staticWeight, weightDelta: staticUpcomingWeightDelta(context) };
};

// Read the upcoming horse's value for the given metric; only weight/delta yield
// a value (the guard in buildUpcomingPoint never calls this for other metrics).
const upcomingMetricValue = (
  values: UpcomingWeightValues,
  metric: HorseRaceChartMetric,
): number | null => (metric === "weight" ? values.weight : values.weightDelta);

// Build the synthetic upcoming weight/delta point, or null when the horse has no
// usable weight (missing runner, 000/FFF/999 sentinel, non-numeric, and no
// realtime override).
const buildUpcomingPointSource = (
  context: UpcomingRaceContext,
  metric: HorseRaceChartMetric,
): HorseRaceChartPointSource | null => {
  const values = resolveUpcomingWeightValues(context);
  if (values === null) {
    return null;
  }
  const value = upcomingMetricValue(values, metric);
  if (value === null) {
    return null;
  }
  return {
    dateValue: context.dateValue,
    point: {
      dateValue: context.dateValue,
      isUpcoming: true,
      jockey: null,
      kyori: null,
      raceDate: context.raceDate,
      value,
    },
    raceBango: UPCOMING_RACE_BANGO,
  };
};

const compareRaceDateOrderKeys = (left: RaceDateOrderKey, right: RaceDateOrderKey): number => {
  if (left.dateValue !== right.dateValue) {
    return left.dateValue - right.dateValue;
  }
  return compareText(left.raceBango, right.raceBango);
};

interface BuildSeriesPointsInput {
  // When true, past "weight" points sum body weight and carried weight; other
  // metrics ignore it. The synthetic upcoming point is never affected.
  combineFutan: boolean;
  metric: HorseRaceChartMetric;
  results: HorseRaceResult[];
  // Upcoming-race context for this horse, or null when the horse is not entered
  // / no target context supplied. Only weight & weightDelta consume it.
  upcoming: UpcomingRaceContext | null;
}

// Collect the optional upcoming weight/delta point as a 0-or-1 element list so
// flatMap can append it without branching the array type.
const collectUpcomingPointSources = (
  input: BuildSeriesPointsInput,
): HorseRaceChartPointSource[] => {
  if (input.upcoming === null || !UPCOMING_POINT_METRICS.has(input.metric)) {
    return [];
  }
  const source = buildUpcomingPointSource(input.upcoming, input.metric);
  return source === null ? [] : [source];
};

// Resolve a past point's value: the "weight" metric routes through
// getCombinedWeightValue so the combineFutan toggle can add carried weight; every
// other metric keeps its plain extractor.
const resolvePastValue = (input: BuildSeriesPointsInput, result: HorseRaceResult): number | null =>
  input.metric === "weight"
    ? getCombinedWeightValue(result, input.combineFutan)
    : getHorseRaceChartMetricValue(result, input.metric);

const buildSeriesPoints = (input: BuildSeriesPointsInput): HorseRaceChartPoint[] => {
  const pastSources = input.results.filter(hasValidRaceDate).flatMap((result) => {
    const value = resolvePastValue(input, result);
    return value === null ? [] : [toChartPointSource(result, value, input.metric)];
  });
  return [...pastSources, ...collectUpcomingPointSources(input)]
    .toSorted(compareRaceDateOrderKeys)
    .map((source) => source.point);
};

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

// Index runners by trimmed kettoTorokuBango; blank keys are dropped so they can
// never match a series. The last runner wins on duplicate keys.
const buildRunnerMap = (runners: HorseRaceChartRunner[]): Map<string, HorseRaceChartRunner> => {
  const map = new Map<string, HorseRaceChartRunner>();
  runners.forEach((runner) => {
    const ketto = cleanText(runner.kettoTorokuBango, "");
    if (ketto) {
      map.set(ketto, runner);
    }
  });
  return map;
};

const resolveSeriesFrame = (runner: HorseRaceChartRunner | undefined): string | null =>
  runner === undefined ? null : cleanText(runner.wakuban, "") || null;

// Index the realtime numeric weight overrides by normalized umaban (the same
// normalization runner umaban uses) so a series can match its realtime entry by
// horse number. Entries whose umaban normalizes to the "-" sentinel are dropped
// so they can never match a horse; the last entry wins on duplicate keys.
const buildUpcomingWeightMap = (
  upcomingWeights: UpcomingWeightOverride[],
): Map<string, UpcomingWeightOverride> => {
  const map = new Map<string, UpcomingWeightOverride>();
  upcomingWeights.forEach((entry) => {
    const key = formatRunnerNumber(entry.umaban);
    if (key !== EMPTY_UMABAN_KEY) {
      map.set(key, entry);
    }
  });
  return map;
};

interface UpcomingContextInputs {
  runnerMap: Map<string, HorseRaceChartRunner>;
  targetKeibajoCode: string | null;
  targetRaceDate: string | null;
  weightMap: Map<string, UpcomingWeightOverride>;
}

// Resolve the upcoming-race context for one horse: requires a matching runner
// and a well-formed target race date. Returns null otherwise so no point/row is
// appended. The realtime weight override is matched by the runner's normalized
// umaban (null when no realtime entry matched).
const resolveUpcomingContext = (
  ketto: string,
  inputs: UpcomingContextInputs,
): UpcomingRaceContext | null => {
  const runner = inputs.runnerMap.get(ketto);
  const raceDate = inputs.targetRaceDate ?? "";
  if (runner === undefined || !RACE_DATE_PATTERN.test(raceDate)) {
    return null;
  }
  const umabanKey = formatRunnerNumber(runner.umaban);
  return {
    dateValue: toDateValue(raceDate),
    keibajoCode: inputs.targetKeibajoCode ?? null,
    raceDate,
    runner,
    weightOverride:
      umabanKey === EMPTY_UMABAN_KEY ? null : (inputs.weightMap.get(umabanKey) ?? null),
  };
};

export const buildHorseRaceChartSeriesList = (
  options: BuildHorseRaceChartSeriesListOptions,
): HorseRaceChartSeries[] => {
  const drafts = buildSeriesDrafts(options.results).toSorted(compareSeriesDrafts);
  const umabanKeyedCount = drafts.filter(isUmabanKeyedDraft).length;
  const unusedColors = collectUnusedColors(drafts);
  const runnerMap = buildRunnerMap(options.runners ?? []);
  const upcomingInputs: UpcomingContextInputs = {
    runnerMap,
    targetKeibajoCode: options.targetKeibajoCode ?? null,
    targetRaceDate: options.targetRaceDate ?? null,
    weightMap: buildUpcomingWeightMap(options.upcomingWeights ?? []),
  };
  return drafts.map((draft, index) => ({
    bamei: draft.bamei,
    color: resolveSeriesColor({
      seriesIndex: index,
      umaban: draft.umaban,
      umabanKeyedCount,
      unusedColors,
    }),
    frame: resolveSeriesFrame(runnerMap.get(draft.kettoTorokuBango)),
    kettoTorokuBango: draft.kettoTorokuBango,
    points: buildSeriesPoints({
      combineFutan: options.combineFutan ?? false,
      metric: options.metric,
      results: draft.results,
      upcoming: resolveUpcomingContext(draft.kettoTorokuBango, upcomingInputs),
    }),
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
      futan: getHorseRaceChartMetricValue(result, "futan"),
      popularity: getHorseRaceChartMetricValue(result, "popularity"),
      raceDate,
      weight: getHorseRaceChartMetricValue(result, "weight"),
      weightDelta: getHorseRaceChartMetricValue(result, "weightDelta"),
    },
  };
};

// Build the upcoming-race correlation row from the resolved weight values: only
// weight and weightDelta are known (finish/popularity null). The realtime numeric
// override takes precedence over the static-runner bataiju path, identical to the
// series builder. Returns a 0-or-1 element list so it can be concatenated without
// branching the array type; an unusable weight (no override and a sentinel static
// value) drops the row entirely.
const collectUpcomingCorrelationRowSources = (
  context: UpcomingRaceContext | null,
): HorseRaceCorrelationRowSource[] => {
  if (context === null) {
    return [];
  }
  const values = resolveUpcomingWeightValues(context);
  if (values === null) {
    return [];
  }
  return [
    {
      dateValue: context.dateValue,
      raceBango: UPCOMING_RACE_BANGO,
      row: {
        dateValue: context.dateValue,
        finish: null,
        futan: null,
        popularity: null,
        raceDate: context.raceDate,
        weight: values.weight,
        weightDelta: values.weightDelta,
      },
    },
  ];
};

// Rows are kept even when every metric is null so the correlation view still
// shows that the race happened; sentinel and Ban-ei hex handling is shared
// with the per-metric series via getHorseRaceChartMetricValue. When runner +
// target context match the horse, the upcoming race is appended as the newest
// row (weight/delta only).
export const buildHorseRaceCorrelationRows = (
  options: BuildHorseRaceCorrelationRowsOptions,
): HorseRaceCorrelationRow[] => {
  const targetKetto = cleanText(options.kettoTorokuBango, "");
  if (!targetKetto) {
    return [];
  }
  const upcoming = resolveUpcomingContext(targetKetto, {
    runnerMap: buildRunnerMap(options.runners ?? []),
    targetKeibajoCode: options.targetKeibajoCode ?? null,
    targetRaceDate: options.targetRaceDate ?? null,
    weightMap: buildUpcomingWeightMap(options.upcomingWeights ?? []),
  });
  const pastSources = options.results
    .filter((result) => cleanText(result.kettoTorokuBango, "") === targetKetto)
    .filter(hasValidRaceDate)
    .map(toCorrelationRowSource);
  return [...pastSources, ...collectUpcomingCorrelationRowSources(upcoming)]
    .toSorted(compareRaceDateOrderKeys)
    .map((source) => source.row);
};

export const formatHorseRaceChartDate = (dateValue: number): string => {
  const date = new Date(dateValue);
  const month = String(date.getUTCMonth() + 1).padStart(DATE_PART_LENGTH, DATE_PAD_CHAR);
  const day = String(date.getUTCDate()).padStart(DATE_PART_LENGTH, DATE_PAD_CHAR);
  return `${date.getUTCFullYear()}/${month}/${day}`;
};
