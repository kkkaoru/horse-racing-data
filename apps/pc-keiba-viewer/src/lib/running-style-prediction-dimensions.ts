// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

export type RunningStyleClass = "nige" | "senkou" | "sashi" | "oikomi";

export type RunningStyleBucketEvaluationPeriod = "all" | "oos-only";

export type ConfusionMatrix = readonly [
  readonly [number, number, number, number],
  readonly [number, number, number, number],
  readonly [number, number, number, number],
  readonly [number, number, number, number],
];

export interface RunningStyleDimensionFlags {
  keibajo: boolean;
  distance: boolean;
  kyosoShubetsu: boolean;
  kyosoJoken: boolean;
  condition: boolean;
  track: boolean;
  grade: boolean;
  raceName: boolean;
}

export type RunningStyleBucketScopeLevel = "exact" | "keibajo" | "category";

export interface RunningStyleBucketScope {
  level: RunningStyleBucketScopeLevel;
  flags: RunningStyleDimensionFlags;
}

export interface RunningStyleBucketFilter {
  category: string;
  source: "jra" | "nar";
  keibajoCode: string;
  kyori: number;
  kyosoShubetsuCode: string;
  kyosoJokenCode: string | null;
  conditionKey: string | null;
  trackCode: string | null;
  gradeCode: string | null;
  raceName: string | null;
  enabled: RunningStyleDimensionFlags;
  period: RunningStyleBucketEvaluationPeriod;
}

export interface RunningStylePerClassMetric {
  precision: number | null;
  recall: number | null;
  f1: number | null;
  support: number;
}

export interface RunningStyleConfidenceInterval {
  lower: number;
  upper: number;
}

export interface RunningStyleBucketMetrics {
  raceCount: number;
  predictionCount: number;
  accuracy: number;
  accuracyCI: RunningStyleConfidenceInterval;
  macroF1: number | null;
  weightedF1: number | null;
  qwk: number;
  top2Accuracy: number;
  overallLogLoss: number | null;
  perClass: Record<RunningStyleClass, RunningStylePerClassMetric>;
  perClassLogLoss: Record<RunningStyleClass, number | null>;
  confusionMatrix: ConfusionMatrix;
  smallSampleWarning: boolean;
}

export interface RaceRowForRunningStyleBucketFilter {
  source: "jra" | "nar";
  keibajoCode: string;
  kyori: number;
  kyosoShubetsuCode: string;
  kyosoJokenCode: string | null;
  kyosoJokenMeisho: string | null;
  trackCode: string | null;
  gradeCode: string | null;
  kyosomeiHondai: string | null;
}

export interface GetRunningStyleDimensionFlagsInput {
  query: Record<string, string | string[] | undefined>;
  source: "jra" | "nar";
  gradeCode: string | null;
  isBanEi: boolean;
}

export interface BuildRunningStyleBucketFilterInput {
  race: RaceRowForRunningStyleBucketFilter;
  flags: RunningStyleDimensionFlags;
  query?: Record<string, string | string[] | undefined>;
}

export interface DeriveWilsonScoreCIInput {
  successes: number;
  trials: number;
  confidence: number;
}

export interface DeriveLogLossInput {
  sumByClass: Record<RunningStyleClass, number>;
  countByClass: Record<RunningStyleClass, number>;
}

export interface DeriveLogLossResult {
  perClass: Record<RunningStyleClass, number | null>;
  overall: number | null;
}

export interface DeriveTop2AccuracyInput {
  hitCount: number;
  total: number;
}

export const RUNNING_STYLE_CLASSES: readonly RunningStyleClass[] = [
  "nige",
  "senkou",
  "sashi",
  "oikomi",
] satisfies readonly RunningStyleClass[];

export const RUNNING_STYLE_PREDICTION_PARAM_NAMES = {
  keibajo: "runningStyleKeibajo",
  distance: "runningStyleDistance",
  kyosoShubetsu: "runningStyleShubetsu",
  kyosoJoken: "runningStyleJoken",
  condition: "runningStyleCondition",
  track: "runningStyleTrack",
  grade: "runningStyleGrade",
  raceName: "runningStyleRaceName",
} satisfies Record<keyof RunningStyleDimensionFlags, string>;

export const RUNNING_STYLE_BUCKET_PERIOD_PARAM_NAME = "rs_period";

const RUNNING_STYLE_BUCKET_PERIOD_OOS_ONLY = "oos-only";
const RUNNING_STYLE_BUCKET_PERIOD_ALL = "all";

const SMALL_SAMPLE_THRESHOLD = 30;
const MIN_SUPPORT_FOR_F1 = 5;
const WILSON_Z_95 = 1.96;
const LOG_EPSILON = 1e-15;
const CLASS_COUNT = 4;
const RACE_NAME_GRADE_CODES = new Set<string>(["A", "F"]);
const CATEGORY_JRA = "jra";
const CATEGORY_NAR = "nar";

type ClassIndex = 0 | 1 | 2 | 3;

const NIGE_INDEX: ClassIndex = 0;
const SENKOU_INDEX: ClassIndex = 1;
const SASHI_INDEX: ClassIndex = 2;
const OIKOMI_INDEX: ClassIndex = 3;

const readFlag = (query: Record<string, string | string[] | undefined>, name: string): boolean => {
  const raw = query[name];
  const value = Array.isArray(raw) ? raw[0] : raw;
  return value !== "0";
};

const isGradeEligible = (gradeCode: string | null): boolean =>
  gradeCode !== null && gradeCode !== "";

const isRaceNameEligible = (gradeCode: string | null): boolean =>
  gradeCode !== null && RACE_NAME_GRADE_CODES.has(gradeCode);

const sumRow = (row: readonly [number, number, number, number]): number =>
  row[0] + row[1] + row[2] + row[3];

const sumColumn = (cm: ConfusionMatrix, columnIndex: ClassIndex): number =>
  cm[0][columnIndex] + cm[1][columnIndex] + cm[2][columnIndex] + cm[3][columnIndex];

const totalSum = (cm: ConfusionMatrix): number =>
  sumRow(cm[0]) + sumRow(cm[1]) + sumRow(cm[2]) + sumRow(cm[3]);

const computePrecision = (tp: number, fp: number): number | null => {
  const denominator = tp + fp;
  return denominator === 0 ? null : tp / denominator;
};

const computeRecall = (tp: number, fn: number): number | null => {
  const denominator = tp + fn;
  return denominator === 0 ? null : tp / denominator;
};

const computeF1 = ({
  precision,
  recall,
  support,
}: {
  precision: number | null;
  recall: number | null;
  support: number;
}): number | null => {
  if (precision === null || recall === null) {
    return null;
  }
  if (support < MIN_SUPPORT_FOR_F1) {
    return null;
  }
  const denominator = precision + recall;
  return denominator === 0 ? null : (2 * precision * recall) / denominator;
};

const computeMetricForClass = (
  cm: ConfusionMatrix,
  classIndex: ClassIndex,
): RunningStylePerClassMetric => {
  const tp = cm[classIndex][classIndex];
  const rowTotal = sumRow(cm[classIndex]);
  const colTotal = sumColumn(cm, classIndex);
  const fp = colTotal - tp;
  const fn = rowTotal - tp;
  const support = rowTotal;
  const precision = computePrecision(tp, fp);
  const recall = computeRecall(tp, fn);
  const f1 = computeF1({ precision, recall, support });
  return { precision, recall, f1, support };
};

export const derivePerClassMetrics = (
  cm: ConfusionMatrix,
): Record<RunningStyleClass, RunningStylePerClassMetric> => ({
  nige: computeMetricForClass(cm, NIGE_INDEX),
  senkou: computeMetricForClass(cm, SENKOU_INDEX),
  sashi: computeMetricForClass(cm, SASHI_INDEX),
  oikomi: computeMetricForClass(cm, OIKOMI_INDEX),
});

export const deriveAccuracy = (cm: ConfusionMatrix): number => {
  const total = totalSum(cm);
  if (total === 0) {
    return 0;
  }
  const trace =
    cm[NIGE_INDEX][NIGE_INDEX] +
    cm[SENKOU_INDEX][SENKOU_INDEX] +
    cm[SASHI_INDEX][SASHI_INDEX] +
    cm[OIKOMI_INDEX][OIKOMI_INDEX];
  return trace / total;
};

const collectEligibleF1s = (
  perClass: Record<RunningStyleClass, RunningStylePerClassMetric>,
): readonly number[] =>
  RUNNING_STYLE_CLASSES.map((className) => perClass[className]).flatMap((metric) =>
    metric.f1 === null ? [] : [metric.f1],
  );

export const deriveMacroF1 = (
  perClass: Record<RunningStyleClass, RunningStylePerClassMetric>,
): number | null => {
  const eligible = collectEligibleF1s(perClass);
  if (eligible.length === 0) {
    return null;
  }
  const sum = eligible.reduce((acc, value) => acc + value, 0);
  return sum / eligible.length;
};

const toQualifiedMetric = (
  metric: RunningStylePerClassMetric,
): readonly { readonly f1: number; readonly support: number }[] =>
  metric.f1 === null ? [] : [{ f1: metric.f1, support: metric.support }];

export const deriveWeightedF1 = (
  perClass: Record<RunningStyleClass, RunningStylePerClassMetric>,
): number | null => {
  const qualified = RUNNING_STYLE_CLASSES.flatMap((className) =>
    toQualifiedMetric(perClass[className]),
  );
  const aggregated = qualified.reduce(
    (acc, metric) => ({
      weightedSum: acc.weightedSum + metric.f1 * metric.support,
      supportSum: acc.supportSum + metric.support,
    }),
    { weightedSum: 0, supportSum: 0 },
  );
  if (aggregated.supportSum === 0) {
    return null;
  }
  return aggregated.weightedSum / aggregated.supportSum;
};

const kappaWeight = (i: ClassIndex, j: ClassIndex): number => {
  const denominator = (CLASS_COUNT - 1) * (CLASS_COUNT - 1);
  const diff = i - j;
  return (diff * diff) / denominator;
};

const CLASS_INDICES: readonly ClassIndex[] = [0, 1, 2, 3];

const INDEX_PAIRS: readonly { i: ClassIndex; j: ClassIndex }[] = CLASS_INDICES.flatMap((i) =>
  CLASS_INDICES.map((j) => ({ i, j })),
);

interface KappaTotals {
  readonly total: number;
  readonly rowTotals: readonly [number, number, number, number];
  readonly colTotals: readonly [number, number, number, number];
}

const buildKappaTotals = (cm: ConfusionMatrix): KappaTotals => ({
  total: totalSum(cm),
  rowTotals: [sumRow(cm[0]), sumRow(cm[1]), sumRow(cm[2]), sumRow(cm[3])],
  colTotals: [
    sumColumn(cm, NIGE_INDEX),
    sumColumn(cm, SENKOU_INDEX),
    sumColumn(cm, SASHI_INDEX),
    sumColumn(cm, OIKOMI_INDEX),
  ],
});

const sumObservedDisagreement = (cm: ConfusionMatrix, total: number): number =>
  INDEX_PAIRS.reduce((acc, { i, j }) => acc + kappaWeight(i, j) * (cm[i][j] / total), 0);

const sumExpectedDisagreement = (
  rowTotals: readonly [number, number, number, number],
  colTotals: readonly [number, number, number, number],
  total: number,
): number =>
  INDEX_PAIRS.reduce(
    (acc, { i, j }) => acc + kappaWeight(i, j) * ((rowTotals[i] * colTotals[j]) / (total * total)),
    0,
  );

export const deriveQuadraticWeightedKappa = (cm: ConfusionMatrix): number => {
  const totals = buildKappaTotals(cm);
  if (totals.total === 0) {
    return 0;
  }
  const observed = sumObservedDisagreement(cm, totals.total);
  const expected = sumExpectedDisagreement(totals.rowTotals, totals.colTotals, totals.total);
  if (expected === 0) {
    return 0;
  }
  return 1 - observed / expected;
};

export const deriveWilsonScoreCI = (
  input: DeriveWilsonScoreCIInput,
): RunningStyleConfidenceInterval => {
  const { successes, trials } = input;
  if (trials === 0) {
    return { lower: 0, upper: 0 };
  }
  const z = WILSON_Z_95;
  const proportion = successes / trials;
  const zSquared = z * z;
  const denominator = 1 + zSquared / trials;
  const center = (proportion + zSquared / (2 * trials)) / denominator;
  const marginNumerator = Math.sqrt(
    (proportion * (1 - proportion)) / trials + zSquared / (4 * trials * trials),
  );
  const margin = (z * marginNumerator) / denominator;
  const lower = Math.max(0, center - margin);
  const upper = Math.min(1, center + margin);
  return { lower, upper };
};

const perClassLogLossValue = (
  sumByClass: Record<RunningStyleClass, number>,
  countByClass: Record<RunningStyleClass, number>,
  className: RunningStyleClass,
): number | null => {
  const count = countByClass[className];
  return count === 0 ? null : sumByClass[className] / count;
};

export const deriveLogLoss = (input: DeriveLogLossInput): DeriveLogLossResult => {
  const { sumByClass, countByClass } = input;
  const perClass: Record<RunningStyleClass, number | null> = {
    nige: perClassLogLossValue(sumByClass, countByClass, "nige"),
    senkou: perClassLogLossValue(sumByClass, countByClass, "senkou"),
    sashi: perClassLogLossValue(sumByClass, countByClass, "sashi"),
    oikomi: perClassLogLossValue(sumByClass, countByClass, "oikomi"),
  };
  const totalCount = RUNNING_STYLE_CLASSES.reduce(
    (acc, className) => acc + countByClass[className],
    0,
  );
  if (totalCount === 0) {
    return { perClass, overall: null };
  }
  const totalSumValue = RUNNING_STYLE_CLASSES.reduce(
    (acc, className) => acc + sumByClass[className],
    0,
  );
  return { perClass, overall: totalSumValue / totalCount };
};

export const deriveTop2Accuracy = (input: DeriveTop2AccuracyInput): number => {
  const { hitCount, total } = input;
  if (total === 0) {
    return 0;
  }
  return hitCount / total;
};

export const isSmallSample = (predictionCount: number): boolean =>
  predictionCount < SMALL_SAMPLE_THRESHOLD;

export const getRunningStyleDimensionFlags = (
  input: GetRunningStyleDimensionFlagsInput,
): RunningStyleDimensionFlags => {
  const { query, source, gradeCode, isBanEi } = input;
  if (isBanEi) {
    return {
      keibajo: false,
      distance: false,
      kyosoShubetsu: false,
      kyosoJoken: false,
      condition: false,
      track: false,
      grade: false,
      raceName: false,
    };
  }
  const keibajo = readFlag(query, RUNNING_STYLE_PREDICTION_PARAM_NAMES.keibajo);
  const distance = readFlag(query, RUNNING_STYLE_PREDICTION_PARAM_NAMES.distance);
  const kyosoShubetsu = readFlag(query, RUNNING_STYLE_PREDICTION_PARAM_NAMES.kyosoShubetsu);
  const kyosoJokenRaw = readFlag(query, RUNNING_STYLE_PREDICTION_PARAM_NAMES.kyosoJoken);
  const conditionRaw = readFlag(query, RUNNING_STYLE_PREDICTION_PARAM_NAMES.condition);
  const trackRaw = readFlag(query, RUNNING_STYLE_PREDICTION_PARAM_NAMES.track);
  const gradeRaw = readFlag(query, RUNNING_STYLE_PREDICTION_PARAM_NAMES.grade);
  const raceNameRaw = readFlag(query, RUNNING_STYLE_PREDICTION_PARAM_NAMES.raceName);
  const kyosoJoken = source === "nar" ? false : kyosoJokenRaw;
  const condition = source === "jra" ? false : conditionRaw;
  const gradeAllowed = source !== "jra" && isGradeEligible(gradeCode);
  const grade = gradeAllowed ? gradeRaw : false;
  const raceName = isRaceNameEligible(gradeCode) ? raceNameRaw : false;
  return {
    keibajo,
    distance,
    kyosoShubetsu,
    kyosoJoken,
    condition,
    track: trackRaw,
    grade,
    raceName,
  };
};

const resolveConditionKey = (race: RaceRowForRunningStyleBucketFilter): string | null => {
  if (race.source === "jra") {
    return null;
  }
  const meisho = race.kyosoJokenMeisho;
  if (meisho === null) {
    return null;
  }
  const trimmed = meisho.trim();
  return trimmed === "" ? null : trimmed;
};

// Strip leading/trailing whitespace AND U+3000 (ideographic space).
// Required because PG persists `race_name` as char(30) padded with U+3000
// (e.g. "有馬記念" + U+3000 x 26), which JS String.prototype.trim does not remove.
const RACE_NAME_OUTER_WHITESPACE_PATTERN = /^[\s　]+|[\s　]+$/gu;

const resolveRaceName = (race: RaceRowForRunningStyleBucketFilter): string | null => {
  if (!isRaceNameEligible(race.gradeCode)) {
    return null;
  }
  const hondai = race.kyosomeiHondai;
  if (hondai === null) {
    return null;
  }
  const trimmed = hondai.replace(RACE_NAME_OUTER_WHITESPACE_PATTERN, "");
  return trimmed === "" ? null : trimmed;
};

const resolveCategory = (source: "jra" | "nar"): string =>
  source === "nar" ? CATEGORY_NAR : CATEGORY_JRA;

const resolvePeriod = (
  query: Record<string, string | string[] | undefined> | undefined,
): RunningStyleBucketEvaluationPeriod => {
  if (query === undefined) {
    return RUNNING_STYLE_BUCKET_PERIOD_ALL;
  }
  const raw = query[RUNNING_STYLE_BUCKET_PERIOD_PARAM_NAME];
  const value = Array.isArray(raw) ? raw[0] : raw;
  return value === RUNNING_STYLE_BUCKET_PERIOD_OOS_ONLY
    ? RUNNING_STYLE_BUCKET_PERIOD_OOS_ONLY
    : RUNNING_STYLE_BUCKET_PERIOD_ALL;
};

export const buildRunningStyleBucketFilter = (
  input: BuildRunningStyleBucketFilterInput,
): RunningStyleBucketFilter => {
  const { race, flags, query } = input;
  const category = resolveCategory(race.source);
  const conditionKey = resolveConditionKey(race);
  const raceName = resolveRaceName(race);
  const period = resolvePeriod(query);
  return {
    category,
    source: race.source,
    keibajoCode: race.keibajoCode,
    kyori: race.kyori,
    kyosoShubetsuCode: race.kyosoShubetsuCode,
    kyosoJokenCode: flags.kyosoJoken ? race.kyosoJokenCode : null,
    conditionKey: flags.condition ? conditionKey : null,
    trackCode: flags.track ? race.trackCode : null,
    gradeCode: flags.grade ? race.gradeCode : null,
    raceName: flags.raceName ? raceName : null,
    enabled: flags,
    period,
  };
};

// Re-export internal constants required by SQL builders downstream.
export const RUNNING_STYLE_LOG_EPSILON = LOG_EPSILON;
