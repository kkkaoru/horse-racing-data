// Run with: bun run src/scripts/finish-position-features/identify-weak-buckets.ts \
//   --model-version-jra X --model-version-nar Y --output tmp/v8/weak-buckets.json
//
// Stage 0B helper for the v8 iterative loop plan. Reads the
// model_prediction_bucket_evaluations table for the given JRA + NAR model
// versions (Ban-ei is intentionally excluded from the v8 loop), normalises the
// per-bucket hit sums into accuracy ratios, computes 4-metric Wilson-LB gaps
// against the category mean, ranks buckets by composite gap, and writes the
// ranked list as JSON for downstream lever selection.

import { writeFile } from "node:fs/promises";

import { Pool } from "pg";

export type WeakCategory = "jra" | "nar";
export type WeakDim = "keibajo_code" | "kyori" | "track_code" | "grade_code" | "condition_key";

export interface WeakBucketRow {
  category: WeakCategory;
  source: string;
  keibajo_code: string;
  kyori: number;
  kyoso_shubetsu_code: string;
  kyoso_joken_code: string | null;
  condition_key: string | null;
  track_code: string | null;
  grade_code: string | null;
  race_name: string | null;
  race_count: number;
  top1_hit_sum: number;
  place2_hit_sum: number;
  place3_hit_sum: number;
  top3_box_hit_sum: number;
}

export interface MetricSnapshot {
  top1: number;
  place2: number;
  place3: number;
  top3_box: number;
}

export interface WeakBucketMetrics {
  top1: number;
  place2: number;
  place3: number;
  top3_box: number;
}

export interface WeakBucketGaps {
  top1: number;
  place2: number;
  place3: number;
  top3_box: number;
}

export interface WeakBucketEntry {
  cat: WeakCategory;
  dim: WeakDim;
  value: string;
  metrics: WeakBucketMetrics;
  wilson_lower: WeakBucketMetrics;
  gaps: WeakBucketGaps;
  composite_gap: number;
  race_count: number;
  sample_size_warning: boolean;
}

export interface WeakBucketTopRanking {
  metric: keyof WeakBucketMetrics;
  entries: WeakBucketEntry[];
}

export interface WeakBucketOutput {
  schema_version: 1;
  model_version_jra: string;
  model_version_nar: string;
  buckets: WeakBucketEntry[];
  top_by_metric: WeakBucketTopRanking[];
}

export interface IdentifyWeakBucketsCliOptions {
  pgUrl: string;
  modelVersionJra: string;
  modelVersionNar: string;
  output: string;
  sampleSizeThreshold: number;
  topPerMetric: number;
}

export interface BucketQueryResult<Row> {
  rows: Row[];
}

export interface BucketQueryRunner {
  query: <Row>(sql: string, params?: unknown[]) => Promise<BucketQueryResult<Row>>;
}

interface CategoryAccuracySummary {
  cat: WeakCategory;
  totalRaces: number;
  mean: MetricSnapshot;
}

interface ApplyArgResult {
  advanceBy: number;
}

const DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing";
const DEFAULT_SAMPLE_SIZE_THRESHOLD = 50;
const DEFAULT_TOP_PER_METRIC = 5;
const BUCKET_TABLE = "model_prediction_bucket_evaluations";
const WILSON_Z_SCORE = 1.96;
const METRIC_KEYS = ["top1", "place2", "place3", "top3_box"] satisfies (keyof WeakBucketMetrics)[];
const DIM_LABELS = [
  "keibajo_code",
  "kyori",
  "track_code",
  "grade_code",
  "condition_key",
] satisfies WeakDim[];
const ROUND_DIGITS = 6;
const ROUND_MULT = 1_000_000;

export const buildUsageText = (): string =>
  [
    "Usage:",
    "  bun run src/scripts/finish-position-features/identify-weak-buckets.ts \\",
    "    --model-version-jra <model_version_jra> \\",
    "    --model-version-nar <model_version_nar> \\",
    "    --output <output-json-path> \\",
    "    [--pg-url <connection-string>] \\",
    "    [--sample-size-threshold 50] \\",
    "    [--top-per-metric 5]",
  ].join("\n");

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value.`);
  return value;
};

export const initialOptions = (): IdentifyWeakBucketsCliOptions => ({
  pgUrl: process.env.DATABASE_URL_LOCAL ?? DEFAULT_PG_URL,
  modelVersionJra: "",
  modelVersionNar: "",
  output: "",
  sampleSizeThreshold: DEFAULT_SAMPLE_SIZE_THRESHOLD,
  topPerMetric: DEFAULT_TOP_PER_METRIC,
});

const applyArg = (
  options: IdentifyWeakBucketsCliOptions,
  name: string,
  value: string | undefined,
): ApplyArgResult => {
  if (name === "--pg-url") {
    options.pgUrl = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--model-version-jra") {
    options.modelVersionJra = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--model-version-nar") {
    options.modelVersionNar = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--output") {
    options.output = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--sample-size-threshold") {
    options.sampleSizeThreshold = Number.parseInt(requireValue(name, value), 10);
    return { advanceBy: 2 };
  }
  if (name === "--top-per-metric") {
    options.topPerMetric = Number.parseInt(requireValue(name, value), 10);
    return { advanceBy: 2 };
  }
  if (name === "--help" || name === "-h") {
    console.log(buildUsageText());
    process.exit(0);
  }
  throw new Error(`Unknown argument: ${name}`);
};

const consumeArgs = (
  options: IdentifyWeakBucketsCliOptions,
  argv: readonly string[],
  cursor: number,
): IdentifyWeakBucketsCliOptions => {
  const name = argv[cursor];
  if (name === undefined) return options;
  const { advanceBy } = applyArg(options, name, argv[cursor + 1]);
  return consumeArgs(options, argv, cursor + advanceBy);
};

export const parseArgs = (argv: readonly string[]): IdentifyWeakBucketsCliOptions => {
  const options = consumeArgs(initialOptions(), argv, 0);
  if (options.modelVersionJra === "") throw new Error("--model-version-jra is required.");
  if (options.modelVersionNar === "") throw new Error("--model-version-nar is required.");
  if (options.output === "") throw new Error("--output is required.");
  return options;
};

export const buildBucketSelectSql = (): string => `
    select
      source,
      keibajo_code,
      kyori,
      kyoso_shubetsu_code,
      kyoso_joken_code,
      condition_key,
      track_code,
      grade_code,
      race_name,
      sum(race_count)::numeric as race_count,
      sum(top1_hit_sum)::numeric as top1_hit_sum,
      sum(place2_hit_sum)::numeric as place2_hit_sum,
      sum(place3_hit_sum)::numeric as place3_hit_sum,
      sum(top3_box_hit_sum)::numeric as top3_box_hit_sum
    from ${BUCKET_TABLE}
    where model_version = $1 and category = $2
    group by source, keibajo_code, kyori, kyoso_shubetsu_code,
             kyoso_joken_code, condition_key, track_code, grade_code, race_name
  `;

const toNumber = (raw: unknown): number => {
  if (raw === null || raw === undefined) return 0;
  if (typeof raw === "number") return raw;
  return Number(raw);
};

interface BucketRowRaw {
  source: string;
  keibajo_code: string;
  kyori: string | number;
  kyoso_shubetsu_code: string;
  kyoso_joken_code: string | null;
  condition_key: string | null;
  track_code: string | null;
  grade_code: string | null;
  race_name: string | null;
  race_count: string | number;
  top1_hit_sum: string | number;
  place2_hit_sum: string | number;
  place3_hit_sum: string | number;
  top3_box_hit_sum: string | number;
}

export const normalizeBucketRow = (cat: WeakCategory, raw: BucketRowRaw): WeakBucketRow => ({
  category: cat,
  source: raw.source,
  keibajo_code: raw.keibajo_code,
  kyori: toNumber(raw.kyori),
  kyoso_shubetsu_code: raw.kyoso_shubetsu_code,
  kyoso_joken_code: raw.kyoso_joken_code,
  condition_key: raw.condition_key,
  track_code: raw.track_code,
  grade_code: raw.grade_code,
  race_name: raw.race_name,
  race_count: toNumber(raw.race_count),
  top1_hit_sum: toNumber(raw.top1_hit_sum),
  place2_hit_sum: toNumber(raw.place2_hit_sum),
  place3_hit_sum: toNumber(raw.place3_hit_sum),
  top3_box_hit_sum: toNumber(raw.top3_box_hit_sum),
});

export const fetchBucketRows = async (
  runner: BucketQueryRunner,
  cat: WeakCategory,
  modelVersion: string,
): Promise<WeakBucketRow[]> => {
  const result = await runner.query<BucketRowRaw>(buildBucketSelectSql(), [modelVersion, cat]);
  return result.rows.map((raw) => normalizeBucketRow(cat, raw));
};

const safeDivide = (numerator: number, denominator: number): number =>
  denominator > 0 ? numerator / denominator : 0;

export const wilsonLowerBound = (successes: number, trials: number): number => {
  if (trials <= 0) return 0;
  const p = successes / trials;
  const z = WILSON_Z_SCORE;
  const denom = 1 + (z * z) / trials;
  const center = p + (z * z) / (2 * trials);
  const margin = z * Math.sqrt((p * (1 - p)) / trials + (z * z) / (4 * trials * trials));
  return Math.max(0, (center - margin) / denom);
};

const roundValue = (value: number): number => Math.round(value * ROUND_MULT) / ROUND_MULT;

export const computeBucketAccuracy = (row: WeakBucketRow): WeakBucketMetrics => ({
  top1: safeDivide(row.top1_hit_sum, row.race_count),
  place2: safeDivide(row.place2_hit_sum, row.race_count),
  place3: safeDivide(row.place3_hit_sum, row.race_count),
  top3_box: safeDivide(row.top3_box_hit_sum, row.race_count),
});

export const computeBucketWilsonLower = (row: WeakBucketRow): WeakBucketMetrics => ({
  top1: wilsonLowerBound(row.top1_hit_sum, row.race_count),
  place2: wilsonLowerBound(row.place2_hit_sum, row.race_count),
  place3: wilsonLowerBound(row.place3_hit_sum, row.race_count),
  top3_box: wilsonLowerBound(row.top3_box_hit_sum, row.race_count),
});

const sumMetric = (rows: WeakBucketRow[], key: keyof WeakBucketRow): number =>
  rows.reduce((acc, row) => acc + toNumber(row[key]), 0);

export const computeCategoryMean = (
  rows: WeakBucketRow[],
  cat: WeakCategory,
): CategoryAccuracySummary => {
  const totalRaces = sumMetric(rows, "race_count");
  return {
    cat,
    totalRaces,
    mean: {
      top1: safeDivide(sumMetric(rows, "top1_hit_sum"), totalRaces),
      place2: safeDivide(sumMetric(rows, "place2_hit_sum"), totalRaces),
      place3: safeDivide(sumMetric(rows, "place3_hit_sum"), totalRaces),
      top3_box: safeDivide(sumMetric(rows, "top3_box_hit_sum"), totalRaces),
    },
  };
};

const computeBucketGaps = (metrics: WeakBucketMetrics, mean: MetricSnapshot): WeakBucketGaps => ({
  top1: roundValue(mean.top1 - metrics.top1),
  place2: roundValue(mean.place2 - metrics.place2),
  place3: roundValue(mean.place3 - metrics.place3),
  top3_box: roundValue(mean.top3_box - metrics.top3_box),
});

const computeCompositeGap = (gaps: WeakBucketGaps): number =>
  roundValue((gaps.top1 + gaps.place2 + gaps.place3 + gaps.top3_box) / METRIC_KEYS.length);

const resolveDimValue = (row: WeakBucketRow, dim: WeakDim): string | null => {
  if (dim === "keibajo_code") return row.keibajo_code;
  if (dim === "kyori") return String(row.kyori);
  if (dim === "track_code") return row.track_code;
  if (dim === "grade_code") return row.grade_code;
  return row.condition_key;
};

interface DimAggregate {
  rows: WeakBucketRow[];
}

const groupBucketsByDim = (rows: WeakBucketRow[], dim: WeakDim): Map<string, DimAggregate> =>
  rows.reduce<Map<string, DimAggregate>>((acc, row) => {
    const value = resolveDimValue(row, dim);
    if (value === null) return acc;
    const existing = acc.get(value);
    if (existing === undefined) {
      acc.set(value, { rows: [row] });
      return acc;
    }
    existing.rows.push(row);
    return acc;
  }, new Map<string, DimAggregate>());

const aggregateDimRows = (
  rows: WeakBucketRow[],
  cat: WeakCategory,
  dim: WeakDim,
  value: string,
): WeakBucketRow => ({
  category: cat,
  source: cat,
  keibajo_code: dim === "keibajo_code" ? value : "",
  kyori: dim === "kyori" ? Number(value) : 0,
  kyoso_shubetsu_code: "",
  kyoso_joken_code: null,
  condition_key: dim === "condition_key" ? value : null,
  track_code: dim === "track_code" ? value : null,
  grade_code: dim === "grade_code" ? value : null,
  race_name: null,
  race_count: sumMetric(rows, "race_count"),
  top1_hit_sum: sumMetric(rows, "top1_hit_sum"),
  place2_hit_sum: sumMetric(rows, "place2_hit_sum"),
  place3_hit_sum: sumMetric(rows, "place3_hit_sum"),
  top3_box_hit_sum: sumMetric(rows, "top3_box_hit_sum"),
});

const buildEntryFromAggregate = (
  cat: WeakCategory,
  dim: WeakDim,
  value: string,
  aggregateRow: WeakBucketRow,
  mean: MetricSnapshot,
  sampleSizeThreshold: number,
): WeakBucketEntry => {
  const metrics = computeBucketAccuracy(aggregateRow);
  const wilson = computeBucketWilsonLower(aggregateRow);
  const gaps = computeBucketGaps(metrics, mean);
  return {
    cat,
    dim,
    value,
    metrics: {
      top1: roundValue(metrics.top1),
      place2: roundValue(metrics.place2),
      place3: roundValue(metrics.place3),
      top3_box: roundValue(metrics.top3_box),
    },
    wilson_lower: {
      top1: roundValue(wilson.top1),
      place2: roundValue(wilson.place2),
      place3: roundValue(wilson.place3),
      top3_box: roundValue(wilson.top3_box),
    },
    gaps,
    composite_gap: computeCompositeGap(gaps),
    race_count: aggregateRow.race_count,
    sample_size_warning: aggregateRow.race_count < sampleSizeThreshold,
  };
};

const computeEntriesForDim = (
  rows: WeakBucketRow[],
  cat: WeakCategory,
  dim: WeakDim,
  mean: MetricSnapshot,
  sampleSizeThreshold: number,
): WeakBucketEntry[] => {
  const grouped = groupBucketsByDim(rows, dim);
  const entries: WeakBucketEntry[] = [];
  grouped.forEach((agg, value) => {
    const aggregateRow = aggregateDimRows(agg.rows, cat, dim, value);
    entries.push(buildEntryFromAggregate(cat, dim, value, aggregateRow, mean, sampleSizeThreshold));
  });
  return entries;
};

const sortByCompositeGap = (a: WeakBucketEntry, b: WeakBucketEntry): number =>
  b.composite_gap - a.composite_gap;

const sortByMetricGap =
  (metric: keyof WeakBucketMetrics) =>
  (a: WeakBucketEntry, b: WeakBucketEntry): number =>
    b.gaps[metric] - a.gaps[metric];

export const buildTopRankingsByMetric = (
  entries: WeakBucketEntry[],
  topPerMetric: number,
): WeakBucketTopRanking[] =>
  METRIC_KEYS.map((metric) => ({
    metric,
    entries: entries.toSorted(sortByMetricGap(metric)).slice(0, topPerMetric),
  }));

export interface BuildWeakBucketsArgs {
  cat: WeakCategory;
  modelVersion: string;
  rows: WeakBucketRow[];
  sampleSizeThreshold: number;
}

export const buildWeakBucketsForCategory = (args: BuildWeakBucketsArgs): WeakBucketEntry[] => {
  const summary = computeCategoryMean(args.rows, args.cat);
  return DIM_LABELS.flatMap((dim) =>
    computeEntriesForDim(args.rows, args.cat, dim, summary.mean, args.sampleSizeThreshold),
  );
};

export interface BuildWeakBucketOutputArgs {
  options: IdentifyWeakBucketsCliOptions;
  jraRows: WeakBucketRow[];
  narRows: WeakBucketRow[];
}

export const buildWeakBucketOutput = (args: BuildWeakBucketOutputArgs): WeakBucketOutput => {
  const jraEntries = buildWeakBucketsForCategory({
    cat: "jra",
    modelVersion: args.options.modelVersionJra,
    rows: args.jraRows,
    sampleSizeThreshold: args.options.sampleSizeThreshold,
  });
  const narEntries = buildWeakBucketsForCategory({
    cat: "nar",
    modelVersion: args.options.modelVersionNar,
    rows: args.narRows,
    sampleSizeThreshold: args.options.sampleSizeThreshold,
  });
  const combined = [...jraEntries, ...narEntries].toSorted(sortByCompositeGap);
  return {
    schema_version: 1,
    model_version_jra: args.options.modelVersionJra,
    model_version_nar: args.options.modelVersionNar,
    buckets: combined,
    top_by_metric: buildTopRankingsByMetric(combined, args.options.topPerMetric),
  };
};

export interface RunIdentifyDeps {
  runner: BucketQueryRunner;
  writeOutput: (path: string, contents: string) => Promise<void>;
  log: (message: string) => void;
}

export const runIdentifyWeakBuckets = async (
  deps: RunIdentifyDeps,
  options: IdentifyWeakBucketsCliOptions,
): Promise<WeakBucketOutput> => {
  deps.log(`Fetching JRA bucket rows for ${options.modelVersionJra}`);
  const jraRows = await fetchBucketRows(deps.runner, "jra", options.modelVersionJra);
  deps.log(`Fetching NAR bucket rows for ${options.modelVersionNar}`);
  const narRows = await fetchBucketRows(deps.runner, "nar", options.modelVersionNar);
  const output = buildWeakBucketOutput({ options, jraRows, narRows });
  await deps.writeOutput(options.output, JSON.stringify(output, null, 2));
  deps.log(`Wrote ${output.buckets.length} weak buckets to ${options.output}`);
  return output;
};

const defaultLog = (message: string): void => {
  console.log(`[identify-weak-buckets] ${message}`);
};

const main = async (): Promise<void> => {
  const options = parseArgs(process.argv.slice(2));
  const pool = new Pool({ connectionString: options.pgUrl });
  try {
    await runIdentifyWeakBuckets(
      { runner: pool, writeOutput: writeFile, log: defaultLog },
      options,
    );
  } finally {
    await pool.end();
  }
};

if (import.meta.main) {
  main().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}

export {
  applyArg,
  computeCompositeGap,
  computeBucketGaps,
  computeEntriesForDim,
  groupBucketsByDim,
  resolveDimValue,
  roundValue,
  ROUND_DIGITS,
  safeDivide,
  toNumber,
};
