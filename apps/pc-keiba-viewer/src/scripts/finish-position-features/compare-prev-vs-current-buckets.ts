// Run with: bun run src/scripts/finish-position-features/compare-prev-vs-current-buckets.ts \
//   --prev-model-version X --curr-model-version Y \
//   --output-csv tmp/v8/delta.csv --output-md docs/finish-position-accuracy/buckets/iter{N}-delta.md
//
// Stage 0B helper for v8 iterative loop plan. Reads model_prediction_bucket_evaluations
// for two model versions (prev + curr), computes per-bucket delta for 4 metrics
// (top1, place2, place3, top3_box) plus a normalized composite delta and Wilson
// LB composite delta, then writes both a wide CSV report and a grouped Markdown
// table. The CSV is sorted by composite_delta_normalized descending so the
// strongest improvements appear first; the Markdown table groups by category +
// dim with [+]/[-]/[~] markers (no emoji per project rules).

import { writeFile } from "node:fs/promises";

import { Pool } from "pg";

export type CompareCategory = "jra" | "nar";
export type CompareDim = "keibajo_code" | "kyori" | "track_code" | "grade_code" | "condition_key";

export interface CompareBucketRow {
  category: CompareCategory;
  dim: CompareDim;
  value: string;
  race_count: number;
  top1: number;
  place2: number;
  place3: number;
  top3_box: number;
  top1_wilson_lower: number;
  place2_wilson_lower: number;
  place3_wilson_lower: number;
  top3_box_wilson_lower: number;
}

export interface CompareBucketDelta {
  category: CompareCategory;
  dim: CompareDim;
  value: string;
  race_count: number;
  top1_prev: number;
  top1_curr: number;
  top1_delta: number;
  place2_prev: number;
  place2_curr: number;
  place2_delta: number;
  place3_prev: number;
  place3_curr: number;
  place3_delta: number;
  top3_box_prev: number;
  top3_box_curr: number;
  top3_box_delta: number;
  composite_delta_normalized: number;
  wilson_lower_delta_composite: number;
}

export interface CompareCliOptions {
  pgUrl: string;
  prevModelVersion: string;
  currModelVersion: string;
  outputCsv: string;
  outputMd: string;
}

export interface CompareBucketQueryResult<Row> {
  rows: Row[];
}

export interface CompareBucketQueryRunner {
  query: <Row>(sql: string, params?: unknown[]) => Promise<CompareBucketQueryResult<Row>>;
}

interface BucketRawRow {
  source: string;
  keibajo_code: string;
  kyori: string | number;
  kyoso_joken_code: string | null;
  condition_key: string | null;
  track_code: string | null;
  grade_code: string | null;
  race_count: string | number;
  top1_hit_sum: string | number;
  place2_hit_sum: string | number;
  place3_hit_sum: string | number;
  top3_box_hit_sum: string | number;
}

interface ApplyArgResult {
  advanceBy: number;
}

const DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing";
const BUCKET_TABLE = "model_prediction_bucket_evaluations";
const WILSON_Z_SCORE = 1.96;
const METRIC_KEY_COUNT = 4;
const ROUND_MULT = 1_000_000;
const DIM_LABELS = [
  "keibajo_code",
  "kyori",
  "track_code",
  "grade_code",
  "condition_key",
] satisfies CompareDim[];
const CSV_HEADER = [
  "category",
  "dim",
  "value",
  "n",
  "top1_prev",
  "top1_curr",
  "top1_delta",
  "place2_prev",
  "place2_curr",
  "place2_delta",
  "place3_prev",
  "place3_curr",
  "place3_delta",
  "top3_box_prev",
  "top3_box_curr",
  "top3_box_delta",
  "composite_delta_normalized",
  "wilson_lower_delta_composite",
].join(",");
const MARKER_IMPROVE = "[+]";
const MARKER_WORSEN = "[-]";
const MARKER_FLAT = "[~]";
const MARKDOWN_FLAT_THRESHOLD = 0.0005;

export const buildUsageText = (): string =>
  [
    "Usage:",
    "  bun run src/scripts/finish-position-features/compare-prev-vs-current-buckets.ts \\",
    "    --prev-model-version <prev> \\",
    "    --curr-model-version <curr> \\",
    "    --output-csv <csv-path> \\",
    "    --output-md <md-path> \\",
    "    [--pg-url <connection-string>]",
  ].join("\n");

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value.`);
  return value;
};

export const initialOptions = (): CompareCliOptions => ({
  pgUrl: process.env.DATABASE_URL_LOCAL ?? DEFAULT_PG_URL,
  prevModelVersion: "",
  currModelVersion: "",
  outputCsv: "",
  outputMd: "",
});

const applyArg = (
  options: CompareCliOptions,
  name: string,
  value: string | undefined,
): ApplyArgResult => {
  if (name === "--pg-url") {
    options.pgUrl = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--prev-model-version") {
    options.prevModelVersion = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--curr-model-version") {
    options.currModelVersion = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--output-csv") {
    options.outputCsv = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--output-md") {
    options.outputMd = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--help" || name === "-h") {
    console.log(buildUsageText());
    process.exit(0);
  }
  throw new Error(`Unknown argument: ${name}`);
};

const consumeArgs = (
  options: CompareCliOptions,
  argv: readonly string[],
  cursor: number,
): CompareCliOptions => {
  const name = argv[cursor];
  if (name === undefined) return options;
  const { advanceBy } = applyArg(options, name, argv[cursor + 1]);
  return consumeArgs(options, argv, cursor + advanceBy);
};

export const parseArgs = (argv: readonly string[]): CompareCliOptions => {
  const options = consumeArgs(initialOptions(), argv, 0);
  if (options.prevModelVersion === "") throw new Error("--prev-model-version is required.");
  if (options.currModelVersion === "") throw new Error("--curr-model-version is required.");
  if (options.outputCsv === "") throw new Error("--output-csv is required.");
  if (options.outputMd === "") throw new Error("--output-md is required.");
  return options;
};

export const buildCompareSelectSql = (): string => `
    select
      source,
      keibajo_code,
      kyori,
      kyoso_joken_code,
      condition_key,
      track_code,
      grade_code,
      sum(race_count)::numeric as race_count,
      sum(top1_hit_sum)::numeric as top1_hit_sum,
      sum(place2_hit_sum)::numeric as place2_hit_sum,
      sum(place3_hit_sum)::numeric as place3_hit_sum,
      sum(top3_box_hit_sum)::numeric as top3_box_hit_sum
    from ${BUCKET_TABLE}
    where model_version = $1 and category = $2
    group by source, keibajo_code, kyori, kyoso_joken_code, condition_key, track_code, grade_code
  `;

const toNumber = (raw: unknown): number => {
  if (raw === null || raw === undefined) return 0;
  if (typeof raw === "number") return raw;
  return Number(raw);
};

const roundValue = (value: number): number => Math.round(value * ROUND_MULT) / ROUND_MULT;

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

const resolveDimValue = (raw: BucketRawRow, dim: CompareDim): string | null => {
  if (dim === "keibajo_code") return raw.keibajo_code;
  if (dim === "kyori") return String(toNumber(raw.kyori));
  if (dim === "track_code") return raw.track_code;
  if (dim === "grade_code") return raw.grade_code;
  return raw.condition_key;
};

interface DimAccumulatorValue {
  race_count: number;
  top1_hit_sum: number;
  place2_hit_sum: number;
  place3_hit_sum: number;
  top3_box_hit_sum: number;
}

const addRowToAccumulator = (acc: DimAccumulatorValue, raw: BucketRawRow): DimAccumulatorValue => ({
  race_count: acc.race_count + toNumber(raw.race_count),
  top1_hit_sum: acc.top1_hit_sum + toNumber(raw.top1_hit_sum),
  place2_hit_sum: acc.place2_hit_sum + toNumber(raw.place2_hit_sum),
  place3_hit_sum: acc.place3_hit_sum + toNumber(raw.place3_hit_sum),
  top3_box_hit_sum: acc.top3_box_hit_sum + toNumber(raw.top3_box_hit_sum),
});

const buildEmptyAccumulator = (): DimAccumulatorValue => ({
  race_count: 0,
  top1_hit_sum: 0,
  place2_hit_sum: 0,
  place3_hit_sum: 0,
  top3_box_hit_sum: 0,
});

const aggregateByDim = (
  rawRows: BucketRawRow[],
  dim: CompareDim,
): Map<string, DimAccumulatorValue> =>
  rawRows.reduce<Map<string, DimAccumulatorValue>>((acc, raw) => {
    const value = resolveDimValue(raw, dim);
    if (value === null) return acc;
    const existing = acc.get(value) ?? buildEmptyAccumulator();
    acc.set(value, addRowToAccumulator(existing, raw));
    return acc;
  }, new Map<string, DimAccumulatorValue>());

const buildCompareRowFromAcc = (
  cat: CompareCategory,
  dim: CompareDim,
  value: string,
  acc: DimAccumulatorValue,
): CompareBucketRow => ({
  category: cat,
  dim,
  value,
  race_count: acc.race_count,
  top1: roundValue(safeDivide(acc.top1_hit_sum, acc.race_count)),
  place2: roundValue(safeDivide(acc.place2_hit_sum, acc.race_count)),
  place3: roundValue(safeDivide(acc.place3_hit_sum, acc.race_count)),
  top3_box: roundValue(safeDivide(acc.top3_box_hit_sum, acc.race_count)),
  top1_wilson_lower: roundValue(wilsonLowerBound(acc.top1_hit_sum, acc.race_count)),
  place2_wilson_lower: roundValue(wilsonLowerBound(acc.place2_hit_sum, acc.race_count)),
  place3_wilson_lower: roundValue(wilsonLowerBound(acc.place3_hit_sum, acc.race_count)),
  top3_box_wilson_lower: roundValue(wilsonLowerBound(acc.top3_box_hit_sum, acc.race_count)),
});

export const buildBucketRowsForCategory = (
  rawRows: BucketRawRow[],
  cat: CompareCategory,
): CompareBucketRow[] =>
  DIM_LABELS.flatMap((dim) => {
    const grouped = aggregateByDim(rawRows, dim);
    const entries: CompareBucketRow[] = [];
    grouped.forEach((acc, value) => {
      entries.push(buildCompareRowFromAcc(cat, dim, value, acc));
    });
    return entries;
  });

export const fetchCompareRows = async (
  runner: CompareBucketQueryRunner,
  cat: CompareCategory,
  modelVersion: string,
): Promise<CompareBucketRow[]> => {
  const result = await runner.query<BucketRawRow>(buildCompareSelectSql(), [modelVersion, cat]);
  return buildBucketRowsForCategory(result.rows, cat);
};

const buildBucketKey = (row: CompareBucketRow): string => `${row.category}/${row.dim}/${row.value}`;

const computeCompositeDelta = (deltas: number[]): number =>
  roundValue(deltas.reduce((sum, d) => sum + d, 0) / METRIC_KEY_COUNT);

const buildDeltaRow = (prev: CompareBucketRow, curr: CompareBucketRow): CompareBucketDelta => {
  const top1Delta = roundValue(curr.top1 - prev.top1);
  const place2Delta = roundValue(curr.place2 - prev.place2);
  const place3Delta = roundValue(curr.place3 - prev.place3);
  const top3BoxDelta = roundValue(curr.top3_box - prev.top3_box);
  const wilsonDeltaTop1 = curr.top1_wilson_lower - prev.top1_wilson_lower;
  const wilsonDeltaPlace2 = curr.place2_wilson_lower - prev.place2_wilson_lower;
  const wilsonDeltaPlace3 = curr.place3_wilson_lower - prev.place3_wilson_lower;
  const wilsonDeltaTop3Box = curr.top3_box_wilson_lower - prev.top3_box_wilson_lower;
  return {
    category: curr.category,
    dim: curr.dim,
    value: curr.value,
    race_count: curr.race_count,
    top1_prev: prev.top1,
    top1_curr: curr.top1,
    top1_delta: top1Delta,
    place2_prev: prev.place2,
    place2_curr: curr.place2,
    place2_delta: place2Delta,
    place3_prev: prev.place3,
    place3_curr: curr.place3,
    place3_delta: place3Delta,
    top3_box_prev: prev.top3_box,
    top3_box_curr: curr.top3_box,
    top3_box_delta: top3BoxDelta,
    composite_delta_normalized: computeCompositeDelta([
      top1Delta,
      place2Delta,
      place3Delta,
      top3BoxDelta,
    ]),
    wilson_lower_delta_composite: computeCompositeDelta([
      wilsonDeltaTop1,
      wilsonDeltaPlace2,
      wilsonDeltaPlace3,
      wilsonDeltaTop3Box,
    ]),
  };
};

const sortByCompositeDeltaDesc = (a: CompareBucketDelta, b: CompareBucketDelta): number =>
  b.composite_delta_normalized - a.composite_delta_normalized;

export interface BuildDeltasArgs {
  prevRows: CompareBucketRow[];
  currRows: CompareBucketRow[];
}

export const buildBucketDeltas = (args: BuildDeltasArgs): CompareBucketDelta[] => {
  const prevByKey = new Map<string, CompareBucketRow>(
    args.prevRows.map((row) => [buildBucketKey(row), row]),
  );
  const matched = args.currRows.flatMap((curr) => {
    const prev = prevByKey.get(buildBucketKey(curr));
    return prev === undefined ? [] : [buildDeltaRow(prev, curr)];
  });
  return matched.toSorted(sortByCompositeDeltaDesc);
};

const formatCsvCell = (value: string | number): string => {
  if (typeof value === "number") return String(value);
  if (value.includes(",") || value.includes('"')) return `"${value.replaceAll('"', '""')}"`;
  return value;
};

export const buildCsvLine = (delta: CompareBucketDelta): string =>
  [
    delta.category,
    delta.dim,
    delta.value,
    delta.race_count,
    delta.top1_prev,
    delta.top1_curr,
    delta.top1_delta,
    delta.place2_prev,
    delta.place2_curr,
    delta.place2_delta,
    delta.place3_prev,
    delta.place3_curr,
    delta.place3_delta,
    delta.top3_box_prev,
    delta.top3_box_curr,
    delta.top3_box_delta,
    delta.composite_delta_normalized,
    delta.wilson_lower_delta_composite,
  ]
    .map(formatCsvCell)
    .join(",");

export const buildCsvBody = (deltas: CompareBucketDelta[]): string =>
  [CSV_HEADER, ...deltas.map(buildCsvLine)].join("\n");

const resolveMarker = (delta: number): string => {
  if (Math.abs(delta) < MARKDOWN_FLAT_THRESHOLD) return MARKER_FLAT;
  return delta > 0 ? MARKER_IMPROVE : MARKER_WORSEN;
};

const formatMarkdownDelta = (delta: number): string =>
  `${resolveMarker(delta)} ${delta.toFixed(4)}`;

interface MarkdownGroupKey {
  category: CompareCategory;
  dim: CompareDim;
}

const buildGroupKey = (delta: CompareBucketDelta): string => `${delta.category}/${delta.dim}`;

const collectGroupKeys = (deltas: CompareBucketDelta[]): MarkdownGroupKey[] => {
  const seen = new Set<string>();
  const keys: MarkdownGroupKey[] = [];
  deltas.forEach((delta) => {
    const key = buildGroupKey(delta);
    if (seen.has(key)) return;
    seen.add(key);
    keys.push({ category: delta.category, dim: delta.dim });
  });
  return keys;
};

const filterGroupDeltas = (
  deltas: CompareBucketDelta[],
  group: MarkdownGroupKey,
): CompareBucketDelta[] =>
  deltas.filter((delta) => delta.category === group.category && delta.dim === group.dim);

const buildMarkdownRow = (delta: CompareBucketDelta): string =>
  [
    `| ${delta.value}`,
    String(delta.race_count),
    formatMarkdownDelta(delta.top1_delta),
    formatMarkdownDelta(delta.place2_delta),
    formatMarkdownDelta(delta.place3_delta),
    formatMarkdownDelta(delta.top3_box_delta),
    formatMarkdownDelta(delta.composite_delta_normalized),
    formatMarkdownDelta(delta.wilson_lower_delta_composite),
  ].join(" | ") + " |";

const MARKDOWN_HEADER_ROW =
  "| value | n | top1 delta | place2 delta | place3 delta | top3_box delta | composite delta | wilson LB delta |";
const MARKDOWN_SEPARATOR_ROW = "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |";

const buildGroupSection = (deltas: CompareBucketDelta[], group: MarkdownGroupKey): string => {
  const rows = filterGroupDeltas(deltas, group);
  const header = `### ${group.category} / ${group.dim}`;
  const tableBody = rows.map(buildMarkdownRow).join("\n");
  return [header, "", MARKDOWN_HEADER_ROW, MARKDOWN_SEPARATOR_ROW, tableBody].join("\n");
};

export interface BuildMarkdownArgs {
  options: CompareCliOptions;
  deltas: CompareBucketDelta[];
}

export const buildMarkdownReport = (args: BuildMarkdownArgs): string => {
  const groups = collectGroupKeys(args.deltas);
  const headerBlock = [
    `# Bucket delta: ${args.options.prevModelVersion} -> ${args.options.currModelVersion}`,
    "",
    `Total buckets matched: ${args.deltas.length}`,
    "",
  ].join("\n");
  const sections = groups.map((group) => buildGroupSection(args.deltas, group)).join("\n\n");
  return [headerBlock, sections, ""].join("\n");
};

export interface CompareRunDeps {
  runner: CompareBucketQueryRunner;
  writeFile: (path: string, contents: string) => Promise<void>;
  log: (message: string) => void;
}

export interface CompareRunResult {
  deltas: CompareBucketDelta[];
  csv: string;
  markdown: string;
}

export const runComparePrevVsCurrent = async (
  deps: CompareRunDeps,
  options: CompareCliOptions,
): Promise<CompareRunResult> => {
  const prevJra = await fetchCompareRows(deps.runner, "jra", options.prevModelVersion);
  const currJra = await fetchCompareRows(deps.runner, "jra", options.currModelVersion);
  const prevNar = await fetchCompareRows(deps.runner, "nar", options.prevModelVersion);
  const currNar = await fetchCompareRows(deps.runner, "nar", options.currModelVersion);
  const deltas = [
    ...buildBucketDeltas({ prevRows: prevJra, currRows: currJra }),
    ...buildBucketDeltas({ prevRows: prevNar, currRows: currNar }),
  ].toSorted(sortByCompositeDeltaDesc);
  const csv = buildCsvBody(deltas);
  const markdown = buildMarkdownReport({ options, deltas });
  await deps.writeFile(options.outputCsv, csv);
  await deps.writeFile(options.outputMd, markdown);
  deps.log(`Wrote ${deltas.length} delta rows to ${options.outputCsv} and ${options.outputMd}`);
  return { deltas, csv, markdown };
};

const defaultLog = (message: string): void => {
  console.log(`[compare-prev-vs-current-buckets] ${message}`);
};

const main = async (): Promise<void> => {
  const options = parseArgs(process.argv.slice(2));
  const pool = new Pool({ connectionString: options.pgUrl });
  try {
    await runComparePrevVsCurrent({ runner: pool, writeFile, log: defaultLog }, options);
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

export { applyArg, buildBucketKey, resolveMarker, safeDivide, toNumber };
