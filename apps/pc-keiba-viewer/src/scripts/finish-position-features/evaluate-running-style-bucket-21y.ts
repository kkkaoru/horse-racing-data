// Run with: bun run src/scripts/finish-position-features/evaluate-running-style-bucket-21y.ts \
//   --pg-url $DATABASE_URL_LOCAL --running-style-feature-version v1 \
//   --model-version-jra <jra-model> --model-version-nar <nar-model>

import { Pool } from "pg";

import { createBucketEvalRpcClient } from "./bucket-eval-rpc-client";
import type { BucketEvalRpcChildLike } from "./bucket-eval-rpc-client";
import {
  buildRunningStyleAnalyzeSqls,
  buildRunningStyleBucketAggregateSql,
  buildRunningStyleBucketBatchUpsertSql,
  buildRunningStyleBucketEvaluationsDdl,
} from "./evaluate-running-style-bucket-sql";
import { runVoidTasksWithConcurrencyLimit } from "./generate-running-style-local";

export type RunningStyleBucketCategory = "jra" | "nar";

export interface RunningStyleBucketEvalCliOptions {
  pgUrl: string;
  runningStyleFeatureVersion: string;
  modelVersionJra: string;
  modelVersionNar: string;
  maxYearsPerRun: number;
  statementTimeoutMs: number;
  ignoreNightWindow: boolean;
  perYearSleepMs: number;
  perCategorySleepMs: number;
  minColimaCpu: number;
  minColimaMemoryGb: number;
  predictionsRoot: string;
  categoryFilter: RunningStyleBucketCategory | null;
  chunkConcurrency: number;
}

export interface RunningStyleCategoryYearWindow {
  category: RunningStyleBucketCategory;
  years: number[];
}

export interface RunningStyleAggregateRow {
  source: string;
  keibajo_code: string;
  kyori: number;
  kyoso_shubetsu_code: string;
  kyoso_joken_code: string | null;
  condition_key: string | null;
  track_code: string | null;
  grade_code: string | null;
  race_name: string | null;
  race_count: string | number;
  prediction_count: string | number;
  cm_actual_nige_pred_nige_count: string | number;
  cm_actual_nige_pred_senkou_count: string | number;
  cm_actual_nige_pred_sashi_count: string | number;
  cm_actual_nige_pred_oikomi_count: string | number;
  cm_actual_senkou_pred_nige_count: string | number;
  cm_actual_senkou_pred_senkou_count: string | number;
  cm_actual_senkou_pred_sashi_count: string | number;
  cm_actual_senkou_pred_oikomi_count: string | number;
  cm_actual_sashi_pred_nige_count: string | number;
  cm_actual_sashi_pred_senkou_count: string | number;
  cm_actual_sashi_pred_sashi_count: string | number;
  cm_actual_sashi_pred_oikomi_count: string | number;
  cm_actual_oikomi_pred_nige_count: string | number;
  cm_actual_oikomi_pred_senkou_count: string | number;
  cm_actual_oikomi_pred_sashi_count: string | number;
  cm_actual_oikomi_pred_oikomi_count: string | number;
  log_loss_nige_sum: string;
  log_loss_nige_count: string | number;
  log_loss_senkou_sum: string;
  log_loss_senkou_count: string | number;
  log_loss_sashi_sum: string;
  log_loss_sashi_count: string | number;
  log_loss_oikomi_sum: string;
  log_loss_oikomi_count: string | number;
  top2_hit_count: string | number;
}

export interface ColimaResources {
  cpu: number;
  memory: number;
}

export interface NightWindowInput {
  hourJst: number;
  ignoreNightWindow: boolean;
}

export interface RunningStyleBucketQueryResult<Row> {
  rows: Row[];
}

export interface RunningStyleBucketQueryRunner {
  query: <Row>(sql: string, params?: unknown[]) => Promise<RunningStyleBucketQueryResult<Row>>;
}

export interface RunningStyleBucketChunkLoaderArgs {
  category: string;
  yearFrom: number;
  yearTo: number;
  predictionsRoot: string;
  pgUrl: string;
  runningStyleFeatureVersion: string;
  modelVersion: string;
}

export interface RunningStyleBucketChunkClient {
  runner: RunningStyleBucketQueryRunner;
  close: () => Promise<void>;
  loadedRows: number;
}

export interface RunRunningStyleBucketEvalDeps {
  pool: RunningStyleBucketQueryRunner;
  openChunkClient: (
    args: RunningStyleBucketChunkLoaderArgs,
  ) => Promise<RunningStyleBucketChunkClient>;
  sleep: (ms: number) => Promise<void>;
  log: (message: string) => void;
}

export interface RunRunningStyleBucketEvalRequest {
  options: RunningStyleBucketEvalCliOptions;
  windows: RunningStyleCategoryYearWindow[];
}

export interface RunningStyleProcessYearArgs {
  runner: RunningStyleBucketQueryRunner;
  options: RunningStyleBucketEvalCliOptions;
  category: string;
  year: number;
  log: (m: string) => void;
}

interface RunningStyleUpsertContext {
  options: RunningStyleBucketEvalCliOptions;
  category: string;
  fromDate: string;
  toDate: string;
}

interface RunningStyleBucketEvalAccumulator {
  totalRows: number;
  totalRaces: number;
}

const NIGHT_WINDOW_HOURS_JST = new Set<number>([23, 0, 1, 2, 3, 4]);
const DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing";
const DEFAULT_MAX_YEARS_PER_RUN = 5;
const DEFAULT_STATEMENT_TIMEOUT_MS = 900_000;
const DEFAULT_PER_YEAR_SLEEP_MS = 2_000;
const DEFAULT_PER_CATEGORY_SLEEP_MS = 5_000;
const DEFAULT_MIN_COLIMA_CPU = 8;
const DEFAULT_MIN_COLIMA_MEMORY_GB = 24;
const DEFAULT_CHUNK_CONCURRENCY = 1;
const MIN_CHUNK_CONCURRENCY = 1;
const MAX_CHUNK_CONCURRENCY = 10;
// PG bind-parameter cap is 65535. We have 41 columns per row, so 100 rows
// emit 4100 placeholders — well within budget while reducing round-trips by
// 100x compared to the per-row UPSERT path.
const UPSERT_BATCH_SIZE = 100;
const DEFAULT_PREDICTIONS_ROOT =
  "apps/pc-keiba-viewer/tmp/bucket-eval/running-style/v1/predictions";
const PYTHON_LOADER_SCRIPT = "apps/pc-keiba-viewer/src/scripts/load_running_style_predictions.py";
const PYTHON_LOADER_TEMP_TABLE = "bucket_running_style_predictions_loaded";
const PYTHON_LOADER_UV_PROJECT = "apps/pc-keiba-viewer";
const JRA_YEARS = [
  2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021,
  2022, 2023, 2024, 2025, 2026,
];
const NAR_YEARS = [
  2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2026,
];

export const CATEGORY_YEAR_WINDOWS: RunningStyleCategoryYearWindow[] = [
  { category: "jra", years: JRA_YEARS },
  { category: "nar", years: NAR_YEARS },
] satisfies RunningStyleCategoryYearWindow[];

export const buildUsageText = (): string =>
  [
    "Usage:",
    "  bun run src/scripts/finish-position-features/evaluate-running-style-bucket-21y.ts \\",
    "    --pg-url <connection-string> \\",
    "    --running-style-feature-version v1 \\",
    "    --model-version-jra <jra-model> \\",
    "    --model-version-nar <nar-model> \\",
    "    [--max-years-per-run 5] \\",
    "    [--statement-timeout-ms 900000] \\",
    "    [--ignore-night-window] \\",
    "    [--category jra|nar] \\",
    "    [--chunk-concurrency 1]",
  ].join("\n");

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value.`);
  return value;
};

export const initialOptions = (): RunningStyleBucketEvalCliOptions => ({
  pgUrl: process.env.DATABASE_URL_LOCAL ?? DEFAULT_PG_URL,
  runningStyleFeatureVersion: "",
  modelVersionJra: "",
  modelVersionNar: "",
  maxYearsPerRun: DEFAULT_MAX_YEARS_PER_RUN,
  statementTimeoutMs: DEFAULT_STATEMENT_TIMEOUT_MS,
  ignoreNightWindow: false,
  perYearSleepMs: DEFAULT_PER_YEAR_SLEEP_MS,
  perCategorySleepMs: DEFAULT_PER_CATEGORY_SLEEP_MS,
  minColimaCpu: DEFAULT_MIN_COLIMA_CPU,
  minColimaMemoryGb: DEFAULT_MIN_COLIMA_MEMORY_GB,
  predictionsRoot: DEFAULT_PREDICTIONS_ROOT,
  categoryFilter: null,
  chunkConcurrency: DEFAULT_CHUNK_CONCURRENCY,
});

export const parseChunkConcurrency = (raw: string): number => {
  const value = Number.parseInt(raw, 10);
  if (!Number.isFinite(value)) {
    throw new Error(`--chunk-concurrency must be an integer (got: ${raw})`);
  }
  if (value < MIN_CHUNK_CONCURRENCY || value > MAX_CHUNK_CONCURRENCY) {
    throw new Error(
      `--chunk-concurrency must be between ${MIN_CHUNK_CONCURRENCY} and ${MAX_CHUNK_CONCURRENCY} (got: ${raw})`,
    );
  }
  return value;
};

export const parseCategoryFilter = (value: string): RunningStyleBucketCategory => {
  if (value === "jra") return "jra";
  if (value === "nar") return "nar";
  throw new Error(`--category must be one of jra, nar (got: ${value})`);
};

interface ApplyArgResult {
  advanceBy: number;
}

export const applyArg = (
  options: RunningStyleBucketEvalCliOptions,
  name: string,
  value: string | undefined,
): ApplyArgResult => {
  if (name === "--pg-url") {
    options.pgUrl = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--running-style-feature-version") {
    options.runningStyleFeatureVersion = requireValue(name, value);
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
  if (name === "--max-years-per-run") {
    options.maxYearsPerRun = Number.parseInt(requireValue(name, value), 10);
    return { advanceBy: 2 };
  }
  if (name === "--statement-timeout-ms") {
    options.statementTimeoutMs = Number.parseInt(requireValue(name, value), 10);
    return { advanceBy: 2 };
  }
  if (name === "--predictions-root") {
    options.predictionsRoot = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--ignore-night-window") {
    options.ignoreNightWindow = true;
    return { advanceBy: 1 };
  }
  if (name === "--category") {
    options.categoryFilter = parseCategoryFilter(requireValue(name, value));
    return { advanceBy: 2 };
  }
  if (name === "--chunk-concurrency") {
    options.chunkConcurrency = parseChunkConcurrency(requireValue(name, value));
    return { advanceBy: 2 };
  }
  if (name === "--help" || name === "-h") {
    console.log(buildUsageText());
    process.exit(0);
  }
  throw new Error(`Unknown argument: ${name}`);
};

const consumeArgs = (
  options: RunningStyleBucketEvalCliOptions,
  argv: readonly string[],
  cursor: number,
): RunningStyleBucketEvalCliOptions => {
  const name = argv[cursor];
  if (name === undefined) return options;
  const { advanceBy } = applyArg(options, name, argv[cursor + 1]);
  return consumeArgs(options, argv, cursor + advanceBy);
};

export const parseArgs = (argv: readonly string[]): RunningStyleBucketEvalCliOptions => {
  const options = consumeArgs(initialOptions(), argv, 0);
  if (options.runningStyleFeatureVersion === "") {
    throw new Error("--running-style-feature-version is required.");
  }
  if (options.modelVersionJra === "") {
    throw new Error("--model-version-jra is required.");
  }
  if (options.modelVersionNar === "") {
    throw new Error("--model-version-nar is required.");
  }
  return options;
};

export const isWithinNightWindow = (input: NightWindowInput): boolean => {
  if (input.ignoreNightWindow) return true;
  return NIGHT_WINDOW_HOURS_JST.has(input.hourJst);
};

export const ensureColimaCapacity = (
  resources: ColimaResources,
  minCpu: number,
  minMemoryGb: number,
): void => {
  if (resources.cpu < minCpu) {
    throw new Error(
      `Colima CPU is below required minimum: ${resources.cpu} < ${minCpu}. Run 'colima start --cpu ${minCpu} --memory ${minMemoryGb}'.`,
    );
  }
  if (resources.memory < minMemoryGb) {
    throw new Error(
      `Colima memory is below required minimum: ${resources.memory}GB < ${minMemoryGb}GB. Run 'colima start --cpu ${minCpu} --memory ${minMemoryGb}'.`,
    );
  }
};

const parseColimaMemoryBytes = (raw: unknown): number => {
  const num = typeof raw === "number" ? raw : Number(raw);
  if (!Number.isFinite(num)) return 0;
  return num;
};

const isStringObject = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object";

export const parseColimaStatusJson = (raw: string): ColimaResources => {
  const parsed: unknown = JSON.parse(raw);
  if (!isStringObject(parsed)) {
    throw new Error("Colima status JSON is not an object.");
  }
  const cpu = typeof parsed["cpu"] === "number" ? parsed["cpu"] : Number(parsed["cpu"] ?? 0);
  const memoryRaw = parsed["memory"] ?? 0;
  const memoryBytes = parseColimaMemoryBytes(memoryRaw);
  const memoryGb = memoryBytes > 1024 ? memoryBytes / 1024 / 1024 / 1024 : memoryBytes;
  return { cpu, memory: memoryGb };
};

export const buildYearDateWindow = (year: number): { fromDate: string; toDate: string } => ({
  fromDate: `${year}0101`,
  toDate: `${year}1231`,
});

const chunkYearsRecursive = (
  years: number[],
  chunkSize: number,
  index: number,
  acc: number[][],
): number[][] => {
  if (index >= years.length) return acc;
  acc.push(years.slice(index, index + chunkSize));
  return chunkYearsRecursive(years, chunkSize, index + chunkSize, acc);
};

export const chunkYears = (years: number[], chunkSize: number): number[][] => {
  if (chunkSize <= 0) {
    throw new Error("chunkSize must be greater than zero.");
  }
  return chunkYearsRecursive(years, chunkSize, 0, []);
};

export const buildSessionStatementSql = (statementTimeoutMs: number): string =>
  `set local statement_timeout = '${statementTimeoutMs}ms'`;

export const buildSessionWorkMemSql = (): string => "set local work_mem = '256MB'";

export const buildSessionIdleTimeoutSql = (statementTimeoutMs: number): string =>
  `set local idle_in_transaction_session_timeout = '${statementTimeoutMs}ms'`;

export const buildSessionTuningSqls = (statementTimeoutMs: number): string[] => [
  buildSessionStatementSql(statementTimeoutMs),
  buildSessionIdleTimeoutSql(statementTimeoutMs),
  buildSessionWorkMemSql(),
];

export const resolveModelVersion = (
  options: RunningStyleBucketEvalCliOptions,
  category: string,
): string => (category === "jra" ? options.modelVersionJra : options.modelVersionNar);

export const numericFromRow = (raw: string | number | null): string => {
  if (raw === null) return "0";
  if (typeof raw === "number") return raw.toString();
  return raw;
};

export const buildUpsertParams = (
  context: RunningStyleUpsertContext,
  row: RunningStyleAggregateRow,
): unknown[] => {
  const modelVersion = resolveModelVersion(context.options, context.category);
  return [
    modelVersion,
    context.options.runningStyleFeatureVersion,
    context.category,
    context.fromDate,
    context.toDate,
    row.source,
    row.keibajo_code,
    row.kyori,
    row.kyoso_shubetsu_code,
    row.kyoso_joken_code,
    row.condition_key,
    row.track_code,
    row.grade_code,
    row.race_name,
    numericFromRow(row.race_count),
    numericFromRow(row.prediction_count),
    numericFromRow(row.cm_actual_nige_pred_nige_count),
    numericFromRow(row.cm_actual_nige_pred_senkou_count),
    numericFromRow(row.cm_actual_nige_pred_sashi_count),
    numericFromRow(row.cm_actual_nige_pred_oikomi_count),
    numericFromRow(row.cm_actual_senkou_pred_nige_count),
    numericFromRow(row.cm_actual_senkou_pred_senkou_count),
    numericFromRow(row.cm_actual_senkou_pred_sashi_count),
    numericFromRow(row.cm_actual_senkou_pred_oikomi_count),
    numericFromRow(row.cm_actual_sashi_pred_nige_count),
    numericFromRow(row.cm_actual_sashi_pred_senkou_count),
    numericFromRow(row.cm_actual_sashi_pred_sashi_count),
    numericFromRow(row.cm_actual_sashi_pred_oikomi_count),
    numericFromRow(row.cm_actual_oikomi_pred_nige_count),
    numericFromRow(row.cm_actual_oikomi_pred_senkou_count),
    numericFromRow(row.cm_actual_oikomi_pred_sashi_count),
    numericFromRow(row.cm_actual_oikomi_pred_oikomi_count),
    row.log_loss_nige_sum,
    numericFromRow(row.log_loss_nige_count),
    row.log_loss_senkou_sum,
    numericFromRow(row.log_loss_senkou_count),
    row.log_loss_sashi_sum,
    numericFromRow(row.log_loss_sashi_count),
    row.log_loss_oikomi_sum,
    numericFromRow(row.log_loss_oikomi_count),
    numericFromRow(row.top2_hit_count),
  ];
};

export const assertSupportedCategory = (category: string): void => {
  if (category !== "jra" && category !== "nar") {
    throw new Error(`Unsupported running-style category: ${category}`);
  }
};

const ensureBucketTable = async (
  pool: RunningStyleBucketQueryRunner,
  log: (m: string) => void,
): Promise<void> => {
  log("Creating running-style bucket evaluations DDL");
  await pool.query(buildRunningStyleBucketEvaluationsDdl());
};

const runStatementsSerially = (
  pool: RunningStyleBucketQueryRunner,
  statements: string[],
  onEach: (sql: string) => void,
): Promise<unknown> =>
  statements.reduce<Promise<unknown>>(
    (chain, sql) =>
      chain.then(() => {
        onEach(sql);
        return pool.query(sql);
      }),
    Promise.resolve(),
  );

const runAggregateForYear = async (
  runner: RunningStyleBucketQueryRunner,
  options: RunningStyleBucketEvalCliOptions,
  category: string,
  year: number,
): Promise<RunningStyleAggregateRow[]> => {
  const { fromDate, toDate } = buildYearDateWindow(year);
  const modelVersion = resolveModelVersion(options, category);
  const sql = buildRunningStyleBucketAggregateSql({
    modelVersion,
    category,
    fromDate,
    toDate,
    runningStyleFeatureVersion: options.runningStyleFeatureVersion,
  });
  const result = await runner.query<RunningStyleAggregateRow>(sql);
  return result.rows;
};

export const chunkRows = <Item>(rows: readonly Item[], batchSize: number): Item[][] => {
  if (batchSize <= 0) {
    throw new Error("batchSize must be greater than zero.");
  }
  const batchCount = Math.ceil(rows.length / batchSize);
  return Array.from({ length: batchCount }, (_, batchIndex) =>
    rows.slice(batchIndex * batchSize, batchIndex * batchSize + batchSize),
  );
};

const runBatchUpsert = (
  runner: RunningStyleBucketQueryRunner,
  context: RunningStyleUpsertContext,
  batch: readonly RunningStyleAggregateRow[],
): Promise<unknown> => {
  const sql = buildRunningStyleBucketBatchUpsertSql(batch.length);
  const params = batch.flatMap((row) => buildUpsertParams(context, row));
  return runner.query(sql, params);
};

const upsertRows = (
  runner: RunningStyleBucketQueryRunner,
  options: RunningStyleBucketEvalCliOptions,
  category: string,
  year: number,
  rows: RunningStyleAggregateRow[],
): Promise<unknown> => {
  const { fromDate, toDate } = buildYearDateWindow(year);
  const context: RunningStyleUpsertContext = { options, category, fromDate, toDate };
  const batches = chunkRows(rows, UPSERT_BATCH_SIZE);
  return batches.reduce<Promise<unknown>>(
    (chain, batch) => chain.then(() => runBatchUpsert(runner, context, batch)),
    Promise.resolve(),
  );
};

export const processYear = async (
  args: RunningStyleProcessYearArgs,
): Promise<{ rowCount: number; raceCount: number }> => {
  assertSupportedCategory(args.category);
  const rows = await runAggregateForYear(args.runner, args.options, args.category, args.year);
  const raceCount = rows.reduce((acc, row) => acc + Number(row.race_count), 0);
  if (raceCount === 0) {
    args.log(`Skip ${args.category} ${args.year}: race_count is zero`);
    return { rowCount: 0, raceCount: 0 };
  }
  await upsertRows(args.runner, args.options, args.category, args.year, rows);
  args.log(`Upserted ${rows.length} rows for ${args.category} ${args.year} (races=${raceCount})`);
  return { rowCount: rows.length, raceCount };
};

const runSessionTuning = (
  runner: RunningStyleBucketQueryRunner,
  options: RunningStyleBucketEvalCliOptions,
): Promise<unknown> =>
  runStatementsSerially(runner, buildSessionTuningSqls(options.statementTimeoutMs), () => {});

const runAnalyzes = (
  pool: RunningStyleBucketQueryRunner,
  log: (m: string) => void,
): Promise<unknown> =>
  runStatementsSerially(pool, buildRunningStyleAnalyzeSqls(), (sql) => {
    log(`Running ${sql}`);
  });

const processYearStep = (
  deps: RunRunningStyleBucketEvalDeps,
  options: RunningStyleBucketEvalCliOptions,
  category: string,
  runner: RunningStyleBucketQueryRunner,
  year: number,
  acc: RunningStyleBucketEvalAccumulator,
): Promise<void> =>
  processYear({ runner, options, category, year, log: deps.log }).then(
    ({ rowCount, raceCount }) => {
      acc.totalRows += rowCount;
      acc.totalRaces += raceCount;
      return deps.sleep(options.perYearSleepMs);
    },
  );

const processYearsWithRunner = (
  deps: RunRunningStyleBucketEvalDeps,
  options: RunningStyleBucketEvalCliOptions,
  category: string,
  runner: RunningStyleBucketQueryRunner,
  chunk: number[],
  acc: RunningStyleBucketEvalAccumulator,
): Promise<unknown> =>
  chunk.reduce<Promise<unknown>>(
    (chain, year) => chain.then(() => processYearStep(deps, options, category, runner, year, acc)),
    Promise.resolve(),
  );

const buildChunkLoaderArgs = (
  options: RunningStyleBucketEvalCliOptions,
  category: string,
  chunk: number[],
): RunningStyleBucketChunkLoaderArgs => {
  const yearFrom = chunk[0] ?? 0;
  const yearTo = chunk.at(-1) ?? 0;
  const modelVersion = resolveModelVersion(options, category);
  return {
    category,
    yearFrom,
    yearTo,
    predictionsRoot: options.predictionsRoot,
    pgUrl: options.pgUrl,
    runningStyleFeatureVersion: options.runningStyleFeatureVersion,
    modelVersion,
  };
};

export const processCategoryChunk = async (
  deps: RunRunningStyleBucketEvalDeps,
  options: RunningStyleBucketEvalCliOptions,
  category: string,
  chunk: number[],
  acc: RunningStyleBucketEvalAccumulator,
): Promise<void> => {
  assertSupportedCategory(category);
  const loaderArgs = buildChunkLoaderArgs(options, category, chunk);
  deps.log(`Open chunk ${category} ${loaderArgs.yearFrom}-${loaderArgs.yearTo}`);
  const client = await deps.openChunkClient(loaderArgs);
  try {
    await runSessionTuning(client.runner, options);
    await processYearsWithRunner(deps, options, category, client.runner, chunk, acc);
  } finally {
    await client.close();
  }
};

const processCategoryChunks = (
  deps: RunRunningStyleBucketEvalDeps,
  options: RunningStyleBucketEvalCliOptions,
  category: string,
  yearChunks: number[][],
  acc: RunningStyleBucketEvalAccumulator,
): Promise<void> => {
  const tasks: ReadonlyArray<() => Promise<void>> = yearChunks.map(
    (chunk) => () => processCategoryChunk(deps, options, category, chunk, acc),
  );
  return runVoidTasksWithConcurrencyLimit(tasks, options.chunkConcurrency);
};

const processCategory = async (
  deps: RunRunningStyleBucketEvalDeps,
  options: RunningStyleBucketEvalCliOptions,
  window: RunningStyleCategoryYearWindow,
  acc: RunningStyleBucketEvalAccumulator,
): Promise<void> => {
  assertSupportedCategory(window.category);
  deps.log(`Begin category ${window.category}`);
  const yearChunks = chunkYears(window.years, options.maxYearsPerRun);
  await processCategoryChunks(deps, options, window.category, yearChunks, acc);
  await deps.sleep(options.perCategorySleepMs);
};

const processAllCategories = async (
  deps: RunRunningStyleBucketEvalDeps,
  options: RunningStyleBucketEvalCliOptions,
  windows: RunningStyleCategoryYearWindow[],
  acc: RunningStyleBucketEvalAccumulator,
): Promise<void> => {
  await Promise.all(windows.map((window) => processCategory(deps, options, window, acc)));
};

export const filterWindowsByCategory = (
  windows: RunningStyleCategoryYearWindow[],
  categoryFilter: RunningStyleBucketCategory | null,
): RunningStyleCategoryYearWindow[] => {
  if (categoryFilter === null) return windows;
  return windows.filter((window) => window.category === categoryFilter);
};

export const runRunningStyleBucketEval = async (
  deps: RunRunningStyleBucketEvalDeps,
  request: RunRunningStyleBucketEvalRequest,
): Promise<RunningStyleBucketEvalAccumulator> => {
  const { options, windows } = request;
  const acc: RunningStyleBucketEvalAccumulator = { totalRows: 0, totalRaces: 0 };
  const filteredWindows = filterWindowsByCategory(windows, options.categoryFilter);
  await ensureBucketTable(deps.pool, deps.log);
  await processAllCategories(deps, options, filteredWindows, acc);
  await runAnalyzes(deps.pool, deps.log);
  return acc;
};

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((r) => {
    setTimeout(r, ms);
  });

const defaultLog = (message: string): void => {
  console.log(`[running-style-bucket-eval] ${message}`);
};

export const getJstHour = (date: Date): number => {
  const utcMs = date.getTime() + date.getTimezoneOffset() * 60_000;
  const jstDate = new Date(utcMs + 9 * 60 * 60 * 1000);
  return jstDate.getHours();
};

const checkColima = async (options: RunningStyleBucketEvalCliOptions): Promise<void> => {
  const proc = Bun.spawn(["colima", "status", "--json"], { stdout: "pipe", stderr: "pipe" });
  const stdout = await new Response(proc.stdout).text();
  await proc.exited;
  if (proc.exitCode !== 0) {
    throw new Error("colima status --json failed; ensure colima is running.");
  }
  const resources = parseColimaStatusJson(stdout);
  ensureColimaCapacity(resources, options.minColimaCpu, options.minColimaMemoryGb);
};

export const buildPredictionsParquetPath = (predictionsRoot: string, category: string): string =>
  `${predictionsRoot}/category=${category}`;

export const buildPythonLoaderArgv = (args: RunningStyleBucketChunkLoaderArgs): string[] => [
  "uv",
  "--project",
  PYTHON_LOADER_UV_PROJECT,
  "run",
  "python",
  PYTHON_LOADER_SCRIPT,
  "--pg-url",
  args.pgUrl,
  "--predictions-parquet-glob",
  buildPredictionsParquetPath(args.predictionsRoot, args.category),
  "--temp-table-name",
  PYTHON_LOADER_TEMP_TABLE,
  "--category",
  args.category,
  "--year-from",
  String(args.yearFrom),
  "--year-to",
  String(args.yearTo),
  "--running-style-feature-version",
  args.runningStyleFeatureVersion,
  "--model-version",
  args.modelVersion,
];

const openChunkClientImpl = async (
  args: RunningStyleBucketChunkLoaderArgs,
): Promise<RunningStyleBucketChunkClient> => {
  const argv = buildPythonLoaderArgv(args);
  const proc = Bun.spawn(argv, { stdin: "pipe", stdout: "pipe", stderr: "inherit" });
  const child: BucketEvalRpcChildLike = {
    stdin: {
      write: (chunk: string) => {
        proc.stdin.write(chunk);
        return true;
      },
      end: () => {
        proc.stdin.end();
      },
    },
    stdout: {
      on: (_event, listener) => {
        const reader = proc.stdout.getReader();
        const decoder = new TextDecoder();
        const pump = (): Promise<void> =>
          reader.read().then((next) => {
            if (next.done) return Promise.resolve();
            listener(decoder.decode(next.value));
            return pump();
          });
        void pump();
      },
    },
  };
  const client = createBucketEvalRpcClient({ child });
  const ready = await client.ready;
  return {
    runner: client,
    loadedRows: ready.loadedRows,
    close: async () => {
      await client.close();
      await proc.exited;
    },
  };
};

const main = async (): Promise<void> => {
  const options = parseArgs(process.argv.slice(2));
  const hour = getJstHour(new Date());
  if (!isWithinNightWindow({ hourJst: hour, ignoreNightWindow: options.ignoreNightWindow })) {
    throw new Error("Outside night window (JST 23-04). Pass --ignore-night-window to override.");
  }
  await checkColima(options);
  const pool = new Pool({ connectionString: options.pgUrl });
  try {
    const result = await runRunningStyleBucketEval(
      {
        pool,
        openChunkClient: openChunkClientImpl,
        sleep: defaultSleep,
        log: defaultLog,
      },
      { options, windows: CATEGORY_YEAR_WINDOWS },
    );
    console.log(
      JSON.stringify({
        totalRows: result.totalRows,
        totalRaces: result.totalRaces,
        modelVersionJra: options.modelVersionJra,
        modelVersionNar: options.modelVersionNar,
        runningStyleFeatureVersion: options.runningStyleFeatureVersion,
      }),
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
