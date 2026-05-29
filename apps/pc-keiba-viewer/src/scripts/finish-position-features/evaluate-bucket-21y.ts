// Run with: bun run src/scripts/finish-position-features/evaluate-bucket-21y.ts \
//   --pg-url $DATABASE_URL_LOCAL --running-style-feature-version v1 --finish-position-version v1

import { Pool } from "pg";

import { createBucketEvalRpcClient } from "./bucket-eval-rpc-client";
import type { BucketEvalRpcChildLike } from "./bucket-eval-rpc-client";
import {
  buildAnalyzeSqls,
  buildBucketAggregateSql,
  buildBucketEvaluationsDdl,
  buildBucketUpsertSql,
  buildConcurrentIndexSqls,
} from "./evaluate-bucket-predictions-sql";

export interface BucketEvalCliOptions {
  pgUrl: string;
  runningStyleFeatureVersion: string;
  finishPositionVersion: string;
  modelVersion: string;
  maxYearsPerRun: number;
  statementTimeoutMs: number;
  ignoreNightWindow: boolean;
  perYearSleepMs: number;
  perCategorySleepMs: number;
  minColimaCpu: number;
  minColimaMemoryGb: number;
  predictionsRoot: string;
}

export interface CategoryYearWindow {
  category: "jra" | "nar" | "ban-ei";
  years: number[];
}

export interface AggregateRow {
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
  top1_hit_sum: string;
  place1_hit_sum: string;
  place2_hit_sum: string;
  place3_hit_sum: string;
  top3_box_hit_sum: string;
  top3_exact_hit_sum: string;
  top3_winner_capture_sum: string;
  top5_winner_capture_sum: string;
  top3_place_relation_sum: string;
  pair_score_sum: string;
  pair_score_pair_count: string | number;
  ndcg_at_3_sum: string;
  ndcg_at_3_race_count: string | number;
}

export interface ColimaResources {
  cpu: number;
  memory: number;
}

export interface NightWindowInput {
  hourJst: number;
  ignoreNightWindow: boolean;
}

export interface BucketQueryResult<Row> {
  rows: Row[];
}

export interface BucketQueryRunner {
  query: <Row>(sql: string, params?: unknown[]) => Promise<BucketQueryResult<Row>>;
}

export interface BucketChunkLoaderArgs {
  category: "jra" | "nar" | "ban-ei";
  yearFrom: number;
  yearTo: number;
  predictionsRoot: string;
  pgUrl: string;
  runningStyleFeatureVersion: string;
  finishPositionVersion: string;
  modelVersion: string;
}

export interface BucketChunkClient {
  runner: BucketQueryRunner;
  close: () => Promise<void>;
  loadedRows: number;
}

export interface RunBucketEvalDeps {
  pool: BucketQueryRunner;
  openChunkClient: (args: BucketChunkLoaderArgs) => Promise<BucketChunkClient>;
  sleep: (ms: number) => Promise<void>;
  log: (message: string) => void;
}

export interface RunBucketEvalRequest {
  options: BucketEvalCliOptions;
  windows: CategoryYearWindow[];
}

const NIGHT_WINDOW_HOURS_JST = new Set<number>([23, 0, 1, 2, 3, 4]);
const DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing";
const DEFAULT_MAX_YEARS_PER_RUN = 5;
const DEFAULT_STATEMENT_TIMEOUT_MS = 900_000;
const DEFAULT_PER_YEAR_SLEEP_MS = 2_000;
const DEFAULT_PER_CATEGORY_SLEEP_MS = 5_000;
const DEFAULT_MIN_COLIMA_CPU = 8;
const DEFAULT_MIN_COLIMA_MEMORY_GB = 24;
const DEFAULT_MODEL_VERSION_SENTINEL = "active";
const DEFAULT_PREDICTIONS_ROOT =
  "apps/pc-keiba-viewer/tmp/bucket-eval/finish-position/v1/predictions";
const DEFAULT_PREDICTIONS_PARQUET_GLOB_SUFFIX = "/**/*.parquet";
const PYTHON_LOADER_SCRIPT = "src/scripts/load_bucket_predictions.py";
const PYTHON_LOADER_TEMP_TABLE = "bucket_predictions_loaded";
const JRA_YEARS = [
  2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021,
  2022, 2023, 2024, 2025, 2026,
];
const NAR_YEARS = [
  2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2026,
];
const BAN_EI_YEARS = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026];

export const CATEGORY_YEAR_WINDOWS: CategoryYearWindow[] = [
  { category: "jra", years: JRA_YEARS },
  { category: "nar", years: NAR_YEARS },
  { category: "ban-ei", years: BAN_EI_YEARS },
];

export const buildUsageText = (): string =>
  [
    "Usage:",
    "  bun run src/scripts/finish-position-features/evaluate-bucket-21y.ts \\",
    "    --pg-url <connection-string> \\",
    "    --running-style-feature-version v1 \\",
    "    --finish-position-version v1 \\",
    "    [--model-version active] \\",
    "    [--max-years-per-run 5] \\",
    "    [--statement-timeout-ms 900000] \\",
    "    [--ignore-night-window]",
  ].join("\n");

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value.`);
  return value;
};

const initialOptions = (): BucketEvalCliOptions => ({
  pgUrl: process.env.DATABASE_URL_LOCAL ?? DEFAULT_PG_URL,
  runningStyleFeatureVersion: "",
  finishPositionVersion: "",
  modelVersion: DEFAULT_MODEL_VERSION_SENTINEL,
  maxYearsPerRun: DEFAULT_MAX_YEARS_PER_RUN,
  statementTimeoutMs: DEFAULT_STATEMENT_TIMEOUT_MS,
  ignoreNightWindow: false,
  perYearSleepMs: DEFAULT_PER_YEAR_SLEEP_MS,
  perCategorySleepMs: DEFAULT_PER_CATEGORY_SLEEP_MS,
  minColimaCpu: DEFAULT_MIN_COLIMA_CPU,
  minColimaMemoryGb: DEFAULT_MIN_COLIMA_MEMORY_GB,
  predictionsRoot: DEFAULT_PREDICTIONS_ROOT,
});

interface ApplyArgResult {
  advanceBy: number;
}

const applyArg = (
  options: BucketEvalCliOptions,
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
  if (name === "--finish-position-version") {
    options.finishPositionVersion = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--model-version") {
    options.modelVersion = requireValue(name, value);
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
  if (name === "--help" || name === "-h") {
    console.log(buildUsageText());
    process.exit(0);
  }
  throw new Error(`Unknown argument: ${name}`);
};

const consumeArgs = (
  options: BucketEvalCliOptions,
  argv: readonly string[],
  cursor: number,
): BucketEvalCliOptions => {
  const name = argv[cursor];
  if (name === undefined) return options;
  const { advanceBy } = applyArg(options, name, argv[cursor + 1]);
  return consumeArgs(options, argv, cursor + advanceBy);
};

export const parseArgs = (argv: readonly string[]): BucketEvalCliOptions => {
  const options = consumeArgs(initialOptions(), argv, 0);
  if (options.runningStyleFeatureVersion === "") {
    throw new Error("--running-style-feature-version is required.");
  }
  if (options.finishPositionVersion === "") {
    throw new Error("--finish-position-version is required.");
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

const buildSessionStatementSql = (statementTimeoutMs: number): string =>
  `set local statement_timeout = '${statementTimeoutMs}ms'`;

const buildSessionWorkMemSql = (): string => "set local work_mem = '256MB'";

const buildSessionIdleTimeoutSql = (statementTimeoutMs: number): string =>
  `set local idle_in_transaction_session_timeout = '${statementTimeoutMs}ms'`;

export const buildSessionTuningSqls = (statementTimeoutMs: number): string[] => [
  buildSessionStatementSql(statementTimeoutMs),
  buildSessionIdleTimeoutSql(statementTimeoutMs),
  buildSessionWorkMemSql(),
];

const ensureBucketTable = async (
  pool: BucketQueryRunner,
  log: (m: string) => void,
): Promise<void> => {
  log("Creating bucket evaluations DDL");
  await pool.query(buildBucketEvaluationsDdl());
};

const runStatementsSerially = (
  pool: BucketQueryRunner,
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

const ensureConcurrentIndexes = (
  pool: BucketQueryRunner,
  log: (m: string) => void,
): Promise<unknown> =>
  runStatementsSerially(pool, buildConcurrentIndexSqls(), () => {
    log("Applying concurrent index");
  });

const runAggregateForYear = async (
  runner: BucketQueryRunner,
  options: BucketEvalCliOptions,
  category: "jra" | "nar" | "ban-ei",
  year: number,
): Promise<AggregateRow[]> => {
  const { fromDate, toDate } = buildYearDateWindow(year);
  const sql = buildBucketAggregateSql({
    modelVersion: options.modelVersion,
    category,
    fromDate,
    toDate,
    runningStyleFeatureVersion: options.runningStyleFeatureVersion,
    finishPositionVersion: options.finishPositionVersion,
  });
  const result = await runner.query<AggregateRow>(sql);
  return result.rows;
};

const numericFromRow = (raw: string | number | null): string => {
  if (raw === null) return "0";
  if (typeof raw === "number") return raw.toString();
  return raw;
};

interface UpsertContext {
  runner: BucketQueryRunner;
  options: BucketEvalCliOptions;
  category: "jra" | "nar" | "ban-ei";
  fromDate: string;
  toDate: string;
  upsertSql: string;
}

export const buildUpsertParams = (context: UpsertContext, row: AggregateRow): unknown[] => [
  context.options.modelVersion,
  context.options.runningStyleFeatureVersion,
  context.options.finishPositionVersion,
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
  row.top1_hit_sum,
  row.place1_hit_sum,
  row.place2_hit_sum,
  row.place3_hit_sum,
  row.top3_box_hit_sum,
  row.top3_exact_hit_sum,
  row.top3_winner_capture_sum,
  row.top5_winner_capture_sum,
  row.top3_place_relation_sum,
  row.pair_score_sum,
  numericFromRow(row.pair_score_pair_count),
  row.ndcg_at_3_sum,
  numericFromRow(row.ndcg_at_3_race_count),
];

const upsertRows = (
  runner: BucketQueryRunner,
  options: BucketEvalCliOptions,
  category: "jra" | "nar" | "ban-ei",
  year: number,
  rows: AggregateRow[],
): Promise<unknown> => {
  const upsertSql = buildBucketUpsertSql();
  const { fromDate, toDate } = buildYearDateWindow(year);
  const context: UpsertContext = { runner, options, category, fromDate, toDate, upsertSql };
  return rows.reduce<Promise<unknown>>(
    (chain, row) => chain.then(() => runner.query(upsertSql, buildUpsertParams(context, row))),
    Promise.resolve(),
  );
};

export interface ProcessYearArgs {
  runner: BucketQueryRunner;
  options: BucketEvalCliOptions;
  category: "jra" | "nar" | "ban-ei";
  year: number;
  log: (m: string) => void;
}

export const processYear = async (
  args: ProcessYearArgs,
): Promise<{ rowCount: number; raceCount: number }> => {
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
  runner: BucketQueryRunner,
  options: BucketEvalCliOptions,
): Promise<unknown> => {
  const sqls = ["begin", ...buildSessionTuningSqls(options.statementTimeoutMs)];
  return runStatementsSerially(runner, sqls, () => {});
};

const finalizeSessionTuning = (runner: BucketQueryRunner): Promise<unknown> =>
  runner.query("commit");

const runAnalyzes = (pool: BucketQueryRunner, log: (m: string) => void): Promise<unknown> =>
  runStatementsSerially(pool, buildAnalyzeSqls(), (sql) => {
    log(`Running ${sql}`);
  });

interface BucketEvalAccumulator {
  totalRows: number;
  totalRaces: number;
}

const processYearStep = (
  deps: RunBucketEvalDeps,
  options: BucketEvalCliOptions,
  category: "jra" | "nar" | "ban-ei",
  runner: BucketQueryRunner,
  year: number,
  acc: BucketEvalAccumulator,
): Promise<void> =>
  processYear({ runner, options, category, year, log: deps.log }).then(
    ({ rowCount, raceCount }) => {
      acc.totalRows += rowCount;
      acc.totalRaces += raceCount;
      return deps.sleep(options.perYearSleepMs);
    },
  );

const processYearsWithRunner = (
  deps: RunBucketEvalDeps,
  options: BucketEvalCliOptions,
  category: "jra" | "nar" | "ban-ei",
  runner: BucketQueryRunner,
  chunk: number[],
  acc: BucketEvalAccumulator,
): Promise<unknown> =>
  chunk.reduce<Promise<unknown>>(
    (chain, year) => chain.then(() => processYearStep(deps, options, category, runner, year, acc)),
    Promise.resolve(),
  );

const buildChunkLoaderArgs = (
  options: BucketEvalCliOptions,
  category: "jra" | "nar" | "ban-ei",
  chunk: number[],
): BucketChunkLoaderArgs => {
  const yearFrom = chunk[0] ?? 0;
  const yearTo = chunk.at(-1) ?? 0;
  return {
    category,
    yearFrom,
    yearTo,
    predictionsRoot: options.predictionsRoot,
    pgUrl: options.pgUrl,
    runningStyleFeatureVersion: options.runningStyleFeatureVersion,
    finishPositionVersion: options.finishPositionVersion,
    modelVersion: options.modelVersion,
  };
};

export const processCategoryChunk = async (
  deps: RunBucketEvalDeps,
  options: BucketEvalCliOptions,
  category: "jra" | "nar" | "ban-ei",
  chunk: number[],
  acc: BucketEvalAccumulator,
): Promise<void> => {
  const loaderArgs = buildChunkLoaderArgs(options, category, chunk);
  deps.log(`Open chunk ${category} ${loaderArgs.yearFrom}-${loaderArgs.yearTo}`);
  const client = await deps.openChunkClient(loaderArgs);
  try {
    await runSessionTuning(client.runner, options);
    await processYearsWithRunner(deps, options, category, client.runner, chunk, acc);
    await finalizeSessionTuning(client.runner);
  } finally {
    await client.close();
  }
};

const processCategoryChunks = (
  deps: RunBucketEvalDeps,
  options: BucketEvalCliOptions,
  category: "jra" | "nar" | "ban-ei",
  yearChunks: number[][],
  acc: BucketEvalAccumulator,
): Promise<unknown> =>
  yearChunks.reduce<Promise<unknown>>(
    (chain, chunk) => chain.then(() => processCategoryChunk(deps, options, category, chunk, acc)),
    Promise.resolve(),
  );

const processCategory = async (
  deps: RunBucketEvalDeps,
  options: BucketEvalCliOptions,
  window: CategoryYearWindow,
  acc: BucketEvalAccumulator,
): Promise<void> => {
  deps.log(`Begin category ${window.category}`);
  const yearChunks = chunkYears(window.years, options.maxYearsPerRun);
  await processCategoryChunks(deps, options, window.category, yearChunks, acc);
  await deps.sleep(options.perCategorySleepMs);
};

const processAllCategories = (
  deps: RunBucketEvalDeps,
  options: BucketEvalCliOptions,
  windows: CategoryYearWindow[],
  acc: BucketEvalAccumulator,
): Promise<unknown> =>
  windows.reduce<Promise<unknown>>(
    (chain, window) => chain.then(() => processCategory(deps, options, window, acc)),
    Promise.resolve(),
  );

export const runBucketEval = async (
  deps: RunBucketEvalDeps,
  request: RunBucketEvalRequest,
): Promise<BucketEvalAccumulator> => {
  const { options, windows } = request;
  const acc: BucketEvalAccumulator = { totalRows: 0, totalRaces: 0 };
  await ensureConcurrentIndexes(deps.pool, deps.log);
  await ensureBucketTable(deps.pool, deps.log);
  await processAllCategories(deps, options, windows, acc);
  await runAnalyzes(deps.pool, deps.log);
  return acc;
};

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((r) => {
    setTimeout(r, ms);
  });

const defaultLog = (message: string): void => {
  console.log(`[bucket-eval] ${message}`);
};

const getJstHour = (date: Date): number => {
  const utcMs = date.getTime() + date.getTimezoneOffset() * 60_000;
  const jstDate = new Date(utcMs + 9 * 60 * 60 * 1000);
  return jstDate.getHours();
};

const checkColima = async (options: BucketEvalCliOptions): Promise<void> => {
  const proc = Bun.spawn(["colima", "status", "--json"], { stdout: "pipe", stderr: "pipe" });
  const stdout = await new Response(proc.stdout).text();
  await proc.exited;
  if (proc.exitCode !== 0) {
    throw new Error("colima status --json failed; ensure colima is running.");
  }
  const resources = parseColimaStatusJson(stdout);
  ensureColimaCapacity(resources, options.minColimaCpu, options.minColimaMemoryGb);
};

export const buildPredictionsParquetGlob = (predictionsRoot: string): string =>
  `${predictionsRoot}${DEFAULT_PREDICTIONS_PARQUET_GLOB_SUFFIX}`;

export const buildPythonLoaderArgv = (args: BucketChunkLoaderArgs): string[] => [
  "uv",
  "run",
  "python",
  PYTHON_LOADER_SCRIPT,
  "--pg-url",
  args.pgUrl,
  "--predictions-parquet-glob",
  buildPredictionsParquetGlob(args.predictionsRoot),
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
  "--finish-position-version",
  args.finishPositionVersion,
];

const openChunkClientImpl = async (args: BucketChunkLoaderArgs): Promise<BucketChunkClient> => {
  const argv = buildPythonLoaderArgv(args);
  const proc = Bun.spawn(argv, {
    stdin: "pipe",
    stdout: "pipe",
    stderr: "inherit",
  });
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
    const result = await runBucketEval(
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
        modelVersion: options.modelVersion,
        runningStyleFeatureVersion: options.runningStyleFeatureVersion,
        finishPositionVersion: options.finishPositionVersion,
      }),
    );
  } finally {
    await pool.end();
  }
};

/* v8 ignore start */
if (import.meta.main) {
  main().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}
/* v8 ignore stop */

export {
  buildSessionStatementSql,
  buildSessionWorkMemSql,
  buildSessionIdleTimeoutSql,
  getJstHour,
  initialOptions,
  applyArg,
  numericFromRow,
};
