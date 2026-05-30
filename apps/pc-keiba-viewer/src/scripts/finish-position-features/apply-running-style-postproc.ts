// Run with: bun run src/scripts/finish-position-features/apply-running-style-postproc.ts \
//   --logits-parquet <input.parquet> \
//   --output-parquet <output.parquet> \
//   --feature-version v1
//
// Phase C (bucket-eval running-style X5): reads a parquet of per-runner raw
// probabilities (or LightGBM logits) emitted by Phase B and produces the
// post-processed predictions parquet consumed by the bucket evaluator. Adds
// argmax (predicted_class), top-2 (second_predicted_class), and predicted_label.
//
// Race-level nige cap / RaceLevelNigeConstraint is intentionally NOT applied
// here. memory rule `feedback_no_race_level_nige_constraint.md` forbids forcing
// at most one nige per race; raw softmax + argmax is the final decision.

interface ApplyRunningStylePostprocOptions {
  logitsParquet: string;
  outputParquet: string;
  featureVersion: string;
}

interface ApplyRunningStylePostprocCliRunDeps {
  duckdbModule: DuckDBModuleLike;
  logger: PostprocLogger;
}

interface PostprocLogger {
  info: (message: string) => void;
}

interface DuckDBConnectionLike {
  query(sql: string): { toArray(): readonly Record<string, unknown>[] };
  run(sql: string): void;
  close(): void;
}

interface DuckDBModuleLike {
  Database: new (path: string) => DuckDBDatabaseLike;
}

interface DuckDBDatabaseLike {
  connect(): DuckDBConnectionLike;
  close(): void;
}

interface RawInputRow {
  source: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  ketto_toroku_bango: string;
  p_nige: number;
  p_senkou: number;
  p_sashi: number;
  p_oikomi: number;
}

interface PostprocPredictionRow {
  source: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  ketto_toroku_bango: string;
  p_nige: number;
  p_senkou: number;
  p_sashi: number;
  p_oikomi: number;
  predicted_class: number;
  second_predicted_class: number;
  predicted_label: string;
  feature_version: string;
}

interface ApplyArgResult {
  advanceBy: number;
}

const CLASS_LABELS = ["nige", "senkou", "sashi", "oikomi"] satisfies readonly string[];
const PROB_COLUMNS = ["p_nige", "p_senkou", "p_sashi", "p_oikomi"] satisfies readonly string[];
const LOGIT_COLUMNS = [
  "logit_nige",
  "logit_senkou",
  "logit_sashi",
  "logit_oikomi",
] satisfies readonly string[];
const ARGMAX_PRIMARY_INDEX = 0;
const ARGMAX_SECONDARY_INDEX = 1;
const CLASS_COUNT = 4;
const ZERO_SUM_FALLBACK_PROB = 1 / CLASS_COUNT;

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value.`);
  return value;
};

export const initialOptions = (): ApplyRunningStylePostprocOptions => ({
  logitsParquet: "",
  outputParquet: "",
  featureVersion: "",
});

export const buildUsageText = (): string =>
  [
    "Usage:",
    "  bun run src/scripts/finish-position-features/apply-running-style-postproc.ts \\",
    "    --logits-parquet <input.parquet> \\",
    "    --output-parquet <output.parquet> \\",
    "    --feature-version <version>",
  ].join("\n");

export const applyArg = (
  options: ApplyRunningStylePostprocOptions,
  name: string,
  value: string | undefined,
): ApplyArgResult => {
  if (name === "--logits-parquet") {
    options.logitsParquet = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--output-parquet") {
    options.outputParquet = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--feature-version") {
    options.featureVersion = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--help" || name === "-h") {
    console.log(buildUsageText());
    process.exit(0);
  }
  throw new Error(`Unknown argument: ${name}`);
};

const consumeArgs = (
  options: ApplyRunningStylePostprocOptions,
  argv: readonly string[],
  cursor: number,
): ApplyRunningStylePostprocOptions => {
  const name = argv[cursor];
  if (name === undefined) return options;
  const { advanceBy } = applyArg(options, name, argv[cursor + 1]);
  return consumeArgs(options, argv, cursor + advanceBy);
};

export const parseArgs = (argv: readonly string[]): ApplyRunningStylePostprocOptions => {
  const options = consumeArgs(initialOptions(), argv, 0);
  if (options.logitsParquet === "") throw new Error("--logits-parquet is required.");
  if (options.outputParquet === "") throw new Error("--output-parquet is required.");
  if (options.featureVersion === "") throw new Error("--feature-version is required.");
  return options;
};

const requireString = (value: unknown, name: string): string => {
  if (typeof value !== "string") throw new Error(`Column ${name} is not a string.`);
  return value;
};

const coerceNumber = (value: unknown, name: string): number => {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) throw new Error(`Column ${name} is not numeric.`);
  return parsed;
};

const sumNormalize = (values: readonly number[]): number[] => {
  const total = values.reduce((acc, value) => acc + value, 0);
  if (total <= 0) return values.map(() => ZERO_SUM_FALLBACK_PROB);
  return values.map((value) => value / total);
};

export const softmaxNormalize = (logits: readonly number[]): number[] => {
  if (logits.length === 0) return [];
  const maxLogit = Math.max(...logits);
  const exponents = logits.map((logit) => Math.exp(logit - maxLogit));
  return sumNormalize(exponents);
};

const compareDesc = (a: { value: number; index: number }, b: { value: number; index: number }) => {
  if (a.value !== b.value) return b.value - a.value;
  return a.index - b.index;
};

const sortIndicesByProbDesc = (probabilities: readonly number[]): number[] => {
  const indexed = probabilities.map((value, index) => ({ value, index }));
  const sorted = indexed.toSorted(compareDesc);
  return sorted.map((item) => item.index);
};

export const pickArgmax = (probabilities: readonly number[]): number => {
  const sorted = sortIndicesByProbDesc(probabilities);
  return sorted[ARGMAX_PRIMARY_INDEX] ?? 0;
};

export const pickSecondArgmax = (probabilities: readonly number[]): number => {
  const sorted = sortIndicesByProbDesc(probabilities);
  return sorted[ARGMAX_SECONDARY_INDEX] ?? 0;
};

export const buildLabelFromClass = (predictedClass: number): string =>
  CLASS_LABELS[predictedClass] ?? "";

interface RawRowProbabilityResolver {
  resolve: (raw: Record<string, unknown>) => number[];
}

const buildLogitResolver = (): RawRowProbabilityResolver => ({
  resolve: (raw) =>
    softmaxNormalize(LOGIT_COLUMNS.map((column) => coerceNumber(raw[column], column))),
});

const buildProbResolver = (): RawRowProbabilityResolver => ({
  resolve: (raw) => sumNormalize(PROB_COLUMNS.map((column) => coerceNumber(raw[column], column))),
});

const hasAllColumns = (raw: Record<string, unknown>, columns: readonly string[]): boolean =>
  columns.every((column) => raw[column] !== undefined);

export const detectProbabilityResolver = (
  raw: Record<string, unknown>,
): RawRowProbabilityResolver => {
  if (hasAllColumns(raw, LOGIT_COLUMNS)) return buildLogitResolver();
  if (hasAllColumns(raw, PROB_COLUMNS)) return buildProbResolver();
  throw new Error(
    "Input parquet must provide either logit_nige/logit_senkou/logit_sashi/logit_oikomi or p_nige/p_senkou/p_sashi/p_oikomi columns.",
  );
};

const extractRaceKey = (raw: Record<string, unknown>): Pick<RawInputRow, keyof RawInputRow> => ({
  source: requireString(raw.source, "source"),
  kaisai_nen: requireString(raw.kaisai_nen, "kaisai_nen"),
  kaisai_tsukihi: requireString(raw.kaisai_tsukihi, "kaisai_tsukihi"),
  keibajo_code: requireString(raw.keibajo_code, "keibajo_code"),
  race_bango: requireString(raw.race_bango, "race_bango"),
  ketto_toroku_bango: requireString(raw.ketto_toroku_bango, "ketto_toroku_bango"),
  p_nige: 0,
  p_senkou: 0,
  p_sashi: 0,
  p_oikomi: 0,
});

interface BuildPredictionRowParams {
  raceKey: Pick<RawInputRow, keyof RawInputRow>;
  probabilities: readonly number[];
  featureVersion: string;
}

const buildPredictionRow = (params: BuildPredictionRowParams): PostprocPredictionRow => {
  const { raceKey, probabilities, featureVersion } = params;
  const predictedClass = pickArgmax(probabilities);
  const secondPredictedClass = pickSecondArgmax(probabilities);
  return {
    source: raceKey.source,
    kaisai_nen: raceKey.kaisai_nen,
    kaisai_tsukihi: raceKey.kaisai_tsukihi,
    keibajo_code: raceKey.keibajo_code,
    race_bango: raceKey.race_bango,
    ketto_toroku_bango: raceKey.ketto_toroku_bango,
    p_nige: probabilities[0] ?? 0,
    p_senkou: probabilities[1] ?? 0,
    p_sashi: probabilities[2] ?? 0,
    p_oikomi: probabilities[3] ?? 0,
    predicted_class: predictedClass,
    second_predicted_class: secondPredictedClass,
    predicted_label: buildLabelFromClass(predictedClass),
    feature_version: featureVersion,
  };
};

export const applyPostprocToRow = (
  raw: Record<string, unknown>,
  featureVersion: string,
): PostprocPredictionRow => {
  const resolver = detectProbabilityResolver(raw);
  const probabilities = resolver.resolve(raw);
  const raceKey = extractRaceKey(raw);
  return buildPredictionRow({ raceKey, probabilities, featureVersion });
};

export const applyPostprocToRows = (
  rows: readonly Record<string, unknown>[],
  featureVersion: string,
): PostprocPredictionRow[] => rows.map((row) => applyPostprocToRow(row, featureVersion));

export const buildReadInputSql = (logitsParquet: string): string =>
  `SELECT * FROM read_parquet('${logitsParquet}')`;

const escapeStringLiteral = (value: string): string => value.replaceAll("'", "''");

const formatValueForSql = (value: string | number): string => {
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "NULL";
  return `'${escapeStringLiteral(value)}'`;
};

const formatRowAsValuesTuple = (row: PostprocPredictionRow): string =>
  [
    formatValueForSql(row.source),
    formatValueForSql(row.kaisai_nen),
    formatValueForSql(row.kaisai_tsukihi),
    formatValueForSql(row.keibajo_code),
    formatValueForSql(row.race_bango),
    formatValueForSql(row.ketto_toroku_bango),
    formatValueForSql(row.p_nige),
    formatValueForSql(row.p_senkou),
    formatValueForSql(row.p_sashi),
    formatValueForSql(row.p_oikomi),
    formatValueForSql(row.predicted_class),
    formatValueForSql(row.second_predicted_class),
    formatValueForSql(row.predicted_label),
    formatValueForSql(row.feature_version),
  ].join(", ");

const OUTPUT_COLUMN_NAMES = [
  "source",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
  "ketto_toroku_bango",
  "p_nige",
  "p_senkou",
  "p_sashi",
  "p_oikomi",
  "predicted_class",
  "second_predicted_class",
  "predicted_label",
  "feature_version",
] satisfies readonly string[];

export const buildEmptyOutputCopySql = (outputParquet: string): string => {
  const typedColumns = [
    "CAST(NULL AS VARCHAR) AS source",
    "CAST(NULL AS VARCHAR) AS kaisai_nen",
    "CAST(NULL AS VARCHAR) AS kaisai_tsukihi",
    "CAST(NULL AS VARCHAR) AS keibajo_code",
    "CAST(NULL AS VARCHAR) AS race_bango",
    "CAST(NULL AS VARCHAR) AS ketto_toroku_bango",
    "CAST(NULL AS DOUBLE) AS p_nige",
    "CAST(NULL AS DOUBLE) AS p_senkou",
    "CAST(NULL AS DOUBLE) AS p_sashi",
    "CAST(NULL AS DOUBLE) AS p_oikomi",
    "CAST(NULL AS INTEGER) AS predicted_class",
    "CAST(NULL AS INTEGER) AS second_predicted_class",
    "CAST(NULL AS VARCHAR) AS predicted_label",
    "CAST(NULL AS VARCHAR) AS feature_version",
  ].join(", ");
  return `COPY (SELECT ${typedColumns} WHERE 1 = 0) TO '${outputParquet}' (FORMAT PARQUET, COMPRESSION ZSTD)`;
};

export const buildWriteOutputCopySql = (
  outputParquet: string,
  rows: readonly PostprocPredictionRow[],
): string => {
  const tuples = rows.map((row) => `(${formatRowAsValuesTuple(row)})`).join(", ");
  const columns = OUTPUT_COLUMN_NAMES.join(", ");
  return `COPY (SELECT * FROM (VALUES ${tuples}) AS t(${columns})) TO '${outputParquet}' (FORMAT PARQUET, COMPRESSION ZSTD)`;
};

interface ReadInputRowsParams {
  duckdbModule: DuckDBModuleLike;
  logitsParquet: string;
}

export const readInputRows = (params: ReadInputRowsParams): readonly Record<string, unknown>[] => {
  const database = new params.duckdbModule.Database(":memory:");
  const connection = database.connect();
  try {
    return connection.query(buildReadInputSql(params.logitsParquet)).toArray();
  } finally {
    connection.close();
    database.close();
  }
};

interface WriteOutputRowsParams {
  duckdbModule: DuckDBModuleLike;
  outputParquet: string;
  rows: readonly PostprocPredictionRow[];
}

export const writeOutputRows = (params: WriteOutputRowsParams): void => {
  const database = new params.duckdbModule.Database(":memory:");
  const connection = database.connect();
  try {
    if (params.rows.length === 0) {
      connection.run(buildEmptyOutputCopySql(params.outputParquet));
      return;
    }
    connection.run(buildWriteOutputCopySql(params.outputParquet, params.rows));
  } finally {
    connection.close();
    database.close();
  }
};

interface RunPostprocParams {
  duckdbModule: DuckDBModuleLike;
  options: ApplyRunningStylePostprocOptions;
  logger: PostprocLogger;
}

export const runPostproc = (params: RunPostprocParams): { rowCount: number } => {
  const rawRows = readInputRows({
    duckdbModule: params.duckdbModule,
    logitsParquet: params.options.logitsParquet,
  });
  const predictionRows = applyPostprocToRows(rawRows, params.options.featureVersion);
  writeOutputRows({
    duckdbModule: params.duckdbModule,
    outputParquet: params.options.outputParquet,
    rows: predictionRows,
  });
  params.logger.info(
    `[apply-running-style-postproc] feature_version=${params.options.featureVersion} input=${params.options.logitsParquet} output=${params.options.outputParquet} rows=${predictionRows.length}`,
  );
  return { rowCount: predictionRows.length };
};

interface RunCliParams {
  argv: readonly string[];
  duckdbModule: DuckDBModuleLike;
  logger: PostprocLogger;
}

export const runCli = (params: RunCliParams): { rowCount: number } => {
  const options = parseArgs(params.argv);
  return runPostproc({
    duckdbModule: params.duckdbModule,
    options,
    logger: params.logger,
  });
};

const defaultLogger = (): PostprocLogger => ({
  info: (message) => {
    console.log(message);
  },
});

const isObjectRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isDuckdbModuleLike = (value: unknown): value is DuckDBModuleLike => {
  if (!isObjectRecord(value)) return false;
  return typeof value["Database"] === "function";
};

const extractNestedDefault = (value: Record<string, unknown>): unknown => value["default"];

// The dynamic specifier defeats vite/vitest static-analysis so the test runner
// does not need to resolve the optional `@duckdb/node-api` native addon.
const DUCKDB_MODULE_SPECIFIER = "@duckdb/node-api";

const loadDuckdbModule = async (): Promise<DuckDBModuleLike> => {
  const specifier: string = DUCKDB_MODULE_SPECIFIER;
  const moduleNamespace: unknown = await import(specifier);
  if (isDuckdbModuleLike(moduleNamespace)) return moduleNamespace;
  if (isObjectRecord(moduleNamespace)) {
    const nested = extractNestedDefault(moduleNamespace);
    if (isDuckdbModuleLike(nested)) return nested;
  }
  throw new Error("@duckdb/node-api does not export Database.");
};

const main = async (): Promise<void> => {
  const duckdbModule = await loadDuckdbModule();
  runCli({
    argv: process.argv.slice(2),
    duckdbModule,
    logger: defaultLogger(),
  });
};

if (import.meta.main) {
  main().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}

export type {
  ApplyRunningStylePostprocCliRunDeps,
  ApplyRunningStylePostprocOptions,
  DuckDBConnectionLike,
  DuckDBDatabaseLike,
  DuckDBModuleLike,
  PostprocLogger,
  PostprocPredictionRow,
  RawInputRow,
};
