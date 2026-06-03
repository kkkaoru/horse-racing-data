// Run with: bun run src/scripts/finish-position-features/verify-finish-position-v7lineage-coverage.ts \
//   --pg-url $DATABASE_URL_LOCAL [--out tmp/stage-8-verify-<ts>.json]
//
// Stage 8 of the finish-position v7-lineage walk-forward 21y plan. A read-only
// PostgreSQL verifier that confirms Stage 3/4's WF 21y backfill is complete and
// plausible. It (1) checks per-category fold coverage in
// model_prediction_bucket_evaluations, (2) computes the global top1 hit rate and
// warns when it falls outside a wide plausibility band, (3) cross-checks the
// per-year bucket race_count against jvd_ra / nvd_ra distinct races, (4) asserts
// the three global rollup rows exist in model_prediction_evaluations, and reports
// (5) the active-model wiring and (6) the last cron run when those tables exist.
// Only SELECTs are issued; nothing is written to the database. All hard-fail
// gates (missing fold coverage, missing global rollup) exit non-zero, while soft
// plausibility / cross-check deviations only raise warnings.

import { writeFile } from "node:fs/promises";

import { Pool } from "pg";

import { FINISH_POSITION_V7_LINEAGE_MODEL_VERSIONS } from "./v7-lineage-model-versions";

export type VerifyCategory = "jra" | "nar" | "banei";
export type VerifySource = "jra" | "nar" | "ban-ei";
export type VerifyStoredSource = "jra" | "nar";
export type OverallStatus = "pass" | "warn" | "fail";

export interface FoldCoverageRow {
  model_version: string;
  source: string;
  years: string | number;
  race_count_sum: string | number | null;
}

export interface Top1PlausibilityRow {
  model_version: string;
  top1_hit_sum: string | number | null;
  race_count: string | number | null;
}

export interface BucketYearRaceCountRow {
  source: string;
  year: string;
  bucket_race_count: string | number | null;
}

export interface ActualYearRaceCountRow {
  year: string;
  actual_race_count: string | number | null;
}

export interface GlobalEvalRow {
  model_version: string;
  top1_accuracy: string | number | null;
}

export interface ActiveModelRow {
  category: string;
  model_version: string;
}

export interface CronExecutionRow {
  max_run_date: string | null;
  status: string | null;
}

export interface TableExistsRow {
  exists: boolean;
}

export interface CategoryExpectation {
  category: VerifyCategory;
  source: VerifySource;
  storedSource: VerifyStoredSource;
  modelVersion: string;
  expectedYears: number;
  top1Low: number;
  top1High: number;
}

export interface FoldCoverageAssessment {
  category: VerifyCategory;
  source: VerifySource;
  storedSource: VerifyStoredSource;
  modelVersion: string;
  expectedYears: number;
  observedYears: number;
  raceCountSum: number;
  ok: boolean;
}

export interface Top1PlausibilityAssessment {
  category: VerifyCategory;
  modelVersion: string;
  top1Rate: number | null;
  low: number;
  high: number;
  withinBand: boolean;
}

export interface RaceCountCrosscheckEntry {
  source: VerifySource;
  year: string;
  bucketRaceCount: number;
  actualRaceCount: number;
  relativeDelta: number | null;
  withinTolerance: boolean;
}

export interface RaceCountCrosscheckResult {
  source: VerifySource;
  entries: RaceCountCrosscheckEntry[];
  worstRelativeDelta: number | null;
  allWithinTolerance: boolean;
}

export interface AssessRaceCountCrosscheckInput {
  source: VerifySource;
  storedSource: VerifyStoredSource;
  bucketRows: BucketYearRaceCountRow[];
  actualRows: ActualYearRaceCountRow[];
}

export interface ActiveModelsSummary {
  byCategory: Record<VerifyCategory, string | null>;
  flippedToWf21y: Record<VerifyCategory, boolean>;
  allFlipped: boolean;
}

export interface CronSummary {
  tableExists: boolean;
  lastRunDate: string | null;
  status: string | null;
}

export interface VerifyReport {
  coverage: FoldCoverageAssessment[];
  top1_plausibility: Top1PlausibilityAssessment[];
  race_count_crosscheck: RaceCountCrosscheckResult[];
  global_eval_present: boolean;
  active_models: ActiveModelsSummary;
  cron_last_run: CronSummary;
  overall: OverallStatus;
  issues: string[];
}

export interface QueryResultLike<Row> {
  rows: Row[];
}

export interface QueryRunner {
  query: <Row>(sql: string, params?: unknown[]) => Promise<QueryResultLike<Row>>;
}

export interface VerifyCliOptions {
  pgUrl: string;
  out: string | null;
}

export interface VerifyDeps {
  runner: QueryRunner;
  log: (message: string) => void;
  writeOut: (path: string, body: string) => Promise<void>;
}

const DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing";
const BUCKET_TABLE = "model_prediction_bucket_evaluations";
const EVALUATIONS_TABLE = "model_prediction_evaluations";
const ACTIVE_MODELS_TABLE = "finish_position_active_models";
const CRON_TABLE = "finish_position_cron_executions";
const JRA_RA_TABLE = "jvd_ra";
const NAR_RA_TABLE = "nvd_ra";
const BANEI_KEIBAJO_CODE = "83";
const WF_21Y_SUFFIX = "%-v7-lineage-wf-21y";
const RACE_COUNT_TOLERANCE = 0.02;
const EXPECTED_GLOBAL_ROWS = 3;
const YEAR_PREFIX_LENGTH = 4;
const PERCENT_SCALE = 100;

const CATEGORY_EXPECTATIONS: CategoryExpectation[] = [
  {
    category: "jra",
    source: "jra",
    storedSource: "jra",
    modelVersion: FINISH_POSITION_V7_LINEAGE_MODEL_VERSIONS.jra,
    expectedYears: 20,
    top1Low: 0.37,
    top1High: 0.43,
  },
  {
    category: "nar",
    source: "nar",
    storedSource: "nar",
    modelVersion: FINISH_POSITION_V7_LINEAGE_MODEL_VERSIONS.nar,
    expectedYears: 20,
    top1Low: 0.5,
    top1High: 0.6,
  },
  {
    // Ban-ei bucket rows are persisted with category='ban-ei' but source='nar'
    // (keibajo_code='83'), matching the PG path + deployed models. The verifier
    // therefore matches stored rows by storedSource='nar' while comparing fold
    // race counts against the keibajo='83' slice of nvd_ra (source='ban-ei').
    category: "banei",
    source: "ban-ei",
    storedSource: "nar",
    modelVersion: FINISH_POSITION_V7_LINEAGE_MODEL_VERSIONS.banei,
    expectedYears: 19,
    top1Low: 0.3,
    top1High: 0.4,
  },
];

const VERIFY_CATEGORIES: VerifyCategory[] = ["jra", "nar", "banei"];

const escapeSqlLiteral = (value: string): string => value.replaceAll("'", "''");

const toFiniteNumber = (value: string | number | null | undefined): number => {
  if (value === null || value === undefined) return 0;
  const parsed = typeof value === "number" ? value : Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : 0;
};

export const buildFoldCoverageSql = (modelVersion: string): string => `
    select
      model_version,
      source,
      count(distinct evaluation_window_from) as years,
      sum(race_count) as race_count_sum
    from ${BUCKET_TABLE}
    where model_version = '${escapeSqlLiteral(modelVersion)}'
    group by model_version, source
  `;

export const buildTop1PlausibilitySql = (modelVersion: string): string => `
    select
      model_version,
      sum(top1_hit_sum) as top1_hit_sum,
      sum(race_count) as race_count
    from ${BUCKET_TABLE}
    where model_version = '${escapeSqlLiteral(modelVersion)}'
    group by model_version
  `;

export const buildBucketYearRaceCountSql = (modelVersion: string): string => `
    select
      source,
      left(evaluation_window_from, ${YEAR_PREFIX_LENGTH}) as year,
      sum(race_count) as bucket_race_count
    from ${BUCKET_TABLE}
    where model_version = '${escapeSqlLiteral(modelVersion)}'
    group by source, left(evaluation_window_from, ${YEAR_PREFIX_LENGTH})
  `;

export const buildActualRaceCountSql = (source: VerifySource): string => {
  if (source === "jra") {
    return `
    select
      kaisai_nen as year,
      count(distinct (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)) as actual_race_count
    from ${JRA_RA_TABLE}
    group by kaisai_nen
  `;
  }
  const baneiFilter =
    source === "ban-ei"
      ? `keibajo_code = '${BANEI_KEIBAJO_CODE}'`
      : `keibajo_code <> '${BANEI_KEIBAJO_CODE}'`;
  return `
    select
      kaisai_nen as year,
      count(distinct (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)) as actual_race_count
    from ${NAR_RA_TABLE}
    where ${baneiFilter}
    group by kaisai_nen
  `;
};

export const buildGlobalEvalSql = (): string => `
    select model_version, top1_accuracy
    from ${EVALUATIONS_TABLE}
    where model_version like '${WF_21Y_SUFFIX}'
  `;

export const buildActiveModelsSql = (): string =>
  `select category, model_version from ${ACTIVE_MODELS_TABLE}`;

export const buildCronTableExistsSql = (): string =>
  `select to_regclass('public.${CRON_TABLE}') is not null as exists`;

export const buildCronLastRunSql = (): string =>
  `select max(run_date) as max_run_date, max(status) as status from ${CRON_TABLE}`;

export const assessFoldCoverage = (
  expectation: CategoryExpectation,
  rows: FoldCoverageRow[],
): FoldCoverageAssessment => {
  const match = rows.find(
    (row) =>
      row.model_version === expectation.modelVersion && row.source === expectation.storedSource,
  );
  const observedYears = match === undefined ? 0 : toFiniteNumber(match.years);
  const raceCountSum = match === undefined ? 0 : toFiniteNumber(match.race_count_sum);
  return {
    category: expectation.category,
    source: expectation.source,
    storedSource: expectation.storedSource,
    modelVersion: expectation.modelVersion,
    expectedYears: expectation.expectedYears,
    observedYears,
    raceCountSum,
    ok: observedYears === expectation.expectedYears,
  };
};

export const assessTop1Plausibility = (
  expectation: CategoryExpectation,
  rows: Top1PlausibilityRow[],
): Top1PlausibilityAssessment => {
  const match = rows.find((row) => row.model_version === expectation.modelVersion);
  // top1_hit_sum is a per-race indicator (predicted-rank-1 horse == actual
  // winner), so the plausible top1 rate divides by race_count, not the
  // per-horse prediction_count.
  const raceCount = match === undefined ? 0 : toFiniteNumber(match.race_count);
  const top1HitSum = match === undefined ? 0 : toFiniteNumber(match.top1_hit_sum);
  const top1Rate = raceCount > 0 ? top1HitSum / raceCount : null;
  const withinBand =
    top1Rate !== null && top1Rate >= expectation.top1Low && top1Rate <= expectation.top1High;
  return {
    category: expectation.category,
    modelVersion: expectation.modelVersion,
    top1Rate,
    low: expectation.top1Low,
    high: expectation.top1High,
    withinBand,
  };
};

const buildCrosscheckEntry = (
  source: VerifySource,
  year: string,
  bucketRaceCount: number,
  actualRaceCount: number,
): RaceCountCrosscheckEntry => {
  const relativeDelta =
    actualRaceCount > 0 ? Math.abs(bucketRaceCount - actualRaceCount) / actualRaceCount : null;
  return {
    source,
    year,
    bucketRaceCount,
    actualRaceCount,
    relativeDelta,
    withinTolerance: relativeDelta !== null && relativeDelta <= RACE_COUNT_TOLERANCE,
  };
};

const pickWorstDelta = (entries: RaceCountCrosscheckEntry[]): number | null =>
  entries.reduce<number | null>((worst, entry) => {
    if (entry.relativeDelta === null) return worst;
    if (worst === null) return entry.relativeDelta;
    return entry.relativeDelta > worst ? entry.relativeDelta : worst;
  }, null);

export const assessRaceCountCrosscheck = (
  input: AssessRaceCountCrosscheckInput,
): RaceCountCrosscheckResult => {
  const { source, storedSource, bucketRows, actualRows } = input;
  const actualByYear = new Map<string, number>(
    actualRows.map((row) => [row.year, toFiniteNumber(row.actual_race_count)]),
  );
  const entries = bucketRows
    .filter((row) => row.source === storedSource)
    .map((row) =>
      buildCrosscheckEntry(
        source,
        row.year,
        toFiniteNumber(row.bucket_race_count),
        actualByYear.get(row.year) ?? 0,
      ),
    );
  return {
    source,
    entries,
    worstRelativeDelta: pickWorstDelta(entries),
    allWithinTolerance: entries.every((entry) => entry.withinTolerance),
  };
};

export const assessGlobalEvalPresence = (rows: GlobalEvalRow[]): boolean =>
  rows.length === EXPECTED_GLOBAL_ROWS;

const isWf21yModelVersion = (modelVersion: string | null): boolean =>
  modelVersion !== null && modelVersion.endsWith("-v7-lineage-wf-21y");

export const summarizeActiveModels = (rows: ActiveModelRow[]): ActiveModelsSummary => {
  const byCategory = VERIFY_CATEGORIES.reduce<Record<VerifyCategory, string | null>>(
    (acc, category) => {
      const match = rows.find((row) => row.category === category);
      acc[category] = match === undefined ? null : match.model_version;
      return acc;
    },
    { jra: null, nar: null, banei: null },
  );
  const flippedToWf21y = VERIFY_CATEGORIES.reduce<Record<VerifyCategory, boolean>>(
    (acc, category) => {
      acc[category] = isWf21yModelVersion(byCategory[category]);
      return acc;
    },
    { jra: false, nar: false, banei: false },
  );
  return {
    byCategory,
    flippedToWf21y,
    allFlipped: VERIFY_CATEGORIES.every((category) => flippedToWf21y[category]),
  };
};

export const summarizeCron = (tableExists: boolean, rows: CronExecutionRow[]): CronSummary => {
  const row = rows[0];
  if (!tableExists || row === undefined) {
    return { tableExists, lastRunDate: null, status: null };
  }
  return { tableExists, lastRunDate: row.max_run_date, status: row.status };
};

const formatRate = (rate: number | null): string =>
  rate === null ? "n/a" : `${(rate * PERCENT_SCALE).toFixed(2)}%`;

export const collectIssues = (parts: {
  coverage: FoldCoverageAssessment[];
  top1: Top1PlausibilityAssessment[];
  crosschecks: RaceCountCrosscheckResult[];
  globalEvalPresent: boolean;
}): string[] => {
  const coverageIssues = parts.coverage
    .filter((entry) => !entry.ok)
    .map(
      (entry) =>
        `FAIL fold-coverage ${entry.category}: expected ${entry.expectedYears} year-windows, observed ${entry.observedYears}`,
    );
  const globalIssue = parts.globalEvalPresent
    ? []
    : [`FAIL global-eval: expected ${EXPECTED_GLOBAL_ROWS} rows in ${EVALUATIONS_TABLE}`];
  const top1Issues = parts.top1
    .filter((entry) => !entry.withinBand)
    .map(
      (entry) =>
        `WARN top1-plausibility ${entry.category}: ${formatRate(entry.top1Rate)} outside [${formatRate(entry.low)}, ${formatRate(entry.high)}]`,
    );
  const crosscheckIssues = parts.crosschecks
    .filter((entry) => !entry.allWithinTolerance)
    .map(
      (entry) =>
        `WARN race-count-crosscheck ${entry.source}: worst relative delta ${formatRate(entry.worstRelativeDelta)} exceeds ${formatRate(RACE_COUNT_TOLERANCE)}`,
    );
  return [...coverageIssues, ...globalIssue, ...top1Issues, ...crosscheckIssues];
};

export const deriveOverall = (issues: string[]): OverallStatus => {
  if (issues.some((issue) => issue.startsWith("FAIL"))) return "fail";
  if (issues.length > 0) return "warn";
  return "pass";
};

export const buildReport = (parts: {
  coverage: FoldCoverageAssessment[];
  top1: Top1PlausibilityAssessment[];
  crosschecks: RaceCountCrosscheckResult[];
  globalEvalPresent: boolean;
  activeModels: ActiveModelsSummary;
  cron: CronSummary;
}): VerifyReport => {
  const issues = collectIssues({
    coverage: parts.coverage,
    top1: parts.top1,
    crosschecks: parts.crosschecks,
    globalEvalPresent: parts.globalEvalPresent,
  });
  return {
    coverage: parts.coverage,
    top1_plausibility: parts.top1,
    race_count_crosscheck: parts.crosschecks,
    global_eval_present: parts.globalEvalPresent,
    active_models: parts.activeModels,
    cron_last_run: parts.cron,
    overall: deriveOverall(issues),
    issues,
  };
};

export const buildUsageText = (): string =>
  [
    "Usage:",
    "  bun run src/scripts/finish-position-features/verify-finish-position-v7lineage-coverage.ts \\",
    "    [--pg-url <connection-string>] [--out tmp/stage-8-verify-<ts>.json]",
  ].join("\n");

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value.`);
  return value;
};

export const initialOptions = (): VerifyCliOptions => ({
  pgUrl: process.env.DATABASE_URL_LOCAL ?? DEFAULT_PG_URL,
  out: null,
});

interface ApplyArgResult {
  advanceBy: number;
}

const applyArg = (
  options: VerifyCliOptions,
  name: string,
  value: string | undefined,
): ApplyArgResult => {
  if (name === "--pg-url") {
    options.pgUrl = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--out") {
    options.out = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--help" || name === "-h") {
    console.log(buildUsageText());
    process.exit(0);
  }
  throw new Error(`Unknown argument: ${name}`);
};

const consumeArgs = (
  options: VerifyCliOptions,
  argv: readonly string[],
  cursor: number,
): VerifyCliOptions => {
  const name = argv[cursor];
  if (name === undefined) return options;
  const { advanceBy } = applyArg(options, name, argv[cursor + 1]);
  return consumeArgs(options, argv, cursor + advanceBy);
};

export const parseArgs = (argv: readonly string[]): VerifyCliOptions =>
  consumeArgs(initialOptions(), argv, 0);

const queryRows = async <Row>(
  runner: QueryRunner,
  sql: string,
  params?: unknown[],
): Promise<Row[]> => {
  const result = await runner.query<Row>(sql, params);
  return result.rows;
};

const gatherCoverage = (
  expectation: CategoryExpectation,
  runner: QueryRunner,
): Promise<FoldCoverageAssessment> =>
  queryRows<FoldCoverageRow>(runner, buildFoldCoverageSql(expectation.modelVersion)).then((rows) =>
    assessFoldCoverage(expectation, rows),
  );

const gatherTop1 = (
  expectation: CategoryExpectation,
  runner: QueryRunner,
): Promise<Top1PlausibilityAssessment> =>
  queryRows<Top1PlausibilityRow>(runner, buildTop1PlausibilitySql(expectation.modelVersion)).then(
    (rows) => assessTop1Plausibility(expectation, rows),
  );

const gatherCrosscheck = async (
  expectation: CategoryExpectation,
  runner: QueryRunner,
): Promise<RaceCountCrosscheckResult> => {
  const bucketRows = await queryRows<BucketYearRaceCountRow>(
    runner,
    buildBucketYearRaceCountSql(expectation.modelVersion),
  );
  const actualRows = await queryRows<ActualYearRaceCountRow>(
    runner,
    buildActualRaceCountSql(expectation.source),
  );
  return assessRaceCountCrosscheck({
    source: expectation.source,
    storedSource: expectation.storedSource,
    bucketRows,
    actualRows,
  });
};

const gatherCron = async (runner: QueryRunner): Promise<CronSummary> => {
  const existsRows = await queryRows<TableExistsRow>(runner, buildCronTableExistsSql());
  const tableExists = existsRows[0]?.exists === true;
  if (!tableExists) return summarizeCron(false, []);
  const cronRows = await queryRows<CronExecutionRow>(runner, buildCronLastRunSql());
  return summarizeCron(true, cronRows);
};

export const runVerification = async (deps: VerifyDeps): Promise<VerifyReport> => {
  const coverage = await Promise.all(
    CATEGORY_EXPECTATIONS.map((expectation) => gatherCoverage(expectation, deps.runner)),
  );
  const top1 = await Promise.all(
    CATEGORY_EXPECTATIONS.map((expectation) => gatherTop1(expectation, deps.runner)),
  );
  const crosschecks = await Promise.all(
    CATEGORY_EXPECTATIONS.map((expectation) => gatherCrosscheck(expectation, deps.runner)),
  );
  const globalRows = await queryRows<GlobalEvalRow>(deps.runner, buildGlobalEvalSql());
  const activeRows = await queryRows<ActiveModelRow>(deps.runner, buildActiveModelsSql());
  const cron = await gatherCron(deps.runner);
  return buildReport({
    coverage,
    top1,
    crosschecks,
    globalEvalPresent: assessGlobalEvalPresence(globalRows),
    activeModels: summarizeActiveModels(activeRows),
    cron,
  });
};

export const emitReport = async (
  deps: VerifyDeps,
  options: VerifyCliOptions,
  report: VerifyReport,
): Promise<void> => {
  const body = JSON.stringify(report, null, 2);
  deps.log(body);
  if (options.out !== null) await deps.writeOut(options.out, body);
};

export const exitCodeFor = (overall: OverallStatus): number => (overall === "fail" ? 1 : 0);

export const defaultLog = (message: string): void => {
  console.log(message);
};

export const defaultWriteOut = (path: string, body: string): Promise<void> =>
  writeFile(path, `${body}\n`, "utf-8");

export interface CliDeps {
  openPool: (pgUrl: string) => QueryRunner & { end: () => Promise<void> };
  log: (message: string) => void;
  writeOut: (path: string, body: string) => Promise<void>;
}

export const runCli = async (cliDeps: CliDeps, options: VerifyCliOptions): Promise<number> => {
  const pool = cliDeps.openPool(options.pgUrl);
  try {
    const deps: VerifyDeps = { runner: pool, log: cliDeps.log, writeOut: cliDeps.writeOut };
    const report = await runVerification(deps);
    await emitReport(deps, options, report);
    return exitCodeFor(report.overall);
  } finally {
    await pool.end();
  }
};

const openPgPool = (pgUrl: string): QueryRunner & { end: () => Promise<void> } =>
  new Pool({ connectionString: pgUrl });

const main = async (): Promise<void> => {
  const options = parseArgs(process.argv.slice(2));
  const cliDeps: CliDeps = { openPool: openPgPool, log: defaultLog, writeOut: defaultWriteOut };
  const code = await runCli(cliDeps, options);
  process.exit(code);
};

if (import.meta.main) {
  main().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}

export { CATEGORY_EXPECTATIONS, escapeSqlLiteral };
