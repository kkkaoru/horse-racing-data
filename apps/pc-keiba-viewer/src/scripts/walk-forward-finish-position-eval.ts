// Run with: bun run src/scripts/walk-forward-finish-position-eval.ts \
//   --tuning-config src/scripts/finish-position-tuning/default-parameters.json \
//   --category jra --holdout-years 2020,2021,2022,2023,2024,2025

import { spawn } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

import {
  AGGREGATE_METRIC_KEYS,
  type AggregateMetricKey,
  type AggregateMetrics,
  type AggregateStats,
  type CliArgs,
  type CompareFinishJson,
  type CompareFinishRunner,
  type FoldResult,
  type ReportPayload,
} from "./walk-forward-finish-position-eval-types";

const DAYS_END_OF_YEAR = "1231";
const DAYS_START_OF_YEAR = "0101";
const COMPARE_FINISH_SCRIPT = "src/scripts/compare-finish-position-predictions.ts";
const STDEV_DENOM_FALLBACK = 1;
const ROUND_DENOM = 100;
const MIN_YEAR = 1900;
const MAX_YEAR = 2999;

const CATEGORY_SET = new Set<CliArgs["category"]>(["all", "ban-ei", "jra", "nar"]);
const TARGET_SET = new Set<CliArgs["target"]>(["local", "neon"]);

const isCategory = (value: string): value is CliArgs["category"] => {
  for (const candidate of CATEGORY_SET) {
    if (candidate === value) return true;
  }
  return false;
};

const isTarget = (value: string): value is CliArgs["target"] => {
  for (const candidate of TARGET_SET) {
    if (candidate === value) return true;
  }
  return false;
};

const parseYearList = (raw: string): number[] => {
  const parsed = raw
    .split(",")
    .map((piece) => piece.trim())
    .filter((piece) => piece.length > 0)
    .map((piece) => Number.parseInt(piece, 10));
  if (parsed.some((value) => Number.isNaN(value) || value < MIN_YEAR || value > MAX_YEAR)) {
    throw new Error(`Invalid year in --holdout-years: ${raw}`);
  }
  return [...new Set(parsed)].toSorted((left, right) => left - right);
};

const buildUsageText = (): string =>
  [
    "Usage:",
    "  bun run src/scripts/walk-forward-finish-position-eval.ts \\",
    "    --tuning-config <path> --holdout-years 2022,2023,2024 [--category jra] \\",
    "    [--target local|neon] [--concurrency N] [--output report.json]",
    "",
    "Runs compare-finish-position-predictions per holdout year and aggregates metrics.",
  ].join("\n");

const initialArgs = (): CliArgs => ({
  category: "jra",
  concurrency: 6,
  holdoutYears: [],
  outputPath: null,
  target: "local",
  tuningConfigPath: "",
});

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value.`);
  return value;
};

const applyArg = (
  args: CliArgs,
  name: string,
  value: string | undefined,
): { advanceBy: number } => {
  if (name === "--tuning-config") {
    args.tuningConfigPath = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--category") {
    const raw = requireValue(name, value);
    if (!isCategory(raw)) throw new Error("--category must be all, jra, nar, or ban-ei.");
    args.category = raw;
    return { advanceBy: 2 };
  }
  if (name === "--target") {
    const raw = requireValue(name, value);
    if (!isTarget(raw)) throw new Error("--target must be local or neon.");
    args.target = raw;
    return { advanceBy: 2 };
  }
  if (name === "--holdout-years") {
    args.holdoutYears = parseYearList(requireValue(name, value));
    return { advanceBy: 2 };
  }
  if (name === "--concurrency") {
    args.concurrency = Math.max(1, Number.parseInt(requireValue(name, value), 10));
    return { advanceBy: 2 };
  }
  if (name === "--output") {
    args.outputPath = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--help" || name === "-h") {
    console.log(buildUsageText());
    process.exit(0);
  }
  throw new Error(`Unknown argument: ${name}`);
};

const parseArgs = (argv: readonly string[]): CliArgs => {
  const args = initialArgs();
  let cursor = 0;
  while (cursor < argv.length) {
    const name = argv[cursor];
    if (name === undefined) break;
    const { advanceBy } = applyArg(args, name, argv[cursor + 1]);
    cursor += advanceBy;
  }
  if (args.tuningConfigPath === "") throw new Error("--tuning-config is required.");
  if (args.holdoutYears.length === 0) {
    throw new Error("--holdout-years must list at least one year.");
  }
  return args;
};

const formatRange = (year: number): { fromDate: string; toDate: string } => ({
  fromDate: `${year}${DAYS_START_OF_YEAR}`,
  toDate: `${year}${DAYS_END_OF_YEAR}`,
});

const buildCompareArgs = (args: CliArgs, year: number): string[] => {
  const range = formatRange(year);
  return [
    "run",
    COMPARE_FINISH_SCRIPT,
    "--target",
    args.target,
    "--category",
    args.category,
    "--from-date",
    range.fromDate,
    "--to-date",
    range.toDate,
    "--concurrency",
    String(args.concurrency),
    "--tuning-config",
    args.tuningConfigPath,
  ];
};

const spawnCompareFinish: CompareFinishRunner = async (year, compareArgs) => {
  const stdoutChunks: Buffer[] = [];
  const stderrChunks: Buffer[] = [];
  const child = spawn("bun", [...compareArgs], { stdio: ["ignore", "pipe", "pipe"] });
  child.stdout.on("data", (chunk: Buffer) => stdoutChunks.push(chunk));
  child.stderr.on("data", (chunk: Buffer) => stderrChunks.push(chunk));
  const exitCode = await new Promise<number>((resolvePromise, rejectPromise) => {
    child.once("error", rejectPromise);
    child.once("close", (code) => resolvePromise(code ?? 0));
  });
  if (exitCode !== 0) {
    const stderr = Buffer.concat(stderrChunks).toString("utf8");
    throw new Error(`compare-finish exited with code ${exitCode} for year ${year}: ${stderr}`);
  }
  return Buffer.concat(stdoutChunks).toString("utf8");
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const REQUIRED_JSON_NUMBER_KEYS: readonly (keyof CompareFinishJson)[] = [
  "pairScore",
  "place1Accuracy",
  "place2Accuracy",
  "place3Accuracy",
  "raceCount",
  "top1Accuracy",
  "top3BoxAccuracy",
  "top3ExactOrderAccuracy",
  "top3PlaceRelation",
  "top3WinnerCapture",
  "top5WinnerCapture",
];

const REQUIRED_JSON_STRING_KEYS: readonly (keyof CompareFinishJson)[] = ["fromDate", "toDate"];

const validateCompareFinishJson = (raw: unknown): CompareFinishJson => {
  if (!isRecord(raw)) throw new Error("compare-finish output is not an object.");
  for (const key of REQUIRED_JSON_NUMBER_KEYS) {
    if (typeof raw[key] !== "number") {
      throw new Error(`compare-finish output missing number field: ${key}`);
    }
  }
  for (const key of REQUIRED_JSON_STRING_KEYS) {
    if (typeof raw[key] !== "string") {
      throw new Error(`compare-finish output missing string field: ${key}`);
    }
  }
  return {
    fromDate: String(raw.fromDate),
    pairScore: Number(raw.pairScore),
    place1Accuracy: Number(raw.place1Accuracy),
    place2Accuracy: Number(raw.place2Accuracy),
    place3Accuracy: Number(raw.place3Accuracy),
    raceCount: Number(raw.raceCount),
    toDate: String(raw.toDate),
    top1Accuracy: Number(raw.top1Accuracy),
    top3BoxAccuracy: Number(raw.top3BoxAccuracy),
    top3ExactOrderAccuracy: Number(raw.top3ExactOrderAccuracy),
    top3PlaceRelation: Number(raw.top3PlaceRelation),
    top3WinnerCapture: Number(raw.top3WinnerCapture),
    top5WinnerCapture: Number(raw.top5WinnerCapture),
  };
};

const extractJson = (raw: string): CompareFinishJson => {
  const trimmed = raw.trim();
  const startIndex = trimmed.indexOf("{");
  const endIndex = trimmed.lastIndexOf("}");
  if (startIndex < 0 || endIndex < startIndex) {
    throw new Error(`compare-finish output is not JSON: ${raw.slice(0, 200)}`);
  }
  const parsed: unknown = JSON.parse(trimmed.slice(startIndex, endIndex + 1));
  return validateCompareFinishJson(parsed);
};

const toFoldResult = (year: number, payload: CompareFinishJson): FoldResult => ({
  fromDate: payload.fromDate,
  pairScore: payload.pairScore,
  place1Accuracy: payload.place1Accuracy,
  place2Accuracy: payload.place2Accuracy,
  place3Accuracy: payload.place3Accuracy,
  raceCount: payload.raceCount,
  toDate: payload.toDate,
  top1Accuracy: payload.top1Accuracy,
  top3BoxAccuracy: payload.top3BoxAccuracy,
  top3ExactOrderAccuracy: payload.top3ExactOrderAccuracy,
  top3PlaceRelation: payload.top3PlaceRelation,
  top3WinnerCapture: payload.top3WinnerCapture,
  top5WinnerCapture: payload.top5WinnerCapture,
  year,
});

const round2 = (value: number): number => Math.round(value * ROUND_DENOM) / ROUND_DENOM;

const computeStats = (values: readonly number[]): AggregateStats => {
  if (values.length === 0) return { count: 0, max: 0, mean: 0, min: 0, stdev: 0 };
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  const variance =
    values.reduce((sum, value) => sum + (value - mean) ** 2, 0) /
    Math.max(values.length - 1, STDEV_DENOM_FALLBACK);
  return {
    count: values.length,
    max: round2(Math.max(...values)),
    mean: round2(mean),
    min: round2(Math.min(...values)),
    stdev: round2(Math.sqrt(variance)),
  };
};

const buildAggregateForKey = (
  key: AggregateMetricKey,
  folds: readonly FoldResult[],
): AggregateStats => computeStats(folds.map((fold) => fold[key]));

const buildAggregate = (folds: readonly FoldResult[]): AggregateMetrics => ({
  pairScore: buildAggregateForKey("pairScore", folds),
  place1Accuracy: buildAggregateForKey("place1Accuracy", folds),
  place2Accuracy: buildAggregateForKey("place2Accuracy", folds),
  place3Accuracy: buildAggregateForKey("place3Accuracy", folds),
  raceCount: buildAggregateForKey("raceCount", folds),
  top1Accuracy: buildAggregateForKey("top1Accuracy", folds),
  top3BoxAccuracy: buildAggregateForKey("top3BoxAccuracy", folds),
  top3ExactOrderAccuracy: buildAggregateForKey("top3ExactOrderAccuracy", folds),
  top3PlaceRelation: buildAggregateForKey("top3PlaceRelation", folds),
  top3WinnerCapture: buildAggregateForKey("top3WinnerCapture", folds),
  top5WinnerCapture: buildAggregateForKey("top5WinnerCapture", folds),
});

const runFolds = (args: CliArgs, runner: CompareFinishRunner): Promise<FoldResult[]> =>
  args.holdoutYears.reduce<Promise<FoldResult[]>>(
    (accPromise, year) =>
      accPromise.then(async (acc) => {
        const stdout = await runner(year, buildCompareArgs(args, year));
        return [...acc, toFoldResult(year, extractJson(stdout))];
      }),
    Promise.resolve([]),
  );

const buildReport = (args: CliArgs, folds: FoldResult[]): ReportPayload => ({
  aggregate: buildAggregate(folds),
  category: args.category,
  folds,
  generatedAt: new Date().toISOString(),
  target: args.target,
  tuningConfigPath: args.tuningConfigPath,
});

const writeReport = async (report: ReportPayload, outputPath: string | null): Promise<void> => {
  const serialized = JSON.stringify(report, null, 2);
  if (outputPath === null) {
    console.log(serialized);
    return;
  }
  const absolutePath = resolve(process.cwd(), outputPath);
  await mkdir(dirname(absolutePath), { recursive: true });
  await writeFile(absolutePath, `${serialized}\n`, "utf8");
};

const logFoldSummary = (fold: FoldResult): void => {
  console.error(
    `[walk-forward] year=${fold.year} races=${fold.raceCount} pairScore=${fold.pairScore} place1=${fold.place1Accuracy} top3Box=${fold.top3BoxAccuracy} top3Exact=${fold.top3ExactOrderAccuracy}`,
  );
};

const main = async (): Promise<void> => {
  const args = parseArgs(process.argv.slice(2));
  const folds = await runFolds(args, spawnCompareFinish);
  folds.forEach((fold) => logFoldSummary(fold));
  const report = buildReport(args, folds);
  await writeReport(report, args.outputPath);
};

if (import.meta.main) {
  main().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}

export {
  AGGREGATE_METRIC_KEYS,
  buildAggregate,
  buildAggregateForKey,
  buildCompareArgs,
  buildReport,
  buildUsageText,
  computeStats,
  extractJson,
  formatRange,
  isCategory,
  isTarget,
  parseArgs,
  parseYearList,
  round2,
  runFolds,
  toFoldResult,
  validateCompareFinishJson,
};
