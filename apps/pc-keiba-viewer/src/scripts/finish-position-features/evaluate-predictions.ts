// Run with: bun run src/scripts/finish-position-features/evaluate-predictions.ts \
//   --target local --model-version lambdarank-jra-v1 --category jra \
//   --from-date 20260101 --to-date 20260510

import { Pool } from "pg";

import { getConnectionString, loadEnv } from "../compare-corner-predictions";
import type { FeatureCategory, FeatureTarget } from "./build-finish-position-features-types";
import {
  buildAggregateMetricsSql,
  buildEvaluationsDdl,
  buildUpsertSql,
} from "./evaluate-predictions-sql";

const CATEGORY_SET = new Set<FeatureCategory>(["all", "ban-ei", "jra", "nar"]);
const TARGET_SET = new Set<FeatureTarget>(["local", "neon"]);

interface EvalOptions {
  category: FeatureCategory;
  fromDate: string;
  modelVersion: string;
  target: FeatureTarget;
  toDate: string;
}

interface MetricsRow {
  race_count: number;
  prediction_count: number;
  top1_accuracy: string | null;
  top3_box_accuracy: string | null;
  top3_exact_accuracy: string | null;
  place1_accuracy: string | null;
  place2_accuracy: string | null;
  place3_accuracy: string | null;
  top3_winner_capture: string | null;
  top5_winner_capture: string | null;
  pair_score: string | null;
  ndcg_at_3: string | null;
}

const isCategory = (value: string): value is FeatureCategory => {
  for (const candidate of CATEGORY_SET) {
    if (candidate === value) return true;
  }
  return false;
};

const isTarget = (value: string): value is FeatureTarget => {
  for (const candidate of TARGET_SET) {
    if (candidate === value) return true;
  }
  return false;
};

const buildUsageText = (): string =>
  [
    "Usage:",
    "  bun run src/scripts/finish-position-features/evaluate-predictions.ts \\",
    "    --model-version <id> --category jra|nar|ban-ei \\",
    "    --from-date YYYYMMDD --to-date YYYYMMDD [--target local|neon]",
  ].join("\n");

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value.`);
  return value;
};

const initialOptions = (): EvalOptions => ({
  category: "jra",
  fromDate: "",
  modelVersion: "",
  target: "local",
  toDate: "",
});

const applyArg = (
  options: EvalOptions,
  name: string,
  value: string | undefined,
): { advanceBy: number } => {
  if (name === "--target") {
    const raw = requireValue(name, value);
    if (!isTarget(raw)) throw new Error("--target must be local or neon.");
    options.target = raw;
    return { advanceBy: 2 };
  }
  if (name === "--category") {
    const raw = requireValue(name, value);
    if (!isCategory(raw)) throw new Error("--category must be all, jra, nar, or ban-ei.");
    options.category = raw;
    return { advanceBy: 2 };
  }
  if (name === "--model-version") {
    options.modelVersion = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--from-date") {
    options.fromDate = requireValue(name, value).replaceAll("-", "");
    return { advanceBy: 2 };
  }
  if (name === "--to-date") {
    options.toDate = requireValue(name, value).replaceAll("-", "");
    return { advanceBy: 2 };
  }
  if (name === "--help" || name === "-h") {
    console.log(buildUsageText());
    process.exit(0);
  }
  throw new Error(`Unknown argument: ${name}`);
};

export const parseArgs = (argv: readonly string[]): EvalOptions => {
  const options = initialOptions();
  let cursor = 0;
  while (cursor < argv.length) {
    const name = argv[cursor];
    if (name === undefined) break;
    const { advanceBy } = applyArg(options, name, argv[cursor + 1]);
    cursor += advanceBy;
  }
  if (options.modelVersion === "") throw new Error("--model-version is required.");
  if (options.fromDate === "") throw new Error("--from-date is required.");
  if (options.toDate === "") throw new Error("--to-date is required.");
  return options;
};

const ensureTable = async (pool: Pool): Promise<void> => {
  await pool.query(buildEvaluationsDdl());
};

const computeMetrics = async (pool: Pool, options: EvalOptions): Promise<MetricsRow> => {
  const sql = buildAggregateMetricsSql({
    category: options.category,
    fromDate: options.fromDate,
    modelVersion: options.modelVersion,
    toDate: options.toDate,
  });
  const result = await pool.query<MetricsRow>(sql);
  const row = result.rows[0];
  if (row === undefined) throw new Error("Empty metrics result.");
  return row;
};

const persistMetrics = async (
  pool: Pool,
  options: EvalOptions,
  metrics: MetricsRow,
): Promise<void> => {
  await pool.query(buildUpsertSql(), [
    options.modelVersion,
    options.category,
    options.fromDate,
    options.toDate,
    metrics.race_count,
    metrics.prediction_count,
    metrics.top1_accuracy,
    metrics.top3_box_accuracy,
    metrics.top3_exact_accuracy,
    metrics.place1_accuracy,
    metrics.place2_accuracy,
    metrics.place3_accuracy,
    metrics.top3_winner_capture,
    metrics.top5_winner_capture,
    metrics.pair_score,
    metrics.ndcg_at_3,
  ]);
};

const main = async (): Promise<void> => {
  const options = parseArgs(process.argv.slice(2));
  await loadEnv();
  const pool = new Pool({ connectionString: getConnectionString(options.target) });
  try {
    await ensureTable(pool);
    const metrics = await computeMetrics(pool, options);
    await persistMetrics(pool, options, metrics);
    console.log(
      JSON.stringify({
        category: options.category,
        evaluation_window: `${options.fromDate}..${options.toDate}`,
        metrics,
        model_version: options.modelVersion,
        target: options.target,
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

export { buildUsageText, initialOptions, isCategory, isTarget };
