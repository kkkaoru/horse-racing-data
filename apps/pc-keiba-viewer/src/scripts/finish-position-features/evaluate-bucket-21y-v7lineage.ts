// Run with: bun run src/scripts/finish-position-features/evaluate-bucket-21y-v7lineage.ts \
//   --pg-url $DATABASE_URL_LOCAL --running-style-feature-version v3 --finish-position-version v1
//
// Stage 4 of the finish-position v7-lineage walk-forward 21y plan. A fork of
// evaluate-bucket-21y.ts that (1) reads Stage 3's per-fold predictions parquet
// from the v7-lineage WF root, (2) runs the shared bucket-eval aggregate + REPLACE
// upsert once per category with that category's WF model_version, then (3) rolls
// the per-bucket sums up into one global model_prediction_evaluations row per
// category. The bucket-eval aggregate / upsert SQL and the RPC chunk client are
// reused verbatim from evaluate-bucket-21y.ts / evaluate-bucket-predictions-sql.ts.

import { Pool } from "pg";

import { createBucketEvalRpcClient } from "./bucket-eval-rpc-client";
import type { BucketEvalRpcChildLike } from "./bucket-eval-rpc-client";
import type {
  BucketChunkClient,
  BucketChunkLoaderArgs,
  BucketEvalCliOptions,
  BucketQueryRunner,
  CategoryYearWindow,
  RunBucketEvalDeps,
} from "./evaluate-bucket-21y";
import {
  buildPythonLoaderArgv,
  initialOptions as baseInitialOptions,
  runBucketEval,
} from "./evaluate-bucket-21y";
import { buildBucketEvaluationsDdl } from "./evaluate-bucket-predictions-sql";
import { buildEvaluationsAlterColumnsSql, buildEvaluationsDdl } from "./evaluate-predictions-sql";
import { FINISH_POSITION_V7_LINEAGE_MODEL_VERSIONS } from "./v7-lineage-model-versions";

export type BucketCategory = "jra" | "nar" | "ban-ei";

export interface V7LineageCliOptions {
  pgUrl: string;
  runningStyleFeatureVersion: string;
  finishPositionVersion: string;
  modelVersionJra: string;
  modelVersionNar: string;
  modelVersionBanei: string;
  maxYearsPerRun: number;
  statementTimeoutMs: number;
  ignoreNightWindow: boolean;
  predictionsRoot: string;
}

export interface CategoryRunPlan {
  category: BucketCategory;
  modelVersion: string;
  years: number[];
}

export interface GlobalRollupRow {
  race_count: string | number;
  prediction_count: string | number;
  top1_accuracy: string | null;
  place1_accuracy: string | null;
  place2_accuracy: string | null;
  place3_accuracy: string | null;
  top3_box_accuracy: string | null;
  top3_exact_accuracy: string | null;
  top3_winner_capture: string | null;
  top5_winner_capture: string | null;
  top3_place_relation: string | null;
  pair_score: string | null;
  ndcg_at_3: string | null;
}

export interface GlobalRollupArgs {
  category: BucketCategory;
  modelVersion: string;
  runningStyleFeatureVersion: string;
  finishPositionVersion: string;
  windowFrom: string;
  windowTo: string;
}

const DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing";
const DEFAULT_MAX_YEARS_PER_RUN = 5;
const DEFAULT_STATEMENT_TIMEOUT_MS = 900_000;
const DEFAULT_PREDICTIONS_ROOT =
  "apps/pc-keiba-viewer/tmp/bucket-eval/finish-position/v7-lineage-wf-21y/predictions";
const BUCKET_TABLE = "model_prediction_bucket_evaluations";
const EVALUATIONS_TABLE = "model_prediction_evaluations";
const JANUARY_FIRST_SUFFIX = "0101";
const DECEMBER_LAST_SUFFIX = "1231";

const JRA_YEARS = [
  2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022,
  2023, 2024, 2025, 2026,
];
const NAR_YEARS = [
  2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022,
  2023, 2024, 2025, 2026,
];
const BAN_EI_YEARS = [
  2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023,
  2024, 2025, 2026,
];

export const buildUsageText = (): string =>
  [
    "Usage:",
    "  bun run src/scripts/finish-position-features/evaluate-bucket-21y-v7lineage.ts \\",
    "    --pg-url <connection-string> \\",
    "    --running-style-feature-version v3 \\",
    "    --finish-position-version v1 \\",
    "    [--model-version-jra jra-cb-v7-lineage-wf-21y] \\",
    "    [--model-version-nar nar-xgb-v7-lineage-wf-21y] \\",
    "    [--model-version-banei banei-cb-v7-lineage-wf-21y] \\",
    "    [--predictions-root <dir>] \\",
    "    [--max-years-per-run 5] \\",
    "    [--statement-timeout-ms 900000] \\",
    "    [--ignore-night-window]",
  ].join("\n");

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value.`);
  return value;
};

export const initialOptions = (): V7LineageCliOptions => ({
  pgUrl: process.env.DATABASE_URL_LOCAL ?? DEFAULT_PG_URL,
  runningStyleFeatureVersion: "",
  finishPositionVersion: "",
  modelVersionJra: FINISH_POSITION_V7_LINEAGE_MODEL_VERSIONS.jra,
  modelVersionNar: FINISH_POSITION_V7_LINEAGE_MODEL_VERSIONS.nar,
  modelVersionBanei: FINISH_POSITION_V7_LINEAGE_MODEL_VERSIONS.banei,
  maxYearsPerRun: DEFAULT_MAX_YEARS_PER_RUN,
  statementTimeoutMs: DEFAULT_STATEMENT_TIMEOUT_MS,
  ignoreNightWindow: false,
  predictionsRoot: DEFAULT_PREDICTIONS_ROOT,
});

interface ApplyArgResult {
  advanceBy: number;
}

const applyArg = (
  options: V7LineageCliOptions,
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
  if (name === "--model-version-jra") {
    options.modelVersionJra = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--model-version-nar") {
    options.modelVersionNar = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--model-version-banei") {
    options.modelVersionBanei = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--predictions-root") {
    options.predictionsRoot = requireValue(name, value);
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
  options: V7LineageCliOptions,
  argv: readonly string[],
  cursor: number,
): V7LineageCliOptions => {
  const name = argv[cursor];
  if (name === undefined) return options;
  const { advanceBy } = applyArg(options, name, argv[cursor + 1]);
  return consumeArgs(options, argv, cursor + advanceBy);
};

export const parseArgs = (argv: readonly string[]): V7LineageCliOptions => {
  const options = consumeArgs(initialOptions(), argv, 0);
  if (options.runningStyleFeatureVersion === "") {
    throw new Error("--running-style-feature-version is required.");
  }
  if (options.finishPositionVersion === "") {
    throw new Error("--finish-position-version is required.");
  }
  return options;
};

export const resolveModelVersion = (
  options: V7LineageCliOptions,
  category: BucketCategory,
): string => {
  if (category === "jra") return options.modelVersionJra;
  if (category === "nar") return options.modelVersionNar;
  return options.modelVersionBanei;
};

export const buildCategoryRunPlans = (options: V7LineageCliOptions): CategoryRunPlan[] => [
  { category: "jra", modelVersion: options.modelVersionJra, years: JRA_YEARS },
  { category: "nar", modelVersion: options.modelVersionNar, years: NAR_YEARS },
  { category: "ban-ei", modelVersion: options.modelVersionBanei, years: BAN_EI_YEARS },
];

export const buildPlanWindowDates = (years: number[]): { windowFrom: string; windowTo: string } => {
  const first = years[0] ?? 0;
  const last = years.at(-1) ?? 0;
  return {
    windowFrom: `${first}${JANUARY_FIRST_SUFFIX}`,
    windowTo: `${last}${DECEMBER_LAST_SUFFIX}`,
  };
};

export const buildBaseBucketOptions = (
  options: V7LineageCliOptions,
  modelVersion: string,
): BucketEvalCliOptions => ({
  ...baseInitialOptions(),
  pgUrl: options.pgUrl,
  runningStyleFeatureVersion: options.runningStyleFeatureVersion,
  finishPositionVersion: options.finishPositionVersion,
  modelVersion,
  maxYearsPerRun: options.maxYearsPerRun,
  statementTimeoutMs: options.statementTimeoutMs,
  ignoreNightWindow: options.ignoreNightWindow,
  predictionsRoot: options.predictionsRoot,
});

const escapeSqlLiteral = (value: string): string => value.replaceAll("'", "''");

export const buildGlobalRollupSql = (args: GlobalRollupArgs): string => `
    with buckets as (
      select *
      from ${BUCKET_TABLE}
      where model_version = '${escapeSqlLiteral(args.modelVersion)}'
        and running_style_feature_version = '${escapeSqlLiteral(args.runningStyleFeatureVersion)}'
        and finish_position_version = '${escapeSqlLiteral(args.finishPositionVersion)}'
        and category = '${escapeSqlLiteral(args.category)}'
    )
    select
      coalesce(sum(race_count), 0) race_count,
      coalesce(sum(prediction_count), 0) prediction_count,
      case when sum(race_count) > 0 then sum(top1_hit_sum) / sum(race_count) end top1_accuracy,
      case when sum(race_count) > 0 then sum(place1_hit_sum) / sum(race_count) end place1_accuracy,
      case when sum(race_count) > 0 then sum(place2_hit_sum) / sum(race_count) end place2_accuracy,
      case when sum(race_count) > 0 then sum(place3_hit_sum) / sum(race_count) end place3_accuracy,
      case when sum(race_count) > 0 then sum(top3_box_hit_sum) / sum(race_count) end top3_box_accuracy,
      case when sum(race_count) > 0 then sum(top3_exact_hit_sum) / sum(race_count) end top3_exact_accuracy,
      case when sum(race_count) > 0 then sum(top3_winner_capture_sum) / sum(race_count) end top3_winner_capture,
      case when sum(race_count) > 0 then sum(top5_winner_capture_sum) / sum(race_count) end top5_winner_capture,
      case when sum(race_count) > 0 then sum(top3_place_relation_sum) / sum(race_count) end top3_place_relation,
      case when sum(pair_score_pair_count) > 0 then sum(pair_score_sum) / sum(pair_score_pair_count) end pair_score,
      case when sum(ndcg_at_3_race_count) > 0 then sum(ndcg_at_3_sum) / sum(ndcg_at_3_race_count) end ndcg_at_3
    from buckets
  `;

export const buildGlobalUpsertSql = (): string => `
    insert into ${EVALUATIONS_TABLE} (
      model_version, category, evaluation_window_from, evaluation_window_to,
      race_count, prediction_count,
      top1_accuracy, top3_box_accuracy, top3_exact_accuracy,
      place1_accuracy, place2_accuracy, place3_accuracy,
      top3_winner_capture, top5_winner_capture,
      pair_score, ndcg_at_3, top3_place_relation, evaluated_at
    )
    values (
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, now()
    )
    on conflict (model_version, category, evaluation_window_from, evaluation_window_to)
    do update set
      race_count = excluded.race_count,
      prediction_count = excluded.prediction_count,
      top1_accuracy = excluded.top1_accuracy,
      top3_box_accuracy = excluded.top3_box_accuracy,
      top3_exact_accuracy = excluded.top3_exact_accuracy,
      place1_accuracy = excluded.place1_accuracy,
      place2_accuracy = excluded.place2_accuracy,
      place3_accuracy = excluded.place3_accuracy,
      top3_winner_capture = excluded.top3_winner_capture,
      top5_winner_capture = excluded.top5_winner_capture,
      pair_score = excluded.pair_score,
      ndcg_at_3 = excluded.ndcg_at_3,
      top3_place_relation = excluded.top3_place_relation,
      evaluated_at = now()
  `;

export const buildGlobalUpsertParams = (
  args: GlobalRollupArgs,
  row: GlobalRollupRow,
): unknown[] => [
  args.modelVersion,
  args.category,
  args.windowFrom,
  args.windowTo,
  row.race_count,
  row.prediction_count,
  row.top1_accuracy,
  row.top3_box_accuracy,
  row.top3_exact_accuracy,
  row.place1_accuracy,
  row.place2_accuracy,
  row.place3_accuracy,
  row.top3_winner_capture,
  row.top5_winner_capture,
  row.pair_score,
  row.ndcg_at_3,
  row.top3_place_relation,
];

export interface RollupDeps {
  runner: BucketQueryRunner;
  log: (message: string) => void;
}

export const rollupCategoryToGlobal = async (
  deps: RollupDeps,
  args: GlobalRollupArgs,
): Promise<GlobalRollupRow | null> => {
  const result = await deps.runner.query<GlobalRollupRow>(buildGlobalRollupSql(args));
  const row = result.rows[0];
  if (row === undefined) {
    deps.log(`No bucket rows to roll up for ${args.category}`);
    return null;
  }
  await deps.runner.query(buildGlobalUpsertSql(), buildGlobalUpsertParams(args, row));
  deps.log(`Rolled up ${args.category} into ${EVALUATIONS_TABLE} (races=${row.race_count})`);
  return row;
};

export interface RunV7LineageDeps {
  pool: BucketQueryRunner;
  openChunkClient: (args: BucketChunkLoaderArgs) => Promise<BucketChunkClient>;
  sleep: (ms: number) => Promise<void>;
  log: (message: string) => void;
}

const ensureGlobalEvaluationsTable = async (pool: BucketQueryRunner): Promise<void> => {
  await pool.query(buildEvaluationsDdl());
  await pool.query(buildEvaluationsAlterColumnsSql());
};

const ensureBucketTable = async (pool: BucketQueryRunner): Promise<void> => {
  await pool.query(buildBucketEvaluationsDdl());
};

const buildCategoryWindow = (plan: CategoryRunPlan): CategoryYearWindow => ({
  category: plan.category,
  years: plan.years,
});

const runCategoryPlan = async (
  deps: RunV7LineageDeps,
  options: V7LineageCliOptions,
  plan: CategoryRunPlan,
): Promise<GlobalRollupRow | null> => {
  const bucketOptions = buildBaseBucketOptions(options, plan.modelVersion);
  const bucketDeps: RunBucketEvalDeps = {
    pool: deps.pool,
    openChunkClient: deps.openChunkClient,
    sleep: deps.sleep,
    log: deps.log,
  };
  deps.log(`Begin category ${plan.category} model_version=${plan.modelVersion}`);
  await runBucketEval(bucketDeps, {
    options: bucketOptions,
    windows: [buildCategoryWindow(plan)],
  });
  const { windowFrom, windowTo } = buildPlanWindowDates(plan.years);
  return rollupCategoryToGlobal(
    { runner: deps.pool, log: deps.log },
    {
      category: plan.category,
      modelVersion: plan.modelVersion,
      runningStyleFeatureVersion: options.runningStyleFeatureVersion,
      finishPositionVersion: options.finishPositionVersion,
      windowFrom,
      windowTo,
    },
  );
};

export interface V7LineageRunResult {
  categories: number;
  rolledUp: number;
}

export const runV7LineageBucketEval = async (
  deps: RunV7LineageDeps,
  options: V7LineageCliOptions,
): Promise<V7LineageRunResult> => {
  await ensureBucketTable(deps.pool);
  await ensureGlobalEvaluationsTable(deps.pool);
  const plans = buildCategoryRunPlans(options);
  const rollups = await plans.reduce<Promise<GlobalRollupRow[]>>(
    (chain, plan) =>
      chain.then(async (acc) => {
        const row = await runCategoryPlan(deps, options, plan);
        return row === null ? acc : [...acc, row];
      }),
    Promise.resolve([]),
  );
  return { categories: plans.length, rolledUp: rollups.length };
};

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((r) => {
    setTimeout(r, ms);
  });

const defaultLog = (message: string): void => {
  console.log(`[bucket-eval-v7lineage] ${message}`);
};

const buildChildFromProc = (proc: ReturnType<typeof Bun.spawn>): BucketEvalRpcChildLike => ({
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
});

const openChunkClientImpl = async (args: BucketChunkLoaderArgs): Promise<BucketChunkClient> => {
  const argv = buildPythonLoaderArgv(args);
  const proc = Bun.spawn(argv, { stdin: "pipe", stdout: "pipe", stderr: "inherit" });
  const client = createBucketEvalRpcClient({ child: buildChildFromProc(proc) });
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
  const pool = new Pool({ connectionString: options.pgUrl });
  try {
    const result = await runV7LineageBucketEval(
      { pool, openChunkClient: openChunkClientImpl, sleep: defaultSleep, log: defaultLog },
      options,
    );
    console.log(JSON.stringify(result));
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

export { applyArg };
