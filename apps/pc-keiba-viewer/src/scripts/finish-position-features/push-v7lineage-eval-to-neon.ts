// Run with: bun run src/scripts/finish-position-features/push-v7lineage-eval-to-neon.ts \
//   --source-pg-url <local-replica-url> --target-pg-url <neon-direct-url> \
//   [--batch-size 500]
//
// Stage 4 workaround sync: the heavy bucket-eval aggregate runs against the local
// Colima PostgreSQL replica (no connection time limit), then this script copies the
// small aggregated result rows for the v7-lineage walk-forward 21y model versions
// from that local replica into Neon so the viewer can read them via Hyperdrive.
// It SELECTs the wf-21y rows from model_prediction_bucket_evaluations and
// model_prediction_evaluations on the source, then upserts them into the same Neon
// tables with REPLACE semantics matching each table's unique key. No DELETE is ever
// issued against Neon; the wf-21y namespace is new and will not collide.

import { Pool } from "pg";

import {
  BUCKET_TABLE,
  buildBucketEvaluationsDdl,
  buildBucketUpsertSql,
} from "./evaluate-bucket-predictions-sql";
import {
  EVALUATIONS_TABLE,
  buildEvaluationsDdl,
  buildUpsertSql as buildGlobalUpsertSql,
} from "./evaluate-predictions-sql";

export interface BucketEvalRow {
  model_version: string;
  running_style_feature_version: string;
  finish_position_version: string;
  category: string;
  evaluation_window_from: string;
  evaluation_window_to: string;
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
  prediction_count: number;
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
  pair_score_pair_count: number;
  ndcg_at_3_sum: string;
  ndcg_at_3_race_count: number;
}

export interface GlobalEvalRow {
  model_version: string;
  category: string;
  evaluation_window_from: string;
  evaluation_window_to: string;
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
  top3_place_relation: string | null;
}

export interface QueryResultLike<Row> {
  rows: Row[];
}

export interface QueryRunner {
  query: <Row>(sql: string, params?: unknown[]) => Promise<QueryResultLike<Row>>;
}

export interface SyncCliOptions {
  sourcePgUrl: string;
  targetPgUrl: string;
  batchSize: number;
}

export interface SyncDeps {
  source: QueryRunner;
  target: QueryRunner;
  log: (message: string) => void;
}

export interface SyncResult {
  bucketRowsRead: number;
  bucketRowsUpserted: number;
  globalRowsRead: number;
  globalRowsUpserted: number;
}

const DEFAULT_BATCH_SIZE = 500;
const WF_21Y_SUFFIX = "%-v7-lineage-wf-21y";

export const buildSelectBucketRowsSql = (): string => `
    select
      model_version, running_style_feature_version, finish_position_version,
      category, evaluation_window_from, evaluation_window_to,
      source, keibajo_code, kyori, kyoso_shubetsu_code,
      kyoso_joken_code, condition_key, track_code, grade_code, race_name,
      race_count, prediction_count,
      top1_hit_sum, place1_hit_sum, place2_hit_sum, place3_hit_sum,
      top3_box_hit_sum, top3_exact_hit_sum,
      top3_winner_capture_sum, top5_winner_capture_sum, top3_place_relation_sum,
      pair_score_sum, pair_score_pair_count,
      ndcg_at_3_sum, ndcg_at_3_race_count
    from ${BUCKET_TABLE}
    where model_version like '${WF_21Y_SUFFIX}'
  `;

export const buildSelectGlobalRowsSql = (): string => `
    select
      model_version, category, evaluation_window_from, evaluation_window_to,
      race_count, prediction_count,
      top1_accuracy, top3_box_accuracy, top3_exact_accuracy,
      place1_accuracy, place2_accuracy, place3_accuracy,
      top3_winner_capture, top5_winner_capture,
      pair_score, ndcg_at_3, top3_place_relation
    from ${EVALUATIONS_TABLE}
    where model_version like '${WF_21Y_SUFFIX}'
  `;

export const buildBucketUpsertParams = (row: BucketEvalRow): unknown[] => [
  row.model_version,
  row.running_style_feature_version,
  row.finish_position_version,
  row.category,
  row.evaluation_window_from,
  row.evaluation_window_to,
  row.source,
  row.keibajo_code,
  row.kyori,
  row.kyoso_shubetsu_code,
  row.kyoso_joken_code,
  row.condition_key,
  row.track_code,
  row.grade_code,
  row.race_name,
  row.race_count,
  row.prediction_count,
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
  row.pair_score_pair_count,
  row.ndcg_at_3_sum,
  row.ndcg_at_3_race_count,
];

export const buildGlobalUpsertParams = (row: GlobalEvalRow): unknown[] => [
  row.model_version,
  row.category,
  row.evaluation_window_from,
  row.evaluation_window_to,
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

export const chunkRows = <Row>(rows: Row[], batchSize: number): Row[][] => {
  if (batchSize <= 0) throw new Error("batchSize must be greater than zero.");
  return rows.reduce<Row[][]>((acc, row, index) => {
    if (index % batchSize === 0) return [...acc, [row]];
    const last = acc.at(-1) ?? [];
    return [...acc.slice(0, -1), [...last, row]];
  }, []);
};

const upsertBatch = async (
  runner: QueryRunner,
  sql: string,
  batch: unknown[][],
): Promise<number> => {
  await batch.reduce<Promise<unknown>>(
    (chain, params) => chain.then(() => runner.query(sql, params)),
    Promise.resolve(),
  );
  return batch.length;
};

const upsertAll = async (args: {
  runner: QueryRunner;
  sql: string;
  paramBatches: unknown[][][];
}): Promise<number> =>
  args.paramBatches.reduce<Promise<number>>(
    (chain, batch) =>
      chain.then(async (acc) => acc + (await upsertBatch(args.runner, args.sql, batch))),
    Promise.resolve(0),
  );

export const ensureTargetTables = async (target: QueryRunner): Promise<void> => {
  await target.query(buildBucketEvaluationsDdl());
  await target.query(buildEvaluationsDdl());
};

export const syncBucketRows = async (
  deps: SyncDeps,
  batchSize: number,
): Promise<{
  read: number;
  upserted: number;
}> => {
  const result = await deps.source.query<BucketEvalRow>(buildSelectBucketRowsSql());
  const paramBatches = chunkRows(result.rows.map(buildBucketUpsertParams), batchSize);
  const upserted = await upsertAll({
    runner: deps.target,
    sql: buildBucketUpsertSql(),
    paramBatches,
  });
  deps.log(`Synced ${upserted} bucket rows into ${BUCKET_TABLE}`);
  return { read: result.rows.length, upserted };
};

export const syncGlobalRows = async (
  deps: SyncDeps,
  batchSize: number,
): Promise<{
  read: number;
  upserted: number;
}> => {
  const result = await deps.source.query<GlobalEvalRow>(buildSelectGlobalRowsSql());
  const paramBatches = chunkRows(result.rows.map(buildGlobalUpsertParams), batchSize);
  const upserted = await upsertAll({
    runner: deps.target,
    sql: buildGlobalUpsertSql(),
    paramBatches,
  });
  deps.log(`Synced ${upserted} global rows into ${EVALUATIONS_TABLE}`);
  return { read: result.rows.length, upserted };
};

export const runSync = async (deps: SyncDeps, options: SyncCliOptions): Promise<SyncResult> => {
  await ensureTargetTables(deps.target);
  const bucket = await syncBucketRows(deps, options.batchSize);
  const global = await syncGlobalRows(deps, options.batchSize);
  return {
    bucketRowsRead: bucket.read,
    bucketRowsUpserted: bucket.upserted,
    globalRowsRead: global.read,
    globalRowsUpserted: global.upserted,
  };
};

export const buildUsageText = (): string =>
  [
    "Usage:",
    "  bun run src/scripts/finish-position-features/push-v7lineage-eval-to-neon.ts \\",
    "    --source-pg-url <local-replica-url> \\",
    "    --target-pg-url <neon-direct-url> \\",
    "    [--batch-size 500]",
  ].join("\n");

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value.`);
  return value;
};

export const initialOptions = (): SyncCliOptions => ({
  sourcePgUrl: "",
  targetPgUrl: "",
  batchSize: DEFAULT_BATCH_SIZE,
});

interface ApplyArgResult {
  advanceBy: number;
}

const applyArg = (
  options: SyncCliOptions,
  name: string,
  value: string | undefined,
): ApplyArgResult => {
  if (name === "--source-pg-url") {
    options.sourcePgUrl = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--target-pg-url") {
    options.targetPgUrl = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--batch-size") {
    options.batchSize = Number.parseInt(requireValue(name, value), 10);
    return { advanceBy: 2 };
  }
  if (name === "--help" || name === "-h") {
    console.log(buildUsageText());
    process.exit(0);
  }
  throw new Error(`Unknown argument: ${name}`);
};

const consumeArgs = (
  options: SyncCliOptions,
  argv: readonly string[],
  cursor: number,
): SyncCliOptions => {
  const name = argv[cursor];
  if (name === undefined) return options;
  const { advanceBy } = applyArg(options, name, argv[cursor + 1]);
  return consumeArgs(options, argv, cursor + advanceBy);
};

export const parseArgs = (argv: readonly string[]): SyncCliOptions => {
  const options = consumeArgs(initialOptions(), argv, 0);
  if (options.sourcePgUrl === "") throw new Error("--source-pg-url is required.");
  if (options.targetPgUrl === "") throw new Error("--target-pg-url is required.");
  return options;
};

export const defaultLog = (message: string): void => {
  console.log(`[push-v7lineage-eval-to-neon] ${message}`);
};

const main = async (): Promise<void> => {
  const options = parseArgs(process.argv.slice(2));
  const source = new Pool({ connectionString: options.sourcePgUrl });
  const target = new Pool({ connectionString: options.targetPgUrl });
  try {
    const result = await runSync({ source, target, log: defaultLog }, options);
    console.log(JSON.stringify(result));
  } finally {
    await source.end();
    await target.end();
  }
};

if (import.meta.main) {
  main().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}

export { applyArg };
