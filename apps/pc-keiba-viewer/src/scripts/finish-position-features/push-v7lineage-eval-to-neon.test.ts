// Run with: bun run test (vitest)

import { expect, test, vi } from "vitest";

import type {
  BucketEvalRow,
  GlobalEvalRow,
  QueryResultLike,
  QueryRunner,
} from "./push-v7lineage-eval-to-neon";
import {
  applyArg,
  buildBucketUpsertParams,
  buildGlobalUpsertParams,
  buildSelectBucketRowsSql,
  buildSelectGlobalRowsSql,
  buildUsageText,
  chunkRows,
  ensureTargetTables,
  initialOptions,
  parseArgs,
  runSync,
  syncBucketRows,
  syncGlobalRows,
} from "./push-v7lineage-eval-to-neon";

const bucketRowFixture = (): BucketEvalRow => ({
  model_version: "jra-cb-v7-lineage-wf-21y",
  running_style_feature_version: "v3",
  finish_position_version: "v1",
  category: "jra",
  evaluation_window_from: "20070101",
  evaluation_window_to: "20071231",
  source: "jra",
  keibajo_code: "05",
  kyori: 1600,
  kyoso_shubetsu_code: "11",
  kyoso_joken_code: "005",
  condition_key: null,
  track_code: "10",
  grade_code: null,
  race_name: null,
  race_count: 12,
  prediction_count: 144,
  top1_hit_sum: "6",
  place1_hit_sum: "6",
  place2_hit_sum: "4",
  place3_hit_sum: "3",
  top3_box_hit_sum: "2",
  top3_exact_hit_sum: "1",
  top3_winner_capture_sum: "9",
  top5_winner_capture_sum: "11",
  top3_place_relation_sum: "7.5",
  pair_score_sum: "500",
  pair_score_pair_count: 700,
  ndcg_at_3_sum: "8.5",
  ndcg_at_3_race_count: 12,
});

const globalRowFixture = (): GlobalEvalRow => ({
  model_version: "nar-xgb-v7-lineage-wf-21y",
  category: "nar",
  evaluation_window_from: "20070101",
  evaluation_window_to: "20261231",
  race_count: 1000,
  prediction_count: 12000,
  top1_accuracy: "0.55",
  top3_box_accuracy: "0.2",
  top3_exact_accuracy: "0.05",
  place1_accuracy: "0.55",
  place2_accuracy: "0.3",
  place3_accuracy: "0.25",
  top3_winner_capture: "0.8",
  top5_winner_capture: "0.9",
  pair_score: "0.7",
  ndcg_at_3: "0.65",
  top3_place_relation: "0.6",
});

test("buildSelectBucketRowsSql selects all bucket columns for the wf-21y suffix", () => {
  expect(buildSelectBucketRowsSql()).toBe(`
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
    from model_prediction_bucket_evaluations
    where model_version like '%-v7-lineage-wf-21y'
  `);
});

test("buildSelectGlobalRowsSql selects all global columns for the wf-21y suffix", () => {
  expect(buildSelectGlobalRowsSql()).toBe(`
    select
      model_version, category, evaluation_window_from, evaluation_window_to,
      race_count, prediction_count,
      top1_accuracy, top3_box_accuracy, top3_exact_accuracy,
      place1_accuracy, place2_accuracy, place3_accuracy,
      top3_winner_capture, top5_winner_capture,
      pair_score, ndcg_at_3, top3_place_relation
    from model_prediction_evaluations
    where model_version like '%-v7-lineage-wf-21y'
  `);
});

test("buildBucketUpsertParams maps all 30 bucket columns in order", () => {
  const params = buildBucketUpsertParams(bucketRowFixture());
  expect(params).toStrictEqual([
    "jra-cb-v7-lineage-wf-21y",
    "v3",
    "v1",
    "jra",
    "20070101",
    "20071231",
    "jra",
    "05",
    1600,
    "11",
    "005",
    null,
    "10",
    null,
    null,
    12,
    144,
    "6",
    "6",
    "4",
    "3",
    "2",
    "1",
    "9",
    "11",
    "7.5",
    "500",
    700,
    "8.5",
    12,
  ]);
});

test("buildGlobalUpsertParams maps all 17 global columns in order", () => {
  const params = buildGlobalUpsertParams(globalRowFixture());
  expect(params).toStrictEqual([
    "nar-xgb-v7-lineage-wf-21y",
    "nar",
    "20070101",
    "20261231",
    1000,
    12000,
    "0.55",
    "0.2",
    "0.05",
    "0.55",
    "0.3",
    "0.25",
    "0.8",
    "0.9",
    "0.7",
    "0.65",
    "0.6",
  ]);
});

test("chunkRows splits into batches not exceeding batch size", () => {
  expect(chunkRows([1, 2, 3, 4, 5], 2)).toStrictEqual([[1, 2], [3, 4], [5]]);
});

test("chunkRows returns empty array for empty input", () => {
  expect(chunkRows([], 500)).toStrictEqual([]);
});

test("chunkRows returns a single batch when batch size exceeds length", () => {
  expect(chunkRows([1, 2, 3], 10)).toStrictEqual([[1, 2, 3]]);
});

test("chunkRows throws when batch size is zero", () => {
  expect(() => chunkRows([1], 0)).toThrowError("batchSize must be greater than zero.");
});

test("ensureTargetTables issues the bucket and evaluations DDL", async () => {
  const query = vi.fn<QueryRunner["query"]>().mockResolvedValue({ rows: [] });
  await ensureTargetTables({ query });
  expect(query).toHaveBeenCalledTimes(2);
});

test("syncBucketRows reads source rows and upserts each into the target", async () => {
  const sourceQuery = vi
    .fn<QueryRunner["query"]>()
    .mockResolvedValue({ rows: [bucketRowFixture(), bucketRowFixture()] });
  const targetQuery = vi.fn<QueryRunner["query"]>().mockResolvedValue({ rows: [] });
  const log = vi.fn<(message: string) => void>();
  const result = await syncBucketRows(
    { source: { query: sourceQuery }, target: { query: targetQuery }, log },
    500,
  );
  expect(result).toStrictEqual({ read: 2, upserted: 2 });
});

test("syncBucketRows batches upserts according to the batch size", async () => {
  const sourceQuery = vi
    .fn<QueryRunner["query"]>()
    .mockResolvedValue({ rows: [bucketRowFixture(), bucketRowFixture(), bucketRowFixture()] });
  const targetQuery = vi.fn<QueryRunner["query"]>().mockResolvedValue({ rows: [] });
  const log = vi.fn<(message: string) => void>();
  await syncBucketRows({ source: { query: sourceQuery }, target: { query: targetQuery }, log }, 1);
  expect(targetQuery).toHaveBeenCalledTimes(3);
});

test("syncGlobalRows reads source rows and upserts each into the target", async () => {
  const sourceQuery = vi
    .fn<QueryRunner["query"]>()
    .mockResolvedValue({ rows: [globalRowFixture()] });
  const targetQuery = vi.fn<QueryRunner["query"]>().mockResolvedValue({ rows: [] });
  const log = vi.fn<(message: string) => void>();
  const result = await syncGlobalRows(
    { source: { query: sourceQuery }, target: { query: targetQuery }, log },
    500,
  );
  expect(result).toStrictEqual({ read: 1, upserted: 1 });
});

test("runSync ensures tables then syncs both bucket and global rows", async () => {
  const bucketSelect = buildSelectBucketRowsSql();
  const globalSelect = buildSelectGlobalRowsSql();
  const sourceQuery = vi.fn<QueryRunner["query"]>(
    (sql: string): Promise<QueryResultLike<BucketEvalRow | GlobalEvalRow>> => {
      if (sql === bucketSelect) return Promise.resolve({ rows: [bucketRowFixture()] });
      if (sql === globalSelect) return Promise.resolve({ rows: [globalRowFixture()] });
      return Promise.resolve({ rows: [] });
    },
  );
  const targetQuery = vi.fn<QueryRunner["query"]>().mockResolvedValue({ rows: [] });
  const log = vi.fn<(message: string) => void>();
  const result = await runSync(
    { source: { query: sourceQuery }, target: { query: targetQuery }, log },
    { sourcePgUrl: "s", targetPgUrl: "t", batchSize: 500 },
  );
  expect(result).toStrictEqual({
    bucketRowsRead: 1,
    bucketRowsUpserted: 1,
    globalRowsRead: 1,
    globalRowsUpserted: 1,
  });
});

test("parseArgs parses source url, target url, and batch size", () => {
  const options = parseArgs([
    "--source-pg-url",
    "postgres://local",
    "--target-pg-url",
    "postgres://neon",
    "--batch-size",
    "250",
  ]);
  expect(options).toStrictEqual({
    sourcePgUrl: "postgres://local",
    targetPgUrl: "postgres://neon",
    batchSize: 250,
  });
});

test("parseArgs throws when source url is missing", () => {
  expect(() => parseArgs(["--target-pg-url", "postgres://neon"])).toThrowError(
    "--source-pg-url is required.",
  );
});

test("parseArgs throws when target url is missing", () => {
  expect(() => parseArgs(["--source-pg-url", "postgres://local"])).toThrowError(
    "--target-pg-url is required.",
  );
});

test("initialOptions returns empty urls and the default batch size", () => {
  expect(initialOptions()).toStrictEqual({ sourcePgUrl: "", targetPgUrl: "", batchSize: 500 });
});

test("applyArg throws on an unknown argument", () => {
  expect(() => applyArg(initialOptions(), "--nope", "x")).toThrowError("Unknown argument: --nope");
});

test("applyArg throws when source url value is missing", () => {
  expect(() => applyArg(initialOptions(), "--source-pg-url", undefined)).toThrowError(
    "--source-pg-url requires a value.",
  );
});

test("applyArg throws when target url value is missing", () => {
  expect(() => applyArg(initialOptions(), "--target-pg-url", undefined)).toThrowError(
    "--target-pg-url requires a value.",
  );
});

test("applyArg throws when batch size value is missing", () => {
  expect(() => applyArg(initialOptions(), "--batch-size", undefined)).toThrowError(
    "--batch-size requires a value.",
  );
});

test("applyArg advances by two and sets the batch size for a value flag", () => {
  const options = initialOptions();
  const advance = applyArg(options, "--batch-size", "250");
  expect(advance).toStrictEqual({ advanceBy: 2 });
});

test("applyArg sets the parsed batch size on the options object", () => {
  const options = initialOptions();
  applyArg(options, "--batch-size", "250");
  expect(options.batchSize).toBe(250);
});

test("buildUsageText documents the source-pg-url and target-pg-url flags", () => {
  expect(buildUsageText()).toBe(
    [
      "Usage:",
      "  bun run src/scripts/finish-position-features/push-v7lineage-eval-to-neon.ts \\",
      "    --source-pg-url <local-replica-url> \\",
      "    --target-pg-url <neon-direct-url> \\",
      "    [--batch-size 500]",
    ].join("\n"),
  );
});
