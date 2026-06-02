// Run with: bunx vitest run src/scripts/finish-position-features/evaluate-bucket-21y-v7lineage.test.ts
import { expect, test, vi } from "vitest";

import type {
  BucketChunkClient,
  BucketChunkLoaderArgs,
  BucketQueryResult,
  BucketQueryRunner,
} from "./evaluate-bucket-21y";
import type {
  GlobalRollupArgs,
  GlobalRollupRow,
  RunV7LineageDeps,
  V7LineageCliOptions,
} from "./evaluate-bucket-21y-v7lineage";
import {
  buildBaseBucketOptions,
  buildCategoryRunPlans,
  buildGlobalRollupSql,
  buildGlobalUpsertParams,
  buildGlobalUpsertSql,
  buildPlanWindowDates,
  buildUsageText,
  initialOptions,
  parseArgs,
  resolveModelVersion,
  rollupCategoryToGlobal,
  runV7LineageBucketEval,
} from "./evaluate-bucket-21y-v7lineage";

const baseOptions = (overrides: Partial<V7LineageCliOptions>): V7LineageCliOptions => ({
  pgUrl: "postgres://test",
  runningStyleFeatureVersion: "v3",
  finishPositionVersion: "v1",
  modelVersionJra: "jra-cb-v7-lineage-wf-21y",
  modelVersionNar: "nar-xgb-v7-lineage-wf-21y",
  modelVersionBanei: "banei-cb-v7-lineage-wf-21y",
  maxYearsPerRun: 5,
  statementTimeoutMs: 900_000,
  ignoreNightWindow: false,
  predictionsRoot: "/tmp/parquet",
  ...overrides,
});

const baseRollupRow = (overrides: Partial<GlobalRollupRow>): GlobalRollupRow => ({
  race_count: "100",
  prediction_count: "1200",
  top1_accuracy: "0.31",
  place1_accuracy: "0.31",
  place2_accuracy: "0.18",
  place3_accuracy: "0.14",
  top3_box_accuracy: "0.42",
  top3_exact_accuracy: "0.05",
  top3_winner_capture: "0.55",
  top5_winner_capture: "0.70",
  top3_place_relation: "0.60",
  pair_score: "0.66",
  ndcg_at_3: "0.72",
  ...overrides,
});

const baseRollupArgs = (overrides: Partial<GlobalRollupArgs>): GlobalRollupArgs => ({
  category: "jra",
  modelVersion: "jra-cb-v7-lineage-wf-21y",
  runningStyleFeatureVersion: "v3",
  finishPositionVersion: "v1",
  windowFrom: "20070101",
  windowTo: "20261231",
  ...overrides,
});

test("buildUsageText renders the per-category model-version flags", () => {
  expect(buildUsageText()).toBe(
    `Usage:\n  bun run src/scripts/finish-position-features/evaluate-bucket-21y-v7lineage.ts \\\n    --pg-url <connection-string> \\\n    --running-style-feature-version v3 \\\n    --finish-position-version v1 \\\n    [--model-version-jra jra-cb-v7-lineage-wf-21y] \\\n    [--model-version-nar nar-xgb-v7-lineage-wf-21y] \\\n    [--model-version-banei banei-cb-v7-lineage-wf-21y] \\\n    [--predictions-root <dir>] \\\n    [--max-years-per-run 5] \\\n    [--statement-timeout-ms 900000] \\\n    [--ignore-night-window]`,
  );
});

test("initialOptions defaults the three model versions to the WF namespaces", () => {
  const options = initialOptions();
  expect(options.modelVersionJra).toBe("jra-cb-v7-lineage-wf-21y");
  expect(options.modelVersionNar).toBe("nar-xgb-v7-lineage-wf-21y");
  expect(options.modelVersionBanei).toBe("banei-cb-v7-lineage-wf-21y");
});

test("initialOptions points the predictions root at the v7-lineage WF dir", () => {
  expect(initialOptions().predictionsRoot).toBe(
    "apps/pc-keiba-viewer/tmp/bucket-eval/finish-position/v7-lineage-wf-21y/predictions",
  );
});

test("parseArgs overrides every per-category model version", () => {
  const options = parseArgs([
    "--pg-url",
    "postgres://x",
    "--running-style-feature-version",
    "v3",
    "--finish-position-version",
    "v1",
    "--model-version-jra",
    "jra-custom",
    "--model-version-nar",
    "nar-custom",
    "--model-version-banei",
    "banei-custom",
    "--predictions-root",
    "/tmp/p",
    "--max-years-per-run",
    "3",
    "--statement-timeout-ms",
    "600000",
    "--ignore-night-window",
  ]);
  expect(options).toStrictEqual({
    pgUrl: "postgres://x",
    runningStyleFeatureVersion: "v3",
    finishPositionVersion: "v1",
    modelVersionJra: "jra-custom",
    modelVersionNar: "nar-custom",
    modelVersionBanei: "banei-custom",
    maxYearsPerRun: 3,
    statementTimeoutMs: 600_000,
    ignoreNightWindow: true,
    predictionsRoot: "/tmp/p",
  });
});

test("parseArgs throws when --running-style-feature-version is missing", () => {
  expect(() => parseArgs(["--finish-position-version", "v1"])).toThrowError(
    "--running-style-feature-version is required.",
  );
});

test("parseArgs throws when --finish-position-version is missing", () => {
  expect(() => parseArgs(["--running-style-feature-version", "v3"])).toThrowError(
    "--finish-position-version is required.",
  );
});

test("parseArgs throws on unknown argument", () => {
  expect(() =>
    parseArgs([
      "--running-style-feature-version",
      "v3",
      "--finish-position-version",
      "v1",
      "--bogus",
    ]),
  ).toThrowError("Unknown argument: --bogus");
});

test("parseArgs throws when a flag value is missing", () => {
  expect(() =>
    parseArgs([
      "--running-style-feature-version",
      "v3",
      "--finish-position-version",
      "v1",
      "--model-version-jra",
    ]),
  ).toThrowError("--model-version-jra requires a value.");
});

test("resolveModelVersion returns the jra version for jra", () => {
  expect(resolveModelVersion(baseOptions({}), "jra")).toBe("jra-cb-v7-lineage-wf-21y");
});

test("resolveModelVersion returns the nar version for nar", () => {
  expect(resolveModelVersion(baseOptions({}), "nar")).toBe("nar-xgb-v7-lineage-wf-21y");
});

test("resolveModelVersion returns the banei version for ban-ei", () => {
  expect(resolveModelVersion(baseOptions({}), "ban-ei")).toBe("banei-cb-v7-lineage-wf-21y");
});

test("buildCategoryRunPlans yields jra 2007-2026 with its model version", () => {
  const plans = buildCategoryRunPlans(baseOptions({}));
  expect(plans[0]).toStrictEqual({
    category: "jra",
    modelVersion: "jra-cb-v7-lineage-wf-21y",
    years: [
      2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021,
      2022, 2023, 2024, 2025, 2026,
    ],
  });
});

test("buildCategoryRunPlans yields nar 2007-2026 with its model version", () => {
  const plans = buildCategoryRunPlans(baseOptions({}));
  expect(plans[1]).toStrictEqual({
    category: "nar",
    modelVersion: "nar-xgb-v7-lineage-wf-21y",
    years: [
      2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021,
      2022, 2023, 2024, 2025, 2026,
    ],
  });
});

test("buildCategoryRunPlans yields ban-ei 2008-2026 with its model version", () => {
  const plans = buildCategoryRunPlans(baseOptions({}));
  expect(plans[2]).toStrictEqual({
    category: "ban-ei",
    modelVersion: "banei-cb-v7-lineage-wf-21y",
    years: [
      2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022,
      2023, 2024, 2025, 2026,
    ],
  });
});

test("buildPlanWindowDates wraps the first and last year into YYYYMMDD bounds", () => {
  expect(buildPlanWindowDates([2008, 2009, 2026])).toStrictEqual({
    windowFrom: "20080101",
    windowTo: "20261231",
  });
});

test("buildBaseBucketOptions copies the single model version into the shared options", () => {
  const options = buildBaseBucketOptions(baseOptions({}), "nar-xgb-v7-lineage-wf-21y");
  expect(options.modelVersion).toBe("nar-xgb-v7-lineage-wf-21y");
  expect(options.predictionsRoot).toBe("/tmp/parquet");
  expect(options.runningStyleFeatureVersion).toBe("v3");
  expect(options.finishPositionVersion).toBe("v1");
  expect(options.maxYearsPerRun).toBe(5);
});

test("buildGlobalRollupSql averages the per-bucket sums over race and pair counts", () => {
  expect(buildGlobalRollupSql(baseRollupArgs({}))).toBe(
    "\n    with buckets as (\n      select *\n      from model_prediction_bucket_evaluations\n      where model_version = 'jra-cb-v7-lineage-wf-21y'\n        and running_style_feature_version = 'v3'\n        and finish_position_version = 'v1'\n        and category = 'jra'\n    )\n    select\n      coalesce(sum(race_count), 0) race_count,\n      coalesce(sum(prediction_count), 0) prediction_count,\n      case when sum(race_count) > 0 then sum(top1_hit_sum) / sum(race_count) end top1_accuracy,\n      case when sum(race_count) > 0 then sum(place1_hit_sum) / sum(race_count) end place1_accuracy,\n      case when sum(race_count) > 0 then sum(place2_hit_sum) / sum(race_count) end place2_accuracy,\n      case when sum(race_count) > 0 then sum(place3_hit_sum) / sum(race_count) end place3_accuracy,\n      case when sum(race_count) > 0 then sum(top3_box_hit_sum) / sum(race_count) end top3_box_accuracy,\n      case when sum(race_count) > 0 then sum(top3_exact_hit_sum) / sum(race_count) end top3_exact_accuracy,\n      case when sum(race_count) > 0 then sum(top3_winner_capture_sum) / sum(race_count) end top3_winner_capture,\n      case when sum(race_count) > 0 then sum(top5_winner_capture_sum) / sum(race_count) end top5_winner_capture,\n      case when sum(race_count) > 0 then sum(top3_place_relation_sum) / sum(race_count) end top3_place_relation,\n      case when sum(pair_score_pair_count) > 0 then sum(pair_score_sum) / sum(pair_score_pair_count) end pair_score,\n      case when sum(ndcg_at_3_race_count) > 0 then sum(ndcg_at_3_sum) / sum(ndcg_at_3_race_count) end ndcg_at_3\n    from buckets\n  ",
  );
});

test("buildGlobalRollupSql escapes single quotes in the model version", () => {
  expect(buildGlobalRollupSql(baseRollupArgs({ modelVersion: "it's-a-model" }))).toBe(
    "\n    with buckets as (\n      select *\n      from model_prediction_bucket_evaluations\n      where model_version = 'it''s-a-model'\n        and running_style_feature_version = 'v3'\n        and finish_position_version = 'v1'\n        and category = 'jra'\n    )\n    select\n      coalesce(sum(race_count), 0) race_count,\n      coalesce(sum(prediction_count), 0) prediction_count,\n      case when sum(race_count) > 0 then sum(top1_hit_sum) / sum(race_count) end top1_accuracy,\n      case when sum(race_count) > 0 then sum(place1_hit_sum) / sum(race_count) end place1_accuracy,\n      case when sum(race_count) > 0 then sum(place2_hit_sum) / sum(race_count) end place2_accuracy,\n      case when sum(race_count) > 0 then sum(place3_hit_sum) / sum(race_count) end place3_accuracy,\n      case when sum(race_count) > 0 then sum(top3_box_hit_sum) / sum(race_count) end top3_box_accuracy,\n      case when sum(race_count) > 0 then sum(top3_exact_hit_sum) / sum(race_count) end top3_exact_accuracy,\n      case when sum(race_count) > 0 then sum(top3_winner_capture_sum) / sum(race_count) end top3_winner_capture,\n      case when sum(race_count) > 0 then sum(top5_winner_capture_sum) / sum(race_count) end top5_winner_capture,\n      case when sum(race_count) > 0 then sum(top3_place_relation_sum) / sum(race_count) end top3_place_relation,\n      case when sum(pair_score_pair_count) > 0 then sum(pair_score_sum) / sum(pair_score_pair_count) end pair_score,\n      case when sum(ndcg_at_3_race_count) > 0 then sum(ndcg_at_3_sum) / sum(ndcg_at_3_race_count) end ndcg_at_3\n    from buckets\n  ",
  );
});

test("buildGlobalUpsertSql targets model_prediction_evaluations on conflict", () => {
  expect(buildGlobalUpsertSql()).toBe(
    "\n    insert into model_prediction_evaluations (\n      model_version, category, evaluation_window_from, evaluation_window_to,\n      race_count, prediction_count,\n      top1_accuracy, top3_box_accuracy, top3_exact_accuracy,\n      place1_accuracy, place2_accuracy, place3_accuracy,\n      top3_winner_capture, top5_winner_capture,\n      pair_score, ndcg_at_3, top3_place_relation, evaluated_at\n    )\n    values (\n      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, now()\n    )\n    on conflict (model_version, category, evaluation_window_from, evaluation_window_to)\n    do update set\n      race_count = excluded.race_count,\n      prediction_count = excluded.prediction_count,\n      top1_accuracy = excluded.top1_accuracy,\n      top3_box_accuracy = excluded.top3_box_accuracy,\n      top3_exact_accuracy = excluded.top3_exact_accuracy,\n      place1_accuracy = excluded.place1_accuracy,\n      place2_accuracy = excluded.place2_accuracy,\n      place3_accuracy = excluded.place3_accuracy,\n      top3_winner_capture = excluded.top3_winner_capture,\n      top5_winner_capture = excluded.top5_winner_capture,\n      pair_score = excluded.pair_score,\n      ndcg_at_3 = excluded.ndcg_at_3,\n      top3_place_relation = excluded.top3_place_relation,\n      evaluated_at = now()\n  ",
  );
});

test("buildGlobalUpsertParams returns 17 ordered parameters", () => {
  expect(buildGlobalUpsertParams(baseRollupArgs({}), baseRollupRow({}))).toStrictEqual([
    "jra-cb-v7-lineage-wf-21y",
    "jra",
    "20070101",
    "20261231",
    "100",
    "1200",
    "0.31",
    "0.42",
    "0.05",
    "0.31",
    "0.18",
    "0.14",
    "0.55",
    "0.70",
    "0.66",
    "0.72",
    "0.60",
  ]);
});

test("rollupCategoryToGlobal upserts the aggregated row and returns it", async () => {
  const rollupRow = baseRollupRow({ race_count: "55" });
  const queryMock = vi
    .fn<BucketQueryRunner["query"]>()
    .mockImplementationOnce(() => Promise.resolve({ rows: [rollupRow] }))
    .mockResolvedValue({ rows: [] });
  const runner: BucketQueryRunner = { query: queryMock };
  const logMock = vi.fn<(message: string) => void>();
  const result = await rollupCategoryToGlobal({ runner, log: logMock }, baseRollupArgs({}));
  expect(result).toStrictEqual(rollupRow);
  expect(logMock).toHaveBeenCalledWith(
    "Rolled up jra into model_prediction_evaluations (races=55)",
  );
  expect(queryMock).toHaveBeenCalledTimes(2);
});

test("rollupCategoryToGlobal returns null and logs when no bucket rows exist", async () => {
  const queryMock = vi.fn<BucketQueryRunner["query"]>().mockResolvedValue({ rows: [] });
  const runner: BucketQueryRunner = { query: queryMock };
  const logMock = vi.fn<(message: string) => void>();
  const result = await rollupCategoryToGlobal({ runner, log: logMock }, baseRollupArgs({}));
  expect(result).toBe(null);
  expect(logMock).toHaveBeenCalledWith("No bucket rows to roll up for jra");
  expect(queryMock).toHaveBeenCalledTimes(1);
});

function isRowArray<Row>(rows: unknown[]): rows is Row[] {
  void rows;
  return true;
}

function buildAggregateRunner(rollupRow: GlobalRollupRow | null): BucketQueryRunner {
  function buildResult<Row>(rows: unknown[]): BucketQueryResult<Row> {
    if (isRowArray<Row>(rows)) return { rows };
    return { rows: [] };
  }
  function queryFn<Row>(sql: string, _params?: unknown[]): Promise<BucketQueryResult<Row>> {
    if (sql.includes("from model_prediction_bucket_evaluations\n") && rollupRow !== null) {
      const widened: unknown[] = [rollupRow];
      return Promise.resolve(buildResult<Row>(widened));
    }
    return Promise.resolve(buildResult<Row>([]));
  }
  return { query: queryFn };
}

const buildChunkOpener = (
  capture: BucketChunkLoaderArgs[],
  closeMock: () => Promise<void>,
): RunV7LineageDeps["openChunkClient"] =>
  vi.fn<RunV7LineageDeps["openChunkClient"]>((loader: BucketChunkLoaderArgs) => {
    capture.push(loader);
    const chunkRunner: BucketQueryRunner = {
      query: vi.fn<BucketQueryRunner["query"]>().mockResolvedValue({ rows: [] }),
    };
    const client: BucketChunkClient = { runner: chunkRunner, loadedRows: 0, close: closeMock };
    return Promise.resolve(client);
  });

test("runV7LineageBucketEval rolls up all three categories that returned rows", async () => {
  const runner = buildAggregateRunner(baseRollupRow({ race_count: "10" }));
  const closeMock = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  const capture: BucketChunkLoaderArgs[] = [];
  const openChunkClient = buildChunkOpener(capture, closeMock);
  const sleepMock = vi.fn<(ms: number) => Promise<void>>().mockResolvedValue(undefined);
  const logMock = vi.fn<(message: string) => void>();
  const deps: RunV7LineageDeps = { pool: runner, openChunkClient, sleep: sleepMock, log: logMock };
  const result = await runV7LineageBucketEval(deps, baseOptions({ maxYearsPerRun: 50 }));
  expect(result).toStrictEqual({ categories: 3, rolledUp: 3 });
  expect(logMock).toHaveBeenCalledWith("Begin category jra model_version=jra-cb-v7-lineage-wf-21y");
  expect(logMock).toHaveBeenCalledWith(
    "Begin category nar model_version=nar-xgb-v7-lineage-wf-21y",
  );
  expect(logMock).toHaveBeenCalledWith(
    "Begin category ban-ei model_version=banei-cb-v7-lineage-wf-21y",
  );
});

test("runV7LineageBucketEval reports zero rolled up when buckets are empty", async () => {
  const runner = buildAggregateRunner(null);
  const closeMock = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  const capture: BucketChunkLoaderArgs[] = [];
  const openChunkClient = buildChunkOpener(capture, closeMock);
  const sleepMock = vi.fn<(ms: number) => Promise<void>>().mockResolvedValue(undefined);
  const logMock = vi.fn<(message: string) => void>();
  const deps: RunV7LineageDeps = { pool: runner, openChunkClient, sleep: sleepMock, log: logMock };
  const result = await runV7LineageBucketEval(deps, baseOptions({ maxYearsPerRun: 50 }));
  expect(result).toStrictEqual({ categories: 3, rolledUp: 0 });
  expect(logMock).toHaveBeenCalledWith("No bucket rows to roll up for jra");
});
