// Run with: bunx vitest run src/scripts/finish-position-features/identify-weak-buckets.test.ts
import { expect, test, vi } from "vitest";

import type {
  BucketQueryRunner,
  IdentifyWeakBucketsCliOptions,
  RunIdentifyDeps,
  WeakBucketRow,
} from "./identify-weak-buckets";
import {
  buildBucketSelectSql,
  buildTopRankingsByMetric,
  buildUsageText,
  buildWeakBucketOutput,
  buildWeakBucketsForCategory,
  computeBucketAccuracy,
  computeBucketWilsonLower,
  computeCategoryMean,
  fetchBucketRows,
  initialOptions,
  normalizeBucketRow,
  parseArgs,
  runIdentifyWeakBuckets,
  wilsonLowerBound,
} from "./identify-weak-buckets";

const baseRow = (overrides: Partial<WeakBucketRow>): WeakBucketRow => ({
  category: "jra",
  source: "jra",
  keibajo_code: "05",
  kyori: 1600,
  kyoso_shubetsu_code: "11",
  kyoso_joken_code: null,
  condition_key: null,
  track_code: "10",
  grade_code: null,
  race_name: null,
  race_count: 100,
  top1_hit_sum: 30,
  place2_hit_sum: 18,
  place3_hit_sum: 14,
  top3_box_hit_sum: 42,
  ...overrides,
});

test("buildUsageText renders the v8 weak-bucket helper signature", () => {
  expect(buildUsageText()).toBe(
    "Usage:\n  bun run src/scripts/finish-position-features/identify-weak-buckets.ts \\\n    --model-version-jra <model_version_jra> \\\n    --model-version-nar <model_version_nar> \\\n    --output <output-json-path> \\\n    [--pg-url <connection-string>] \\\n    [--sample-size-threshold 50] \\\n    [--top-per-metric 5]",
  );
});

test("initialOptions seeds empty model versions and the defaults", () => {
  const options = initialOptions();
  expect(options.modelVersionJra).toBe("");
  expect(options.modelVersionNar).toBe("");
  expect(options.output).toBe("");
  expect(options.sampleSizeThreshold).toBe(50);
  expect(options.topPerMetric).toBe(5);
});

test("parseArgs reads every flag including overrides for sample threshold and top per metric", () => {
  const options = parseArgs([
    "--pg-url",
    "postgres://x",
    "--model-version-jra",
    "jra-v8-iter1",
    "--model-version-nar",
    "nar-v8-iter1",
    "--output",
    "tmp/v8/weak.json",
    "--sample-size-threshold",
    "30",
    "--top-per-metric",
    "3",
  ]);
  expect(options).toStrictEqual({
    pgUrl: "postgres://x",
    modelVersionJra: "jra-v8-iter1",
    modelVersionNar: "nar-v8-iter1",
    output: "tmp/v8/weak.json",
    sampleSizeThreshold: 30,
    topPerMetric: 3,
  } satisfies IdentifyWeakBucketsCliOptions);
});

test("parseArgs throws when --model-version-jra is missing", () => {
  expect(() => parseArgs(["--model-version-nar", "n", "--output", "x.json"])).toThrowError(
    "--model-version-jra is required.",
  );
});

test("parseArgs throws when --model-version-nar is missing", () => {
  expect(() => parseArgs(["--model-version-jra", "j", "--output", "x.json"])).toThrowError(
    "--model-version-nar is required.",
  );
});

test("parseArgs throws when --output is missing", () => {
  expect(() => parseArgs(["--model-version-jra", "j", "--model-version-nar", "n"])).toThrowError(
    "--output is required.",
  );
});

test("parseArgs throws on unknown flag", () => {
  expect(() =>
    parseArgs([
      "--model-version-jra",
      "j",
      "--model-version-nar",
      "n",
      "--output",
      "x.json",
      "--bogus",
    ]),
  ).toThrowError("Unknown argument: --bogus");
});

test("parseArgs throws when a flag value is missing", () => {
  expect(() =>
    parseArgs(["--model-version-jra", "j", "--model-version-nar", "n", "--output"]),
  ).toThrowError("--output requires a value.");
});

test("buildBucketSelectSql aggregates the four hit metrics with the right where clause", () => {
  expect(buildBucketSelectSql()).toBe(
    "\n    select\n      source,\n      keibajo_code,\n      kyori,\n      kyoso_shubetsu_code,\n      kyoso_joken_code,\n      condition_key,\n      track_code,\n      grade_code,\n      race_name,\n      sum(race_count)::numeric as race_count,\n      sum(top1_hit_sum)::numeric as top1_hit_sum,\n      sum(place2_hit_sum)::numeric as place2_hit_sum,\n      sum(place3_hit_sum)::numeric as place3_hit_sum,\n      sum(top3_box_hit_sum)::numeric as top3_box_hit_sum\n    from model_prediction_bucket_evaluations\n    where model_version = $1 and category = $2\n    group by source, keibajo_code, kyori, kyoso_shubetsu_code,\n             kyoso_joken_code, condition_key, track_code, grade_code, race_name\n  ",
  );
});

test("normalizeBucketRow coerces numeric strings into numbers", () => {
  expect(
    normalizeBucketRow("jra", {
      source: "jra",
      keibajo_code: "05",
      kyori: "1600",
      kyoso_shubetsu_code: "11",
      kyoso_joken_code: "010",
      condition_key: null,
      track_code: "10",
      grade_code: null,
      race_name: null,
      race_count: "100",
      top1_hit_sum: "30.5",
      place2_hit_sum: "18",
      place3_hit_sum: "14",
      top3_box_hit_sum: "42",
    }),
  ).toStrictEqual({
    category: "jra",
    source: "jra",
    keibajo_code: "05",
    kyori: 1600,
    kyoso_shubetsu_code: "11",
    kyoso_joken_code: "010",
    condition_key: null,
    track_code: "10",
    grade_code: null,
    race_name: null,
    race_count: 100,
    top1_hit_sum: 30.5,
    place2_hit_sum: 18,
    place3_hit_sum: 14,
    top3_box_hit_sum: 42,
  });
});

test("normalizeBucketRow treats null numeric inputs as zero", () => {
  const result = normalizeBucketRow("nar", {
    source: "nar",
    keibajo_code: "30",
    kyori: 1200,
    kyoso_shubetsu_code: "11",
    kyoso_joken_code: null,
    condition_key: null,
    track_code: "10",
    grade_code: null,
    race_name: null,
    race_count: 0,
    top1_hit_sum: 0,
    place2_hit_sum: 0,
    place3_hit_sum: 0,
    top3_box_hit_sum: 0,
  });
  expect(result.race_count).toBe(0);
  expect(result.top1_hit_sum).toBe(0);
});

test("fetchBucketRows binds modelVersion and category in params order", async () => {
  const queryMock = vi.fn<BucketQueryRunner["query"]>().mockResolvedValue({
    rows: [
      {
        source: "jra",
        keibajo_code: "05",
        kyori: "1600",
        kyoso_shubetsu_code: "11",
        kyoso_joken_code: null,
        condition_key: null,
        track_code: "10",
        grade_code: null,
        race_name: null,
        race_count: "12",
        top1_hit_sum: "3",
        place2_hit_sum: "1",
        place3_hit_sum: "1",
        top3_box_hit_sum: "5",
      },
    ],
  });
  const runner: BucketQueryRunner = { query: queryMock };
  const rows = await fetchBucketRows(runner, "jra", "jra-v8-iter1");
  expect(rows).toStrictEqual([
    {
      category: "jra",
      source: "jra",
      keibajo_code: "05",
      kyori: 1600,
      kyoso_shubetsu_code: "11",
      kyoso_joken_code: null,
      condition_key: null,
      track_code: "10",
      grade_code: null,
      race_name: null,
      race_count: 12,
      top1_hit_sum: 3,
      place2_hit_sum: 1,
      place3_hit_sum: 1,
      top3_box_hit_sum: 5,
    },
  ]);
  expect(queryMock).toHaveBeenCalledWith(buildBucketSelectSql(), ["jra-v8-iter1", "jra"]);
});

test("wilsonLowerBound returns 0 for zero trials", () => {
  expect(wilsonLowerBound(0, 0)).toBe(0);
});

test("wilsonLowerBound is strictly less than the point estimate for finite trials", () => {
  const lower = wilsonLowerBound(30, 100);
  expect(lower).toBeGreaterThan(0);
  expect(lower).toBeLessThan(0.3);
});

test("wilsonLowerBound for all successes returns positive but below one", () => {
  const lower = wilsonLowerBound(50, 50);
  expect(lower).toBeGreaterThan(0.9);
  expect(lower).toBeLessThan(1);
});

test("computeBucketAccuracy divides each hit sum by race count", () => {
  expect(computeBucketAccuracy(baseRow({}))).toStrictEqual({
    top1: 0.3,
    place2: 0.18,
    place3: 0.14,
    top3_box: 0.42,
  });
});

test("computeBucketAccuracy returns all zero when race_count is zero", () => {
  expect(computeBucketAccuracy(baseRow({ race_count: 0 }))).toStrictEqual({
    top1: 0,
    place2: 0,
    place3: 0,
    top3_box: 0,
  });
});

test("computeBucketWilsonLower returns the four LB values", () => {
  const wl = computeBucketWilsonLower(baseRow({}));
  expect(wl.top1).toBeGreaterThan(0);
  expect(wl.place2).toBeGreaterThan(0);
  expect(wl.place3).toBeGreaterThan(0);
  expect(wl.top3_box).toBeGreaterThan(0);
});

test("computeCategoryMean sums hit sums weighted by races", () => {
  const summary = computeCategoryMean(
    [
      baseRow({}),
      baseRow({
        race_count: 100,
        top1_hit_sum: 50,
        place2_hit_sum: 30,
        place3_hit_sum: 20,
        top3_box_hit_sum: 60,
      }),
    ],
    "jra",
  );
  expect(summary.cat).toBe("jra");
  expect(summary.totalRaces).toBe(200);
  expect(summary.mean).toStrictEqual({
    top1: 0.4,
    place2: 0.24,
    place3: 0.17,
    top3_box: 0.51,
  });
});

test("computeCategoryMean returns zero mean when no rows", () => {
  const summary = computeCategoryMean([], "nar");
  expect(summary).toStrictEqual({
    cat: "nar",
    totalRaces: 0,
    mean: { top1: 0, place2: 0, place3: 0, top3_box: 0 },
  });
});

test("buildWeakBucketsForCategory yields entries per dim with sample warning honored", () => {
  const entries = buildWeakBucketsForCategory({
    cat: "jra",
    modelVersion: "jra-v8",
    rows: [
      baseRow({ keibajo_code: "05", race_count: 100, top1_hit_sum: 20 }),
      baseRow({ keibajo_code: "06", race_count: 10, top1_hit_sum: 1 }),
    ],
    sampleSizeThreshold: 50,
  });
  const keibajoEntries = entries.filter((e) => e.dim === "keibajo_code");
  expect(keibajoEntries.length).toBe(2);
  const weak = keibajoEntries.find((e) => e.value === "06");
  expect(weak?.sample_size_warning).toBe(true);
  const strong = keibajoEntries.find((e) => e.value === "05");
  expect(strong?.sample_size_warning).toBe(false);
});

test("buildWeakBucketsForCategory excludes null dim values like missing track_code", () => {
  const entries = buildWeakBucketsForCategory({
    cat: "jra",
    modelVersion: "jra-v8",
    rows: [baseRow({ track_code: null })],
    sampleSizeThreshold: 50,
  });
  expect(entries.some((e) => e.dim === "track_code")).toBe(false);
});

test("buildTopRankingsByMetric returns 4 metric rankings sliced to top N", () => {
  const entries = [
    {
      cat: "jra" as const,
      dim: "keibajo_code" as const,
      value: "05",
      metrics: { top1: 0.3, place2: 0.18, place3: 0.14, top3_box: 0.42 },
      wilson_lower: { top1: 0.2, place2: 0.1, place3: 0.08, top3_box: 0.3 },
      gaps: { top1: 0.1, place2: 0.05, place3: 0.02, top3_box: 0.04 },
      composite_gap: 0.05,
      race_count: 100,
      sample_size_warning: false,
    },
    {
      cat: "jra" as const,
      dim: "kyori" as const,
      value: "1600",
      metrics: { top1: 0.25, place2: 0.16, place3: 0.12, top3_box: 0.38 },
      wilson_lower: { top1: 0.18, place2: 0.09, place3: 0.06, top3_box: 0.27 },
      gaps: { top1: 0.15, place2: 0.07, place3: 0.04, top3_box: 0.08 },
      composite_gap: 0.085,
      race_count: 200,
      sample_size_warning: false,
    },
  ];
  const result = buildTopRankingsByMetric(entries, 1);
  expect(result).toStrictEqual([
    {
      metric: "top1",
      entries: [
        {
          cat: "jra",
          dim: "kyori",
          value: "1600",
          metrics: { top1: 0.25, place2: 0.16, place3: 0.12, top3_box: 0.38 },
          wilson_lower: { top1: 0.18, place2: 0.09, place3: 0.06, top3_box: 0.27 },
          gaps: { top1: 0.15, place2: 0.07, place3: 0.04, top3_box: 0.08 },
          composite_gap: 0.085,
          race_count: 200,
          sample_size_warning: false,
        },
      ],
    },
    {
      metric: "place2",
      entries: [
        {
          cat: "jra",
          dim: "kyori",
          value: "1600",
          metrics: { top1: 0.25, place2: 0.16, place3: 0.12, top3_box: 0.38 },
          wilson_lower: { top1: 0.18, place2: 0.09, place3: 0.06, top3_box: 0.27 },
          gaps: { top1: 0.15, place2: 0.07, place3: 0.04, top3_box: 0.08 },
          composite_gap: 0.085,
          race_count: 200,
          sample_size_warning: false,
        },
      ],
    },
    {
      metric: "place3",
      entries: [
        {
          cat: "jra",
          dim: "kyori",
          value: "1600",
          metrics: { top1: 0.25, place2: 0.16, place3: 0.12, top3_box: 0.38 },
          wilson_lower: { top1: 0.18, place2: 0.09, place3: 0.06, top3_box: 0.27 },
          gaps: { top1: 0.15, place2: 0.07, place3: 0.04, top3_box: 0.08 },
          composite_gap: 0.085,
          race_count: 200,
          sample_size_warning: false,
        },
      ],
    },
    {
      metric: "top3_box",
      entries: [
        {
          cat: "jra",
          dim: "kyori",
          value: "1600",
          metrics: { top1: 0.25, place2: 0.16, place3: 0.12, top3_box: 0.38 },
          wilson_lower: { top1: 0.18, place2: 0.09, place3: 0.06, top3_box: 0.27 },
          gaps: { top1: 0.15, place2: 0.07, place3: 0.04, top3_box: 0.08 },
          composite_gap: 0.085,
          race_count: 200,
          sample_size_warning: false,
        },
      ],
    },
  ]);
});

test("buildWeakBucketOutput sorts combined buckets by composite gap descending", () => {
  const jraRows = [
    baseRow({ keibajo_code: "05", race_count: 100, top1_hit_sum: 30 }),
    baseRow({ keibajo_code: "06", race_count: 60, top1_hit_sum: 10 }),
  ];
  const narRows = [
    baseRow({
      category: "nar",
      source: "nar",
      keibajo_code: "30",
      race_count: 80,
      top1_hit_sum: 16,
    }),
  ];
  const output = buildWeakBucketOutput({
    options: {
      pgUrl: "postgres://x",
      modelVersionJra: "jra-v8",
      modelVersionNar: "nar-v8",
      output: "out.json",
      sampleSizeThreshold: 50,
      topPerMetric: 5,
    },
    jraRows,
    narRows,
  });
  expect(output.schema_version).toBe(1);
  expect(output.model_version_jra).toBe("jra-v8");
  expect(output.model_version_nar).toBe("nar-v8");
  expect(output.buckets.length).toBeGreaterThan(0);
  expect(output.top_by_metric.length).toBe(4);
  const sorted = output.buckets.every(
    (entry, idx) => idx === 0 || entry.composite_gap <= output.buckets[idx - 1].composite_gap,
  );
  expect(sorted).toBe(true);
});

test("runIdentifyWeakBuckets fetches both categories then writes the output JSON", async () => {
  const queryMock = vi.fn<BucketQueryRunner["query"]>().mockResolvedValue({ rows: [] });
  const runner: BucketQueryRunner = { query: queryMock };
  const writeMock = vi.fn<RunIdentifyDeps["writeOutput"]>().mockResolvedValue(undefined);
  const logMock = vi.fn<(message: string) => void>();
  const options: IdentifyWeakBucketsCliOptions = {
    pgUrl: "postgres://x",
    modelVersionJra: "jra-v8-iter1",
    modelVersionNar: "nar-v8-iter1",
    output: "tmp/v8/weak.json",
    sampleSizeThreshold: 50,
    topPerMetric: 5,
  };
  const result = await runIdentifyWeakBuckets(
    { runner, writeOutput: writeMock, log: logMock },
    options,
  );
  expect(result.schema_version).toBe(1);
  expect(result.buckets).toStrictEqual([]);
  expect(queryMock).toHaveBeenCalledTimes(2);
  expect(queryMock).toHaveBeenNthCalledWith(1, buildBucketSelectSql(), ["jra-v8-iter1", "jra"]);
  expect(queryMock).toHaveBeenNthCalledWith(2, buildBucketSelectSql(), ["nar-v8-iter1", "nar"]);
  expect(writeMock).toHaveBeenCalledTimes(1);
});
