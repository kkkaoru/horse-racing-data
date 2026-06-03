// Run with: bunx vitest run src/scripts/finish-position-features/compare-prev-vs-current-buckets.test.ts
import { expect, test, vi } from "vitest";

import type {
  CompareBucketQueryRunner,
  CompareBucketRow,
  CompareCliOptions,
  CompareRunDeps,
} from "./compare-prev-vs-current-buckets";
import {
  buildBucketDeltas,
  buildCompareSelectSql,
  buildCsvBody,
  buildCsvLine,
  buildMarkdownReport,
  buildUsageText,
  fetchCompareRows,
  initialOptions,
  parseArgs,
  resolveMarker,
  runComparePrevVsCurrent,
  wilsonLowerBound,
} from "./compare-prev-vs-current-buckets";

const baseRow = (overrides: Partial<CompareBucketRow>): CompareBucketRow => ({
  category: "jra",
  dim: "keibajo_code",
  value: "05",
  race_count: 100,
  top1: 0.3,
  place2: 0.18,
  place3: 0.14,
  top3_box: 0.42,
  top1_wilson_lower: 0.22,
  place2_wilson_lower: 0.12,
  place3_wilson_lower: 0.09,
  top3_box_wilson_lower: 0.33,
  ...overrides,
});

test("buildUsageText renders the prev/curr compare signature", () => {
  expect(buildUsageText()).toBe(
    "Usage:\n  bun run src/scripts/finish-position-features/compare-prev-vs-current-buckets.ts \\\n    --prev-model-version <prev> \\\n    --curr-model-version <curr> \\\n    --output-csv <csv-path> \\\n    --output-md <md-path> \\\n    [--pg-url <connection-string>]",
  );
});

test("initialOptions seeds the four required strings as empty", () => {
  const options = initialOptions();
  expect(options.prevModelVersion).toBe("");
  expect(options.currModelVersion).toBe("");
  expect(options.outputCsv).toBe("");
  expect(options.outputMd).toBe("");
});

test("parseArgs reads every flag in any order", () => {
  expect(
    parseArgs([
      "--prev-model-version",
      "jra-iter0",
      "--curr-model-version",
      "jra-iter1",
      "--output-csv",
      "tmp/v8/delta.csv",
      "--output-md",
      "tmp/v8/delta.md",
      "--pg-url",
      "postgres://x",
    ]),
  ).toStrictEqual({
    pgUrl: "postgres://x",
    prevModelVersion: "jra-iter0",
    currModelVersion: "jra-iter1",
    outputCsv: "tmp/v8/delta.csv",
    outputMd: "tmp/v8/delta.md",
  } satisfies CompareCliOptions);
});

test("parseArgs throws when --prev-model-version is missing", () => {
  expect(() =>
    parseArgs(["--curr-model-version", "c", "--output-csv", "a.csv", "--output-md", "b.md"]),
  ).toThrowError("--prev-model-version is required.");
});

test("parseArgs throws when --curr-model-version is missing", () => {
  expect(() =>
    parseArgs(["--prev-model-version", "p", "--output-csv", "a.csv", "--output-md", "b.md"]),
  ).toThrowError("--curr-model-version is required.");
});

test("parseArgs throws when --output-csv is missing", () => {
  expect(() =>
    parseArgs(["--prev-model-version", "p", "--curr-model-version", "c", "--output-md", "b.md"]),
  ).toThrowError("--output-csv is required.");
});

test("parseArgs throws when --output-md is missing", () => {
  expect(() =>
    parseArgs(["--prev-model-version", "p", "--curr-model-version", "c", "--output-csv", "a.csv"]),
  ).toThrowError("--output-md is required.");
});

test("parseArgs throws on unknown argument", () => {
  expect(() =>
    parseArgs([
      "--prev-model-version",
      "p",
      "--curr-model-version",
      "c",
      "--output-csv",
      "a.csv",
      "--output-md",
      "b.md",
      "--bogus",
    ]),
  ).toThrowError("Unknown argument: --bogus");
});

test("parseArgs throws when a flag value is missing", () => {
  expect(() =>
    parseArgs([
      "--prev-model-version",
      "p",
      "--curr-model-version",
      "c",
      "--output-csv",
      "a.csv",
      "--output-md",
    ]),
  ).toThrowError("--output-md requires a value.");
});

test("buildCompareSelectSql binds modelVersion and category placeholders", () => {
  expect(buildCompareSelectSql()).toBe(
    "\n    select\n      source,\n      keibajo_code,\n      kyori,\n      kyoso_joken_code,\n      condition_key,\n      track_code,\n      grade_code,\n      sum(race_count)::numeric as race_count,\n      sum(top1_hit_sum)::numeric as top1_hit_sum,\n      sum(place2_hit_sum)::numeric as place2_hit_sum,\n      sum(place3_hit_sum)::numeric as place3_hit_sum,\n      sum(top3_box_hit_sum)::numeric as top3_box_hit_sum\n    from model_prediction_bucket_evaluations\n    where model_version = $1 and category = $2\n    group by source, keibajo_code, kyori, kyoso_joken_code, condition_key, track_code, grade_code\n  ",
  );
});

test("wilsonLowerBound returns zero when trials is zero", () => {
  expect(wilsonLowerBound(0, 0)).toBe(0);
});

test("wilsonLowerBound returns a finite positive value for typical inputs", () => {
  const lower = wilsonLowerBound(30, 100);
  expect(lower).toBeGreaterThan(0);
  expect(lower).toBeLessThan(0.3);
});

test("fetchCompareRows aggregates raw rows by keibajo dim with derived metrics", async () => {
  const queryMock = vi.fn<CompareBucketQueryRunner["query"]>().mockResolvedValue({
    rows: [
      {
        source: "jra",
        keibajo_code: "05",
        kyori: 1600,
        kyoso_joken_code: null,
        condition_key: null,
        track_code: "10",
        grade_code: null,
        race_count: 100,
        top1_hit_sum: 30,
        place2_hit_sum: 18,
        place3_hit_sum: 14,
        top3_box_hit_sum: 42,
      },
    ],
  });
  const runner: CompareBucketQueryRunner = { query: queryMock };
  const rows = await fetchCompareRows(runner, "jra", "jra-iter1");
  const keibajo = rows.find((r) => r.dim === "keibajo_code" && r.value === "05");
  expect(keibajo?.top1).toBe(0.3);
  expect(keibajo?.race_count).toBe(100);
  expect(queryMock).toHaveBeenCalledWith(buildCompareSelectSql(), ["jra-iter1", "jra"]);
});

test("buildBucketDeltas joins prev and curr by bucket key and produces 4 deltas", () => {
  const prevRows = [baseRow({ value: "05", top1: 0.3 })];
  const currRows = [baseRow({ value: "05", top1: 0.32 })];
  const deltas = buildBucketDeltas({ prevRows, currRows });
  expect(deltas.length).toBe(1);
  expect(deltas[0].top1_delta).toBe(0.02);
  expect(deltas[0].top1_prev).toBe(0.3);
  expect(deltas[0].top1_curr).toBe(0.32);
});

test("buildBucketDeltas skips current buckets that have no prev counterpart", () => {
  const prevRows = [baseRow({ value: "05" })];
  const currRows = [baseRow({ value: "99" })];
  expect(buildBucketDeltas({ prevRows, currRows })).toStrictEqual([]);
});

test("buildBucketDeltas sorts deltas descending by composite", () => {
  const prevRows = [
    baseRow({ value: "05", top1: 0.3, place2: 0.1, place3: 0.1, top3_box: 0.3 }),
    baseRow({ value: "06", top1: 0.2, place2: 0.1, place3: 0.1, top3_box: 0.2 }),
  ];
  const currRows = [
    baseRow({ value: "05", top1: 0.31, place2: 0.1, place3: 0.1, top3_box: 0.3 }),
    baseRow({ value: "06", top1: 0.3, place2: 0.2, place3: 0.2, top3_box: 0.3 }),
  ];
  const deltas = buildBucketDeltas({ prevRows, currRows });
  expect(deltas[0].value).toBe("06");
  expect(deltas[1].value).toBe("05");
});

test("buildCsvLine renders the 18 expected columns in order", () => {
  const csv = buildCsvLine({
    category: "jra",
    dim: "keibajo_code",
    value: "05",
    race_count: 100,
    top1_prev: 0.3,
    top1_curr: 0.32,
    top1_delta: 0.02,
    place2_prev: 0.18,
    place2_curr: 0.19,
    place2_delta: 0.01,
    place3_prev: 0.14,
    place3_curr: 0.15,
    place3_delta: 0.01,
    top3_box_prev: 0.42,
    top3_box_curr: 0.43,
    top3_box_delta: 0.01,
    composite_delta_normalized: 0.0125,
    wilson_lower_delta_composite: 0.01,
  });
  expect(csv).toBe(
    "jra,keibajo_code,05,100,0.3,0.32,0.02,0.18,0.19,0.01,0.14,0.15,0.01,0.42,0.43,0.01,0.0125,0.01",
  );
});

test("buildCsvBody emits the header then a row per delta", () => {
  const csv = buildCsvBody([
    {
      category: "jra",
      dim: "keibajo_code",
      value: "05",
      race_count: 100,
      top1_prev: 0.3,
      top1_curr: 0.32,
      top1_delta: 0.02,
      place2_prev: 0.18,
      place2_curr: 0.19,
      place2_delta: 0.01,
      place3_prev: 0.14,
      place3_curr: 0.15,
      place3_delta: 0.01,
      top3_box_prev: 0.42,
      top3_box_curr: 0.43,
      top3_box_delta: 0.01,
      composite_delta_normalized: 0.0125,
      wilson_lower_delta_composite: 0.01,
    },
  ]);
  expect(csv).toBe(
    "category,dim,value,n,top1_prev,top1_curr,top1_delta,place2_prev,place2_curr,place2_delta,place3_prev,place3_curr,place3_delta,top3_box_prev,top3_box_curr,top3_box_delta,composite_delta_normalized,wilson_lower_delta_composite\njra,keibajo_code,05,100,0.3,0.32,0.02,0.18,0.19,0.01,0.14,0.15,0.01,0.42,0.43,0.01,0.0125,0.01",
  );
});

test("resolveMarker returns flat marker for near-zero deltas", () => {
  expect(resolveMarker(0)).toBe("[~]");
});

test("resolveMarker returns improve marker for positive deltas", () => {
  expect(resolveMarker(0.01)).toBe("[+]");
});

test("resolveMarker returns worsen marker for negative deltas", () => {
  expect(resolveMarker(-0.01)).toBe("[-]");
});

test("buildMarkdownReport headers prev curr and groups by cat dim", () => {
  const md = buildMarkdownReport({
    options: {
      pgUrl: "postgres://x",
      prevModelVersion: "jra-iter0",
      currModelVersion: "jra-iter1",
      outputCsv: "a.csv",
      outputMd: "b.md",
    },
    deltas: [
      {
        category: "jra",
        dim: "keibajo_code",
        value: "05",
        race_count: 100,
        top1_prev: 0.3,
        top1_curr: 0.32,
        top1_delta: 0.02,
        place2_prev: 0.18,
        place2_curr: 0.19,
        place2_delta: 0.01,
        place3_prev: 0.14,
        place3_curr: 0.15,
        place3_delta: 0.01,
        top3_box_prev: 0.42,
        top3_box_curr: 0.43,
        top3_box_delta: 0.01,
        composite_delta_normalized: 0.0125,
        wilson_lower_delta_composite: 0.01,
      },
    ],
  });
  expect(md).toBe(
    "# Bucket delta: jra-iter0 -> jra-iter1\n\nTotal buckets matched: 1\n\n### jra / keibajo_code\n\n| value | n | top1 delta | place2 delta | place3 delta | top3_box delta | composite delta | wilson LB delta |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n| 05 | 100 | [+] 0.0200 | [+] 0.0100 | [+] 0.0100 | [+] 0.0100 | [+] 0.0125 | [+] 0.0100 |\n",
  );
});

test("runComparePrevVsCurrent fetches 4 categories writes csv and md", async () => {
  const queryMock = vi.fn<CompareBucketQueryRunner["query"]>().mockResolvedValue({ rows: [] });
  const runner: CompareBucketQueryRunner = { query: queryMock };
  const writeMock = vi.fn<CompareRunDeps["writeFile"]>().mockResolvedValue(undefined);
  const logMock = vi.fn<(message: string) => void>();
  const result = await runComparePrevVsCurrent(
    { runner, writeFile: writeMock, log: logMock },
    {
      pgUrl: "postgres://x",
      prevModelVersion: "jra-iter0",
      currModelVersion: "jra-iter1",
      outputCsv: "tmp/v8/delta.csv",
      outputMd: "tmp/v8/delta.md",
    },
  );
  expect(result.deltas).toStrictEqual([]);
  expect(queryMock).toHaveBeenCalledTimes(4);
  expect(writeMock).toHaveBeenCalledTimes(2);
});
