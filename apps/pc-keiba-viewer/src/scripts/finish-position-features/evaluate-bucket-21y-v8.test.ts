// Run with: bunx vitest run src/scripts/finish-position-features/evaluate-bucket-21y-v8.test.ts
import { expect, test, vi } from "vitest";

import type {
  BucketChunkClient,
  BucketChunkLoaderArgs,
  BucketQueryRunner,
} from "./evaluate-bucket-21y";
import type { RunV8Deps, V8CliOptions } from "./evaluate-bucket-21y-v8";
import {
  buildBaseBucketOptions,
  buildCategoryYearWindow,
  buildUsageText,
  initialOptions,
  isV8Category,
  parseArgs,
  parseV8Category,
  resolveCategoryYears,
  runV8BucketEval,
} from "./evaluate-bucket-21y-v8";

const baseOptions = (overrides: Partial<V8CliOptions>): V8CliOptions => ({
  pgUrl: "postgres://test",
  runningStyleFeatureVersion: "v3",
  finishPositionVersion: "v1",
  modelVersion: "jra-v8-iter1",
  category: "jra",
  predictionsRoot: "/tmp/parquet",
  maxYearsPerRun: 5,
  statementTimeoutMs: 900_000,
  ignoreNightWindow: false,
  ...overrides,
});

test("buildUsageText renders the v8 single-category signature", () => {
  expect(buildUsageText()).toBe(
    "Usage:\n  bun run src/scripts/finish-position-features/evaluate-bucket-21y-v8.ts \\\n    --running-style-feature-version <v3> \\\n    --finish-position-version <v1> \\\n    --model-version <model-version> \\\n    --category <jra|nar|ban-ei> \\\n    --predictions-root <dir> \\\n    [--pg-url <connection-string>] \\\n    [--max-years-per-run 5] \\\n    [--statement-timeout-ms 900000] \\\n    [--ignore-night-window]",
  );
});

test("initialOptions defaults category to jra and timeouts to the constants", () => {
  const options = initialOptions();
  expect(options.category).toBe("jra");
  expect(options.maxYearsPerRun).toBe(5);
  expect(options.statementTimeoutMs).toBe(900_000);
  expect(options.modelVersion).toBe("");
  expect(options.predictionsRoot).toBe("");
});

test("parseArgs reads every flag and parses category enum", () => {
  expect(
    parseArgs([
      "--running-style-feature-version",
      "v3",
      "--finish-position-version",
      "v1",
      "--model-version",
      "jra-v8-iter1",
      "--category",
      "nar",
      "--predictions-root",
      "/tmp/p",
      "--pg-url",
      "postgres://x",
      "--max-years-per-run",
      "3",
      "--statement-timeout-ms",
      "600000",
      "--ignore-night-window",
    ]),
  ).toStrictEqual({
    pgUrl: "postgres://x",
    runningStyleFeatureVersion: "v3",
    finishPositionVersion: "v1",
    modelVersion: "jra-v8-iter1",
    category: "nar",
    predictionsRoot: "/tmp/p",
    maxYearsPerRun: 3,
    statementTimeoutMs: 600_000,
    ignoreNightWindow: true,
  } satisfies V8CliOptions);
});

test("parseArgs throws when --running-style-feature-version is missing", () => {
  expect(() =>
    parseArgs([
      "--finish-position-version",
      "v1",
      "--model-version",
      "m",
      "--predictions-root",
      "/p",
    ]),
  ).toThrowError("--running-style-feature-version is required.");
});

test("parseArgs throws when --finish-position-version is missing", () => {
  expect(() =>
    parseArgs([
      "--running-style-feature-version",
      "v3",
      "--model-version",
      "m",
      "--predictions-root",
      "/p",
    ]),
  ).toThrowError("--finish-position-version is required.");
});

test("parseArgs throws when --model-version is missing", () => {
  expect(() =>
    parseArgs([
      "--running-style-feature-version",
      "v3",
      "--finish-position-version",
      "v1",
      "--predictions-root",
      "/p",
    ]),
  ).toThrowError("--model-version is required.");
});

test("parseArgs throws when --predictions-root is missing", () => {
  expect(() =>
    parseArgs([
      "--running-style-feature-version",
      "v3",
      "--finish-position-version",
      "v1",
      "--model-version",
      "m",
    ]),
  ).toThrowError("--predictions-root is required.");
});

test("parseArgs throws on unknown argument", () => {
  expect(() =>
    parseArgs([
      "--running-style-feature-version",
      "v3",
      "--finish-position-version",
      "v1",
      "--model-version",
      "m",
      "--predictions-root",
      "/p",
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
      "--model-version",
      "m",
      "--predictions-root",
    ]),
  ).toThrowError("--predictions-root requires a value.");
});

test("parseArgs throws on an invalid --category value", () => {
  expect(() =>
    parseArgs([
      "--running-style-feature-version",
      "v3",
      "--finish-position-version",
      "v1",
      "--model-version",
      "m",
      "--predictions-root",
      "/p",
      "--category",
      "invalid-cat",
    ]),
  ).toThrowError("--category must be one of jra | nar | ban-ei. Got: invalid-cat");
});

test("isV8Category accepts jra nar and ban-ei", () => {
  expect(isV8Category("jra")).toBe(true);
  expect(isV8Category("nar")).toBe(true);
  expect(isV8Category("ban-ei")).toBe(true);
});

test("isV8Category rejects other strings", () => {
  expect(isV8Category("banei")).toBe(false);
});

test("parseV8Category returns the value when valid", () => {
  expect(parseV8Category("nar")).toBe("nar");
});

test("parseV8Category throws when value is not allowed", () => {
  expect(() => parseV8Category("bogus")).toThrowError(
    "--category must be one of jra | nar | ban-ei. Got: bogus",
  );
});

test("resolveCategoryYears returns JRA window for jra", () => {
  expect(resolveCategoryYears("jra")).toStrictEqual([
    2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022,
    2023, 2024, 2025, 2026,
  ]);
});

test("resolveCategoryYears returns NAR window for nar", () => {
  expect(resolveCategoryYears("nar")).toStrictEqual([
    2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022,
    2023, 2024, 2025, 2026,
  ]);
});

test("resolveCategoryYears returns Ban-ei window starting 2008 for ban-ei", () => {
  expect(resolveCategoryYears("ban-ei")).toStrictEqual([
    2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023,
    2024, 2025, 2026,
  ]);
});

test("buildCategoryYearWindow wraps the requested category and its 21y window", () => {
  expect(buildCategoryYearWindow(baseOptions({ category: "nar" }))).toStrictEqual({
    category: "nar",
    years: [
      2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021,
      2022, 2023, 2024, 2025, 2026,
    ],
  });
});

test("buildBaseBucketOptions copies the model version and predictions root", () => {
  const options = buildBaseBucketOptions(baseOptions({}));
  expect(options.modelVersion).toBe("jra-v8-iter1");
  expect(options.predictionsRoot).toBe("/tmp/parquet");
  expect(options.runningStyleFeatureVersion).toBe("v3");
  expect(options.finishPositionVersion).toBe("v1");
  expect(options.maxYearsPerRun).toBe(5);
});

test("runV8BucketEval runs the underlying bucket eval for the selected category", async () => {
  const queryMock = vi.fn<BucketQueryRunner["query"]>().mockResolvedValue({ rows: [] });
  const runner: BucketQueryRunner = { query: queryMock };
  const closeMock = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  const chunkRunner: BucketQueryRunner = {
    query: vi.fn<BucketQueryRunner["query"]>().mockResolvedValue({ rows: [] }),
  };
  const captured: BucketChunkLoaderArgs[] = [];
  const openChunkClient = vi.fn<RunV8Deps["openChunkClient"]>((args: BucketChunkLoaderArgs) => {
    captured.push(args);
    const client: BucketChunkClient = { runner: chunkRunner, loadedRows: 0, close: closeMock };
    return Promise.resolve(client);
  });
  const sleepMock = vi.fn<(ms: number) => Promise<void>>().mockResolvedValue(undefined);
  const logMock = vi.fn<(message: string) => void>();
  const result = await runV8BucketEval(
    { pool: runner, openChunkClient, sleep: sleepMock, log: logMock },
    baseOptions({ category: "nar", maxYearsPerRun: 50 }),
  );
  expect(result.category).toBe("nar");
  expect(result.modelVersion).toBe("jra-v8-iter1");
  expect(result.totalRows).toBe(0);
  expect(result.totalRaces).toBe(0);
  expect(logMock).toHaveBeenCalledWith("Begin v8 category=nar model_version=jra-v8-iter1");
});
