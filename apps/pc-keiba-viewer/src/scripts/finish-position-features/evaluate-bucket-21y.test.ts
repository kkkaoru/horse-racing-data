// Run with: bunx vitest run src/scripts/finish-position-features/evaluate-bucket-21y.test.ts
import { expect, test, vi } from "vitest";

import type {
  AggregateRow,
  BucketChunkClient,
  BucketChunkLoaderArgs,
  BucketEvalCliOptions,
  BucketQueryResult,
  BucketQueryRunner,
  CategoryYearWindow,
  RunBucketEvalDeps,
} from "./evaluate-bucket-21y";
import {
  CATEGORY_YEAR_WINDOWS,
  buildPredictionsParquetGlob,
  buildPythonLoaderArgv,
  buildSessionIdleTimeoutSql,
  buildSessionStatementSql,
  buildSessionTuningSqls,
  buildSessionWorkMemSql,
  buildUpsertParams,
  buildUsageText,
  buildYearDateWindow,
  chunkYears,
  ensureColimaCapacity,
  isWithinNightWindow,
  numericFromRow,
  parseArgs,
  parseColimaStatusJson,
  processCategoryChunk,
  processYear,
  runBucketEval,
} from "./evaluate-bucket-21y";

const baseAggregateRow = (overrides: Partial<AggregateRow>): AggregateRow => ({
  source: "jra",
  keibajo_code: "05",
  kyori: 2400,
  kyoso_shubetsu_code: "12",
  kyoso_joken_code: "703",
  condition_key: null,
  track_code: "10",
  grade_code: null,
  race_name: null,
  race_count: 1,
  prediction_count: 16,
  top1_hit_sum: "0",
  place1_hit_sum: "0",
  place2_hit_sum: "0",
  place3_hit_sum: "0",
  top3_box_hit_sum: "0",
  top3_exact_hit_sum: "0",
  top3_winner_capture_sum: "0",
  top5_winner_capture_sum: "0",
  top3_place_relation_sum: "0",
  pair_score_sum: "0",
  pair_score_pair_count: 0,
  ndcg_at_3_sum: "0",
  ndcg_at_3_race_count: 0,
  ...overrides,
});

const baseOptions = (overrides: Partial<BucketEvalCliOptions>): BucketEvalCliOptions => ({
  pgUrl: "postgres://test",
  runningStyleFeatureVersion: "v1",
  finishPositionVersion: "v1",
  modelVersion: "active",
  maxYearsPerRun: 5,
  statementTimeoutMs: 900_000,
  ignoreNightWindow: false,
  perYearSleepMs: 0,
  perCategorySleepMs: 0,
  minColimaCpu: 8,
  minColimaMemoryGb: 24,
  predictionsRoot: "/tmp/parquet",
  ...overrides,
});

function isRowArray<Row>(rows: unknown[]): rows is Row[] {
  void rows;
  return true;
}

function buildAggregateRunner(rowsByYear: Map<string, AggregateRow[]>): BucketQueryRunner {
  function buildResult<Row>(rows: unknown[]): BucketQueryResult<Row> {
    if (isRowArray<Row>(rows)) return { rows };
    return { rows: [] };
  }
  function queryFn<Row>(sql: string, _params?: unknown[]): Promise<BucketQueryResult<Row>> {
    if (!sql.startsWith("\n    with predictions")) {
      return Promise.resolve(buildResult<Row>([]));
    }
    const match = sql.match(/between '(\d{8})' and '(\d{8})'/);
    const key = match === null ? "?" : (match[1] ?? "?");
    const stored = rowsByYear.get(key) ?? [];
    const widened: unknown[] = Array.from(stored);
    return Promise.resolve(buildResult<Row>(widened));
  }
  return { query: queryFn };
}

interface BuildChunkOpenerArgs {
  runner: BucketQueryRunner;
  loadedRows: number;
  capture: BucketChunkLoaderArgs[];
  closeMock: () => Promise<void>;
}

const buildChunkOpener = (args: BuildChunkOpenerArgs): RunBucketEvalDeps["openChunkClient"] =>
  vi.fn<RunBucketEvalDeps["openChunkClient"]>((loader: BucketChunkLoaderArgs) => {
    args.capture.push(loader);
    const client: BucketChunkClient = {
      runner: args.runner,
      loadedRows: args.loadedRows,
      close: args.closeMock,
    };
    return Promise.resolve(client);
  });

test("buildUsageText renders the multi-line CLI usage", () => {
  expect(buildUsageText()).toBe(
    `Usage:\n  bun run src/scripts/finish-position-features/evaluate-bucket-21y.ts \\\n    --pg-url <connection-string> \\\n    --running-style-feature-version v1 \\\n    --finish-position-version v1 \\\n    [--model-version active] \\\n    [--max-years-per-run 5] \\\n    [--statement-timeout-ms 900000] \\\n    [--ignore-night-window]`,
  );
});

test("CATEGORY_YEAR_WINDOWS exposes jra range 2006-2026", () => {
  expect(CATEGORY_YEAR_WINDOWS[0]).toStrictEqual({
    category: "jra",
    years: [
      2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020,
      2021, 2022, 2023, 2024, 2025, 2026,
    ],
  });
});

test("CATEGORY_YEAR_WINDOWS exposes nar range 2005-2017 plus 2026", () => {
  expect(CATEGORY_YEAR_WINDOWS[1]).toStrictEqual({
    category: "nar",
    years: [2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2026],
  });
});

test("CATEGORY_YEAR_WINDOWS exposes ban-ei range 2016-2026", () => {
  expect(CATEGORY_YEAR_WINDOWS[2]).toStrictEqual({
    category: "ban-ei",
    years: [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026],
  });
});

test("buildYearDateWindow returns from-to YYYYMMDD pair", () => {
  expect(buildYearDateWindow(2024)).toStrictEqual({ fromDate: "20240101", toDate: "20241231" });
});

test("chunkYears splits a 6-element array into chunks of 5 and 1", () => {
  expect(chunkYears([2020, 2021, 2022, 2023, 2024, 2025], 5)).toStrictEqual([
    [2020, 2021, 2022, 2023, 2024],
    [2025],
  ]);
});

test("chunkYears returns one chunk when chunk size exceeds length", () => {
  expect(chunkYears([2024, 2025], 10)).toStrictEqual([[2024, 2025]]);
});

test("chunkYears throws on chunk size zero", () => {
  expect(() => chunkYears([2024], 0)).toThrowError("chunkSize must be greater than zero.");
});

test("buildSessionStatementSql sets local statement_timeout", () => {
  expect(buildSessionStatementSql(900_000)).toBe("set local statement_timeout = '900000ms'");
});

test("buildSessionIdleTimeoutSql sets idle_in_transaction_session_timeout", () => {
  expect(buildSessionIdleTimeoutSql(900_000)).toBe(
    "set local idle_in_transaction_session_timeout = '900000ms'",
  );
});

test("buildSessionWorkMemSql sets work_mem to 256MB", () => {
  expect(buildSessionWorkMemSql()).toBe("set local work_mem = '256MB'");
});

test("buildSessionTuningSqls returns the three SET LOCAL statements", () => {
  expect(buildSessionTuningSqls(900_000)).toStrictEqual([
    "set local statement_timeout = '900000ms'",
    "set local idle_in_transaction_session_timeout = '900000ms'",
    "set local work_mem = '256MB'",
  ]);
});

test("isWithinNightWindow returns true for JST 03 hour", () => {
  expect(isWithinNightWindow({ hourJst: 3, ignoreNightWindow: false })).toBe(true);
});

test("isWithinNightWindow returns true for JST 23 hour", () => {
  expect(isWithinNightWindow({ hourJst: 23, ignoreNightWindow: false })).toBe(true);
});

test("isWithinNightWindow returns false for JST 12 hour by default", () => {
  expect(isWithinNightWindow({ hourJst: 12, ignoreNightWindow: false })).toBe(false);
});

test("isWithinNightWindow returns true when ignoreNightWindow is set even outside window", () => {
  expect(isWithinNightWindow({ hourJst: 12, ignoreNightWindow: true })).toBe(true);
});

test("ensureColimaCapacity passes when cpu and memory satisfy minimum", () => {
  expect(() => ensureColimaCapacity({ cpu: 8, memory: 24 }, 8, 24)).not.toThrowError();
});

test("ensureColimaCapacity throws when cpu is below minimum", () => {
  expect(() => ensureColimaCapacity({ cpu: 4, memory: 24 }, 8, 24)).toThrowError(
    "Colima CPU is below required minimum: 4 < 8. Run 'colima start --cpu 8 --memory 24'.",
  );
});

test("ensureColimaCapacity throws when memory is below minimum", () => {
  expect(() => ensureColimaCapacity({ cpu: 8, memory: 16 }, 8, 24)).toThrowError(
    "Colima memory is below required minimum: 16GB < 24GB. Run 'colima start --cpu 8 --memory 24'.",
  );
});

test("parseColimaStatusJson interprets numeric cpu and gigabyte memory", () => {
  expect(parseColimaStatusJson('{"cpu":8,"memory":24}')).toStrictEqual({ cpu: 8, memory: 24 });
});

test("parseColimaStatusJson converts byte memory above 1024 to gigabytes", () => {
  const bytes = 24 * 1024 * 1024 * 1024;
  expect(parseColimaStatusJson(`{"cpu":8,"memory":${bytes}}`)).toStrictEqual({
    cpu: 8,
    memory: 24,
  });
});

test("parseColimaStatusJson throws on non-object JSON", () => {
  expect(() => parseColimaStatusJson('"oops"')).toThrowError(
    "Colima status JSON is not an object.",
  );
});

test("parseColimaStatusJson coerces non-numeric cpu via Number()", () => {
  expect(parseColimaStatusJson('{"cpu":"8","memory":24}')).toStrictEqual({ cpu: 8, memory: 24 });
});

test("parseColimaStatusJson defaults missing memory to zero", () => {
  expect(parseColimaStatusJson('{"cpu":8}')).toStrictEqual({ cpu: 8, memory: 0 });
});

test("numericFromRow returns '0' for null", () => {
  expect(numericFromRow(null)).toBe("0");
});

test("numericFromRow stringifies a number", () => {
  expect(numericFromRow(42)).toBe("42");
});

test("numericFromRow returns the string as-is", () => {
  expect(numericFromRow("12.5")).toBe("12.5");
});

test("parseArgs accepts a fully specified argv", () => {
  expect(
    parseArgs([
      "--pg-url",
      "postgres://x",
      "--running-style-feature-version",
      "v1",
      "--finish-position-version",
      "v2",
      "--model-version",
      "ensemble-v3",
      "--max-years-per-run",
      "3",
      "--statement-timeout-ms",
      "600000",
      "--predictions-root",
      "/tmp/p",
      "--ignore-night-window",
    ]),
  ).toStrictEqual({
    pgUrl: "postgres://x",
    runningStyleFeatureVersion: "v1",
    finishPositionVersion: "v2",
    modelVersion: "ensemble-v3",
    maxYearsPerRun: 3,
    statementTimeoutMs: 600_000,
    ignoreNightWindow: true,
    perYearSleepMs: 2_000,
    perCategorySleepMs: 5_000,
    minColimaCpu: 8,
    minColimaMemoryGb: 24,
    predictionsRoot: "/tmp/p",
  });
});

test("parseArgs throws when --running-style-feature-version is missing", () => {
  expect(() => parseArgs(["--finish-position-version", "v1"])).toThrowError(
    "--running-style-feature-version is required.",
  );
});

test("parseArgs throws when --finish-position-version is missing", () => {
  expect(() => parseArgs(["--running-style-feature-version", "v1"])).toThrowError(
    "--finish-position-version is required.",
  );
});

test("parseArgs throws on unknown argument", () => {
  expect(() =>
    parseArgs([
      "--running-style-feature-version",
      "v1",
      "--finish-position-version",
      "v1",
      "--bogus",
    ]),
  ).toThrowError("Unknown argument: --bogus");
});

test("parseArgs throws when value is missing for a key", () => {
  expect(() =>
    parseArgs([
      "--running-style-feature-version",
      "v1",
      "--finish-position-version",
      "v1",
      "--model-version",
    ]),
  ).toThrowError("--model-version requires a value.");
});

test("buildUpsertParams returns 30 parameters in the upsert column order", () => {
  const queryFn = vi.fn<BucketQueryRunner["query"]>();
  expect(
    buildUpsertParams(
      {
        runner: { query: queryFn },
        options: baseOptions({}),
        category: "jra",
        fromDate: "20240101",
        toDate: "20241231",
        upsertSql: "upsert",
      },
      baseAggregateRow({
        top1_hit_sum: "1",
        place1_hit_sum: "1",
        top3_box_hit_sum: "1",
        pair_score_sum: "0.5",
        pair_score_pair_count: 120,
        ndcg_at_3_sum: "0.9",
        ndcg_at_3_race_count: 1,
      }),
    ),
  ).toStrictEqual([
    "active",
    "v1",
    "v1",
    "jra",
    "20240101",
    "20241231",
    "jra",
    "05",
    2400,
    "12",
    "703",
    null,
    "10",
    null,
    null,
    "1",
    "16",
    "1",
    "1",
    "0",
    "0",
    "1",
    "0",
    "0",
    "0",
    "0",
    "0.5",
    "120",
    "0.9",
    "1",
  ]);
});

test("processYear skips upsert when race_count is zero", async () => {
  const queryMock = vi.fn<BucketQueryRunner["query"]>().mockResolvedValue({ rows: [] });
  const runner: BucketQueryRunner = { query: queryMock };
  const logMock = vi.fn<(message: string) => void>();
  const result = await processYear({
    runner,
    options: baseOptions({}),
    category: "jra",
    year: 2006,
    log: logMock,
  });
  expect(result).toStrictEqual({ rowCount: 0, raceCount: 0 });
  expect(logMock).toHaveBeenCalledWith("Skip jra 2006: race_count is zero");
});

test("processYear upserts every aggregate row when race_count is positive", async () => {
  const aggregateRows: AggregateRow[] = [
    baseAggregateRow({ race_count: 4, prediction_count: 64 }),
    baseAggregateRow({ keibajo_code: "06", race_count: 2, prediction_count: 30 }),
  ];
  const queryMock = vi
    .fn<BucketQueryRunner["query"]>()
    .mockImplementation((sql: string) =>
      sql.startsWith("\n    with predictions")
        ? Promise.resolve({ rows: aggregateRows })
        : Promise.resolve({ rows: [] }),
    );
  const runner: BucketQueryRunner = { query: queryMock };
  const logMock = vi.fn<(message: string) => void>();
  const result = await processYear({
    runner,
    options: baseOptions({}),
    category: "jra",
    year: 2024,
    log: logMock,
  });
  expect(result).toStrictEqual({ rowCount: 2, raceCount: 6 });
  expect(logMock).toHaveBeenCalledWith("Upserted 2 rows for jra 2024 (races=6)");
});

test("processCategoryChunk opens loader, runs aggregate per year, closes loader", async () => {
  const rowsByYear = new Map<string, AggregateRow[]>([
    ["20240101", [baseAggregateRow({ race_count: 3, prediction_count: 30 })]],
    ["20250101", [baseAggregateRow({ race_count: 1, prediction_count: 10 })]],
  ]);
  const runner = buildAggregateRunner(rowsByYear);
  const closeMock = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  const capture: BucketChunkLoaderArgs[] = [];
  const openChunkClient = buildChunkOpener({ runner, loadedRows: 12, capture, closeMock });
  const sleepMock = vi.fn<(ms: number) => Promise<void>>().mockResolvedValue(undefined);
  const logMock = vi.fn<(message: string) => void>();
  const acc = { totalRows: 0, totalRaces: 0 };
  const poolMock: BucketQueryRunner = { query: vi.fn<BucketQueryRunner["query"]>() };
  await processCategoryChunk(
    {
      pool: poolMock,
      openChunkClient,
      sleep: sleepMock,
      log: logMock,
    },
    baseOptions({}),
    "jra",
    [2024, 2025],
    acc,
  );
  expect(capture).toStrictEqual([
    {
      category: "jra",
      yearFrom: 2024,
      yearTo: 2025,
      predictionsRoot: "/tmp/parquet",
      pgUrl: "postgres://test",
      runningStyleFeatureVersion: "v1",
      finishPositionVersion: "v1",
      modelVersion: "active",
      statementTimeoutMs: 900_000,
    },
  ]);
  expect(closeMock).toHaveBeenCalledTimes(1);
  expect(acc).toStrictEqual({ totalRows: 2, totalRaces: 4 });
});

test("processCategoryChunk closes loader even when an inner query rejects", async () => {
  const runner: BucketQueryRunner = {
    query: vi
      .fn<BucketQueryRunner["query"]>()
      .mockRejectedValueOnce(new Error("boom"))
      .mockResolvedValue({ rows: [] }),
  };
  const closeMock = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  const capture: BucketChunkLoaderArgs[] = [];
  const openChunkClient = buildChunkOpener({ runner, loadedRows: 0, capture, closeMock });
  const poolMock: BucketQueryRunner = { query: vi.fn<BucketQueryRunner["query"]>() };
  await expect(
    processCategoryChunk(
      {
        pool: poolMock,
        openChunkClient,
        sleep: vi.fn<(ms: number) => Promise<void>>().mockResolvedValue(undefined),
        log: vi.fn<(message: string) => void>(),
      },
      baseOptions({}),
      "nar",
      [2010],
      { totalRows: 0, totalRaces: 0 },
    ),
  ).rejects.toThrowError("boom");
  expect(closeMock).toHaveBeenCalledTimes(1);
});

test("runBucketEval iterates categories sequentially and runs ANALYZE at end", async () => {
  const rowsByYear = new Map<string, AggregateRow[]>([
    ["20240101", [baseAggregateRow({ race_count: 1, prediction_count: 16 })]],
    ["20100101", [baseAggregateRow({ race_count: 1, prediction_count: 16 })]],
    ["20200101", [baseAggregateRow({ race_count: 1, prediction_count: 16 })]],
  ]);
  const runner = buildAggregateRunner(rowsByYear);
  const poolQueryMock = vi.fn<BucketQueryRunner["query"]>().mockResolvedValue({ rows: [] });
  const fakePool: BucketQueryRunner = { query: poolQueryMock };
  const closeMock = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  const capture: BucketChunkLoaderArgs[] = [];
  const openChunkClient = buildChunkOpener({ runner, loadedRows: 16, capture, closeMock });
  const sleepMock = vi.fn<(ms: number) => Promise<void>>().mockResolvedValue(undefined);
  const logMock = vi.fn<(message: string) => void>();
  const deps: RunBucketEvalDeps = {
    pool: fakePool,
    openChunkClient,
    sleep: sleepMock,
    log: logMock,
  };
  const singleYearWindows: CategoryYearWindow[] = [
    { category: "jra", years: [2024] },
    { category: "nar", years: [2010] },
    { category: "ban-ei", years: [2020] },
  ];
  const result = await runBucketEval(deps, {
    options: baseOptions({ maxYearsPerRun: 5 }),
    windows: singleYearWindows,
  });
  expect(result).toStrictEqual({ totalRows: 3, totalRaces: 3 });
  expect(capture).toHaveLength(3);
  expect(closeMock).toHaveBeenCalledTimes(3);
  expect(poolQueryMock).toHaveBeenCalledWith("analyze model_prediction_bucket_evaluations");
  expect(poolQueryMock).toHaveBeenCalledWith("analyze race_entry_corner_features");
  expect(poolQueryMock).toHaveBeenCalledWith("analyze jvd_ra");
  expect(poolQueryMock).toHaveBeenCalledWith("analyze nvd_ra");
});

test("runBucketEval silently skips years with zero race_count", async () => {
  const runner: BucketQueryRunner = {
    query: vi.fn<BucketQueryRunner["query"]>().mockResolvedValue({ rows: [] }),
  };
  const poolMock: BucketQueryRunner = {
    query: vi.fn<BucketQueryRunner["query"]>().mockResolvedValue({ rows: [] }),
  };
  const capture: BucketChunkLoaderArgs[] = [];
  const closeMock = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  const openChunkClient = buildChunkOpener({ runner, loadedRows: 0, capture, closeMock });
  const sleepMock = vi.fn<(ms: number) => Promise<void>>().mockResolvedValue(undefined);
  const logMock = vi.fn<(message: string) => void>();
  const deps: RunBucketEvalDeps = {
    pool: poolMock,
    openChunkClient,
    sleep: sleepMock,
    log: logMock,
  };
  const result = await runBucketEval(deps, {
    options: baseOptions({ maxYearsPerRun: 5 }),
    windows: [{ category: "jra", years: [1999, 2000] }],
  });
  expect(result).toStrictEqual({ totalRows: 0, totalRaces: 0 });
  expect(logMock).toHaveBeenCalledWith("Skip jra 1999: race_count is zero");
  expect(logMock).toHaveBeenCalledWith("Skip jra 2000: race_count is zero");
});

test("buildPredictionsParquetGlob appends the hive glob suffix", () => {
  expect(buildPredictionsParquetGlob("/tmp/predictions")).toBe("/tmp/predictions/**/*.parquet");
});

test("buildPythonLoaderArgv emits the year-from / year-to / parquet-glob flags", () => {
  expect(
    buildPythonLoaderArgv({
      category: "jra",
      yearFrom: 2020,
      yearTo: 2024,
      predictionsRoot: "/tmp/p",
      pgUrl: "postgres://x",
      runningStyleFeatureVersion: "v1",
      finishPositionVersion: "v1",
      modelVersion: "active",
      statementTimeoutMs: 900_000,
    }),
  ).toStrictEqual([
    "uv",
    "run",
    "python",
    "src/scripts/load_bucket_predictions.py",
    "--pg-url",
    "postgres://x",
    "--predictions-parquet-glob",
    "/tmp/p/**/*.parquet",
    "--temp-table-name",
    "bucket_predictions_loaded",
    "--category",
    "jra",
    "--year-from",
    "2020",
    "--year-to",
    "2024",
    "--running-style-feature-version",
    "v1",
    "--finish-position-version",
    "v1",
    "--statement-timeout",
    "900000ms",
  ]);
});
