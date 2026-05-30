// Run with: bunx vitest run src/scripts/finish-position-features/evaluate-running-style-bucket-21y.test.ts
import { expect, test, vi } from "vitest";

import type {
  RunningStyleAggregateRow,
  RunningStyleBucketChunkClient,
  RunningStyleBucketChunkLoaderArgs,
  RunningStyleBucketEvalCliOptions,
  RunningStyleBucketQueryResult,
  RunningStyleBucketQueryRunner,
  RunningStyleCategoryYearWindow,
  RunRunningStyleBucketEvalDeps,
} from "./evaluate-running-style-bucket-21y";
import {
  CATEGORY_YEAR_WINDOWS,
  applyArg,
  assertSupportedCategory,
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
  getJstHour,
  initialOptions,
  isWithinNightWindow,
  numericFromRow,
  parseArgs,
  parseColimaStatusJson,
  processCategoryChunk,
  processYear,
  resolveModelVersion,
  runRunningStyleBucketEval,
} from "./evaluate-running-style-bucket-21y";

const baseAggregateRow = (
  overrides: Partial<RunningStyleAggregateRow>,
): RunningStyleAggregateRow => ({
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
  cm_actual_nige_pred_nige_count: 0,
  cm_actual_nige_pred_senkou_count: 0,
  cm_actual_nige_pred_sashi_count: 0,
  cm_actual_nige_pred_oikomi_count: 0,
  cm_actual_senkou_pred_nige_count: 0,
  cm_actual_senkou_pred_senkou_count: 0,
  cm_actual_senkou_pred_sashi_count: 0,
  cm_actual_senkou_pred_oikomi_count: 0,
  cm_actual_sashi_pred_nige_count: 0,
  cm_actual_sashi_pred_senkou_count: 0,
  cm_actual_sashi_pred_sashi_count: 0,
  cm_actual_sashi_pred_oikomi_count: 0,
  cm_actual_oikomi_pred_nige_count: 0,
  cm_actual_oikomi_pred_senkou_count: 0,
  cm_actual_oikomi_pred_sashi_count: 0,
  cm_actual_oikomi_pred_oikomi_count: 0,
  log_loss_nige_sum: "0",
  log_loss_nige_count: 0,
  log_loss_senkou_sum: "0",
  log_loss_senkou_count: 0,
  log_loss_sashi_sum: "0",
  log_loss_sashi_count: 0,
  log_loss_oikomi_sum: "0",
  log_loss_oikomi_count: 0,
  top2_hit_count: 0,
  ...overrides,
});

const baseOptions = (
  overrides: Partial<RunningStyleBucketEvalCliOptions>,
): RunningStyleBucketEvalCliOptions => ({
  pgUrl: "postgres://test",
  runningStyleFeatureVersion: "v1",
  modelVersionJra: "jra-model-v1",
  modelVersionNar: "nar-model-v1",
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

function buildAggregateRunner(
  rowsByYear: Map<string, RunningStyleAggregateRow[]>,
): RunningStyleBucketQueryRunner {
  function buildResult<Row>(rows: unknown[]): RunningStyleBucketQueryResult<Row> {
    if (isRowArray<Row>(rows)) return { rows };
    return { rows: [] };
  }
  function queryFn<Row>(
    sql: string,
    _params?: unknown[],
  ): Promise<RunningStyleBucketQueryResult<Row>> {
    if (!sql.startsWith("\n    with predictions")) {
      return Promise.resolve(buildResult<Row>([]));
    }
    const match = sql.match(/race_date between '(\d{8})' and '(\d{8})'/);
    const key = match === null ? "?" : (match[1] ?? "?");
    const stored = rowsByYear.get(key) ?? [];
    const widened: unknown[] = Array.from(stored);
    return Promise.resolve(buildResult<Row>(widened));
  }
  return { query: queryFn };
}

interface BuildChunkOpenerArgs {
  runner: RunningStyleBucketQueryRunner;
  loadedRows: number;
  capture: RunningStyleBucketChunkLoaderArgs[];
  closeMock: () => Promise<void>;
}

const buildChunkOpener = (
  args: BuildChunkOpenerArgs,
): RunRunningStyleBucketEvalDeps["openChunkClient"] =>
  vi.fn<RunRunningStyleBucketEvalDeps["openChunkClient"]>(
    (loader: RunningStyleBucketChunkLoaderArgs) => {
      args.capture.push(loader);
      const client: RunningStyleBucketChunkClient = {
        runner: args.runner,
        loadedRows: args.loadedRows,
        close: args.closeMock,
      };
      return Promise.resolve(client);
    },
  );

test("buildUsageText renders the CLI usage with model-version-jra/nar flags", () => {
  expect(buildUsageText()).toBe(
    `Usage:\n  bun run src/scripts/finish-position-features/evaluate-running-style-bucket-21y.ts \\\n    --pg-url <connection-string> \\\n    --running-style-feature-version v1 \\\n    --model-version-jra <jra-model> \\\n    --model-version-nar <nar-model> \\\n    [--max-years-per-run 5] \\\n    [--statement-timeout-ms 900000] \\\n    [--ignore-night-window]`,
  );
});

test("CATEGORY_YEAR_WINDOWS exposes only jra and nar (no ban-ei)", () => {
  expect(CATEGORY_YEAR_WINDOWS).toStrictEqual([
    {
      category: "jra",
      years: [
        2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020,
        2021, 2022, 2023, 2024, 2025, 2026,
      ],
    },
    {
      category: "nar",
      years: [2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2026],
    },
  ]);
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

test("resolveModelVersion picks modelVersionJra for jra category", () => {
  expect(
    resolveModelVersion(
      baseOptions({ modelVersionJra: "jra-vX", modelVersionNar: "nar-vY" }),
      "jra",
    ),
  ).toBe("jra-vX");
});

test("resolveModelVersion picks modelVersionNar for nar category", () => {
  expect(
    resolveModelVersion(
      baseOptions({ modelVersionJra: "jra-vX", modelVersionNar: "nar-vY" }),
      "nar",
    ),
  ).toBe("nar-vY");
});

test("assertSupportedCategory accepts jra", () => {
  expect(() => assertSupportedCategory("jra")).not.toThrowError();
});

test("assertSupportedCategory accepts nar", () => {
  expect(() => assertSupportedCategory("nar")).not.toThrowError();
});

test("assertSupportedCategory throws for ban-ei category", () => {
  expect(() => assertSupportedCategory("ban-ei")).toThrowError(
    "Unsupported running-style category: ban-ei",
  );
});

test("assertSupportedCategory throws for unknown string category", () => {
  expect(() => assertSupportedCategory("xxx")).toThrowError(
    "Unsupported running-style category: xxx",
  );
});

test("initialOptions seeds empty version strings and 0-based sleeps", () => {
  const options = initialOptions();
  expect(options.runningStyleFeatureVersion).toBe("");
  expect(options.modelVersionJra).toBe("");
  expect(options.modelVersionNar).toBe("");
});

test("applyArg handles --pg-url", () => {
  const options = initialOptions();
  expect(applyArg(options, "--pg-url", "postgres://x")).toStrictEqual({ advanceBy: 2 });
  expect(options.pgUrl).toBe("postgres://x");
});

test("applyArg throws on unknown argument", () => {
  expect(() => applyArg(initialOptions(), "--bogus", "x")).toThrowError(
    "Unknown argument: --bogus",
  );
});

test("applyArg throws when --pg-url value is missing", () => {
  expect(() => applyArg(initialOptions(), "--pg-url", undefined)).toThrowError(
    "--pg-url requires a value.",
  );
});

test("applyArg handles --running-style-feature-version", () => {
  const options = initialOptions();
  expect(applyArg(options, "--running-style-feature-version", "v2")).toStrictEqual({
    advanceBy: 2,
  });
  expect(options.runningStyleFeatureVersion).toBe("v2");
});

test("applyArg handles --max-years-per-run integer", () => {
  const options = initialOptions();
  expect(applyArg(options, "--max-years-per-run", "3")).toStrictEqual({ advanceBy: 2 });
  expect(options.maxYearsPerRun).toBe(3);
});

test("applyArg handles --statement-timeout-ms integer", () => {
  const options = initialOptions();
  expect(applyArg(options, "--statement-timeout-ms", "600000")).toStrictEqual({ advanceBy: 2 });
  expect(options.statementTimeoutMs).toBe(600_000);
});

test("applyArg handles --predictions-root", () => {
  const options = initialOptions();
  expect(applyArg(options, "--predictions-root", "/x")).toStrictEqual({ advanceBy: 2 });
  expect(options.predictionsRoot).toBe("/x");
});

test("applyArg handles --ignore-night-window single flag", () => {
  const options = initialOptions();
  expect(applyArg(options, "--ignore-night-window", undefined)).toStrictEqual({ advanceBy: 1 });
  expect(options.ignoreNightWindow).toBe(true);
});

test("parseArgs accepts a fully specified argv", () => {
  expect(
    parseArgs([
      "--pg-url",
      "postgres://x",
      "--running-style-feature-version",
      "v1",
      "--model-version-jra",
      "jra-v1.3",
      "--model-version-nar",
      "nar-v1.4",
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
    modelVersionJra: "jra-v1.3",
    modelVersionNar: "nar-v1.4",
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
  expect(() => parseArgs(["--model-version-jra", "j", "--model-version-nar", "n"])).toThrowError(
    "--running-style-feature-version is required.",
  );
});

test("parseArgs throws when --model-version-jra is missing", () => {
  expect(() =>
    parseArgs(["--running-style-feature-version", "v1", "--model-version-nar", "n"]),
  ).toThrowError("--model-version-jra is required.");
});

test("parseArgs throws when --model-version-nar is missing", () => {
  expect(() =>
    parseArgs(["--running-style-feature-version", "v1", "--model-version-jra", "j"]),
  ).toThrowError("--model-version-nar is required.");
});

test("parseArgs throws on unknown argument", () => {
  expect(() =>
    parseArgs([
      "--running-style-feature-version",
      "v1",
      "--model-version-jra",
      "j",
      "--model-version-nar",
      "n",
      "--bogus",
    ]),
  ).toThrowError("Unknown argument: --bogus");
});

test("parseArgs throws when value is missing for a key", () => {
  expect(() =>
    parseArgs([
      "--running-style-feature-version",
      "v1",
      "--model-version-jra",
      "j",
      "--model-version-nar",
    ]),
  ).toThrowError("--model-version-nar requires a value.");
});

test("buildUpsertParams returns 41 parameters with jra model version for jra category", () => {
  expect(
    buildUpsertParams(
      {
        options: baseOptions({}),
        category: "jra",
        fromDate: "20240101",
        toDate: "20241231",
      },
      baseAggregateRow({
        cm_actual_nige_pred_nige_count: 4,
        cm_actual_senkou_pred_senkou_count: 10,
        log_loss_nige_sum: "1.5",
        log_loss_nige_count: 5,
        top2_hit_count: 12,
      }),
    ),
  ).toStrictEqual([
    "jra-model-v1",
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
    "4",
    "0",
    "0",
    "0",
    "0",
    "10",
    "0",
    "0",
    "0",
    "0",
    "0",
    "0",
    "0",
    "0",
    "0",
    "0",
    "1.5",
    "5",
    "0",
    "0",
    "0",
    "0",
    "0",
    "0",
    "12",
  ]);
});

test("buildUpsertParams uses modelVersionNar for nar category", () => {
  const params = buildUpsertParams(
    {
      options: baseOptions({ modelVersionNar: "nar-vY" }),
      category: "nar",
      fromDate: "20100101",
      toDate: "20101231",
    },
    baseAggregateRow({ source: "nar" }),
  );
  expect(params[0]).toBe("nar-vY");
  expect(params[2]).toBe("nar");
});

test("processYear skips upsert when race_count is zero", async () => {
  const queryMock = vi.fn<RunningStyleBucketQueryRunner["query"]>().mockResolvedValue({ rows: [] });
  const runner: RunningStyleBucketQueryRunner = { query: queryMock };
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
  const aggregateRows: RunningStyleAggregateRow[] = [
    baseAggregateRow({ race_count: 4, prediction_count: 64 }),
    baseAggregateRow({ keibajo_code: "06", race_count: 2, prediction_count: 30 }),
  ];
  const queryMock = vi
    .fn<RunningStyleBucketQueryRunner["query"]>()
    .mockImplementation((sql: string) =>
      sql.startsWith("\n    with predictions")
        ? Promise.resolve({ rows: aggregateRows })
        : Promise.resolve({ rows: [] }),
    );
  const runner: RunningStyleBucketQueryRunner = { query: queryMock };
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

test("processYear throws when category is ban-ei", async () => {
  const queryMock = vi.fn<RunningStyleBucketQueryRunner["query"]>().mockResolvedValue({ rows: [] });
  const runner: RunningStyleBucketQueryRunner = { query: queryMock };
  const logMock = vi.fn<(message: string) => void>();
  await expect(
    processYear({
      runner,
      options: baseOptions({}),
      category: "ban-ei",
      year: 2024,
      log: logMock,
    }),
  ).rejects.toThrowError("Unsupported running-style category: ban-ei");
});

test("processCategoryChunk opens loader, runs aggregate per year, closes loader", async () => {
  const rowsByYear = new Map<string, RunningStyleAggregateRow[]>([
    ["20240101", [baseAggregateRow({ race_count: 3, prediction_count: 30 })]],
    ["20250101", [baseAggregateRow({ race_count: 1, prediction_count: 10 })]],
  ]);
  const runner = buildAggregateRunner(rowsByYear);
  const closeMock = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  const capture: RunningStyleBucketChunkLoaderArgs[] = [];
  const openChunkClient = buildChunkOpener({ runner, loadedRows: 12, capture, closeMock });
  const sleepMock = vi.fn<(ms: number) => Promise<void>>().mockResolvedValue(undefined);
  const logMock = vi.fn<(message: string) => void>();
  const acc = { totalRows: 0, totalRaces: 0 };
  const poolMock: RunningStyleBucketQueryRunner = {
    query: vi.fn<RunningStyleBucketQueryRunner["query"]>(),
  };
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
      modelVersion: "jra-model-v1",
    },
  ]);
  expect(closeMock).toHaveBeenCalledTimes(1);
  expect(acc).toStrictEqual({ totalRows: 2, totalRaces: 4 });
});

test("processCategoryChunk throws and closes loader for ban-ei category", async () => {
  const runner: RunningStyleBucketQueryRunner = {
    query: vi.fn<RunningStyleBucketQueryRunner["query"]>().mockResolvedValue({ rows: [] }),
  };
  const closeMock = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  const capture: RunningStyleBucketChunkLoaderArgs[] = [];
  const openChunkClient = buildChunkOpener({ runner, loadedRows: 0, capture, closeMock });
  const poolMock: RunningStyleBucketQueryRunner = {
    query: vi.fn<RunningStyleBucketQueryRunner["query"]>(),
  };
  await expect(
    processCategoryChunk(
      {
        pool: poolMock,
        openChunkClient,
        sleep: vi.fn<(ms: number) => Promise<void>>().mockResolvedValue(undefined),
        log: vi.fn<(message: string) => void>(),
      },
      baseOptions({}),
      "ban-ei",
      [2024],
      { totalRows: 0, totalRaces: 0 },
    ),
  ).rejects.toThrowError("Unsupported running-style category: ban-ei");
});

test("processCategoryChunk closes loader even when an inner query rejects", async () => {
  const runner: RunningStyleBucketQueryRunner = {
    query: vi
      .fn<RunningStyleBucketQueryRunner["query"]>()
      .mockRejectedValueOnce(new Error("boom"))
      .mockResolvedValue({ rows: [] }),
  };
  const closeMock = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  const capture: RunningStyleBucketChunkLoaderArgs[] = [];
  const openChunkClient = buildChunkOpener({ runner, loadedRows: 0, capture, closeMock });
  const poolMock: RunningStyleBucketQueryRunner = {
    query: vi.fn<RunningStyleBucketQueryRunner["query"]>(),
  };
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

test("runRunningStyleBucketEval iterates jra and nar sequentially and ANALYZE at end", async () => {
  const rowsByYear = new Map<string, RunningStyleAggregateRow[]>([
    ["20240101", [baseAggregateRow({ race_count: 1, prediction_count: 16 })]],
    ["20100101", [baseAggregateRow({ race_count: 1, prediction_count: 16 })]],
  ]);
  const runner = buildAggregateRunner(rowsByYear);
  const poolQueryMock = vi
    .fn<RunningStyleBucketQueryRunner["query"]>()
    .mockResolvedValue({ rows: [] });
  const fakePool: RunningStyleBucketQueryRunner = { query: poolQueryMock };
  const closeMock = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  const capture: RunningStyleBucketChunkLoaderArgs[] = [];
  const openChunkClient = buildChunkOpener({ runner, loadedRows: 16, capture, closeMock });
  const sleepMock = vi.fn<(ms: number) => Promise<void>>().mockResolvedValue(undefined);
  const logMock = vi.fn<(message: string) => void>();
  const deps: RunRunningStyleBucketEvalDeps = {
    pool: fakePool,
    openChunkClient,
    sleep: sleepMock,
    log: logMock,
  };
  const singleYearWindows: RunningStyleCategoryYearWindow[] = [
    { category: "jra", years: [2024] },
    { category: "nar", years: [2010] },
  ];
  const result = await runRunningStyleBucketEval(deps, {
    options: baseOptions({ maxYearsPerRun: 5 }),
    windows: singleYearWindows,
  });
  expect(result).toStrictEqual({ totalRows: 2, totalRaces: 2 });
  expect(capture).toHaveLength(2);
  expect(closeMock).toHaveBeenCalledTimes(2);
  expect(poolQueryMock).toHaveBeenCalledWith("analyze running_style_model_bucket_evaluations");
  expect(poolQueryMock).toHaveBeenCalledWith("analyze jvd_ra");
  expect(poolQueryMock).toHaveBeenCalledWith("analyze nvd_ra");
});

test("runRunningStyleBucketEval silently skips years with zero race_count", async () => {
  const runner: RunningStyleBucketQueryRunner = {
    query: vi.fn<RunningStyleBucketQueryRunner["query"]>().mockResolvedValue({ rows: [] }),
  };
  const poolMock: RunningStyleBucketQueryRunner = {
    query: vi.fn<RunningStyleBucketQueryRunner["query"]>().mockResolvedValue({ rows: [] }),
  };
  const capture: RunningStyleBucketChunkLoaderArgs[] = [];
  const closeMock = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  const openChunkClient = buildChunkOpener({ runner, loadedRows: 0, capture, closeMock });
  const sleepMock = vi.fn<(ms: number) => Promise<void>>().mockResolvedValue(undefined);
  const logMock = vi.fn<(message: string) => void>();
  const deps: RunRunningStyleBucketEvalDeps = {
    pool: poolMock,
    openChunkClient,
    sleep: sleepMock,
    log: logMock,
  };
  const result = await runRunningStyleBucketEval(deps, {
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

test("buildPythonLoaderArgv passes year-from / year-to / model-version flags", () => {
  expect(
    buildPythonLoaderArgv({
      category: "jra",
      yearFrom: 2020,
      yearTo: 2024,
      predictionsRoot: "/tmp/p",
      pgUrl: "postgres://x",
      runningStyleFeatureVersion: "v1",
      modelVersion: "jra-v1.3",
    }),
  ).toStrictEqual([
    "uv",
    "run",
    "python",
    "src/scripts/load_running_style_predictions.py",
    "--pg-url",
    "postgres://x",
    "--predictions-parquet-glob",
    "/tmp/p/**/*.parquet",
    "--temp-table-name",
    "bucket_running_style_predictions_loaded",
    "--category",
    "jra",
    "--year-from",
    "2020",
    "--year-to",
    "2024",
    "--running-style-feature-version",
    "v1",
    "--model-version",
    "jra-v1.3",
  ]);
});

test("getJstHour returns the hour in JST", () => {
  const sample = new Date("2026-05-30T17:00:00Z");
  expect(getJstHour(sample)).toBe(2);
});
