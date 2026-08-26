// run with: bun run test
import { beforeEach, expect, it, vi } from "vitest";
import type { RunningStyleRaceParams } from "./running-style-features";
import type { RaceHorseFeatureRow } from "./running-style-r2";
import type { Env } from "./types";

vi.mock("./running-style-catalog-client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./running-style-catalog-client")>();
  return {
    ...actual,
    fetchRunningStyleFeaturesFromCatalog: vi.fn(),
  };
});
vi.mock("./finish-position-lite-pool", () => ({
  getFinishPositionPool: vi.fn(() => ({ query: vi.fn() })),
}));
vi.mock("./running-style-feature-sql", () => ({
  buildRunningStyleFeaturesForRaceFromPostgres: vi.fn(),
}));
vi.mock("./running-style-finish-feature-hit", () => ({
  loadRunningStyleFeaturesFromFinishPositionDayBase: vi.fn(async () => null),
}));
vi.mock("./running-style-feature-parquet", () => ({
  buildRunningStyleFeatureParquetKey: vi.fn(() => "features.parquet"),
  loadRunningStyleFeatureParquet: vi.fn(),
  putRunningStyleFeatureParquet: vi.fn(),
  validateFeatureCoverage: vi.fn(),
}));
vi.mock("./running-style-features", () => ({
  buildRunningStyleRaceKey: vi.fn(() => "jra:20260513:08:01"),
}));
vi.mock("./running-style-model-binary", () => ({
  buildRunningStyleFlatModelKey: vi.fn(() => "model.flatbin"),
  loadFlatLightGBMHeaderFromR2: vi.fn(async () => ({
    feature_names: ["f1"],
    model_version: "v3",
  })),
  loadFlatLightGBMModelFromR2: vi.fn(async () => ({
    header: { feature_names: ["f1"], model_version: "v3" },
  })),
}));
vi.mock("./running-style-race-list", () => ({
  listRunningStyleRacesByDate: vi.fn(),
}));
vi.mock("./format-error", () => ({
  formatError: vi.fn((error: unknown) => (error instanceof Error ? error.message : "error")),
}));

const makeEnv = (writeEnabled: string): Env =>
  Object.assign(JSON.parse("{}"), {
    PC_KEIBA_R2_CATALOG: { fetch: vi.fn() },
    FEATURES_ARCHIVE: {},
    REALTIME_DB: {},
    RUNNING_STYLE_D1_WRITE_ENABLED: writeEnabled,
    RUNNING_STYLE_MODELS: {},
  });

const RACE: RunningStyleRaceParams = {
  kaisaiNen: "2026",
  kaisaiTsukihi: "0513",
  keibajoCode: "08",
  raceBango: "01",
  source: "jra",
};

const rows = (): RaceHorseFeatureRow[] =>
  JSON.parse('[{"raceKey":"jra:20260513:08:01","umaban":1}]');

beforeEach(async () => {
  vi.clearAllMocks();
  const { loadRunningStyleFeatureParquet } = await import("./running-style-feature-parquet");
  vi.mocked(loadRunningStyleFeatureParquet).mockRejectedValue(new Error("R2 cache missing"));
  const { buildRunningStyleFeaturesForRaceFromPostgres } =
    await import("./running-style-feature-sql");
  vi.mocked(buildRunningStyleFeaturesForRaceFromPostgres).mockRejectedValue(
    new Error("PostgreSQL fallback unavailable"),
  );
  const { loadRunningStyleFeaturesFromFinishPositionDayBase } =
    await import("./running-style-finish-feature-hit");
  vi.mocked(loadRunningStyleFeaturesFromFinishPositionDayBase).mockResolvedValue(null);
});

it("uses the finish-position day-base HIT before Catalog", async () => {
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { putRunningStyleFeatureParquet, validateFeatureCoverage } =
    await import("./running-style-feature-parquet");
  const { loadRunningStyleFeaturesFromFinishPositionDayBase } =
    await import("./running-style-finish-feature-hit");
  const env = makeEnv("1");
  vi.spyOn(console, "log").mockImplementation(() => {});
  vi.mocked(loadRunningStyleFeaturesFromFinishPositionDayBase).mockResolvedValue(rows());
  vi.mocked(validateFeatureCoverage).mockReturnValue({
    missingCells: 0,
    missingFeatureNames: [],
  });
  vi.mocked(putRunningStyleFeatureParquet).mockResolvedValue(42);

  await expect(
    loadOrBuildRunningStyleFeatureParquet({ env, featureNames: ["f1"], race: RACE }),
  ).resolves.toStrictEqual({ featuresR2Key: "features.parquet", rebuilt: true, rows: rows() });
  expect(loadRunningStyleFeaturesFromFinishPositionDayBase).toHaveBeenCalledWith({
    bucket: env.FEATURES_ARCHIVE,
    featureNames: ["f1"],
    race: RACE,
  });
  expect(fetchRunningStyleFeaturesFromCatalog).not.toHaveBeenCalled();
  expect(vi.mocked(console.log).mock.calls[0]?.[0]).toBe(
    "Running-style features HIT finish-position day-base for jra:20260513:08:01",
  );
});

it("falls back to Catalog when the finish-position day-base read fails", async () => {
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { putRunningStyleFeatureParquet, validateFeatureCoverage } =
    await import("./running-style-feature-parquet");
  const { loadRunningStyleFeaturesFromFinishPositionDayBase } =
    await import("./running-style-finish-feature-hit");
  vi.spyOn(console, "warn").mockImplementation(() => {});
  vi.mocked(loadRunningStyleFeaturesFromFinishPositionDayBase).mockRejectedValue(
    new Error("invalid day-base parquet"),
  );
  vi.mocked(fetchRunningStyleFeaturesFromCatalog).mockResolvedValue(rows());
  vi.mocked(validateFeatureCoverage).mockReturnValue({
    missingCells: 0,
    missingFeatureNames: [],
  });
  vi.mocked(putRunningStyleFeatureParquet).mockResolvedValue(42);

  await expect(
    loadOrBuildRunningStyleFeatureParquet({
      env: makeEnv("1"),
      featureNames: ["f1"],
      race: RACE,
    }),
  ).resolves.toStrictEqual({ featuresR2Key: "features.parquet", rebuilt: true, rows: rows() });
  expect(vi.mocked(console.warn).mock.calls[0]?.[0]).toBe(
    "Running-style finish-position day-base MISS for jra:20260513:08:01: invalid day-base parquet",
  );
});

it("returns a coverage-complete per-race cache before day-base, Catalog, or PostgreSQL", async () => {
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { getFinishPositionPool } = await import("./finish-position-lite-pool");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { loadRunningStyleFeatureParquet, putRunningStyleFeatureParquet, validateFeatureCoverage } =
    await import("./running-style-feature-parquet");
  const { buildRunningStyleFeaturesForRaceFromPostgres } =
    await import("./running-style-feature-sql");
  const { loadRunningStyleFeaturesFromFinishPositionDayBase } =
    await import("./running-style-finish-feature-hit");
  const env = makeEnv("1");
  const cachedRows: RaceHorseFeatureRow[] = JSON.parse(
    '[{"raceKey":"jra:20260513:08:01","umaban":1},{"raceKey":"jra:20260513:08:01","umaban":9}]',
  );
  vi.mocked(loadRunningStyleFeatureParquet).mockResolvedValue(cachedRows);
  vi.mocked(validateFeatureCoverage).mockReturnValue({
    missingCells: 0,
    missingFeatureNames: [],
  });
  const result = await loadOrBuildRunningStyleFeatureParquet({
    env,
    featureNames: ["f1"],
    race: RACE,
  });
  expect(result).toStrictEqual({
    featuresR2Key: "features.parquet",
    rebuilt: false,
    rows: cachedRows,
  });
  expect(loadRunningStyleFeatureParquet).toHaveBeenCalledWith(
    env.RUNNING_STYLE_MODELS,
    "features.parquet",
    ["f1"],
  );
  expect(loadRunningStyleFeaturesFromFinishPositionDayBase).not.toHaveBeenCalled();
  expect(fetchRunningStyleFeaturesFromCatalog).not.toHaveBeenCalled();
  expect(getFinishPositionPool).not.toHaveBeenCalled();
  expect(buildRunningStyleFeaturesForRaceFromPostgres).not.toHaveBeenCalled();
  expect(putRunningStyleFeatureParquet).not.toHaveBeenCalled();
});

it("rebuilds when the processed cache cannot be decoded", async () => {
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { loadRunningStyleFeatureParquet, putRunningStyleFeatureParquet } =
    await import("./running-style-feature-parquet");
  const env = makeEnv("1");
  vi.mocked(loadRunningStyleFeatureParquet).mockRejectedValue(new Error("invalid cache parquet"));
  vi.mocked(fetchRunningStyleFeaturesFromCatalog).mockRejectedValue(
    new Error("Catalog unavailable"),
  );
  await expect(
    loadOrBuildRunningStyleFeatureParquet({
      env,
      featureNames: ["f1"],
      race: RACE,
    }),
  ).rejects.toThrow("Catalog unavailable");
  expect(loadRunningStyleFeatureParquet).toHaveBeenCalledWith(
    env.RUNNING_STYLE_MODELS,
    "features.parquet",
    ["f1"],
  );
  expect(putRunningStyleFeatureParquet).not.toHaveBeenCalled();
});

it("rebuilds from Catalog when cached rows miss a requested model feature", async () => {
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { loadRunningStyleFeatureParquet, putRunningStyleFeatureParquet, validateFeatureCoverage } =
    await import("./running-style-feature-parquet");
  const env = makeEnv("1");
  vi.mocked(loadRunningStyleFeatureParquet).mockResolvedValue(rows());
  vi.mocked(validateFeatureCoverage)
    .mockReturnValueOnce({ missingCells: 1, missingFeatureNames: ["f1"] })
    .mockReturnValueOnce({ missingCells: 0, missingFeatureNames: [] });
  vi.mocked(fetchRunningStyleFeaturesFromCatalog).mockResolvedValue(rows());
  vi.mocked(putRunningStyleFeatureParquet).mockResolvedValue(42);

  await expect(
    loadOrBuildRunningStyleFeatureParquet({ env, featureNames: ["f1"], race: RACE }),
  ).resolves.toStrictEqual({ featuresR2Key: "features.parquet", rebuilt: true, rows: rows() });
  expect(fetchRunningStyleFeaturesFromCatalog).toHaveBeenCalledWith(env.PC_KEIBA_R2_CATALOG, RACE, [
    "f1",
  ]);
  expect(putRunningStyleFeatureParquet).toHaveBeenCalledWith(
    env.RUNNING_STYLE_MODELS,
    "features.parquet",
    rows(),
    ["f1"],
  );
});

it("returns a valid R2 parquet without calling an unavailable Catalog", async () => {
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { loadRunningStyleFeatureParquet, putRunningStyleFeatureParquet, validateFeatureCoverage } =
    await import("./running-style-feature-parquet");
  vi.mocked(fetchRunningStyleFeaturesFromCatalog).mockRejectedValue(new Error("must not run"));
  vi.mocked(loadRunningStyleFeatureParquet).mockResolvedValue(rows());
  vi.mocked(validateFeatureCoverage).mockReturnValue({
    missingCells: 0,
    missingFeatureNames: [],
  });
  const result = await loadOrBuildRunningStyleFeatureParquet({
    env: makeEnv("1"),
    featureNames: ["f1"],
    race: RACE,
  });
  expect(result).toStrictEqual({
    featuresR2Key: "features.parquet",
    rebuilt: false,
    rows: rows(),
  });
  expect(putRunningStyleFeatureParquet).not.toHaveBeenCalled();
  expect(fetchRunningStyleFeaturesFromCatalog).not.toHaveBeenCalled();
});

it("rebuilds and writes the current Parquet from PostgreSQL when Catalog is unavailable", async () => {
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { getFinishPositionPool } = await import("./finish-position-lite-pool");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { putRunningStyleFeatureParquet, validateFeatureCoverage } =
    await import("./running-style-feature-parquet");
  const { buildRunningStyleFeaturesForRaceFromPostgres } =
    await import("./running-style-feature-sql");
  const env = makeEnv("1");
  vi.spyOn(console, "error").mockImplementation(() => {});
  vi.mocked(fetchRunningStyleFeaturesFromCatalog).mockRejectedValue(
    new Error("running-style Catalog request timed out after 45000ms"),
  );
  vi.mocked(buildRunningStyleFeaturesForRaceFromPostgres).mockResolvedValue({
    elapsedMs: 321,
    rows: rows(),
    sqlRows: 1,
  });
  vi.mocked(validateFeatureCoverage).mockReturnValue({
    missingCells: 0,
    missingFeatureNames: [],
  });
  vi.mocked(putRunningStyleFeatureParquet).mockResolvedValue(42);

  await expect(
    loadOrBuildRunningStyleFeatureParquet({ env, featureNames: ["f1"], race: RACE }),
  ).resolves.toStrictEqual({
    featuresR2Key: "features.parquet",
    rebuilt: true,
    rows: rows(),
  });
  expect(getFinishPositionPool).toHaveBeenCalledWith(env);
  expect(buildRunningStyleFeaturesForRaceFromPostgres).toHaveBeenCalledWith(
    vi.mocked(getFinishPositionPool).mock.results[0]?.value,
    RACE,
    ["f1"],
  );
  expect(putRunningStyleFeatureParquet).toHaveBeenCalledWith(
    env.RUNNING_STYLE_MODELS,
    "features.parquet",
    rows(),
    ["f1"],
  );
  expect(vi.mocked(console.error).mock.calls[0]?.[0]).toBe(
    "Running-style Catalog unavailable, rebuilt from PostgreSQL mirror in 321ms: running-style Catalog request timed out after 45000ms",
  );
});

it("keeps the Catalog error when fallback parquet is missing", async () => {
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { loadRunningStyleFeatureParquet, putRunningStyleFeatureParquet } =
    await import("./running-style-feature-parquet");
  const catalogError = new Error(
    "PC_KEIBA_R2_CATALOG /v1/running-style-features failed with HTTP 503",
  );
  vi.mocked(fetchRunningStyleFeaturesFromCatalog).mockRejectedValue(catalogError);
  vi.mocked(loadRunningStyleFeatureParquet).mockRejectedValue(new Error("R2 object not found"));
  await expect(
    loadOrBuildRunningStyleFeatureParquet({
      env: makeEnv("1"),
      featureNames: ["f1"],
      race: RACE,
    }),
  ).rejects.toThrow("PC_KEIBA_R2_CATALOG /v1/running-style-features failed with HTTP 503");
  expect(putRunningStyleFeatureParquet).not.toHaveBeenCalled();
});

it("does not start the slow PostgreSQL fallback for an R2 SQL resource exhaustion", async () => {
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { buildRunningStyleFeaturesForRaceFromPostgres } =
    await import("./running-style-feature-sql");
  const catalogError = new Error(
    "PC_KEIBA_R2_CATALOG /v1/running-style-features failed with HTTP 502: r2_sql_unavailable code=70200 R2 SQL HTTP 500: 70200 Internal Server Error",
  );
  vi.mocked(fetchRunningStyleFeaturesFromCatalog).mockRejectedValue(catalogError);

  await expect(
    loadOrBuildRunningStyleFeatureParquet({
      env: makeEnv("1"),
      featureNames: ["f1"],
      race: RACE,
    }),
  ).rejects.toThrow(catalogError.message);
  expect(buildRunningStyleFeaturesForRaceFromPostgres).not.toHaveBeenCalled();
});

it("also treats R2 SQL query-plan rejection 60104 as resource exhaustion", async () => {
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { buildRunningStyleFeaturesForRaceFromPostgres } =
    await import("./running-style-feature-sql");
  const catalogError = new Error(
    "PC_KEIBA_R2_CATALOG /v1/running-style-features failed with HTTP 502: r2_sql_unavailable code=60104 R2 SQL query plan rejected",
  );
  vi.mocked(fetchRunningStyleFeaturesFromCatalog).mockRejectedValue(catalogError);

  await expect(
    loadOrBuildRunningStyleFeatureParquet({
      env: makeEnv("1"),
      featureNames: ["f1"],
      race: RACE,
    }),
  ).rejects.toThrow(catalogError.message);
  expect(buildRunningStyleFeaturesForRaceFromPostgres).not.toHaveBeenCalled();
});

it("keeps the Catalog error when fallback parquet misses model features", async () => {
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { loadRunningStyleFeatureParquet, putRunningStyleFeatureParquet, validateFeatureCoverage } =
    await import("./running-style-feature-parquet");
  const catalogError = new Error(
    "PC_KEIBA_R2_CATALOG /v1/running-style-features failed with HTTP 502: r2_sql_unavailable",
  );
  vi.mocked(fetchRunningStyleFeaturesFromCatalog).mockRejectedValue(catalogError);
  vi.mocked(loadRunningStyleFeatureParquet).mockResolvedValue(rows());
  vi.mocked(validateFeatureCoverage).mockReturnValue({
    missingCells: 1,
    missingFeatureNames: ["f1"],
  });
  await expect(
    loadOrBuildRunningStyleFeatureParquet({
      env: makeEnv("1"),
      featureNames: ["f1"],
      race: RACE,
    }),
  ).rejects.toThrow(
    "PC_KEIBA_R2_CATALOG /v1/running-style-features failed with HTTP 502: r2_sql_unavailable",
  );
  expect(putRunningStyleFeatureParquet).not.toHaveBeenCalled();
});

it("keeps the Catalog error when fallback parquet has no rows", async () => {
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { loadRunningStyleFeatureParquet, putRunningStyleFeatureParquet } =
    await import("./running-style-feature-parquet");
  const catalogError = new Error(
    "PC_KEIBA_R2_CATALOG /v1/running-style-features failed with HTTP 502: r2_sql_unavailable",
  );
  vi.mocked(fetchRunningStyleFeaturesFromCatalog).mockRejectedValue(catalogError);
  vi.mocked(loadRunningStyleFeatureParquet).mockResolvedValue([]);
  await expect(
    loadOrBuildRunningStyleFeatureParquet({
      env: makeEnv("1"),
      featureNames: ["f1"],
      race: RACE,
    }),
  ).rejects.toThrow(
    "PC_KEIBA_R2_CATALOG /v1/running-style-features failed with HTTP 502: r2_sql_unavailable",
  );
  expect(putRunningStyleFeatureParquet).not.toHaveBeenCalled();
});

it("fails closed when Catalog has no rows", async () => {
  const { materializeRunningStyleFeatureParquetForRace } =
    await import("./running-style-feature-materialize");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  vi.mocked(fetchRunningStyleFeaturesFromCatalog).mockResolvedValue([]);
  await expect(
    materializeRunningStyleFeatureParquetForRace({
      env: makeEnv("1"),
      featureNames: ["f1"],
      race: RACE,
    }),
  ).rejects.toThrow("no running-style feature rows found");
});

it("uses the PostgreSQL mirror when Catalog returns an empty race slice", async () => {
  const { materializeRunningStyleFeatureParquetForRace } =
    await import("./running-style-feature-materialize");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { buildRunningStyleFeaturesForRaceFromPostgres } =
    await import("./running-style-feature-sql");
  const { putRunningStyleFeatureParquet, validateFeatureCoverage } =
    await import("./running-style-feature-parquet");
  vi.mocked(fetchRunningStyleFeaturesFromCatalog).mockResolvedValue([]);
  vi.mocked(buildRunningStyleFeaturesForRaceFromPostgres).mockResolvedValue({
    elapsedMs: 12,
    rows: rows(),
    sqlRows: 1,
  });
  vi.mocked(validateFeatureCoverage).mockReturnValue({
    missingCells: 0,
    missingFeatureNames: [],
  });
  vi.mocked(putRunningStyleFeatureParquet).mockResolvedValue(42);
  await expect(
    materializeRunningStyleFeatureParquetForRace({
      env: makeEnv("1"),
      featureNames: ["f1"],
      race: RACE,
    }),
  ).resolves.toMatchObject({ builtRowCount: 1, bytesWritten: 42 });
  expect(buildRunningStyleFeaturesForRaceFromPostgres).toHaveBeenCalled();
});

it("fails closed when Catalog rows do not cover the model", async () => {
  const { materializeRunningStyleFeatureParquetForRace } =
    await import("./running-style-feature-materialize");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { validateFeatureCoverage } = await import("./running-style-feature-parquet");
  vi.mocked(fetchRunningStyleFeaturesFromCatalog).mockResolvedValue(rows());
  vi.mocked(validateFeatureCoverage).mockReturnValue({
    missingCells: 1,
    missingFeatureNames: ["f1"],
  });
  await expect(
    materializeRunningStyleFeatureParquetForRace({
      env: makeEnv("1"),
      featureNames: ["f1"],
      race: RACE,
    }),
  ).rejects.toThrow("catalog feature build missing model features: f1");
});

it("materializes Catalog rows into the current generation", async () => {
  const { materializeRunningStyleFeatureParquetForRace } =
    await import("./running-style-feature-materialize");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { putRunningStyleFeatureParquet, validateFeatureCoverage } =
    await import("./running-style-feature-parquet");
  vi.mocked(fetchRunningStyleFeaturesFromCatalog).mockResolvedValue(rows());
  vi.mocked(validateFeatureCoverage).mockReturnValue({
    missingCells: 0,
    missingFeatureNames: [],
  });
  vi.mocked(putRunningStyleFeatureParquet).mockResolvedValue(42);
  await expect(
    materializeRunningStyleFeatureParquetForRace({
      env: makeEnv("1"),
      featureNames: ["f1"],
      race: RACE,
    }),
  ).resolves.toStrictEqual({
    builtRowCount: 1,
    bytesWritten: 42,
    featuresR2Key: "features.parquet",
  });
});

it("skips date materialization when inference writes are disabled", async () => {
  const { materializeRunningStyleFeatureParquetsForDate } =
    await import("./running-style-feature-materialize");
  await expect(
    materializeRunningStyleFeatureParquetsForDate(makeEnv("0"), "20260513"),
  ).resolves.toStrictEqual({ date: "20260513", materialized: 0, scanned: 0, skipped: 0 });
});

it("materializes Catalog races and records a per-race failure", async () => {
  const { materializeRunningStyleFeatureParquetsForDate } =
    await import("./running-style-feature-materialize");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { putRunningStyleFeatureParquet, validateFeatureCoverage } =
    await import("./running-style-feature-parquet");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0513",
        keibajo_code: "08",
        race_bango: "01",
        source: "jra",
      },
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0513",
        keibajo_code: "08",
        race_bango: "02",
        source: "jra",
      },
    ],
    source: "catalog",
  });
  vi.mocked(fetchRunningStyleFeaturesFromCatalog)
    .mockRejectedValueOnce(new Error("catalog unavailable"))
    .mockResolvedValueOnce(rows());
  vi.mocked(validateFeatureCoverage).mockReturnValue({
    missingCells: 0,
    missingFeatureNames: [],
  });
  vi.mocked(putRunningStyleFeatureParquet).mockResolvedValue(42);
  await expect(
    materializeRunningStyleFeatureParquetsForDate(makeEnv("1"), "20260513"),
  ).resolves.toStrictEqual({
    date: "20260513",
    materializeError: "catalog unavailable",
    materialized: 1,
    scanned: 2,
    skipped: 1,
  });
});

it("date materialization keeps a valid Worker R2 warm cache without rebuilding it", async () => {
  const { materializeRunningStyleFeatureParquetsForDate } =
    await import("./running-style-feature-materialize");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { loadRunningStyleFeatureParquet, putRunningStyleFeatureParquet, validateFeatureCoverage } =
    await import("./running-style-feature-parquet");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0513",
        keibajo_code: "08",
        race_bango: "01",
        source: "jra",
      },
    ],
    source: "catalog",
  });
  vi.mocked(loadRunningStyleFeatureParquet).mockResolvedValue(rows());
  vi.mocked(validateFeatureCoverage).mockReturnValue({
    missingCells: 0,
    missingFeatureNames: [],
  });
  await expect(
    materializeRunningStyleFeatureParquetsForDate(makeEnv("1"), "20260513"),
  ).resolves.toStrictEqual({
    date: "20260513",
    materialized: 0,
    scanned: 1,
    skipped: 1,
  });
  expect(fetchRunningStyleFeaturesFromCatalog).not.toHaveBeenCalled();
  expect(putRunningStyleFeatureParquet).not.toHaveBeenCalled();
});

it("returns an empty summary for a Catalog date without races", async () => {
  const { materializeRunningStyleFeatureParquetsForDate } =
    await import("./running-style-feature-materialize");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({ races: [], source: "catalog" });
  await expect(
    materializeRunningStyleFeatureParquetsForDate(makeEnv("1"), "20260513"),
  ).resolves.toStrictEqual({ date: "20260513", materialized: 0, scanned: 0, skipped: 0 });
});
