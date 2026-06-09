// run with: bun run test
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Env } from "./types";

vi.mock("./finish-position-lite-pool", () => ({
  getFinishPositionPool: vi.fn(),
}));
vi.mock("./daily-feature-build", () => ({
  listDailyRaceEntriesForRace: vi.fn(),
}));
vi.mock("./running-style-feature-sql", () => ({
  buildRunningStyleFeaturesForRaceFromD1Target: vi.fn(),
  buildRunningStyleFeaturesForRaceFromPostgres: vi.fn(),
}));
vi.mock("./running-style-feature-parquet", () => ({
  buildRunningStyleFeatureParquetKey: vi.fn(() => "features.parquet"),
  loadRunningStyleFeatureParquet: vi.fn(),
  putRunningStyleFeatureParquet: vi.fn(),
  validateFeatureCoverage: vi.fn(),
}));
vi.mock("./running-style-features", () => ({
  buildRunningStyleRaceKey: vi.fn(() => "jra:20260513:08:01"),
  normalizeKeibajoCode: vi.fn((value: string) => value),
  normalizeRaceBango: vi.fn((value: string) => value),
}));
vi.mock("./running-style-model-binary", () => ({
  buildRunningStyleFlatModelKey: vi.fn(() => "model.flatbin"),
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
  ({
    REALTIME_DB: {},
    RUNNING_STYLE_D1_WRITE_ENABLED: writeEnabled,
    RUNNING_STYLE_MODELS: {},
  }) as unknown as Env;

const makePool = (): never => ({}) as never;

beforeEach(() => {
  vi.clearAllMocks();
});

describe("loadOrBuildRunningStyleFeatureParquet", () => {
  it("returns the R2 object without rebuilding on a hit", async () => {
    const { loadOrBuildRunningStyleFeatureParquet } =
      await import("./running-style-feature-materialize");
    const {
      loadRunningStyleFeatureParquet,
      putRunningStyleFeatureParquet,
      validateFeatureCoverage,
    } = await import("./running-style-feature-parquet");
    vi.mocked(loadRunningStyleFeatureParquet).mockResolvedValue([{}, {}] as never);
    vi.mocked(validateFeatureCoverage).mockReturnValue({
      missingCells: 0,
      missingFeatureNames: [],
    });
    const result = await loadOrBuildRunningStyleFeatureParquet({
      env: makeEnv("1"),
      featureNames: ["f1"],
      pool: makePool(),
      race: {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0513",
        keibajoCode: "08",
        raceBango: "01",
        source: "jra",
      },
    });
    expect(result.rebuilt).toBe(false);
    expect(result.featuresR2Key).toBe("features.parquet");
    expect(result.rows.length).toBe(2);
    expect(putRunningStyleFeatureParquet).not.toHaveBeenCalled();
  });

  it("rebuilds from PostgreSQL when the R2 object is missing", async () => {
    const { loadOrBuildRunningStyleFeatureParquet } =
      await import("./running-style-feature-materialize");
    const { listDailyRaceEntriesForRace } = await import("./daily-feature-build");
    const { buildRunningStyleFeaturesForRaceFromD1Target } =
      await import("./running-style-feature-sql");
    const {
      loadRunningStyleFeatureParquet,
      putRunningStyleFeatureParquet,
      validateFeatureCoverage,
    } = await import("./running-style-feature-parquet");
    vi.mocked(loadRunningStyleFeatureParquet).mockRejectedValueOnce(
      new Error("R2 object not found: features.parquet"),
    );
    vi.mocked(listDailyRaceEntriesForRace).mockResolvedValue([{}] as never);
    vi.mocked(buildRunningStyleFeaturesForRaceFromD1Target).mockResolvedValue({
      elapsedMs: 1,
      rows: [{}] as never,
      sqlRows: 1,
    });
    vi.mocked(validateFeatureCoverage).mockReturnValue({
      missingCells: 0,
      missingFeatureNames: [],
    });
    vi.mocked(putRunningStyleFeatureParquet).mockResolvedValue(10);
    const result = await loadOrBuildRunningStyleFeatureParquet({
      env: makeEnv("1"),
      featureNames: ["f1"],
      pool: makePool(),
      race: {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0513",
        keibajoCode: "08",
        raceBango: "01",
        source: "jra",
      },
    });
    expect(result.rebuilt).toBe(true);
    expect(putRunningStyleFeatureParquet).toHaveBeenCalledTimes(1);
    expect(loadRunningStyleFeatureParquet).toHaveBeenCalledTimes(1);
    expect(result.rows.length).toBe(1);
  });

  it("rethrows a non-not-found R2 error", async () => {
    const { loadOrBuildRunningStyleFeatureParquet } =
      await import("./running-style-feature-materialize");
    const { loadRunningStyleFeatureParquet, putRunningStyleFeatureParquet } =
      await import("./running-style-feature-parquet");
    vi.mocked(loadRunningStyleFeatureParquet).mockRejectedValue(new Error("boom"));
    await expect(
      loadOrBuildRunningStyleFeatureParquet({
        env: makeEnv("1"),
        featureNames: ["f1"],
        pool: makePool(),
        race: {
          kaisaiNen: "2026",
          kaisaiTsukihi: "0513",
          keibajoCode: "08",
          raceBango: "01",
          source: "jra",
        },
      }),
    ).rejects.toThrow("boom");
    expect(putRunningStyleFeatureParquet).not.toHaveBeenCalled();
  });

  it("rebuilds when the R2 object is empty", async () => {
    const { loadOrBuildRunningStyleFeatureParquet } =
      await import("./running-style-feature-materialize");
    const { listDailyRaceEntriesForRace } = await import("./daily-feature-build");
    const { buildRunningStyleFeaturesForRaceFromD1Target } =
      await import("./running-style-feature-sql");
    const {
      loadRunningStyleFeatureParquet,
      putRunningStyleFeatureParquet,
      validateFeatureCoverage,
    } = await import("./running-style-feature-parquet");
    vi.mocked(loadRunningStyleFeatureParquet).mockResolvedValueOnce([] as never);
    vi.mocked(listDailyRaceEntriesForRace).mockResolvedValue([{}] as never);
    vi.mocked(buildRunningStyleFeaturesForRaceFromD1Target).mockResolvedValue({
      elapsedMs: 1,
      rows: [{}] as never,
      sqlRows: 1,
    });
    vi.mocked(validateFeatureCoverage).mockReturnValue({
      missingCells: 0,
      missingFeatureNames: [],
    });
    vi.mocked(putRunningStyleFeatureParquet).mockResolvedValue(10);
    const result = await loadOrBuildRunningStyleFeatureParquet({
      env: makeEnv("1"),
      featureNames: ["f1"],
      pool: makePool(),
      race: {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0513",
        keibajoCode: "08",
        raceBango: "01",
        source: "jra",
      },
    });
    expect(result.rebuilt).toBe(true);
    expect(loadRunningStyleFeatureParquet).toHaveBeenCalledTimes(1);
  });

  it("rebuilds when the R2 object has a coverage gap", async () => {
    const { loadOrBuildRunningStyleFeatureParquet } =
      await import("./running-style-feature-materialize");
    const { listDailyRaceEntriesForRace } = await import("./daily-feature-build");
    const { buildRunningStyleFeaturesForRaceFromD1Target } =
      await import("./running-style-feature-sql");
    const {
      loadRunningStyleFeatureParquet,
      putRunningStyleFeatureParquet,
      validateFeatureCoverage,
    } = await import("./running-style-feature-parquet");
    vi.mocked(loadRunningStyleFeatureParquet).mockResolvedValueOnce([{}] as never);
    vi.mocked(validateFeatureCoverage)
      .mockReturnValueOnce({ missingCells: 1, missingFeatureNames: ["new_feat"] })
      .mockReturnValueOnce({ missingCells: 0, missingFeatureNames: [] });
    vi.mocked(listDailyRaceEntriesForRace).mockResolvedValue([{}] as never);
    vi.mocked(buildRunningStyleFeaturesForRaceFromD1Target).mockResolvedValue({
      elapsedMs: 1,
      rows: [{}] as never,
      sqlRows: 1,
    });
    vi.mocked(putRunningStyleFeatureParquet).mockResolvedValue(10);
    const result = await loadOrBuildRunningStyleFeatureParquet({
      env: makeEnv("1"),
      featureNames: ["f1"],
      pool: makePool(),
      race: {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0513",
        keibajoCode: "08",
        raceBango: "01",
        source: "jra",
      },
    });
    expect(result.rebuilt).toBe(true);
    expect(loadRunningStyleFeatureParquet).toHaveBeenCalledTimes(1);
  });

  it("on rebuild returns the in-memory rows from the build step without re-fetching from R2", async () => {
    const { loadOrBuildRunningStyleFeatureParquet } =
      await import("./running-style-feature-materialize");
    const { listDailyRaceEntriesForRace } = await import("./daily-feature-build");
    const { buildRunningStyleFeaturesForRaceFromD1Target } =
      await import("./running-style-feature-sql");
    const {
      loadRunningStyleFeatureParquet,
      putRunningStyleFeatureParquet,
      validateFeatureCoverage,
    } = await import("./running-style-feature-parquet");
    const builtRows = [{ raceKey: "jra:20260513:08:01", umaban: 1 }];
    vi.mocked(loadRunningStyleFeatureParquet).mockRejectedValueOnce(
      new Error("R2 object not found: features.parquet"),
    );
    vi.mocked(listDailyRaceEntriesForRace).mockResolvedValue([{}] as never);
    vi.mocked(buildRunningStyleFeaturesForRaceFromD1Target).mockResolvedValue({
      elapsedMs: 1,
      rows: builtRows as never,
      sqlRows: 1,
    });
    vi.mocked(validateFeatureCoverage).mockReturnValue({
      missingCells: 0,
      missingFeatureNames: [],
    });
    vi.mocked(putRunningStyleFeatureParquet).mockResolvedValue(10);
    const result = await loadOrBuildRunningStyleFeatureParquet({
      env: makeEnv("1"),
      featureNames: ["f1"],
      pool: makePool(),
      race: {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0513",
        keibajoCode: "08",
        raceBango: "01",
        source: "jra",
      },
    });
    expect(result.rows).toBe(builtRows);
    expect(loadRunningStyleFeatureParquet).toHaveBeenCalledTimes(1);
  });
});

describe("materializeRunningStyleFeatureParquetForRace", () => {
  it("builds and writes the full row set", async () => {
    const { materializeRunningStyleFeatureParquetForRace } =
      await import("./running-style-feature-materialize");
    const { listDailyRaceEntriesForRace } = await import("./daily-feature-build");
    const { buildRunningStyleFeaturesForRaceFromD1Target } =
      await import("./running-style-feature-sql");
    const { putRunningStyleFeatureParquet, validateFeatureCoverage } =
      await import("./running-style-feature-parquet");
    vi.mocked(listDailyRaceEntriesForRace).mockResolvedValue([{}] as never);
    vi.mocked(buildRunningStyleFeaturesForRaceFromD1Target).mockResolvedValue({
      elapsedMs: 1,
      rows: [{}] as never,
      sqlRows: 1,
    });
    vi.mocked(validateFeatureCoverage).mockReturnValue({
      missingCells: 0,
      missingFeatureNames: [],
    });
    vi.mocked(putRunningStyleFeatureParquet).mockResolvedValue(42);
    const result = await materializeRunningStyleFeatureParquetForRace({
      env: makeEnv("1"),
      featureNames: ["f1"],
      pool: makePool(),
      race: {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0513",
        keibajoCode: "08",
        raceBango: "01",
        source: "jra",
      },
    });
    expect(result).toStrictEqual({
      builtRowCount: 1,
      bytesWritten: 42,
      featuresR2Key: "features.parquet",
    });
  });

  it("falls back to Hyperdrive-direct Postgres build when D1 daily_race_entries are empty", async () => {
    const { materializeRunningStyleFeatureParquetForRace } =
      await import("./running-style-feature-materialize");
    const { listDailyRaceEntriesForRace } = await import("./daily-feature-build");
    const {
      buildRunningStyleFeaturesForRaceFromD1Target,
      buildRunningStyleFeaturesForRaceFromPostgres,
    } = await import("./running-style-feature-sql");
    const { putRunningStyleFeatureParquet, validateFeatureCoverage } =
      await import("./running-style-feature-parquet");
    vi.mocked(listDailyRaceEntriesForRace).mockResolvedValue([] as never);
    vi.mocked(buildRunningStyleFeaturesForRaceFromPostgres).mockResolvedValue({
      elapsedMs: 1,
      rows: [{}] as never,
      sqlRows: 1,
    });
    vi.mocked(validateFeatureCoverage).mockReturnValue({
      missingCells: 0,
      missingFeatureNames: [],
    });
    vi.mocked(putRunningStyleFeatureParquet).mockResolvedValue(42);
    const result = await materializeRunningStyleFeatureParquetForRace({
      env: makeEnv("1"),
      featureNames: ["f1"],
      pool: makePool(),
      race: {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0513",
        keibajoCode: "08",
        raceBango: "01",
        source: "jra",
      },
    });
    expect(result).toStrictEqual({
      builtRowCount: 1,
      bytesWritten: 42,
      featuresR2Key: "features.parquet",
    });
    expect(buildRunningStyleFeaturesForRaceFromD1Target).not.toHaveBeenCalled();
    expect(buildRunningStyleFeaturesForRaceFromPostgres).toHaveBeenCalledTimes(1);
  });

  it("throws when both D1 and Hyperdrive-direct builds return zero rows", async () => {
    const { materializeRunningStyleFeatureParquetForRace } =
      await import("./running-style-feature-materialize");
    const { listDailyRaceEntriesForRace } = await import("./daily-feature-build");
    const { buildRunningStyleFeaturesForRaceFromPostgres } =
      await import("./running-style-feature-sql");
    vi.mocked(listDailyRaceEntriesForRace).mockResolvedValue([] as never);
    vi.mocked(buildRunningStyleFeaturesForRaceFromPostgres).mockResolvedValue({
      elapsedMs: 1,
      rows: [] as never,
      sqlRows: 0,
    });
    await expect(
      materializeRunningStyleFeatureParquetForRace({
        env: makeEnv("1"),
        featureNames: ["f1"],
        pool: makePool(),
        race: {
          kaisaiNen: "2026",
          kaisaiTsukihi: "0513",
          keibajoCode: "08",
          raceBango: "01",
          source: "jra",
        },
      }),
    ).rejects.toThrow("no running-style feature rows found");
  });

  it("throws when the PostgreSQL build returns no rows", async () => {
    const { materializeRunningStyleFeatureParquetForRace } =
      await import("./running-style-feature-materialize");
    const { listDailyRaceEntriesForRace } = await import("./daily-feature-build");
    const { buildRunningStyleFeaturesForRaceFromD1Target } =
      await import("./running-style-feature-sql");
    vi.mocked(listDailyRaceEntriesForRace).mockResolvedValue([{}] as never);
    vi.mocked(buildRunningStyleFeaturesForRaceFromD1Target).mockResolvedValue({
      elapsedMs: 1,
      rows: [] as never,
      sqlRows: 0,
    });
    await expect(
      materializeRunningStyleFeatureParquetForRace({
        env: makeEnv("1"),
        featureNames: ["f1"],
        pool: makePool(),
        race: {
          kaisaiNen: "2026",
          kaisaiTsukihi: "0513",
          keibajoCode: "08",
          raceBango: "01",
          source: "jra",
        },
      }),
    ).rejects.toThrow("no running-style feature rows found");
  });

  it("throws when the PostgreSQL build is missing model features", async () => {
    const { materializeRunningStyleFeatureParquetForRace } =
      await import("./running-style-feature-materialize");
    const { listDailyRaceEntriesForRace } = await import("./daily-feature-build");
    const { buildRunningStyleFeaturesForRaceFromD1Target } =
      await import("./running-style-feature-sql");
    const { validateFeatureCoverage } = await import("./running-style-feature-parquet");
    vi.mocked(listDailyRaceEntriesForRace).mockResolvedValue([{}] as never);
    vi.mocked(buildRunningStyleFeaturesForRaceFromD1Target).mockResolvedValue({
      elapsedMs: 1,
      rows: [{}] as never,
      sqlRows: 1,
    });
    vi.mocked(validateFeatureCoverage).mockReturnValue({
      missingCells: 1,
      missingFeatureNames: ["x"],
    });
    await expect(
      materializeRunningStyleFeatureParquetForRace({
        env: makeEnv("1"),
        featureNames: ["f1"],
        pool: makePool(),
        race: {
          kaisaiNen: "2026",
          kaisaiTsukihi: "0513",
          keibajoCode: "08",
          raceBango: "01",
          source: "jra",
        },
      }),
    ).rejects.toThrow("PostgreSQL feature build missing model features: x");
  });
});

describe("materializeRunningStyleFeatureParquetsForDate", () => {
  it("returns a zeroed summary when D1 writes are disabled", async () => {
    const { materializeRunningStyleFeatureParquetsForDate } =
      await import("./running-style-feature-materialize");
    const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
    const result = await materializeRunningStyleFeatureParquetsForDate(makeEnv("0"), "20260513");
    expect(result).toStrictEqual({ date: "20260513", materialized: 0, scanned: 0, skipped: 0 });
    expect(listRunningStyleRacesByDate).not.toHaveBeenCalled();
  });

  it("materializes a single race successfully", async () => {
    const { materializeRunningStyleFeatureParquetsForDate } =
      await import("./running-style-feature-materialize");
    const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
    const { getFinishPositionPool } = await import("./finish-position-lite-pool");
    const { listDailyRaceEntriesForRace } = await import("./daily-feature-build");
    const { buildRunningStyleFeaturesForRaceFromD1Target } =
      await import("./running-style-feature-sql");
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
      ],
      source: "d1",
    });
    vi.mocked(getFinishPositionPool).mockReturnValue({} as never);
    vi.mocked(listDailyRaceEntriesForRace).mockResolvedValue([{}] as never);
    vi.mocked(buildRunningStyleFeaturesForRaceFromD1Target).mockResolvedValue({
      elapsedMs: 1,
      rows: [{}] as never,
      sqlRows: 1,
    });
    vi.mocked(validateFeatureCoverage).mockReturnValue({
      missingCells: 0,
      missingFeatureNames: [],
    });
    vi.mocked(putRunningStyleFeatureParquet).mockResolvedValue(10);
    const result = await materializeRunningStyleFeatureParquetsForDate(makeEnv("1"), "20260513");
    expect(result.materialized).toBe(1);
    expect(result.scanned).toBe(1);
    expect(result.skipped).toBe(0);
  });

  it("continues past a per-race error and records it", async () => {
    const { materializeRunningStyleFeatureParquetsForDate } =
      await import("./running-style-feature-materialize");
    const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
    const { getFinishPositionPool } = await import("./finish-position-lite-pool");
    const { listDailyRaceEntriesForRace } = await import("./daily-feature-build");
    const { buildRunningStyleFeaturesForRaceFromD1Target } =
      await import("./running-style-feature-sql");
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
      source: "d1",
    });
    vi.mocked(getFinishPositionPool).mockReturnValue({} as never);
    vi.mocked(listDailyRaceEntriesForRace).mockResolvedValue([{}] as never);
    vi.mocked(buildRunningStyleFeaturesForRaceFromD1Target)
      .mockRejectedValueOnce(new Error("pg failed"))
      .mockResolvedValueOnce({ elapsedMs: 1, rows: [{}] as never, sqlRows: 1 });
    vi.mocked(validateFeatureCoverage).mockReturnValue({
      missingCells: 0,
      missingFeatureNames: [],
    });
    vi.mocked(putRunningStyleFeatureParquet).mockResolvedValue(10);
    const result = await materializeRunningStyleFeatureParquetsForDate(makeEnv("1"), "20260513");
    expect(result.scanned).toBe(2);
    expect(result.materialized).toBe(1);
    expect(result.skipped).toBe(1);
    expect(typeof result.materializeError).toBe("string");
  });

  it("returns a zeroed summary when the date has no races", async () => {
    const { materializeRunningStyleFeatureParquetsForDate } =
      await import("./running-style-feature-materialize");
    const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
    const { getFinishPositionPool } = await import("./finish-position-lite-pool");
    vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({ races: [], source: "d1" });
    vi.mocked(getFinishPositionPool).mockReturnValue({} as never);
    const result = await materializeRunningStyleFeatureParquetsForDate(makeEnv("1"), "20260513");
    expect(result).toStrictEqual({ date: "20260513", materialized: 0, scanned: 0, skipped: 0 });
  });
});
