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

beforeEach(() => {
  vi.clearAllMocks();
});

it("ignores a stale processed object and refreshes it from Catalog", async () => {
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { loadRunningStyleFeatureParquet, putRunningStyleFeatureParquet, validateFeatureCoverage } =
    await import("./running-style-feature-parquet");
  const env = makeEnv("1");
  const staleRows: RaceHorseFeatureRow[] = JSON.parse(
    '[{"raceKey":"jra:20260513:08:01","umaban":9}]',
  );
  vi.mocked(loadRunningStyleFeatureParquet).mockResolvedValue(staleRows);
  vi.mocked(fetchRunningStyleFeaturesFromCatalog).mockResolvedValue(rows());
  vi.mocked(validateFeatureCoverage).mockReturnValue({
    missingCells: 0,
    missingFeatureNames: [],
  });
  vi.mocked(putRunningStyleFeatureParquet).mockResolvedValue(42);
  const result = await loadOrBuildRunningStyleFeatureParquet({
    env,
    featureNames: ["f1"],
    race: RACE,
  });
  expect(result).toStrictEqual({
    featuresR2Key: "features.parquet",
    rebuilt: true,
    rows: rows(),
  });
  expect(loadRunningStyleFeatureParquet).not.toHaveBeenCalled();
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

it("does not fall back to a stale processed object when Catalog fails", async () => {
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { loadRunningStyleFeatureParquet, putRunningStyleFeatureParquet } =
    await import("./running-style-feature-parquet");
  vi.mocked(loadRunningStyleFeatureParquet).mockResolvedValue(rows());
  vi.mocked(fetchRunningStyleFeaturesFromCatalog).mockRejectedValue(
    new Error("Catalog unavailable"),
  );
  await expect(
    loadOrBuildRunningStyleFeatureParquet({
      env: makeEnv("1"),
      featureNames: ["f1"],
      race: RACE,
    }),
  ).rejects.toThrow("Catalog unavailable");
  expect(loadRunningStyleFeatureParquet).not.toHaveBeenCalled();
  expect(putRunningStyleFeatureParquet).not.toHaveBeenCalled();
});

it("falls back to R2 parquet when Catalog is unavailable and coverage is complete", async () => {
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { loadRunningStyleFeatureParquet, putRunningStyleFeatureParquet, validateFeatureCoverage } =
    await import("./running-style-feature-parquet");
  const catalogError = new Error(
    "PC_KEIBA_R2_CATALOG /v1/running-style-features failed with HTTP 502: r2_sql_unavailable",
  );
  vi.spyOn(console, "error").mockImplementation(() => {});
  vi.mocked(fetchRunningStyleFeaturesFromCatalog).mockRejectedValue(catalogError);
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
  expect(vi.mocked(console.error).mock.calls[0]?.[0]).toBe(
    "Running-style features catalog unavailable, using R2 parquet fallback features.parquet: PC_KEIBA_R2_CATALOG /v1/running-style-features failed with HTTP 502: r2_sql_unavailable",
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

it("returns an empty summary for a Catalog date without races", async () => {
  const { materializeRunningStyleFeatureParquetsForDate } =
    await import("./running-style-feature-materialize");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({ races: [], source: "catalog" });
  await expect(
    materializeRunningStyleFeatureParquetsForDate(makeEnv("1"), "20260513"),
  ).resolves.toStrictEqual({ date: "20260513", materialized: 0, scanned: 0, skipped: 0 });
});
