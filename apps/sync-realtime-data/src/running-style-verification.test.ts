// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { Env } from "./types";

vi.mock("./running-style-feature-parquet", () => ({
  loadRunningStyleFeatureParquet: vi.fn(),
  putRunningStyleFeatureParquet: vi.fn(),
  runningStyleParquetVerificationKey: vi.fn(() => "verification/key"),
  validateFeatureCoverage: vi.fn(),
}));
vi.mock("./running-style-feature-sql", () => ({
  buildRunningStyleFeaturesForRaceFromPostgres: vi.fn(),
}));
vi.mock("./running-style-inference", () => ({
  runRunningStyleInferenceRowsWithFlatModel: vi.fn(),
}));
vi.mock("./finish-position-lite-pool", () => ({
  getFinishPositionPool: vi.fn(() => ({})),
}));
vi.mock("./running-style-model-binary", () => ({
  buildRunningStyleFlatModelKey: vi.fn(() => "models/v7-lineage.bin"),
  loadFlatLightGBMModelFromR2: vi.fn(),
}));

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("parseRunningStylePostgresVerificationParams returns null when path does not match", async () => {
  const { parseRunningStylePostgresVerificationParams } =
    await import("./running-style-verification");
  expect(parseRunningStylePostgresVerificationParams(new URL("https://x.test/other"))).toBeNull();
});

it("parseRunningStylePostgresVerificationParams parses jra path", async () => {
  const { parseRunningStylePostgresVerificationParams } =
    await import("./running-style-verification");
  const url = new URL("https://x.test/admin/running-style/verify-postgres/jra/2026/05/12/08/01");
  expect(parseRunningStylePostgresVerificationParams(url)).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "08",
    raceBango: "01",
    source: "jra",
  });
});

it("parseRunningStylePostgresVerificationParams parses nar path with alphanumeric keibajo", async () => {
  const { parseRunningStylePostgresVerificationParams } =
    await import("./running-style-verification");
  const url = new URL("https://x.test/admin/running-style/verify-postgres/nar/2026/05/12/B0/12");
  expect(parseRunningStylePostgresVerificationParams(url)).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "B0",
    raceBango: "12",
    source: "nar",
  });
});

it("runRunningStyleWorkerPostgresVerification returns a summary on success", async () => {
  const { runRunningStyleWorkerPostgresVerification } =
    await import("./running-style-verification");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  const { buildRunningStyleFeaturesForRaceFromPostgres } =
    await import("./running-style-feature-sql");
  const { loadRunningStyleFeatureParquet, putRunningStyleFeatureParquet, validateFeatureCoverage } =
    await import("./running-style-feature-parquet");
  const { runRunningStyleInferenceRowsWithFlatModel } = await import("./running-style-inference");
  vi.mocked(loadFlatLightGBMModelFromR2).mockResolvedValue({
    header: { feature_names: ["a", "b"] },
  } as never);
  vi.mocked(buildRunningStyleFeaturesForRaceFromPostgres).mockResolvedValue({
    elapsedMs: 123,
    rows: [{}, {}],
  } as never);
  vi.mocked(validateFeatureCoverage).mockReturnValue({ missingCells: 0, missingFeatureNames: [] });
  vi.mocked(putRunningStyleFeatureParquet).mockResolvedValue(8192);
  vi.mocked(loadRunningStyleFeatureParquet).mockResolvedValue([{}, {}] as never);
  vi.mocked(runRunningStyleInferenceRowsWithFlatModel).mockResolvedValue({
    modelVersion: "v7-lineage",
    writtenCount: 2,
  } as never);

  const env = {
    REALTIME_DB: {},
    RUNNING_STYLE_MODELS: {},
  } as unknown as Env;
  const summary = await runRunningStyleWorkerPostgresVerification(
    env,
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      raceBango: "01",
      source: "jra",
    },
    "2026-05-12T11:30:00.000Z",
  );
  expect(summary.featureBuildMs).toBe(123);
  expect(summary.featureCount).toBe(2);
  expect(summary.inputFeaturesKey).toBe("postgres");
  expect(summary.modelKey).toBe("models/v7-lineage.bin");
  expect(summary.modelVersion).toBe("v7-lineage");
  expect(summary.parquetBytes).toBe(8192);
  expect(summary.parquetKey).toBe("verification/key");
  expect(summary.readBackRows).toBe(2);
  expect(summary.writtenCount).toBe(2);
});

it("runRunningStyleWorkerPostgresVerification throws when missing model features detected", async () => {
  const { runRunningStyleWorkerPostgresVerification } =
    await import("./running-style-verification");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  const { buildRunningStyleFeaturesForRaceFromPostgres } =
    await import("./running-style-feature-sql");
  const { validateFeatureCoverage } = await import("./running-style-feature-parquet");
  vi.mocked(loadFlatLightGBMModelFromR2).mockResolvedValue({
    header: { feature_names: ["a"] },
  } as never);
  vi.mocked(buildRunningStyleFeaturesForRaceFromPostgres).mockResolvedValue({
    elapsedMs: 1,
    rows: [],
  } as never);
  vi.mocked(validateFeatureCoverage).mockReturnValue({
    missingCells: 1,
    missingFeatureNames: ["a"],
  });

  const env = {
    REALTIME_DB: {},
    RUNNING_STYLE_MODELS: {},
  } as unknown as Env;
  await expect(
    runRunningStyleWorkerPostgresVerification(env, {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      raceBango: "01",
      source: "jra",
    }),
  ).rejects.toThrow("PostgreSQL feature build missing model features: a");
});
