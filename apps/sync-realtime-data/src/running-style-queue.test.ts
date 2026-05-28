// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { Env, RunningStylePredictionJob } from "./types";

vi.mock("./finish-position-d1", () => ({
  markFinishPositionFeaturesCached: vi.fn(async () => {}),
}));
vi.mock("./finish-position-inputs-cache", () => ({
  putFinishPositionInputsCache: vi.fn(async () => true),
}));
vi.mock("./finish-position-lite-pool", () => ({
  getFinishPositionPool: vi.fn(() => ({})),
}));
vi.mock("./running-style-expected-horses", () => ({
  filterRunningStyleFeatureRowsByActiveEntries: vi.fn((rows: unknown[]) => rows),
  resolveRunningStyleExpectedHorseCount: vi.fn((featureCount: number) => featureCount),
}));
vi.mock("./viewer-running-style-cache", () => ({
  putViewerRunningStyleRaceCache: vi.fn(async () => true),
}));
vi.mock("./running-style-d1", () => ({
  getRunningStyleInferenceState: vi.fn(),
  listRaceRunningStylesForRace: vi.fn(),
  markRunningStyleInferenceCompleted: vi.fn(async () => {}),
  markRunningStyleInferenceFailed: vi.fn(async () => {}),
  markRunningStyleInferenceProcessing: vi.fn(async () => {}),
}));
vi.mock("./running-style-feature-parquet", () => ({
  buildRunningStyleFeatureParquetKey: vi.fn(() => "features.parquet"),
  loadRunningStyleFeatureParquet: vi.fn(),
  putRunningStyleFeatureParquet: vi.fn(async () => 100),
  validateFeatureCoverage: vi.fn(),
}));
vi.mock("./daily-feature-build", () => ({
  listDailyRaceEntriesForRace: vi.fn(),
}));
vi.mock("./running-style-feature-sql", () => ({
  buildRunningStyleFeaturesForRaceFromD1Target: vi.fn(),
}));
vi.mock("./running-style-features", () => ({
  buildRealtimeRaceKeyFromRunningStyle: vi.fn(() => "jra:20260512:08:01"),
  buildRunningStyleRaceKey: vi.fn(() => "jra:20260512:08:01"),
}));
vi.mock("./running-style-inference", () => ({
  runRunningStyleInferenceRowsWithFlatModel: vi.fn(),
}));
vi.mock("./running-style-model-binary", () => ({
  buildRunningStyleFlatModelKey: vi.fn(() => "models/jra/latest.flatbin"),
  loadFlatLightGBMModelFromR2: vi.fn(),
}));
vi.mock("./storage", () => ({
  getLatestRaceEntries: vi.fn(),
}));

const JOB: RunningStylePredictionJob = {
  kaisaiNen: "2026",
  kaisaiTsukihi: "0512",
  keibajoCode: "08",
  predictedAt: "2026-05-12T11:00:00+09:00",
  raceBango: "01",
  raceKey: "jra:20260512:08:01",
  source: "jra",
  type: "generate-running-style-predictions",
};

const buildEnv = (overrides?: Partial<Env>): Env => {
  return {
    REALTIME_DB: {},
    RUNNING_STYLE_D1_WRITE_ENABLED: "1",
    RUNNING_STYLE_MODELS: {},
    ...overrides,
  } as unknown as Env;
};

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("returns null when RUNNING_STYLE_D1_WRITE_ENABLED is not '1'", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const env = buildEnv({ RUNNING_STYLE_D1_WRITE_ENABLED: "0" });
  expect(await handleRunningStylePredictionJob(env, JOB)).toBeNull();
});

it("returns a skipped summary when state already completed and counts meet expectations", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue({
    expectedHorseCount: 5,
    featuresR2Key: "features.parquet",
    modelVersion: "v7-lineage",
    status: "completed",
    writtenHorseCount: 5,
  } as never);
  vi.mocked(listRaceRunningStylesForRace).mockResolvedValue([
    { raceKey: "jra:20260512:08:01" },
  ] as never);

  const summary = await handleRunningStylePredictionJob(buildEnv(), JOB);
  expect(summary?.skipped).toBe(true);
  expect(summary?.writtenCount).toBe(5);
  expect(summary?.modelVersion).toBe("v7-lineage");
});

it("throws when no daily race entries are found", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState } = await import("./running-style-d1");
  const { listDailyRaceEntriesForRace } = await import("./daily-feature-build");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue(null);
  vi.mocked(loadFlatLightGBMModelFromR2).mockResolvedValue({
    header: { feature_names: ["x"], model_version: "v7-lineage" },
  } as never);
  vi.mocked(listDailyRaceEntriesForRace).mockResolvedValue([]);

  await expect(handleRunningStylePredictionJob(buildEnv(), JOB)).rejects.toThrow(
    "no D1 daily_race_entries rows",
  );
});

it("throws when no running-style feature rows are returned", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState } = await import("./running-style-d1");
  const { listDailyRaceEntriesForRace } = await import("./daily-feature-build");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  const { buildRunningStyleFeaturesForRaceFromD1Target } =
    await import("./running-style-feature-sql");
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue(null);
  vi.mocked(loadFlatLightGBMModelFromR2).mockResolvedValue({
    header: { feature_names: ["x"], model_version: "v7-lineage" },
  } as never);
  vi.mocked(listDailyRaceEntriesForRace).mockResolvedValue([{}] as never);
  vi.mocked(buildRunningStyleFeaturesForRaceFromD1Target).mockResolvedValue({
    rows: [],
  } as never);

  await expect(handleRunningStylePredictionJob(buildEnv(), JOB)).rejects.toThrow(
    "no running-style feature rows found",
  );
});

it("throws when validateFeatureCoverage returns missingFeatureNames", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState } = await import("./running-style-d1");
  const { listDailyRaceEntriesForRace } = await import("./daily-feature-build");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  const { buildRunningStyleFeaturesForRaceFromD1Target } =
    await import("./running-style-feature-sql");
  const { validateFeatureCoverage } = await import("./running-style-feature-parquet");
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue(null);
  vi.mocked(loadFlatLightGBMModelFromR2).mockResolvedValue({
    header: { feature_names: ["x"], model_version: "v7-lineage" },
  } as never);
  vi.mocked(listDailyRaceEntriesForRace).mockResolvedValue([{}] as never);
  vi.mocked(buildRunningStyleFeaturesForRaceFromD1Target).mockResolvedValue({
    rows: [{}],
  } as never);
  vi.mocked(validateFeatureCoverage).mockReturnValue({
    missingCells: 1,
    missingFeatureNames: ["x"],
  });

  await expect(handleRunningStylePredictionJob(buildEnv(), JOB)).rejects.toThrow(
    "PostgreSQL feature build missing model features: x",
  );
});

it("completes the job and returns the success summary", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
  const { listDailyRaceEntriesForRace } = await import("./daily-feature-build");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  const { buildRunningStyleFeaturesForRaceFromD1Target } =
    await import("./running-style-feature-sql");
  const { loadRunningStyleFeatureParquet, validateFeatureCoverage } =
    await import("./running-style-feature-parquet");
  const { runRunningStyleInferenceRowsWithFlatModel } = await import("./running-style-inference");
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue(null);
  vi.mocked(loadFlatLightGBMModelFromR2).mockResolvedValue({
    header: { feature_names: ["x"], model_version: "v7-lineage" },
  } as never);
  vi.mocked(listDailyRaceEntriesForRace).mockResolvedValue([{}] as never);
  vi.mocked(buildRunningStyleFeaturesForRaceFromD1Target).mockResolvedValue({
    rows: [{ raceKey: "k" }, { raceKey: "k" }],
  } as never);
  vi.mocked(validateFeatureCoverage).mockReturnValue({
    missingCells: 0,
    missingFeatureNames: [],
  });
  vi.mocked(loadRunningStyleFeatureParquet).mockResolvedValue([{}, {}] as never);
  vi.mocked(runRunningStyleInferenceRowsWithFlatModel).mockResolvedValue({
    modelVersion: "v7-lineage",
    writtenCount: 2,
  } as never);
  vi.mocked(listRaceRunningStylesForRace).mockResolvedValue([{}, {}] as never);

  const summary = await handleRunningStylePredictionJob(buildEnv(), JOB);
  expect(summary?.modelVersion).toBe("v7-lineage");
  expect(summary?.writtenCount).toBe(2);
  expect(summary?.raceKey).toBe("jra:20260512:08:01");
  expect(summary?.cacheWritten).toBe(true);
});

it("throws when filterRunningStyleFeatureRowsByActiveEntries leaves no rows", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState } = await import("./running-style-d1");
  const { listDailyRaceEntriesForRace } = await import("./daily-feature-build");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  const { buildRunningStyleFeaturesForRaceFromD1Target } =
    await import("./running-style-feature-sql");
  const { filterRunningStyleFeatureRowsByActiveEntries } =
    await import("./running-style-expected-horses");
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue(null);
  vi.mocked(loadFlatLightGBMModelFromR2).mockResolvedValue({
    header: { feature_names: ["x"], model_version: "v7-lineage" },
  } as never);
  vi.mocked(listDailyRaceEntriesForRace).mockResolvedValue([{}] as never);
  vi.mocked(buildRunningStyleFeaturesForRaceFromD1Target).mockResolvedValue({
    rows: [{ umaban: 1 }, { umaban: 2 }],
  } as never);
  vi.mocked(filterRunningStyleFeatureRowsByActiveEntries).mockReturnValue([]);

  await expect(handleRunningStylePredictionJob(buildEnv(), JOB)).rejects.toThrow(
    "no active running-style feature rows found",
  );
});

it("skips cacheCompletedRunningStyles when written count is less than expected horse count", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
  const { listDailyRaceEntriesForRace } = await import("./daily-feature-build");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  const { buildRunningStyleFeaturesForRaceFromD1Target } =
    await import("./running-style-feature-sql");
  const { loadRunningStyleFeatureParquet, validateFeatureCoverage } =
    await import("./running-style-feature-parquet");
  const { runRunningStyleInferenceRowsWithFlatModel } = await import("./running-style-inference");
  const { filterRunningStyleFeatureRowsByActiveEntries, resolveRunningStyleExpectedHorseCount } =
    await import("./running-style-expected-horses");
  const passThrough = vi.fn((rows: unknown[]) => rows);
  vi.mocked(filterRunningStyleFeatureRowsByActiveEntries).mockImplementation(passThrough as never);
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue(null);
  vi.mocked(loadFlatLightGBMModelFromR2).mockResolvedValue({
    header: { feature_names: ["x"], model_version: "v7-lineage" },
  } as never);
  vi.mocked(listDailyRaceEntriesForRace).mockResolvedValue([{}] as never);
  vi.mocked(buildRunningStyleFeaturesForRaceFromD1Target).mockResolvedValue({
    rows: [{ raceKey: "k" }, { raceKey: "k" }, { raceKey: "k" }],
  } as never);
  vi.mocked(validateFeatureCoverage).mockReturnValue({
    missingCells: 0,
    missingFeatureNames: [],
  });
  vi.mocked(resolveRunningStyleExpectedHorseCount).mockReturnValue(3);
  vi.mocked(loadRunningStyleFeatureParquet).mockResolvedValue([{}, {}] as never);
  vi.mocked(runRunningStyleInferenceRowsWithFlatModel).mockResolvedValue({
    modelVersion: "v7-lineage",
    writtenCount: 2,
  } as never);

  const summary = await handleRunningStylePredictionJob(buildEnv(), JOB);
  expect(summary?.writtenCount).toBe(2);
  expect(summary?.cacheWritten).toBe(false);
  expect(listRaceRunningStylesForRace).not.toHaveBeenCalled();
});

it("captures cacheCompletedRunningStyles errors via cacheError", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue({
    expectedHorseCount: 5,
    featuresR2Key: "features.parquet",
    modelVersion: "v7-lineage",
    status: "completed",
    writtenHorseCount: 5,
  } as never);
  vi.mocked(listRaceRunningStylesForRace).mockRejectedValue(new Error("d1 read failure"));

  const summary = await handleRunningStylePredictionJob(buildEnv(), JOB);
  expect(summary?.cacheError).toBe("d1 read failure");
  expect(summary?.cacheWritten).toBe(false);
});
