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
vi.mock("./running-style-feature-materialize", () => ({
  loadOrBuildRunningStyleFeatureParquet: vi.fn(),
}));
vi.mock("./running-style-features", () => ({
  buildRealtimeRaceKeyFromRunningStyle: vi.fn(() => "jra:20260512:08:01"),
  buildRunningStyleRaceKey: vi.fn(() => "jra:20260512:08:01"),
}));
vi.mock("./running-style-inference", () => ({
  runRunningStyleInferenceRowsWithFlatModel: vi.fn(),
}));
vi.mock("./running-style-calibration", () => ({
  buildCalibrationR2Key: vi.fn(() => "running-style/models/jra/calibrators.json"),
  loadCalibratorsFromR2: vi.fn(),
}));
vi.mock("./running-style-model-binary", () => ({
  buildRunningStyleFlatModelKey: vi.fn(() => "models/jra/latest.flatbin"),
  loadFlatLightGBMModelFromR2: vi.fn(),
}));
vi.mock("./running-style-neon", () => ({
  upsertRunningStylePredictionsToNeon: vi.fn(async () => 2),
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

it("completes the job from an R2 hit and returns the success summary", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { filterRunningStyleFeatureRowsByActiveEntries } =
    await import("./running-style-expected-horses");
  const { runRunningStyleInferenceRowsWithFlatModel } = await import("./running-style-inference");
  const { markFinishPositionFeaturesCached } = await import("./finish-position-d1");
  const { putFinishPositionInputsCache } = await import("./finish-position-inputs-cache");
  const { markRunningStyleInferenceCompleted } = await import("./running-style-d1");
  const { upsertRunningStylePredictionsToNeon } = await import("./running-style-neon");
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue(null);
  vi.mocked(loadFlatLightGBMModelFromR2).mockResolvedValue({
    header: { feature_names: ["x"], model_version: "v7-lineage" },
  } as never);
  vi.mocked(loadOrBuildRunningStyleFeatureParquet).mockResolvedValue({
    featuresR2Key: "features.parquet",
    rebuilt: false,
    rows: [{ umaban: 1 }, { umaban: 2 }, { umaban: 3 }],
  } as never);
  vi.mocked(filterRunningStyleFeatureRowsByActiveEntries).mockReturnValue([
    { umaban: 1 },
    { umaban: 2 },
  ] as never);
  vi.mocked(runRunningStyleInferenceRowsWithFlatModel).mockResolvedValue({
    modelVersion: "v7-lineage",
    writtenCount: 2,
  } as never);
  vi.mocked(listRaceRunningStylesForRace).mockResolvedValue([{}, {}] as never);
  vi.mocked(upsertRunningStylePredictionsToNeon).mockResolvedValue(2);

  const summary = await handleRunningStylePredictionJob(buildEnv(), JOB);
  expect(summary?.modelVersion).toBe("v7-lineage");
  expect(summary?.writtenCount).toBe(2);
  expect(summary?.raceKey).toBe("jra:20260512:08:01");
  expect(summary?.horseCount).toBe(2);
  expect(summary?.featuresR2Key).toBe("features.parquet");
  expect(summary?.cacheWritten).toBe(true);
  expect(summary?.neonWrittenCount).toBe(2);
  expect(summary?.neonError).toBeUndefined();
  expect(vi.mocked(markFinishPositionFeaturesCached).mock.calls[0]?.[2]?.featuresR2Key).toBe(
    "features.parquet",
  );
  expect(vi.mocked(putFinishPositionInputsCache).mock.calls[0]?.[0]?.payload.featuresR2Key).toBe(
    "features.parquet",
  );
  expect(
    vi.mocked(runRunningStyleInferenceRowsWithFlatModel).mock.calls[0]?.[1]?.rows,
  ).toStrictEqual([{ umaban: 1 }, { umaban: 2 }]);
  expect(vi.mocked(markRunningStyleInferenceCompleted).mock.calls[0]?.[1]?.writtenHorseCount).toBe(
    2,
  );
});

it("throws when filterRunningStyleFeatureRowsByActiveEntries leaves no rows", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState, markRunningStyleInferenceFailed } =
    await import("./running-style-d1");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { filterRunningStyleFeatureRowsByActiveEntries } =
    await import("./running-style-expected-horses");
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue(null);
  vi.mocked(loadFlatLightGBMModelFromR2).mockResolvedValue({
    header: { feature_names: ["x"], model_version: "v7-lineage" },
  } as never);
  vi.mocked(loadOrBuildRunningStyleFeatureParquet).mockResolvedValue({
    featuresR2Key: "features.parquet",
    rebuilt: false,
    rows: [{ umaban: 1 }, { umaban: 2 }],
  } as never);
  vi.mocked(filterRunningStyleFeatureRowsByActiveEntries).mockReturnValue([]);

  await expect(handleRunningStylePredictionJob(buildEnv(), JOB)).rejects.toThrow(
    "no active running-style feature rows found",
  );
  expect(markRunningStyleInferenceFailed).toHaveBeenCalledTimes(1);
});

it("marks the job failed and rethrows when loadOrBuildRunningStyleFeatureParquet rejects", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState, markRunningStyleInferenceFailed } =
    await import("./running-style-d1");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue(null);
  vi.mocked(loadFlatLightGBMModelFromR2).mockResolvedValue({
    header: { feature_names: ["x"], model_version: "v7-lineage" },
  } as never);
  vi.mocked(loadOrBuildRunningStyleFeatureParquet).mockRejectedValue(
    new Error("hyperdrive pool exhausted"),
  );

  await expect(handleRunningStylePredictionJob(buildEnv(), JOB)).rejects.toThrow(
    "hyperdrive pool exhausted",
  );
  expect(markRunningStyleInferenceFailed).toHaveBeenCalledTimes(1);
});

it("skips cacheCompletedRunningStyles when written count is less than expected horse count", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { filterRunningStyleFeatureRowsByActiveEntries, resolveRunningStyleExpectedHorseCount } =
    await import("./running-style-expected-horses");
  const { runRunningStyleInferenceRowsWithFlatModel } = await import("./running-style-inference");
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue(null);
  vi.mocked(loadFlatLightGBMModelFromR2).mockResolvedValue({
    header: { feature_names: ["x"], model_version: "v7-lineage" },
  } as never);
  vi.mocked(loadOrBuildRunningStyleFeatureParquet).mockResolvedValue({
    featuresR2Key: "features.parquet",
    rebuilt: true,
    rows: [{ umaban: 1 }, { umaban: 2 }, { umaban: 3 }],
  } as never);
  vi.mocked(filterRunningStyleFeatureRowsByActiveEntries).mockReturnValue([
    { umaban: 1 },
    { umaban: 2 },
    { umaban: 3 },
  ] as never);
  vi.mocked(resolveRunningStyleExpectedHorseCount).mockReturnValue(3);
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

it("returns neonWrittenCount when Neon write succeeds", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
  const { upsertRunningStylePredictionsToNeon } = await import("./running-style-neon");
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue({
    expectedHorseCount: 3,
    featuresR2Key: "features.parquet",
    modelVersion: "v7-lineage",
    status: "completed",
    writtenHorseCount: 3,
  } as never);
  vi.mocked(listRaceRunningStylesForRace).mockResolvedValue([{}, {}, {}] as never);
  vi.mocked(upsertRunningStylePredictionsToNeon).mockResolvedValue(3);

  const summary = await handleRunningStylePredictionJob(buildEnv(), JOB);
  expect(summary?.neonWrittenCount).toBe(3);
  expect(summary?.neonError).toBeUndefined();
});

it("sets neonError when Neon write fails but does not throw", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
  const { upsertRunningStylePredictionsToNeon } = await import("./running-style-neon");
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue({
    expectedHorseCount: 2,
    featuresR2Key: "features.parquet",
    modelVersion: "v7-lineage",
    status: "completed",
    writtenHorseCount: 2,
  } as never);
  vi.mocked(listRaceRunningStylesForRace).mockResolvedValue([{}, {}] as never);
  vi.mocked(upsertRunningStylePredictionsToNeon).mockRejectedValue(
    new Error("neon connection refused"),
  );

  const summary = await handleRunningStylePredictionJob(buildEnv(), JOB);
  expect(summary?.neonWrittenCount).toBe(0);
  expect(summary?.neonError).toBe("neon connection refused");
  expect(summary?.skipped).toBe(true);
});

it("sets cacheWritten false when putViewerRunningStyleRaceCache rejects", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
  const { putViewerRunningStyleRaceCache } = await import("./viewer-running-style-cache");
  const { upsertRunningStylePredictionsToNeon } = await import("./running-style-neon");
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue({
    expectedHorseCount: 2,
    featuresR2Key: "features.parquet",
    modelVersion: "v7-lineage",
    status: "completed",
    writtenHorseCount: 2,
  } as never);
  vi.mocked(listRaceRunningStylesForRace).mockResolvedValue([{}, {}] as never);
  vi.mocked(putViewerRunningStyleRaceCache).mockRejectedValue(new Error("cache write failed"));
  vi.mocked(upsertRunningStylePredictionsToNeon).mockResolvedValue(2);

  const summary = await handleRunningStylePredictionJob(buildEnv(), JOB);
  expect(summary?.cacheWritten).toBe(false);
  expect(summary?.neonWrittenCount).toBe(2);
});

it("passes calibrators to inference when loadCalibratorsFromR2 resolves", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState } = await import("./running-style-d1");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { filterRunningStyleFeatureRowsByActiveEntries } =
    await import("./running-style-expected-horses");
  const { runRunningStyleInferenceRowsWithFlatModel } = await import("./running-style-inference");
  const { loadCalibratorsFromR2 } = await import("./running-style-calibration");
  const calibratorsTable = {
    calibrators: {
      nige: { x: [0, 1], y: [0, 1] },
      senkou: { x: [0, 1], y: [0, 1] },
      sashi: { x: [0, 1], y: [0, 1] },
      oikomi: { x: [0, 1], y: [0, 1] },
    },
    category: "jra",
    classes: ["nige", "senkou", "sashi", "oikomi"],
    fit_year: 2025,
  };
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue(null);
  vi.mocked(loadFlatLightGBMModelFromR2).mockResolvedValue({
    header: { feature_names: ["x"], model_version: "v7-lineage" },
  } as never);
  vi.mocked(loadOrBuildRunningStyleFeatureParquet).mockResolvedValue({
    featuresR2Key: "features.parquet",
    rebuilt: false,
    rows: [{ umaban: 1 }],
  } as never);
  vi.mocked(filterRunningStyleFeatureRowsByActiveEntries).mockReturnValue([{ umaban: 1 }] as never);
  vi.mocked(runRunningStyleInferenceRowsWithFlatModel).mockResolvedValue({
    modelVersion: "v7-lineage",
    writtenCount: 1,
  } as never);
  vi.mocked(loadCalibratorsFromR2).mockResolvedValue(calibratorsTable as never);

  await handleRunningStylePredictionJob(buildEnv(), JOB);
  expect(
    vi.mocked(runRunningStyleInferenceRowsWithFlatModel).mock.calls[0]?.[1]?.calibrators,
  ).toStrictEqual(calibratorsTable);
});

it("falls back to uncalibrated inference when loadCalibratorsFromR2 rejects", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState } = await import("./running-style-d1");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { filterRunningStyleFeatureRowsByActiveEntries } =
    await import("./running-style-expected-horses");
  const { runRunningStyleInferenceRowsWithFlatModel } = await import("./running-style-inference");
  const { loadCalibratorsFromR2 } = await import("./running-style-calibration");
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue(null);
  vi.mocked(loadFlatLightGBMModelFromR2).mockResolvedValue({
    header: { feature_names: ["x"], model_version: "v7-lineage" },
  } as never);
  vi.mocked(loadOrBuildRunningStyleFeatureParquet).mockResolvedValue({
    featuresR2Key: "features.parquet",
    rebuilt: false,
    rows: [{ umaban: 1 }],
  } as never);
  vi.mocked(filterRunningStyleFeatureRowsByActiveEntries).mockReturnValue([{ umaban: 1 }] as never);
  vi.mocked(runRunningStyleInferenceRowsWithFlatModel).mockResolvedValue({
    modelVersion: "v7-lineage",
    writtenCount: 1,
  } as never);
  vi.mocked(loadCalibratorsFromR2).mockRejectedValue(new Error("R2 not found"));

  const summary = await handleRunningStylePredictionJob(buildEnv(), JOB);
  expect(summary?.writtenCount).toBe(1);
  expect(
    vi.mocked(runRunningStyleInferenceRowsWithFlatModel).mock.calls[0]?.[1]?.calibrators,
  ).toBeUndefined();
});
