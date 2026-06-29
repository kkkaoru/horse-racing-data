// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { Env, RunningStylePredictionJob } from "./types";

const finishPositionPoolMock = vi.hoisted(() => ({
  readPool: { pool: "read" },
  writePool: { pool: "write" },
}));

vi.mock("./finish-position-d1", () => ({
  markFinishPositionFeaturesCached: vi.fn(async () => {}),
}));
vi.mock("./finish-position-inputs-cache", () => ({
  putFinishPositionInputsCache: vi.fn(async () => true),
}));
vi.mock("./finish-position-lite-pool", () => ({
  getFinishPositionPool: vi.fn(() => finishPositionPoolMock.readPool),
  getFinishPositionWritePool: vi.fn(() => finishPositionPoolMock.writePool),
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
  normalizeKeibajoCode: vi.fn((value: string) => value.padStart(2, "0")),
  normalizeRaceBango: vi.fn((value: string) => value.padStart(2, "0")),
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

const CELL_ROUTING_JSON = JSON.stringify({
  jra: {
    defaultVariantId: "latest",
    rules: [{ conditions: [{ dimension: "grade_code", values: ["A"] }], variantId: "grade-a" }],
    variants: {
      latest: { modelKey: "models/jra/latest.flatbin" },
      "grade-a": { modelKey: "models/jra/grade-a.flatbin" },
    },
  },
});

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
  const { upsertRunningStylePredictionsToNeon } = await import("./running-style-neon");
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue({
    expectedHorseCount: 5,
    featuresR2Key: "features.parquet",
    modelVersion: "v7-lineage",
    status: "completed",
    writtenHorseCount: 5,
  } as never);
  vi.mocked(listRaceRunningStylesForRace).mockResolvedValue([
    { raceKey: "jra:20260512:08:01" },
    { raceKey: "jra:20260512:08:01" },
    { raceKey: "jra:20260512:08:01" },
    { raceKey: "jra:20260512:08:01" },
    { raceKey: "jra:20260512:08:01" },
  ] as never);
  vi.mocked(upsertRunningStylePredictionsToNeon).mockResolvedValue(5);

  const summary = await handleRunningStylePredictionJob(buildEnv(), JOB);
  expect(summary?.skipped).toBe(true);
  expect(summary?.writtenCount).toBe(5);
  expect(summary?.modelVersion).toBe("v7-lineage");
  expect(summary?.cellVariantId).toBe("latest");
  expect(summary?.cellModelKey).toBe("models/jra/latest.flatbin");
  expect(errorSpy).toHaveBeenCalledWith(
    "Finish-position full trigger not sent for jra:20260512:08:01: missing FINISH_POSITION_CRON binding",
  );
});

it("triggers finish-position full run when state is already completed", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
  const { upsertRunningStylePredictionsToNeon } = await import("./running-style-neon");
  const fetch = vi.fn<typeof globalThis.fetch>(
    async (_input) => new Response("queued", { status: 202 }),
  );
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue({
    expectedHorseCount: 5,
    featuresR2Key: "features.parquet",
    modelVersion: "v7-lineage",
    status: "completed",
    writtenHorseCount: 5,
  } as never);
  vi.mocked(listRaceRunningStylesForRace).mockResolvedValue([
    { raceKey: "jra:20260512:08:01" },
    { raceKey: "jra:20260512:08:01" },
    { raceKey: "jra:20260512:08:01" },
    { raceKey: "jra:20260512:08:01" },
    { raceKey: "jra:20260512:08:01" },
  ] as never);
  vi.mocked(upsertRunningStylePredictionsToNeon).mockResolvedValue(5);

  const summary = await handleRunningStylePredictionJob(
    buildEnv({ FINISH_POSITION_CRON: { fetch }, TRIGGER_TOKEN: "secret-token" }),
    JOB,
  );
  const request = fetch.mock.calls[0]![0] as Request;
  expect(summary?.skipped).toBe(true);
  expect(fetch).toHaveBeenCalledTimes(1);
  expect(request.url).toBe("https://finish-position-cron.internal/run");
  expect(request.method).toBe("POST");
  expect(request.headers.get("authorization")).toBe("Bearer secret-token");
  expect(await request.json()).toStrictEqual({
    category: "jra",
    keibajoCode: "08",
    mode: "full",
    raceBango: "01",
    runDate: "20260512",
    skipDedup: true,
  });
});

it("does not call finish-position binding when trigger token is missing or empty", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
  const { upsertRunningStylePredictionsToNeon } = await import("./running-style-neon");
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  const fetch = vi.fn<typeof globalThis.fetch>(
    async (_input) => new Response("queued", { status: 202 }),
  );
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue({
    expectedHorseCount: 2,
    featuresR2Key: "features.parquet",
    modelVersion: "v7-lineage",
    status: "completed",
    writtenHorseCount: 2,
  } as never);
  vi.mocked(listRaceRunningStylesForRace).mockResolvedValue([{}, {}] as never);
  vi.mocked(upsertRunningStylePredictionsToNeon).mockResolvedValue(2);

  const missingTokenSummary = await handleRunningStylePredictionJob(
    buildEnv({ FINISH_POSITION_CRON: { fetch } }),
    JOB,
  );
  const emptyTokenSummary = await handleRunningStylePredictionJob(
    buildEnv({ FINISH_POSITION_CRON: { fetch }, TRIGGER_TOKEN: "" }),
    JOB,
  );
  expect(missingTokenSummary?.skipped).toBe(true);
  expect(emptyTokenSummary?.skipped).toBe(true);
  expect(fetch).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenNthCalledWith(
    1,
    "Finish-position full trigger not sent for jra:20260512:08:01: missing TRIGGER_TOKEN",
  );
  expect(errorSpy).toHaveBeenNthCalledWith(
    2,
    "Finish-position full trigger not sent for jra:20260512:08:01: empty TRIGGER_TOKEN",
  );
});

it("skips finish-position full trigger on completed short-circuit when Neon write count is short", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
  const { upsertRunningStylePredictionsToNeon } = await import("./running-style-neon");
  const fetch = vi.fn<typeof globalThis.fetch>(
    async (_input) => new Response("queued", { status: 202 }),
  );
  vi.spyOn(console, "log").mockImplementation(() => {});
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue({
    expectedHorseCount: 3,
    featuresR2Key: "features.parquet",
    modelVersion: "v7-lineage",
    status: "completed",
    writtenHorseCount: 3,
  } as never);
  vi.mocked(listRaceRunningStylesForRace).mockResolvedValue([{}, {}, {}] as never);
  vi.mocked(upsertRunningStylePredictionsToNeon).mockResolvedValue(2);

  const summary = await handleRunningStylePredictionJob(
    buildEnv({ FINISH_POSITION_CRON: { fetch }, TRIGGER_TOKEN: "secret-token" }),
    JOB,
  );
  expect(summary?.skipped).toBe(true);
  expect(summary?.neonWrittenCount).toBe(2);
  expect(fetch).not.toHaveBeenCalled();
  expect(vi.mocked(console.log).mock.calls[0]?.[0]).toBe(
    "finish-position trigger skipped for jra:20260512:08:01: Neon written count 2 is below expected horse count 3",
  );
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
  const { getFinishPositionPool, getFinishPositionWritePool } =
    await import("./finish-position-lite-pool");
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
  expect(summary?.cellVariantId).toBe("latest");
  expect(summary?.cellModelKey).toBe("models/jra/latest.flatbin");
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
  expect(getFinishPositionPool).toHaveBeenCalledTimes(1);
  expect(getFinishPositionWritePool).toHaveBeenCalledTimes(1);
  expect(vi.mocked(loadOrBuildRunningStyleFeatureParquet).mock.calls[0]?.[0]?.pool).toBe(
    finishPositionPoolMock.readPool,
  );
  expect(vi.mocked(upsertRunningStylePredictionsToNeon).mock.calls[0]?.[0]).toBe(
    finishPositionPoolMock.writePool,
  );
});

it("loads the selected cell model key and rematerializes with the selected header", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { filterRunningStyleFeatureRowsByActiveEntries } =
    await import("./running-style-expected-horses");
  const { runRunningStyleInferenceRowsWithFlatModel } = await import("./running-style-inference");
  const fetch = vi.fn<typeof globalThis.fetch>(
    async (_input) => new Response("queued", { status: 202 }),
  );
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue(null);
  vi.mocked(loadFlatLightGBMModelFromR2)
    .mockResolvedValueOnce({
      header: { feature_names: ["initial_feature"], model_version: "default-model" },
    } as never)
    .mockResolvedValueOnce({
      header: { feature_names: ["selected_feature"], model_version: "grade-a-model" },
    } as never);
  vi.mocked(loadOrBuildRunningStyleFeatureParquet)
    .mockResolvedValueOnce({
      featuresR2Key: "features.parquet",
      rebuilt: false,
      rows: [{ gradeCode: "A", umaban: 1 }],
    } as never)
    .mockResolvedValueOnce({
      featuresR2Key: "features.parquet",
      rebuilt: false,
      rows: [{ gradeCode: "A", perHorseFeatures: { selected_feature: 1 }, umaban: 1 }],
    } as never);
  vi.mocked(filterRunningStyleFeatureRowsByActiveEntries).mockReturnValue([
    { gradeCode: "A", perHorseFeatures: { selected_feature: 1 }, umaban: 1 },
  ] as never);
  vi.mocked(runRunningStyleInferenceRowsWithFlatModel).mockResolvedValue({
    modelVersion: "grade-a-model",
    writtenCount: 1,
  } as never);
  vi.mocked(listRaceRunningStylesForRace).mockResolvedValue([{}] as never);

  const summary = await handleRunningStylePredictionJob(
    buildEnv({
      FINISH_POSITION_CRON: { fetch },
      RUNNING_STYLE_CELL_ROUTING_JSON: CELL_ROUTING_JSON,
      TRIGGER_TOKEN: "secret-token",
    }),
    JOB,
  );
  expect(summary?.modelVersion).toBe("grade-a-model");
  expect(summary?.cellVariantId).toBe("grade-a");
  expect(summary?.cellModelKey).toBe("models/jra/grade-a.flatbin");
  expect(vi.mocked(loadFlatLightGBMModelFromR2).mock.calls[0]?.[1]).toBe(
    "models/jra/latest.flatbin",
  );
  expect(vi.mocked(loadFlatLightGBMModelFromR2).mock.calls[1]?.[1]).toBe(
    "models/jra/grade-a.flatbin",
  );
  expect(
    vi.mocked(loadOrBuildRunningStyleFeatureParquet).mock.calls[0]?.[0]?.featureNames,
  ).toStrictEqual(["initial_feature"]);
  expect(
    vi.mocked(loadOrBuildRunningStyleFeatureParquet).mock.calls[1]?.[0]?.featureNames,
  ).toStrictEqual(["selected_feature"]);
  expect(
    vi.mocked(runRunningStyleInferenceRowsWithFlatModel).mock.calls[0]?.[1]?.model.header
      .model_version,
  ).toBe("grade-a-model");
  expect(fetch).toHaveBeenCalledTimes(1);
});

it("does not fail the running-style job when finish-position full trigger rejects", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { filterRunningStyleFeatureRowsByActiveEntries } =
    await import("./running-style-expected-horses");
  const { runRunningStyleInferenceRowsWithFlatModel } = await import("./running-style-inference");
  const fetch = vi.fn<typeof globalThis.fetch>(async (_input): Promise<Response> => {
    throw new Error("service binding unavailable");
  });
  vi.spyOn(console, "error").mockImplementation(() => {});
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
  vi.mocked(listRaceRunningStylesForRace).mockResolvedValue([{}] as never);

  const summary = await handleRunningStylePredictionJob(
    buildEnv({ FINISH_POSITION_CRON: { fetch }, TRIGGER_TOKEN: "secret-token" }),
    JOB,
  );
  expect(summary?.writtenCount).toBe(1);
  expect(fetch).toHaveBeenCalledTimes(1);
});

it("does not fail the running-style job when finish-position full trigger returns an error", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { filterRunningStyleFeatureRowsByActiveEntries } =
    await import("./running-style-expected-horses");
  const { runRunningStyleInferenceRowsWithFlatModel } = await import("./running-style-inference");
  const fetch = vi.fn<typeof globalThis.fetch>(
    async (_input) => new Response("failed", { status: 500 }),
  );
  vi.spyOn(console, "error").mockImplementation(() => {});
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
  vi.mocked(listRaceRunningStylesForRace).mockResolvedValue([{}] as never);

  const summary = await handleRunningStylePredictionJob(
    buildEnv({ FINISH_POSITION_CRON: { fetch }, TRIGGER_TOKEN: "secret-token" }),
    JOB,
  );
  expect(summary?.writtenCount).toBe(1);
  expect(fetch).toHaveBeenCalledTimes(1);
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
  const fetch = vi.fn<typeof globalThis.fetch>(
    async (_input) => new Response("queued", { status: 202 }),
  );
  vi.spyOn(console, "log").mockImplementation(() => {});
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
  vi.mocked(resolveRunningStyleExpectedHorseCount).mockReturnValueOnce(3);
  vi.mocked(runRunningStyleInferenceRowsWithFlatModel).mockResolvedValue({
    modelVersion: "v7-lineage",
    writtenCount: 2,
  } as never);

  const summary = await handleRunningStylePredictionJob(
    buildEnv({ FINISH_POSITION_CRON: { fetch }, TRIGGER_TOKEN: "secret-token" }),
    JOB,
  );
  expect(summary?.writtenCount).toBe(2);
  expect(summary?.cacheWritten).toBe(false);
  expect(listRaceRunningStylesForRace).not.toHaveBeenCalled();
  expect(fetch).not.toHaveBeenCalled();
  expect(vi.mocked(console.log).mock.calls[0]?.[0]).toBe(
    "finish-position trigger skipped for jra:20260512:08:01: written count 2 is below expected horse count 3",
  );
});

it("skips finish-position full trigger after inference when Neon sync fails", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  const { loadOrBuildRunningStyleFeatureParquet } =
    await import("./running-style-feature-materialize");
  const { filterRunningStyleFeatureRowsByActiveEntries } =
    await import("./running-style-expected-horses");
  const { runRunningStyleInferenceRowsWithFlatModel } = await import("./running-style-inference");
  const { upsertRunningStylePredictionsToNeon } = await import("./running-style-neon");
  const fetch = vi.fn<typeof globalThis.fetch>(
    async (_input) => new Response("queued", { status: 202 }),
  );
  vi.spyOn(console, "error").mockImplementation(() => {});
  vi.spyOn(console, "log").mockImplementation(() => {});
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue(null);
  vi.mocked(loadFlatLightGBMModelFromR2).mockResolvedValue({
    header: { feature_names: ["x"], model_version: "v7-lineage" },
  } as never);
  vi.mocked(loadOrBuildRunningStyleFeatureParquet).mockResolvedValue({
    featuresR2Key: "features.parquet",
    rebuilt: false,
    rows: [{ umaban: 1 }, { umaban: 2 }],
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
  vi.mocked(upsertRunningStylePredictionsToNeon).mockRejectedValue(
    new Error("neon connection refused"),
  );

  const summary = await handleRunningStylePredictionJob(
    buildEnv({ FINISH_POSITION_CRON: { fetch }, TRIGGER_TOKEN: "secret-token" }),
    JOB,
  );
  expect(summary?.writtenCount).toBe(2);
  expect(summary?.neonError).toBe("neon connection refused");
  expect(fetch).not.toHaveBeenCalled();
  expect(vi.mocked(console.log).mock.calls[0]?.[0]).toBe(
    "finish-position trigger skipped for jra:20260512:08:01: Neon sync failed: neon connection refused",
  );
});

it("captures cacheCompletedRunningStyles errors via cacheError", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
  vi.spyOn(console, "log").mockImplementation(() => {});
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
  const fetch = vi.fn<typeof globalThis.fetch>(
    async (_input) => new Response("queued", { status: 202 }),
  );
  vi.spyOn(console, "log").mockImplementation(() => {});
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

  const summary = await handleRunningStylePredictionJob(
    buildEnv({ FINISH_POSITION_CRON: { fetch }, TRIGGER_TOKEN: "secret-token" }),
    JOB,
  );
  expect(summary?.neonWrittenCount).toBe(0);
  expect(summary?.neonError).toBe("neon connection refused");
  expect(summary?.skipped).toBe(true);
  expect(fetch).not.toHaveBeenCalled();
  expect(vi.mocked(console.log).mock.calls[0]?.[0]).toBe(
    "finish-position trigger skipped for jra:20260512:08:01: Neon sync failed: neon connection refused",
  );
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
  const { getRunningStyleInferenceState, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
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
  vi.mocked(listRaceRunningStylesForRace).mockResolvedValue([{}] as never);

  await handleRunningStylePredictionJob(buildEnv(), JOB);
  expect(
    vi.mocked(runRunningStyleInferenceRowsWithFlatModel).mock.calls[0]?.[1]?.calibrators,
  ).toStrictEqual(calibratorsTable);
});

it("falls back to uncalibrated inference when loadCalibratorsFromR2 rejects", async () => {
  const { handleRunningStylePredictionJob } = await import("./running-style-queue");
  const { getRunningStyleInferenceState, listRaceRunningStylesForRace } =
    await import("./running-style-d1");
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
  vi.mocked(listRaceRunningStylesForRace).mockResolvedValue([{}] as never);

  const summary = await handleRunningStylePredictionJob(buildEnv(), JOB);
  expect(summary?.writtenCount).toBe(1);
  expect(
    vi.mocked(runRunningStyleInferenceRowsWithFlatModel).mock.calls[0]?.[1]?.calibrators,
  ).toBeUndefined();
});
