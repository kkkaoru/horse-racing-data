// Run with bun.
import { expect, test, vi } from "vitest";

vi.mock("../src/finish-position-lite-pool", () => ({
  getFinishPositionPool: vi.fn(() => ({}) as never),
  getFinishPositionWritePool: vi.fn(() => ({}) as never),
}));
vi.mock("../src/finish-position-d1", () => ({
  markFinishPositionFeaturesCached: vi.fn(async () => undefined),
}));
vi.mock("../src/finish-position-inputs-cache", () => ({
  putFinishPositionInputsCache: vi.fn(async () => undefined),
}));
vi.mock("../src/viewer-running-style-cache", () => ({
  putViewerRunningStyleRaceCache: vi.fn(async () => true),
}));
vi.mock("../src/running-style-expected-horses", () => ({
  filterRunningStyleFeatureRowsByActiveEntries: vi.fn((rows: unknown[]) => rows),
  resolveRunningStyleExpectedHorseCount: vi.fn(() => 0),
}));
vi.mock("../src/running-style-features", async () => {
  const actual = await vi.importActual<typeof import("../src/running-style-features")>(
    "../src/running-style-features",
  );
  return {
    ...actual,
    buildRunningStyleFeaturesForRaceFromD1Target: vi.fn(async () => ({ rows: [] })),
    validateFeatureCoverage: vi.fn(() => ({ missingFeatureNames: [] })),
  };
});
vi.mock("../src/storage", () => ({
  getLatestRaceEntries: vi.fn(async () => null),
  listDailyRaceEntriesForRace: vi.fn(async () => []),
}));
vi.mock("../src/running-style-d1", () => ({
  getRunningStyleInferenceState: vi.fn(async () => null),
  listRaceRunningStylesForRace: vi.fn(async () => []),
  markRunningStyleInferenceFailed: vi.fn(async () => undefined),
  markRunningStyleInferenceProcessing: vi.fn(async () => undefined),
  markRunningStyleInferenceSucceeded: vi.fn(async () => undefined),
  upsertRaceRunningStyles: vi.fn(async () => undefined),
}));
vi.mock("../src/running-style-model-binary", () => ({
  buildRunningStyleFlatModelKey: vi.fn(() => "flat-key"),
  loadFlatLightGBMModelFromR2: vi.fn(async () => ({
    header: { feature_names: [], num_class: 4 },
  })),
}));

import { handleRunningStylePredictionJob } from "../src/running-style-queue";
import type { Env, RunningStylePredictionJob } from "../src/types";

const JOB: RunningStylePredictionJob = {
  kaisaiNen: "2026",
  kaisaiTsukihi: "0528",
  keibajoCode: "50",
  predictedAt: "2026-05-28T00:00:00.000Z",
  raceBango: "04",
  raceKey: "nar:20260528:50:04",
  source: "nar",
  type: "generate-running-style-predictions",
};

const buildEnv = (overrides: Partial<Env> = {}): Env =>
  ({
    REALTIME_DB: {} as never,
    RUNNING_STYLE_D1_WRITE_ENABLED: "1",
    RUNNING_STYLE_MODELS: {} as never,
    ...overrides,
  }) as Env;

test("returns null when RUNNING_STYLE_D1_WRITE_ENABLED is not '1'", async () => {
  const env = buildEnv({ RUNNING_STYLE_D1_WRITE_ENABLED: "0" });
  expect(await handleRunningStylePredictionJob(env, JOB)).toBe(null);
});

test("returns skipped summary when inference state is already completed", async () => {
  const { getRunningStyleInferenceState } = await import("../src/running-style-d1");
  vi.mocked(getRunningStyleInferenceState).mockResolvedValueOnce({
    attemptedAt: null,
    expectedHorseCount: 8,
    featuresR2Key: "flat-key/2026/0528/50/04.bin",
    modelVersion: "v20260518",
    status: "completed",
    writtenHorseCount: 8,
  } as never);
  const env = buildEnv();
  const summary = await handleRunningStylePredictionJob(env, JOB);
  expect(summary?.skipped).toBe(true);
  expect(summary?.featuresR2Key).toBe("flat-key/2026/0528/50/04.bin");
  expect(summary?.modelVersion).toBe("v20260518");
});

test("returns skipped summary when completed state has null featuresR2Key and modelVersion", async () => {
  const { getRunningStyleInferenceState } = await import("../src/running-style-d1");
  vi.mocked(getRunningStyleInferenceState).mockResolvedValueOnce({
    attemptedAt: null,
    expectedHorseCount: 8,
    featuresR2Key: null,
    modelVersion: null,
    status: "completed",
    writtenHorseCount: 8,
  } as never);
  const env = buildEnv();
  const summary = await handleRunningStylePredictionJob(env, JOB);
  expect(summary?.skipped).toBe(true);
  expect(summary?.featuresR2Key).toBe("");
  expect(summary?.modelVersion).toBe("completed");
});
