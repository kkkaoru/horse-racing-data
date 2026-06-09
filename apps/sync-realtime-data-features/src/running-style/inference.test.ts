// Run with: bun run --filter sync-realtime-data-features test
import { beforeEach, expect, it, vi } from "vitest";

vi.mock("../features/parquet", () => ({
  decodeRaceFeaturesParquet: vi.fn(async () => []),
}));
vi.mock("../storage", () => ({
  upsertRunningStyle: vi.fn(async () => {}),
  upsertRunningStyleInferenceState: vi.fn(async () => {}),
}));

import { decodeRaceFeaturesParquet } from "../features/parquet";
import { upsertRunningStyle, upsertRunningStyleInferenceState } from "../storage";
import { handleRunningStylePredictionJob } from "./inference";
import type { DailyRaceEntryRow, Env, Job, RaceJobKey } from "../types";

const SKELETON_DISABLED_STATUS = "skeleton-disabled";

interface InferenceTestEnv {
  send: ReturnType<typeof vi.fn>;
  r2Get: ReturnType<typeof vi.fn>;
  env: Env;
}

const buildJob = (): RaceJobKey & { predictedAt: string } => ({
  kaisaiNen: "2026",
  kaisaiTsukihi: "0531",
  keibajoCode: "30",
  predictedAt: "2026-05-31T03:00:00.000Z",
  raceBango: "08",
  raceKey: "nar:2026:0531:30:08",
  source: "nar",
});

const buildInferenceTestEnv = (objectFromR2: unknown): InferenceTestEnv => {
  const send = vi.fn().mockResolvedValue(undefined);
  const r2Get = vi.fn().mockResolvedValue(objectFromR2);
  const env = {
    REALTIME_FEATURES_DB: {} as unknown as D1Database,
    REALTIME_FEATURES_JOBS: { send } as unknown as Queue<Job>,
    FEATURES_KV: {} as unknown as KVNamespace,
    FEATURES_ARCHIVE: { get: r2Get } as unknown as R2Bucket,
    MODELS: {} as unknown as R2Bucket,
  } as unknown as Env;
  return { env, r2Get, send };
};

beforeEach(() => {
  vi.mocked(decodeRaceFeaturesParquet).mockReset();
  vi.mocked(decodeRaceFeaturesParquet).mockResolvedValue([]);
  vi.mocked(upsertRunningStyle).mockReset();
  vi.mocked(upsertRunningStyle).mockResolvedValue(undefined);
  vi.mocked(upsertRunningStyleInferenceState).mockReset();
  vi.mocked(upsertRunningStyleInferenceState).mockResolvedValue(undefined);
});

it("running-style auto-enqueues build-race-features when parquet is missing", async () => {
  const { env, send } = buildInferenceTestEnv(null);
  const result = await handleRunningStylePredictionJob(buildJob(), env);
  expect(result).toStrictEqual({
    raceKey: "nar:2026:0531:30:08",
    writtenCount: 0,
  });
  expect(send).toHaveBeenCalledTimes(1);
  expect(send).toHaveBeenCalledWith({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0531",
    keibajoCode: "30",
    raceBango: "08",
    raceKey: "nar:2026:0531:30:08",
    source: "nar",
    type: "build-race-features",
  });
});

it("running-style writes missing-parquet status before auto-recovery enqueue", async () => {
  const { env, send } = buildInferenceTestEnv(null);
  await handleRunningStylePredictionJob(buildJob(), env);
  expect(upsertRunningStyleInferenceState).toHaveBeenCalledTimes(1);
  const stateCall = vi.mocked(upsertRunningStyleInferenceState).mock.calls[0]![1];
  expect(stateCall.status).toBe("missing-parquet");
  expect(stateCall.errorMessage).toBe("features parquet not found in R2");
  expect(send).toHaveBeenCalledTimes(1);
});

it("running-style does not enqueue when parquet is present", async () => {
  const arrayBuffer = vi.fn().mockResolvedValue(new ArrayBuffer(8));
  const objectFromR2 = { arrayBuffer } as unknown as R2ObjectBody;
  const { env, send } = buildInferenceTestEnv(objectFromR2);
  const result = await handleRunningStylePredictionJob(buildJob(), env);
  expect(send).not.toHaveBeenCalled();
  expect(result.writtenCount).toBe(0);
});

it("running-style does NOT call upsertRunningStyle when parquet is present (skeleton-disabled)", async () => {
  const arrayBuffer = vi.fn().mockResolvedValue(new ArrayBuffer(8));
  const objectFromR2 = { arrayBuffer } as unknown as R2ObjectBody;
  const dailyRow: DailyRaceEntryRow = {
    source: "nar",
    race_date: "2026-05-31",
    kaisai_nen: "2026",
    kaisai_tsukihi: "0531",
    keibajo_code: "30",
    race_bango: "08",
    ketto_toroku_bango: "2020100001",
    wakuban: "1",
    umaban: 1,
    bamei: "Horse-1",
    race_name: null,
    hasso_jikoku: null,
    track_code: null,
    grade_code: null,
    kyoso_shubetsu_code: null,
    juryo_shubetsu_code: null,
    kyoso_joken_code: null,
    babajotai_code_shiba: null,
    babajotai_code_dirt: null,
    kyori: null,
    shusso_tosu: null,
    seibetsu_code: null,
    barei: null,
    futan_juryo: null,
    kishumei_ryakusho: null,
    chokyoshimei_ryakusho: null,
    banushimei: null,
    finish_position: null,
    finish_norm: null,
    tansho_ninkijun: null,
    tansho_odds: null,
    soha_time: null,
    time_sa: null,
    kohan_3f: null,
    corner1_norm: null,
    corner2_norm: null,
    corner3_norm: null,
    corner4_norm: null,
    corner_1: null,
    corner_2: null,
    corner_3: null,
    corner_4: null,
    bataiju: null,
    zogen_fugo: null,
    zogen_sa: null,
  };
  vi.mocked(decodeRaceFeaturesParquet).mockResolvedValueOnce([dailyRow]);
  const { env, send } = buildInferenceTestEnv(objectFromR2);
  const result = await handleRunningStylePredictionJob(buildJob(), env);
  expect(result.writtenCount).toBe(0);
  expect(upsertRunningStyle).not.toHaveBeenCalled();
  expect(send).not.toHaveBeenCalled();
});

it("running-style writes inference_state with skeleton-disabled status when parquet is present", async () => {
  const arrayBuffer = vi.fn().mockResolvedValue(new ArrayBuffer(8));
  const objectFromR2 = { arrayBuffer } as unknown as R2ObjectBody;
  vi.mocked(decodeRaceFeaturesParquet).mockResolvedValueOnce([]);
  const { env } = buildInferenceTestEnv(objectFromR2);
  await handleRunningStylePredictionJob(buildJob(), env);
  expect(upsertRunningStyleInferenceState).toHaveBeenCalledTimes(1);
  const stateCall = vi.mocked(upsertRunningStyleInferenceState).mock.calls[0]![1];
  expect(stateCall.status).toBe(SKELETON_DISABLED_STATUS);
  expect(stateCall.writtenHorseCount).toBe(0);
});

it("running-style reports expectedHorseCount from the decoded parquet even when disabled", async () => {
  const arrayBuffer = vi.fn().mockResolvedValue(new ArrayBuffer(8));
  const objectFromR2 = { arrayBuffer } as unknown as R2ObjectBody;
  const dailyRow: DailyRaceEntryRow = {
    source: "nar",
    race_date: "2026-05-31",
    kaisai_nen: "2026",
    kaisai_tsukihi: "0531",
    keibajo_code: "30",
    race_bango: "08",
    ketto_toroku_bango: "2020100003",
    wakuban: "2",
    umaban: 2,
    bamei: "Horse-2",
    race_name: null,
    hasso_jikoku: null,
    track_code: null,
    grade_code: null,
    kyoso_shubetsu_code: null,
    juryo_shubetsu_code: null,
    kyoso_joken_code: null,
    babajotai_code_shiba: null,
    babajotai_code_dirt: null,
    kyori: null,
    shusso_tosu: null,
    seibetsu_code: null,
    barei: null,
    futan_juryo: null,
    kishumei_ryakusho: null,
    chokyoshimei_ryakusho: null,
    banushimei: null,
    finish_position: null,
    finish_norm: null,
    tansho_ninkijun: null,
    tansho_odds: null,
    soha_time: null,
    time_sa: null,
    kohan_3f: null,
    corner1_norm: null,
    corner2_norm: null,
    corner3_norm: null,
    corner4_norm: null,
    corner_1: null,
    corner_2: null,
    corner_3: null,
    corner_4: null,
    bataiju: null,
    zogen_fugo: null,
    zogen_sa: null,
  };
  vi.mocked(decodeRaceFeaturesParquet).mockResolvedValueOnce([dailyRow, dailyRow]);
  const { env } = buildInferenceTestEnv(objectFromR2);
  await handleRunningStylePredictionJob(buildJob(), env);
  const stateCall = vi.mocked(upsertRunningStyleInferenceState).mock.calls[0]![1];
  expect(stateCall.expectedHorseCount).toBe(2);
  expect(stateCall.writtenHorseCount).toBe(0);
});
