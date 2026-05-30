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

it("running-style writes completed status with horse rows when parquet is present", async () => {
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
  expect(result.writtenCount).toBe(1);
  expect(upsertRunningStyle).toHaveBeenCalledTimes(1);
  expect(send).not.toHaveBeenCalled();
  const stateCall = vi.mocked(upsertRunningStyleInferenceState).mock.calls.at(-1)![1];
  expect(stateCall.status).toBe("completed");
});

// Base fixture for "undefined leaks from parquet decoder" tests. The
// DailyRaceEntryRow type narrows undefined out of the optional columns,
// so we mutate via Reflect.set to inject runtime `undefined`, simulating
// hyparquet returning undefined for optional parquet columns.
const buildDailyRow = (): DailyRaceEntryRow => ({
  babajotai_code_dirt: null,
  babajotai_code_shiba: null,
  bamei: null,
  banushimei: null,
  barei: null,
  bataiju: null,
  chokyoshimei_ryakusho: null,
  corner1_norm: null,
  corner2_norm: null,
  corner3_norm: null,
  corner4_norm: null,
  corner_1: null,
  corner_2: null,
  corner_3: null,
  corner_4: null,
  finish_norm: null,
  finish_position: null,
  futan_juryo: null,
  grade_code: null,
  hasso_jikoku: null,
  juryo_shubetsu_code: null,
  kaisai_nen: "2026",
  kaisai_tsukihi: "0531",
  keibajo_code: "30",
  ketto_toroku_bango: "2020100099",
  kishumei_ryakusho: null,
  kohan_3f: null,
  kyori: null,
  kyoso_joken_code: null,
  kyoso_shubetsu_code: null,
  race_bango: "08",
  race_date: "2026-05-31",
  race_name: null,
  seibetsu_code: null,
  shusso_tosu: null,
  soha_time: null,
  source: "nar",
  tansho_ninkijun: null,
  tansho_odds: null,
  time_sa: null,
  track_code: null,
  umaban: 1,
  wakuban: null,
  zogen_fugo: null,
  zogen_sa: null,
});

it("running-style coerces undefined bamei to null in the persisted row", async () => {
  const arrayBuffer = vi.fn().mockResolvedValue(new ArrayBuffer(8));
  const objectFromR2 = { arrayBuffer };
  const undefinedBameiRow = buildDailyRow();
  Reflect.set(undefinedBameiRow, "bamei", undefined);
  vi.mocked(decodeRaceFeaturesParquet).mockResolvedValueOnce([undefinedBameiRow]);
  const { env } = buildInferenceTestEnv(objectFromR2);
  const result = await handleRunningStylePredictionJob(buildJob(), env);
  expect(result.writtenCount).toBe(1);
  expect(upsertRunningStyle).toHaveBeenCalledTimes(1);
  const persistedRow = vi.mocked(upsertRunningStyle).mock.calls[0]![1];
  expect(persistedRow.bamei).toBeNull();
});

it("running-style coerces missing bamei key to null in the persisted row", async () => {
  const arrayBuffer = vi.fn().mockResolvedValue(new ArrayBuffer(8));
  const objectFromR2 = { arrayBuffer };
  const missingBameiRow = buildDailyRow();
  Reflect.deleteProperty(missingBameiRow, "bamei");
  vi.mocked(decodeRaceFeaturesParquet).mockResolvedValueOnce([missingBameiRow]);
  const { env } = buildInferenceTestEnv(objectFromR2);
  const result = await handleRunningStylePredictionJob(buildJob(), env);
  expect(result.writtenCount).toBe(1);
  expect(upsertRunningStyle).toHaveBeenCalledTimes(1);
  const persistedRow = vi.mocked(upsertRunningStyle).mock.calls[0]![1];
  expect(persistedRow.bamei).toBeNull();
});

it("running-style skips a row whose umaban is undefined", async () => {
  const arrayBuffer = vi.fn().mockResolvedValue(new ArrayBuffer(8));
  const objectFromR2 = { arrayBuffer };
  const undefinedUmabanRow = buildDailyRow();
  Reflect.set(undefinedUmabanRow, "umaban", undefined);
  vi.mocked(decodeRaceFeaturesParquet).mockResolvedValueOnce([undefinedUmabanRow]);
  const { env } = buildInferenceTestEnv(objectFromR2);
  const result = await handleRunningStylePredictionJob(buildJob(), env);
  expect(result.writtenCount).toBe(0);
  expect(upsertRunningStyle).not.toHaveBeenCalled();
});

it("running-style skips a row whose ketto_toroku_bango is undefined", async () => {
  const arrayBuffer = vi.fn().mockResolvedValue(new ArrayBuffer(8));
  const objectFromR2 = { arrayBuffer };
  const missingKettoRow = buildDailyRow();
  Reflect.set(missingKettoRow, "ketto_toroku_bango", undefined);
  vi.mocked(decodeRaceFeaturesParquet).mockResolvedValueOnce([missingKettoRow]);
  const { env } = buildInferenceTestEnv(objectFromR2);
  const result = await handleRunningStylePredictionJob(buildJob(), env);
  expect(result.writtenCount).toBe(0);
  expect(upsertRunningStyle).not.toHaveBeenCalled();
});

it("running-style skips rows whose umaban is null but writes completed status", async () => {
  const arrayBuffer = vi.fn().mockResolvedValue(new ArrayBuffer(8));
  const objectFromR2 = { arrayBuffer } as unknown as R2ObjectBody;
  const dailyRow: DailyRaceEntryRow = {
    source: "nar",
    race_date: "2026-05-31",
    kaisai_nen: "2026",
    kaisai_tsukihi: "0531",
    keibajo_code: "30",
    race_bango: "08",
    ketto_toroku_bango: "2020100002",
    wakuban: null,
    umaban: null,
    bamei: null,
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
  const { env } = buildInferenceTestEnv(objectFromR2);
  const result = await handleRunningStylePredictionJob(buildJob(), env);
  expect(result.writtenCount).toBe(0);
  expect(upsertRunningStyle).not.toHaveBeenCalled();
});
