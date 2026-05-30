// Run with: bun run --filter sync-realtime-data-features test
import { expect, it, vi } from "vitest";

vi.mock("./scheduled-race-list", () => ({
  listTodayRaceKeysFromHyperdrive: vi.fn(),
}));

import { runPredictionsForDay } from "./admin-predict-for-day";
import { listTodayRaceKeysFromHyperdrive, type TodayRaceKey } from "./scheduled-race-list";
import type { Env } from "./types";

interface InferenceStateRaceKeyRow {
  race_key: string;
}

const buildEnv = (sendMock: ReturnType<typeof vi.fn>): Env =>
  ({
    REALTIME_FEATURES_DB: {} as unknown as D1Database,
    REALTIME_FEATURES_JOBS: { send: sendMock } as unknown as Queue,
    FEATURES_KV: {} as unknown as KVNamespace,
    FEATURES_ARCHIVE: {} as unknown as R2Bucket,
    HYPERDRIVE: { connectionString: "postgres://test" },
  }) as unknown as Env;

const buildEnvWithInferenceState = (args: {
  sendMock: ReturnType<typeof vi.fn>;
  runningStyleCompletedRaceKeys: string[];
  finishPositionCompletedRaceKeys: string[];
}): Env => {
  const runningStyleResults: InferenceStateRaceKeyRow[] = args.runningStyleCompletedRaceKeys.map(
    (raceKey) => ({ race_key: raceKey }),
  );
  const finishPositionResults: InferenceStateRaceKeyRow[] =
    args.finishPositionCompletedRaceKeys.map((raceKey) => ({ race_key: raceKey }));
  const allRunningStyle = vi.fn().mockResolvedValue({ results: runningStyleResults });
  const allFinishPosition = vi.fn().mockResolvedValue({ results: finishPositionResults });
  const bindRunningStyle = vi.fn().mockReturnValue({ all: allRunningStyle });
  const bindFinishPosition = vi.fn().mockReturnValue({ all: allFinishPosition });
  const prepare = vi.fn().mockImplementation((sql: string) => {
    if (sql.includes("running_style_inference_state")) {
      return { bind: bindRunningStyle };
    }
    return { bind: bindFinishPosition };
  });
  return {
    REALTIME_FEATURES_DB: { prepare } as unknown as D1Database,
    REALTIME_FEATURES_JOBS: { send: args.sendMock } as unknown as Queue,
    FEATURES_KV: {} as unknown as KVNamespace,
    FEATURES_ARCHIVE: {} as unknown as R2Bucket,
    HYPERDRIVE: { connectionString: "postgres://test" },
  } as unknown as Env;
};

const threeRaces: TodayRaceKey[] = [
  {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0531",
    keibajoCode: "05",
    raceBango: "01",
    raceKey: "jra:2026:0531:05:01",
    source: "jra",
  },
  {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0531",
    keibajoCode: "30",
    raceBango: "08",
    raceKey: "nar:2026:0531:30:08",
    source: "nar",
  },
  {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0531",
    keibajoCode: "83",
    raceBango: "11",
    raceKey: "nar:2026:0531:83:11",
    source: "nar",
  },
];

it("runPredictionsForDay source=all enqueues 6 messages (2 per race)", async () => {
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce(threeRaces);
  const send = vi.fn().mockResolvedValue(undefined);
  const env = buildEnv(send);
  const result = await runPredictionsForDay(env, { source: "all", targetYmd: "20260531" });
  expect(send).toHaveBeenCalledTimes(6);
  expect(result.enqueuedRunningStyle).toStrictEqual([
    "jra:2026:0531:05:01",
    "nar:2026:0531:30:08",
    "nar:2026:0531:83:11",
  ]);
  expect(result.enqueuedFinishPosition).toStrictEqual([
    "jra:2026:0531:05:01",
    "nar:2026:0531:30:08",
    "nar:2026:0531:83:11",
  ]);
  expect(result.skippedReasons).toStrictEqual([]);
});

it("runPredictionsForDay source=jra enqueues only JRA races (2 messages)", async () => {
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce(threeRaces);
  const send = vi.fn().mockResolvedValue(undefined);
  const env = buildEnv(send);
  const result = await runPredictionsForDay(env, { source: "jra", targetYmd: "20260531" });
  expect(send).toHaveBeenCalledTimes(2);
  expect(result.enqueuedRunningStyle).toStrictEqual(["jra:2026:0531:05:01"]);
  expect(result.enqueuedFinishPosition).toStrictEqual(["jra:2026:0531:05:01"]);
  expect(result.skippedReasons).toStrictEqual([]);
});

it("runPredictionsForDay source=nar enqueues only NAR races (4 messages)", async () => {
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce(threeRaces);
  const send = vi.fn().mockResolvedValue(undefined);
  const env = buildEnv(send);
  const result = await runPredictionsForDay(env, { source: "nar", targetYmd: "20260531" });
  expect(send).toHaveBeenCalledTimes(4);
  expect(result.enqueuedRunningStyle).toStrictEqual(["nar:2026:0531:30:08", "nar:2026:0531:83:11"]);
  expect(result.enqueuedFinishPosition).toStrictEqual([
    "nar:2026:0531:30:08",
    "nar:2026:0531:83:11",
  ]);
  expect(result.skippedReasons).toStrictEqual([]);
});

it("runPredictionsForDay enqueues predict-running-style and predict-finish-position with correct shape", async () => {
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([threeRaces[0]!]);
  const send = vi.fn().mockResolvedValue(undefined);
  const env = buildEnv(send);
  await runPredictionsForDay(env, { source: "all", targetYmd: "20260531" });
  const firstMessage = send.mock.calls[0]![0];
  expect(firstMessage).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0531",
    keibajoCode: "05",
    predictedAt: expect.any(String),
    raceBango: "01",
    raceKey: "jra:2026:0531:05:01",
    source: "jra",
    type: "predict-running-style",
  });
  const secondMessage = send.mock.calls[1]![0];
  expect(secondMessage).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0531",
    keibajoCode: "05",
    predictedAt: expect.any(String),
    raceBango: "01",
    raceKey: "jra:2026:0531:05:01",
    source: "jra",
    type: "predict-finish-position",
  });
});

it("runPredictionsForDay records skipped reasons when send rejects for running-style", async () => {
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([threeRaces[0]!]);
  const send = vi
    .fn()
    .mockRejectedValueOnce(new Error("queue boom"))
    .mockResolvedValueOnce(undefined);
  const env = buildEnv(send);
  const result = await runPredictionsForDay(env, { source: "jra", targetYmd: "20260531" });
  expect(result.enqueuedRunningStyle).toStrictEqual([]);
  expect(result.enqueuedFinishPosition).toStrictEqual(["jra:2026:0531:05:01"]);
  expect(result.skippedReasons).toStrictEqual([
    { raceKey: "jra:2026:0531:05:01", reason: "predict-running-style: queue boom" },
  ]);
});

it("runPredictionsForDay records skipped reasons when send rejects for finish-position", async () => {
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([threeRaces[1]!]);
  const send = vi
    .fn()
    .mockResolvedValueOnce(undefined)
    .mockRejectedValueOnce(new Error("queue down"));
  const env = buildEnv(send);
  const result = await runPredictionsForDay(env, { source: "nar", targetYmd: "20260531" });
  expect(result.enqueuedRunningStyle).toStrictEqual(["nar:2026:0531:30:08"]);
  expect(result.enqueuedFinishPosition).toStrictEqual([]);
  expect(result.skippedReasons).toStrictEqual([
    { raceKey: "nar:2026:0531:30:08", reason: "predict-finish-position: queue down" },
  ]);
});

it("runPredictionsForDay records both skipped reasons when send rejects for both jobs", async () => {
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([threeRaces[0]!]);
  const send = vi
    .fn()
    .mockRejectedValueOnce(new Error("rs down"))
    .mockRejectedValueOnce(new Error("fp down"));
  const env = buildEnv(send);
  const result = await runPredictionsForDay(env, { source: "jra", targetYmd: "20260531" });
  expect(result.enqueuedRunningStyle).toStrictEqual([]);
  expect(result.enqueuedFinishPosition).toStrictEqual([]);
  expect(result.skippedReasons).toStrictEqual([
    { raceKey: "jra:2026:0531:05:01", reason: "predict-running-style: rs down" },
    { raceKey: "jra:2026:0531:05:01", reason: "predict-finish-position: fp down" },
  ]);
});

it("runPredictionsForDay stringifies non-Error rejection reasons", async () => {
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([threeRaces[0]!]);
  const send = vi.fn().mockRejectedValueOnce("string-error").mockResolvedValueOnce(undefined);
  const env = buildEnv(send);
  const result = await runPredictionsForDay(env, { source: "jra", targetYmd: "20260531" });
  expect(result.skippedReasons).toStrictEqual([
    { raceKey: "jra:2026:0531:05:01", reason: "predict-running-style: string-error" },
  ]);
});

it("runPredictionsForDay returns empty result when no races match the source filter", async () => {
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([threeRaces[1]!]);
  const send = vi.fn().mockResolvedValue(undefined);
  const env = buildEnv(send);
  const result = await runPredictionsForDay(env, { source: "jra", targetYmd: "20260531" });
  expect(send).toHaveBeenCalledTimes(0);
  expect(result).toStrictEqual({
    enqueuedFinishPosition: [],
    enqueuedRunningStyle: [],
    skippedReasons: [],
  });
});

it("runPredictionsForDay returns empty result when Hyperdrive returns no races", async () => {
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([]);
  const send = vi.fn().mockResolvedValue(undefined);
  const env = buildEnv(send);
  const result = await runPredictionsForDay(env, { source: "all", targetYmd: "20260531" });
  expect(send).toHaveBeenCalledTimes(0);
  expect(result).toStrictEqual({
    enqueuedFinishPosition: [],
    enqueuedRunningStyle: [],
    skippedReasons: [],
  });
});

it("runPredictionsForDay forwards targetYmd to listTodayRaceKeysFromHyperdrive", async () => {
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([]);
  const send = vi.fn().mockResolvedValue(undefined);
  const env = buildEnv(send);
  await runPredictionsForDay(env, { source: "all", targetYmd: "20260601" });
  expect(listTodayRaceKeysFromHyperdrive).toHaveBeenCalledWith(env, "20260601");
});

it("runPredictionsForDay skipCompleted=true skips races completed in both inference state tables", async () => {
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce(threeRaces);
  const send = vi.fn().mockResolvedValue(undefined);
  const env = buildEnvWithInferenceState({
    finishPositionCompletedRaceKeys: ["jra:2026:0531:05:01", "nar:2026:0531:30:08"],
    runningStyleCompletedRaceKeys: ["jra:2026:0531:05:01", "nar:2026:0531:30:08"],
    sendMock: send,
  });
  const result = await runPredictionsForDay(env, {
    skipCompleted: true,
    source: "all",
    targetYmd: "20260531",
  });
  expect(send).toHaveBeenCalledTimes(2);
  expect(result.enqueuedRunningStyle).toStrictEqual(["nar:2026:0531:83:11"]);
  expect(result.enqueuedFinishPosition).toStrictEqual(["nar:2026:0531:83:11"]);
  expect(result.skippedReasons).toStrictEqual([
    { raceKey: "jra:2026:0531:05:01", reason: "already-completed" },
    { raceKey: "nar:2026:0531:30:08", reason: "already-completed" },
  ]);
});

it("runPredictionsForDay skipCompleted=true does NOT skip races completed only in running-style", async () => {
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce(threeRaces);
  const send = vi.fn().mockResolvedValue(undefined);
  const env = buildEnvWithInferenceState({
    finishPositionCompletedRaceKeys: [],
    runningStyleCompletedRaceKeys: ["jra:2026:0531:05:01"],
    sendMock: send,
  });
  const result = await runPredictionsForDay(env, {
    skipCompleted: true,
    source: "all",
    targetYmd: "20260531",
  });
  expect(send).toHaveBeenCalledTimes(6);
  expect(result.enqueuedRunningStyle).toStrictEqual([
    "jra:2026:0531:05:01",
    "nar:2026:0531:30:08",
    "nar:2026:0531:83:11",
  ]);
  expect(result.enqueuedFinishPosition).toStrictEqual([
    "jra:2026:0531:05:01",
    "nar:2026:0531:30:08",
    "nar:2026:0531:83:11",
  ]);
  expect(result.skippedReasons).toStrictEqual([]);
});

it("runPredictionsForDay skipCompleted=true does NOT skip races completed only in finish-position", async () => {
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce(threeRaces);
  const send = vi.fn().mockResolvedValue(undefined);
  const env = buildEnvWithInferenceState({
    finishPositionCompletedRaceKeys: ["nar:2026:0531:30:08"],
    runningStyleCompletedRaceKeys: [],
    sendMock: send,
  });
  const result = await runPredictionsForDay(env, {
    skipCompleted: true,
    source: "all",
    targetYmd: "20260531",
  });
  expect(send).toHaveBeenCalledTimes(6);
  expect(result.enqueuedRunningStyle).toStrictEqual([
    "jra:2026:0531:05:01",
    "nar:2026:0531:30:08",
    "nar:2026:0531:83:11",
  ]);
  expect(result.enqueuedFinishPosition).toStrictEqual([
    "jra:2026:0531:05:01",
    "nar:2026:0531:30:08",
    "nar:2026:0531:83:11",
  ]);
  expect(result.skippedReasons).toStrictEqual([]);
});

it("runPredictionsForDay skipCompleted=true emits already-completed skippedReasons entries", async () => {
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([
    threeRaces[0]!,
    threeRaces[2]!,
  ]);
  const send = vi.fn().mockResolvedValue(undefined);
  const env = buildEnvWithInferenceState({
    finishPositionCompletedRaceKeys: ["nar:2026:0531:83:11"],
    runningStyleCompletedRaceKeys: ["nar:2026:0531:83:11"],
    sendMock: send,
  });
  const result = await runPredictionsForDay(env, {
    skipCompleted: true,
    source: "all",
    targetYmd: "20260531",
  });
  expect(result.skippedReasons).toStrictEqual([
    { raceKey: "nar:2026:0531:83:11", reason: "already-completed" },
  ]);
});

it("runPredictionsForDay skipCompleted=false (default) ignores inference_state and enqueues all", async () => {
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce(threeRaces);
  const send = vi.fn().mockResolvedValue(undefined);
  const prepare = vi.fn();
  const env = {
    REALTIME_FEATURES_DB: { prepare } as unknown as D1Database,
    REALTIME_FEATURES_JOBS: { send } as unknown as Queue,
    FEATURES_KV: {} as unknown as KVNamespace,
    FEATURES_ARCHIVE: {} as unknown as R2Bucket,
    HYPERDRIVE: { connectionString: "postgres://test" },
  } as unknown as Env;
  const result = await runPredictionsForDay(env, { source: "all", targetYmd: "20260531" });
  expect(prepare).toHaveBeenCalledTimes(0);
  expect(send).toHaveBeenCalledTimes(6);
  expect(result.enqueuedRunningStyle).toStrictEqual([
    "jra:2026:0531:05:01",
    "nar:2026:0531:30:08",
    "nar:2026:0531:83:11",
  ]);
  expect(result.skippedReasons).toStrictEqual([]);
});

it("runPredictionsForDay skipCompleted=true with empty Hyperdrive returns empty result", async () => {
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([]);
  const send = vi.fn().mockResolvedValue(undefined);
  const env = buildEnvWithInferenceState({
    finishPositionCompletedRaceKeys: ["jra:2026:0531:05:01"],
    runningStyleCompletedRaceKeys: ["jra:2026:0531:05:01"],
    sendMock: send,
  });
  const result = await runPredictionsForDay(env, {
    skipCompleted: true,
    source: "all",
    targetYmd: "20260531",
  });
  expect(send).toHaveBeenCalledTimes(0);
  expect(result).toStrictEqual({
    enqueuedFinishPosition: [],
    enqueuedRunningStyle: [],
    skippedReasons: [],
  });
});

it("runPredictionsForDay skipCompleted=true handles undefined results from D1 .all()", async () => {
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([threeRaces[0]!]);
  const send = vi.fn().mockResolvedValue(undefined);
  const all = vi.fn().mockResolvedValue({});
  const bind = vi.fn().mockReturnValue({ all });
  const prepare = vi.fn().mockReturnValue({ bind });
  const env = {
    REALTIME_FEATURES_DB: { prepare } as unknown as D1Database,
    REALTIME_FEATURES_JOBS: { send } as unknown as Queue,
    FEATURES_KV: {} as unknown as KVNamespace,
    FEATURES_ARCHIVE: {} as unknown as R2Bucket,
    HYPERDRIVE: { connectionString: "postgres://test" },
  } as unknown as Env;
  const result = await runPredictionsForDay(env, {
    skipCompleted: true,
    source: "all",
    targetYmd: "20260531",
  });
  expect(send).toHaveBeenCalledTimes(2);
  expect(result.enqueuedRunningStyle).toStrictEqual(["jra:2026:0531:05:01"]);
  expect(result.skippedReasons).toStrictEqual([]);
});

it("runPredictionsForDay skipCompleted=true with invalid targetYmd length skips no races", async () => {
  vi.mocked(listTodayRaceKeysFromHyperdrive).mockResolvedValueOnce([threeRaces[0]!]);
  const send = vi.fn().mockResolvedValue(undefined);
  const prepare = vi.fn();
  const env = {
    REALTIME_FEATURES_DB: { prepare } as unknown as D1Database,
    REALTIME_FEATURES_JOBS: { send } as unknown as Queue,
    FEATURES_KV: {} as unknown as KVNamespace,
    FEATURES_ARCHIVE: {} as unknown as R2Bucket,
    HYPERDRIVE: { connectionString: "postgres://test" },
  } as unknown as Env;
  const result = await runPredictionsForDay(env, {
    skipCompleted: true,
    source: "all",
    targetYmd: "2026053",
  });
  expect(prepare).toHaveBeenCalledTimes(0);
  expect(send).toHaveBeenCalledTimes(2);
  expect(result.enqueuedRunningStyle).toStrictEqual(["jra:2026:0531:05:01"]);
  expect(result.skippedReasons).toStrictEqual([]);
});
