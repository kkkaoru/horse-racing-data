// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { Env } from "./types";

vi.mock("../../pc-keiba-viewer/src/lib/win5/prediction", () => ({
  buildWin5PredictionPayload: vi.fn(),
}));
vi.mock("./finish-position-lite-pool", () => ({
  getFinishPositionPool: vi.fn(),
}));
vi.mock("./win5-d1", () => ({
  getWin5Schedule: vi.fn(),
  markWin5InferenceState: vi.fn(async () => {}),
  upsertWin5Prediction: vi.fn(async () => {}),
  upsertWin5Schedule: vi.fn(async () => {}),
}));
vi.mock("./win5-postgres", () => ({
  buildWin5LegInputsFromPostgres: vi.fn(),
  buildWin5ScheduleFromJvdWfRow: vi.fn(),
  enrichWin5ScheduleLegs: vi.fn(async (_pool: unknown, schedule: unknown) => schedule),
  getAverageWin5PayoutYen: vi.fn(),
}));

const SCHEDULE = {
  fetchedAt: "2026-05-10T09:00:00+09:00",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0511",
  legs: [{ kaisaiKai: "02", kaisaiNichime: "06", keibajoCode: "05", legIndex: 1, raceBango: "9" }],
  saleDeadline: null,
  source: "jra_web" as const,
};

const FIVE_LEG_INPUTS = [{}, {}, {}, {}, {}];

const buildEnv = (): Env => {
  return { REALTIME_DB: {} } as unknown as Env;
};

const buildJob = () => ({
  kaisaiNen: "2026",
  kaisaiTsukihi: "0511",
  predictedAt: "2026-05-11T11:55:00+09:00",
  type: "generate-win5-predictions" as const,
});

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("handleWin5PredictionJob marks processing then completed on success", async () => {
  const { handleWin5PredictionJob } = await import("./win5-queue");
  const { getWin5Schedule, markWin5InferenceState, upsertWin5Prediction } =
    await import("./win5-d1");
  const { buildWin5LegInputsFromPostgres, enrichWin5ScheduleLegs, getAverageWin5PayoutYen } =
    await import("./win5-postgres");
  const { buildWin5PredictionPayload } =
    await import("../../pc-keiba-viewer/src/lib/win5/prediction");
  const { getFinishPositionPool } = await import("./finish-position-lite-pool");
  vi.mocked(getWin5Schedule).mockResolvedValue(SCHEDULE);
  vi.mocked(buildWin5LegInputsFromPostgres).mockResolvedValue(FIVE_LEG_INPUTS as never);
  vi.mocked(getAverageWin5PayoutYen).mockResolvedValue(300000);
  vi.mocked(buildWin5PredictionPayload).mockReturnValue({
    defaultBudgetYen: 2000,
    kaisaiNen: "2026",
    kaisaiTsukihi: "0511",
    legs: [{ horses: [], leg: SCHEDULE.legs[0]! }],
    modelVersion: "win5-xgb-v7-lineage-v1",
    plans: {},
    predictedAt: "2026-05-11T11:55:00+09:00",
    recommendedBudgetYen: 3000,
  });
  vi.mocked(getFinishPositionPool).mockReturnValue({} as never);
  vi.mocked(enrichWin5ScheduleLegs).mockResolvedValue(SCHEDULE);

  const summary = await handleWin5PredictionJob(buildEnv(), buildJob());
  expect(summary.kaisaiNen).toBe("2026");
  expect(summary.kaisaiTsukihi).toBe("0511");
  expect(summary.legCount).toBe(1);
  expect(summary.modelVersion).toBe("win5-xgb-v7-lineage-v1");
  expect(markWin5InferenceState).toHaveBeenCalledTimes(2);
  expect(vi.mocked(markWin5InferenceState).mock.calls[0]![1].status).toBe("processing");
  expect(vi.mocked(markWin5InferenceState).mock.calls[1]![1].status).toBe("completed");
  expect(upsertWin5Prediction).toHaveBeenCalledTimes(1);
});

it("handleWin5PredictionJob falls back to jvd_wf row when D1 schedule is missing", async () => {
  const { handleWin5PredictionJob } = await import("./win5-queue");
  const { getWin5Schedule, upsertWin5Schedule } = await import("./win5-d1");
  const {
    buildWin5LegInputsFromPostgres,
    buildWin5ScheduleFromJvdWfRow,
    enrichWin5ScheduleLegs,
    getAverageWin5PayoutYen,
  } = await import("./win5-postgres");
  const { buildWin5PredictionPayload } =
    await import("../../pc-keiba-viewer/src/lib/win5/prediction");
  const { getFinishPositionPool } = await import("./finish-position-lite-pool");
  vi.mocked(getWin5Schedule).mockResolvedValue(null);
  vi.mocked(buildWin5ScheduleFromJvdWfRow).mockReturnValue(SCHEDULE);
  vi.mocked(enrichWin5ScheduleLegs).mockResolvedValue(SCHEDULE);
  vi.mocked(buildWin5LegInputsFromPostgres).mockResolvedValue(FIVE_LEG_INPUTS as never);
  vi.mocked(getAverageWin5PayoutYen).mockResolvedValue(300000);
  vi.mocked(buildWin5PredictionPayload).mockReturnValue({
    defaultBudgetYen: 2000,
    kaisaiNen: "2026",
    kaisaiTsukihi: "0511",
    legs: [],
    modelVersion: "win5-xgb-v7-lineage-v1",
    plans: {},
    predictedAt: "2026-05-11T11:55:00+09:00",
    recommendedBudgetYen: 3000,
  });
  vi.mocked(getFinishPositionPool).mockReturnValue({
    query: vi.fn(async () => ({ rows: [{ race_joho_1: "" }] })),
  } as never);

  await handleWin5PredictionJob(buildEnv(), buildJob());
  expect(buildWin5ScheduleFromJvdWfRow).toHaveBeenCalledTimes(1);
  expect(upsertWin5Schedule).toHaveBeenCalledTimes(1);
});

it("handleWin5PredictionJob throws when jvd_wf returns no schedule", async () => {
  const { handleWin5PredictionJob } = await import("./win5-queue");
  const { getWin5Schedule, markWin5InferenceState } = await import("./win5-d1");
  const { buildWin5ScheduleFromJvdWfRow } = await import("./win5-postgres");
  const { getFinishPositionPool } = await import("./finish-position-lite-pool");
  vi.mocked(getWin5Schedule).mockResolvedValue(null);
  vi.mocked(buildWin5ScheduleFromJvdWfRow).mockReturnValue(null);
  vi.mocked(getFinishPositionPool).mockReturnValue({
    query: vi.fn(async () => ({ rows: [] })),
  } as never);

  await expect(handleWin5PredictionJob(buildEnv(), buildJob())).rejects.toThrow(
    "WIN5 schedule not found",
  );
  expect(vi.mocked(markWin5InferenceState).mock.calls.at(-1)![1].status).toBe("failed");
});

it("handleWin5PredictionJob throws when fewer than 5 legs have inputs", async () => {
  const { handleWin5PredictionJob } = await import("./win5-queue");
  const { getWin5Schedule, markWin5InferenceState } = await import("./win5-d1");
  const { buildWin5LegInputsFromPostgres, enrichWin5ScheduleLegs } =
    await import("./win5-postgres");
  const { getFinishPositionPool } = await import("./finish-position-lite-pool");
  vi.mocked(getWin5Schedule).mockResolvedValue(SCHEDULE);
  vi.mocked(enrichWin5ScheduleLegs).mockResolvedValue(SCHEDULE);
  vi.mocked(buildWin5LegInputsFromPostgres).mockResolvedValue([{}, {}] as never);
  vi.mocked(getFinishPositionPool).mockReturnValue({} as never);

  await expect(handleWin5PredictionJob(buildEnv(), buildJob())).rejects.toThrow(
    "WIN5 runners incomplete",
  );
  expect(vi.mocked(markWin5InferenceState).mock.calls.at(-1)![1].status).toBe("failed");
});

it("handleWin5PredictionJob falls back to D1 schedule path when getWin5Schedule returns one", async () => {
  const { handleWin5PredictionJob } = await import("./win5-queue");
  const { getWin5Schedule, upsertWin5Schedule } = await import("./win5-d1");
  const { buildWin5LegInputsFromPostgres, enrichWin5ScheduleLegs, getAverageWin5PayoutYen } =
    await import("./win5-postgres");
  const { buildWin5PredictionPayload } =
    await import("../../pc-keiba-viewer/src/lib/win5/prediction");
  const { getFinishPositionPool } = await import("./finish-position-lite-pool");
  vi.mocked(getWin5Schedule).mockResolvedValue(SCHEDULE);
  vi.mocked(enrichWin5ScheduleLegs).mockResolvedValue(SCHEDULE);
  vi.mocked(buildWin5LegInputsFromPostgres).mockResolvedValue(FIVE_LEG_INPUTS as never);
  vi.mocked(getAverageWin5PayoutYen).mockResolvedValue(300000);
  vi.mocked(buildWin5PredictionPayload).mockReturnValue({
    defaultBudgetYen: 2000,
    kaisaiNen: "2026",
    kaisaiTsukihi: "0511",
    legs: [],
    modelVersion: "win5-xgb-v7-lineage-v1",
    plans: {},
    predictedAt: "2026-05-11T11:55:00+09:00",
    recommendedBudgetYen: 3000,
  });
  vi.mocked(getFinishPositionPool).mockReturnValue({} as never);

  await handleWin5PredictionJob(buildEnv(), buildJob());
  expect(upsertWin5Schedule).toHaveBeenCalledTimes(1);
});
