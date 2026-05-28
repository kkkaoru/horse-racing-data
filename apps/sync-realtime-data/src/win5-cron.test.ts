// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { Win5Schedule } from "../../pc-keiba-viewer/src/lib/win5/types";
import type { Env } from "./types";

vi.mock("../../pc-keiba-viewer/src/lib/win5/jra-parse", () => ({
  fetchWin5SchedulesFromJra: vi.fn(),
}));
vi.mock("./finish-position-lite-pool", () => ({
  getFinishPositionPool: vi.fn(() => ({})),
}));
vi.mock("./storage", () => ({
  logFetch: vi.fn(async () => {}),
}));
vi.mock("./win5-d1", () => ({
  getWin5Prediction: vi.fn(),
  upsertWin5Schedule: vi.fn(async () => {}),
}));
vi.mock("./win5-postgres", () => ({
  enrichWin5ScheduleLegs: vi.fn(async (_pool: unknown, schedule: Win5Schedule) => schedule),
}));
vi.mock("./running-style-cron", () => ({
  formatTomorrowYYYYMMDDInJst: vi.fn(),
  formatYYYYMMDDInJst: vi.fn(),
}));

const importCron = async () => await import("./win5-cron");

const SCHEDULE: Win5Schedule = {
  fetchedAt: "2026-05-10T09:00:00+09:00",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0511",
  legs: [],
  saleDeadline: null,
  source: "jra_web",
};

const buildEnv = (overrides?: Partial<Env>): Env => {
  return {
    REALTIME_DB: {},
    WIN5_D1_WRITE_ENABLED: "1",
    WIN5_JOBS: {
      sendBatch: vi.fn(async () => {}),
    },
    ...overrides,
  } as unknown as Env;
};

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("syncWin5SchedulesFromJra enriches and upserts every schedule, returning enriched list", async () => {
  const { syncWin5SchedulesFromJra } = await importCron();
  const { fetchWin5SchedulesFromJra } =
    await import("../../pc-keiba-viewer/src/lib/win5/jra-parse");
  const { upsertWin5Schedule } = await import("./win5-d1");
  const { enrichWin5ScheduleLegs } = await import("./win5-postgres");
  vi.mocked(fetchWin5SchedulesFromJra).mockResolvedValue([SCHEDULE]);
  const env = buildEnv();
  const result = await syncWin5SchedulesFromJra(env, { fetchedAt: "2026-05-10T09:00:00+09:00" });
  expect(result).toStrictEqual([SCHEDULE]);
  expect(enrichWin5ScheduleLegs).toHaveBeenCalledTimes(1);
  expect(upsertWin5Schedule).toHaveBeenCalledTimes(1);
});

it("syncWin5SchedulesFromJra defaults fetchedAt to now when options omits it", async () => {
  const { syncWin5SchedulesFromJra } = await importCron();
  const { fetchWin5SchedulesFromJra } =
    await import("../../pc-keiba-viewer/src/lib/win5/jra-parse");
  vi.mocked(fetchWin5SchedulesFromJra).mockResolvedValue([SCHEDULE]);
  const env = buildEnv();
  await syncWin5SchedulesFromJra(env);
  const call = vi.mocked(fetchWin5SchedulesFromJra).mock.calls[0]?.[0];
  expect(typeof call?.fetchedAt).toBe("string");
});

it("planWin5PredictionsForDate skips when WIN5_D1_WRITE_ENABLED is not '1'", async () => {
  const { planWin5PredictionsForDate } = await importCron();
  const { getWin5Prediction } = await import("./win5-d1");
  const env = buildEnv({ WIN5_D1_WRITE_ENABLED: "0" });
  const result = await planWin5PredictionsForDate(env, "20260511");
  expect(result).toStrictEqual({ date: "20260511", enqueued: 0, scanned: 0 });
  expect(getWin5Prediction).not.toHaveBeenCalled();
});

it("planWin5PredictionsForDate skips when a prediction already exists", async () => {
  const { planWin5PredictionsForDate } = await importCron();
  const { getWin5Prediction } = await import("./win5-d1");
  vi.mocked(getWin5Prediction).mockResolvedValue({
    defaultBudgetYen: 2000,
    kaisaiNen: "2026",
    kaisaiTsukihi: "0511",
    legs: [],
    modelVersion: "win5-heuristic-v1",
    plans: {},
    predictedAt: "x",
    recommendedBudgetYen: 2000,
  });
  const env = buildEnv();
  const result = await planWin5PredictionsForDate(env, "20260511");
  expect(result).toStrictEqual({ date: "20260511", enqueued: 0, scanned: 1 });
});

it("planWin5PredictionsForDate enqueues a job when no prediction exists", async () => {
  const { planWin5PredictionsForDate } = await importCron();
  const { getWin5Prediction } = await import("./win5-d1");
  vi.mocked(getWin5Prediction).mockResolvedValue(null);
  const env = buildEnv();
  const sendBatchSpy = vi.spyOn(env.WIN5_JOBS!, "sendBatch");
  const result = await planWin5PredictionsForDate(
    env,
    "20260511",
    new Date("2026-05-10T12:00:00.000Z"),
  );
  expect(result).toStrictEqual({ date: "20260511", enqueued: 1, scanned: 1 });
  expect(sendBatchSpy).toHaveBeenCalledTimes(1);
});

it("discoverWin5Schedules returns zeros when WIN5_D1_WRITE_ENABLED is not '1'", async () => {
  const { discoverWin5Schedules } = await importCron();
  const env = buildEnv({ WIN5_D1_WRITE_ENABLED: undefined });
  const result = await discoverWin5Schedules(env);
  expect(result).toStrictEqual({ discovered: 0, enqueued: 0 });
});

it("discoverWin5Schedules dispatches one job per schedule", async () => {
  const { discoverWin5Schedules } = await importCron();
  const { fetchWin5SchedulesFromJra } =
    await import("../../pc-keiba-viewer/src/lib/win5/jra-parse");
  const { formatYYYYMMDDInJst } = await import("./running-style-cron");
  vi.mocked(formatYYYYMMDDInJst).mockReturnValue("20260510");
  vi.mocked(fetchWin5SchedulesFromJra).mockResolvedValue([
    SCHEDULE,
    { ...SCHEDULE, kaisaiTsukihi: "0518" },
  ]);
  const env = buildEnv();
  const result = await discoverWin5Schedules(env, new Date("2026-05-10T12:00:00.000Z"));
  expect(result).toStrictEqual({ discovered: 2, enqueued: 2 });
});

it("runWin5CronTick aggregates discovery + tomorrow plan into a single summary", async () => {
  const { runWin5CronTick } = await importCron();
  const { fetchWin5SchedulesFromJra } =
    await import("../../pc-keiba-viewer/src/lib/win5/jra-parse");
  const { formatTomorrowYYYYMMDDInJst, formatYYYYMMDDInJst } = await import("./running-style-cron");
  const { getWin5Prediction } = await import("./win5-d1");
  vi.mocked(fetchWin5SchedulesFromJra).mockResolvedValue([SCHEDULE]);
  vi.mocked(formatYYYYMMDDInJst).mockReturnValue("20260510");
  vi.mocked(formatTomorrowYYYYMMDDInJst).mockReturnValue("20260511");
  vi.mocked(getWin5Prediction).mockResolvedValue(null);

  const env = buildEnv();
  const summary = await runWin5CronTick(env, new Date("2026-05-10T12:00:00.000Z"));
  expect(summary).toStrictEqual({ discovered: 1, enqueued: 1, tomorrowPlanned: 1 });
});

it("logWin5CronResult logs success when the tick completes", async () => {
  const { logWin5CronResult } = await importCron();
  const { fetchWin5SchedulesFromJra } =
    await import("../../pc-keiba-viewer/src/lib/win5/jra-parse");
  const { formatTomorrowYYYYMMDDInJst, formatYYYYMMDDInJst } = await import("./running-style-cron");
  const { getWin5Prediction } = await import("./win5-d1");
  const { logFetch } = await import("./storage");
  vi.mocked(fetchWin5SchedulesFromJra).mockResolvedValue([]);
  vi.mocked(formatYYYYMMDDInJst).mockReturnValue("20260510");
  vi.mocked(formatTomorrowYYYYMMDDInJst).mockReturnValue("20260511");
  vi.mocked(getWin5Prediction).mockResolvedValue(null);
  await logWin5CronResult(buildEnv(), new Date("2026-05-10T12:00:00.000Z"));
  expect(logFetch).toHaveBeenCalledTimes(1);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "discover-win5-schedules",
    "ok",
    null,
    expect.any(String),
  );
});

it("logWin5CronResult logs error message when the tick throws", async () => {
  const { logWin5CronResult } = await importCron();
  const { fetchWin5SchedulesFromJra } =
    await import("../../pc-keiba-viewer/src/lib/win5/jra-parse");
  const { formatYYYYMMDDInJst } = await import("./running-style-cron");
  const { logFetch } = await import("./storage");
  vi.mocked(formatYYYYMMDDInJst).mockReturnValue("20260510");
  vi.mocked(fetchWin5SchedulesFromJra).mockRejectedValue(new Error("boom"));
  await logWin5CronResult(buildEnv(), new Date("2026-05-10T12:00:00.000Z"));
  expect(logFetch).toHaveBeenCalledTimes(1);
  expect(logFetch).toHaveBeenCalledWith(
    expect.anything(),
    "discover-win5-schedules",
    "error",
    null,
    "boom",
  );
});
