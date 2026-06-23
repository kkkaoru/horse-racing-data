import { beforeEach, expect, it, vi } from "vitest";
import { buildWeatherJobs, dispatchWeatherJobs, getTodayJst, handleScheduled } from "./scheduled";
import { VENUE_COORDS } from "./venue-coords";

const mockSendBatch = vi.fn().mockResolvedValue(undefined);
const mockEnv = {
  WEATHER_DB: {} as unknown as D1Database,
  WEATHER_JOBS: { sendBatch: mockSendBatch },
} as unknown as import("./types").Env;

beforeEach(() => {
  vi.clearAllMocks();
});

it("getTodayJst returns a string in YYYY-MM-DD format", () => {
  const result = getTodayJst();
  expect(/^\d{4}-\d{2}-\d{2}$/.test(result)).toBe(true);
});

it("getTodayJst returns JST date (9 hours ahead of UTC)", () => {
  // Use a fixed UTC time where JST crosses midnight
  const mockNow = new Date("2026-06-22T15:30:00.000Z"); // UTC 15:30 → JST 00:30 next day
  vi.setSystemTime(mockNow);

  const result = getTodayJst();
  expect(result).toBe("2026-06-23");

  vi.useRealTimers();
});

it("getTodayJst stays on same day when UTC time does not cross midnight in JST", () => {
  const mockNow = new Date("2026-06-22T12:00:00.000Z"); // UTC 12:00 → JST 21:00 same day
  vi.setSystemTime(mockNow);

  const result = getTodayJst();
  expect(result).toBe("2026-06-22");

  vi.useRealTimers();
});

it("buildWeatherJobs returns one job per venue for all 25 venues", () => {
  const jobs = buildWeatherJobs("2026-06-22", "forecast");
  expect(jobs).toHaveLength(25);
});

it("buildWeatherJobs returns jobs with correct structure", () => {
  const jobs = buildWeatherJobs("2026-06-22", "actual");
  const job = jobs[0]!;
  expect(job.raceDate).toBe("2026-06-22");
  expect(job.type).toBe("actual");
  expect(typeof job.keibajoCode).toBe("string");
});

it("buildWeatherJobs includes all venue codes from VENUE_COORDS", () => {
  const jobs = buildWeatherJobs("2026-06-22", "forecast");
  const codes = jobs.map((j) => j.keibajoCode);
  const expectedCodes = Object.keys(VENUE_COORDS).sort();
  expect(codes).toStrictEqual(expectedCodes);
});

it("buildWeatherJobs sets correct weatherType for forecast", () => {
  const jobs = buildWeatherJobs("2026-06-22", "forecast");
  expect(jobs.every((j) => j.type === "forecast")).toBe(true);
});

it("buildWeatherJobs sets correct weatherType for actual", () => {
  const jobs = buildWeatherJobs("2026-06-22", "actual");
  expect(jobs.every((j) => j.type === "actual")).toBe(true);
});

it("dispatchWeatherJobs calls sendBatch with 25 job messages", async () => {
  await dispatchWeatherJobs(mockEnv, "2026-06-22", "forecast");

  expect(mockSendBatch).toHaveBeenCalledTimes(1);
  const batchArg = mockSendBatch.mock.calls[0]![0] as Array<{ body: unknown }>;
  expect(batchArg).toHaveLength(25);
});

it("dispatchWeatherJobs wraps each job in a body object", async () => {
  await dispatchWeatherJobs(mockEnv, "2026-06-22", "actual");

  const batchArg = mockSendBatch.mock.calls[0]![0] as Array<{ body: unknown }>;
  // First key after .sort() is "01" (lexicographic order)
  expect(batchArg[0]).toStrictEqual({
    body: {
      type: "actual",
      keibajoCode: "01",
      raceDate: "2026-06-22",
    },
  });
});

it("handleScheduled dispatches forecast jobs for forecast cron", async () => {
  const mockEvent = { cron: "30 21 * * *" } as ScheduledController;

  vi.setSystemTime(new Date("2026-06-22T12:00:00.000Z"));
  await handleScheduled(mockEvent, mockEnv);
  vi.useRealTimers();

  expect(mockSendBatch).toHaveBeenCalledTimes(1);
  const batchArg = mockSendBatch.mock.calls[0]![0] as Array<{
    body: { type: string };
  }>;
  expect(batchArg[0]!.body.type).toBe("forecast");
});

it("handleScheduled dispatches actual jobs for non-forecast cron", async () => {
  const mockEvent = { cron: "0 11 * * *" } as ScheduledController;

  vi.setSystemTime(new Date("2026-06-22T12:00:00.000Z"));
  await handleScheduled(mockEvent, mockEnv);
  vi.useRealTimers();

  expect(mockSendBatch).toHaveBeenCalledTimes(1);
  const batchArg = mockSendBatch.mock.calls[0]![0] as Array<{
    body: { type: string };
  }>;
  expect(batchArg[0]!.body.type).toBe("actual");
});

it("handleScheduled uses JST today as raceDate", async () => {
  const mockEvent = { cron: "30 21 * * *" } as ScheduledController;

  vi.setSystemTime(new Date("2026-06-22T12:00:00.000Z")); // JST: 2026-06-22 21:00
  await handleScheduled(mockEvent, mockEnv);
  vi.useRealTimers();

  const batchArg = mockSendBatch.mock.calls[0]![0] as Array<{
    body: { raceDate: string };
  }>;
  expect(batchArg[0]!.body.raceDate).toBe("2026-06-22");
});
