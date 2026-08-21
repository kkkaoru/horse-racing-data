// Run with bun. Tests for first prediction fanout after a fresh day-base HIT.

import { beforeEach, expect, test, vi } from "vitest";
import type { RaceEntry } from "./cron-decision";

const { enqueuePredictMock, enumerateTodaysRacesMock } = vi.hoisted(() => ({
  enqueuePredictMock: vi.fn(async () => ["jra"]),
  enumerateTodaysRacesMock: vi.fn(async (): Promise<RaceEntry[]> => []),
}));

vi.mock("./cron-decision", () => ({ enumerateTodaysRaces: enumerateTodaysRacesMock }));
vi.mock("./queue-producer", () => ({ enqueuePredict: enqueuePredictMock }));

import { fanOutPredictionsAfterDayBaseHit } from "./feature-hit-prediction";
import type { Env } from "./types";

const env = {
  PREDICT_DAYS_AHEAD: "2",
  REALTIME_DB: { name: "realtime" },
} as unknown as Env;

beforeEach(() => {
  enqueuePredictMock.mockClear();
  enumerateTodaysRacesMock.mockReset();
  enumerateTodaysRacesMock.mockResolvedValue([]);
});

test("fans out one full prediction per category race after the caller proves a HIT", async () => {
  enumerateTodaysRacesMock.mockResolvedValue([
    { category: "jra", keibajoCode: "05", raceBango: "01" },
    { category: "jra", keibajoCode: "05", raceBango: "02" },
    { category: "nar", keibajoCode: "44", raceBango: "01" },
  ]);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

  await expect(
    fanOutPredictionsAfterDayBaseHit({ category: "jra", env, runYmd: "20260822" }),
  ).resolves.toBe(2);

  expect(enumerateTodaysRacesMock).toHaveBeenCalledWith(env.REALTIME_DB, "20260822");
  expect(enqueuePredictMock).toHaveBeenCalledTimes(2);
  expect(enqueuePredictMock).toHaveBeenNthCalledWith(1, {
    category: "jra",
    daysAhead: 2,
    env,
    keibajoCode: "05",
    mode: "full",
    raceBango: "01",
    runDate: "2026-08-22",
    runYmd: "20260822",
    skipDedup: true,
  });
  expect(logSpy).toHaveBeenCalledWith(
    "[feature-hit-prediction] enqueued category=jra runYmd=20260822 races=2",
  );
  logSpy.mockRestore();
});

test("does not enqueue another category and reports an empty category day", async () => {
  enumerateTodaysRacesMock.mockResolvedValue([
    { category: "nar", keibajoCode: "44", raceBango: "01" },
  ]);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

  await expect(
    fanOutPredictionsAfterDayBaseHit({ category: "jra", env, runYmd: "20260822" }),
  ).resolves.toBe(0);
  expect(enqueuePredictMock).not.toHaveBeenCalled();
  logSpy.mockRestore();
});
