// Run with bun. Tests for first prediction fanout after a fresh day-base HIT.

import { beforeEach, expect, test, vi } from "vitest";
import type { RaceEntry } from "./cron-decision";

const { enqueuePredictMock, enumerateTodaysRacesMock, getRunningStyleRaceReadinessMock } =
  vi.hoisted(() => ({
    enqueuePredictMock: vi.fn(async () => ["jra"]),
    enumerateTodaysRacesMock: vi.fn(async (): Promise<RaceEntry[]> => []),
    getRunningStyleRaceReadinessMock: vi.fn(
      async (params: {
        races: readonly RaceEntry[];
      }): Promise<readonly { race: RaceEntry; reason: string | null }[]> =>
        params.races.map((race) => ({ race, reason: null })),
    ),
  }));

vi.mock("./cron-decision", () => ({ enumerateTodaysRaces: enumerateTodaysRacesMock }));
vi.mock("./queue-producer", () => ({ enqueuePredict: enqueuePredictMock }));
vi.mock("./running-style-readiness", () => ({
  getRunningStyleRaceReadiness: getRunningStyleRaceReadinessMock,
}));

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
  getRunningStyleRaceReadinessMock.mockClear();
});

test("fans out one full prediction per category race after the caller proves a HIT", async () => {
  enumerateTodaysRacesMock.mockResolvedValue([
    {
      category: "jra",
      keibajoCode: "05",
      raceBango: "01",
      raceStartAtJst: "2026-08-22T09:50:00+09:00",
    },
    {
      category: "jra",
      keibajoCode: "05",
      raceBango: "02",
      raceStartAtJst: "2026-08-22T10:20:00+09:00",
    },
    { category: "nar", keibajoCode: "44", raceBango: "01" },
  ]);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

  await expect(
    fanOutPredictionsAfterDayBaseHit({ category: "jra", env, runYmd: "20260822" }),
  ).resolves.toBe(2);

  expect(enumerateTodaysRacesMock).toHaveBeenCalledWith(env.REALTIME_DB, "20260822");
  expect(getRunningStyleRaceReadinessMock).toHaveBeenCalledWith({
    category: "jra",
    db: env.REALTIME_DB,
    races: [
      {
        category: "jra",
        keibajoCode: "05",
        raceBango: "01",
        raceStartAtJst: "2026-08-22T09:50:00+09:00",
      },
      {
        category: "jra",
        keibajoCode: "05",
        raceBango: "02",
        raceStartAtJst: "2026-08-22T10:20:00+09:00",
      },
    ],
    runYmd: "20260822",
  });
  expect(enqueuePredictMock).toHaveBeenCalledTimes(2);
  expect(enqueuePredictMock).toHaveBeenNthCalledWith(1, {
    category: "jra",
    daysAhead: 2,
    env,
    keibajoCode: "05",
    mode: "full",
    raceBango: "01",
    raceStartAtJst: "2026-08-22T09:50:00+09:00",
    runDate: "2026-08-22",
    runYmd: "20260822",
    skipDedup: true,
  });
  expect(logSpy).toHaveBeenCalledWith(
    "[feature-hit-prediction] enqueued category=jra runYmd=20260822 races=2 duplicates=0 runningStyleIncomplete=0",
  );
  logSpy.mockRestore();
});

test("waits for each earlier post-time enqueue before sending the next race", async () => {
  enumerateTodaysRacesMock.mockResolvedValue([
    { category: "jra", keibajoCode: "01", raceBango: "01" },
    { category: "jra", keibajoCode: "05", raceBango: "01" },
  ]);
  let releaseFirst: (() => void) | undefined;
  enqueuePredictMock.mockImplementationOnce(
    () =>
      new Promise<string[]>((resolve) => {
        releaseFirst = () => resolve(["jra"]);
      }),
  );

  const fanOut = fanOutPredictionsAfterDayBaseHit({
    category: "jra",
    env,
    runYmd: "20260822",
  });
  await vi.waitFor(() => expect(enqueuePredictMock).toHaveBeenCalledTimes(1));
  releaseFirst?.();
  await fanOut;

  expect(enqueuePredictMock).toHaveBeenCalledTimes(2);
  expect(enqueuePredictMock).toHaveBeenNthCalledWith(
    1,
    expect.objectContaining({
      keibajoCode: "01",
      raceBango: "01",
    }),
  );
  expect(enqueuePredictMock).toHaveBeenNthCalledWith(
    2,
    expect.objectContaining({
      keibajoCode: "05",
      raceBango: "01",
    }),
  );
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

test("reports only newly reserved races when another producer already owns one", async () => {
  enumerateTodaysRacesMock.mockResolvedValue([
    { category: "jra", keibajoCode: "01", raceBango: "01" },
    { category: "jra", keibajoCode: "01", raceBango: "02" },
  ]);
  enqueuePredictMock.mockResolvedValueOnce([]).mockResolvedValueOnce(["jra"]);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await expect(
    fanOutPredictionsAfterDayBaseHit({ category: "jra", env, runYmd: "20260822" }),
  ).resolves.toBe(1);
  expect(logSpy).toHaveBeenCalledWith(
    "[feature-hit-prediction] enqueued category=jra runYmd=20260822 races=1 duplicates=1 runningStyleIncomplete=0",
  );
  logSpy.mockRestore();
});

test("skips only the race whose running-style entrants are incomplete", async () => {
  const readyRace: RaceEntry = { category: "jra", keibajoCode: "01", raceBango: "01" };
  const incompleteRace: RaceEntry = { category: "jra", keibajoCode: "01", raceBango: "02" };
  enumerateTodaysRacesMock.mockResolvedValue([readyRace, incompleteRace]);
  getRunningStyleRaceReadinessMock.mockResolvedValueOnce([
    { race: readyRace, reason: null },
    { race: incompleteRace, reason: "prediction-count-7-of-14" },
  ]);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

  await expect(
    fanOutPredictionsAfterDayBaseHit({ category: "jra", env, runYmd: "20260822" }),
  ).resolves.toBe(1);

  expect(enqueuePredictMock).toHaveBeenCalledTimes(1);
  expect(enqueuePredictMock).toHaveBeenCalledWith(
    expect.objectContaining({ keibajoCode: "01", raceBango: "01" }),
  );
  expect(warnSpy).toHaveBeenCalledWith(
    "[feature-hit-prediction] skipped-running-style-incomplete category=jra runYmd=20260822 keibajoCode=01 raceBango=02 reason=prediction-count-7-of-14",
  );
  expect(logSpy).toHaveBeenCalledWith(
    "[feature-hit-prediction] enqueued category=jra runYmd=20260822 races=1 duplicates=0 runningStyleIncomplete=1",
  );
  warnSpy.mockRestore();
  logSpy.mockRestore();
});

test("fans out every ban-ei race without requiring optional running-style state", async () => {
  enumerateTodaysRacesMock.mockResolvedValue([
    { category: "ban-ei", keibajoCode: "83", raceBango: "11" },
    { category: "ban-ei", keibajoCode: "83", raceBango: "12" },
    { category: "nar", keibajoCode: "55", raceBango: "10" },
  ]);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

  await expect(
    fanOutPredictionsAfterDayBaseHit({ category: "ban-ei", env, runYmd: "20260824" }),
  ).resolves.toBe(2);

  expect(getRunningStyleRaceReadinessMock).not.toHaveBeenCalled();
  expect(enqueuePredictMock).toHaveBeenCalledTimes(2);
  expect(enqueuePredictMock).toHaveBeenNthCalledWith(
    1,
    expect.objectContaining({ category: "ban-ei", keibajoCode: "83", raceBango: "11" }),
  );
  expect(enqueuePredictMock).toHaveBeenNthCalledWith(
    2,
    expect.objectContaining({ category: "ban-ei", keibajoCode: "83", raceBango: "12" }),
  );
  expect(warnSpy).not.toHaveBeenCalled();
  expect(logSpy).toHaveBeenCalledWith(
    "[feature-hit-prediction] enqueued category=ban-ei runYmd=20260824 races=2 duplicates=0 runningStyleIncomplete=0",
  );
  warnSpy.mockRestore();
  logSpy.mockRestore();
});
