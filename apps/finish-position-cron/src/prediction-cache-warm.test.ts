// Run with bun. Tests for the viewer prediction cache warming module.

import { afterEach, beforeEach, expect, test, vi } from "vitest";
import {
  warmPredictionCacheForCategory,
  warmPredictionCacheForRace,
} from "./prediction-cache-warm";
import type { Env } from "./types";

interface RaceWarmRow {
  keibajo_code: string;
  race_bango: string;
}

const allMock = vi.fn(async (): Promise<{ results: RaceWarmRow[] }> => ({ results: [] }));
const bindMock = vi.fn(() => ({ all: allMock }));
const prepareMock = vi.fn(() => ({ bind: bindMock }));

const makeEnv = (): Env => ({
  FEATURES_CACHE: {} as unknown as R2Bucket,
  FINISH_POSITION_CRON_DB: {} as unknown as D1Database,
  FINISH_POSITION_PREDICT_CONTAINER: {} as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
  NEON_DATABASE_URL: "postgres://example",
  PREDICT_DAYS_AHEAD: "2",
  PREDICT_QUEUE: {} as unknown as Env["PREDICT_QUEUE"],
  PREDICT_RUN_COORDINATOR: {} as unknown as Env["PREDICT_RUN_COORDINATOR"],
  REALTIME_DB: { prepare: prepareMock } as unknown as D1Database,
  TRIGGER_TOKEN: "secret-token",
});

interface FetchInit {
  signal: AbortSignal;
}

const fetchMock = vi.fn(
  async (_url: string, _init: FetchInit): Promise<Response> => new Response(null, { status: 200 }),
);

beforeEach(() => {
  fetchMock.mockClear();
  prepareMock.mockClear();
  bindMock.mockClear();
  allMock.mockClear();
  fetchMock.mockResolvedValue(new Response(null, { status: 200 }));
  allMock.mockResolvedValue({ results: [] });
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

test("warmPredictionCacheForRace builds the viewer section URL with refresh param", async () => {
  await warmPredictionCacheForRace({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    year: "2026",
  });
  expect(fetchMock).toHaveBeenCalledTimes(1);
  const fetchUrl = (fetchMock.mock.calls[0] as unknown as [string])[0];
  expect(fetchUrl).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/races/2026/06/19/05/11/sections/finish-prediction?__predictionRefresh=1",
  );
});

test("warmPredictionCacheForRace returns true on a 200 response", async () => {
  fetchMock.mockResolvedValue(new Response(null, { status: 200 }));
  const result = await warmPredictionCacheForRace({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    year: "2026",
  });
  expect(result).toBe(true);
});

test("warmPredictionCacheForRace returns false on a non-200 response", async () => {
  fetchMock.mockResolvedValue(new Response(null, { status: 500 }));
  const result = await warmPredictionCacheForRace({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    year: "2026",
  });
  expect(result).toBe(false);
});

test("warmPredictionCacheForRace returns false when fetch rejects", async () => {
  fetchMock.mockRejectedValue(new Error("network timeout"));
  const result = await warmPredictionCacheForRace({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    year: "2026",
  });
  expect(result).toBe(false);
});

test("warmPredictionCacheForRace aborts the fetch when the timeout elapses", async () => {
  vi.useFakeTimers();
  fetchMock.mockImplementation(
    (_url: string, init: FetchInit): Promise<Response> =>
      new Promise((_resolve, reject) => {
        init.signal.addEventListener("abort", () => reject(new Error("aborted")));
      }),
  );
  const pending = warmPredictionCacheForRace({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    year: "2026",
  });
  await vi.advanceTimersByTimeAsync(5000);
  const result = await pending;
  expect(result).toBe(false);
  vi.useRealTimers();
});

test("warmPredictionCacheForCategory queries D1 with the jra source and date parts", async () => {
  allMock.mockResolvedValue({ results: [{ keibajo_code: "5", race_bango: "1" }] });
  await warmPredictionCacheForCategory({
    category: "jra",
    env: makeEnv(),
    runDate: "2026-06-19",
    runYmd: "20260619",
  });
  expect(bindMock).toHaveBeenCalledWith("jra", "2026", "0619");
});

test("warmPredictionCacheForCategory uses the nar source for ban-ei", async () => {
  allMock.mockResolvedValue({ results: [] });
  await warmPredictionCacheForCategory({
    category: "ban-ei",
    env: makeEnv(),
    runDate: "2026-06-19",
    runYmd: "20260619",
  });
  expect(bindMock).toHaveBeenCalledWith("nar", "2026", "0619");
});

test("warmPredictionCacheForCategory warms each race with zero-padded codes", async () => {
  allMock.mockResolvedValue({
    results: [
      { keibajo_code: "5", race_bango: "1" },
      { keibajo_code: "10", race_bango: "12" },
    ],
  });
  const count = await warmPredictionCacheForCategory({
    category: "jra",
    env: makeEnv(),
    runDate: "2026-06-19",
    runYmd: "20260619",
  });
  expect(count).toBe(2);
  const firstUrl = (fetchMock.mock.calls[0] as unknown as [string])[0];
  const secondUrl = (fetchMock.mock.calls[1] as unknown as [string])[0];
  expect(firstUrl).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/races/2026/06/19/05/01/sections/finish-prediction?__predictionRefresh=1",
  );
  expect(secondUrl).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/races/2026/06/19/10/12/sections/finish-prediction?__predictionRefresh=1",
  );
});

test("warmPredictionCacheForCategory returns 0 when no races are found", async () => {
  allMock.mockResolvedValue({ results: [] });
  const count = await warmPredictionCacheForCategory({
    category: "nar",
    env: makeEnv(),
    runDate: "2026-06-19",
    runYmd: "20260619",
  });
  expect(count).toBe(0);
  expect(fetchMock).not.toHaveBeenCalled();
});

test("warmPredictionCacheForCategory counts only races that warmed successfully", async () => {
  allMock.mockResolvedValue({
    results: [
      { keibajo_code: "05", race_bango: "01" },
      { keibajo_code: "05", race_bango: "02" },
    ],
  });
  fetchMock.mockResolvedValueOnce(new Response(null, { status: 200 }));
  fetchMock.mockResolvedValueOnce(new Response(null, { status: 503 }));
  const count = await warmPredictionCacheForCategory({
    category: "jra",
    env: makeEnv(),
    runDate: "2026-06-19",
    runYmd: "20260619",
  });
  expect(count).toBe(1);
});
