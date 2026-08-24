// Run with bun. Tests for the viewer prediction cache warming module.

import { afterEach, beforeEach, expect, test, vi } from "vitest";
import type { PredictionKvPublishResult } from "./prediction-kv-writer";

const { publishFinishPositionPredictionCacheMock } = vi.hoisted(() => ({
  publishFinishPositionPredictionCacheMock: vi.fn(
    async (): Promise<PredictionKvPublishResult> => ({
      busted: true,
      expectedGeneratedAt: "2026-08-17T03:04:05.678Z",
      status: "written",
    }),
  ),
}));

vi.mock("./prediction-kv-writer", () => ({
  publishFinishPositionPredictionCache: publishFinishPositionPredictionCacheMock,
}));

import {
  buildWarmRaceParamsFromYmd,
  populateViewerDisplayCache,
  retryPopulateViewerDisplayCache,
  warmPredictionCacheForCategory,
  warmPredictionCacheForRace,
  warmRaceDetailPage,
  warmRaceDetailSsrSnapshot,
  warmViewerDisplayForRace,
} from "./prediction-cache-warm";
import type { Env } from "./types";

interface RaceWarmRow {
  keibajo_code: string;
  race_bango: string;
}

const allMock = vi.fn(async (): Promise<{ results: RaceWarmRow[] }> => ({ results: [] }));
const bindMock = vi.fn(() => ({ all: allMock }));
const prepareMock = vi.fn(() => ({ bind: bindMock }));

const makeEnv = (overrides: Partial<Env> = {}): Env =>
  ({
    FEATURES_CACHE: {} as unknown as R2Bucket,
    FINISH_POSITION_CRON_DB: {} as unknown as D1Database,
    FINISH_POSITION_PREDICT_CONTAINER: {} as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
    NEON_DATABASE_URL: "postgres://example",
    PC_KEIBA_VIEWER_INTERNAL_TOKEN: "secret-token",
    PREDICT_DAYS_AHEAD: "2",
    PREDICT_QUEUE: {} as unknown as Env["PREDICT_QUEUE"],
    PREDICT_RUN_COORDINATOR: {} as unknown as Env["PREDICT_RUN_COORDINATOR"],
    REALTIME_DB: { prepare: prepareMock } as unknown as D1Database,
    TRIGGER_TOKEN: "secret-token",
    ...overrides,
  }) satisfies Env;

interface FetchInit {
  headers?: Readonly<Record<string, string>>;
  method?: string;
  signal: AbortSignal;
}

const fetchMock = vi.fn(
  async (_url: string, _init: FetchInit): Promise<Response> => new Response(null, { status: 200 }),
);

beforeEach(() => {
  fetchMock.mockReset();
  prepareMock.mockClear();
  bindMock.mockClear();
  allMock.mockClear();
  publishFinishPositionPredictionCacheMock.mockClear();
  fetchMock.mockResolvedValue(new Response(null, { status: 200 }));
  allMock.mockResolvedValue({ results: [] });
  publishFinishPositionPredictionCacheMock.mockResolvedValue({
    busted: true,
    expectedGeneratedAt: "2026-08-17T03:04:05.678Z",
    status: "written",
  });
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

test("warmPredictionCacheForRace builds the viewer section URL without refresh by default", async () => {
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
    "https://pc-keiba-viewer.kkk4oru.com/api/races/2026/06/19/05/11/sections/finish-prediction",
  );
});

test("warmPredictionCacheForRace appends refresh when requested", async () => {
  await warmPredictionCacheForRace({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    refresh: true,
    year: "2026",
  });
  const fetchUrl = (fetchMock.mock.calls[0] as unknown as [string])[0];
  expect(fetchUrl).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/races/2026/06/19/05/11/sections/finish-prediction?__predictionRefresh=1",
  );
});

test("warmPredictionCacheForRace binds expected generation and internal authentication", async () => {
  expect(
    await warmPredictionCacheForRace({
      day: "19",
      expectedGeneratedAt: "2026-06-19T03:04:05.678Z",
      internalToken: " secret-token ",
      keibajoCode: "05",
      month: "06",
      raceNumber: "11",
      refresh: true,
      year: "2026",
    }),
  ).toBe(true);
  expect((fetchMock.mock.calls[0] as unknown as [string])[0]).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/races/2026/06/19/05/11/sections/finish-prediction?__predictionRefresh=1&expectedPredictionGeneratedAt=2026-06-19T03%3A04%3A05.678Z",
  );
  expect((fetchMock.mock.calls[0] as unknown as [string, FetchInit])[1].method).toBe("GET");
  expect((fetchMock.mock.calls[0] as unknown as [string, FetchInit])[1].headers).toStrictEqual({
    "x-pc-keiba-internal-token": "secret-token",
  });
});

test("warmPredictionCacheForRace fails closed without an internal token for an expected generation", async () => {
  expect(
    await warmPredictionCacheForRace({
      day: "19",
      expectedGeneratedAt: "2026-06-19T03:04:05.678Z",
      internalToken: "   ",
      keibajoCode: "05",
      month: "06",
      raceNumber: "11",
      refresh: true,
      year: "2026",
    }),
  ).toBe(false);
  expect(fetchMock).not.toHaveBeenCalled();
});

test("warmViewerDisplayForRace prefers the viewer service binding", async () => {
  const globalFetchSpy = vi.spyOn(globalThis, "fetch");
  const serviceFetch = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 200 }));
  const result = await warmViewerDisplayForRace({
    day: "23",
    keibajoCode: "83",
    month: "08",
    raceNumber: "12",
    refresh: true,
    viewer: { fetch: serviceFetch },
    year: "2026",
  });

  expect(result).toBe(true);
  expect(globalFetchSpy).not.toHaveBeenCalled();
  expect(serviceFetch).toHaveBeenCalledTimes(3);
  expect(serviceFetch.mock.calls.map(([url]) => url)).toStrictEqual([
    "https://pc-keiba-viewer.kkk4oru.com/api/races/2026/08/23/83/12/sections/finish-prediction?__predictionRefresh=1",
    "https://pc-keiba-viewer.kkk4oru.com/api/cache-warm/race-detail-ssr?date=2026-08-23&keibajo=83&race=12",
    "https://pc-keiba-viewer.kkk4oru.com/races/2026/08/23/83/12",
  ]);
  expect(serviceFetch.mock.calls[1]?.[1]?.method).toBe("POST");
  expect(new Headers(serviceFetch.mock.calls[1]?.[1]?.headers).get("X-PC-Keiba-Cache-Warm")).toBe(
    "scheduled",
  );
});

test("warmRaceDetailPage fetches the public race detail path", async () => {
  await warmRaceDetailPage({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    year: "2026",
  });
  const fetchUrl = (fetchMock.mock.calls[0] as unknown as [string])[0];
  expect(fetchUrl).toBe("https://pc-keiba-viewer.kkk4oru.com/races/2026/06/19/05/11");
});

test("warmRaceDetailPage returns true on a 200 response", async () => {
  const response = new Response("warmed", { status: 200 });
  fetchMock.mockResolvedValue(response);
  const result = await warmRaceDetailPage({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    year: "2026",
  });
  expect(result).toBe(true);
  expect(response.bodyUsed).toBe(true);
});

test("warmRaceDetailPage returns false when the response stream fails", async () => {
  const response = new Response(
    new ReadableStream<Uint8Array>({
      start(controller) {
        controller.error(new Error("response stream failed"));
      },
    }),
    { status: 200 },
  );
  fetchMock.mockResolvedValue(response);
  const result = await warmRaceDetailPage({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    year: "2026",
  });
  expect(result).toBe(false);
  expect(response.bodyUsed).toBe(true);
});

test("warmRaceDetailPage returns false and cancels an oversized response", async () => {
  const response = new Response(new Uint8Array(1024 * 1024 + 1), { status: 200 });
  fetchMock.mockResolvedValue(response);
  const result = await warmRaceDetailPage({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    year: "2026",
  });
  expect(result).toBe(false);
  expect(response.bodyUsed).toBe(true);
});

test("warmRaceDetailPage returns false on a non-200 response", async () => {
  fetchMock.mockResolvedValue(new Response(null, { status: 503 }));
  const result = await warmRaceDetailPage({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    year: "2026",
  });
  expect(result).toBe(false);
});

test("warmRaceDetailPage returns false when fetch rejects", async () => {
  fetchMock.mockRejectedValue(new Error("network timeout"));
  const result = await warmRaceDetailPage({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    year: "2026",
  });
  expect(result).toBe(false);
});

test("warmRaceDetailSsrSnapshot posts the single-race cache-warm URL", async () => {
  await warmRaceDetailSsrSnapshot({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    year: "2026",
  });
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect((fetchMock.mock.calls[0] as unknown as [string])[0]).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/cache-warm/race-detail-ssr?date=2026-06-19&keibajo=05&race=11",
  );
  expect((fetchMock.mock.calls[0] as unknown as [string, FetchInit])[1].method).toBe("POST");
  expect((fetchMock.mock.calls[0] as unknown as [string, FetchInit])[1].headers).toStrictEqual({
    "X-PC-Keiba-Cache-Warm": "scheduled",
  });
});

test("warmRaceDetailSsrSnapshot returns true on a 200 response", async () => {
  fetchMock.mockResolvedValue(new Response(null, { status: 200 }));
  const result = await warmRaceDetailSsrSnapshot({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    year: "2026",
  });
  expect(result).toBe(true);
});

test("warmRaceDetailSsrSnapshot returns false on a non-200 response", async () => {
  fetchMock.mockResolvedValue(new Response(null, { status: 404 }));
  const result = await warmRaceDetailSsrSnapshot({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    year: "2026",
  });
  expect(result).toBe(false);
});

test("warmRaceDetailSsrSnapshot returns false when fetch rejects", async () => {
  fetchMock.mockRejectedValue(new Error("network timeout"));
  const result = await warmRaceDetailSsrSnapshot({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    year: "2026",
  });
  expect(result).toBe(false);
});

test("warmViewerDisplayForRace warms the section, SSR, and detail page sequentially", async () => {
  const sectionGate = Promise.withResolvers<Response>();
  fetchMock
    .mockReturnValueOnce(sectionGate.promise)
    .mockResolvedValueOnce(new Response(null, { status: 200 }))
    .mockResolvedValueOnce(new Response(null, { status: 200 }));
  const running = warmViewerDisplayForRace({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    year: "2026",
  });
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect((fetchMock.mock.calls[0] as unknown as [string])[0]).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/races/2026/06/19/05/11/sections/finish-prediction",
  );
  sectionGate.resolve(new Response(null, { status: 200 }));
  expect(await running).toBe(true);
  expect(fetchMock).toHaveBeenCalledTimes(3);
  expect((fetchMock.mock.calls[1] as unknown as [string])[0]).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/cache-warm/race-detail-ssr?date=2026-06-19&keibajo=05&race=11",
  );
  expect((fetchMock.mock.calls[1] as unknown as [string, FetchInit])[1].method).toBe("POST");
  expect((fetchMock.mock.calls[1] as unknown as [string, FetchInit])[1].headers).toStrictEqual({
    "X-PC-Keiba-Cache-Warm": "scheduled",
  });
  expect((fetchMock.mock.calls[2] as unknown as [string])[0]).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/races/2026/06/19/05/11",
  );
});

test("warmViewerDisplayForRace returns false when the section warm fails", async () => {
  fetchMock.mockResolvedValueOnce(new Response(null, { status: 500 }));
  const result = await warmViewerDisplayForRace({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    year: "2026",
  });
  expect(result).toBe(false);
  expect(fetchMock).toHaveBeenCalledTimes(1);
});

test("warmViewerDisplayForRace returns false when the SSR snapshot warm fails", async () => {
  fetchMock.mockResolvedValueOnce(new Response(null, { status: 200 }));
  fetchMock.mockResolvedValueOnce(new Response(null, { status: 503 }));
  const result = await warmViewerDisplayForRace({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    year: "2026",
  });
  expect(result).toBe(false);
  expect(fetchMock).toHaveBeenCalledTimes(2);
});

test("warmViewerDisplayForRace returns false when the detail page warm fails", async () => {
  fetchMock.mockResolvedValueOnce(new Response(null, { status: 200 }));
  fetchMock.mockResolvedValueOnce(new Response(null, { status: 200 }));
  fetchMock.mockResolvedValueOnce(new Response(null, { status: 503 }));
  const result = await warmViewerDisplayForRace({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    year: "2026",
  });
  expect(result).toBe(false);
});

test("buildWarmRaceParamsFromYmd splits YYYYMMDD and pads codes", () => {
  expect(buildWarmRaceParamsFromYmd("20260817", "5", "1")).toStrictEqual({
    day: "17",
    keibajoCode: "05",
    month: "08",
    raceNumber: "01",
    year: "2026",
  });
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
  await vi.advanceTimersByTimeAsync(20_000);
  const result = await pending;
  expect(result).toBe(false);
  vi.useRealTimers();
});

test("warmPredictionCacheForRace keeps the timeout active while draining the response", async () => {
  vi.useFakeTimers();
  fetchMock.mockImplementation(
    (_url: string, init: FetchInit): Promise<Response> =>
      Promise.resolve(
        new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(new TextEncoder().encode("partial"));
              init.signal.addEventListener("abort", () => {
                controller.error(new Error("aborted while draining"));
              });
            },
          }),
          { status: 200 },
        ),
      ),
  );
  const pending = warmPredictionCacheForRace({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    year: "2026",
  });
  await vi.advanceTimersByTimeAsync(20_000);
  expect(await pending).toBe(false);
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
  expect(bindMock).toHaveBeenCalledWith("nar", "2026", "0619", "83");
});

test("warmPredictionCacheForCategory excludes the ban-ei keibajo code for normal nar", async () => {
  allMock.mockResolvedValue({ results: [] });
  await warmPredictionCacheForCategory({
    category: "nar",
    env: makeEnv(),
    runDate: "2026-06-19",
    runYmd: "20260619",
  });
  expect(prepareMock).toHaveBeenCalledWith(expect.stringContaining("keibajo_code not in (?)"));
  expect(bindMock).toHaveBeenCalledWith("nar", "2026", "0619", "83");
});

test("warmPredictionCacheForCategory includes only the ban-ei keibajo code for ban-ei", async () => {
  allMock.mockResolvedValue({ results: [] });
  await warmPredictionCacheForCategory({
    category: "ban-ei",
    env: makeEnv(),
    runDate: "2026-06-19",
    runYmd: "20260619",
  });
  expect(prepareMock).toHaveBeenCalledWith(expect.stringContaining("keibajo_code in (?)"));
  expect(bindMock).toHaveBeenCalledWith("nar", "2026", "0619", "83");
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
    "https://pc-keiba-viewer.kkk4oru.com/api/races/2026/06/19/05/01/sections/finish-prediction",
  );
  expect(secondUrl).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/races/2026/06/19/10/12/sections/finish-prediction",
  );
});

test("warmPredictionCacheForCategory skips an overseas A8 venue on the jra source", async () => {
  allMock.mockResolvedValue({
    results: [
      { keibajo_code: "05", race_bango: "11" },
      { keibajo_code: "A8", race_bango: "04" },
    ],
  });
  const count = await warmPredictionCacheForCategory({
    category: "jra",
    env: makeEnv(),
    runDate: "2026-06-19",
    runYmd: "20260619",
  });
  expect(count).toBe(1);
  expect(fetchMock).toHaveBeenCalledTimes(1);
  const warmedUrl = (fetchMock.mock.calls[0] as unknown as [string])[0];
  expect(warmedUrl).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/races/2026/06/19/05/11/sections/finish-prediction",
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

test("populateViewerDisplayCache writes KV then warms section, SSR snapshot, and page", async () => {
  publishFinishPositionPredictionCacheMock.mockResolvedValueOnce({
    busted: true,
    expectedGeneratedAt: "2026-08-17T03:04:05.678Z",
    status: "written",
  });
  const ok = await populateViewerDisplayCache({
    category: "nar",
    env: makeEnv(),
    keibajoCode: "35",
    raceBango: "02",
    runYmd: "20260817",
  });
  expect(ok).toBe(true);
  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledWith({
    bustCacheApi: true,
    category: "nar",
    env: expect.any(Object),
    keibajoCode: "35",
    raceBango: "02",
    runYmd: "20260817",
  });
  expect(fetchMock).toHaveBeenCalledTimes(3);
  expect((fetchMock.mock.calls[0] as unknown as [string])[0]).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/races/2026/08/17/35/02/sections/finish-prediction?__predictionRefresh=1&expectedPredictionGeneratedAt=2026-08-17T03%3A04%3A05.678Z",
  );
  expect((fetchMock.mock.calls[1] as unknown as [string])[0]).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/cache-warm/race-detail-ssr?date=2026-08-17&keibajo=35&race=02",
  );
  expect((fetchMock.mock.calls[2] as unknown as [string])[0]).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/races/2026/08/17/35/02",
  );
});

test("populateViewerDisplayCache forces a prediction refresh when cache bust is unavailable", async () => {
  publishFinishPositionPredictionCacheMock.mockResolvedValueOnce({
    busted: false,
    expectedGeneratedAt: "2026-08-17T03:04:05.678Z",
    status: "written",
  });
  const ok = await populateViewerDisplayCache({
    category: "jra",
    env: makeEnv(),
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260817",
  });
  expect(ok).toBe(true);
  expect((fetchMock.mock.calls[0] as unknown as [string])[0]).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/races/2026/08/17/05/11/sections/finish-prediction?__predictionRefresh=1&expectedPredictionGeneratedAt=2026-08-17T03%3A04%3A05.678Z",
  );
});

test("populateViewerDisplayCache returns false when the viewer warm fails after KV write", async () => {
  publishFinishPositionPredictionCacheMock.mockResolvedValueOnce({
    busted: false,
    expectedGeneratedAt: "2026-08-17T03:04:05.678Z",
    status: "written",
  });
  fetchMock.mockResolvedValueOnce(new Response(null, { status: 200 }));
  fetchMock.mockResolvedValueOnce(new Response(null, { status: 503 }));
  const ok = await populateViewerDisplayCache({
    category: "jra",
    env: makeEnv(),
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260817",
  });
  expect(ok).toBe(false);
  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledTimes(1);
});

test("populateViewerDisplayCache returns false for a non-ymd date", async () => {
  const ok = await populateViewerDisplayCache({
    category: "nar",
    env: makeEnv(),
    keibajoCode: "35",
    raceBango: "02",
    runYmd: "2026-08-17",
  });
  expect(ok).toBe(false);
  expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
});

test("populateViewerDisplayCache returns false when KV publish is empty", async () => {
  publishFinishPositionPredictionCacheMock.mockResolvedValueOnce({
    busted: false,
    expectedGeneratedAt: null,
    status: "skipped-empty",
  });
  const ok = await populateViewerDisplayCache({
    category: "nar",
    env: makeEnv(),
    keibajoCode: "35",
    raceBango: "02",
    runYmd: "20260817",
  });
  expect(ok).toBe(false);
  expect(fetchMock).not.toHaveBeenCalled();
});

test("populateViewerDisplayCache fails closed without a verified generation", async () => {
  publishFinishPositionPredictionCacheMock.mockResolvedValueOnce({
    busted: false,
    expectedGeneratedAt: null,
    status: "written",
  });
  expect(
    await populateViewerDisplayCache({
      category: "nar",
      env: makeEnv(),
      keibajoCode: "35",
      raceBango: "02",
      runYmd: "20260817",
    }),
  ).toBe(false);
  expect(fetchMock).not.toHaveBeenCalled();
});

test("populateViewerDisplayCache fails closed without the Viewer internal token", async () => {
  publishFinishPositionPredictionCacheMock.mockResolvedValueOnce({
    busted: false,
    expectedGeneratedAt: "2026-08-17T03:04:05.678Z",
    status: "written",
  });
  expect(
    await populateViewerDisplayCache({
      category: "nar",
      env: makeEnv({ PC_KEIBA_VIEWER_INTERNAL_TOKEN: "   " }),
      keibajoCode: "35",
      raceBango: "02",
      runYmd: "20260817",
    }),
  ).toBe(false);
  expect(fetchMock).not.toHaveBeenCalled();
});

test("retryPopulateViewerDisplayCache retries until KV write succeeds", async () => {
  vi.useFakeTimers();
  publishFinishPositionPredictionCacheMock.mockResolvedValueOnce({
    busted: false,
    expectedGeneratedAt: null,
    status: "skipped-empty",
  });
  publishFinishPositionPredictionCacheMock.mockResolvedValueOnce({
    busted: false,
    expectedGeneratedAt: "2026-08-17T03:04:05.678Z",
    status: "written",
  });
  const pending = retryPopulateViewerDisplayCache({
    category: "ban-ei",
    env: makeEnv(),
    keibajoCode: "83",
    raceBango: "04",
    runYmd: "20260817",
  });
  await vi.advanceTimersByTimeAsync(10_000);
  const ok = await pending;
  expect(ok).toBe(true);
  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledTimes(2);
  vi.useRealTimers();
});

test("retryPopulateViewerDisplayCache returns true on the first successful attempt", async () => {
  publishFinishPositionPredictionCacheMock.mockResolvedValueOnce({
    busted: false,
    expectedGeneratedAt: "2026-08-17T03:04:05.678Z",
    status: "written",
  });
  const ok = await retryPopulateViewerDisplayCache({
    category: "jra",
    env: makeEnv(),
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260817",
  });
  expect(ok).toBe(true);
  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledTimes(1);
});

test("retryPopulateViewerDisplayCache returns false after all attempts fail", async () => {
  vi.useFakeTimers();
  publishFinishPositionPredictionCacheMock.mockResolvedValue({
    busted: false,
    expectedGeneratedAt: null,
    status: "skipped-empty",
  });
  const pending = retryPopulateViewerDisplayCache({
    category: "nar",
    env: makeEnv(),
    keibajoCode: "35",
    raceBango: "02",
    runYmd: "20260817",
  });
  await vi.advanceTimersByTimeAsync(70_000);
  const ok = await pending;
  expect(ok).toBe(false);
  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledTimes(8);
  vi.useRealTimers();
});
