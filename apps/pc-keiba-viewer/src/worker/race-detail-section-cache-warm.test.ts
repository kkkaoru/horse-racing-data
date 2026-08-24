// Run with bun (vitest).
// @vitest-environment node
import { expect, it, vi } from "vitest";

import type { RaceTrendCacheWarmMessage } from "../lib/race-trend-cache";
import {
  handleRaceDetailSectionCacheQueue,
  scheduleDueRaceTrendCache,
  scheduleRaceDetailSsrCacheWarm,
  scheduleTodayRaceDetailSectionCache,
  scheduleTomorrowRaceDetailSectionCache,
} from "./race-detail-section-cache-warm";

type FetchFn = (
  request: Request,
  env: CloudflareEnv,
  ctx: PcKeibaExecutionContext,
) => Promise<Response>;

interface FakeWorker {
  fetch: ReturnType<typeof vi.fn<FetchFn>>;
}

const buildOpenNextWorker = (response: Response): FakeWorker => ({
  fetch: vi.fn<FetchFn>().mockResolvedValue(response),
});

const buildEnv = (): CloudflareEnv => ({});

const buildCtx = (): PcKeibaExecutionContext => ({
  waitUntil: vi.fn<(promise: Promise<unknown>) => void>(),
});

const getFirstRequest = (worker: FakeWorker): Request => {
  const calls = worker.fetch.mock.calls;
  if (!calls[0]?.[0]) {
    throw new Error("fetch was not called");
  }
  return calls[0][0];
};

it("schedule-today-posts-correct-url-with-date-query", async () => {
  const response = new Response("ok", { status: 200 });
  const worker = buildOpenNextWorker(response);
  const env = buildEnv();
  const ctx = buildCtx();
  await scheduleTodayRaceDetailSectionCache({
    ctx,
    env,
    openNextWorker: worker,
    todayJstYmd: "2026-06-01",
  });
  expect(worker.fetch).toHaveBeenCalledTimes(1);
  const request = getFirstRequest(worker);
  expect(request.url).toBe(
    "https://pc-keiba-viewer.local/api/cache-warm/race-detail-sections?date=2026-06-01",
  );
  expect(request.method).toBe("POST");
  expect(request.headers.get("X-PC-Keiba-Cache-Warm")).toBe("scheduled");
  expect(response.bodyUsed).toBe(true);
});

it("schedule-today-throws-on-non-ok-response", async () => {
  const worker = buildOpenNextWorker(new Response("nope", { status: 500 }));
  const env = buildEnv();
  const ctx = buildCtx();
  await expect(
    scheduleTodayRaceDetailSectionCache({
      ctx,
      env,
      openNextWorker: worker,
      todayJstYmd: "2026-06-01",
    }),
  ).rejects.toThrowError("race detail today cache schedule failed: 500");
});

it("schedule-tomorrow-posts-correct-url-without-date-query", async () => {
  const response = new Response("ok", { status: 200 });
  const worker = buildOpenNextWorker(response);
  const env = buildEnv();
  const ctx = buildCtx();
  await scheduleTomorrowRaceDetailSectionCache(worker, env, ctx);
  expect(worker.fetch).toHaveBeenCalledTimes(1);
  const request = getFirstRequest(worker);
  expect(request.url).toBe("https://pc-keiba-viewer.local/api/cache-warm/race-detail-sections");
  expect(request.method).toBe("POST");
  expect(request.headers.get("X-PC-Keiba-Cache-Warm")).toBe("scheduled");
  expect(response.bodyUsed).toBe(true);
});

it("schedule-tomorrow-throws-on-non-ok-response", async () => {
  const worker = buildOpenNextWorker(new Response("nope", { status: 502 }));
  const env = buildEnv();
  const ctx = buildCtx();
  await expect(scheduleTomorrowRaceDetailSectionCache(worker, env, ctx)).rejects.toThrowError(
    "race detail cache schedule failed: 502",
  );
});

it("schedule-due-race-trend-posts-to-trend-endpoint", async () => {
  const response = new Response("ok", { status: 200 });
  const worker = buildOpenNextWorker(response);
  const env = buildEnv();
  const ctx = buildCtx();
  await scheduleDueRaceTrendCache(worker, env, ctx);
  expect(worker.fetch).toHaveBeenCalledTimes(1);
  const request = getFirstRequest(worker);
  expect(request.url).toBe("https://pc-keiba-viewer.local/api/cache-warm/race-trends");
  expect(request.method).toBe("POST");
  expect(response.bodyUsed).toBe(true);
});

it("schedule-due-race-trend-throws-on-non-ok-response", async () => {
  const worker = buildOpenNextWorker(new Response("nope", { status: 503 }));
  const env = buildEnv();
  const ctx = buildCtx();
  await expect(scheduleDueRaceTrendCache(worker, env, ctx)).rejects.toThrowError(
    "race trend cache schedule failed: 503",
  );
});

it("schedule-ssr-warm-without-date-omits-query", async () => {
  const response = new Response("ok", { status: 200 });
  const worker = buildOpenNextWorker(response);
  const env = buildEnv();
  const ctx = buildCtx();
  await scheduleRaceDetailSsrCacheWarm(worker, env, ctx);
  expect(worker.fetch).toHaveBeenCalledTimes(1);
  const request = getFirstRequest(worker);
  expect(request.url).toBe("https://pc-keiba-viewer.local/api/cache-warm/race-detail-ssr");
  expect(response.bodyUsed).toBe(true);
});

it("schedule-ssr-warm-with-date-adds-query", async () => {
  const worker = buildOpenNextWorker(new Response("ok", { status: 200 }));
  const env = buildEnv();
  const ctx = buildCtx();
  await scheduleRaceDetailSsrCacheWarm(worker, env, ctx, { date: "2026-06-01" });
  expect(worker.fetch).toHaveBeenCalledTimes(1);
  const request = getFirstRequest(worker);
  expect(request.url).toBe(
    "https://pc-keiba-viewer.local/api/cache-warm/race-detail-ssr?date=2026-06-01",
  );
});

it("warms cheaper sections before heatmap and retries unstored heatmap responses", async () => {
  const worker = {
    fetch: vi
      .fn<FetchFn>()
      .mockResolvedValueOnce(new Response("ok", { status: 200 }))
      .mockResolvedValueOnce(
        new Response("{}", {
          headers: { "X-Win-Rate-Heatmap-Cache": "MISS" },
          status: 200,
        }),
      )
      .mockResolvedValueOnce(
        new Response("{}", {
          headers: { "X-Win-Rate-Heatmap-Cache": "MISS-STORED" },
          status: 200,
        }),
      ),
  };
  const resultsMessage = {
    ack: vi.fn<() => void>(),
    retry: vi.fn<() => void>(),
    body: {
      day: "22",
      keibajoCode: "07",
      month: "08",
      raceNumber: "10",
      section: "results" as const,
      source: "jra" as const,
      year: "2026",
    },
  };
  const heatmapMessage = {
    ack: vi.fn<() => void>(),
    retry: vi.fn<() => void>(),
    body: {
      day: "22",
      keibajoCode: "07",
      month: "08",
      raceNumber: "10",
      section: "win-rate-heatmap" as const,
      source: "jra" as const,
      year: "2026",
    },
  };
  const env = buildEnv();
  const ctx = buildCtx();
  const consoleMock = vi.spyOn(console, "error").mockImplementation(() => undefined);
  await handleRaceDetailSectionCacheQueue(
    worker,
    { messages: [heatmapMessage, resultsMessage], queue: "pc-keiba-detail-section-cache-warm" },
    env,
    ctx,
  );
  const firstUrl = worker.fetch.mock.calls[0]?.[0];
  expect(firstUrl).toBeInstanceOf(Request);
  if (!(firstUrl instanceof Request)) throw new Error("Request expected");
  expect(new URL(firstUrl.url).pathname).toBe("/api/races/2026/08/22/07/10/sections/results");
  expect(resultsMessage.ack).toHaveBeenCalledTimes(1);
  expect(heatmapMessage.retry).toHaveBeenCalledTimes(1);
  expect(heatmapMessage.ack).not.toHaveBeenCalled();
  expect(consoleMock).toHaveBeenCalledTimes(1);
  await handleRaceDetailSectionCacheQueue(
    worker,
    { messages: [heatmapMessage], queue: "pc-keiba-detail-section-cache-warm" },
    env,
    ctx,
  );
  expect(heatmapMessage.ack).toHaveBeenCalledTimes(1);
});

it("schedule-ssr-warm-throws-on-non-ok-response", async () => {
  const worker = buildOpenNextWorker(new Response("nope", { status: 504 }));
  const env = buildEnv();
  const ctx = buildCtx();
  await expect(
    scheduleRaceDetailSsrCacheWarm(worker, env, ctx, { date: "2026-06-01" }),
  ).rejects.toThrowError("race detail SSR cache warm failed: 504");
});

it("warms a trend once, drains the response, and records its generation", async () => {
  const response = new Response('{"ok":true}', { status: 200 });
  const worker = buildOpenNextWorker(response);
  const get = vi.fn<(key: string) => Promise<string | null>>();
  get.mockResolvedValueOnce("2").mockResolvedValueOnce(null).mockResolvedValueOnce("2");
  const put = vi
    .fn<(key: string, value: string, options?: { expirationTtl?: number }) => Promise<void>>()
    .mockResolvedValue(undefined);
  const message = {
    ack: vi.fn<() => void>(),
    retry: vi.fn<() => void>(),
    body: {
      cacheGeneration: "2",
      day: "24",
      kind: "race-trend",
      keibajoCode: "35",
      month: "08",
      options: {
        frameEndYmd: "20260824",
        frameStartYmd: "20260810",
        includeRealtimeResults: true,
        jockeyEndYmd: "20260824",
        jockeyStartYmd: "20260810",
        source: "nar",
      },
      raceNumber: "03",
      source: "nar",
      year: "2026",
    },
  } satisfies PcKeibaMessage<RaceTrendCacheWarmMessage>;
  const env: CloudflareEnv = {
    DETAIL_SECTION_CACHE_KV: {
      delete: vi.fn<(key: string) => Promise<void>>(),
      get,
      list: vi
        .fn<() => Promise<PcKeibaKvListResult>>()
        .mockResolvedValue({ keys: [], list_complete: true }),
      put,
    },
  };
  await handleRaceDetailSectionCacheQueue(
    worker,
    { messages: [message], queue: "pc-keiba-detail-section-cache-warm" },
    env,
    buildCtx(),
  );
  expect(message.ack).toHaveBeenCalledTimes(1);
  expect(message.retry).not.toHaveBeenCalled();
  expect(response.bodyUsed).toBe(true);
  expect(put).toHaveBeenCalledTimes(1);
});

it("acks an already valid trend generation without recomputing it", async () => {
  const worker = buildOpenNextWorker(new Response("unused", { status: 200 }));
  const get = vi.fn<(key: string) => Promise<string | null>>();
  get.mockResolvedValueOnce("2").mockResolvedValueOnce("2");
  const message = {
    ack: vi.fn<() => void>(),
    retry: vi.fn<() => void>(),
    body: {
      cacheGeneration: "2",
      day: "24",
      kind: "race-trend",
      keibajoCode: "35",
      month: "08",
      options: {
        frameEndYmd: "20260824",
        frameStartYmd: "20260810",
        includeRealtimeResults: true,
        jockeyEndYmd: "20260824",
        jockeyStartYmd: "20260810",
        source: "nar",
      },
      raceNumber: "03",
      source: "nar",
      year: "2026",
    },
  } satisfies PcKeibaMessage<RaceTrendCacheWarmMessage>;
  const env: CloudflareEnv = {
    DETAIL_SECTION_CACHE_KV: {
      delete: vi.fn<(key: string) => Promise<void>>(),
      get,
      list: vi
        .fn<() => Promise<PcKeibaKvListResult>>()
        .mockResolvedValue({ keys: [], list_complete: true }),
      put: vi.fn<(key: string, value: string) => Promise<void>>(),
    },
  };
  await handleRaceDetailSectionCacheQueue(
    worker,
    { messages: [message], queue: "pc-keiba-detail-section-cache-warm" },
    env,
    buildCtx(),
  );
  expect(message.ack).toHaveBeenCalledTimes(1);
  expect(message.retry).not.toHaveBeenCalled();
  expect(worker.fetch).not.toHaveBeenCalled();
});
