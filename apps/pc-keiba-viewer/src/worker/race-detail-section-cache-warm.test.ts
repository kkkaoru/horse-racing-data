// Run with bun (vitest).
import { expect, it, vi } from "vitest";

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
  const worker = buildOpenNextWorker(new Response("ok", { status: 200 }));
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
  const worker = buildOpenNextWorker(new Response("ok", { status: 200 }));
  const env = buildEnv();
  const ctx = buildCtx();
  await scheduleTomorrowRaceDetailSectionCache(worker, env, ctx);
  expect(worker.fetch).toHaveBeenCalledTimes(1);
  const request = getFirstRequest(worker);
  expect(request.url).toBe("https://pc-keiba-viewer.local/api/cache-warm/race-detail-sections");
  expect(request.method).toBe("POST");
  expect(request.headers.get("X-PC-Keiba-Cache-Warm")).toBe("scheduled");
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
  const worker = buildOpenNextWorker(new Response("ok", { status: 200 }));
  const env = buildEnv();
  const ctx = buildCtx();
  await scheduleDueRaceTrendCache(worker, env, ctx);
  expect(worker.fetch).toHaveBeenCalledTimes(1);
  const request = getFirstRequest(worker);
  expect(request.url).toBe("https://pc-keiba-viewer.local/api/cache-warm/race-trends");
  expect(request.method).toBe("POST");
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
  const worker = buildOpenNextWorker(new Response("ok", { status: 200 }));
  const env = buildEnv();
  const ctx = buildCtx();
  await scheduleRaceDetailSsrCacheWarm(worker, env, ctx);
  expect(worker.fetch).toHaveBeenCalledTimes(1);
  const request = getFirstRequest(worker);
  expect(request.url).toBe("https://pc-keiba-viewer.local/api/cache-warm/race-detail-ssr");
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
