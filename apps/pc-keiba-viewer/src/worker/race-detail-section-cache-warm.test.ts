// Run with bun (vitest).
import { expect, it, vi } from "vitest";

import {
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

it("schedule-ssr-warm-throws-on-non-ok-response", async () => {
  const worker = buildOpenNextWorker(new Response("nope", { status: 504 }));
  const env = buildEnv();
  const ctx = buildCtx();
  await expect(
    scheduleRaceDetailSsrCacheWarm(worker, env, ctx, { date: "2026-06-01" }),
  ).rejects.toThrowError("race detail SSR cache warm failed: 504");
});
