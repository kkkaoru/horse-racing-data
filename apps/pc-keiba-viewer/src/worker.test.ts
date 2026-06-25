// Run with bun (vitest).
import { afterEach, beforeEach, expect, it, vi } from "vitest";

import type { DetailSectionCacheWarmMessage } from "./lib/race-detail-section-cache";
import type { ScheduleTodayRaceDetailSectionCacheParams } from "./worker/race-detail-section-cache-warm";

type FetchFn = (
  request: Request,
  env: CloudflareEnv,
  ctx: PcKeibaExecutionContext,
) => Promise<Response>;

type QueueFn = (
  batch: PcKeibaMessageBatch<DetailSectionCacheWarmMessage>,
  env: CloudflareEnv,
  ctx: PcKeibaExecutionContext,
) => Promise<void>;

type ScheduleTomorrowFn = (
  openNextWorker: unknown,
  env: CloudflareEnv,
  ctx: PcKeibaExecutionContext,
) => Promise<void>;

type ScheduleDueTrendFn = (
  openNextWorker: unknown,
  env: CloudflareEnv,
  ctx: PcKeibaExecutionContext,
) => Promise<void>;

type ScheduleSsrFn = (
  openNextWorker: unknown,
  env: CloudflareEnv,
  ctx: PcKeibaExecutionContext,
  options?: { date?: string },
) => Promise<void>;

type ScheduleTodayFn = (params: ScheduleTodayRaceDetailSectionCacheParams) => Promise<void>;

const { fetchMock } = vi.hoisted(() => ({
  fetchMock: vi.fn<FetchFn>(),
}));

vi.mock("../.open-next/worker.js", () => ({
  default: {
    fetch: fetchMock,
  },
}));

vi.mock("./worker/paddock-room", () => ({
  PaddockRoom: class FakePaddockRoom {
    public readonly name = "fake-paddock";
  },
}));

vi.mock("./worker/race-trend-room", () => ({
  RaceTrendRoom: class FakeRaceTrendRoom {
    public readonly name = "fake-race-trend";
  },
}));

const {
  handleRaceDetailSectionCacheQueueMock,
  scheduleDueRaceTrendCacheMock,
  scheduleRaceDetailSsrCacheWarmMock,
  scheduleTodayRaceDetailSectionCacheMock,
  scheduleTomorrowRaceDetailSectionCacheMock,
} = vi.hoisted(() => ({
  handleRaceDetailSectionCacheQueueMock: vi.fn<QueueFn>(),
  scheduleDueRaceTrendCacheMock: vi.fn<ScheduleDueTrendFn>(),
  scheduleRaceDetailSsrCacheWarmMock: vi.fn<ScheduleSsrFn>(),
  scheduleTodayRaceDetailSectionCacheMock: vi.fn<ScheduleTodayFn>(),
  scheduleTomorrowRaceDetailSectionCacheMock: vi.fn<ScheduleTomorrowFn>(),
}));

vi.mock("./worker/race-detail-section-cache-warm", () => ({
  handleRaceDetailSectionCacheQueue: handleRaceDetailSectionCacheQueueMock,
  scheduleDueRaceTrendCache: scheduleDueRaceTrendCacheMock,
  scheduleRaceDetailSsrCacheWarm: scheduleRaceDetailSsrCacheWarmMock,
  scheduleTodayRaceDetailSectionCache: scheduleTodayRaceDetailSectionCacheMock,
  scheduleTomorrowRaceDetailSectionCache: scheduleTomorrowRaceDetailSectionCacheMock,
}));

import worker from "./worker";

const buildEnv = (): CloudflareEnv => ({});

interface CtxStub {
  waitUntil: ReturnType<typeof vi.fn<(promise: Promise<unknown>) => void>>;
}

const buildCtx = (): CtxStub => ({
  waitUntil: vi.fn<(promise: Promise<unknown>) => void>(),
});

const emptyBatch = (): PcKeibaMessageBatch<DetailSectionCacheWarmMessage> => ({
  messages: [],
  queue: "pc-keiba-detail-section-cache-warm",
});

beforeEach(() => {
  vi.useFakeTimers();
  fetchMock.mockReset();
  handleRaceDetailSectionCacheQueueMock.mockReset();
  scheduleDueRaceTrendCacheMock.mockReset();
  scheduleRaceDetailSsrCacheWarmMock.mockReset();
  scheduleTodayRaceDetailSectionCacheMock.mockReset();
  scheduleTomorrowRaceDetailSectionCacheMock.mockReset();
  scheduleDueRaceTrendCacheMock.mockResolvedValue(undefined);
  scheduleRaceDetailSsrCacheWarmMock.mockResolvedValue(undefined);
  scheduleTodayRaceDetailSectionCacheMock.mockResolvedValue(undefined);
  scheduleTomorrowRaceDetailSectionCacheMock.mockResolvedValue(undefined);
});

afterEach(() => {
  vi.useRealTimers();
});

it("cron-0-12-utc-warms-tomorrow-sections-and-ssr-with-jst-date", () => {
  vi.setSystemTime(new Date("2026-05-31T12:00:00Z"));
  const env = buildEnv();
  const ctx = buildCtx();
  worker.scheduled({ cron: "0 12 * * *" }, env, ctx);
  expect(ctx.waitUntil).toHaveBeenCalledTimes(2);
  expect(scheduleTomorrowRaceDetailSectionCacheMock).toHaveBeenCalledTimes(1);
  expect(scheduleRaceDetailSsrCacheWarmMock).toHaveBeenCalledTimes(1);
  const ssrOptions = scheduleRaceDetailSsrCacheWarmMock.mock.calls[0]?.[3];
  expect(ssrOptions).toStrictEqual({ date: "2026-06-01" });
});

it("cron-0-21-utc-warms-today-sections-and-ssr-with-jst-date", () => {
  vi.setSystemTime(new Date("2026-05-31T21:00:00Z"));
  const env = buildEnv();
  const ctx = buildCtx();
  worker.scheduled({ cron: "0 21 * * *" }, env, ctx);
  expect(ctx.waitUntil).toHaveBeenCalledTimes(2);
  expect(scheduleTodayRaceDetailSectionCacheMock).toHaveBeenCalledTimes(1);
  const todayParams = scheduleTodayRaceDetailSectionCacheMock.mock.calls[0]?.[0];
  expect(todayParams?.todayJstYmd).toBe("2026-06-01");
  expect(scheduleRaceDetailSsrCacheWarmMock).toHaveBeenCalledTimes(1);
  const ssrOptions = scheduleRaceDetailSsrCacheWarmMock.mock.calls[0]?.[3];
  expect(ssrOptions).toStrictEqual({ date: "2026-06-01" });
});

it("cron-every-5-min-utc-warms-due-race-trend", () => {
  const env = buildEnv();
  const ctx = buildCtx();
  worker.scheduled({ cron: "*/5 0-14 * * *" }, env, ctx);
  expect(ctx.waitUntil).toHaveBeenCalledTimes(1);
  expect(scheduleDueRaceTrendCacheMock).toHaveBeenCalledTimes(1);
  expect(scheduleTomorrowRaceDetailSectionCacheMock).toHaveBeenCalledTimes(0);
  expect(scheduleTodayRaceDetailSectionCacheMock).toHaveBeenCalledTimes(0);
});

it("cron-every-15-min-utc-warms-ssr-cache-without-date", () => {
  const env = buildEnv();
  const ctx = buildCtx();
  worker.scheduled({ cron: "*/15 0-14 * * *" }, env, ctx);
  expect(ctx.waitUntil).toHaveBeenCalledTimes(1);
  expect(scheduleRaceDetailSsrCacheWarmMock).toHaveBeenCalledTimes(1);
  const ssrOptions = scheduleRaceDetailSsrCacheWarmMock.mock.calls[0]?.[3];
  expect(ssrOptions).toBeUndefined();
});

it("cron-unknown-schedule-does-not-warm-anything", () => {
  const env = buildEnv();
  const ctx = buildCtx();
  worker.scheduled({ cron: "0 0 1 1 *" }, env, ctx);
  expect(ctx.waitUntil).toHaveBeenCalledTimes(0);
  expect(scheduleTomorrowRaceDetailSectionCacheMock).toHaveBeenCalledTimes(0);
  expect(scheduleTodayRaceDetailSectionCacheMock).toHaveBeenCalledTimes(0);
  expect(scheduleRaceDetailSsrCacheWarmMock).toHaveBeenCalledTimes(0);
  expect(scheduleDueRaceTrendCacheMock).toHaveBeenCalledTimes(0);
});

it("queue-handler-delegates-to-cache-queue-worker", async () => {
  const env = buildEnv();
  const ctx = buildCtx();
  handleRaceDetailSectionCacheQueueMock.mockResolvedValue(undefined);
  await worker.queue(emptyBatch(), env, ctx);
  expect(handleRaceDetailSectionCacheQueueMock).toHaveBeenCalledTimes(1);
});

it("fetch-delegates-to-open-next-worker", async () => {
  fetchMock.mockResolvedValue(new Response("ok", { status: 200 }));
  const env = buildEnv();
  const ctx = buildCtx();
  const request = new Request("https://example.com/");
  const response = await worker.fetch(request, env, ctx);
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(response.status).toBe(200);
});
