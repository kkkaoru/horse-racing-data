// Run with: bun run --filter pipeline-health-monitor test
import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("./queue-health-client", () => ({
  fetchQueueHealth: vi.fn(),
}));

vi.mock("./finish-position-client", () => ({
  fetchDeliveryCanaries: vi.fn(),
  fetchPredictionReadiness: vi.fn(),
}));

vi.mock("./incident-engine", () => ({
  processIncidentSignal: vi.fn(async () => undefined),
  sendDailyMonitorHeartbeat: vi.fn(async () => undefined),
}));

import { fetchDeliveryCanaries, fetchPredictionReadiness } from "./finish-position-client";
import { processIncidentSignal, sendDailyMonitorHeartbeat } from "./incident-engine";
import { fetchQueueHealth } from "./queue-health-client";
import { isQuarterHourTick, runScheduled } from "./scheduled-handler";
import type { AlertMessage, Env, QueueHealthMetrics } from "./types";

interface KvState {
  store: Map<string, string>;
  send: ReturnType<typeof vi.fn>;
}

const buildKvState = (preload: Record<string, string> = {}): KvState => {
  const store = new Map<string, string>(Object.entries(preload));
  return { store, send: vi.fn(async () => undefined) };
};

const buildEnv = (state: KvState): Env =>
  ({
    REALTIME: { fetch: vi.fn() },
    REALTIME_ADMIN_TOKEN: "test-token",
    ALERT_QUEUE: { send: state.send },
    STATE_KV: {
      get: vi.fn(async (key: string) => state.store.get(key) ?? null),
      put: vi.fn(async (key: string, value: string) => {
        state.store.set(key, value);
      }),
      delete: vi.fn(async (key: string) => {
        state.store.delete(key);
      }),
    },
  }) as unknown as Env;

// 2026-06-28 15:00 JST = 06:00 UTC. Inside both staleness windows.
const ON_WINDOW_NOW = new Date("2026-06-28T06:00:00Z");

const HEALTHY_METRICS: QueueHealthMetrics = {
  lastSuccessfulFetchResultsAt: "2026-06-28T05:55:00Z",
  lastSuccessfulFetchWeightsAt: "2026-06-28T05:55:00Z",
  racesQueuedNotFetchedToday: 0,
  racesStuckOverThirtyMin: 0,
};

const FAILING_RESULTS_METRICS: QueueHealthMetrics = {
  lastSuccessfulFetchResultsAt: "2026-06-28T04:00:00Z",
  lastSuccessfulFetchWeightsAt: "2026-06-28T05:55:00Z",
  racesQueuedNotFetchedToday: 0,
  racesStuckOverThirtyMin: 0,
};

beforeEach(() => {
  vi.mocked(fetchDeliveryCanaries).mockResolvedValue({
    canaries: [
      {
        consumedAt: ON_WINDOW_NOW.toISOString(),
        deliveryLagMs: 1,
        enqueuedAt: ON_WINDOW_NOW.toISOString(),
        id: "canary",
      },
    ],
    checkedAt: ON_WINDOW_NOW.toISOString(),
  });
  vi.mocked(fetchPredictionReadiness).mockResolvedValue({
    checkedAt: ON_WINDOW_NOW.toISOString(),
    races: [],
    runYmd: "20260628",
  });
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("recognizes quarter-hour ticks", () => {
  expect(isQuarterHourTick(new Date("2026-06-28T06:15:00Z"))).toBe(true);
  expect(isQuarterHourTick(new Date("2026-06-28T06:10:00Z"))).toBe(false);
});

it("runs only the canary monitor on a five-minute non-quarter tick", async () => {
  const state = buildKvState();
  const env = buildEnv(state);
  await runScheduled({ env, now: new Date("2026-06-28T06:10:00Z") });
  expect(sendDailyMonitorHeartbeat).toHaveBeenCalledTimes(1);
  expect(fetchDeliveryCanaries).toHaveBeenCalledTimes(1);
  expect(fetchPredictionReadiness).not.toHaveBeenCalled();
  expect(fetchQueueHealth).not.toHaveBeenCalled();
  expect(processIncidentSignal).toHaveBeenCalledTimes(2);
});

it("runScheduled produces no alert messages when all checks pass and no prior failures exist", async () => {
  vi.mocked(fetchQueueHealth).mockResolvedValue(HEALTHY_METRICS);
  const state = buildKvState();
  const env = buildEnv(state);
  await runScheduled({ env, now: ON_WINDOW_NOW });
  expect(state.send).not.toHaveBeenCalled();
});

it("runScheduled records one failure but does not alert on the first failed tick", async () => {
  vi.mocked(fetchQueueHealth).mockResolvedValue(FAILING_RESULTS_METRICS);
  const state = buildKvState();
  const env = buildEnv(state);
  await runScheduled({ env, now: ON_WINDOW_NOW });
  expect(state.store.get("failures:fetch-results-staleness")).toBe("1");
  expect(state.send).not.toHaveBeenCalled();
});

it("runScheduled logs a warning at exactly two consecutive failures without sending a queue message", async () => {
  vi.mocked(fetchQueueHealth).mockResolvedValue(FAILING_RESULTS_METRICS);
  const state = buildKvState({ "failures:fetch-results-staleness": "1" });
  const env = buildEnv(state);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await runScheduled({ env, now: ON_WINDOW_NOW });
  expect(state.store.get("failures:fetch-results-staleness")).toBe("2");
  expect(state.send).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalled();
});

it("runScheduled produces a critical alert at exactly three consecutive failures", async () => {
  vi.mocked(fetchQueueHealth).mockResolvedValue(FAILING_RESULTS_METRICS);
  const state = buildKvState({ "failures:fetch-results-staleness": "2" });
  const env = buildEnv(state);
  await runScheduled({ env, now: ON_WINDOW_NOW });
  expect(state.store.get("failures:fetch-results-staleness")).toBe("3");
  expect(state.send).toHaveBeenCalledTimes(1);
  const sent = state.send.mock.calls[0]?.[0] as AlertMessage;
  expect(sent.severity).toBe("critical");
  expect(sent.checkName).toBe("fetch-results-staleness");
});

it("runScheduled does NOT produce a still-failing alert at the four-six fail range", async () => {
  vi.mocked(fetchQueueHealth).mockResolvedValue(FAILING_RESULTS_METRICS);
  const state = buildKvState({ "failures:fetch-results-staleness": "5" });
  const env = buildEnv(state);
  await runScheduled({ env, now: ON_WINDOW_NOW });
  expect(state.store.get("failures:fetch-results-staleness")).toBe("6");
  expect(state.send).not.toHaveBeenCalled();
});

it("runScheduled produces a still-failing critical alert at seven consecutive failures", async () => {
  vi.mocked(fetchQueueHealth).mockResolvedValue(FAILING_RESULTS_METRICS);
  const state = buildKvState({ "failures:fetch-results-staleness": "6" });
  const env = buildEnv(state);
  await runScheduled({ env, now: ON_WINDOW_NOW });
  expect(state.store.get("failures:fetch-results-staleness")).toBe("7");
  expect(state.send).toHaveBeenCalledTimes(1);
  const sent = state.send.mock.calls[0]?.[0] as AlertMessage;
  expect(sent.severity).toBe("critical");
});

it("runScheduled produces a recovery alert and resets the counter when a previously failing check recovers", async () => {
  vi.mocked(fetchQueueHealth).mockResolvedValue(HEALTHY_METRICS);
  const state = buildKvState({ "failures:fetch-results-staleness": "4" });
  const env = buildEnv(state);
  await runScheduled({ env, now: ON_WINDOW_NOW });
  expect(state.store.has("failures:fetch-results-staleness")).toBe(false);
  expect(state.send).toHaveBeenCalledTimes(1);
  const sent = state.send.mock.calls[0]?.[0] as AlertMessage;
  expect(sent.severity).toBe("recovery");
  expect(sent.checkName).toBe("fetch-results-staleness");
});

it("turns finish-position endpoint failures into incident signals", async () => {
  vi.mocked(fetchDeliveryCanaries).mockRejectedValue(new Error("canary unavailable"));
  vi.mocked(fetchPredictionReadiness).mockRejectedValue(new Error("readiness unavailable"));
  vi.mocked(fetchQueueHealth).mockResolvedValue(HEALTHY_METRICS);
  await runScheduled({ env: buildEnv(buildKvState()), now: ON_WINDOW_NOW });
  expect(processIncidentSignal).toHaveBeenCalledWith(
    expect.anything(),
    expect.objectContaining({ key: "finish-position-monitor-endpoint:delivery-canaries" }),
    ON_WINDOW_NOW,
  );
  expect(processIncidentSignal).toHaveBeenCalledWith(
    expect.anything(),
    expect.objectContaining({ key: "finish-position-monitor-endpoint:prediction-readiness" }),
    ON_WINDOW_NOW,
  );
});

it("reports queue-health failures and still runs readiness", async () => {
  vi.mocked(fetchQueueHealth).mockRejectedValue(
    new Error("queue-health request failed with status 403"),
  );
  vi.mocked(fetchPredictionReadiness).mockResolvedValue({
    checkedAt: ON_WINDOW_NOW.toISOString(),
    races: [],
    runYmd: "20260628",
  });
  await expect(
    runScheduled({ env: buildEnv(buildKvState()), now: ON_WINDOW_NOW }),
  ).resolves.toBeUndefined();
  expect(fetchPredictionReadiness).toHaveBeenCalled();
  expect(processIncidentSignal).toHaveBeenCalledWith(
    expect.anything(),
    expect.objectContaining({
      description:
        "Monitor endpoint failed and cannot be evaluated: Error: queue-health request failed with status 403",
      key: "finish-position-monitor-endpoint:queue-health",
    }),
    ON_WINDOW_NOW,
  );
});

it("emits a queue-health recovery signal after the endpoint becomes available", async () => {
  vi.mocked(fetchQueueHealth).mockResolvedValue(HEALTHY_METRICS);
  await runScheduled({ env: buildEnv(buildKvState()), now: ON_WINDOW_NOW });
  expect(processIncidentSignal).toHaveBeenCalledWith(
    expect.anything(),
    expect.objectContaining({ key: "finish-position-monitor-endpoint:queue-health", ok: true }),
    ON_WINDOW_NOW,
  );
});

it("contains incident delivery failures so a scheduled tick still completes", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  vi.mocked(processIncidentSignal).mockRejectedValue(new Error("discord unavailable"));
  vi.mocked(fetchQueueHealth).mockRejectedValue(
    new Error("queue-health request failed with status 403"),
  );
  await expect(
    runScheduled({ env: buildEnv(buildKvState()), now: ON_WINDOW_NOW }),
  ).resolves.toBeUndefined();
  expect(errorSpy).toHaveBeenCalled();
  vi.mocked(processIncidentSignal).mockResolvedValue(undefined);
});

it("runScheduled skips processing for checks outside their JST window", async () => {
  vi.mocked(fetchQueueHealth).mockResolvedValue({
    lastSuccessfulFetchResultsAt: null,
    lastSuccessfulFetchWeightsAt: null,
    racesQueuedNotFetchedToday: 0,
    racesStuckOverThirtyMin: 0,
  });
  const state = buildKvState();
  const env = buildEnv(state);
  // 2026-06-28 09:00 JST = 00:00 UTC -- outside both staleness windows.
  await runScheduled({ env, now: new Date("2026-06-28T00:00:00Z") });
  expect(state.send).not.toHaveBeenCalled();
  expect(state.store.has("failures:fetch-results-staleness")).toBe(false);
  expect(state.store.has("failures:fetch-weights-staleness")).toBe(false);
});
