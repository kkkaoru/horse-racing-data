// Run with: bun run --filter pipeline-health-monitor test
import { afterEach, expect, it, vi } from "vitest";

vi.mock("./queue-health-client", () => ({
  fetchQueueHealth: vi.fn(),
}));

import { fetchQueueHealth } from "./queue-health-client";
import { runScheduled } from "./scheduled-handler";
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

afterEach(() => {
  vi.restoreAllMocks();
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
