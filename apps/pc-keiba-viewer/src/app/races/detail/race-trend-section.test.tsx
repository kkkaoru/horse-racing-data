// Run with: bun run --filter pc-keiba-viewer test src/app/races/detail/race-trend-section.test.tsx

import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import React from "react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import type { RaceTrendRawPayload } from "../../../lib/race-types";

vi.mock("./realtime-client", () => ({
  useRealtimeRaceSelector: <T,>(selector: (state: { payload: null }) => T): T =>
    selector({ payload: null }),
}));

import { RaceTrendSection } from "./race-trend-section";

interface MockWebSocketLike {
  url: string;
  close: ReturnType<typeof vi.fn<() => void>>;
  readyState: number;
  listeners: Map<string, Array<(event: unknown) => void>>;
  dispatch: (type: string, event: unknown) => void;
}

interface WebSocketStash {
  original: typeof WebSocket;
}

interface ResolveFetchStash {
  current: ((value: Response) => void) | null;
}

interface FetchCallCounter {
  count: number;
}

const installedSockets: MockWebSocketLike[] = [];
const webSocketStash: WebSocketStash = { original: globalThis.WebSocket };

const installMockWebSocket = (): void => {
  installedSockets.length = 0;
  webSocketStash.original = globalThis.WebSocket;
  const buildMockSocket = (url: string): MockWebSocketLike => {
    const listeners = new Map<string, Array<(event: unknown) => void>>();
    const socket: MockWebSocketLike = {
      url,
      close: vi.fn<() => void>(() => {
        socket.readyState = 3;
      }),
      readyState: 0,
      listeners,
      dispatch: (type, event) => (listeners.get(type) ?? []).forEach((handler) => handler(event)),
    };
    return socket;
  };
  class MockWebSocket {
    static readonly CONNECTING = 0;
    static readonly OPEN = 1;
    static readonly CLOSING = 2;
    static readonly CLOSED = 3;
    inner: MockWebSocketLike;
    constructor(url: string) {
      this.inner = buildMockSocket(url);
      installedSockets.push(this.inner);
    }
    addEventListener(type: string, handler: (event: unknown) => void): void {
      const existing = this.inner.listeners.get(type) ?? [];
      existing.push(handler);
      this.inner.listeners.set(type, existing);
    }
    close(): void {
      this.inner.close();
    }
    get readyState(): number {
      return this.inner.readyState;
    }
    set readyState(value: number) {
      this.inner.readyState = value;
    }
  }
  Object.assign(globalThis, { WebSocket: MockWebSocket });
};

const restoreWebSocket = (): void => {
  Object.assign(globalThis, { WebSocket: webSocketStash.original });
  installedSockets.length = 0;
};

const buildRawPayload = (): RaceTrendRawPayload => ({
  starterRows: [],
  currentRunningStyles: [],
  historicalRunningStyles: [],
  raceContext: { keibajoCode: "06", raceBango: "11", source: "jra" },
  runners: [],
});

const PROPS = {
  day: "30",
  defaultEndDate: "2026-05-29",
  defaultStartDate: "2025-05-30",
  keibajoCode: "06",
  minStartDate: "2023-01-01",
  month: "05",
  raceNumber: "11",
  source: "jra",
  year: "2026",
} satisfies Parameters<typeof RaceTrendSection>[0];

const buildOkResponse = (payload: RaceTrendRawPayload): Response =>
  new Response(JSON.stringify(payload), {
    status: 200,
    headers: { "content-type": "application/json" },
  });

const buildErrorResponse = (): Response => new Response("err", { status: 500 });

const renderSection = (): ReturnType<typeof render> => render(<RaceTrendSection {...PROPS} />);

const flushAllAsync = async (): Promise<void> => {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();
  });
};

const extractRequestUrl = (input: RequestInfo | URL): string => {
  if (typeof input === "string") return input;
  if (input instanceof URL) return input.toString();
  return input.url;
};

beforeEach(() => {
  installMockWebSocket();
  vi.useFakeTimers();
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  restoreWebSocket();
  vi.restoreAllMocks();
});

test("WebSocket close triggers a reconnect after the exponential backoff delay", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(buildOkResponse(buildRawPayload()));
  renderSection();
  await flushAllAsync();
  expect(installedSockets).toHaveLength(1);
  const firstSocket = installedSockets[0];
  if (!firstSocket) throw new Error("first socket missing");
  act(() => {
    firstSocket.dispatch("close", {});
  });
  expect(installedSockets).toHaveLength(1);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(1000);
  });
  expect(installedSockets).toHaveLength(2);
  expect(fetchSpy).toHaveBeenCalled();
});

test("WebSocket error triggers a close and a reconnect attempt", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(buildOkResponse(buildRawPayload()));
  renderSection();
  await flushAllAsync();
  const firstSocket = installedSockets[0];
  if (!firstSocket) throw new Error("first socket missing");
  act(() => {
    firstSocket.dispatch("error", {});
  });
  expect(firstSocket.close).toHaveBeenCalled();
  act(() => {
    firstSocket.dispatch("close", {});
  });
  await act(async () => {
    await vi.advanceTimersByTimeAsync(1000);
  });
  expect(installedSockets).toHaveLength(2);
});

test("WebSocket open resets the reconnect attempt counter so the next backoff starts at 1s", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(buildOkResponse(buildRawPayload()));
  renderSection();
  await flushAllAsync();
  const firstSocket = installedSockets[0];
  if (!firstSocket) throw new Error("first socket missing");
  act(() => {
    firstSocket.dispatch("close", {});
  });
  await act(async () => {
    await vi.advanceTimersByTimeAsync(1000);
  });
  expect(installedSockets).toHaveLength(2);
  const secondSocket = installedSockets[1];
  if (!secondSocket) throw new Error("second socket missing");
  act(() => {
    secondSocket.dispatch("open", {});
  });
  act(() => {
    secondSocket.dispatch("close", {});
  });
  await act(async () => {
    await vi.advanceTimersByTimeAsync(999);
  });
  expect(installedSockets).toHaveLength(2);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(1);
  });
  expect(installedSockets).toHaveLength(3);
});

test("retry button is rendered when the initial fetch fails", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(buildErrorResponse());
  renderSection();
  // Drain the bounded retry chain (300/600/1200/2400 = 4500ms total of sleeps).
  await act(async () => {
    await vi.advanceTimersByTimeAsync(6000);
  });
  // Both the inline empty-state and the alert region render a retry button
  // when the initial fetch fails (rawPayload=null → rows empty + status=error),
  // so we assert at least one is present.
  expect(screen.getAllByRole("button", { name: "再取得" }).length).toBeGreaterThan(0);
});

test("retry button click passes an AbortSignal to fetchWithRetry so the request is abortable", async () => {
  vi.useRealTimers();
  const manualSignals: AbortSignal[] = [];
  const pendingResolvers: Array<(value: Response) => void> = [];
  const fetchMock = vi.fn<(input: RequestInfo | URL, init?: RequestInit) => Promise<Response>>(
    async (_input, init) => {
      const signal = init?.signal;
      if (signal === undefined || signal === null) {
        return buildErrorResponse();
      }
      manualSignals.push(signal);
      return new Promise<Response>((resolve) => {
        pendingResolvers.push(resolve);
      });
    },
  );
  vi.spyOn(globalThis, "fetch").mockImplementation(fetchMock);
  renderSection();
  await waitFor(
    () => {
      expect(screen.getAllByRole("button", { name: "再取得" }).length).toBeGreaterThan(0);
    },
    { timeout: 5000 },
  );
  const retryButtons = screen.getAllByRole("button", { name: "再取得" });
  const firstRetryButton = retryButtons[0];
  if (!firstRetryButton) throw new Error("retry button missing");
  await act(async () => {
    fireEvent.click(firstRetryButton);
  });
  await waitFor(() => {
    expect(manualSignals.length).toBeGreaterThanOrEqual(1);
  });
  const firstManualSignal = manualSignals[0];
  if (!firstManualSignal) throw new Error("first manual signal missing");
  expect(firstManualSignal).toBeInstanceOf(AbortSignal);
  expect(firstManualSignal.aborted).toBe(false);
  await act(async () => {
    pendingResolvers.forEach((resolve) => resolve(buildOkResponse(buildRawPayload())));
  });
});

test("unmount aborts the in-flight manual refresh AbortController", async () => {
  vi.useRealTimers();
  const manualSignals: AbortSignal[] = [];
  const pendingResolvers: Array<(value: Response) => void> = [];
  vi.spyOn(globalThis, "fetch").mockImplementation((_input, init) => {
    const signal = init?.signal ?? null;
    if (signal === null) {
      return Promise.resolve(buildErrorResponse());
    }
    manualSignals.push(signal);
    return new Promise<Response>((resolve) => {
      pendingResolvers.push(resolve);
    });
  });
  const { unmount } = renderSection();
  await waitFor(
    () => {
      expect(screen.getAllByRole("button", { name: "再取得" }).length).toBeGreaterThan(0);
    },
    { timeout: 5000 },
  );
  const retryButtons = screen.getAllByRole("button", { name: "再取得" });
  const firstRetryButton = retryButtons[0];
  if (!firstRetryButton) throw new Error("retry button missing");
  await act(async () => {
    fireEvent.click(firstRetryButton);
  });
  await waitFor(() => {
    expect(manualSignals.length).toBeGreaterThanOrEqual(1);
  });
  const firstManualSignal = manualSignals[0];
  if (!firstManualSignal) throw new Error("first manual signal missing");
  expect(firstManualSignal.aborted).toBe(false);
  unmount();
  expect(firstManualSignal.aborted).toBe(true);
});

test("retry button is disabled while a manual refresh is in flight to block double-click", async () => {
  vi.useRealTimers();
  const pendingResolvers: Array<(value: Response) => void> = [];
  vi.spyOn(globalThis, "fetch").mockImplementation((_input, init) => {
    const signal = init?.signal ?? null;
    if (signal === null) {
      return Promise.resolve(buildErrorResponse());
    }
    return new Promise<Response>((resolve) => {
      pendingResolvers.push(resolve);
    });
  });
  renderSection();
  // Inspect the error-region retry button specifically (it carries the
  // aria-busy / disabled attributes wired to isRefreshing — the empty-state
  // retry button is presentational and does not toggle disabled state).
  await waitFor(
    () => {
      expect(screen.getByRole("alert")).toBeTruthy();
    },
    { timeout: 5000 },
  );
  const alert = screen.getByRole("alert");
  const errorRetryButton = alert.querySelector("button.race-trend-retry-button");
  if (!errorRetryButton) throw new Error("error retry button missing");
  expect(errorRetryButton.hasAttribute("disabled")).toBe(false);
  await act(async () => {
    fireEvent.click(errorRetryButton);
  });
  await waitFor(() => {
    expect(errorRetryButton.hasAttribute("disabled")).toBe(true);
  });
  await act(async () => {
    pendingResolvers.forEach((resolve) => resolve(buildOkResponse(buildRawPayload())));
  });
});

test("retry button reports aria-busy while a manual refresh is in flight", async () => {
  vi.useRealTimers();
  const resolveFetchStash: ResolveFetchStash = { current: null };
  const pendingFetch = new Promise<Response>((resolve) => {
    resolveFetchStash.current = resolve;
  });
  const fetchCounter: FetchCallCounter = { count: 0 };
  const initialErrorThreshold = 3;
  vi.spyOn(globalThis, "fetch").mockImplementation(() => {
    fetchCounter.count += 1;
    if (fetchCounter.count <= initialErrorThreshold) {
      return Promise.resolve(buildErrorResponse());
    }
    return pendingFetch;
  });
  renderSection();
  await waitFor(
    () => {
      expect(screen.getByRole("alert")).toBeTruthy();
    },
    { timeout: 5000 },
  );
  const alert = screen.getByRole("alert");
  const errorRetryButton = alert.querySelector("button.race-trend-retry-button");
  if (!errorRetryButton) throw new Error("error retry button missing");
  await act(async () => {
    fireEvent.click(errorRetryButton);
  });
  await waitFor(() => {
    expect(errorRetryButton.getAttribute("aria-busy")).toBe("true");
  });
  expect(errorRetryButton.hasAttribute("disabled")).toBe(true);
  await act(async () => {
    resolveFetchStash.current?.(buildOkResponse(buildRawPayload()));
  });
});

test("stale banner is NOT shown when rawPayload is null and a background refresh fails", async () => {
  vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("network down"));
  renderSection();
  // Drain the bounded retry chain on the initial fetch (clearOnError path
  // will set status to error but should NOT set the stale banner).
  await act(async () => {
    await vi.advanceTimersByTimeAsync(6000);
  });
  expect(screen.queryByText("最新化に失敗したため、 直近のデータを表示しています。")).toBeNull();
});

test("stale banner IS shown when rawPayload exists and a background refresh fails", async () => {
  const fetchCounter: FetchCallCounter = { count: 0 };
  vi.spyOn(globalThis, "fetch").mockImplementation(() => {
    fetchCounter.count += 1;
    if (fetchCounter.count === 1) {
      return Promise.resolve(buildOkResponse(buildRawPayload()));
    }
    return Promise.reject(new Error("network down"));
  });
  renderSection();
  await flushAllAsync();
  // Trigger the WebSocket-driven refresh (non-clearOnError path) by dispatching
  // a trend-updated message. The retry chain will exhaust and the stale banner
  // should appear because rawPayloadRef.current is now populated.
  const firstSocket = installedSockets[0];
  if (!firstSocket) throw new Error("first socket missing");
  act(() => {
    firstSocket.dispatch("message", { data: JSON.stringify({ type: "trend-updated" }) });
  });
  await act(async () => {
    await vi.advanceTimersByTimeAsync(6000);
  });
  expect(screen.getByText("最新化に失敗したため、 直近のデータを表示しています。")).toBeTruthy();
});

test("WebSocket reconnect timer is cleared when the component unmounts", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(buildOkResponse(buildRawPayload()));
  const { unmount } = renderSection();
  await flushAllAsync();
  const firstSocket = installedSockets[0];
  if (!firstSocket) throw new Error("first socket missing");
  act(() => {
    firstSocket.dispatch("close", {});
  });
  unmount();
  await act(async () => {
    await vi.advanceTimersByTimeAsync(5000);
  });
  expect(installedSockets).toHaveLength(1);
});

test("livePath change recreates the WebSocket connection", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(buildOkResponse(buildRawPayload()));
  const { rerender } = renderSection();
  await flushAllAsync();
  expect(installedSockets).toHaveLength(1);
  rerender(<RaceTrendSection {...PROPS} raceNumber="12" />);
  await flushAllAsync();
  expect(installedSockets.length).toBeGreaterThanOrEqual(2);
});

test("visibilitychange to visible forces a WebSocket reconnect when the socket is closed", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(buildOkResponse(buildRawPayload()));
  renderSection();
  await flushAllAsync();
  const firstSocket = installedSockets[0];
  if (!firstSocket) throw new Error("first socket missing");
  firstSocket.readyState = 3;
  Object.defineProperty(document, "visibilityState", { configurable: true, get: () => "visible" });
  act(() => {
    document.dispatchEvent(new Event("visibilitychange"));
  });
  expect(firstSocket.close).toHaveBeenCalled();
});

test("WebSocket message of an unknown type does not trigger a refresh fetch", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(buildOkResponse(buildRawPayload()));
  renderSection();
  await flushAllAsync();
  fetchSpy.mockClear();
  const firstSocket = installedSockets[0];
  if (!firstSocket) throw new Error("first socket missing");
  act(() => {
    firstSocket.dispatch("message", { data: JSON.stringify({ type: "other" }) });
  });
  expect(fetchSpy).not.toHaveBeenCalled();
});

test("WebSocket message with invalid JSON does not trigger a refresh fetch", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(buildOkResponse(buildRawPayload()));
  renderSection();
  await flushAllAsync();
  fetchSpy.mockClear();
  const firstSocket = installedSockets[0];
  if (!firstSocket) throw new Error("first socket missing");
  act(() => {
    firstSocket.dispatch("message", { data: "not-json" });
  });
  expect(fetchSpy).not.toHaveBeenCalled();
});

test("WebSocket trend-updated triggers a force-refresh fetch carrying __trendCacheRefresh=1", async () => {
  const observedUrls: string[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
    observedUrls.push(extractRequestUrl(input));
    return Promise.resolve(buildOkResponse(buildRawPayload()));
  });
  renderSection();
  await flushAllAsync();
  const initialCallCount = observedUrls.length;
  const firstSocket = installedSockets[0];
  if (!firstSocket) throw new Error("first socket missing");
  act(() => {
    firstSocket.dispatch("message", { data: JSON.stringify({ type: "trend-updated" }) });
  });
  await flushAllAsync();
  const newUrls = observedUrls.slice(initialCallCount);
  expect(newUrls.length).toBeGreaterThanOrEqual(1);
  const firstNewUrl = newUrls[0];
  if (!firstNewUrl) throw new Error("first new url missing");
  // Resolve the path against an absolute base so URL searchParams parsing works
  // even when the fetch input is a path-only string.
  const firstNewUrlObject = new URL(firstNewUrl, "http://localhost");
  expect(firstNewUrlObject.searchParams.get("__trendCacheRefresh")).toBe("1");
});

test("visibilitychange to visible force-reconnects when socket is null and reconnect timer is pending", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(buildOkResponse(buildRawPayload()));
  renderSection();
  await flushAllAsync();
  const firstSocket = installedSockets[0];
  if (!firstSocket) throw new Error("first socket missing");
  // Trigger scheduleReconnect by firing close — this nulls liveSocketRef and
  // sets a pending reconnect timer (computeRaceTrendLiveBackoffMs(0) = 1000ms).
  act(() => {
    firstSocket.dispatch("close", {});
  });
  expect(installedSockets).toHaveLength(1);
  Object.defineProperty(document, "visibilityState", { configurable: true, get: () => "visible" });
  act(() => {
    document.dispatchEvent(new Event("visibilitychange"));
  });
  // Without advancing fake timers, the force-reconnect path must have already
  // cleared the pending timer and invoked `connect()` synchronously, so a
  // second socket is installed without waiting for the 1s backoff to elapse.
  expect(installedSockets).toHaveLength(2);
});

// ---------------- X4 restoration cases (BUG-4) ----------------

test("empty-state copy + retry button rendered when initial payload has 0 rows", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(buildOkResponse(buildRawPayload()));
  renderSection();
  await flushAllAsync();
  expect(screen.getByText("成績データが揃うのを待っています")).toBeTruthy();
  expect(
    screen.getByText(
      "確定後のレースから順に表示します。 数十秒で自動更新しますが、手動で再取得することもできます。",
    ),
  ).toBeTruthy();
  expect(screen.getAllByRole("button", { name: "再取得" }).length).toBeGreaterThan(0);
});

test("empty-state retry click forces a __trendCacheRefresh=1 fetch", async () => {
  vi.useRealTimers();
  const observedUrls: string[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
    observedUrls.push(extractRequestUrl(input));
    return Promise.resolve(buildOkResponse(buildRawPayload()));
  });
  renderSection();
  await waitFor(() => {
    expect(screen.getByText("成績データが揃うのを待っています")).toBeTruthy();
  });
  const initialCallCount = observedUrls.length;
  const retryButtons = screen.getAllByRole("button", { name: "再取得" });
  const firstRetry = retryButtons[0];
  if (!firstRetry) throw new Error("no retry button rendered");
  await act(async () => {
    fireEvent.click(firstRetry);
  });
  await waitFor(() => {
    expect(observedUrls.length).toBeGreaterThan(initialCallCount);
  });
  const nextUrl = observedUrls[initialCallCount];
  if (!nextUrl) throw new Error("no follow-up fetch observed");
  const nextUrlObject = new URL(nextUrl, "http://localhost");
  expect(nextUrlObject.searchParams.get("__trendCacheRefresh")).toBe("1");
});

test("error UI renders detail copy and a retry button when the initial fetch rejects", async () => {
  vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("network down"));
  renderSection();
  await act(async () => {
    await vi.advanceTimersByTimeAsync(6000);
  });
  expect(screen.getByText("レース傾向を取得できませんでした。")).toBeTruthy();
  expect(
    screen.getByText("通信エラーで再取得します。 手動で再試行することもできます。"),
  ).toBeTruthy();
  expect(screen.getAllByRole("button", { name: "再取得" }).length).toBeGreaterThan(0);
});

test("stale-data banner with retry button appears when background refresh fails after a prior success", async () => {
  const fetchCounter: FetchCallCounter = { count: 0 };
  vi.spyOn(globalThis, "fetch").mockImplementation(() => {
    fetchCounter.count += 1;
    if (fetchCounter.count === 1) {
      return Promise.resolve(buildOkResponse(buildRawPayload()));
    }
    return Promise.reject(new Error("flaky upstream"));
  });
  renderSection();
  await flushAllAsync();
  const firstSocket = installedSockets[0];
  if (!firstSocket) throw new Error("first socket missing");
  act(() => {
    firstSocket.dispatch("message", { data: JSON.stringify({ type: "trend-updated" }) });
  });
  await act(async () => {
    await vi.advanceTimersByTimeAsync(6000);
  });
  expect(screen.getByText("最新化に失敗したため、 直近のデータを表示しています。")).toBeTruthy();
  // Banner exposes a retry affordance alongside the message.
  expect(screen.getAllByRole("button", { name: "再取得" }).length).toBeGreaterThan(0);
});
