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

const installedSockets: MockWebSocketLike[] = [];
let originalWebSocket: typeof WebSocket;

const installMockWebSocket = (): void => {
  installedSockets.length = 0;
  originalWebSocket = globalThis.WebSocket;
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
  Object.assign(globalThis, { WebSocket: originalWebSocket });
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
  expect(screen.getByRole("button", { name: "再試行" })).toBeTruthy();
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
  const retryButton = await screen.findByRole("button", { name: "再試行" }, { timeout: 5000 });
  await act(async () => {
    fireEvent.click(retryButton);
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
  const retryButton = await screen.findByRole("button", { name: "再試行" }, { timeout: 5000 });
  await act(async () => {
    fireEvent.click(retryButton);
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
  const retryButton = await screen.findByRole("button", { name: "再試行" }, { timeout: 5000 });
  expect(retryButton.hasAttribute("disabled")).toBe(false);
  await act(async () => {
    fireEvent.click(retryButton);
  });
  await waitFor(() => {
    expect(retryButton.hasAttribute("disabled")).toBe(true);
  });
  await act(async () => {
    pendingResolvers.forEach((resolve) => resolve(buildOkResponse(buildRawPayload())));
  });
});

test("retry button reports aria-busy while a manual refresh is in flight", async () => {
  vi.useRealTimers();
  let resolveFetch: ((value: Response) => void) | null = null;
  const pendingFetch = new Promise<Response>((resolve) => {
    resolveFetch = resolve;
  });
  let fetchCallCount = 0;
  const initialErrorThreshold = 3;
  vi.spyOn(globalThis, "fetch").mockImplementation(() => {
    fetchCallCount += 1;
    if (fetchCallCount <= initialErrorThreshold) {
      return Promise.resolve(buildErrorResponse());
    }
    return pendingFetch;
  });
  renderSection();
  const retryButton = await screen.findByRole("button", { name: "再試行" }, { timeout: 5000 });
  await act(async () => {
    fireEvent.click(retryButton);
  });
  await waitFor(() => {
    expect(retryButton.getAttribute("aria-busy")).toBe("true");
  });
  expect(retryButton.hasAttribute("disabled")).toBe(true);
  await act(async () => {
    resolveFetch?.(buildOkResponse(buildRawPayload()));
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
  expect(screen.queryByText("直近のデータを表示中")).toBeNull();
});

test("stale banner IS shown when rawPayload exists and a background refresh fails", async () => {
  let fetchCallCount = 0;
  vi.spyOn(globalThis, "fetch").mockImplementation(() => {
    fetchCallCount += 1;
    if (fetchCallCount === 1) {
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
  expect(screen.getByText("直近のデータを表示中")).toBeTruthy();
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
