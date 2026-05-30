// Run with: bunx vitest run src/app/races/detail/race-trend-section.test.tsx

import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import React from "react";
import { afterEach, expect, test, vi } from "vitest";

import type { RaceTrendRawPayload } from "../../../lib/race-types";

const fetchWithRetryMock = vi.fn<(input: string, init?: RequestInit) => Promise<Response>>();

vi.mock("../../../lib/fetch-with-retry", () => ({
  fetchWithRetry: (input: string, init?: RequestInit) => fetchWithRetryMock(input, init),
}));

const getRaceTrendLiveUrlMock = vi.fn<(path: string) => string>(
  (path) => `wss://example.test${path}`,
);

vi.mock("../../../lib/paddock-client-url", () => ({
  getRaceTrendLiveUrl: (path: string) => getRaceTrendLiveUrlMock(path),
}));

vi.mock("./realtime-client", () => ({
  useRealtimeRaceSelector: <T,>(selector: (state: { payload: null }) => T): T =>
    selector({ payload: null }),
}));

type WebSocketEventHandler = (event: MessageEvent | Event) => void;

class WebSocketStub {
  public listeners: Map<string, WebSocketEventHandler[]> = new Map();
  public closed = false;
  constructor(public url: string) {}
  addEventListener(name: string, handler: WebSocketEventHandler): void {
    const existing = this.listeners.get(name) ?? [];
    existing.push(handler);
    this.listeners.set(name, existing);
  }
  removeEventListener(): void {}
  close(): void {
    this.closed = true;
  }
}

interface WebSocketStubRegistry {
  sockets: WebSocketStub[];
}

const installWebSocketStub = (): WebSocketStubRegistry => {
  const registry: WebSocketStubRegistry = { sockets: [] };
  const StubFactory = function StubFactory(this: WebSocketStub, url: string) {
    const instance = new WebSocketStub(url);
    registry.sockets.push(instance);
    return instance;
  };
  (globalThis as { WebSocket: unknown }).WebSocket = StubFactory;
  return registry;
};

const buildEmptyPayload = (): RaceTrendRawPayload => ({
  raceContext: { keibajoCode: "05", raceBango: "03", source: "jra" },
  runners: [],
  starterRows: [],
  currentRunningStyles: [],
  historicalRunningStyles: [],
});

interface JsonResponseInit {
  body: unknown;
  ok: boolean;
  status?: number;
}

const buildJsonResponse = (init: JsonResponseInit): Response => {
  const status = init.status ?? (init.ok ? 200 : 500);
  return new Response(JSON.stringify(init.body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
};

const baseProps = {
  day: "17",
  defaultEndDate: "2025-05-17",
  defaultStartDate: "2025-05-03",
  keibajoCode: "05",
  minStartDate: "2025-05-03",
  month: "05",
  raceNumber: "03",
  source: "jra" as const,
  year: "2025",
};

const triggerTrendUpdated = async (registry: WebSocketStubRegistry): Promise<void> => {
  await waitFor(() => {
    expect(registry.sockets.length).toBeGreaterThan(0);
  });
  const socket = registry.sockets[0];
  if (!socket) throw new Error("websocket stub missing");
  const handlers = socket.listeners.get("message") ?? [];
  const handler = handlers[0];
  if (!handler) throw new Error("no websocket message handler");
  await act(async () => {
    handler(new MessageEvent("message", { data: JSON.stringify({ type: "trend-updated" }) }));
  });
};

afterEach(() => {
  cleanup();
  fetchWithRetryMock.mockReset();
  getRaceTrendLiveUrlMock.mockClear();
});

test("renders empty-state copy and retry button when initial payload has zero rows", async () => {
  installWebSocketStub();
  fetchWithRetryMock.mockResolvedValueOnce(
    buildJsonResponse({ body: buildEmptyPayload(), ok: true }),
  );
  const { RaceTrendSection } = await import("./race-trend-section");
  render(<RaceTrendSection {...baseProps} />);
  await waitFor(() => {
    expect(screen.getByText("成績データが揃うのを待っています")).toBeTruthy();
  });
  expect(
    screen.getByText(
      "確定後のレースから順に表示します。 数十秒で自動更新しますが、手動で再取得することもできます。",
    ),
  ).toBeTruthy();
  expect(screen.getAllByRole("button", { name: "再取得" }).length).toBeGreaterThan(0);
});

test("clicking the empty-state retry button forces a __trendCacheRefresh fetch", async () => {
  installWebSocketStub();
  fetchWithRetryMock
    .mockResolvedValueOnce(buildJsonResponse({ body: buildEmptyPayload(), ok: true }))
    .mockResolvedValueOnce(buildJsonResponse({ body: buildEmptyPayload(), ok: true }));
  const { RaceTrendSection } = await import("./race-trend-section");
  render(<RaceTrendSection {...baseProps} />);
  await waitFor(() => {
    expect(screen.getByText("成績データが揃うのを待っています")).toBeTruthy();
  });
  const retryButtons = screen.getAllByRole("button", { name: "再取得" });
  const firstRetry = retryButtons[0];
  if (!firstRetry) throw new Error("no retry button rendered");
  await act(async () => {
    fireEvent.click(firstRetry);
  });
  await waitFor(() => {
    expect(fetchWithRetryMock).toHaveBeenCalledTimes(2);
  });
  const lastCallUrl = fetchWithRetryMock.mock.calls[1]?.[0] ?? "";
  expect(lastCallUrl.includes("__trendCacheRefresh=1")).toBe(true);
});

test("renders error UI with retry button when the initial fetch rejects", async () => {
  installWebSocketStub();
  fetchWithRetryMock.mockRejectedValueOnce(new Error("network down"));
  const { RaceTrendSection } = await import("./race-trend-section");
  render(<RaceTrendSection {...baseProps} />);
  await waitFor(() => {
    expect(screen.getByText("レース傾向を取得できませんでした。")).toBeTruthy();
  });
  expect(
    screen.getByText("通信エラーで再取得します。 手動で再試行することもできます。"),
  ).toBeTruthy();
  expect(screen.getAllByRole("button", { name: "再取得" }).length).toBeGreaterThan(0);
});

test("renders the stale-data banner when a background refresh fails after a prior success", async () => {
  const registry = installWebSocketStub();
  fetchWithRetryMock
    .mockResolvedValueOnce(buildJsonResponse({ body: buildEmptyPayload(), ok: true }))
    .mockRejectedValueOnce(new Error("flaky upstream"));
  const { RaceTrendSection } = await import("./race-trend-section");
  render(<RaceTrendSection {...baseProps} />);
  await waitFor(() => {
    expect(screen.getByText("成績データが揃うのを待っています")).toBeTruthy();
  });
  await triggerTrendUpdated(registry);
  await waitFor(() => {
    expect(screen.getByText("最新化に失敗したため、 直近のデータを表示しています。")).toBeTruthy();
  });
});

test("trend-updated WebSocket message triggers a __trendCacheRefresh fetch", async () => {
  const registry = installWebSocketStub();
  fetchWithRetryMock
    .mockResolvedValueOnce(buildJsonResponse({ body: buildEmptyPayload(), ok: true }))
    .mockResolvedValueOnce(buildJsonResponse({ body: buildEmptyPayload(), ok: true }));
  const { RaceTrendSection } = await import("./race-trend-section");
  render(<RaceTrendSection {...baseProps} />);
  await waitFor(() => {
    expect(fetchWithRetryMock).toHaveBeenCalledTimes(1);
  });
  await triggerTrendUpdated(registry);
  await waitFor(() => {
    expect(fetchWithRetryMock).toHaveBeenCalledTimes(2);
  });
  const lastCallUrl = fetchWithRetryMock.mock.calls[1]?.[0] ?? "";
  expect(lastCallUrl.includes("__trendCacheRefresh=1")).toBe(true);
});
