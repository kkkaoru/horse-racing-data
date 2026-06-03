// Run with: bun run --filter pc-keiba-viewer test src/lib/websocket-do-router.test.ts

import { expect, test, vi } from "vitest";

import { routeWebSocketUpgradeToDurableObject } from "./websocket-do-router";

interface StubFetchCall {
  raceKey: string;
  request: Request;
}

interface NamespaceStub {
  fetch: ReturnType<typeof vi.fn<(request: Request) => Promise<Response>>>;
  idFromNameCalls: string[];
  namespace: PcKeibaDurableObjectNamespace;
  stubFetchCalls: StubFetchCall[];
}

const buildNamespaceStub = (): NamespaceStub => {
  const idFromNameCalls: string[] = [];
  const stubFetchCalls: StubFetchCall[] = [];
  const fetchMock = vi.fn<(request: Request) => Promise<Response>>(async (request) => {
    const raceKey = new URL(request.url).searchParams.get("raceKey") ?? "";
    stubFetchCalls.push({ raceKey, request });
    return new Response(null, { status: 101 });
  });
  const stub: PcKeibaDurableObjectStub = { fetch: fetchMock };
  const namespace: PcKeibaDurableObjectNamespace = {
    get: () => stub,
    idFromName: (name) => {
      idFromNameCalls.push(name);
      return { toString: () => name };
    },
  };
  return { fetch: fetchMock, idFromNameCalls, namespace, stubFetchCalls };
};

const buildWebSocketUpgradeRequest = (url: string, upgradeValue: string): Request => {
  const request = new Request(url);
  request.headers.set("upgrade", upgradeValue);
  return request;
};

test("non-websocket request returns null so caller can fall back to OpenNext", () => {
  const result = routeWebSocketUpgradeToDurableObject(
    new Request("https://example.com/api/races/2026/06/03/43/10/trends/live?source=nar"),
    {},
  );
  expect(result).toBeNull();
});

test("websocket upgrade with trends/live path and source=nar forwards to RACE_TREND_ROOM with raceKey", async () => {
  const trends = buildNamespaceStub();
  const env: CloudflareEnv = { RACE_TREND_ROOM: trends.namespace };
  const request = buildWebSocketUpgradeRequest(
    "https://example.com/api/races/2026/06/03/43/10/trends/live?source=nar",
    "websocket",
  );
  const result = routeWebSocketUpgradeToDurableObject(request, env);
  if (!result) {
    throw new Error("expected router to return a Promise<Response>");
  }
  const response = await result;
  expect(response.status).toBe(101);
  expect(trends.idFromNameCalls).toStrictEqual(["nar:20260603:43:10"]);
  expect(trends.fetch).toHaveBeenCalledTimes(1);
  const forwardedUrl = trends.stubFetchCalls[0]?.request.url;
  expect(forwardedUrl).toBe("https://race-trend-room/ws?raceKey=nar%3A20260603%3A43%3A10");
});

test("websocket upgrade with trends/live path and source=jra forwards to RACE_TREND_ROOM with raceKey", async () => {
  const trends = buildNamespaceStub();
  const env: CloudflareEnv = { RACE_TREND_ROOM: trends.namespace };
  const request = buildWebSocketUpgradeRequest(
    "https://example.com/api/races/2026/06/03/05/01/trends/live?source=jra",
    "websocket",
  );
  const result = routeWebSocketUpgradeToDurableObject(request, env);
  if (!result) {
    throw new Error("expected router to return a Promise<Response>");
  }
  const response = await result;
  expect(response.status).toBe(101);
  expect(trends.idFromNameCalls).toStrictEqual(["jra:20260603:05:01"]);
  expect(trends.fetch).toHaveBeenCalledTimes(1);
});

test("websocket upgrade with paddock/live path forwards to PADDOCK_ROOM without source prefix", async () => {
  const paddock = buildNamespaceStub();
  const env: CloudflareEnv = { PADDOCK_ROOM: paddock.namespace };
  const request = buildWebSocketUpgradeRequest(
    "https://example.com/api/races/2026/06/03/43/10/paddock/live",
    "websocket",
  );
  const result = routeWebSocketUpgradeToDurableObject(request, env);
  if (!result) {
    throw new Error("expected router to return a Promise<Response>");
  }
  const response = await result;
  expect(response.status).toBe(101);
  expect(paddock.idFromNameCalls).toStrictEqual(["20260603:43:10"]);
  expect(paddock.fetch).toHaveBeenCalledTimes(1);
  const forwardedUrl = paddock.stubFetchCalls[0]?.request.url;
  expect(forwardedUrl).toBe("https://paddock-room/ws?raceKey=20260603%3A43%3A10");
});

test("websocket upgrade with mixed-case Upgrade header still intercepts via case-insensitive comparison", async () => {
  const paddock = buildNamespaceStub();
  const env: CloudflareEnv = { PADDOCK_ROOM: paddock.namespace };
  const request = buildWebSocketUpgradeRequest(
    "https://example.com/api/races/2026/06/03/43/10/paddock/live",
    "WebSocket",
  );
  const result = routeWebSocketUpgradeToDurableObject(request, env);
  if (!result) {
    throw new Error("expected router to return a Promise<Response>");
  }
  await result;
  expect(paddock.fetch).toHaveBeenCalledTimes(1);
});

test("websocket upgrade with missing source on trends/live returns null so 404 can be served", () => {
  const trends = buildNamespaceStub();
  const env: CloudflareEnv = { RACE_TREND_ROOM: trends.namespace };
  const request = buildWebSocketUpgradeRequest(
    "https://example.com/api/races/2026/06/03/43/10/trends/live",
    "websocket",
  );
  const result = routeWebSocketUpgradeToDurableObject(request, env);
  expect(result).toBeNull();
  expect(trends.fetch).toHaveBeenCalledTimes(0);
});

test("websocket upgrade with invalid source value on trends/live returns null", () => {
  const trends = buildNamespaceStub();
  const env: CloudflareEnv = { RACE_TREND_ROOM: trends.namespace };
  const request = buildWebSocketUpgradeRequest(
    "https://example.com/api/races/2026/06/03/43/10/trends/live?source=invalid",
    "websocket",
  );
  const result = routeWebSocketUpgradeToDurableObject(request, env);
  expect(result).toBeNull();
  expect(trends.fetch).toHaveBeenCalledTimes(0);
});

test("websocket upgrade on unrelated path returns null", () => {
  const trends = buildNamespaceStub();
  const paddock = buildNamespaceStub();
  const env: CloudflareEnv = {
    PADDOCK_ROOM: paddock.namespace,
    RACE_TREND_ROOM: trends.namespace,
  };
  const request = buildWebSocketUpgradeRequest(
    "https://example.com/api/some/other/path",
    "websocket",
  );
  const result = routeWebSocketUpgradeToDurableObject(request, env);
  expect(result).toBeNull();
  expect(trends.fetch).toHaveBeenCalledTimes(0);
  expect(paddock.fetch).toHaveBeenCalledTimes(0);
});

test("websocket upgrade on trends/live with non-numeric year does not match path regex", () => {
  const trends = buildNamespaceStub();
  const env: CloudflareEnv = { RACE_TREND_ROOM: trends.namespace };
  const request = buildWebSocketUpgradeRequest(
    "https://example.com/api/races/abcd/06/03/43/10/trends/live?source=nar",
    "websocket",
  );
  const result = routeWebSocketUpgradeToDurableObject(request, env);
  expect(result).toBeNull();
  expect(trends.fetch).toHaveBeenCalledTimes(0);
});

test("websocket upgrade on paddock/live with single-digit raceNumber does not match path regex", () => {
  const paddock = buildNamespaceStub();
  const env: CloudflareEnv = { PADDOCK_ROOM: paddock.namespace };
  const request = buildWebSocketUpgradeRequest(
    "https://example.com/api/races/2026/06/03/43/1/paddock/live",
    "websocket",
  );
  const result = routeWebSocketUpgradeToDurableObject(request, env);
  expect(result).toBeNull();
  expect(paddock.fetch).toHaveBeenCalledTimes(0);
});

test("websocket upgrade on trends/live without RACE_TREND_ROOM binding returns null", () => {
  const request = buildWebSocketUpgradeRequest(
    "https://example.com/api/races/2026/06/03/43/10/trends/live?source=nar",
    "websocket",
  );
  const result = routeWebSocketUpgradeToDurableObject(request, {});
  expect(result).toBeNull();
});

test("websocket upgrade on paddock/live without PADDOCK_ROOM binding returns null", () => {
  const request = buildWebSocketUpgradeRequest(
    "https://example.com/api/races/2026/06/03/43/10/paddock/live",
    "websocket",
  );
  const result = routeWebSocketUpgradeToDurableObject(request, {});
  expect(result).toBeNull();
});
