// Run with: bun run --filter mlflow-ui-proxy test
import { afterEach, expect, it, vi } from "vitest";

import { buildUpstreamRequest, proxyRequest } from "./proxy";
import type { Env } from "./types";

const TEST_ENV: Env = {
  MLFLOW_ORIGIN: "https://mlflow-origin.test",
  MLFLOW_UI_USERNAME: "operator",
  MLFLOW_UI_PASSWORD: "s3cret",
};

const HOP_BY_HOP_TEST_HEADERS = {
  connection: "keep-alive",
  "keep-alive": "timeout=5",
  "proxy-authenticate": "Basic",
  "proxy-authorization": "Basic abc",
  te: "trailers",
  trailer: "X-Custom",
  "transfer-encoding": "chunked",
  upgrade: "websocket",
  host: "client.test",
  authorization: "Basic client-creds",
  origin: "https://mlflow-ui-proxy.kaoru.workers.dev",
  referer: "https://mlflow-ui-proxy.kaoru.workers.dev/",
  accept: "application/json",
};

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

it("buildUpstreamRequest preserves the request path and query string against the given origin", () => {
  const request = new Request("https://worker.test/api/2.0/mlflow/runs/search?max_results=5");
  const upstream = buildUpstreamRequest(request, "https://mlflow-origin.test");
  expect(upstream.url).toBe("https://mlflow-origin.test/api/2.0/mlflow/runs/search?max_results=5");
});

it("buildUpstreamRequest strips hop-by-hop, host, and authorization headers while forwarding an ordinary header", () => {
  const request = new Request("https://worker.test/", { headers: HOP_BY_HOP_TEST_HEADERS });
  const upstream = buildUpstreamRequest(request, "https://mlflow-origin.test");
  expect(upstream.headers.get("connection")).toBe(null);
  expect(upstream.headers.get("keep-alive")).toBe(null);
  expect(upstream.headers.get("proxy-authenticate")).toBe(null);
  expect(upstream.headers.get("proxy-authorization")).toBe(null);
  expect(upstream.headers.get("te")).toBe(null);
  expect(upstream.headers.get("trailer")).toBe(null);
  expect(upstream.headers.get("transfer-encoding")).toBe(null);
  expect(upstream.headers.get("upgrade")).toBe(null);
  expect(upstream.headers.get("host")).toBe(null);
  expect(upstream.headers.get("authorization")).toBe(null);
  expect(upstream.headers.get("origin")).toBe(null);
  expect(upstream.headers.get("referer")).toBe(null);
  expect(upstream.headers.get("accept")).toBe("application/json");
});

it("buildUpstreamRequest strips Origin and Referer even when no other headers are present", () => {
  const request = new Request("https://worker.test/ajax-api/3.0/mlflow/traces/search", {
    method: "POST",
    headers: {
      origin: "https://mlflow-ui-proxy.kaoru.workers.dev",
      referer: "https://mlflow-ui-proxy.kaoru.workers.dev/experiments/1",
    },
    body: "{}",
  });
  const upstream = buildUpstreamRequest(request, "https://mlflow-origin.test");
  expect(upstream.headers.get("origin")).toBe(null);
  expect(upstream.headers.get("referer")).toBe(null);
});

it("buildUpstreamRequest sends no body for a GET request", () => {
  const request = new Request("https://worker.test/", { method: "GET" });
  const upstream = buildUpstreamRequest(request, "https://mlflow-origin.test");
  expect(upstream.body).toBe(null);
});

it("buildUpstreamRequest forwards the request body for a POST request", async () => {
  const request = new Request("https://worker.test/", {
    method: "POST",
    body: "run-payload",
  });
  const upstream = buildUpstreamRequest(request, "https://mlflow-origin.test");
  await expect(upstream.text()).resolves.toBe("run-payload");
});

it("proxyRequest strips hop-by-hop response headers and adds X-Proxied-By on success", async () => {
  const upstreamResponse = new Response("ok", {
    status: 200,
    headers: { connection: "keep-alive", "content-type": "text/plain" },
  });
  const fetchMock = vi.fn(async () => upstreamResponse);
  vi.stubGlobal("fetch", fetchMock);
  const request = new Request("https://worker.test/api/2.0/mlflow/experiments/list");
  const response = await proxyRequest(request, TEST_ENV);
  expect(response.status).toBe(200);
  expect(response.headers.get("connection")).toBe(null);
  expect(response.headers.get("content-type")).toBe("text/plain");
  expect(response.headers.get("X-Proxied-By")).toBe("mlflow-ui-proxy");
  await expect(response.text()).resolves.toBe("ok");
});

it("proxyRequest calls fetch with a request targeting the MLFLOW_ORIGIN host", async () => {
  const capturedRequests: Request[] = [];
  const fetchMock = vi.fn(async (upstreamRequest: Request) => {
    capturedRequests.push(upstreamRequest);
    return new Response("ok", { status: 200 });
  });
  vi.stubGlobal("fetch", fetchMock);
  const request = new Request("https://worker.test/api/2.0/mlflow/experiments/list");
  await proxyRequest(request, TEST_ENV);
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(capturedRequests[0]?.url).toBe(
    "https://mlflow-origin.test/api/2.0/mlflow/experiments/list",
  );
});

it("proxyRequest returns a 502 when the upstream fetch rejects", async () => {
  const fetchMock = vi.fn(async () => {
    throw new Error("network unreachable");
  });
  vi.stubGlobal("fetch", fetchMock);
  const request = new Request("https://worker.test/");
  const response = await proxyRequest(request, TEST_ENV);
  expect(response.status).toBe(502);
  await expect(response.text()).resolves.toBe("Bad Gateway: unable to reach MLflow origin");
});
