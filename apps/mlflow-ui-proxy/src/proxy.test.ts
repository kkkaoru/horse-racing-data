// Run with: bun run --filter mlflow-ui-proxy test
import { afterEach, expect, it, vi } from "vitest";

import { buildUpstreamRequest, proxyRequest } from "./proxy";
import type { ProxyEnv } from "./proxy";
import type { MlflowContainerStub } from "./types";

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

const buildEnv = (
  container: MlflowContainerStub,
  getByName: (name: string) => MlflowContainerStub = vi.fn(() => container),
): ProxyEnv => ({
  MLFLOW_CONTAINER: {
    getByName,
  },
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("buildUpstreamRequest preserves the request path and query string for the container", () => {
  const request = new Request("https://worker.test/api/2.0/mlflow/runs/search?max_results=5");
  const upstream = buildUpstreamRequest(request);
  expect(upstream.url).toBe("http://mlflow-container/api/2.0/mlflow/runs/search?max_results=5");
});

it("buildUpstreamRequest strips hop-by-hop, host, and authorization headers while forwarding an ordinary header", () => {
  const request = new Request("https://worker.test/", { headers: HOP_BY_HOP_TEST_HEADERS });
  const upstream = buildUpstreamRequest(request);
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
  const upstream = buildUpstreamRequest(request);
  expect(upstream.headers.get("origin")).toBe(null);
  expect(upstream.headers.get("referer")).toBe(null);
});

it("buildUpstreamRequest sends no body for a GET request", () => {
  const request = new Request("https://worker.test/", { method: "GET" });
  const upstream = buildUpstreamRequest(request);
  expect(upstream.body).toBe(null);
});

it("buildUpstreamRequest forwards the request body for a POST request", async () => {
  const request = new Request("https://worker.test/", {
    method: "POST",
    body: "run-payload",
  });
  const upstream = buildUpstreamRequest(request);
  await expect(upstream.text()).resolves.toBe("run-payload");
});

it("proxyRequest strips hop-by-hop response headers and adds X-Proxied-By on success", async () => {
  const container: MlflowContainerStub = {
    fetch: vi.fn(
      async () =>
        new Response("ok", {
          status: 200,
          headers: { connection: "keep-alive", "content-type": "text/plain" },
        }),
    ),
    syncProductionPreview: vi.fn(),
  };
  const response = await proxyRequest(
    new Request("https://worker.test/api/2.0/mlflow/experiments/list"),
    buildEnv(container),
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("connection")).toBe(null);
  expect(response.headers.get("content-type")).toBe("text/plain");
  expect(response.headers.get("X-Proxied-By")).toBe("mlflow-ui-proxy");
  await expect(response.text()).resolves.toBe("ok");
});

it("proxyRequest calls the primary container with the rewritten request", async () => {
  const capturedRequests: Request[] = [];
  const containerFetch = vi.fn(async (upstreamRequest: Request) => {
    capturedRequests.push(upstreamRequest);
    return new Response("ok", { status: 200 });
  });
  const container: MlflowContainerStub = {
    fetch: containerFetch,
    syncProductionPreview: vi.fn(),
  };
  const getByName = vi.fn(() => container);
  const env = buildEnv(container, getByName);
  await proxyRequest(new Request("https://worker.test/api/2.0/mlflow/experiments/list"), env);
  expect(getByName).toHaveBeenCalledWith("primary");
  expect(containerFetch).toHaveBeenCalledTimes(1);
  expect(capturedRequests[0]?.url).toBe("http://mlflow-container/api/2.0/mlflow/experiments/list");
});

it("proxyRequest returns a 502 when the container fetch rejects", async () => {
  const container: MlflowContainerStub = {
    fetch: vi.fn(async () => {
      throw new Error("container unreachable");
    }),
    syncProductionPreview: vi.fn(),
  };
  const response = await proxyRequest(new Request("https://worker.test/"), buildEnv(container));
  expect(response.status).toBe(502);
  await expect(response.text()).resolves.toBe("Bad Gateway: unable to reach MLflow origin");
});
