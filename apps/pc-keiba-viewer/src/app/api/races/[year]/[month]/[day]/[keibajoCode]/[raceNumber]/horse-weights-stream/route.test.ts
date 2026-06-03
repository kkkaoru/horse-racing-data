// Run with bun. `bun run --filter pc-keiba-viewer test`
import { beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const mocks = vi.hoisted(() => ({
  proxyToProductionStreamMock:
    vi.fn<(path: string, searchParams: URLSearchParams) => Promise<Response>>(),
  realtimeDataFetchMock:
    vi.fn<(input: RequestInfo | URL, init?: RequestInit) => Promise<Response>>(),
  safeGetCloudflareEnvMock: vi.fn<() => Promise<unknown>>(),
  useProductionApiProxyMock: vi.fn<() => boolean>(),
}));

vi.mock("../../../../../../../../../lib/cloudflare-context.server", () => ({
  safeGetCloudflareEnv: mocks.safeGetCloudflareEnvMock,
}));

vi.mock("../../../../../../../../../lib/production-api-proxy.server", () => ({
  proxyToProductionStream: mocks.proxyToProductionStreamMock,
  useProductionApiProxy: mocks.useProductionApiProxyMock,
}));

const {
  proxyToProductionStreamMock,
  realtimeDataFetchMock,
  safeGetCloudflareEnvMock,
  useProductionApiProxyMock,
} = mocks;

import { GET } from "./route";

const buildRequest = (search: string): Request =>
  new Request(`https://example.com/api/races/2026/05/29/05/07/horse-weights-stream${search}`);

const buildContext = () => ({
  params: Promise.resolve({
    day: "29",
    keibajoCode: "05",
    month: "05",
    raceNumber: "07",
    year: "2026",
  }),
});

beforeEach(() => {
  proxyToProductionStreamMock.mockReset();
  realtimeDataFetchMock.mockReset();
  safeGetCloudflareEnvMock.mockReset();
  useProductionApiProxyMock.mockReset();
  useProductionApiProxyMock.mockReturnValue(false);
});

it("GET returns 400 when source query param is missing", async () => {
  const response = await GET(buildRequest(""), buildContext());
  expect(response.status).toBe(400);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({ error: "invalid source" });
});

it("GET returns 400 when source query param is neither jra nor nar", async () => {
  const response = await GET(buildRequest("?source=foo"), buildContext());
  expect(response.status).toBe(400);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({ error: "invalid source" });
});

it("GET returns 503 when proxy is disabled and REALTIME_DATA binding is missing", async () => {
  useProductionApiProxyMock.mockReturnValue(false);
  safeGetCloudflareEnvMock.mockResolvedValue(null);
  const response = await GET(buildRequest("?source=jra"), buildContext());
  expect(response.status).toBe(503);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({ error: "binding unavailable" });
});

it("GET returns 503 when proxy is disabled and env exists but REALTIME_DATA binding is undefined", async () => {
  useProductionApiProxyMock.mockReturnValue(false);
  safeGetCloudflareEnvMock.mockResolvedValue({});
  const response = await GET(buildRequest("?source=jra"), buildContext());
  expect(response.status).toBe(503);
});

it("GET proxies to production stream when useProductionApiProxy returns true", async () => {
  useProductionApiProxyMock.mockReturnValue(true);
  proxyToProductionStreamMock.mockResolvedValue(
    new Response("event: ping\ndata: {}\n\n", {
      headers: {
        "Cache-Control": "no-cache, no-transform",
        "Content-Type": "text/event-stream",
        "X-Horse-Weights-Stream-Source": "PROXIED-PRODUCTION",
      },
      status: 200,
    }),
  );
  const response = await GET(buildRequest("?source=jra"), buildContext());
  expect(response.status).toBe(200);
  expect(response.headers.get("X-Horse-Weights-Stream-Source")).toBe("PROXIED-PRODUCTION");
  expect(response.headers.get("Content-Type")).toBe("text/event-stream");
  expect(proxyToProductionStreamMock).toHaveBeenCalledTimes(1);
});

it("GET proxy fallback passes the upstream production stream path with year/month/day/keibajoCode/raceNumber", async () => {
  useProductionApiProxyMock.mockReturnValue(true);
  proxyToProductionStreamMock.mockResolvedValue(new Response(null, { status: 200 }));
  await GET(buildRequest("?source=jra"), buildContext());
  expect(proxyToProductionStreamMock.mock.calls[0]?.[0]).toBe(
    "/api/races/2026/05/29/05/07/horse-weights-stream",
  );
});

it("GET proxy fallback forwards the search params (including source) to the production stream", async () => {
  useProductionApiProxyMock.mockReturnValue(true);
  proxyToProductionStreamMock.mockResolvedValue(new Response(null, { status: 200 }));
  await GET(buildRequest("?source=nar&debug=1"), buildContext());
  const forwarded = proxyToProductionStreamMock.mock.calls[0]?.[1];
  if (!(forwarded instanceof URLSearchParams)) throw new Error("expected URLSearchParams");
  expect(forwarded.get("source")).toBe("nar");
  expect(forwarded.get("debug")).toBe("1");
});

it("GET proxy fallback skips the REALTIME_DATA binding lookup entirely", async () => {
  useProductionApiProxyMock.mockReturnValue(true);
  proxyToProductionStreamMock.mockResolvedValue(new Response(null, { status: 200 }));
  await GET(buildRequest("?source=jra"), buildContext());
  expect(safeGetCloudflareEnvMock).not.toHaveBeenCalled();
});

it("GET uses REALTIME_DATA binding fetch when proxy is disabled and binding is present", async () => {
  useProductionApiProxyMock.mockReturnValue(false);
  realtimeDataFetchMock.mockResolvedValue(
    new Response("event: hello\ndata: {}\n\n", {
      headers: { "Content-Type": "text/event-stream" },
      status: 200,
    }),
  );
  safeGetCloudflareEnvMock.mockResolvedValue({ REALTIME_DATA: { fetch: realtimeDataFetchMock } });
  const response = await GET(buildRequest("?source=jra"), buildContext());
  expect(response.status).toBe(200);
  expect(realtimeDataFetchMock).toHaveBeenCalledTimes(1);
});

it("GET binding path requests the upstream URL with padded raceNumber for single-digit input", async () => {
  useProductionApiProxyMock.mockReturnValue(false);
  realtimeDataFetchMock.mockResolvedValue(
    new Response("ok", {
      headers: { "Content-Type": "text/event-stream" },
      status: 200,
    }),
  );
  safeGetCloudflareEnvMock.mockResolvedValue({ REALTIME_DATA: { fetch: realtimeDataFetchMock } });
  await GET(
    new Request("https://example.com/api/races/2026/05/29/05/3/horse-weights-stream?source=jra"),
    {
      params: Promise.resolve({
        day: "29",
        keibajoCode: "05",
        month: "05",
        raceNumber: "3",
        year: "2026",
      }),
    },
  );
  expect(realtimeDataFetchMock.mock.calls[0]?.[0]).toBe(
    "https://realtime/api/jra/races/2026/05/29/05/03/horse-weights-stream",
  );
});

it("GET binding path requests the upstream URL with nar source segment", async () => {
  useProductionApiProxyMock.mockReturnValue(false);
  realtimeDataFetchMock.mockResolvedValue(
    new Response("ok", {
      headers: { "Content-Type": "text/event-stream" },
      status: 200,
    }),
  );
  safeGetCloudflareEnvMock.mockResolvedValue({ REALTIME_DATA: { fetch: realtimeDataFetchMock } });
  await GET(buildRequest("?source=nar"), buildContext());
  expect(realtimeDataFetchMock.mock.calls[0]?.[0]).toBe(
    "https://realtime/api/nar/races/2026/05/29/05/07/horse-weights-stream",
  );
});

it("GET binding path preserves upstream Content-Type when set to text/event-stream", async () => {
  useProductionApiProxyMock.mockReturnValue(false);
  realtimeDataFetchMock.mockResolvedValue(
    new Response("event: weight\ndata: {}\n\n", {
      headers: { "Content-Type": "text/event-stream" },
      status: 200,
    }),
  );
  safeGetCloudflareEnvMock.mockResolvedValue({ REALTIME_DATA: { fetch: realtimeDataFetchMock } });
  const response = await GET(buildRequest("?source=jra"), buildContext());
  expect(response.headers.get("Content-Type")).toBe("text/event-stream");
  expect(response.headers.get("Cache-Control")).toBe("no-cache, no-transform");
});

it("GET binding path falls back to default Content-Type when upstream lacks one", async () => {
  useProductionApiProxyMock.mockReturnValue(false);
  const upstream = new Response("event: ping\n\n", { status: 200 });
  upstream.headers.delete("Content-Type");
  realtimeDataFetchMock.mockResolvedValue(upstream);
  safeGetCloudflareEnvMock.mockResolvedValue({ REALTIME_DATA: { fetch: realtimeDataFetchMock } });
  const response = await GET(buildRequest("?source=jra"), buildContext());
  expect(response.headers.get("Content-Type")).toBe("text/event-stream");
});

it("GET binding path forwards the upstream status code", async () => {
  useProductionApiProxyMock.mockReturnValue(false);
  realtimeDataFetchMock.mockResolvedValue(
    new Response("upstream gateway error", {
      headers: { "Content-Type": "text/event-stream" },
      status: 502,
    }),
  );
  safeGetCloudflareEnvMock.mockResolvedValue({ REALTIME_DATA: { fetch: realtimeDataFetchMock } });
  const response = await GET(buildRequest("?source=jra"), buildContext());
  expect(response.status).toBe(502);
});
