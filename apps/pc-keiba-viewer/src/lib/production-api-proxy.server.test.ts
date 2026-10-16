// Run with bun. `bun run --filter pc-keiba-viewer test`
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  buildProductionApiUrl,
  fetchProductionApi,
  proxyToProductionStream,
} from "./production-api-proxy.server";

const setAccessEnv = (): void => {
  process.env.PC_KEIBA_PRODUCTION_API_ORIGIN = "https://example.test";
  process.env.PC_KEIBA_ACCESS_CLIENT_ID = "client-id";
  process.env.PC_KEIBA_ACCESS_CLIENT_SECRET = "client-secret";
};

// jsdom's DOMException does not extend Error, so simulate the real Node /
// Edge runtime shape: Error instance with a getter-only `message`. This is
// exactly the shape that triggers the original Next.js dev crash.
const makeGetterOnlyMessageError = (message: string, name: string): Error => {
  const error = new Error();
  error.name = name;
  Object.defineProperty(error, "message", {
    configurable: true,
    get: () => message,
  });
  return error;
};

const clearAccessEnv = (): void => {
  delete process.env.PC_KEIBA_PRODUCTION_API_ORIGIN;
  delete process.env.PC_KEIBA_ACCESS_CLIENT_ID;
  delete process.env.PC_KEIBA_ACCESS_CLIENT_SECRET;
};

describe("production-api-proxy.server", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
    vi.useRealTimers();
    clearAccessEnv();
  });

  it("buildProductionApiUrl prefixes the leading slash when missing", () => {
    setAccessEnv();
    expect(buildProductionApiUrl("api/foo")).toBe("https://example.test/api/foo");
  });

  it("buildProductionApiUrl keeps an explicit leading slash", () => {
    setAccessEnv();
    expect(buildProductionApiUrl("/api/foo")).toBe("https://example.test/api/foo");
  });

  it("buildProductionApiUrl strips trailing slashes from the configured origin", () => {
    process.env.PC_KEIBA_PRODUCTION_API_ORIGIN = "https://example.test///";
    process.env.PC_KEIBA_ACCESS_CLIENT_ID = "client-id";
    process.env.PC_KEIBA_ACCESS_CLIENT_SECRET = "client-secret";
    expect(buildProductionApiUrl("/api/foo")).toBe("https://example.test/api/foo");
  });

  it("fetchProductionApi throws when CF Access credentials are unavailable", async () => {
    await expect(fetchProductionApi("/api/foo")).rejects.toThrow(
      "Production Access credentials are unavailable.",
    );
  });

  it("fetchProductionApi returns the first successful upstream response", async () => {
    setAccessEnv();
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response("ok", { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const response = await fetchProductionApi("/api/foo");
    expect(response.status).toBe(200);
    expect(await response.text()).toBe("ok");
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("fetchProductionApi calls fetch with the absolute production URL", async () => {
    setAccessEnv();
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response("ok"));
    vi.stubGlobal("fetch", fetchMock);
    await fetchProductionApi("/api/foo");
    expect(fetchMock.mock.calls[0]?.[0]).toBe("https://example.test/api/foo");
  });

  it("fetchProductionApi attaches CF Access credentials as request headers", async () => {
    setAccessEnv();
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response("ok"));
    vi.stubGlobal("fetch", fetchMock);
    await fetchProductionApi("/api/foo");
    const init = fetchMock.mock.calls[0]?.[1];
    const headers = init?.headers;
    if (!(headers instanceof Headers)) throw new Error("expected Headers instance");
    expect(headers.get("CF-Access-Client-Id")).toBe("client-id");
    expect(headers.get("CF-Access-Client-Secret")).toBe("client-secret");
  });

  it("fetchProductionApi retries on a transient 502 then returns the next success", async () => {
    setAccessEnv();
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(new Response("retry", { status: 502 }))
      .mockResolvedValueOnce(new Response("ok", { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const response = await fetchProductionApi("/api/foo");
    expect(response.status).toBe(200);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("fetchProductionApi retries on a transient 503 then returns the next success", async () => {
    setAccessEnv();
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(new Response("retry", { status: 503 }))
      .mockResolvedValueOnce(new Response("ok", { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const response = await fetchProductionApi("/api/foo");
    expect(response.status).toBe(200);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("fetchProductionApi returns the transient response after exhausting retries", async () => {
    setAccessEnv();
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValue(new Response("still-down", { status: 502 }));
    vi.stubGlobal("fetch", fetchMock);
    const response = await fetchProductionApi("/api/foo");
    expect(response.status).toBe(502);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("fetchProductionApi retries on a network rejection then returns success", async () => {
    setAccessEnv();
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockRejectedValueOnce(new Error("network"))
      .mockResolvedValueOnce(new Response("ok", { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const response = await fetchProductionApi("/api/foo");
    expect(response.status).toBe(200);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("fetchProductionApi skips retry when the caller supplies an AbortSignal", async () => {
    setAccessEnv();
    const controller = new AbortController();
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValue(new Response("retry", { status: 502 }));
    vi.stubGlobal("fetch", fetchMock);
    const response = await fetchProductionApi("/api/foo", { signal: controller.signal });
    expect(response.status).toBe(502);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("fetchProductionApi rethrows a writable Error when upstream rejects with a getter-only-message Error", async () => {
    setAccessEnv();
    const timeout = makeGetterOnlyMessageError("timed out", "TimeoutError");
    const fetchMock = vi.fn<typeof fetch>().mockRejectedValue(timeout);
    vi.stubGlobal("fetch", fetchMock);
    const captured = await fetchProductionApi("/api/foo").catch((error: unknown) => error);
    expect(captured).toBeInstanceOf(Error);
    if (!(captured instanceof Error)) throw new Error("unreachable");
    expect(captured.name).toBe("TimeoutError");
    expect(captured.message).toBe("timed out");
    expect(captured.cause).toBeInstanceOf(Error);
    captured.message = "mutated";
    expect(captured.message).toBe("mutated");
  });

  it("fetchProductionApi original getter-only Error cannot mutate message (regression sentinel)", () => {
    const original = makeGetterOnlyMessageError("timed out", "TimeoutError");
    expect(() => {
      original.message = "mutated";
    }).toThrow(TypeError);
  });

  it("fetchProductionApi wraps a non-Error rejection in a fresh Error", async () => {
    setAccessEnv();
    const fetchMock = vi.fn<typeof fetch>().mockRejectedValue("plain-string");
    vi.stubGlobal("fetch", fetchMock);
    const captured = await fetchProductionApi("/api/foo").catch((error: unknown) => error);
    expect(captured).toBeInstanceOf(Error);
    if (!(captured instanceof Error)) throw new Error("unreachable");
    expect(captured.message).toBe("plain-string");
  });

  it("keeps an established stream alive past ten seconds and cancels with the client", async () => {
    setAccessEnv();
    vi.useFakeTimers();
    const client = new AbortController();
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response("event: ping\n\n"));
    vi.stubGlobal("fetch", fetchMock);
    const response = await proxyToProductionStream({
      path: "/stream",
      searchParams: new URLSearchParams(),
      signal: client.signal,
    });
    await vi.advanceTimersByTimeAsync(20_000);
    expect(fetchMock.mock.calls[0]?.[1]?.signal?.aborted).toBe(false);
    expect(vi.getTimerCount()).toBe(1);
    expect(await response.text()).toBe("event: ping\n\n");
    await vi.advanceTimersByTimeAsync(0);
    expect(vi.getTimerCount()).toBe(0);
    client.abort();
    expect(fetchMock.mock.calls[0]?.[1]?.signal?.aborted).toBe(true);
  });

  it("rotates an idle stream cleanly before the upstream five-minute timeout", async () => {
    setAccessEnv();
    vi.useFakeTimers();
    const cancel = vi.fn<() => void>();
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("event: ping\n\n"));
      },
      cancel,
    });
    vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockResolvedValue(new Response(body)));
    const response = await proxyToProductionStream({
      path: "/stream",
      searchParams: new URLSearchParams(),
      signal: new AbortController().signal,
    });
    await Promise.all([
      expect(response.text()).resolves.toBe("event: ping\n\n"),
      vi.advanceTimersByTimeAsync(240_000),
    ]);
    expect(cancel).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("cancels the upstream body and clears the lease when the client closes", async () => {
    setAccessEnv();
    vi.useFakeTimers();
    const cancel = vi.fn<() => void>();
    vi.stubGlobal(
      "fetch",
      vi
        .fn<typeof fetch>()
        .mockResolvedValue(new Response(new ReadableStream<Uint8Array>({ cancel }))),
    );
    const client = new AbortController();
    const response = await proxyToProductionStream({
      path: "/stream",
      searchParams: new URLSearchParams(),
      signal: client.signal,
    });
    client.abort();
    await response.body?.cancel();
    await vi.advanceTimersByTimeAsync(0);
    expect(cancel).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("logs an upstream body failure and closes normally for EventSource reconnection", async () => {
    setAccessEnv();
    const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.error(new Error("upstream disconnected"));
      },
    });
    vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockResolvedValue(new Response(body)));
    const response = await proxyToProductionStream({
      path: "/stream",
      searchParams: new URLSearchParams(),
      signal: new AbortController().signal,
    });
    expect(await response.text()).toBe("");
    expect(log).toHaveBeenCalledWith("Production event stream disconnected", expect.any(Error));
  });

  it("wraps a rejected stream connection without leaking a timer or retrying", async () => {
    setAccessEnv();
    vi.useFakeTimers();
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockRejectedValue(makeGetterOnlyMessageError("connection aborted", "AbortError"));
    vi.stubGlobal("fetch", fetchMock);
    const failure: unknown = await proxyToProductionStream({
      path: "/stream",
      searchParams: new URLSearchParams(),
      signal: new AbortController().signal,
    }).catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(Error);
    if (!(failure instanceof Error)) throw new Error("expected Error");
    expect(failure.name).toBe("AbortError");
    expect(failure.message).toBe("connection aborted");
    failure.message = "writable";
    expect(failure.message).toBe("writable");
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("aborts a stalled stream connection and clears its timer", async () => {
    setAccessEnv();
    vi.useFakeTimers();
    const fetchMock = vi.fn<typeof fetch>().mockImplementation(
      (_url, init) =>
        new Promise((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => reject(new Error("connection aborted")), {
            once: true,
          });
        }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const response = proxyToProductionStream({
      path: "/stream",
      searchParams: new URLSearchParams(),
      signal: new AbortController().signal,
    });
    await Promise.all([
      expect(response).rejects.toThrow("connection aborted"),
      vi.advanceTimersByTimeAsync(10_000),
    ]);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("proxyToProductionStream forwards upstream body and preserves event-stream Content-Type", async () => {
    setAccessEnv();
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      new Response("event: weight\ndata: {}\n\n", {
        headers: { "Cache-Control": "no-cache", "Content-Type": "text/event-stream" },
        status: 200,
      }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const response = await proxyToProductionStream({
      path: "/api/races/2026/05/29/05/07/horse-weights-stream",
      searchParams: new URLSearchParams("source=jra"),
      signal: new AbortController().signal,
    });
    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("text/event-stream");
    expect(response.headers.get("Cache-Control")).toBe("no-cache");
    expect(response.headers.get("X-Horse-Weights-Stream-Source")).toBe("PROXIED-PRODUCTION");
    expect(await response.text()).toBe("event: weight\ndata: {}\n\n");
  });

  it("proxyToProductionStream attaches the search params as a query string on the upstream URL", async () => {
    setAccessEnv();
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      new Response("ok", {
        headers: { "Content-Type": "text/event-stream" },
        status: 200,
      }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const response = await proxyToProductionStream({
      path: "/api/races/2026/05/29/05/07/horse-weights-stream",
      searchParams: new URLSearchParams("source=nar&debug=1"),
      signal: new AbortController().signal,
    });
    expect(await response.text()).toBe("ok");
    expect(fetchMock.mock.calls[0]?.[0]).toBe(
      "https://example.test/api/races/2026/05/29/05/07/horse-weights-stream?source=nar&debug=1",
    );
  });

  it("proxyToProductionStream omits the question mark when search params are empty", async () => {
    setAccessEnv();
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      new Response("ok", {
        headers: { "Content-Type": "text/event-stream" },
        status: 200,
      }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const response = await proxyToProductionStream({
      path: "/api/races/2026/05/29/05/07/horse-weights-stream",
      searchParams: new URLSearchParams(),
      signal: new AbortController().signal,
    });
    expect(await response.text()).toBe("ok");
    expect(fetchMock.mock.calls[0]?.[0]).toBe(
      "https://example.test/api/races/2026/05/29/05/07/horse-weights-stream",
    );
  });

  it("proxyToProductionStream falls back to default Cache-Control when upstream lacks one", async () => {
    setAccessEnv();
    const upstream = new Response("ok", {
      headers: { "Content-Type": "text/event-stream" },
      status: 200,
    });
    upstream.headers.delete("Cache-Control");
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(upstream);
    vi.stubGlobal("fetch", fetchMock);
    const response = await proxyToProductionStream({
      path: "/api/races/2026/05/29/05/07/horse-weights-stream",
      searchParams: new URLSearchParams("source=jra"),
      signal: new AbortController().signal,
    });
    expect(response.headers.get("Cache-Control")).toBe("no-cache, no-transform");
    expect(await response.text()).toBe("ok");
  });

  it("proxyToProductionStream falls back to default Content-Type when upstream lacks one", async () => {
    setAccessEnv();
    const upstream = new Response("ok", { status: 200 });
    upstream.headers.delete("Content-Type");
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(upstream);
    vi.stubGlobal("fetch", fetchMock);
    const response = await proxyToProductionStream({
      path: "/api/races/2026/05/29/05/07/horse-weights-stream",
      searchParams: new URLSearchParams("source=jra"),
      signal: new AbortController().signal,
    });
    expect(response.headers.get("Content-Type")).toBe("text/event-stream");
    expect(await response.text()).toBe("ok");
  });

  it("proxyToProductionStream returns a null-body Response when upstream body is null", async () => {
    setAccessEnv();
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      new Response(null, {
        headers: { "Content-Type": "text/event-stream" },
        status: 204,
      }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const response = await proxyToProductionStream({
      path: "/api/races/2026/05/29/05/07/horse-weights-stream",
      searchParams: new URLSearchParams("source=jra"),
      signal: new AbortController().signal,
    });
    expect(response.status).toBe(204);
    expect(response.body).toBeNull();
    expect(response.headers.get("X-Horse-Weights-Stream-Source")).toBe("PROXIED-PRODUCTION");
  });

  it("proxyToProductionStream forwards the upstream status code on error responses", async () => {
    setAccessEnv();
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      new Response("upstream-error", {
        headers: { "Content-Type": "text/event-stream" },
        status: 502,
      }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const response = await proxyToProductionStream({
      path: "/api/races/2026/05/29/05/07/horse-weights-stream",
      searchParams: new URLSearchParams("source=jra"),
      signal: new AbortController().signal,
    });
    expect(response.status).toBe(502);
    expect(await response.text()).toBe("upstream-error");
  });
});
