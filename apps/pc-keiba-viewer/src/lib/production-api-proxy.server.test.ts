// Run with bun. `bun run --filter pc-keiba-viewer test`
import { afterEach, describe, expect, it, vi } from "vitest";

import { buildProductionApiUrl, fetchProductionApi } from "./production-api-proxy.server";

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
});
