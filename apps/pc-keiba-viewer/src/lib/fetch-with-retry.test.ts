import { afterEach, beforeEach, expect, it, vi } from "vitest";

import { fetchWithRetry } from "./fetch-with-retry";

type FetchSignature = typeof fetch;

const noSleep = (): Promise<void> => Promise.resolve();
const originalFetch = globalThis.fetch;

beforeEach(() => {
  vi.unstubAllGlobals();
});

afterEach(() => {
  vi.unstubAllGlobals();
  globalThis.fetch = originalFetch;
});

it("returns the first OK response without retrying", async () => {
  const okResponse = new Response("ok", { status: 200 });
  const fetchMock = vi.fn<FetchSignature>().mockResolvedValue(okResponse);
  vi.stubGlobal("fetch", fetchMock);

  const response = await fetchWithRetry("https://example.test/data", undefined, {
    sleep: noSleep,
  });

  expect(response.status).toBe(200);
  expect(fetchMock).toHaveBeenCalledTimes(1);
});

it("retries on a retryable HTTP status until success", async () => {
  const fetchMock = vi
    .fn<FetchSignature>()
    .mockResolvedValueOnce(new Response("err", { status: 503, statusText: "Unavailable" }))
    .mockResolvedValueOnce(new Response("err", { status: 502 }))
    .mockResolvedValueOnce(new Response("ok", { status: 200 }));
  vi.stubGlobal("fetch", fetchMock);

  const response = await fetchWithRetry("https://example.test/data", undefined, {
    attempts: 3,
    sleep: noSleep,
  });

  expect(response.status).toBe(200);
  expect(fetchMock).toHaveBeenCalledTimes(3);
});

it("retries on network errors thrown by fetch", async () => {
  const fetchMock = vi
    .fn<FetchSignature>()
    .mockRejectedValueOnce(new TypeError("network failure"))
    .mockResolvedValueOnce(new Response("ok", { status: 200 }));
  vi.stubGlobal("fetch", fetchMock);

  const response = await fetchWithRetry("https://example.test/data", undefined, {
    attempts: 2,
    sleep: noSleep,
  });

  expect(response.status).toBe(200);
  expect(fetchMock).toHaveBeenCalledTimes(2);
});

it("falls back to XMLHttpRequest when an injected fetch wrapper breaks a same-origin GET", async () => {
  const fetchMock = vi.fn<FetchSignature>().mockRejectedValue(new TypeError("Failed to fetch"));
  vi.stubGlobal("fetch", fetchMock);
  const open = vi.fn<(method: string, url: string | URL) => void>();
  const send = vi.fn<(this: XMLHttpRequest) => void>(function (this: XMLHttpRequest) {
    Object.defineProperties(this, {
      responseText: { configurable: true, value: '{"ok":true}' },
      status: { configurable: true, value: 200 },
      statusText: { configurable: true, value: "OK" },
    });
    this.dispatchEvent(new Event("load"));
  });
  const xhr = new XMLHttpRequest();
  vi.spyOn(xhr, "open").mockImplementation(open);
  vi.spyOn(xhr, "send").mockImplementation(send);
  vi.stubGlobal(
    "XMLHttpRequest",
    vi.fn(function () {
      return xhr;
    }),
  );

  const response = await fetchWithRetry("/api/race", undefined, { sleep: noSleep });

  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ ok: true });
  expect(open).toHaveBeenCalledWith("GET", "http://localhost:3000/api/race");
  expect(send).toHaveBeenCalledTimes(1);
});

it("does not use XMLHttpRequest for a cross-origin fetch failure", async () => {
  const fetchMock = vi.fn<FetchSignature>().mockRejectedValue(new TypeError("Failed to fetch"));
  vi.stubGlobal("fetch", fetchMock);
  const xhr = vi.fn<() => XMLHttpRequest>();
  vi.stubGlobal("XMLHttpRequest", xhr);

  await expect(
    fetchWithRetry("https://example.test/data", undefined, { attempts: 1, sleep: noSleep }),
  ).rejects.toThrow("Failed to fetch");

  expect(xhr).toHaveBeenCalledTimes(0);
});

it("does not use XMLHttpRequest for a failed same-origin POST", async () => {
  const fetchMock = vi.fn<FetchSignature>().mockRejectedValue(new TypeError("Failed to fetch"));
  vi.stubGlobal("fetch", fetchMock);
  const xhr = vi.fn<() => XMLHttpRequest>();
  vi.stubGlobal("XMLHttpRequest", xhr);

  await expect(
    fetchWithRetry(
      "/api/race",
      { body: "payload", method: "POST" },
      { attempts: 1, sleep: noSleep },
    ),
  ).rejects.toThrow("Failed to fetch");

  expect(xhr).toHaveBeenCalledTimes(0);
});

it("does not retry on AbortError", async () => {
  const abortError = new DOMException("aborted", "AbortError");
  const fetchMock = vi.fn<FetchSignature>().mockRejectedValue(abortError);
  vi.stubGlobal("fetch", fetchMock);

  await expect(
    fetchWithRetry("https://example.test/data", undefined, {
      attempts: 3,
      sleep: noSleep,
    }),
  ).rejects.toThrow("aborted");
  expect(fetchMock).toHaveBeenCalledTimes(1);
});

it("returns non-retryable error responses without retrying", async () => {
  const errResponse = new Response("not found", { status: 404 });
  const fetchMock = vi.fn<FetchSignature>().mockResolvedValue(errResponse);
  vi.stubGlobal("fetch", fetchMock);

  const response = await fetchWithRetry("https://example.test/data", undefined, {
    sleep: noSleep,
  });

  expect(response.status).toBe(404);
  expect(fetchMock).toHaveBeenCalledTimes(1);
});

it("respects a custom retryStatuses set", async () => {
  const fetchMock = vi
    .fn<FetchSignature>()
    .mockResolvedValueOnce(new Response("err", { status: 418 }))
    .mockResolvedValueOnce(new Response("ok", { status: 200 }));
  vi.stubGlobal("fetch", fetchMock);

  const response = await fetchWithRetry("https://example.test/data", undefined, {
    attempts: 2,
    retryStatuses: new Set([418]),
    sleep: noSleep,
  });

  expect(response.status).toBe(200);
  expect(fetchMock).toHaveBeenCalledTimes(2);
});

it("delegates retry decisions to a custom shouldRetry callback", async () => {
  const fetchMock = vi.fn<FetchSignature>().mockRejectedValue(new TypeError("network failure"));
  const shouldRetry = vi.fn<(error: unknown, attempt: number) => boolean>().mockReturnValue(false);
  vi.stubGlobal("fetch", fetchMock);

  await expect(
    fetchWithRetry("https://example.test/data", undefined, {
      attempts: 3,
      shouldRetry,
      sleep: noSleep,
    }),
  ).rejects.toThrow("network failure");

  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(shouldRetry).toHaveBeenCalledTimes(1);
});
