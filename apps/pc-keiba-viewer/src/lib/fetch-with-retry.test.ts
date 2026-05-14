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
