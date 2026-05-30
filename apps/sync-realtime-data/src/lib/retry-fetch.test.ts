// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import {
  computeBackoffDelay,
  DEFAULT_BASE_DELAY_MS,
  DEFAULT_CAP_DELAY_MS,
  DEFAULT_MAX_ATTEMPTS,
  DEFAULT_RETRYABLE_STATUSES,
  fetchWithRetry,
  parseRetryAfter,
} from "./retry-fetch";

const TEST_URL = "https://example.test/page";
const FIXED_NOW_MS = 1_700_000_000_000;
const EMPTY_BODY = "";

interface SequenceResponse {
  status: number;
  body?: string;
  retryAfter?: string;
}

const makeResponse = (entry: SequenceResponse): Response => {
  const headers = new Headers();
  if (entry.retryAfter !== undefined) {
    headers.set("Retry-After", entry.retryAfter);
  }
  return new Response(entry.body ?? EMPTY_BODY, { status: entry.status, headers });
};

const stubFetchSequence = (responses: readonly SequenceResponse[]): ReturnType<typeof vi.fn> => {
  const calls: Response[] = responses.map(makeResponse);
  const mock = vi.fn(async () => {
    const next = calls.shift();
    if (!next) {
      throw new Error("no more mocked responses");
    }
    return next;
  });
  vi.stubGlobal("fetch", mock);
  return mock;
};

const noSleep = async (): Promise<void> => Promise.resolve();

beforeEach(() => {
  vi.unstubAllGlobals();
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

it("exports the documented defaults", () => {
  expect(DEFAULT_MAX_ATTEMPTS).toBe(3);
  expect(DEFAULT_BASE_DELAY_MS).toBe(1000);
  expect(DEFAULT_CAP_DELAY_MS).toBe(8000);
  expect(DEFAULT_RETRYABLE_STATUSES.has(408)).toBe(true);
  expect(DEFAULT_RETRYABLE_STATUSES.has(425)).toBe(true);
  expect(DEFAULT_RETRYABLE_STATUSES.has(429)).toBe(true);
  expect(DEFAULT_RETRYABLE_STATUSES.has(503)).toBe(true);
  expect(DEFAULT_RETRYABLE_STATUSES.has(404)).toBe(false);
});

it("computeBackoffDelay grows exponentially up to the cap", () => {
  expect(computeBackoffDelay({ attempt: 1, baseDelayMs: 1000, capDelayMs: 8000 })).toBe(1000);
  expect(computeBackoffDelay({ attempt: 2, baseDelayMs: 1000, capDelayMs: 8000 })).toBe(2000);
  expect(computeBackoffDelay({ attempt: 3, baseDelayMs: 1000, capDelayMs: 8000 })).toBe(4000);
  expect(computeBackoffDelay({ attempt: 5, baseDelayMs: 1000, capDelayMs: 8000 })).toBe(8000);
});

it("parseRetryAfter returns null when header missing", () => {
  expect(parseRetryAfter(null, FIXED_NOW_MS)).toBeNull();
});

it("parseRetryAfter returns null for empty header", () => {
  expect(parseRetryAfter("   ", FIXED_NOW_MS)).toBeNull();
});

it("parseRetryAfter parses seconds to milliseconds", () => {
  expect(parseRetryAfter("5", FIXED_NOW_MS)).toBe(5000);
});

it("parseRetryAfter rounds fractional seconds", () => {
  expect(parseRetryAfter("0.4", FIXED_NOW_MS)).toBe(400);
});

it("parseRetryAfter rejects negative seconds", () => {
  expect(parseRetryAfter("-3", FIXED_NOW_MS)).toBeNull();
});

it("parseRetryAfter parses HTTP-date relative to nowMs", () => {
  const fixedNow = Date.parse("Wed, 21 Oct 2026 07:28:00 GMT");
  const future = "Wed, 21 Oct 2026 07:28:05 GMT";
  expect(parseRetryAfter(future, fixedNow)).toBe(5000);
});

it("parseRetryAfter clamps past HTTP-date to zero", () => {
  const fixedNow = Date.parse("Wed, 21 Oct 2026 07:28:30 GMT");
  const past = "Wed, 21 Oct 2026 07:28:00 GMT";
  expect(parseRetryAfter(past, fixedNow)).toBe(0);
});

it("parseRetryAfter returns null for unparseable token", () => {
  expect(parseRetryAfter("not-a-date", FIXED_NOW_MS)).toBeNull();
});

it("fetchWithRetry returns the first 200 response without sleeping", async () => {
  stubFetchSequence([{ status: 200, body: "ok" }]);
  const sleep = vi.fn(noSleep);
  const response = await fetchWithRetry(TEST_URL, { sleep });
  expect(response.status).toBe(200);
  expect(sleep).toHaveBeenCalledTimes(0);
});

it("fetchWithRetry retries 429 twice then returns 200", async () => {
  const mock = stubFetchSequence([{ status: 429 }, { status: 429 }, { status: 200, body: "done" }]);
  const sleep = vi.fn(noSleep);
  const response = await fetchWithRetry(TEST_URL, { sleep });
  expect(response.status).toBe(200);
  expect(mock).toHaveBeenCalledTimes(3);
  expect(sleep).toHaveBeenCalledTimes(2);
});

it("fetchWithRetry exhausts attempts on repeated 429 and returns last response", async () => {
  const mock = stubFetchSequence([{ status: 429 }, { status: 429 }, { status: 429 }]);
  const sleep = vi.fn(noSleep);
  const response = await fetchWithRetry(TEST_URL, { sleep });
  expect(response.status).toBe(429);
  expect(mock).toHaveBeenCalledTimes(3);
  expect(sleep).toHaveBeenCalledTimes(2);
});

it("fetchWithRetry honors Retry-After seconds header before retrying", async () => {
  stubFetchSequence([{ status: 429, retryAfter: "5" }, { status: 200 }]);
  const sleep = vi.fn(noSleep);
  await fetchWithRetry(TEST_URL, { sleep });
  expect(sleep).toHaveBeenCalledTimes(1);
  expect(sleep).toHaveBeenNthCalledWith(1, 5000);
});

it("fetchWithRetry caps Retry-After seconds at capDelayMs", async () => {
  stubFetchSequence([{ status: 429, retryAfter: "60" }, { status: 200 }]);
  const sleep = vi.fn(noSleep);
  await fetchWithRetry(TEST_URL, { sleep });
  expect(sleep).toHaveBeenCalledTimes(1);
  expect(sleep).toHaveBeenNthCalledWith(1, 8000);
});

it("fetchWithRetry honors Retry-After HTTP-date header", async () => {
  const fixedNow = Date.parse("Wed, 21 Oct 2026 07:28:00 GMT");
  stubFetchSequence([
    { status: 503, retryAfter: "Wed, 21 Oct 2026 07:28:03 GMT" },
    { status: 200 },
  ]);
  const sleep = vi.fn(noSleep);
  const now = vi.fn(() => fixedNow);
  await fetchWithRetry(TEST_URL, { sleep, now });
  expect(sleep).toHaveBeenCalledTimes(1);
  expect(sleep).toHaveBeenNthCalledWith(1, 3000);
});

it("fetchWithRetry falls back to exponential backoff when Retry-After is invalid", async () => {
  stubFetchSequence([{ status: 429, retryAfter: "not-a-date" }, { status: 429 }, { status: 200 }]);
  const sleep = vi.fn(noSleep);
  await fetchWithRetry(TEST_URL, { sleep });
  expect(sleep).toHaveBeenCalledTimes(2);
  expect(sleep).toHaveBeenNthCalledWith(1, 1000);
  expect(sleep).toHaveBeenNthCalledWith(2, 2000);
});

it("fetchWithRetry retries 408 then succeeds", async () => {
  const mock = stubFetchSequence([{ status: 408 }, { status: 200 }]);
  const sleep = vi.fn(noSleep);
  const response = await fetchWithRetry(TEST_URL, { sleep });
  expect(response.status).toBe(200);
  expect(mock).toHaveBeenCalledTimes(2);
});

it("fetchWithRetry retries 425 then succeeds", async () => {
  const mock = stubFetchSequence([{ status: 425 }, { status: 200 }]);
  const sleep = vi.fn(noSleep);
  const response = await fetchWithRetry(TEST_URL, { sleep });
  expect(response.status).toBe(200);
  expect(mock).toHaveBeenCalledTimes(2);
});

it("fetchWithRetry retries 503 then succeeds", async () => {
  const mock = stubFetchSequence([{ status: 503 }, { status: 200 }]);
  const sleep = vi.fn(noSleep);
  const response = await fetchWithRetry(TEST_URL, { sleep });
  expect(response.status).toBe(200);
  expect(mock).toHaveBeenCalledTimes(2);
});

it("fetchWithRetry returns 404 immediately without retry", async () => {
  const mock = stubFetchSequence([{ status: 404 }]);
  const sleep = vi.fn(noSleep);
  const response = await fetchWithRetry(TEST_URL, { sleep });
  expect(response.status).toBe(404);
  expect(mock).toHaveBeenCalledTimes(1);
  expect(sleep).toHaveBeenCalledTimes(0);
});

it("fetchWithRetry returns 500 immediately without retry", async () => {
  const mock = stubFetchSequence([{ status: 500 }]);
  const sleep = vi.fn(noSleep);
  const response = await fetchWithRetry(TEST_URL, { sleep });
  expect(response.status).toBe(500);
  expect(mock).toHaveBeenCalledTimes(1);
});

it("fetchWithRetry propagates network errors without retry", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => {
      throw new TypeError("network down");
    }),
  );
  const sleep = vi.fn(noSleep);
  await expect(fetchWithRetry(TEST_URL, { sleep })).rejects.toThrowError("network down");
  expect(sleep).toHaveBeenCalledTimes(0);
});

it("fetchWithRetry forwards init headers to fetch", async () => {
  const mock = stubFetchSequence([{ status: 200 }]);
  await fetchWithRetry(TEST_URL, {
    sleep: noSleep,
    init: { headers: { "User-Agent": "ut-agent" } },
  });
  expect(mock).toHaveBeenCalledTimes(1);
  expect(mock.mock.calls[0]?.[1]).toStrictEqual({
    headers: { "User-Agent": "ut-agent" },
  });
});

it("fetchWithRetry respects custom retryableStatuses set", async () => {
  const mock = stubFetchSequence([{ status: 418 }, { status: 200 }]);
  const sleep = vi.fn(noSleep);
  const response = await fetchWithRetry(TEST_URL, {
    sleep,
    retryableStatuses: new Set([418]),
  });
  expect(response.status).toBe(200);
  expect(mock).toHaveBeenCalledTimes(2);
});

it("fetchWithRetry respects custom maxAttempts of 1 (no retry)", async () => {
  const mock = stubFetchSequence([{ status: 429 }]);
  const sleep = vi.fn(noSleep);
  const response = await fetchWithRetry(TEST_URL, { sleep, maxAttempts: 1 });
  expect(response.status).toBe(429);
  expect(mock).toHaveBeenCalledTimes(1);
  expect(sleep).toHaveBeenCalledTimes(0);
});

it("fetchWithRetry uses exponential backoff schedule by default", async () => {
  stubFetchSequence([{ status: 429 }, { status: 429 }, { status: 200 }]);
  const sleep = vi.fn(noSleep);
  await fetchWithRetry(TEST_URL, { sleep });
  expect(sleep).toHaveBeenNthCalledWith(1, 1000);
  expect(sleep).toHaveBeenNthCalledWith(2, 2000);
});

it("fetchWithRetry uses real defaultSleep when sleep option omitted", async () => {
  const mock = stubFetchSequence([{ status: 429 }, { status: 200 }]);
  const setTimeoutSpy = vi.spyOn(globalThis, "setTimeout");
  await fetchWithRetry(TEST_URL, { baseDelayMs: 1, capDelayMs: 1 });
  expect(mock).toHaveBeenCalledTimes(2);
  expect(setTimeoutSpy).toHaveBeenCalled();
});

it("fetchWithRetry uses defaultNow when now option omitted", async () => {
  stubFetchSequence([
    { status: 503, retryAfter: "Wed, 01 Jan 2000 00:00:00 GMT" },
    { status: 200 },
  ]);
  const sleep = vi.fn(noSleep);
  const response = await fetchWithRetry(TEST_URL, { sleep });
  expect(response.status).toBe(200);
  expect(sleep).toHaveBeenCalledWith(0);
});
