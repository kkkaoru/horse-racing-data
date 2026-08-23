// Run with bun.
import { afterEach, expect, test, vi } from "vitest";
import type { Env } from "./types";
import {
  predictionCacheSourceForCategory,
  triggerPredictionCacheBust,
} from "./prediction-cache-bust";

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

const buildEnv = (overrides: Partial<Env> = {}): Env =>
  ({
    PC_KEIBA_VIEWER_INTERNAL_TOKEN: "secret-token",
    PC_KEIBA_VIEWER_ORIGIN: "https://example.test",
    ...overrides,
  }) as unknown as Env;

test("predictionCacheSourceForCategory maps ban-ei onto nar", () => {
  expect(predictionCacheSourceForCategory("jra")).toBe("jra");
  expect(predictionCacheSourceForCategory("nar")).toBe("nar");
  expect(predictionCacheSourceForCategory("ban-ei")).toBe("nar");
});

test("triggerPredictionCacheBust posts JSON and internal token, returns ok on 200", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response("{}", { status: 200 }));
  const outcome = await triggerPredictionCacheBust(buildEnv(), {
    keibajoCode: "05",
    mmdd: "0809",
    raceBango: "11",
    source: "jra",
    year: "2026",
  });
  expect(outcome).toStrictEqual({ attempts: 1, status: "ok" });
  const call = fetchSpy.mock.calls[0];
  expect(call?.[0]).toBe("https://example.test/api/internal/prediction-cache-bust");
  expect(call?.[1]?.method).toBe("POST");
  const headers = call?.[1]?.headers as Record<string, string>;
  expect(headers["content-type"]).toBe("application/json");
  expect(headers["x-pc-keiba-internal-token"]).toBe("secret-token");
  expect(call?.[1]?.body).toBe(
    '{"keibajoCode":"05","mmdd":"0809","raceBango":"11","source":"jra","year":"2026"}',
  );
  expect(call?.[1]?.signal instanceof AbortSignal).toBe(true);
});

test("triggerPredictionCacheBust prefers the viewer service binding", async () => {
  const globalFetchSpy = vi.spyOn(globalThis, "fetch");
  const serviceFetch = vi.fn<typeof fetch>().mockResolvedValue(new Response("{}", { status: 200 }));
  const outcome = await triggerPredictionCacheBust(
    buildEnv({ PC_KEIBA_VIEWER: { fetch: serviceFetch } }),
    {
      keibajoCode: "83",
      mmdd: "0823",
      raceBango: "12",
      source: "nar",
      year: "2026",
    },
  );

  expect(outcome).toStrictEqual({ attempts: 1, status: "ok" });
  expect(globalFetchSpy).not.toHaveBeenCalled();
  expect(serviceFetch).toHaveBeenCalledTimes(1);
  expect(serviceFetch.mock.calls[0]?.[0]).toBe(
    "https://example.test/api/internal/prediction-cache-bust",
  );
  expect(
    new Headers(serviceFetch.mock.calls[0]?.[1]?.headers).get("x-pc-keiba-internal-token"),
  ).toBe("secret-token");
});

test("triggerPredictionCacheBust does not retry on 4xx", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response("bad", { status: 400 }));
  const outcome = await triggerPredictionCacheBust(buildEnv(), {
    keibajoCode: "05",
    mmdd: "0809",
    raceBango: "11",
    source: "jra",
    year: "2026",
  });
  expect(outcome).toStrictEqual({ attempts: 1, message: "HTTP 400", status: "error" });
  expect(fetchSpy).toHaveBeenCalledTimes(1);
});

test("triggerPredictionCacheBust retries once on 5xx and reports final error", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(new Response("nope", { status: 502 }))
    .mockResolvedValueOnce(new Response("nope", { status: 503 }));
  const outcome = await triggerPredictionCacheBust(buildEnv(), {
    keibajoCode: "05",
    mmdd: "0809",
    raceBango: "11",
    source: "jra",
    year: "2026",
  });
  expect(outcome).toStrictEqual({ attempts: 2, message: "HTTP 503", status: "error" });
  expect(fetchSpy).toHaveBeenCalledTimes(2);
});

test("triggerPredictionCacheBust retries a non-Error network throw then succeeds", async () => {
  vi.useFakeTimers();
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockRejectedValueOnce("socket reset")
    .mockResolvedValueOnce(new Response("{}", { status: 200 }));
  const pending = triggerPredictionCacheBust(buildEnv(), {
    keibajoCode: "05",
    mmdd: "0809",
    raceBango: "11",
    source: "jra",
    year: "2026",
  });
  await vi.advanceTimersByTimeAsync(200);
  const outcome = await pending;
  expect(outcome).toStrictEqual({ attempts: 2, status: "ok" });
  expect(fetchSpy).toHaveBeenCalledTimes(2);
});

test("triggerPredictionCacheBust retries once on network error then succeeds", async () => {
  vi.useFakeTimers();
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockRejectedValueOnce(new Error("network down"))
    .mockResolvedValueOnce(new Response("{}", { status: 200 }));
  const pending = triggerPredictionCacheBust(buildEnv(), {
    keibajoCode: "44",
    mmdd: "0809",
    raceBango: "01",
    source: "nar",
    year: "2026",
  });
  await vi.advanceTimersByTimeAsync(200);
  const outcome = await pending;
  expect(outcome).toStrictEqual({ attempts: 2, status: "ok" });
  expect(fetchSpy).toHaveBeenCalledTimes(2);
});

test("triggerPredictionCacheBust skips when internal token is unset", async () => {
  const fetchSpy = vi.spyOn(globalThis, "fetch");
  const outcome = await triggerPredictionCacheBust(
    buildEnv({ PC_KEIBA_VIEWER_INTERNAL_TOKEN: undefined }),
    {
      keibajoCode: "05",
      mmdd: "0809",
      raceBango: "11",
      source: "jra",
      year: "2026",
    },
  );
  expect(outcome).toStrictEqual({
    message: "PC_KEIBA_VIEWER_INTERNAL_TOKEN not configured",
    status: "skipped",
  });
  expect(fetchSpy).not.toHaveBeenCalled();
});

test("triggerPredictionCacheBust skips when internal token is blank", async () => {
  const fetchSpy = vi.spyOn(globalThis, "fetch");
  const outcome = await triggerPredictionCacheBust(
    buildEnv({ PC_KEIBA_VIEWER_INTERNAL_TOKEN: "   " }),
    {
      keibajoCode: "05",
      mmdd: "0809",
      raceBango: "11",
      source: "jra",
      year: "2026",
    },
  );
  expect(outcome).toStrictEqual({
    message: "PC_KEIBA_VIEWER_INTERNAL_TOKEN not configured",
    status: "skipped",
  });
  expect(fetchSpy).not.toHaveBeenCalled();
});

test("triggerPredictionCacheBust falls back to the default viewer origin when origin is unset", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response("{}", { status: 200 }));
  await triggerPredictionCacheBust(buildEnv({ PC_KEIBA_VIEWER_ORIGIN: undefined }), {
    keibajoCode: "05",
    mmdd: "0809",
    raceBango: "11",
    source: "jra",
    year: "2026",
  });
  expect(fetchSpy.mock.calls[0]?.[0]).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/internal/prediction-cache-bust",
  );
});

test("triggerPredictionCacheBust falls back to the default viewer origin when origin is blank", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response("{}", { status: 200 }));
  await triggerPredictionCacheBust(buildEnv({ PC_KEIBA_VIEWER_ORIGIN: "   " }), {
    keibajoCode: "05",
    mmdd: "0809",
    raceBango: "11",
    source: "jra",
    year: "2026",
  });
  expect(fetchSpy.mock.calls[0]?.[0]).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/internal/prediction-cache-bust",
  );
});
