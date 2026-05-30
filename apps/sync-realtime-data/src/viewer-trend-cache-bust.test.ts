// Run with bun: `bunx vitest run src/viewer-trend-cache-bust.test.ts`
import { afterEach, expect, test, vi } from "vitest";

import type { Env } from "./types";
import { buildTrendBustFromRaceContext, requestTrendCacheBust } from "./viewer-trend-cache-bust";

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

const buildEnv = (overrides: Partial<Env> = {}): Env =>
  ({
    PC_KEIBA_VIEWER_INTERNAL_TOKEN: "secret-token",
    RUNNING_STYLE_CACHE_ORIGIN: "https://example.test",
    ...overrides,
  }) as unknown as Env;

test("buildTrendBustFromRaceContext builds source and targetYmd from the race context", () => {
  expect(
    buildTrendBustFromRaceContext({
      kaisaiNen: "2026",
      kaisaiTsukihi: "0525",
      source: "nar",
    }),
  ).toStrictEqual({ source: "nar", targetYmd: "20260525" });
});

test("requestTrendCacheBust posts JSON and bearer auth, returns ok on 200", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response("{}", { status: 200 }));
  const outcome = await requestTrendCacheBust(buildEnv(), {
    source: "jra",
    targetYmd: "20260525",
  });
  expect(outcome).toStrictEqual({ attempts: 1, status: "ok" });
  const call = fetchSpy.mock.calls[0];
  expect(call?.[0]).toBe("https://example.test/api/internal/trend-cache-bust");
  expect(call?.[1]?.method).toBe("POST");
  const headers = call?.[1]?.headers as Record<string, string>;
  expect(headers["content-type"]).toBe("application/json");
  expect(headers["x-pc-keiba-internal-token"]).toBe("secret-token");
  expect(call?.[1]?.body).toBe('{"source":"jra","targetYmd":"20260525"}');
  expect(call?.[1]?.signal instanceof AbortSignal).toBe(true);
});

test("requestTrendCacheBust does not retry on 4xx", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response("bad", { status: 400 }));
  const outcome = await requestTrendCacheBust(buildEnv(), {
    source: "nar",
    targetYmd: "20260525",
  });
  expect(outcome).toStrictEqual({ attempts: 1, message: "HTTP 400", status: "error" });
  expect(fetchSpy).toHaveBeenCalledTimes(1);
});

test("requestTrendCacheBust retries once on 5xx and reports final error", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(new Response("nope", { status: 502 }))
    .mockResolvedValueOnce(new Response("nope", { status: 503 }));
  const outcome = await requestTrendCacheBust(buildEnv(), {
    source: "nar",
    targetYmd: "20260525",
  });
  expect(outcome).toStrictEqual({ attempts: 2, message: "HTTP 503", status: "error" });
  expect(fetchSpy).toHaveBeenCalledTimes(2);
});

test("requestTrendCacheBust recovers on retry when first attempt is 5xx and second is 200", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(new Response("nope", { status: 502 }))
    .mockResolvedValueOnce(new Response("{}", { status: 200 }));
  const outcome = await requestTrendCacheBust(buildEnv(), {
    source: "nar",
    targetYmd: "20260525",
  });
  expect(outcome).toStrictEqual({ attempts: 2, status: "ok" });
  expect(fetchSpy).toHaveBeenCalledTimes(2);
});

test("requestTrendCacheBust retries when first attempt throws and succeeds on retry", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockRejectedValueOnce(new Error("boom"))
    .mockResolvedValueOnce(new Response("{}", { status: 200 }));
  const outcome = await requestTrendCacheBust(buildEnv(), {
    source: "nar",
    targetYmd: "20260525",
  });
  expect(outcome).toStrictEqual({ attempts: 2, status: "ok" });
  expect(fetchSpy).toHaveBeenCalledTimes(2);
});

test("requestTrendCacheBust reports thrown errors after exhausting retries", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockRejectedValueOnce(new Error("boom"))
    .mockRejectedValueOnce(new Error("still down"));
  const outcome = await requestTrendCacheBust(buildEnv(), {
    source: "nar",
    targetYmd: "20260525",
  });
  expect(outcome).toStrictEqual({ attempts: 2, message: "still down", status: "error" });
  expect(fetchSpy).toHaveBeenCalledTimes(2);
});

test("requestTrendCacheBust skips when no internal token is configured", async () => {
  const fetchSpy = vi.spyOn(globalThis, "fetch");
  const outcome = await requestTrendCacheBust(
    buildEnv({ PC_KEIBA_VIEWER_INTERNAL_TOKEN: undefined }),
    {
      source: "jra",
      targetYmd: "20260525",
    },
  );
  expect(outcome).toStrictEqual({
    message: "PC_KEIBA_VIEWER_INTERNAL_TOKEN not configured",
    status: "skipped",
  });
  expect(fetchSpy).not.toHaveBeenCalled();
});

test("requestTrendCacheBust rejects malformed targetYmd", async () => {
  const fetchSpy = vi.spyOn(globalThis, "fetch");
  const outcome = await requestTrendCacheBust(buildEnv(), {
    source: "jra",
    targetYmd: "2026-05-25",
  });
  expect(outcome).toStrictEqual({
    message: "invalid targetYmd: 2026-05-25",
    status: "error",
  });
  expect(fetchSpy).not.toHaveBeenCalled();
});

test("requestTrendCacheBust falls back to the default viewer origin when not configured", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response("{}", { status: 200 }));
  await requestTrendCacheBust(buildEnv({ RUNNING_STYLE_CACHE_ORIGIN: undefined }), {
    source: "jra",
    targetYmd: "20260525",
  });
  expect(fetchSpy.mock.calls[0]?.[0]).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/internal/trend-cache-bust",
  );
});

test("requestTrendCacheBust falls back to the default viewer origin when RUNNING_STYLE_CACHE_ORIGIN is blank", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response("{}", { status: 200 }));
  await requestTrendCacheBust(buildEnv({ RUNNING_STYLE_CACHE_ORIGIN: "   " }), {
    source: "jra",
    targetYmd: "20260525",
  });
  expect(fetchSpy.mock.calls[0]?.[0]).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/internal/trend-cache-bust",
  );
});
