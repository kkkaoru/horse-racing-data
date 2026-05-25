// Run with bun: `bunx vitest run src/viewer-trend-cache-bust.test.ts`
import { afterEach, expect, test, vi } from "vitest";

import type { Env } from "./types";
import {
  buildTrendBustFromRaceContext,
  requestTrendCacheBust,
} from "./viewer-trend-cache-bust";

afterEach(() => {
  vi.restoreAllMocks();
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
  expect(outcome).toStrictEqual({ status: "ok" });
  const call = fetchSpy.mock.calls[0];
  expect(call?.[0]).toBe("https://example.test/api/internal/trend-cache-bust");
  expect(call?.[1]?.method).toBe("POST");
  expect(call?.[1]?.headers).toStrictEqual({
    "content-type": "application/json",
    "x-pc-keiba-internal-token": "secret-token",
  });
  expect(call?.[1]?.body).toBe('{"source":"jra","targetYmd":"20260525"}');
});

test("requestTrendCacheBust reports HTTP error status", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("nope", { status: 500 }));
  const outcome = await requestTrendCacheBust(buildEnv(), {
    source: "nar",
    targetYmd: "20260525",
  });
  expect(outcome).toStrictEqual({ message: "HTTP 500", status: "error" });
});

test("requestTrendCacheBust captures thrown errors as messages", async () => {
  vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("boom"));
  const outcome = await requestTrendCacheBust(buildEnv(), {
    source: "nar",
    targetYmd: "20260525",
  });
  expect(outcome).toStrictEqual({ message: "boom", status: "error" });
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
