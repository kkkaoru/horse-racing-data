// Run with bun: `bunx vitest run src/viewer-race-cache-bust.test.ts`
import { afterEach, expect, test, vi } from "vitest";

import type { Env } from "./types";
import { parseRaceKey, triggerRaceCacheBust } from "./viewer-race-cache-bust";

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

test("parseRaceKey parses a JRA raceKey into source/year/mmdd/keibajoCode/raceBango", () => {
  expect(parseRaceKey("jra:20260628:05:11")).toStrictEqual({
    keibajoCode: "05",
    mmdd: "0628",
    raceBango: "11",
    source: "jra",
    year: "2026",
  });
});

test("parseRaceKey parses a NAR raceKey", () => {
  expect(parseRaceKey("nar:20260529:50:07")).toStrictEqual({
    keibajoCode: "50",
    mmdd: "0529",
    raceBango: "07",
    source: "nar",
    year: "2026",
  });
});

test("parseRaceKey returns null for an unknown source", () => {
  expect(parseRaceKey("ban:20260628:05:11")).toBeNull();
});

test("parseRaceKey returns null for a malformed key", () => {
  expect(parseRaceKey("not-a-race-key")).toBeNull();
});

test("triggerRaceCacheBust posts JSON and bearer auth, returns ok on 200", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response("{}", { status: 200 }));
  const outcome = await triggerRaceCacheBust(buildEnv(), {
    keibajoCode: "05",
    mmdd: "0628",
    raceBango: "11",
    source: "jra",
    year: "2026",
  });
  expect(outcome).toStrictEqual({ attempts: 1, status: "ok" });
  const call = fetchSpy.mock.calls[0];
  expect(call?.[0]).toBe("https://example.test/api/internal/race-cache-bust");
  expect(call?.[1]?.method).toBe("POST");
  const headers = call?.[1]?.headers as Record<string, string>;
  expect(headers["content-type"]).toBe("application/json");
  expect(headers["x-pc-keiba-internal-token"]).toBe("secret-token");
  expect(call?.[1]?.body).toBe(
    '{"keibajoCode":"05","mmdd":"0628","raceBango":"11","source":"jra","year":"2026"}',
  );
  expect(call?.[1]?.signal instanceof AbortSignal).toBe(true);
});

test("triggerRaceCacheBust does not retry on 4xx", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response("bad", { status: 400 }));
  const outcome = await triggerRaceCacheBust(buildEnv(), {
    keibajoCode: "05",
    mmdd: "0628",
    raceBango: "11",
    source: "jra",
    year: "2026",
  });
  expect(outcome).toStrictEqual({ attempts: 1, message: "HTTP 400", status: "error" });
  expect(fetchSpy).toHaveBeenCalledTimes(1);
});

test("triggerRaceCacheBust retries once on 5xx and reports final error", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(new Response("nope", { status: 502 }))
    .mockResolvedValueOnce(new Response("nope", { status: 503 }));
  const outcome = await triggerRaceCacheBust(buildEnv(), {
    keibajoCode: "05",
    mmdd: "0628",
    raceBango: "11",
    source: "jra",
    year: "2026",
  });
  expect(outcome).toStrictEqual({ attempts: 2, message: "HTTP 503", status: "error" });
  expect(fetchSpy).toHaveBeenCalledTimes(2);
});

test("triggerRaceCacheBust recovers on retry when first attempt is 5xx and second is 200", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(new Response("nope", { status: 502 }))
    .mockResolvedValueOnce(new Response("{}", { status: 200 }));
  const outcome = await triggerRaceCacheBust(buildEnv(), {
    keibajoCode: "50",
    mmdd: "0529",
    raceBango: "07",
    source: "nar",
    year: "2026",
  });
  expect(outcome).toStrictEqual({ attempts: 2, status: "ok" });
  expect(fetchSpy).toHaveBeenCalledTimes(2);
});

test("triggerRaceCacheBust retries when first attempt throws and succeeds on retry", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockRejectedValueOnce(new Error("boom"))
    .mockResolvedValueOnce(new Response("{}", { status: 200 }));
  const outcome = await triggerRaceCacheBust(buildEnv(), {
    keibajoCode: "50",
    mmdd: "0529",
    raceBango: "07",
    source: "nar",
    year: "2026",
  });
  expect(outcome).toStrictEqual({ attempts: 2, status: "ok" });
  expect(fetchSpy).toHaveBeenCalledTimes(2);
});

test("triggerRaceCacheBust reports thrown errors after exhausting retries", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockRejectedValueOnce(new Error("boom"))
    .mockRejectedValueOnce(new Error("still down"));
  const outcome = await triggerRaceCacheBust(buildEnv(), {
    keibajoCode: "50",
    mmdd: "0529",
    raceBango: "07",
    source: "nar",
    year: "2026",
  });
  expect(outcome).toStrictEqual({ attempts: 2, message: "still down", status: "error" });
  expect(fetchSpy).toHaveBeenCalledTimes(2);
});

test("triggerRaceCacheBust skips when no internal token is configured", async () => {
  const fetchSpy = vi.spyOn(globalThis, "fetch");
  const outcome = await triggerRaceCacheBust(
    buildEnv({ PC_KEIBA_VIEWER_INTERNAL_TOKEN: undefined }),
    {
      keibajoCode: "05",
      mmdd: "0628",
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

test("triggerRaceCacheBust falls back to the default viewer origin when not configured", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response("{}", { status: 200 }));
  await triggerRaceCacheBust(buildEnv({ RUNNING_STYLE_CACHE_ORIGIN: undefined }), {
    keibajoCode: "05",
    mmdd: "0628",
    raceBango: "11",
    source: "jra",
    year: "2026",
  });
  expect(fetchSpy.mock.calls[0]?.[0]).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/internal/race-cache-bust",
  );
});

test("triggerRaceCacheBust falls back to the default viewer origin when origin is blank", async () => {
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response("{}", { status: 200 }));
  await triggerRaceCacheBust(buildEnv({ RUNNING_STYLE_CACHE_ORIGIN: "   " }), {
    keibajoCode: "05",
    mmdd: "0628",
    raceBango: "11",
    source: "jra",
    year: "2026",
  });
  expect(fetchSpy.mock.calls[0]?.[0]).toBe(
    "https://pc-keiba-viewer.kkk4oru.com/api/internal/race-cache-bust",
  );
});
