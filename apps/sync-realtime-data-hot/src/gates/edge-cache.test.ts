// Run with bun.
import { afterEach, beforeEach, expect, it, vi } from "vitest";

import {
  isForceFreshRequest,
  purgeEdgeCache,
  readFromEdgeCache,
  writeToEdgeCache,
} from "./edge-cache";
import type { Env } from "../types";

interface CacheMockHandle {
  match: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
  delete: ReturnType<typeof vi.fn>;
}

const buildCacheMock = (): CacheMockHandle => ({
  delete: vi.fn(async () => true),
  match: vi.fn(async () => undefined),
  put: vi.fn(async () => undefined),
});

let cacheMock: CacheMockHandle;

beforeEach(() => {
  cacheMock = buildCacheMock();
  vi.stubGlobal("caches", { default: cacheMock });
});

afterEach(() => {
  vi.unstubAllGlobals();
});

const buildEnv = (overrides: Partial<Env> = {}): Env => ({ ...overrides }) as Env;

it("isForceFreshRequest returns true for header", () => {
  const request = new Request("https://x/y", {
    headers: { "X-Odds-Force-Fresh": "1" },
  });
  expect(isForceFreshRequest(request)).toBe(true);
});

it("isForceFreshRequest returns true for ?fresh=1 query", () => {
  expect(isForceFreshRequest(new Request("https://x/y?fresh=1"))).toBe(true);
});

it("isForceFreshRequest returns false without indicators", () => {
  expect(isForceFreshRequest(new Request("https://x/y"))).toBe(false);
});

it("readFromEdgeCache returns cached response", async () => {
  const cached = new Response("payload");
  cacheMock.match.mockResolvedValueOnce(cached);
  const result = await readFromEdgeCache("nar:20260528:42:01");
  expect(result).toBe(cached);
});

it("readFromEdgeCache returns null on miss", async () => {
  const result = await readFromEdgeCache("nar:20260528:42:01");
  expect(result).toBeNull();
});

it("writeToEdgeCache uses default TTL of 15s", async () => {
  await writeToEdgeCache("nar:20260528:42:01", { foo: "bar" }, buildEnv());
  expect(cacheMock.put).toHaveBeenCalledTimes(1);
  const [, response] = cacheMock.put.mock.calls[0]!;
  expect((response as Response).headers.get("Cache-Control")).toBe(
    "public, max-age=15, s-maxage=15",
  );
});

it("writeToEdgeCache honors env override TTL", async () => {
  await writeToEdgeCache(
    "nar:20260528:42:01",
    { foo: "bar" },
    buildEnv({ ODDS_EDGE_CACHE_TTL_SECONDS: "30" }),
  );
  const [, response] = cacheMock.put.mock.calls[0]!;
  expect((response as Response).headers.get("Cache-Control")).toBe(
    "public, max-age=30, s-maxage=30",
  );
});

it("writeToEdgeCache falls back to default when env value is invalid", async () => {
  await writeToEdgeCache(
    "nar:20260528:42:01",
    {},
    buildEnv({ ODDS_EDGE_CACHE_TTL_SECONDS: "bogus" }),
  );
  const [, response] = cacheMock.put.mock.calls[0]!;
  expect((response as Response).headers.get("Cache-Control")).toBe(
    "public, max-age=15, s-maxage=15",
  );
});

it("writeToEdgeCache falls back to default when env value is zero", async () => {
  await writeToEdgeCache("nar:20260528:42:01", {}, buildEnv({ ODDS_EDGE_CACHE_TTL_SECONDS: "0" }));
  const [, response] = cacheMock.put.mock.calls[0]!;
  expect((response as Response).headers.get("Cache-Control")).toBe(
    "public, max-age=15, s-maxage=15",
  );
});

it("purgeEdgeCache calls cache.delete with key", async () => {
  await purgeEdgeCache("nar:20260528:42:01");
  expect(cacheMock.delete).toHaveBeenCalledTimes(1);
});
