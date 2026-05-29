// Run with: bun run --filter sync-realtime-data-features test
import { beforeEach, expect, it, vi } from "vitest";

import {
  purgeRaceTrendEdgeCache,
  readRaceTrendFromEdgeCache,
  writeRaceTrendToEdgeCache,
} from "./edge-cache";
import type { Env } from "../types";

const buildCaches = () => ({
  default: {
    match: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
  },
});

beforeEach(() => {
  vi.unstubAllGlobals();
});

it("returns cached response when present", async () => {
  const cachesStub = buildCaches();
  const cachedResponse = new Response("{}");
  cachesStub.default.match.mockResolvedValueOnce(cachedResponse);
  vi.stubGlobal("caches", cachesStub);
  const result = await readRaceTrendFromEdgeCache("k");
  expect(result).toBe(cachedResponse);
});

it("returns null when cache miss", async () => {
  const cachesStub = buildCaches();
  cachesStub.default.match.mockResolvedValueOnce(undefined);
  vi.stubGlobal("caches", cachesStub);
  await expect(readRaceTrendFromEdgeCache("k")).resolves.toBeNull();
});

it("writes payload with default TTL when env unset", async () => {
  const cachesStub = buildCaches();
  vi.stubGlobal("caches", cachesStub);
  await writeRaceTrendToEdgeCache("k", { v: 1 }, {} as Env);
  expect(cachesStub.default.put).toHaveBeenCalledTimes(1);
  const args = cachesStub.default.put.mock.calls[0]!;
  expect(args[1].headers.get("Cache-Control")).toBe("public, max-age=60, s-maxage=60");
});

it("writes payload with custom TTL from env", async () => {
  const cachesStub = buildCaches();
  vi.stubGlobal("caches", cachesStub);
  await writeRaceTrendToEdgeCache("k", { v: 1 }, { FEATURES_EDGE_CACHE_TTL_SECONDS: "120" } as Env);
  const args = cachesStub.default.put.mock.calls[0]!;
  expect(args[1].headers.get("Cache-Control")).toBe("public, max-age=120, s-maxage=120");
});

it("falls back to default TTL when env value is non-numeric", async () => {
  const cachesStub = buildCaches();
  vi.stubGlobal("caches", cachesStub);
  await writeRaceTrendToEdgeCache("k", { v: 1 }, { FEATURES_EDGE_CACHE_TTL_SECONDS: "x" } as Env);
  const args = cachesStub.default.put.mock.calls[0]!;
  expect(args[1].headers.get("Cache-Control")).toBe("public, max-age=60, s-maxage=60");
});

it("falls back to default TTL when env value is zero", async () => {
  const cachesStub = buildCaches();
  vi.stubGlobal("caches", cachesStub);
  await writeRaceTrendToEdgeCache("k", { v: 1 }, { FEATURES_EDGE_CACHE_TTL_SECONDS: "0" } as Env);
  const args = cachesStub.default.put.mock.calls[0]!;
  expect(args[1].headers.get("Cache-Control")).toBe("public, max-age=60, s-maxage=60");
});

it("purges cache via delete", async () => {
  const cachesStub = buildCaches();
  vi.stubGlobal("caches", cachesStub);
  await purgeRaceTrendEdgeCache("k");
  expect(cachesStub.default.delete).toHaveBeenCalledTimes(1);
});
