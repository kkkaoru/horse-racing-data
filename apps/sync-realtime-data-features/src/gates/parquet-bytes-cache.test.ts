// Run with: bun run --filter sync-realtime-data-features test
import { beforeEach, expect, it, vi } from "vitest";

import { readParquetBytesFromCache, writeParquetBytesToCache } from "./parquet-bytes-cache";
import type { Env } from "../types";

const buildCaches = () => ({
  default: {
    match: vi.fn(),
    put: vi.fn(),
  },
});

beforeEach(() => {
  vi.unstubAllGlobals();
});

it("returns null when cache miss", async () => {
  const cachesStub = buildCaches();
  cachesStub.default.match.mockResolvedValueOnce(undefined);
  vi.stubGlobal("caches", cachesStub);
  await expect(readParquetBytesFromCache("k")).resolves.toBeNull();
});

it("returns ArrayBuffer when cache hit", async () => {
  const cachesStub = buildCaches();
  const expected = new Uint8Array([1, 2, 3]);
  cachesStub.default.match.mockResolvedValueOnce(new Response(expected));
  vi.stubGlobal("caches", cachesStub);
  const result = await readParquetBytesFromCache("k");
  expect(result).not.toBeNull();
  expect(new Uint8Array(result!)).toStrictEqual(expected);
});

it("writes bytes with default TTL when env unset", async () => {
  const cachesStub = buildCaches();
  vi.stubGlobal("caches", cachesStub);
  await writeParquetBytesToCache("k", new ArrayBuffer(8), {} as Env);
  expect(cachesStub.default.put).toHaveBeenCalledTimes(1);
  const response = cachesStub.default.put.mock.calls[0]![1] as Response;
  expect(response.headers.get("Cache-Control")).toBe("public, max-age=300, s-maxage=300");
});

it("writes bytes with TTL from env", async () => {
  const cachesStub = buildCaches();
  vi.stubGlobal("caches", cachesStub);
  await writeParquetBytesToCache("k", new ArrayBuffer(8), {
    FEATURES_PARQUET_BYTES_CACHE_TTL_SECONDS: "30",
  } as Env);
  const response = cachesStub.default.put.mock.calls[0]![1] as Response;
  expect(response.headers.get("Cache-Control")).toBe("public, max-age=30, s-maxage=30");
});

it("falls back to default TTL when env value is non-numeric", async () => {
  const cachesStub = buildCaches();
  vi.stubGlobal("caches", cachesStub);
  await writeParquetBytesToCache("k", new ArrayBuffer(8), {
    FEATURES_PARQUET_BYTES_CACHE_TTL_SECONDS: "x",
  } as Env);
  const response = cachesStub.default.put.mock.calls[0]![1] as Response;
  expect(response.headers.get("Cache-Control")).toBe("public, max-age=300, s-maxage=300");
});

it("falls back to default TTL when env value is zero", async () => {
  const cachesStub = buildCaches();
  vi.stubGlobal("caches", cachesStub);
  await writeParquetBytesToCache("k", new ArrayBuffer(8), {
    FEATURES_PARQUET_BYTES_CACHE_TTL_SECONDS: "0",
  } as Env);
  const response = cachesStub.default.put.mock.calls[0]![1] as Response;
  expect(response.headers.get("Cache-Control")).toBe("public, max-age=300, s-maxage=300");
});
