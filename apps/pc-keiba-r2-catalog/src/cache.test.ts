import { expect, it, vi } from "vitest";

import {
  cacheRequestFor,
  featureDescriptor,
  jsonRowsResponse,
  kvKeyFor,
  parsePositiveSeconds,
  populateCacheApi,
  populateCaches,
  purgeDescriptors,
  readKvRows,
} from "./cache";
import type { CacheStore, KvStore } from "./types";

const cacheMocks = (): {
  cache: CacheStore;
  deleteMock: ReturnType<typeof vi.fn>;
  putMock: ReturnType<typeof vi.fn>;
} => {
  const deleteMock = vi.fn(async () => true);
  const putMock = vi.fn(async () => undefined);
  return {
    cache: {
      delete: deleteMock,
      match: vi.fn(async () => undefined),
      put: putMock,
    },
    deleteMock,
    putMock,
  };
};

const kvMocks = (
  value: string | null,
): { deleteMock: ReturnType<typeof vi.fn>; kv: KvStore; putMock: ReturnType<typeof vi.fn> } => {
  const deleteMock = vi.fn(async () => undefined);
  const putMock = vi.fn(async () => undefined);
  return {
    deleteMock,
    kv: { delete: deleteMock, get: vi.fn(async () => value), put: putMock },
    putMock,
  };
};

it("builds canonical Cache API and KV keys", () => {
  const descriptor = featureDescriptor({
    date: "20260715",
    keibajoCode: "05",
    raceBango: "01",
    source: "jra",
  });
  expect(cacheRequestFor(descriptor).url).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/race-features?date=20260715&source=jra&keibajoCode=05&raceBango=01",
  );
  expect(kvKeyFor(descriptor)).toBe(
    "catalog:v2:v2/race-features?date=20260715&source=jra&keibajoCode=05&raceBango=01",
  );
  expect(cacheRequestFor({ date: "20260715", kind: "race-keys" }).url).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/race-keys?date=20260715",
  );
});

it("uses only positive integer TTL values", () => {
  expect(parsePositiveSeconds("30", 60)).toBe(30);
  expect(parsePositiveSeconds("0", 60)).toBe(60);
  expect(parsePositiveSeconds("2.5", 60)).toBe(60);
  expect(parsePositiveSeconds(undefined, 60)).toBe(60);
});

it("reads only valid rows envelopes from KV", async () => {
  await expect(readKvRows(kvMocks('{"rows":[]}').kv, "key")).resolves.toBe('{"rows":[]}');
  await expect(readKvRows(kvMocks("[]").kv, "key")).resolves.toBe(null);
  await expect(readKvRows(kvMocks("{}").kv, "key")).resolves.toBe(null);
  await expect(readKvRows(kvMocks("bad").kv, "key")).resolves.toBe(null);
  await expect(readKvRows(kvMocks(null).kv, "key")).resolves.toBe(null);
});

it("creates cache responses and writes Cache API plus KV", async () => {
  const cacheState = cacheMocks();
  const kvState = kvMocks(null);
  const descriptor = { date: "20260715", kind: "race-keys" } satisfies Parameters<
    typeof populateCaches
  >[2];
  await populateCaches(cacheState.cache, kvState.kv, descriptor, '{"rows":[]}', 60, 600);
  expect(cacheState.putMock).toHaveBeenCalledOnce();
  expect(kvState.putMock).toHaveBeenCalledWith(
    "catalog:v2:v2/race-keys?date=20260715",
    '{"rows":[]}',
    { expirationTtl: 600 },
  );
  const storedResponse = cacheState.putMock.mock.calls[0]?.[1];
  expect(storedResponse?.headers.get("Cache-Control")).toBe("public, max-age=60");
  await expect(storedResponse?.text()).resolves.toBe('{"rows":[]}');
  expect(jsonRowsResponse('{"rows":[]}', "kv").headers.get("X-Catalog-Cache")).toBe("kv");
});

it("contains cache write failures and purges both cache tiers", async () => {
  const cacheState = cacheMocks();
  const kvState = kvMocks(null);
  cacheState.putMock.mockRejectedValueOnce(new Error("cache unavailable"));
  kvState.putMock.mockRejectedValueOnce(new Error("kv unavailable"));
  await expect(
    populateCaches(
      cacheState.cache,
      kvState.kv,
      { date: "20260715", kind: "race-keys" },
      '{"rows":[]}',
      60,
      600,
    ),
  ).resolves.toBe(undefined);
  await expect(
    purgeDescriptors(cacheState.cache, kvState.kv, [
      { date: "20260715", kind: "race-keys" },
      { date: "20260715", kind: "race-features", source: "all" },
    ]),
  ).resolves.toBe(2);
  expect(cacheState.deleteMock).toHaveBeenCalledTimes(2);
  expect(kvState.deleteMock).toHaveBeenCalledTimes(2);
});

it("writes a single Cache API response", async () => {
  const cacheState = cacheMocks();
  const request = new Request("https://cache.test/item");
  await populateCacheApi(cacheState.cache, request, "[1]", 15);
  expect(cacheState.putMock).toHaveBeenCalledOnce();
});
