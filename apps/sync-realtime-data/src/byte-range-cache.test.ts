// Run with bun run test.
import { expect, it, vi } from "vitest";

import { createByteRangeCache } from "./byte-range-cache";

it("reuses completed bytes without sharing mutable buffers with callers", async () => {
  const cache = createByteRangeCache();
  const load = vi.fn(async () => new Uint8Array([1, 2]).buffer);
  const first = await cache.read("etag:range", load);
  new Uint8Array(first)[0] = 9;
  const second = await cache.read("etag:range", load);
  expect([...new Uint8Array(second)]).toStrictEqual([1, 2]);
  new Uint8Array(second)[0] = 8;
  expect([...new Uint8Array(await cache.read("etag:range", load))]).toStrictEqual([1, 2]);
  expect(load).toHaveBeenCalledTimes(1);
});

it("isolates invocations and immutable object versions", async () => {
  const first = createByteRangeCache();
  const second = createByteRangeCache();
  const load = vi.fn(async () => new ArrayBuffer(1));
  await first.read("etag-1:range", load);
  await first.read("etag-2:range", load);
  await second.read("etag-1:range", load);
  expect(load).toHaveBeenCalledTimes(3);
});

it("does not cache failed reads", async () => {
  const cache = createByteRangeCache();
  const load = vi
    .fn<() => Promise<ArrayBuffer>>()
    .mockRejectedValueOnce(new Error("changed"))
    .mockResolvedValue(new ArrayBuffer(1));
  await expect(cache.read("etag:range", load)).rejects.toThrow("changed");
  await cache.read("etag:range", load);
  await cache.read("etag:range", load);
  expect(load).toHaveBeenCalledTimes(2);
});

it("does not retain oversized ranges or evict useful small ranges for them", async () => {
  const cache = createByteRangeCache();
  const small = vi.fn(async () => new ArrayBuffer(1));
  const large = vi.fn(async () => new ArrayBuffer(2_097_153));
  await cache.read("small", small);
  await cache.read("large", large);
  await cache.read("large", large);
  await cache.read("small", small);
  expect(large).toHaveBeenCalledTimes(2);
  expect(small).toHaveBeenCalledTimes(1);
});

it("retains the exact byte limit and clears before exceeding it", async () => {
  const cache = createByteRangeCache();
  const full = vi.fn(async () => new ArrayBuffer(2_097_152));
  const small = vi.fn(async () => new ArrayBuffer(1));
  await cache.read("full", full);
  await cache.read("full", full);
  expect(full).toHaveBeenCalledTimes(1);
  await cache.read("small", small);
  await cache.read("full", full);
  expect(full).toHaveBeenCalledTimes(2);
});

it("bounds metadata even for empty byte ranges", async () => {
  const cache = createByteRangeCache();
  const load = vi.fn(async () => new ArrayBuffer(0));
  await Promise.all(Array.from({ length: 512 }, (_, index) => cache.read(String(index), load)));
  await cache.read("0", load);
  expect(load).toHaveBeenCalledTimes(512);
  await cache.read("overflow", load);
  await cache.read("0", load);
  expect(load).toHaveBeenCalledTimes(514);
});

it("does not double-count concurrent reads of one immutable range", async () => {
  const cache = createByteRangeCache();
  const load = vi.fn(async () => new ArrayBuffer(1_048_576));
  await Promise.all([cache.read("same", load), cache.read("same", load)]);
  const other = vi.fn(async () => new ArrayBuffer(1_048_576));
  await cache.read("other", other);
  await cache.read("same", load);
  await cache.read("other", other);
  expect(load).toHaveBeenCalledTimes(2);
  expect(other).toHaveBeenCalledTimes(1);
});
