// Run with bun.
import { expect, it, vi } from "vitest";

import {
  acquireEnqueueLock,
  calculateEnqueueLockTtlSeconds,
  isEnqueueLocked,
} from "./enqueue-lock-kv";
import type { Env } from "../types";

interface KvMockHandle {
  get: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
}

const buildEnv = (): Env => {
  const kv = {
    delete: vi.fn(async () => undefined),
    get: vi.fn(async () => null),
    put: vi.fn(async () => undefined),
  } as unknown as KVNamespace;
  return { ODDS_HOT_KV: kv } as Env;
};

it("returns 0 inside final window (3 minutes before race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:57:00+09:00"),
    ),
  ).toBe(0);
});

it("returns 0 at race start", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T10:00:00+09:00"),
    ),
  ).toBe(0);
});

it("returns 0 within final after window (2 minutes after race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T10:02:00+09:00"),
    ),
  ).toBe(0);
});

it("returns 20 in high-frequency window (15 minutes before race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:45:00+09:00"),
    ),
  ).toBe(20);
});

it("returns 60 well before race (45 minutes before)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:15:00+09:00"),
    ),
  ).toBe(60);
});

it("returns 60 well after race (5 minutes after)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T10:05:00+09:00"),
    ),
  ).toBe(60);
});

it("isEnqueueLocked returns true when KV value present", async () => {
  const env = buildEnv();
  const getMock = env.ODDS_HOT_KV.get as unknown as KvMockHandle["get"];
  getMock.mockResolvedValueOnce("1");
  expect(await isEnqueueLocked(env, "nar:20260528:42:01")).toBe(true);
});

it("isEnqueueLocked returns false when KV value absent", async () => {
  const env = buildEnv();
  expect(await isEnqueueLocked(env, "nar:20260528:42:01")).toBe(false);
});

it("acquireEnqueueLock skips PUT when ttlSeconds is zero", async () => {
  const env = buildEnv();
  await acquireEnqueueLock(env, "nar:20260528:42:01", 0);
  expect(env.ODDS_HOT_KV.put).not.toHaveBeenCalled();
});

it("acquireEnqueueLock writes KV entry when ttlSeconds positive", async () => {
  const env = buildEnv();
  await acquireEnqueueLock(env, "nar:20260528:42:01", 60);
  expect(env.ODDS_HOT_KV.put).toHaveBeenCalledWith("odds:enqueue-lock:nar:20260528:42:01", "1", {
    expirationTtl: 60,
  });
});
