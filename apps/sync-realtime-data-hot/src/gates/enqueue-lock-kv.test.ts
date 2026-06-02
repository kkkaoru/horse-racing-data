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

it("returns 60 inside final window (3 minutes before race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:57:00+09:00"),
    ),
  ).toBe(60);
});

it("returns 60 at race start", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T10:00:00+09:00"),
    ),
  ).toBe(60);
});

it("returns 60 just after race start (1 minute after, still in grace)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T10:01:00+09:00"),
    ),
  ).toBe(60);
});

it("returns 60 at past-race grace boundary (2 minutes after race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T10:02:00+09:00"),
    ),
  ).toBe(60);
});

it("returns 60 at final window before boundary (9 minutes before race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:51:00+09:00"),
    ),
  ).toBe(60);
});

it("returns 60 at final window edge (10 minutes before race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:50:00+09:00"),
    ),
  ).toBe(60);
});

it("returns 0 just past grace boundary (3 minutes after race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T10:03:00+09:00"),
    ),
  ).toBe(0);
});

it("caps high-frequency TTL to 60 seconds near final boundary (11 minutes before race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:49:00+09:00"),
    ),
  ).toBe(60);
});

it("caps high-frequency TTL to 300 seconds (15 minutes before race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:45:00+09:00"),
    ),
  ).toBe(300);
});

it("caps high-frequency TTL to 540 seconds (19 minutes before race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:41:00+09:00"),
    ),
  ).toBe(540);
});

it("returns 600 in high-frequency window (45 minutes before race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:15:00+09:00"),
    ),
  ).toBe(600);
});

it("returns 600 at high-frequency window edge (60 minutes before race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T09:00:00+09:00"),
    ),
  ).toBe(600);
});

it("caps default TTL to 60 seconds near high-frequency boundary (61 minutes before race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T08:59:00+09:00"),
    ),
  ).toBe(60);
});

it("caps default TTL to 300 seconds (65 minutes before race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T08:55:00+09:00"),
    ),
  ).toBe(300);
});

it("caps default TTL to 600 seconds (70 minutes before race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T08:50:00+09:00"),
    ),
  ).toBe(600);
});

it("returns 3600 well before race (120 minutes before)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T08:00:00+09:00"),
    ),
  ).toBe(3600);
});

it("returns 0 well after race (4 minutes after)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T10:04:00+09:00"),
    ),
  ).toBe(0);
});

it("returns 0 well after race (5 minutes after)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T10:05:00+09:00"),
    ),
  ).toBe(0);
});

it("returns 0 far past race (1 hour after race)", () => {
  expect(
    calculateEnqueueLockTtlSeconds(
      new Date("2026-05-28T10:00:00+09:00"),
      new Date("2026-05-28T11:00:00+09:00"),
    ),
  ).toBe(0);
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
