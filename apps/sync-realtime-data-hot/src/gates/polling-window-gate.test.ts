// Run with: bun run --filter sync-realtime-data-hot test
import { expect, it, vi } from "vitest";

import { shouldRunOddsCron } from "./polling-window-gate";
import type { Env } from "../types";

interface DbMockHandle {
  prepare: ReturnType<typeof vi.fn>;
  bind: ReturnType<typeof vi.fn>;
  all: ReturnType<typeof vi.fn>;
}

interface DbRow {
  source: string;
  yyyy_mm_dd: string;
  last_start: string;
}

const buildKv = (kvGet?: ReturnType<typeof vi.fn>, kvPut?: ReturnType<typeof vi.fn>): KVNamespace =>
  ({
    delete: vi.fn(async () => undefined),
    get: kvGet ?? vi.fn(async () => null),
    put: kvPut ?? vi.fn(async () => undefined),
  }) as unknown as KVNamespace;

const buildDb = (rows: DbRow[]): { db: D1Database; handle: DbMockHandle } => {
  const all = vi.fn(async () => ({ results: rows, success: true, meta: {} }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  return { db, handle: { all, bind, prepare } };
};

const buildEnv = (db: D1Database, kv: KVNamespace): Env =>
  ({
    ODDS_HOT_KV: kv,
    REALTIME_HOT_DB: db,
  }) as unknown as Env;

it("returns true via KV cache hit without querying D1", async () => {
  const kvGet = vi.fn(async () => "true");
  const kvPut = vi.fn(async () => undefined);
  const kv = buildKv(kvGet, kvPut);
  const { db, handle } = buildDb([]);
  const env = buildEnv(db, kv);
  const result = await shouldRunOddsCron(env, new Date("2026-06-07T00:00:00Z"));
  expect(result).toBe(true);
  expect(kvGet).toHaveBeenCalledWith("odds-polling-window:active");
  expect(handle.prepare).not.toHaveBeenCalled();
  expect(kvPut).not.toHaveBeenCalled();
});

it("returns false via KV cache hit without querying D1", async () => {
  const kvGet = vi.fn(async () => "false");
  const kvPut = vi.fn(async () => undefined);
  const kv = buildKv(kvGet, kvPut);
  const { db, handle } = buildDb([]);
  const env = buildEnv(db, kv);
  const result = await shouldRunOddsCron(env, new Date("2026-06-07T00:00:00Z"));
  expect(result).toBe(false);
  expect(handle.prepare).not.toHaveBeenCalled();
  expect(kvPut).not.toHaveBeenCalled();
});

it("JRA today: returns true at 09:00 JST when last race is 17:30 JST", async () => {
  const kv = buildKv();
  const { db } = buildDb([
    { last_start: "2026-06-07T17:30:00+09:00", source: "jra", yyyy_mm_dd: "2026-06-07" },
  ]);
  const env = buildEnv(db, kv);
  // 2026-06-07T00:00:00Z = 2026-06-07T09:00:00 JST
  const result = await shouldRunOddsCron(env, new Date("2026-06-07T00:00:00Z"));
  expect(result).toBe(true);
});

it("JRA today: returns true at 08:00 JST overnight advance window when last race is 17:30 JST", async () => {
  const kv = buildKv();
  const { db } = buildDb([
    { last_start: "2026-06-07T17:30:00+09:00", source: "jra", yyyy_mm_dd: "2026-06-07" },
  ]);
  const env = buildEnv(db, kv);
  // 2026-06-06T23:00:00Z = 2026-06-07T08:00:00 JST: continuous advance sale active overnight
  const result = await shouldRunOddsCron(env, new Date("2026-06-06T23:00:00Z"));
  expect(result).toBe(true);
});

it("JRA today: returns true at 03:00 JST overnight advance window when last race is 17:30 JST", async () => {
  const kv = buildKv();
  const { db } = buildDb([
    { last_start: "2026-06-07T17:30:00+09:00", source: "jra", yyyy_mm_dd: "2026-06-07" },
  ]);
  const env = buildEnv(db, kv);
  // 2026-06-06T18:00:00Z = 2026-06-07T03:00:00 JST: covers the overnight hourly slot
  const result = await shouldRunOddsCron(env, new Date("2026-06-06T18:00:00Z"));
  expect(result).toBe(true);
});

it("JRA today: returns true at 00:00 JST overnight advance window when last race is 17:30 JST", async () => {
  const kv = buildKv();
  const { db } = buildDb([
    { last_start: "2026-06-07T17:30:00+09:00", source: "jra", yyyy_mm_dd: "2026-06-07" },
  ]);
  const env = buildEnv(db, kv);
  // 2026-06-06T15:00:00Z = 2026-06-07T00:00:00 JST: lower bound edge
  const result = await shouldRunOddsCron(env, new Date("2026-06-06T15:00:00Z"));
  expect(result).toBe(true);
});

it("JRA today: returns false at 18:01 JST when last race is 17:30 JST (past post-race grace)", async () => {
  const kv = buildKv();
  const { db } = buildDb([
    { last_start: "2026-06-07T17:30:00+09:00", source: "jra", yyyy_mm_dd: "2026-06-07" },
  ]);
  const env = buildEnv(db, kv);
  // 2026-06-07T09:01:00Z = 2026-06-07T18:01:00 JST
  const result = await shouldRunOddsCron(env, new Date("2026-06-07T09:01:00Z"));
  expect(result).toBe(false);
});

it("JRA today: returns true at 18:00 JST exactly when last race is 17:30 JST (grace edge)", async () => {
  const kv = buildKv();
  const { db } = buildDb([
    { last_start: "2026-06-07T17:30:00+09:00", source: "jra", yyyy_mm_dd: "2026-06-07" },
  ]);
  const env = buildEnv(db, kv);
  // 2026-06-07T09:00:00Z = 2026-06-07T18:00:00 JST
  const result = await shouldRunOddsCron(env, new Date("2026-06-07T09:00:00Z"));
  expect(result).toBe(true);
});

it("JRA tomorrow prep: returns true at 19:00 JST today when race is tomorrow", async () => {
  const kv = buildKv();
  const { db } = buildDb([
    { last_start: "2026-06-08T15:30:00+09:00", source: "jra", yyyy_mm_dd: "2026-06-08" },
  ]);
  const env = buildEnv(db, kv);
  // 2026-06-07T10:00:00Z = 2026-06-07T19:00:00 JST
  const result = await shouldRunOddsCron(env, new Date("2026-06-07T10:00:00Z"));
  expect(result).toBe(true);
});

it("JRA tomorrow prep: returns false at 18:59 JST today when race is tomorrow", async () => {
  const kv = buildKv();
  const { db } = buildDb([
    { last_start: "2026-06-08T15:30:00+09:00", source: "jra", yyyy_mm_dd: "2026-06-08" },
  ]);
  const env = buildEnv(db, kv);
  // 2026-06-07T09:59:00Z = 2026-06-07T18:59:00 JST
  const result = await shouldRunOddsCron(env, new Date("2026-06-07T09:59:00Z"));
  expect(result).toBe(false);
});

it("JRA tomorrow prep: returns true at 23:59 JST today when race is tomorrow", async () => {
  const kv = buildKv();
  const { db } = buildDb([
    { last_start: "2026-06-08T15:30:00+09:00", source: "jra", yyyy_mm_dd: "2026-06-08" },
  ]);
  const env = buildEnv(db, kv);
  // 2026-06-07T14:59:00Z = 2026-06-07T23:59:00 JST
  const result = await shouldRunOddsCron(env, new Date("2026-06-07T14:59:00Z"));
  expect(result).toBe(true);
});

it("JRA today: returns true at 00:00 JST when only the now-today row has race (overnight rollover)", async () => {
  // From the viewpoint of 2026-06-08T00:00:00 JST, today is 2026-06-08 and the
  // 2026-06-08 row becomes "today". With the JRA today lower bound at 00:00 JST,
  // the overnight advance-sale gate is already active.
  const kv = buildKv();
  const { db } = buildDb([
    { last_start: "2026-06-08T15:30:00+09:00", source: "jra", yyyy_mm_dd: "2026-06-08" },
  ]);
  const env = buildEnv(db, kv);
  // 2026-06-07T15:00:00Z = 2026-06-08T00:00:00 JST
  const result = await shouldRunOddsCron(env, new Date("2026-06-07T15:00:00Z"));
  expect(result).toBe(true);
});

it("JRA today: returns false at 16:00 JST when no race today and no race tomorrow (off-day)", async () => {
  // Sanity check that the overnight bound change does not turn the gate
  // permanently true on off-days: with no jraToday and no jraTomorrow row,
  // the gate stays closed regardless of hour.
  const kv = buildKv();
  const { db } = buildDb([]);
  const env = buildEnv(db, kv);
  // 2026-06-07T07:00:00Z = 2026-06-07T16:00:00 JST
  const result = await shouldRunOddsCron(env, new Date("2026-06-07T07:00:00Z"));
  expect(result).toBe(false);
});

it("NAR today: returns true at 10:00 JST when last race is 22:00 JST", async () => {
  const kv = buildKv();
  const { db } = buildDb([
    { last_start: "2026-06-07T22:00:00+09:00", source: "nar", yyyy_mm_dd: "2026-06-07" },
  ]);
  const env = buildEnv(db, kv);
  // 2026-06-07T01:00:00Z = 2026-06-07T10:00:00 JST
  const result = await shouldRunOddsCron(env, new Date("2026-06-07T01:00:00Z"));
  expect(result).toBe(true);
});

it("NAR today: returns false at 09:59 JST when last race is 22:00 JST", async () => {
  const kv = buildKv();
  const { db } = buildDb([
    { last_start: "2026-06-07T22:00:00+09:00", source: "nar", yyyy_mm_dd: "2026-06-07" },
  ]);
  const env = buildEnv(db, kv);
  // 2026-06-07T00:59:00Z = 2026-06-07T09:59:00 JST
  const result = await shouldRunOddsCron(env, new Date("2026-06-07T00:59:00Z"));
  expect(result).toBe(false);
});

it("NAR today: returns false at 22:31 JST when last race is 22:00 JST (past grace)", async () => {
  const kv = buildKv();
  const { db } = buildDb([
    { last_start: "2026-06-07T22:00:00+09:00", source: "nar", yyyy_mm_dd: "2026-06-07" },
  ]);
  const env = buildEnv(db, kv);
  // 2026-06-07T13:31:00Z = 2026-06-07T22:31:00 JST
  const result = await shouldRunOddsCron(env, new Date("2026-06-07T13:31:00Z"));
  expect(result).toBe(false);
});

it("returns false when no rows exist at all (empty result set)", async () => {
  const kv = buildKv();
  const { db } = buildDb([]);
  const env = buildEnv(db, kv);
  const result = await shouldRunOddsCron(env, new Date("2026-06-07T05:00:00Z"));
  expect(result).toBe(false);
});

it("OR union: JRA today + NAR today both schedule yields true at 14:00 JST", async () => {
  const kv = buildKv();
  const { db } = buildDb([
    { last_start: "2026-06-07T17:30:00+09:00", source: "jra", yyyy_mm_dd: "2026-06-07" },
    { last_start: "2026-06-07T22:00:00+09:00", source: "nar", yyyy_mm_dd: "2026-06-07" },
  ]);
  const env = buildEnv(db, kv);
  // 2026-06-07T05:00:00Z = 2026-06-07T14:00:00 JST
  const result = await shouldRunOddsCron(env, new Date("2026-06-07T05:00:00Z"));
  expect(result).toBe(true);
});

it("OR union: only NAR active (post-JRA grace) still yields true", async () => {
  const kv = buildKv();
  const { db } = buildDb([
    { last_start: "2026-06-07T17:30:00+09:00", source: "jra", yyyy_mm_dd: "2026-06-07" },
    { last_start: "2026-06-07T22:00:00+09:00", source: "nar", yyyy_mm_dd: "2026-06-07" },
  ]);
  const env = buildEnv(db, kv);
  // 2026-06-07T12:30:00Z = 2026-06-07T21:30:00 JST: JRA grace expired, NAR still in window
  const result = await shouldRunOddsCron(env, new Date("2026-06-07T12:30:00Z"));
  expect(result).toBe(true);
});

it("writes 'true' to KV with 60 s TTL on cache miss when active", async () => {
  const kvGet = vi.fn(async () => null);
  const kvPut = vi.fn(async () => undefined);
  const kv = buildKv(kvGet, kvPut);
  const { db } = buildDb([
    { last_start: "2026-06-07T22:00:00+09:00", source: "nar", yyyy_mm_dd: "2026-06-07" },
  ]);
  const env = buildEnv(db, kv);
  const result = await shouldRunOddsCron(env, new Date("2026-06-07T05:00:00Z"));
  expect(result).toBe(true);
  expect(kvPut).toHaveBeenCalledWith("odds-polling-window:active", "true", {
    expirationTtl: 60,
  });
});

it("writes 'false' to KV with 60 s TTL on cache miss when inactive", async () => {
  const kvGet = vi.fn(async () => null);
  const kvPut = vi.fn(async () => undefined);
  const kv = buildKv(kvGet, kvPut);
  const { db } = buildDb([]);
  const env = buildEnv(db, kv);
  const result = await shouldRunOddsCron(env, new Date("2026-06-07T05:00:00Z"));
  expect(result).toBe(false);
  expect(kvPut).toHaveBeenCalledWith("odds-polling-window:active", "false", {
    expirationTtl: 60,
  });
});

it("D1 query binds today and tomorrow yyyy-mm-dd strings", async () => {
  const kv = buildKv();
  const { db, handle } = buildDb([]);
  const env = buildEnv(db, kv);
  // 2026-06-07T05:00:00Z = 2026-06-07T14:00:00 JST so today=2026-06-07 tomorrow=2026-06-08
  await shouldRunOddsCron(env, new Date("2026-06-07T05:00:00Z"));
  expect(handle.bind).toHaveBeenCalledWith("2026-06-07", "2026-06-08");
});

it("unknown KV value falls through to D1 query path", async () => {
  const kvGet = vi.fn(async () => "garbage");
  const kvPut = vi.fn(async () => undefined);
  const kv = buildKv(kvGet, kvPut);
  const { db, handle } = buildDb([]);
  const env = buildEnv(db, kv);
  const result = await shouldRunOddsCron(env, new Date("2026-06-07T05:00:00Z"));
  expect(result).toBe(false);
  expect(handle.prepare).toHaveBeenCalledTimes(1);
  expect(kvPut).toHaveBeenCalledWith("odds-polling-window:active", "false", {
    expirationTtl: 60,
  });
});
