// Run with: bun run --filter sync-realtime-data-hot test
import { afterEach, expect, it, vi } from "vitest";

vi.mock("./postgres-pool", () => ({
  getHotPool: vi.fn(),
}));

import { getExpectedRaceCountForDate } from "./expected-race-count";
import { getHotPool } from "./postgres-pool";
import type { Env } from "./types";

const buildKv = (kvGet?: ReturnType<typeof vi.fn>, kvPut?: ReturnType<typeof vi.fn>): KVNamespace =>
  ({
    delete: vi.fn(async () => undefined),
    get: kvGet ?? vi.fn(async () => null),
    put: kvPut ?? vi.fn(async () => undefined),
  }) as unknown as KVNamespace;

const buildEnv = (kv: KVNamespace): Env =>
  ({
    HYPERDRIVE: { connectionString: "postgres://test" },
    ODDS_HOT_KV: kv,
  }) as unknown as Env;

afterEach(() => {
  vi.restoreAllMocks();
});

// Off-window (early morning JST) reference timestamp: 2026-05-31 06:00 JST
// is well before the race-day window starts, so total=0 may legitimately
// cache. Tests that want to land outside the window pin to this `now`.
const OFF_WINDOW_NOW = new Date("2026-05-30T21:00:00Z");
// On-window reference timestamp: 2026-05-31 13:00 JST is squarely inside
// the JST 09:00-22:00 race-day window where total=0 must NOT cache.
const ON_WINDOW_NOW = new Date("2026-05-31T04:00:00Z");

it("getExpectedRaceCountForDate returns the cached value when KV has a hit and never queries Hyperdrive", async () => {
  const kvGet = vi.fn(async () => "50");
  const kvPut = vi.fn(async () => undefined);
  const env = buildEnv(buildKv(kvGet, kvPut));
  const query = vi.fn();
  const total = await getExpectedRaceCountForDate(env, "20260531", {
    now: OFF_WINDOW_NOW,
    pool: { query } as never,
  });
  expect(total).toBe(50);
  expect(kvGet).toHaveBeenCalledWith("expected-race-count:20260531");
  expect(query).not.toHaveBeenCalled();
  expect(kvPut).not.toHaveBeenCalled();
});

it("getExpectedRaceCountForDate queries Hyperdrive on KV miss and sums jra plus nar from numeric columns", async () => {
  const kvGet = vi.fn(async () => null);
  const kvPut = vi.fn(async () => undefined);
  const env = buildEnv(buildKv(kvGet, kvPut));
  const query = vi.fn().mockResolvedValue({ rows: [{ jra: 12, nar: 38 }] });
  const total = await getExpectedRaceCountForDate(env, "20260531", {
    now: ON_WINDOW_NOW,
    pool: { query } as never,
  });
  expect(total).toBe(50);
  expect(query).toHaveBeenCalledWith(expect.any(String), ["2026", "0531"]);
  expect(kvPut).toHaveBeenCalledWith("expected-race-count:20260531", "50", {
    expirationTtl: 300,
  });
});

it("getExpectedRaceCountForDate handles string count values from pg by parsing them as integers", async () => {
  const kvGet = vi.fn(async () => null);
  const kvPut = vi.fn(async () => undefined);
  const env = buildEnv(buildKv(kvGet, kvPut));
  const query = vi.fn().mockResolvedValue({ rows: [{ jra: "12", nar: "38" }] });
  const total = await getExpectedRaceCountForDate(env, "20260531", {
    now: ON_WINDOW_NOW,
    pool: { query } as never,
  });
  expect(total).toBe(50);
  expect(kvPut).toHaveBeenCalledWith("expected-race-count:20260531", "50", {
    expirationTtl: 300,
  });
});

it("getExpectedRaceCountForDate returns zero when Hyperdrive yields zero counts and caches outside race-day window", async () => {
  const kvGet = vi.fn(async () => null);
  const kvPut = vi.fn(async () => undefined);
  const env = buildEnv(buildKv(kvGet, kvPut));
  const query = vi.fn().mockResolvedValue({ rows: [{ jra: 0, nar: 0 }] });
  const total = await getExpectedRaceCountForDate(env, "20260531", {
    now: OFF_WINDOW_NOW,
    pool: { query } as never,
  });
  expect(total).toBe(0);
  expect(kvPut).toHaveBeenCalledWith("expected-race-count:20260531", "0", {
    expirationTtl: 300,
  });
});

it("getExpectedRaceCountForDate returns zero when Hyperdrive yields an empty rows array and caches outside race-day window", async () => {
  const kvGet = vi.fn(async () => null);
  const kvPut = vi.fn(async () => undefined);
  const env = buildEnv(buildKv(kvGet, kvPut));
  const query = vi.fn().mockResolvedValue({ rows: [] });
  const total = await getExpectedRaceCountForDate(env, "20260531", {
    now: OFF_WINDOW_NOW,
    pool: { query } as never,
  });
  expect(total).toBe(0);
  expect(kvPut).toHaveBeenCalledWith("expected-race-count:20260531", "0", {
    expirationTtl: 300,
  });
});

it("getExpectedRaceCountForDate treats null jra and null nar count columns as zero", async () => {
  const kvGet = vi.fn(async () => null);
  const kvPut = vi.fn(async () => undefined);
  const env = buildEnv(buildKv(kvGet, kvPut));
  const query = vi.fn().mockResolvedValue({ rows: [{ jra: null, nar: null }] });
  const total = await getExpectedRaceCountForDate(env, "20260531", {
    now: OFF_WINDOW_NOW,
    pool: { query } as never,
  });
  expect(total).toBe(0);
});

it("getExpectedRaceCountForDate treats unparseable cached KV value as a miss and falls through to Hyperdrive", async () => {
  const kvGet = vi.fn(async () => "not-a-number");
  const kvPut = vi.fn(async () => undefined);
  const env = buildEnv(buildKv(kvGet, kvPut));
  const query = vi.fn().mockResolvedValue({ rows: [{ jra: 5, nar: 7 }] });
  const total = await getExpectedRaceCountForDate(env, "20260531", {
    now: ON_WINDOW_NOW,
    pool: { query } as never,
  });
  expect(total).toBe(12);
  expect(kvPut).toHaveBeenCalledWith("expected-race-count:20260531", "12", {
    expirationTtl: 300,
  });
});

it("getExpectedRaceCountForDate treats unparseable string count values as zero", async () => {
  const kvGet = vi.fn(async () => null);
  const kvPut = vi.fn(async () => undefined);
  const env = buildEnv(buildKv(kvGet, kvPut));
  const query = vi.fn().mockResolvedValue({ rows: [{ jra: "abc", nar: "xyz" }] });
  const total = await getExpectedRaceCountForDate(env, "20260531", {
    now: OFF_WINDOW_NOW,
    pool: { query } as never,
  });
  expect(total).toBe(0);
});

it("getExpectedRaceCountForDate falls back to getHotPool when context.pool is absent", async () => {
  const kvGet = vi.fn(async () => null);
  const kvPut = vi.fn(async () => undefined);
  const env = buildEnv(buildKv(kvGet, kvPut));
  const query = vi.fn().mockResolvedValue({ rows: [{ jra: 1, nar: 2 }] });
  vi.mocked(getHotPool).mockReturnValueOnce({ query } as never);
  const total = await getExpectedRaceCountForDate(env, "20260531", { now: ON_WINDOW_NOW });
  expect(total).toBe(3);
  expect(getHotPool).toHaveBeenCalledWith(env);
});

it("getExpectedRaceCountForDate skips KV write when total is zero inside JST race-day window (13:00 JST)", async () => {
  const kvGet = vi.fn(async () => null);
  const kvPut = vi.fn(async () => undefined);
  const env = buildEnv(buildKv(kvGet, kvPut));
  const query = vi.fn().mockResolvedValue({ rows: [{ jra: 0, nar: 0 }] });
  const total = await getExpectedRaceCountForDate(env, "20260531", {
    now: ON_WINDOW_NOW,
    pool: { query } as never,
  });
  expect(total).toBe(0);
  expect(kvPut).not.toHaveBeenCalled();
});

it("getExpectedRaceCountForDate skips KV write when total is zero at race-day window lower edge (09:00 JST)", async () => {
  const kvGet = vi.fn(async () => null);
  const kvPut = vi.fn(async () => undefined);
  const env = buildEnv(buildKv(kvGet, kvPut));
  const query = vi.fn().mockResolvedValue({ rows: [{ jra: 0, nar: 0 }] });
  const total = await getExpectedRaceCountForDate(env, "20260531", {
    now: new Date("2026-05-31T00:00:00Z"),
    pool: { query } as never,
  });
  expect(total).toBe(0);
  expect(kvPut).not.toHaveBeenCalled();
});

it("getExpectedRaceCountForDate caches total zero at race-day window upper edge (22:00 JST)", async () => {
  const kvGet = vi.fn(async () => null);
  const kvPut = vi.fn(async () => undefined);
  const env = buildEnv(buildKv(kvGet, kvPut));
  const query = vi.fn().mockResolvedValue({ rows: [{ jra: 0, nar: 0 }] });
  const total = await getExpectedRaceCountForDate(env, "20260531", {
    now: new Date("2026-05-31T13:00:00Z"),
    pool: { query } as never,
  });
  expect(total).toBe(0);
  expect(kvPut).toHaveBeenCalledWith("expected-race-count:20260531", "0", {
    expirationTtl: 300,
  });
});

it("getExpectedRaceCountForDate caches total zero when REPLICA_SYNC_HOT_TRUST_ZERO_COUNT env override is set", async () => {
  const kvGet = vi.fn(async () => null);
  const kvPut = vi.fn(async () => undefined);
  const env = {
    HYPERDRIVE: { connectionString: "postgres://test" },
    ODDS_HOT_KV: buildKv(kvGet, kvPut),
    REPLICA_SYNC_HOT_TRUST_ZERO_COUNT: "1",
  } as unknown as Env;
  const query = vi.fn().mockResolvedValue({ rows: [{ jra: 0, nar: 0 }] });
  const total = await getExpectedRaceCountForDate(env, "20260531", {
    now: ON_WINDOW_NOW,
    pool: { query } as never,
  });
  expect(total).toBe(0);
  expect(kvPut).toHaveBeenCalledWith("expected-race-count:20260531", "0", {
    expirationTtl: 300,
  });
});

it("getExpectedRaceCountForDate uses real clock when context.now is omitted (smoke)", async () => {
  const kvGet = vi.fn(async () => "7");
  const kvPut = vi.fn(async () => undefined);
  const env = buildEnv(buildKv(kvGet, kvPut));
  const query = vi.fn();
  const total = await getExpectedRaceCountForDate(env, "20260531", {
    pool: { query } as never,
  });
  expect(total).toBe(7);
  expect(query).not.toHaveBeenCalled();
});
