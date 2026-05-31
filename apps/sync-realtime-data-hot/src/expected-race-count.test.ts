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

it("getExpectedRaceCountForDate returns the cached value when KV has a hit and never queries Hyperdrive", async () => {
  const kvGet = vi.fn(async () => "50");
  const kvPut = vi.fn(async () => undefined);
  const env = buildEnv(buildKv(kvGet, kvPut));
  const query = vi.fn();
  const total = await getExpectedRaceCountForDate(env, "20260531", {
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
    pool: { query } as never,
  });
  expect(total).toBe(50);
  expect(kvPut).toHaveBeenCalledWith("expected-race-count:20260531", "50", {
    expirationTtl: 300,
  });
});

it("getExpectedRaceCountForDate returns zero when Hyperdrive yields zero counts", async () => {
  const kvGet = vi.fn(async () => null);
  const kvPut = vi.fn(async () => undefined);
  const env = buildEnv(buildKv(kvGet, kvPut));
  const query = vi.fn().mockResolvedValue({ rows: [{ jra: 0, nar: 0 }] });
  const total = await getExpectedRaceCountForDate(env, "20260531", {
    pool: { query } as never,
  });
  expect(total).toBe(0);
  expect(kvPut).toHaveBeenCalledWith("expected-race-count:20260531", "0", {
    expirationTtl: 300,
  });
});

it("getExpectedRaceCountForDate returns zero when Hyperdrive yields an empty rows array", async () => {
  const kvGet = vi.fn(async () => null);
  const kvPut = vi.fn(async () => undefined);
  const env = buildEnv(buildKv(kvGet, kvPut));
  const query = vi.fn().mockResolvedValue({ rows: [] });
  const total = await getExpectedRaceCountForDate(env, "20260531", {
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
  const total = await getExpectedRaceCountForDate(env, "20260531");
  expect(total).toBe(3);
  expect(getHotPool).toHaveBeenCalledWith(env);
});
