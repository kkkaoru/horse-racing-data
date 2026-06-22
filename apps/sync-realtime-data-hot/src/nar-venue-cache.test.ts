// Run with: bun run --filter sync-realtime-data-hot test
import { afterEach, beforeEach, expect, it, vi } from "vitest";

import { getCachedNarVenueLastRaceStartAtJst } from "./nar-venue-cache";
import type { Env } from "./types";

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

interface BuildEnvOptions {
  kvGet?: () => Promise<string | null>;
  kvPut?: () => Promise<void>;
  d1Value?: string | null;
}

const buildEnv = (options: BuildEnvOptions = {}): Env => {
  const kvGet = options.kvGet ?? vi.fn(async () => null);
  const kvPut = options.kvPut ?? vi.fn(async () => undefined);
  const kv = {
    delete: vi.fn(async () => undefined),
    get: kvGet,
    put: kvPut,
  } as unknown as KVNamespace;
  const prepareMock = vi.fn(() => ({
    bind: vi.fn(() => ({
      first: vi.fn(async () => ({ last_race_start_at_jst: options.d1Value ?? null })),
    })),
  }));
  const db = { prepare: prepareMock } as unknown as D1Database;
  return {
    ODDS_HOT_KV: kv,
    REALTIME_HOT_DB: db,
  } as unknown as Env;
};

const sampleKey = {
  kaisaiNen: "2026",
  kaisaiTsukihi: "0528",
  keibajoCode: "42",
};

it("returns the cached ISO string when KV hits", async () => {
  const kvGet = vi.fn(async () => "2026-05-28T22:00:00+09:00");
  const env = buildEnv({ kvGet });
  const result = await getCachedNarVenueLastRaceStartAtJst(env, sampleKey);
  expect(result).toBe("2026-05-28T22:00:00+09:00");
  expect(env.REALTIME_HOT_DB.prepare).not.toHaveBeenCalled();
});

it("decodes the null sentinel as null without falling through to D1", async () => {
  const kvGet = vi.fn(async () => "__null__");
  const env = buildEnv({ kvGet });
  const result = await getCachedNarVenueLastRaceStartAtJst(env, sampleKey);
  expect(result).toBeNull();
  expect(env.REALTIME_HOT_DB.prepare).not.toHaveBeenCalled();
});

it("falls through to D1 and writes the fresh value to KV on cache miss", async () => {
  const kvPut = vi.fn(async () => undefined);
  const env = buildEnv({ d1Value: "2026-05-28T21:30:00+09:00", kvPut });
  const result = await getCachedNarVenueLastRaceStartAtJst(env, sampleKey);
  expect(result).toBe("2026-05-28T21:30:00+09:00");
  expect(env.ODDS_HOT_KV.put).toHaveBeenCalledWith(
    "nar:venue-last-start:20260528:42",
    "2026-05-28T21:30:00+09:00",
    { expirationTtl: 21600 },
  );
});

it("encodes null D1 result as the null sentinel in KV", async () => {
  const kvPut = vi.fn(async () => undefined);
  const env = buildEnv({ d1Value: null, kvPut });
  const result = await getCachedNarVenueLastRaceStartAtJst(env, sampleKey);
  expect(result).toBeNull();
  expect(env.ODDS_HOT_KV.put).toHaveBeenCalledWith("nar:venue-last-start:20260528:42", "__null__", {
    expirationTtl: 21600,
  });
});

it("falls through to D1 when KV read throws", async () => {
  const kvGet = vi.fn(async () => {
    throw new Error("kv down");
  });
  const env = buildEnv({ d1Value: "2026-05-28T20:00:00+09:00", kvGet });
  const result = await getCachedNarVenueLastRaceStartAtJst(env, sampleKey);
  expect(result).toBe("2026-05-28T20:00:00+09:00");
});

it("returns D1 result even when KV write throws", async () => {
  const kvPut = vi.fn(async () => {
    throw new Error("kv put down");
  });
  const env = buildEnv({ d1Value: "2026-05-28T19:00:00+09:00", kvPut });
  const result = await getCachedNarVenueLastRaceStartAtJst(env, sampleKey);
  expect(result).toBe("2026-05-28T19:00:00+09:00");
});
