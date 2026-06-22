// Run with bun.
import { expect, it, vi } from "vitest";

import { planOddsFetches } from "./plan";
import type { Env, RaceListEntry } from "./types";

interface KvMockHandle {
  get: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
  delete: ReturnType<typeof vi.fn>;
}

const buildKv = (): KVNamespace =>
  ({
    delete: vi.fn(async () => undefined),
    get: vi.fn(async () => null),
    put: vi.fn(async () => undefined),
  }) as unknown as KVNamespace;

const buildQueue = (): Queue<unknown> =>
  ({
    send: vi.fn(async () => undefined),
    sendBatch: vi.fn(async () => undefined),
  }) as unknown as Queue<unknown>;

interface BuildEnvOptions {
  kvGet?: (key: string) => Promise<string | null>;
  d1Results?: RaceListEntry[];
}

const buildEnv = (options: BuildEnvOptions = {}): Env => {
  const kv = buildKv();
  if (options.kvGet) {
    (kv.get as unknown as KvMockHandle["get"]).mockImplementation(options.kvGet);
  }
  const all = vi.fn(async () => ({
    results: (options.d1Results ?? []).map((entry) => ({
      last_odds_fetch_at: entry.lastOddsFetchAt,
      race_key: entry.raceKey,
      race_start_at_jst: entry.raceStartAtJst,
      source: entry.source,
    })),
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  return {
    ODDS_HOT_KV: kv,
    REALTIME_HOT_DB: db,
    REALTIME_HOT_JOBS: buildQueue() as unknown as Queue<never>,
  } as unknown as Env;
};

it("enqueues races even outside the legacy JST 06-21 polling window", async () => {
  const env = buildEnv({
    d1Results: [
      {
        lastOddsFetchAt: null,
        raceKey: "jra:20260530:08:01",
        raceStartAtJst: "2026-05-30T09:50:00+09:00",
        source: "jra",
      },
    ],
  });
  const result = await planOddsFetches(env, new Date("2026-05-29T14:41:00Z"), "20260530");
  expect(result.queued).toBeGreaterThan(0);
  expect(env.REALTIME_HOT_JOBS.send).toHaveBeenCalled();
});

it("returns zero counts when no races for the day", async () => {
  const env = buildEnv({ d1Results: [] });
  const result = await planOddsFetches(env, new Date("2026-05-28T01:00:00Z"), "20260528");
  expect(result).toStrictEqual({ queued: 0, skipped: 0 });
});

it("enqueues fetch-odds job and acquires lock in normal window", async () => {
  const env = buildEnv({
    d1Results: [
      {
        lastOddsFetchAt: null,
        raceKey: "nar:20260528:42:01",
        raceStartAtJst: "2026-05-28T15:00:00+09:00",
        source: "nar",
      },
    ],
  });
  const result = await planOddsFetches(env, new Date("2026-05-28T03:00:00Z"), "20260528");
  expect(result.queued).toBeGreaterThan(0);
  expect(env.REALTIME_HOT_JOBS.send).toHaveBeenCalled();
  expect(env.ODDS_HOT_KV.put).toHaveBeenCalled();
});

it("skips enqueue when lock already held outside final window", async () => {
  const env = buildEnv({
    d1Results: [
      {
        lastOddsFetchAt: null,
        raceKey: "nar:20260528:42:01",
        raceStartAtJst: "2026-05-28T15:00:00+09:00",
        source: "nar",
      },
    ],
    kvGet: async (key) => (key.startsWith("odds:enqueue-lock:") ? "1" : null),
  });
  const result = await planOddsFetches(env, new Date("2026-05-28T03:00:00Z"), "20260528");
  expect(result.skipped).toBeGreaterThan(0);
  expect(env.REALTIME_HOT_JOBS.send).not.toHaveBeenCalled();
});

it("skips enqueue when lock already held inside final window", async () => {
  const env = buildEnv({
    d1Results: [
      {
        lastOddsFetchAt: null,
        raceKey: "nar:20260528:42:01",
        raceStartAtJst: "2026-05-28T12:00:00+09:00",
        source: "nar",
      },
    ],
    kvGet: async (key) => (key.startsWith("odds:enqueue-lock:") ? "1" : null),
  });
  const result = await planOddsFetches(env, new Date("2026-05-28T03:00:00Z"), "20260528");
  expect(result.skipped).toBeGreaterThan(0);
  expect(env.REALTIME_HOT_JOBS.send).not.toHaveBeenCalled();
});

it("uses KV race-list cache when available and skips D1", async () => {
  const cachedNar: RaceListEntry[] = [
    {
      lastOddsFetchAt: null,
      raceKey: "nar:20260528:42:01",
      raceStartAtJst: "2026-05-28T15:00:00+09:00",
      source: "nar",
    },
  ];
  const env = buildEnv({
    kvGet: async (key) => {
      if (key === "odds:race-list:v1:nar:20260528") {
        return JSON.stringify(cachedNar);
      }
      if (key === "odds:race-list:v1:jra:20260528") {
        return JSON.stringify([]);
      }
      return null;
    },
  });
  const result = await planOddsFetches(env, new Date("2026-05-28T03:00:00Z"), "20260528");
  expect(result.queued).toBeGreaterThan(0);
  expect(env.REALTIME_HOT_DB.prepare).not.toHaveBeenCalled();
});

it("skips past races whose finalSlot was already captured (no catch-up needed)", async () => {
  // raceStart 11:00 JST, now 12:00 JST (1 hour past). finalSlot = 11:02.
  // lastOddsFetchAt at 11:03 already past final slot → skip.
  const pastNar: RaceListEntry[] = [
    {
      lastOddsFetchAt: "2026-05-28T11:03:00+09:00",
      raceKey: "nar:20260528:42:01",
      raceStartAtJst: "2026-05-28T11:00:00+09:00",
      source: "nar",
    },
  ];
  const env = buildEnv({
    kvGet: async (key) => {
      if (key === "odds:race-list:v1:nar:20260528") {
        return JSON.stringify(pastNar);
      }
      if (key === "odds:race-list:v1:jra:20260528") {
        return JSON.stringify([]);
      }
      return null;
    },
  });
  const result = await planOddsFetches(env, new Date("2026-05-28T03:00:00Z"), "20260528");
  expect(result).toStrictEqual({ queued: 0, skipped: 1 });
  expect(env.REALTIME_HOT_JOBS.send).not.toHaveBeenCalled();
  expect(env.ODDS_HOT_KV.put).not.toHaveBeenCalled();
});

it("future race still enqueues when ttl is positive", async () => {
  const futureNar: RaceListEntry[] = [
    {
      lastOddsFetchAt: null,
      raceKey: "nar:20260528:42:01",
      raceStartAtJst: "2026-05-28T15:00:00+09:00",
      source: "nar",
    },
  ];
  const env = buildEnv({
    kvGet: async (key) => {
      if (key === "odds:race-list:v1:nar:20260528") {
        return JSON.stringify(futureNar);
      }
      if (key === "odds:race-list:v1:jra:20260528") {
        return JSON.stringify([]);
      }
      return null;
    },
  });
  const result = await planOddsFetches(env, new Date("2026-05-28T03:00:00Z"), "20260528");
  expect(result).toStrictEqual({ queued: 1, skipped: 0 });
  expect(env.REALTIME_HOT_JOBS.send).toHaveBeenCalledWith({
    raceKey: "nar:20260528:42:01",
    type: "fetch-odds",
  });
});

it("enqueues a past race within 60 minutes when lastOddsFetchAt never captured the final slot (catch-up)", async () => {
  // raceStart = 2026-05-28T15:00:00+09:00, now = 2026-05-28T06:30:00Z = 15:30 JST
  // → 30 minutes past raceStart, final slot = 15:02, lastOddsFetchAt at 14:50 < 15:02.
  const pastNar: RaceListEntry[] = [
    {
      lastOddsFetchAt: "2026-05-28T14:50:00+09:00",
      raceKey: "nar:20260528:42:01",
      raceStartAtJst: "2026-05-28T15:00:00+09:00",
      source: "nar",
    },
  ];
  const env = buildEnv({
    kvGet: async (key) => {
      if (key === "odds:race-list:v1:nar:20260528") {
        return JSON.stringify(pastNar);
      }
      if (key === "odds:race-list:v1:jra:20260528") {
        return JSON.stringify([]);
      }
      return null;
    },
  });
  const result = await planOddsFetches(env, new Date("2026-05-28T06:30:00Z"), "20260528");
  expect(result).toStrictEqual({ queued: 1, skipped: 0 });
  expect(env.REALTIME_HOT_JOBS.send).toHaveBeenCalledWith({
    raceKey: "nar:20260528:42:01",
    type: "fetch-odds",
  });
});

it("skips past race when lastOddsFetchAt already covers the final slot", async () => {
  // Final slot at 15:02, lastOddsFetchAt at 15:05 — final captured, no catch-up.
  const pastNar: RaceListEntry[] = [
    {
      lastOddsFetchAt: "2026-05-28T15:05:00+09:00",
      raceKey: "nar:20260528:42:01",
      raceStartAtJst: "2026-05-28T15:00:00+09:00",
      source: "nar",
    },
  ];
  const env = buildEnv({
    kvGet: async (key) => {
      if (key === "odds:race-list:v1:nar:20260528") {
        return JSON.stringify(pastNar);
      }
      if (key === "odds:race-list:v1:jra:20260528") {
        return JSON.stringify([]);
      }
      return null;
    },
  });
  const result = await planOddsFetches(env, new Date("2026-05-28T06:30:00Z"), "20260528");
  expect(result).toStrictEqual({ queued: 0, skipped: 1 });
  expect(env.REALTIME_HOT_JOBS.send).not.toHaveBeenCalled();
});

it("skips past race that is older than the 360-minute catch-up window", async () => {
  // raceStart = 04:00 JST, now = 11:00 JST → 420 minutes past, beyond catch-up.
  const ancientNar: RaceListEntry[] = [
    {
      lastOddsFetchAt: null,
      raceKey: "nar:20260528:42:01",
      raceStartAtJst: "2026-05-28T04:00:00+09:00",
      source: "nar",
    },
  ];
  const env = buildEnv({
    kvGet: async (key) => {
      if (key === "odds:race-list:v1:nar:20260528") {
        return JSON.stringify(ancientNar);
      }
      if (key === "odds:race-list:v1:jra:20260528") {
        return JSON.stringify([]);
      }
      return null;
    },
  });
  const result = await planOddsFetches(env, new Date("2026-05-28T02:00:00Z"), "20260528");
  expect(result).toStrictEqual({ queued: 0, skipped: 1 });
  expect(env.REALTIME_HOT_JOBS.send).not.toHaveBeenCalled();
});
