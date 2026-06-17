// Run with bun.
import type { DurableObjectState } from "@cloudflare/workers-types";
import { afterEach, expect, it, vi } from "vitest";

import { OddsCacheHot, getOddsCacheId, readCachedOdds, writeCachedOdds } from "./odds-cache";
import type { Env } from "./types";

interface FakeStorage {
  delete: ReturnType<typeof vi.fn>;
  get: ReturnType<typeof vi.fn>;
  list: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
  setAlarm: ReturnType<typeof vi.fn>;
}

const buildState = (
  initial: Map<string, unknown>,
): { state: { storage: FakeStorage }; storage: FakeStorage } => {
  const storage: FakeStorage = {
    delete: vi.fn(async (key: string) => {
      initial.delete(key);
    }),
    get: vi.fn(async (key: string) => initial.get(key)),
    list: vi.fn(async () => initial),
    put: vi.fn(async (key: string, value: unknown) => {
      initial.set(key, value);
    }),
    setAlarm: vi.fn(async (_at: number) => undefined),
  };
  return { state: { storage }, storage };
};

const buildEnv = (ttlSeconds?: string): Env =>
  ({ ODDS_DO_TTL_SECONDS: ttlSeconds }) as unknown as Env;

afterEach(() => {
  vi.restoreAllMocks();
});

it("returns 400 when raceKey is missing", async () => {
  const { state } = buildState(new Map());
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv());
  const response = await cache.fetch(new Request("https://odds-cache/"));
  expect(response.status).toBe(400);
});

it("PUT stores payload with expiresAt and sets alarm", async () => {
  vi.spyOn(Date, "now").mockReturnValue(1_700_000_000_000);
  const { state, storage } = buildState(new Map());
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv("120"));
  const response = await cache.fetch(
    new Request("https://odds-cache/races/key1", {
      body: JSON.stringify({
        fetchedAt: "2026-05-28T10:00:00+09:00",
        latest: { tansho: [{ combination: "01", odds: 2.5, rank: 1 }] },
      }),
      method: "PUT",
    }),
  );
  expect(response.status).toBe(200);
  expect(storage.put).toHaveBeenCalledTimes(1);
  expect(storage.setAlarm.mock.calls[0]![0]).toBe(1_700_000_000_000 + 120 * 1000 + 60_000);
});

it("PUT enforces a 60-second minimum TTL when env value is below the floor", async () => {
  vi.spyOn(Date, "now").mockReturnValue(2_000_000);
  const { state, storage } = buildState(new Map());
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv("1"));
  await cache.fetch(
    new Request("https://odds-cache/races/keyFloor", {
      body: JSON.stringify({ fetchedAt: "x", latest: {} }),
      method: "PUT",
    }),
  );
  expect(storage.setAlarm.mock.calls[0]![0]).toBe(2_000_000 + 60 * 1000 + 60_000);
});

it("PUT uses default TTL when env unset", async () => {
  vi.spyOn(Date, "now").mockReturnValue(1_000_000);
  const { state, storage } = buildState(new Map());
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv());
  await cache.fetch(
    new Request("https://odds-cache/races/key1", {
      body: JSON.stringify({ fetchedAt: "x", latest: {} }),
      method: "PUT",
    }),
  );
  expect(storage.setAlarm.mock.calls[0]![0]).toBe(1_000_000 + 7200 * 1000 + 60_000);
});

it("PUT stores entry with 4h TTL when ODDS_DO_TTL_SECONDS=14400", async () => {
  vi.spyOn(Date, "now").mockReturnValue(1_000_000);
  const { state, storage } = buildState(new Map());
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv("14400"));
  await cache.fetch(
    new Request("https://odds-cache/races/narKey", {
      body: JSON.stringify({ fetchedAt: "2026-06-11T03:00:00+09:00", latest: {} }),
      method: "PUT",
    }),
  );
  expect(storage.setAlarm.mock.calls[0]![0]).toBe(1_000_000 + 14400 * 1000 + 60_000);
});

it("PUT falls back to default TTL when env value is invalid", async () => {
  vi.spyOn(Date, "now").mockReturnValue(1_000_000);
  const { state, storage } = buildState(new Map());
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv("bad"));
  await cache.fetch(
    new Request("https://odds-cache/races/key1", {
      body: JSON.stringify({ fetchedAt: "x", latest: {} }),
      method: "PUT",
    }),
  );
  expect(storage.setAlarm.mock.calls[0]![0]).toBe(1_000_000 + 7200 * 1000 + 60_000);
});

it("PUT falls back to default TTL when env value is zero", async () => {
  vi.spyOn(Date, "now").mockReturnValue(1_000_000);
  const { state, storage } = buildState(new Map());
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv("0"));
  await cache.fetch(
    new Request("https://odds-cache/races/key1", {
      body: JSON.stringify({ fetchedAt: "x", latest: {} }),
      method: "PUT",
    }),
  );
  expect(storage.setAlarm.mock.calls[0]![0]).toBe(1_000_000 + 7200 * 1000 + 60_000);
});

it("PUT with same fetchedAt is idempotent and does not append history", async () => {
  vi.spyOn(Date, "now").mockReturnValue(5_000_000);
  const initial = new Map<string, unknown>([
    [
      "key1",
      {
        expiresAt: 9_000_000,
        fetchedAt: "2026-05-28T10:00:00+09:00",
        historyByType: {
          tansho: [
            {
              combination: "01",
              fetchedAt: "2026-05-28T10:00:00+09:00",
              odds: 2.5,
              rank: 1,
            },
          ],
        },
        latest: { tansho: [{ combination: "01", odds: 2.5, rank: 1 }] },
      },
    ],
  ]);
  const { state, storage } = buildState(initial);
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv());
  const response = await cache.fetch(
    new Request("https://odds-cache/races/key1", {
      body: JSON.stringify({
        fetchedAt: "2026-05-28T10:00:00+09:00",
        latest: { tansho: [{ combination: "01", odds: 2.5, rank: 1 }] },
      }),
      method: "PUT",
    }),
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ ok: true, skipped: true });
  expect(storage.put).not.toHaveBeenCalled();
  expect(storage.setAlarm).not.toHaveBeenCalled();
});

it("PUT with new fetchedAt appends points to historyByType", async () => {
  vi.spyOn(Date, "now").mockReturnValue(5_000_000);
  const initial = new Map<string, unknown>([
    [
      "key1",
      {
        expiresAt: 9_000_000,
        fetchedAt: "2026-05-28T10:00:00+09:00",
        historyByType: {
          tansho: [
            {
              combination: "01",
              fetchedAt: "2026-05-28T10:00:00+09:00",
              odds: 2.5,
              rank: 1,
            },
          ],
        },
        latest: { tansho: [{ combination: "01", odds: 2.5, rank: 1 }] },
      },
    ],
  ]);
  const { state, storage } = buildState(initial);
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv("120"));
  await cache.fetch(
    new Request("https://odds-cache/races/key1", {
      body: JSON.stringify({
        fetchedAt: "2026-05-28T10:01:00+09:00",
        latest: { tansho: [{ combination: "01", odds: 2.4, rank: 1 }] },
      }),
      method: "PUT",
    }),
  );
  expect(storage.put).toHaveBeenCalledTimes(1);
  const stored = storage.put.mock.calls[0]![1] as { historyByType: { tansho: unknown[] } };
  expect(stored.historyByType.tansho).toStrictEqual([
    { combination: "01", fetchedAt: "2026-05-28T10:00:00+09:00", odds: 2.5, rank: 1 },
    { combination: "01", fetchedAt: "2026-05-28T10:01:00+09:00", odds: 2.4, rank: 1 },
  ]);
});

it("PUT caps tansho history at the tansho budget (800) and drops the oldest entries", async () => {
  vi.spyOn(Date, "now").mockReturnValue(5_000_000);
  // Seed 800 existing tansho points (50 snapshots * 16 horses), then push one
  // more 16-horse snapshot. The oldest 16 points (first snapshot) should be evicted.
  const existingPoints = Array.from({ length: 800 }, (_, index) => ({
    combination: String((index % 16) + 1).padStart(2, "0"),
    fetchedAt: `2026-05-28T08:${String(Math.floor(index / 16)).padStart(2, "0")}:00+09:00`,
    odds: 3.0,
    rank: 1,
  }));
  const newSnapshot = Array.from({ length: 16 }, (_, index) => ({
    combination: String(index + 1).padStart(2, "0"),
    odds: 2.5,
    rank: index + 1,
  }));
  const initial = new Map<string, unknown>([
    [
      "key1",
      {
        expiresAt: 9_000_000,
        fetchedAt: "2026-05-28T09:59:00+09:00",
        historyByType: { tansho: existingPoints },
        latest: {},
      },
    ],
  ]);
  const { state, storage } = buildState(initial);
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv());
  await cache.fetch(
    new Request("https://odds-cache/races/key1", {
      body: JSON.stringify({
        fetchedAt: "2026-05-28T10:00:00+09:00",
        latest: { tansho: newSnapshot },
      }),
      method: "PUT",
    }),
  );
  const stored = storage.put.mock.calls[0]![1] as { historyByType: { tansho: unknown[] } };
  expect(stored.historyByType.tansho.length).toBe(800);
  // The oldest 16 points (snapshot at 08:00) are evicted; first remaining is 08:01.
  expect(stored.historyByType.tansho[0]).toStrictEqual({
    combination: "01",
    fetchedAt: "2026-05-28T08:01:00+09:00",
    odds: 3.0,
    rank: 1,
  });
  expect(stored.historyByType.tansho[799]).toStrictEqual({
    combination: "16",
    fetchedAt: "2026-05-28T10:00:00+09:00",
    odds: 2.5,
    rank: 16,
  });
});

it("PUT coerces missing odds and rank to null when converting to trend points", async () => {
  vi.spyOn(Date, "now").mockReturnValue(5_000_000);
  const { state, storage } = buildState(new Map());
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv());
  await cache.fetch(
    new Request("https://odds-cache/races/key1", {
      body: JSON.stringify({
        fetchedAt: "2026-05-28T10:00:00+09:00",
        latest: { tansho: [{ combination: "01" }] },
      }),
      method: "PUT",
    }),
  );
  const stored = storage.put.mock.calls[0]![1] as {
    historyByType: { tansho: unknown[] };
  };
  expect(stored.historyByType.tansho).toStrictEqual([
    { combination: "01", fetchedAt: "2026-05-28T10:00:00+09:00", odds: null, rank: null },
  ]);
});

it("PUT ignores odds types whose latest array is empty", async () => {
  vi.spyOn(Date, "now").mockReturnValue(5_000_000);
  const { state, storage } = buildState(new Map());
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv());
  await cache.fetch(
    new Request("https://odds-cache/races/key1", {
      body: JSON.stringify({
        fetchedAt: "2026-05-28T10:00:00+09:00",
        latest: { fukusho: [], tansho: [{ combination: "01", odds: 2.5, rank: 1 }] },
      }),
      method: "PUT",
    }),
  );
  const stored = storage.put.mock.calls[0]![1] as {
    historyByType: Partial<Record<string, unknown[]>>;
  };
  expect(stored.historyByType).toStrictEqual({
    tansho: [{ combination: "01", fetchedAt: "2026-05-28T10:00:00+09:00", odds: 2.5, rank: 1 }],
  });
});

it("GET returns 404 when payload missing", async () => {
  const { state } = buildState(new Map());
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv());
  const response = await cache.fetch(new Request("https://odds-cache/races/key1"));
  expect(response.status).toBe(404);
});

it("GET returns built payload with history derived from tansho when fresh", async () => {
  vi.spyOn(Date, "now").mockReturnValue(500);
  const storage = new Map<string, unknown>([
    [
      "key1",
      {
        expiresAt: 1_000,
        fetchedAt: "2026-05-28T10:00:00+09:00",
        historyByType: {
          tansho: [
            {
              combination: "01",
              fetchedAt: "2026-05-28T10:00:00+09:00",
              odds: 2.5,
              rank: 1,
            },
          ],
        },
        latest: { tansho: [{ combination: "01", odds: 2.5, rank: 1 }] },
      },
    ],
  ]);
  const { state } = buildState(storage);
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv());
  const response = await cache.fetch(new Request("https://odds-cache/races/key1"));
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({
    fetchedAt: "2026-05-28T10:00:00+09:00",
    history: [
      {
        horseNumber: "01",
        points: [
          {
            fetchedAt: "2026-05-28T10:00:00+09:00",
            horseNumber: "01",
            odds: 2.5,
            popularity: 1,
          },
        ],
      },
    ],
    historyByType: {
      tansho: [
        {
          combination: "01",
          points: [
            {
              combination: "01",
              fetchedAt: "2026-05-28T10:00:00+09:00",
              odds: 2.5,
              rank: 1,
            },
          ],
        },
      ],
    },
    latest: { tansho: [{ combination: "01", odds: 2.5, rank: 1 }] },
  });
});

it("GET returns an empty history when no tansho points are stored", async () => {
  vi.spyOn(Date, "now").mockReturnValue(500);
  const storage = new Map<string, unknown>([
    [
      "key1",
      {
        expiresAt: 1_000,
        fetchedAt: "2026-05-28T10:00:00+09:00",
        historyByType: {},
        latest: { fukusho: [{ combination: "02", odds: 1.1, rank: 1 }] },
      },
    ],
  ]);
  const { state } = buildState(storage);
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv());
  const response = await cache.fetch(new Request("https://odds-cache/races/key1"));
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({
    fetchedAt: "2026-05-28T10:00:00+09:00",
    history: [],
    historyByType: {},
    latest: { fukusho: [{ combination: "02", odds: 1.1, rank: 1 }] },
  });
});

it("GET returns 404 when payload expired", async () => {
  vi.spyOn(Date, "now").mockReturnValue(2_000);
  const storage = new Map<string, unknown>([
    [
      "key1",
      {
        expiresAt: 1_000,
        fetchedAt: "x",
        historyByType: {},
        latest: {},
      },
    ],
  ]);
  const { state } = buildState(storage);
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv());
  const response = await cache.fetch(new Request("https://odds-cache/races/key1"));
  expect(response.status).toBe(404);
});

it("returns 405 on unsupported method", async () => {
  const { state } = buildState(new Map());
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv());
  const response = await cache.fetch(
    new Request("https://odds-cache/races/key1", { method: "DELETE" }),
  );
  expect(response.status).toBe(405);
});

it("different raceKeys maintain independent state", async () => {
  vi.spyOn(Date, "now").mockReturnValue(5_000_000);
  const { state, storage } = buildState(new Map());
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv());
  await cache.fetch(
    new Request("https://odds-cache/races/keyA", {
      body: JSON.stringify({
        fetchedAt: "2026-05-28T10:00:00+09:00",
        latest: { tansho: [{ combination: "01", odds: 2.5, rank: 1 }] },
      }),
      method: "PUT",
    }),
  );
  await cache.fetch(
    new Request("https://odds-cache/races/keyB", {
      body: JSON.stringify({
        fetchedAt: "2026-05-28T11:00:00+09:00",
        latest: { tansho: [{ combination: "02", odds: 7.7, rank: 4 }] },
      }),
      method: "PUT",
    }),
  );
  expect(storage.put.mock.calls[0]![0]).toBe("keyA");
  expect(storage.put.mock.calls[1]![0]).toBe("keyB");
});

it("alarm deletes expired entries and reschedules to the next earliest", async () => {
  vi.spyOn(Date, "now").mockReturnValue(1_000);
  const storage = new Map<string, unknown>([
    ["expired", { expiresAt: 500, fetchedAt: "x", historyByType: {}, latest: {} }],
    ["near", { expiresAt: 1_500, fetchedAt: "x", historyByType: {}, latest: {} }],
    ["far", { expiresAt: 3_000, fetchedAt: "x", historyByType: {}, latest: {} }],
  ]);
  const { state, storage: storageMock } = buildState(storage);
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv());
  await cache.alarm();
  expect(storageMock.delete).toHaveBeenCalledTimes(1);
  expect(storageMock.setAlarm.mock.calls[0]![0]).toBe(1_500 + 60_000);
});

it("alarm does not reschedule when no entries remain", async () => {
  vi.spyOn(Date, "now").mockReturnValue(10_000);
  const storage = new Map<string, unknown>([
    ["expired", { expiresAt: 500, fetchedAt: "x", historyByType: {}, latest: {} }],
  ]);
  const { state, storage: storageMock } = buildState(storage);
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv());
  await cache.alarm();
  expect(storageMock.setAlarm).not.toHaveBeenCalled();
});

it("getOddsCacheId calls env binding idFromName", () => {
  const idFromName = vi.fn(() => "id-1");
  const env = { ODDS_CACHE: { idFromName } } as unknown as Env;
  const id = getOddsCacheId(env, "key1");
  expect(id).toBe("id-1");
});

it("readCachedOdds returns null on non-ok response", async () => {
  const fetchMock = vi.fn(
    async (_url: string, _init?: RequestInit): Promise<Response> =>
      new Response(null, { status: 404 }),
  );
  const env = {
    ODDS_CACHE: {
      get: () => ({ fetch: fetchMock }),
      idFromName: () => "id-1",
    },
  } as unknown as Env;
  const result = await readCachedOdds(env, "key1");
  expect(result).toBeNull();
});

it("readCachedOdds parses the response body when ok", async () => {
  const fetchMock = vi.fn(
    async (_url: string, _init?: RequestInit): Promise<Response> =>
      new Response(
        JSON.stringify({
          fetchedAt: "x",
          history: [],
          historyByType: {},
          latest: {},
        }),
        {
          headers: { "content-type": "application/json" },
          status: 200,
        },
      ),
  );
  const env = {
    ODDS_CACHE: {
      get: () => ({ fetch: fetchMock }),
      idFromName: () => "id-1",
    },
  } as unknown as Env;
  const result = await readCachedOdds(env, "key1");
  expect(result).toStrictEqual({
    fetchedAt: "x",
    history: [],
    historyByType: {},
    latest: {},
  });
});

it("writeCachedOdds issues a PUT request against the stub", async () => {
  const fetchMock = vi.fn(
    async (_url: string, _init?: RequestInit): Promise<Response> =>
      new Response(null, { status: 200 }),
  );
  const env = {
    ODDS_CACHE: {
      get: () => ({ fetch: fetchMock }),
      idFromName: () => "id-1",
    },
  } as unknown as Env;
  await writeCachedOdds(env, "race key", { fetchedAt: "x", latest: {} });
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(fetchMock.mock.calls[0]![0]).toBe("https://odds-cache/races/race%20key");
});

it("PUT retains the latest 50 tansho snapshots when 100 snapshots are pushed", async () => {
  // 100 snapshots * 16 horses = 1600 points. tansho budget = 800, so the
  // newest 50 snapshots (800 points) survive and the oldest 50 are evicted.
  vi.spyOn(Date, "now").mockReturnValue(5_000_000);
  const seedSnapshots = Array.from({ length: 99 }, (_, snapshotIndex) =>
    Array.from({ length: 16 }, (_, horseIndex) => ({
      combination: String(horseIndex + 1).padStart(2, "0"),
      fetchedAt: `2026-05-28T${String(Math.floor(snapshotIndex / 60) + 8).padStart(2, "0")}:${String(snapshotIndex % 60).padStart(2, "0")}:00+09:00`,
      odds: 2.5,
      rank: horseIndex + 1,
    })),
  ).flat();
  const initial = new Map<string, unknown>([
    [
      "key1",
      {
        expiresAt: 9_000_000,
        fetchedAt: "2026-05-28T09:38:00+09:00",
        historyByType: { tansho: seedSnapshots },
        latest: {},
      },
    ],
  ]);
  const { state, storage } = buildState(initial);
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv());
  const newSnapshot = Array.from({ length: 16 }, (_, horseIndex) => ({
    combination: String(horseIndex + 1).padStart(2, "0"),
    odds: 9.9,
    rank: horseIndex + 1,
  }));
  await cache.fetch(
    new Request("https://odds-cache/races/key1", {
      body: JSON.stringify({
        fetchedAt: "2026-05-28T11:00:00+09:00",
        latest: { tansho: newSnapshot },
      }),
      method: "PUT",
    }),
  );
  const stored = storage.put.mock.calls[0]![1] as {
    historyByType: {
      tansho: { combination: string; fetchedAt: string; odds: number; rank: number }[];
    };
  };
  // History length matches tansho budget exactly.
  expect(stored.historyByType.tansho.length).toBe(800);
  // Newest snapshot is at the tail.
  expect(stored.historyByType.tansho[799]).toStrictEqual({
    combination: "16",
    fetchedAt: "2026-05-28T11:00:00+09:00",
    odds: 9.9,
    rank: 16,
  });
  // 100 snapshots total, budget 50 -> the 51st snapshot (index 50) is the
  // oldest survivor. Snapshot indices 0..49 are the original seed snapshots
  // 50..98 followed by the new snapshot pushed in this PUT.
  // First surviving snapshot = seed snapshot 50, which has fetchedAt 08:50.
  expect(stored.historyByType.tansho[0]).toStrictEqual({
    combination: "01",
    fetchedAt: "2026-05-28T08:50:00+09:00",
    odds: 2.5,
    rank: 1,
  });
});

it("PUT caps umaren history at the umaren budget (120) regardless of input size", async () => {
  // Two umaren snapshots of 120 combinations each = 240 points. umaren
  // budget = 120, so only the newest snapshot fits.
  vi.spyOn(Date, "now").mockReturnValue(5_000_000);
  const seedSnapshot = Array.from({ length: 120 }, (_, combinationIndex) => ({
    combination: `01-${String(combinationIndex + 2).padStart(2, "0")}`,
    fetchedAt: "2026-05-28T10:00:00+09:00",
    odds: 12.3,
    rank: combinationIndex + 1,
  }));
  const initial = new Map<string, unknown>([
    [
      "key1",
      {
        expiresAt: 9_000_000,
        fetchedAt: "2026-05-28T10:00:00+09:00",
        historyByType: { umaren: seedSnapshot },
        latest: {},
      },
    ],
  ]);
  const { state, storage } = buildState(initial);
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv());
  const newSnapshot = Array.from({ length: 120 }, (_, combinationIndex) => ({
    combination: `01-${String(combinationIndex + 2).padStart(2, "0")}`,
    odds: 22.5,
    rank: combinationIndex + 1,
  }));
  await cache.fetch(
    new Request("https://odds-cache/races/key1", {
      body: JSON.stringify({
        fetchedAt: "2026-05-28T10:05:00+09:00",
        latest: { umaren: newSnapshot },
      }),
      method: "PUT",
    }),
  );
  const stored = storage.put.mock.calls[0]![1] as {
    historyByType: {
      umaren: { combination: string; fetchedAt: string; odds: number; rank: number }[];
    };
  };
  expect(stored.historyByType.umaren.length).toBe(120);
  // Oldest survivor is the first combination of the newly pushed snapshot;
  // every seed point should have been evicted.
  expect(stored.historyByType.umaren[0]).toStrictEqual({
    combination: "01-02",
    fetchedAt: "2026-05-28T10:05:00+09:00",
    odds: 22.5,
    rank: 1,
  });
  expect(stored.historyByType.umaren[119]).toStrictEqual({
    combination: "01-121",
    fetchedAt: "2026-05-28T10:05:00+09:00",
    odds: 22.5,
    rank: 120,
  });
});

it("PUT applies independent per-type budgets when tansho and umaren merge together", async () => {
  // 60 existing tansho + 1 new snapshot (16 horses) = 76 points -> under
  // budget 800 -> all retained. Plus a 120-combination umaren snapshot ->
  // exactly fills umaren budget.
  vi.spyOn(Date, "now").mockReturnValue(5_000_000);
  const seedTansho = Array.from({ length: 60 }, (_, index) => ({
    combination: String((index % 16) + 1).padStart(2, "0"),
    fetchedAt: `2026-05-28T09:${String(index).padStart(2, "0")}:00+09:00`,
    odds: 3.0,
    rank: 1,
  }));
  const initial = new Map<string, unknown>([
    [
      "key1",
      {
        expiresAt: 9_000_000,
        fetchedAt: "2026-05-28T09:59:00+09:00",
        historyByType: { tansho: seedTansho },
        latest: {},
      },
    ],
  ]);
  const { state, storage } = buildState(initial);
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv());
  const tanshoSnapshot = Array.from({ length: 16 }, (_, horseIndex) => ({
    combination: String(horseIndex + 1).padStart(2, "0"),
    odds: 5.5,
    rank: horseIndex + 1,
  }));
  const umarenSnapshot = Array.from({ length: 120 }, (_, combinationIndex) => ({
    combination: `01-${String(combinationIndex + 2).padStart(2, "0")}`,
    odds: 18.0,
    rank: combinationIndex + 1,
  }));
  await cache.fetch(
    new Request("https://odds-cache/races/key1", {
      body: JSON.stringify({
        fetchedAt: "2026-05-28T10:00:00+09:00",
        latest: { tansho: tanshoSnapshot, umaren: umarenSnapshot },
      }),
      method: "PUT",
    }),
  );
  const stored = storage.put.mock.calls[0]![1] as {
    historyByType: {
      tansho: { combination: string; fetchedAt: string; odds: number; rank: number }[];
      umaren: { combination: string; fetchedAt: string; odds: number; rank: number }[];
    };
  };
  // tansho stays under budget (60 + 16 = 76).
  expect(stored.historyByType.tansho.length).toBe(76);
  // umaren fills its budget exactly.
  expect(stored.historyByType.umaren.length).toBe(120);
});

it("StoredOddsState serialized at every type's cap stays under the 120 KiB DO budget", async () => {
  // Worst-case StoredOddsState: each odds type filled to its cap with realistic
  // combinations (2-digit horse numbers) and 4-digit odds. Guards against
  // budget changes that would risk hitting the 128 KiB DO per-value limit.
  const tanshoPoints = Array.from({ length: 800 }, (_, index) => ({
    combination: String((index % 16) + 1).padStart(2, "0"),
    fetchedAt: "2026-05-28T10:00:00+09:00",
    odds: 9999.9,
    rank: 18,
  }));
  const fukushoPoints = Array.from({ length: 160 }, (_, index) => ({
    combination: String((index % 16) + 1).padStart(2, "0"),
    fetchedAt: "2026-05-28T10:00:00+09:00",
    odds: 9999.9,
    rank: 18,
  }));
  const wakurenPoints = Array.from({ length: 144 }, (_, index) => ({
    combination: `${String((index % 8) + 1).padStart(2, "0")}-${String((index % 8) + 2).padStart(2, "0")}`,
    fetchedAt: "2026-05-28T10:00:00+09:00",
    odds: 9999.9,
    rank: 18,
  }));
  const umarenPoints = Array.from({ length: 120 }, (_, index) => ({
    combination: `${String((index % 16) + 1).padStart(2, "0")}-${String((index % 16) + 2).padStart(2, "0")}`,
    fetchedAt: "2026-05-28T10:00:00+09:00",
    odds: 9999.9,
    rank: 18,
  }));
  const umatanPoints = Array.from({ length: 60 }, (_, index) => ({
    combination: `${String((index % 16) + 1).padStart(2, "0")}-${String((index % 16) + 2).padStart(2, "0")}`,
    fetchedAt: "2026-05-28T10:00:00+09:00",
    odds: 9999.9,
    rank: 18,
  }));
  const widePoints = Array.from({ length: 60 }, (_, index) => ({
    combination: `${String((index % 16) + 1).padStart(2, "0")}-${String((index % 16) + 2).padStart(2, "0")}`,
    fetchedAt: "2026-05-28T10:00:00+09:00",
    odds: 9999.9,
    rank: 18,
  }));
  const renpukuPoints = Array.from({ length: 30 }, (_, index) => ({
    combination: `${String((index % 16) + 1).padStart(2, "0")}-${String((index % 16) + 2).padStart(2, "0")}-${String((index % 16) + 3).padStart(2, "0")}`,
    fetchedAt: "2026-05-28T10:00:00+09:00",
    odds: 9999.9,
    rank: 18,
  }));
  const rentanPoints = Array.from({ length: 30 }, (_, index) => ({
    combination: `${String((index % 16) + 1).padStart(2, "0")}-${String((index % 16) + 2).padStart(2, "0")}-${String((index % 16) + 3).padStart(2, "0")}`,
    fetchedAt: "2026-05-28T10:00:00+09:00",
    odds: 9999.9,
    rank: 18,
  }));
  const state = {
    expiresAt: 1_700_000_000_000,
    fetchedAt: "2026-05-28T10:00:00+09:00",
    historyByType: {
      "3renpuku": renpukuPoints,
      "3rentan": rentanPoints,
      fukusho: fukushoPoints,
      tansho: tanshoPoints,
      umaren: umarenPoints,
      umatan: umatanPoints,
      wakuren: wakurenPoints,
      wide: widePoints,
    },
    latest: {},
  };
  const json = JSON.stringify(state);
  const byteLength = new TextEncoder().encode(json).length;
  // Cloudflare DO per-value limit is 128 KiB. Leave ~8 KiB headroom.
  expect(byteLength).toBeLessThan(122880);
});
