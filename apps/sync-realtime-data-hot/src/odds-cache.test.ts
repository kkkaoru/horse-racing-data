// Run with bun.
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

it("PUT caps history at the per-race max and drops the oldest entries", async () => {
  vi.spyOn(Date, "now").mockReturnValue(5_000_000);
  const existingPoints = Array.from({ length: 60 }, (_, index) => ({
    combination: "01",
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
        latest: { tansho: [{ combination: "01", odds: 2.5, rank: 1 }] },
      }),
      method: "PUT",
    }),
  );
  const stored = storage.put.mock.calls[0]![1] as { historyByType: { tansho: unknown[] } };
  expect(stored.historyByType.tansho.length).toBe(60);
  expect(stored.historyByType.tansho[0]).toStrictEqual({
    combination: "01",
    fetchedAt: "2026-05-28T09:01:00+09:00",
    odds: 3.0,
    rank: 1,
  });
  expect(stored.historyByType.tansho[59]).toStrictEqual({
    combination: "01",
    fetchedAt: "2026-05-28T10:00:00+09:00",
    odds: 2.5,
    rank: 1,
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
