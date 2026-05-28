// Run with bun.
import { afterEach, expect, it, vi } from "vitest";

import { OddsCacheHot, getOddsCacheId, readCachedOdds, writeCachedOdds } from "./odds-cache";
import type { Env, OddsHistoryPoint } from "./types";

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

const ODDS_HISTORY_POINT: OddsHistoryPoint = {
  fetchedAt: "2026-05-28T10:00:00+09:00",
  horseNumber: "01",
  odds: 2.5,
  popularity: 1,
};

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
        history: [ODDS_HISTORY_POINT],
        latest: {},
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
      body: JSON.stringify({ fetchedAt: "x", history: [], latest: {} }),
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
      body: JSON.stringify({ fetchedAt: "x", history: [], latest: {} }),
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
      body: JSON.stringify({ fetchedAt: "x", history: [], latest: {} }),
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
      body: JSON.stringify({ fetchedAt: "x", history: [], latest: {} }),
      method: "PUT",
    }),
  );
  expect(storage.setAlarm.mock.calls[0]![0]).toBe(1_000_000 + 7200 * 1000 + 60_000);
});

it("GET returns 404 when payload missing", async () => {
  const { state } = buildState(new Map());
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv());
  const response = await cache.fetch(new Request("https://odds-cache/races/key1"));
  expect(response.status).toBe(404);
});

it("GET returns payload when fresh", async () => {
  vi.spyOn(Date, "now").mockReturnValue(500);
  const storage = new Map<string, unknown>([
    ["key1", { expiresAt: 1_000, fetchedAt: "x", history: [], latest: {} }],
  ]);
  const { state } = buildState(storage);
  const cache = new OddsCacheHot(state as unknown as DurableObjectState, buildEnv());
  const response = await cache.fetch(new Request("https://odds-cache/races/key1"));
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({
    expiresAt: 1_000,
    fetchedAt: "x",
    history: [],
    latest: {},
  });
});

it("GET returns 404 when payload expired", async () => {
  vi.spyOn(Date, "now").mockReturnValue(2_000);
  const storage = new Map<string, unknown>([
    ["key1", { expiresAt: 1_000, fetchedAt: "x", history: [], latest: {} }],
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

it("alarm deletes expired entries and reschedules to the next earliest", async () => {
  vi.spyOn(Date, "now").mockReturnValue(1_000);
  const storage = new Map<string, unknown>([
    ["expired", { expiresAt: 500, fetchedAt: "x", history: [], latest: {} }],
    ["near", { expiresAt: 1_500, fetchedAt: "x", history: [], latest: {} }],
    ["far", { expiresAt: 3_000, fetchedAt: "x", history: [], latest: {} }],
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
    ["expired", { expiresAt: 500, fetchedAt: "x", history: [], latest: {} }],
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
      new Response(JSON.stringify({ fetchedAt: "x", history: [], latest: {} }), {
        headers: { "content-type": "application/json" },
        status: 200,
      }),
  );
  const env = {
    ODDS_CACHE: {
      get: () => ({ fetch: fetchMock }),
      idFromName: () => "id-1",
    },
  } as unknown as Env;
  const result = await readCachedOdds(env, "key1");
  expect(result).toStrictEqual({ fetchedAt: "x", history: [], latest: {} });
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
  await writeCachedOdds(env, "race key", { fetchedAt: "x", history: [], latest: {} });
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(fetchMock.mock.calls[0]![0]).toBe("https://odds-cache/races/race%20key");
});
