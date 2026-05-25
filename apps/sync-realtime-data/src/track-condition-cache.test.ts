// run with: bun run test
import { afterEach, expect, it, vi } from "vitest";
import {
  TrackConditionCache,
  readCachedTrackCondition,
  writeCachedTrackCondition,
} from "./track-condition-cache";
import type { Env, TrackCondition } from "./types";

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
    setAlarm: vi.fn(async (_at: number) => {}),
  };
  return { state: { storage }, storage };
};

const buildEnv = (ttlSeconds?: string): Env => {
  const env = {
    TRACK_CONDITION_DO_TTL_SECONDS: ttlSeconds,
  } satisfies Partial<Env>;
  return env as unknown as Env;
};

const TRACK_CONDITION: TrackCondition = {
  dirt: {
    condition: null,
    measurementDate: null,
    moisture: { finalBend: null, finalFurlong: null, measuredAt: null },
  },
  fetchedAt: "2026-05-12T12:00:00+09:00",
  sourceUpdatedAt: null,
  turf: {
    condition: "良",
    courseLayout: null,
    cushionMeasuredAt: null,
    cushionValue: null,
    going: null,
    height: { japaneseZoysiaGrass: null, perennialRyegrass: null },
    measurementDate: null,
    moisture: { finalBend: null, finalFurlong: null, measuredAt: null },
  },
  weather: null,
};

afterEach(() => {
  vi.restoreAllMocks();
});

it("returns 400 when raceKey is missing", async () => {
  const { state } = buildState(new Map());
  const cache = new TrackConditionCache(state as unknown as DurableObjectState, buildEnv());
  const response = await cache.fetch(new Request("https://track-condition-cache/"));
  expect(response.status).toBe(400);
  expect(await response.json()).toStrictEqual({ error: "raceKey is required" });
});

it("PUT writes payload with expiresAt and schedules alarm", async () => {
  vi.spyOn(Date, "now").mockReturnValue(1_700_000_000_000);
  const { state, storage } = buildState(new Map());
  const cache = new TrackConditionCache(state as unknown as DurableObjectState, buildEnv("3600"));
  const response = await cache.fetch(
    new Request("https://track-condition-cache/races/jra%3A2026%3A0512%3A08%3A01", {
      body: JSON.stringify(TRACK_CONDITION),
      method: "PUT",
    }),
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ ok: true });
  expect(storage.put).toHaveBeenCalledTimes(1);
  const expectedExpiresAt = 1_700_000_000_000 + 3600 * 1000;
  expect(storage.put.mock.calls[0]!).toStrictEqual([
    "jra:2026:0512:08:01",
    { ...TRACK_CONDITION, expiresAt: expectedExpiresAt },
  ]);
  expect(storage.setAlarm).toHaveBeenCalledTimes(1);
  expect(storage.setAlarm.mock.calls[0]![0]).toBe(expectedExpiresAt + 60_000);
});

it("GET returns 404 when payload missing", async () => {
  const { state } = buildState(new Map());
  const cache = new TrackConditionCache(state as unknown as DurableObjectState, buildEnv());
  const response = await cache.fetch(
    new Request("https://track-condition-cache/races/jra%3A2026%3A0512%3A08%3A01"),
  );
  expect(response.status).toBe(404);
});

it("GET returns 404 when payload expired", async () => {
  vi.spyOn(Date, "now").mockReturnValue(2_000);
  const storage = new Map<string, unknown>([
    ["jra:2026:0512:08:01", { ...TRACK_CONDITION, expiresAt: 1_000 }],
  ]);
  const { state } = buildState(storage);
  const cache = new TrackConditionCache(state as unknown as DurableObjectState, buildEnv());
  const response = await cache.fetch(
    new Request("https://track-condition-cache/races/jra%3A2026%3A0512%3A08%3A01"),
  );
  expect(response.status).toBe(404);
});

it("GET strips expiresAt when payload is fresh", async () => {
  vi.spyOn(Date, "now").mockReturnValue(500);
  const storage = new Map<string, unknown>([
    ["jra:2026:0512:08:01", { ...TRACK_CONDITION, expiresAt: 1_000 }],
  ]);
  const { state } = buildState(storage);
  const cache = new TrackConditionCache(state as unknown as DurableObjectState, buildEnv());
  const response = await cache.fetch(
    new Request("https://track-condition-cache/races/jra%3A2026%3A0512%3A08%3A01"),
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual(TRACK_CONDITION);
});

it("returns 405 for unsupported method", async () => {
  const { state } = buildState(new Map());
  const cache = new TrackConditionCache(state as unknown as DurableObjectState, buildEnv());
  const response = await cache.fetch(
    new Request("https://track-condition-cache/races/key", { method: "DELETE" }),
  );
  expect(response.status).toBe(405);
  expect(await response.json()).toStrictEqual({ error: "method not allowed" });
});

it("alarm deletes expired entries and reschedules to next non-expired", async () => {
  vi.spyOn(Date, "now").mockReturnValue(1_000);
  const storage = new Map<string, unknown>([
    ["k-expired", { ...TRACK_CONDITION, expiresAt: 500 }],
    ["k-near", { ...TRACK_CONDITION, expiresAt: 2_000 }],
    ["k-far", { ...TRACK_CONDITION, expiresAt: 5_000 }],
  ]);
  const { state, storage: storageMock } = buildState(storage);
  const cache = new TrackConditionCache(state as unknown as DurableObjectState, buildEnv());
  await cache.alarm();
  expect(storageMock.delete).toHaveBeenCalledTimes(1);
  expect(storageMock.delete.mock.calls[0]![0]).toBe("k-expired");
  expect(storageMock.setAlarm).toHaveBeenCalledTimes(1);
  expect(storageMock.setAlarm.mock.calls[0]![0]).toBe(2_000 + 60_000);
});

it("alarm does not schedule when no entries remain", async () => {
  vi.spyOn(Date, "now").mockReturnValue(10_000);
  const storage = new Map<string, unknown>([
    ["k-expired", { ...TRACK_CONDITION, expiresAt: 500 }],
  ]);
  const { state, storage: storageMock } = buildState(storage);
  const cache = new TrackConditionCache(state as unknown as DurableObjectState, buildEnv());
  await cache.alarm();
  expect(storageMock.delete).toHaveBeenCalledTimes(1);
  expect(storageMock.setAlarm).not.toHaveBeenCalled();
});

it("readCachedTrackCondition returns null when stub responds non-ok", async () => {
  const fetchMock = vi.fn(
    async (_url: string, _init?: RequestInit): Promise<Response> =>
      new Response(null, { status: 404 }),
  );
  const env = {
    TRACK_CONDITION_CACHE: {
      get: () => ({ fetch: fetchMock }),
      idFromName: (raceKey: string) => raceKey,
    },
  } as unknown as Env;
  const result = await readCachedTrackCondition(env, "jra:2026:0512:08:01");
  expect(result).toBeNull();
});

it("readCachedTrackCondition parses payload when stub responds ok", async () => {
  const fetchMock = vi.fn(
    async (_url: string, _init?: RequestInit): Promise<Response> =>
      new Response(JSON.stringify(TRACK_CONDITION), {
        headers: { "content-type": "application/json" },
        status: 200,
      }),
  );
  const env = {
    TRACK_CONDITION_CACHE: {
      get: () => ({ fetch: fetchMock }),
      idFromName: (raceKey: string) => raceKey,
    },
  } as unknown as Env;
  const result = await readCachedTrackCondition(env, "jra:2026:0512:08:01");
  expect(result).toStrictEqual(TRACK_CONDITION);
});

it("writeCachedTrackCondition PUTs the payload to the stub", async () => {
  const fetchMock = vi.fn(
    async (_url: string, _init?: RequestInit): Promise<Response> =>
      new Response(null, { status: 200 }),
  );
  const env = {
    TRACK_CONDITION_CACHE: {
      get: () => ({ fetch: fetchMock }),
      idFromName: (raceKey: string) => raceKey,
    },
  } as unknown as Env;
  await writeCachedTrackCondition(env, "jra:2026:0512:08:01", TRACK_CONDITION);
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(fetchMock.mock.calls[0]![0]).toBe(
    "https://track-condition-cache/races/jra%3A2026%3A0512%3A08%3A01",
  );
  expect(fetchMock.mock.calls[0]![1]!.method).toBe("PUT");
  expect(fetchMock.mock.calls[0]![1]!.body).toBe(JSON.stringify(TRACK_CONDITION));
});
