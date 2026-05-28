// run with: bun run test
import { afterEach, expect, it, vi } from "vitest";
import {
  PremiumPaddockCache,
  clearCachedPremiumPaddock,
  readCachedPremiumPaddock,
  writeCachedPremiumPaddock,
} from "./premium-paddock-cache";
import type { Env } from "./types";

interface FakeStorage {
  get: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
  deleteAll: ReturnType<typeof vi.fn>;
}

interface FakeState {
  storage: FakeStorage;
}

const buildEnv = (ttlSeconds?: string): Env => {
  const env = {
    PREMIUM_PADDOCK_DO_TTL_SECONDS: ttlSeconds,
  } satisfies Partial<Env>;
  return env as unknown as Env;
};

const buildState = (
  payload: unknown,
  cachedAt?: number,
): { state: FakeState; storage: FakeStorage } => {
  const storage: FakeStorage = {
    deleteAll: vi.fn(async () => {}),
    get: vi.fn(async (key: string) => {
      if (key === "payload") {
        return payload;
      }
      if (key === "cachedAt") {
        return cachedAt;
      }
      return undefined;
    }),
    put: vi.fn(async (_key: string, _value: unknown) => {}),
  };
  return {
    state: { storage },
    storage,
  };
};

afterEach(() => {
  vi.restoreAllMocks();
});

it("returns 404 when payload missing on GET", async () => {
  const { state } = buildState(undefined, undefined);
  const cache = new PremiumPaddockCache(state as unknown as DurableObjectState, buildEnv());
  const response = await cache.fetch(new Request("https://cache.local/"));
  expect(response.status).toBe(404);
});

it("returns 404 when payload is stale (env ttl)", async () => {
  vi.spyOn(Date, "now").mockReturnValue(1_000_000);
  const { state } = buildState({ horses: [] }, 1_000_000 - 61_000);
  const cache = new PremiumPaddockCache(state as unknown as DurableObjectState, buildEnv("60"));
  const response = await cache.fetch(new Request("https://cache.local/"));
  expect(response.status).toBe(404);
});

it("returns payload as json when fresh", async () => {
  vi.spyOn(Date, "now").mockReturnValue(1_000_000);
  const { state } = buildState({ horses: ["a"] }, 1_000_000 - 1_000);
  const cache = new PremiumPaddockCache(state as unknown as DurableObjectState, buildEnv());
  const response = await cache.fetch(new Request("https://cache.local/"));
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ horses: ["a"] });
});

it("writes payload and cachedAt on PUT", async () => {
  vi.spyOn(Date, "now").mockReturnValue(2_000_000);
  const { state, storage } = buildState(undefined, undefined);
  const cache = new PremiumPaddockCache(state as unknown as DurableObjectState, buildEnv());
  const response = await cache.fetch(
    new Request("https://cache.local/", {
      body: JSON.stringify({ horses: ["b"] }),
      method: "PUT",
    }),
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ ok: true });
  expect(storage.put).toHaveBeenCalledTimes(2);
  expect(storage.put.mock.calls[0]).toStrictEqual(["payload", { horses: ["b"] }]);
  expect(storage.put.mock.calls[1]).toStrictEqual(["cachedAt", 2_000_000]);
});

it("clears storage on POST /clear", async () => {
  const { state, storage } = buildState(undefined, undefined);
  const cache = new PremiumPaddockCache(state as unknown as DurableObjectState, buildEnv());
  const response = await cache.fetch(new Request("https://cache.local/clear", { method: "POST" }));
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ ok: true });
  expect(storage.deleteAll).toHaveBeenCalledTimes(1);
});

it("returns 405 for unsupported method", async () => {
  const { state } = buildState(undefined, undefined);
  const cache = new PremiumPaddockCache(state as unknown as DurableObjectState, buildEnv());
  const response = await cache.fetch(new Request("https://cache.local/", { method: "DELETE" }));
  expect(response.status).toBe(405);
});

it("readCachedPremiumPaddock returns null when stub responds non-ok", async () => {
  const fetchMock = vi.fn(
    async (_url: string, _init?: RequestInit): Promise<Response> =>
      new Response(null, { status: 404 }),
  );
  const env = {
    PREMIUM_PADDOCK_CACHE: {
      get: () => ({ fetch: fetchMock }),
      idFromName: (raceKey: string) => raceKey,
    },
  } as unknown as Env;
  const result = await readCachedPremiumPaddock(env, "jra:2026:0512:08:01");
  expect(result).toBeNull();
  expect(fetchMock).toHaveBeenCalledTimes(1);
});

it("readCachedPremiumPaddock parses payload when stub responds ok", async () => {
  const fetchMock = vi.fn(
    async () =>
      new Response(JSON.stringify({ horses: ["x"] }), {
        headers: { "content-type": "application/json" },
        status: 200,
      }),
  );
  const env = {
    PREMIUM_PADDOCK_CACHE: {
      get: () => ({ fetch: fetchMock }),
      idFromName: (raceKey: string) => raceKey,
    },
  } as unknown as Env;
  const result = await readCachedPremiumPaddock(env, "jra:2026:0512:08:01");
  expect(result).toStrictEqual({ horses: ["x"] });
});

it("writeCachedPremiumPaddock issues a PUT against the stub", async () => {
  const fetchMock = vi.fn(
    async (_url: string, _init?: RequestInit): Promise<Response> =>
      new Response(null, { status: 200 }),
  );
  const env = {
    PREMIUM_PADDOCK_CACHE: {
      get: () => ({ fetch: fetchMock }),
      idFromName: (raceKey: string) => raceKey,
    },
  } as unknown as Env;
  await writeCachedPremiumPaddock(env, "jra:2026:0512:08:01", { ok: true });
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(fetchMock.mock.calls[0]![0]).toBe("https://cache.local/");
  expect(fetchMock.mock.calls[0]![1]!.method).toBe("PUT");
  expect(fetchMock.mock.calls[0]![1]!.body).toBe('{"ok":true}');
});

it("clearCachedPremiumPaddock issues a POST against /clear", async () => {
  const fetchMock = vi.fn(
    async (_url: string, _init?: RequestInit): Promise<Response> =>
      new Response(null, { status: 200 }),
  );
  const env = {
    PREMIUM_PADDOCK_CACHE: {
      get: () => ({ fetch: fetchMock }),
      idFromName: (raceKey: string) => raceKey,
    },
  } as unknown as Env;
  await clearCachedPremiumPaddock(env, "jra:2026:0512:08:01");
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(fetchMock.mock.calls[0]![0]).toBe("https://cache.local/clear");
  expect(fetchMock.mock.calls[0]![1]!.method).toBe("POST");
});
