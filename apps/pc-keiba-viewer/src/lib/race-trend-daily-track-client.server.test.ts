// Run with bun (vitest). `bun run --filter pc-keiba-viewer test`
import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

import type {
  RaceTrendDailyTrackQuery,
  RaceTrendDailyTrackRow,
} from "horse-racing-realtime/race-trend-daily-track-types";

import {
  buildRaceTrendDailyTrackCacheKey,
  fetchRaceTrendDailyTrack,
} from "./race-trend-daily-track-client.server";

type AnyMockFn = (...args: never[]) => unknown;
type ServiceFetch = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

interface CacheStub {
  delete: ReturnType<typeof vi.fn<AnyMockFn>>;
  match: ReturnType<typeof vi.fn<AnyMockFn>>;
  put: ReturnType<typeof vi.fn<AnyMockFn>>;
}

const buildCacheStub = (): CacheStub => ({
  delete: vi.fn<AnyMockFn>().mockResolvedValue(true),
  match: vi.fn<AnyMockFn>().mockResolvedValue(undefined),
  put: vi.fn<AnyMockFn>().mockResolvedValue(undefined),
});

const installCaches = (cache: CacheStub): void => {
  Object.defineProperty(globalThis, "caches", {
    configurable: true,
    value: { default: cache },
  });
};

beforeEach(() => {
  installCaches(buildCacheStub());
});

afterEach(() => {
  Reflect.deleteProperty(globalThis, "caches");
});

const buildRow = (raceBango: string): RaceTrendDailyTrackRow => ({
  fetchedAt: "2026-05-30T07:30:00.000Z",
  finishedAt: "2026-05-30T07:20:00.000Z",
  isComplete: true,
  raceBango,
  raceKey: `jra:2026:0530:05:${raceBango}`,
  runningStyles: [],
  starterRows: [],
});

const buildHitResponse = (rows: RaceTrendDailyTrackRow[]): Response =>
  new Response(JSON.stringify({ races: rows }), {
    headers: { "Content-Type": "application/json", "X-Race-Trend-DO": "hit" },
    status: 200,
  });

const buildMissResponse = (): Response =>
  new Response(JSON.stringify({ races: [] }), {
    headers: { "Content-Type": "application/json", "X-Race-Trend-DO": "miss" },
    status: 200,
  });

it("buildRaceTrendDailyTrackCacheKey includes source, ymd, keibajoCode and beforeRaceBango for jra", () => {
  expect(
    buildRaceTrendDailyTrackCacheKey({
      beforeRaceBango: "07",
      keibajoCode: "05",
      source: "jra",
      targetYmd: "20260530",
    }),
  ).toBe("race-trend-do:v1:jra:20260530:05:07");
});

it("buildRaceTrendDailyTrackCacheKey produces a different key when keibajoCode differs", () => {
  expect(
    buildRaceTrendDailyTrackCacheKey({
      beforeRaceBango: "07",
      keibajoCode: "06",
      source: "jra",
      targetYmd: "20260530",
    }),
  ).toBe("race-trend-do:v1:jra:20260530:06:07");
});

it("fetchRaceTrendDailyTrack returns hit when DO header is hit and rows are populated for jra", async () => {
  const fetchMock = vi.fn<ServiceFetch>(async () =>
    Promise.resolve(buildHitResponse([buildRow("01"), buildRow("02")])),
  );
  const result = await fetchRaceTrendDailyTrack(
    { REALTIME_DATA: { fetch: fetchMock } },
    {
      beforeRaceBango: "03",
      keibajoCode: "05",
      source: "jra",
      targetYmd: "20260530",
    },
  );
  expect(result.status).toBe("hit");
  expect(result.rows).toStrictEqual([
    {
      fetchedAt: "2026-05-30T07:30:00.000Z",
      finishedAt: "2026-05-30T07:20:00.000Z",
      isComplete: true,
      raceBango: "01",
      raceKey: "jra:2026:0530:05:01",
      runningStyles: [],
      starterRows: [],
    },
    {
      fetchedAt: "2026-05-30T07:30:00.000Z",
      finishedAt: "2026-05-30T07:20:00.000Z",
      isComplete: true,
      raceBango: "02",
      raceKey: "jra:2026:0530:05:02",
      runningStyles: [],
      starterRows: [],
    },
  ]);
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(fetchMock).toHaveBeenCalledWith(
    "https://internal/race-trend-daily-track?source=jra&ymd=20260530&keibajo=05&beforeRaceBango=03",
  );
});

it("fetchRaceTrendDailyTrack treats DO hit header with empty races as miss for nar", async () => {
  const fetchMock = vi.fn<ServiceFetch>(async () =>
    Promise.resolve(
      new Response(JSON.stringify({ races: [] }), {
        headers: { "X-Race-Trend-DO": "hit" },
        status: 200,
      }),
    ),
  );
  const result = await fetchRaceTrendDailyTrack(
    { REALTIME_DATA: { fetch: fetchMock } },
    {
      beforeRaceBango: "05",
      keibajoCode: "47",
      source: "nar",
      targetYmd: "20260530",
    },
  );
  expect(result.status).toBe("miss");
  expect(result.rows).toStrictEqual([]);
});

it("fetchRaceTrendDailyTrack returns miss when DO header indicates miss", async () => {
  const fetchMock = vi.fn<ServiceFetch>(async () => Promise.resolve(buildMissResponse()));
  const result = await fetchRaceTrendDailyTrack(
    { REALTIME_DATA: { fetch: fetchMock } },
    {
      beforeRaceBango: "07",
      keibajoCode: "05",
      source: "jra",
      targetYmd: "20260530",
    },
  );
  expect(result.status).toBe("miss");
  expect(result.rows).toStrictEqual([]);
});

it("fetchRaceTrendDailyTrack returns error when service binding fetch throws", async () => {
  const fetchMock = vi.fn<ServiceFetch>(async () => {
    throw new Error("boom");
  });
  const result = await fetchRaceTrendDailyTrack(
    { REALTIME_DATA: { fetch: fetchMock } },
    {
      beforeRaceBango: "07",
      keibajoCode: "05",
      source: "jra",
      targetYmd: "20260530",
    },
  );
  expect(result.status).toBe("error");
  expect(result.rows).toStrictEqual([]);
});

it("fetchRaceTrendDailyTrack returns error when DO responds with 5xx", async () => {
  const fetchMock = vi.fn<ServiceFetch>(async () =>
    Promise.resolve(new Response("server error", { status: 503 })),
  );
  const result = await fetchRaceTrendDailyTrack(
    { REALTIME_DATA: { fetch: fetchMock } },
    {
      beforeRaceBango: "07",
      keibajoCode: "05",
      source: "jra",
      targetYmd: "20260530",
    },
  );
  expect(result.status).toBe("error");
  expect(result.rows).toStrictEqual([]);
});

it("fetchRaceTrendDailyTrack returns error when REALTIME_DATA binding is missing", async () => {
  const result = await fetchRaceTrendDailyTrack(
    {},
    {
      beforeRaceBango: "07",
      keibajoCode: "05",
      source: "jra",
      targetYmd: "20260530",
    },
  );
  expect(result.status).toBe("error");
  expect(result.rows).toStrictEqual([]);
});

it("fetchRaceTrendDailyTrack returns error when env itself is null", async () => {
  const result = await fetchRaceTrendDailyTrack(null, {
    beforeRaceBango: "07",
    keibajoCode: "05",
    source: "jra",
    targetYmd: "20260530",
  });
  expect(result.status).toBe("error");
  expect(result.rows).toStrictEqual([]);
});

it("fetchRaceTrendDailyTrack returns error when DO returns 200 with malformed body shape", async () => {
  const fetchMock = vi.fn<ServiceFetch>(async () =>
    Promise.resolve(
      new Response(JSON.stringify({ unexpected: true }), {
        headers: { "X-Race-Trend-DO": "hit" },
        status: 200,
      }),
    ),
  );
  const result = await fetchRaceTrendDailyTrack(
    { REALTIME_DATA: { fetch: fetchMock } },
    {
      beforeRaceBango: "07",
      keibajoCode: "05",
      source: "jra",
      targetYmd: "20260530",
    },
  );
  expect(result.status).toBe("error");
  expect(result.rows).toStrictEqual([]);
});

it("fetchRaceTrendDailyTrack short-circuits to the cached value on the second call without invoking the service binding", async () => {
  const cacheStore = new Map<string, Response>();
  const cacheStub: CacheStub = {
    delete: vi.fn<AnyMockFn>().mockResolvedValue(true),
    match: vi.fn<AnyMockFn>(async (request: Request) => {
      const cached = cacheStore.get(request.url);
      if (!cached) return undefined;
      return cached.clone();
    }),
    put: vi.fn<AnyMockFn>(async (request: Request, response: Response) => {
      cacheStore.set(request.url, response);
    }),
  };
  installCaches(cacheStub);
  const fetchMock = vi.fn<ServiceFetch>(async () =>
    Promise.resolve(buildHitResponse([buildRow("01")])),
  );
  const env = { REALTIME_DATA: { fetch: fetchMock } };
  const query: RaceTrendDailyTrackQuery = {
    beforeRaceBango: "02",
    keibajoCode: "05",
    source: "jra",
    targetYmd: "20260530",
  };
  const first = await fetchRaceTrendDailyTrack(env, query);
  expect(first.status).toBe("hit");
  expect(fetchMock).toHaveBeenCalledTimes(1);
  const second = await fetchRaceTrendDailyTrack(env, query);
  expect(second.status).toBe("hit");
  expect(second.rows).toStrictEqual([
    {
      fetchedAt: "2026-05-30T07:30:00.000Z",
      finishedAt: "2026-05-30T07:20:00.000Z",
      isComplete: true,
      raceBango: "01",
      raceKey: "jra:2026:0530:05:01",
      runningStyles: [],
      starterRows: [],
    },
  ]);
  expect(fetchMock).toHaveBeenCalledTimes(1);
});

it("fetchRaceTrendDailyTrack reissues the service binding fetch when keibajoCode differs (cache key is venue-scoped)", async () => {
  const cacheStore = new Map<string, Response>();
  const cacheStub: CacheStub = {
    delete: vi.fn<AnyMockFn>().mockResolvedValue(true),
    match: vi.fn<AnyMockFn>(async (request: Request) => {
      const cached = cacheStore.get(request.url);
      if (!cached) return undefined;
      return cached.clone();
    }),
    put: vi.fn<AnyMockFn>(async (request: Request, response: Response) => {
      cacheStore.set(request.url, response);
    }),
  };
  installCaches(cacheStub);
  const fetchMock = vi.fn<ServiceFetch>(async () =>
    Promise.resolve(buildHitResponse([buildRow("01")])),
  );
  const env = { REALTIME_DATA: { fetch: fetchMock } };
  const first = await fetchRaceTrendDailyTrack(env, {
    beforeRaceBango: "02",
    keibajoCode: "05",
    source: "jra",
    targetYmd: "20260530",
  });
  expect(first.status).toBe("hit");
  expect(fetchMock).toHaveBeenCalledTimes(1);
  const second = await fetchRaceTrendDailyTrack(env, {
    beforeRaceBango: "02",
    keibajoCode: "06",
    source: "jra",
    targetYmd: "20260530",
  });
  expect(second.status).toBe("hit");
  expect(fetchMock).toHaveBeenCalledTimes(2);
});
