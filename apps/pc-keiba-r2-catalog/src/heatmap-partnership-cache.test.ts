// Run with bun (bunx vitest).
import { afterEach, expect, it, vi } from "vitest";
import {
  createPartnershipCohortCache,
  type PartnershipCohortRequest,
} from "./heatmap-partnership-cache";

const makeRequest = (): PartnershipCohortRequest => ({
  cache: {
    match: vi.fn(async () => undefined),
    put: vi.fn(async () => undefined),
    delete: vi.fn(async () => true),
  },
  execute: vi.fn(async () => [{ starts: 10, wins: 1 }]),
  kv: {
    get: vi.fn(async () => null),
    put: vi.fn(async () => undefined),
    delete: vi.fn(async () => undefined),
  },
  query: {
    config: {
      R2_SQL_ACCOUNT_ID: "account",
      R2_SQL_BUCKET_NAME: "bucket",
      R2_SQL_NAMESPACE: "ns",
      R2_SQL_TOKEN: "test",
    },
    horseIds: [],
    scope: {
      surface: "芝",
      date: "20260913",
      keibajoCode: "06",
      kind: "jockeyVenue",
      source: "jra",
      revision: "snapshot1",
    },
  },
  warm: true,
});

afterEach(() => vi.useRealTimers());

it("coalesces shared warm work and reuses both target-race and value memory caches", async () => {
  const request = makeRequest();
  const cache = createPartnershipCohortCache();
  const results = await Promise.all([cache.load(request), cache.load(request)]);
  expect(results[0]).toStrictEqual({
    targetRaces: [{ starts: 10, wins: 1 }],
    values: [{ starts: 10, wins: 1 }],
  });
  expect(request.execute).toHaveBeenCalledTimes(2);
  expect(request.kv.put).toHaveBeenCalledTimes(2);
  expect(await cache.load({ ...request, warm: false })).not.toBeNull();
  expect(request.cache.match).toHaveBeenCalledTimes(2);
  expect(request.kv.put).toHaveBeenCalledWith(
    expect.stringMatching(/:targets$/u),
    expect.any(String),
    { expirationTtl: 129600 },
  );
  expect(request.kv.put).toHaveBeenCalledWith(
    expect.stringMatching(/:values$/u),
    expect.any(String),
    { expirationTtl: 129600 },
  );
});

it("does not reuse a turf cohort for dirt or obstacle races", async () => {
  const request = makeRequest();
  const cache = createPartnershipCohortCache();
  await cache.load(request);
  expect(
    await cache.load({
      ...request,
      warm: false,
      query: { ...request.query, scope: { ...request.query.scope, surface: "ダート" } },
    }),
  ).toBeNull();
  expect(
    await cache.load({
      ...request,
      warm: false,
      query: { ...request.query, scope: { ...request.query.scope, surface: "障害" } },
    }),
  ).toBeNull();
  expect(request.execute).toHaveBeenCalledTimes(2);
});

it("read-only misses never aggregate or write fictitious empty values", async () => {
  const request = makeRequest();
  expect(await createPartnershipCohortCache().load({ ...request, warm: false })).toBeNull();
  expect(request.execute).not.toHaveBeenCalled();
  expect(request.kv.put).not.toHaveBeenCalled();
});

it("prioritizes regional cache over KV and SQL", async () => {
  const request = makeRequest();
  vi.mocked(request.cache.match).mockImplementation(async () => Response.json([{ starts: 42 }]));
  expect(await createPartnershipCohortCache().load(request)).toStrictEqual({
    targetRaces: [{ starts: 42 }],
    values: [{ starts: 42 }],
  });
  expect(request.kv.get).not.toHaveBeenCalled();
  expect(request.execute).not.toHaveBeenCalled();
});

it("repairs malformed regional copies from KV despite regional write failures", async () => {
  const request = makeRequest();
  vi.mocked(request.cache.match).mockImplementation(async () => new Response("broken"));
  vi.mocked(request.cache.put).mockRejectedValue(new Error("Cache API unavailable"));
  vi.mocked(request.kv.get).mockResolvedValue('[{"starts":7}]');
  expect(await createPartnershipCohortCache().load(request)).toStrictEqual({
    targetRaces: [{ starts: 7 }],
    values: [{ starts: 7 }],
  });
  expect(request.cache.delete).toHaveBeenCalledTimes(2);
  expect(request.execute).not.toHaveBeenCalled();
});

it.each(["{}", "[null]", "[[]]", "[1]", "invalid"])(
  "treats corrupt durable JSON as unavailable on read: %s",
  async (body) => {
    const request = makeRequest();
    vi.mocked(request.cache.match).mockImplementation(
      async () => new Response("unavailable", { status: 503 }),
    );
    vi.mocked(request.kv.get).mockResolvedValue(body);
    expect(await createPartnershipCohortCache().load({ ...request, warm: false })).toBeNull();
    expect(request.execute).not.toHaveBeenCalled();
  },
);

it("expires isolate entries and bounds large bodies without sacrificing durable caching", async () => {
  vi.useFakeTimers();
  const request = makeRequest();
  const cache = createPartnershipCohortCache();
  await cache.load(request);
  vi.advanceTimersByTime(60_001);
  await cache.load(request);
  expect(request.execute).toHaveBeenCalledTimes(4);
  vi.mocked(request.execute).mockResolvedValue([{ data: "x".repeat(300_000) }]);
  const large = createPartnershipCohortCache();
  await large.load(request);
  await large.load(request);
  expect(request.execute).toHaveBeenCalledTimes(8);
});

it("bounds entry count and isolates horse cohorts by sorted target horse IDs", async () => {
  const request = makeRequest();
  const cache = createPartnershipCohortCache();
  await Promise.all(
    Array.from({ length: 33 }, (_, index) =>
      cache.load({
        ...request,
        query: {
          ...request.query,
          horseIds: [String(2023100000 + index)],
          scope: { ...request.query.scope, kind: "horseJockey" },
        },
      }),
    ),
  );
  expect(request.execute).toHaveBeenCalledTimes(66);
  await cache.load({
    ...request,
    query: {
      ...request.query,
      horseIds: ["2023100000"],
      scope: { ...request.query.scope, kind: "horseJockey" },
    },
  });
  expect(request.execute).toHaveBeenCalledTimes(68);
});

it("does not publish failures and permits retry after a failed singleflight", async () => {
  const request = makeRequest();
  const cache = createPartnershipCohortCache();
  vi.mocked(request.execute).mockRejectedValue(new Error("SQL unavailable"));
  await expect(cache.load(request)).rejects.toThrow("SQL unavailable");
  expect(request.kv.put).not.toHaveBeenCalled();
  vi.mocked(request.execute).mockResolvedValue([]);
  vi.mocked(request.cache.put).mockRejectedValue(new Error("Cache API unavailable"));
  expect(await cache.load(request)).toStrictEqual({ targetRaces: [], values: [] });
});
