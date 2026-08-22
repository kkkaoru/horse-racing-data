import { expect, it } from "vitest";

import { cacheRequestFor, heatmapStatsDescriptor, kvKeyFor } from "./cache";
import type { CacheStore, Env, Fetcher, KvStore, WorkerDependencies } from "./types";
import { handleRequest } from "./worker";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const bloodlineSqlRow = {
  category: "sire",
  name: "Deep Impact",
  places: 4,
  shows: 6,
  starts: 20,
  umaban: 1,
  wins: 2,
};

const similarSqlRow = {
  kind: "jockey",
  name: "Take",
  places: 3,
  shows: 5,
  starts: 10,
  umaban: 1,
  wins: 1,
};

const heatmapUrl =
  "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=08&day=22&keibajoCode=07&raceNumber=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=1";

const createHeatmapHarness = () => {
  const cacheEntries = new Map<string, Response>();
  const kvEntries = new Map<string, string>();
  const fetchCalls: string[] = [];
  const queryBodies: string[] = [];
  const cache: CacheStore = {
    async delete(request) {
      return cacheEntries.delete(request.url);
    },
    async match(request) {
      return cacheEntries.get(request.url)?.clone();
    },
    async put(request, response) {
      cacheEntries.set(request.url, response.clone());
    },
  };
  const kv: KvStore = {
    async delete(key) {
      kvEntries.delete(key);
    },
    async get(key) {
      return kvEntries.get(key) ?? null;
    },
    async put(key, value) {
      kvEntries.set(key, value);
    },
  };
  const fetchImpl: Fetcher = async (_input, init) => {
    const rawBody = init?.body;
    const text = typeof rawBody === "string" ? rawBody : "";
    const parsed: unknown = JSON.parse(text);
    const query = isRecord(parsed) && typeof parsed.query === "string" ? parsed.query : "";
    queryBodies.push(query);
    fetchCalls.push(query.includes("'sire' AS category") ? "bloodline" : "similar");
    if (query.includes("'sire' AS category")) {
      return Response.json({ result: { rows: [bloodlineSqlRow] }, success: true });
    }
    return Response.json({ result: { rows: [similarSqlRow] }, success: true });
  };
  const env: Env = {
    CACHE_TTL_SECONDS: "15",
    CATALOG_KV: kv,
    KV_TTL_SECONDS: "120",
    R2_SQL_ACCOUNT_ID: "account",
    R2_SQL_BUCKET_NAME: "pc-keiba-r2-catalog",
    R2_SQL_NAMESPACE: "pc_keiba",
    R2_SQL_TOKEN: "r2-secret",
  };
  const dependencies: WorkerDependencies = { cache, fetchImpl };
  return { cacheEntries, dependencies, env, fetchCalls, kvEntries, queryBodies };
};

it("coalesces concurrent heatmap misses into one R2 SQL pair", async () => {
  const harness = createHeatmapHarness();
  const [first, second] = await Promise.all([
    handleRequest(new Request(heatmapUrl), harness.env, harness.dependencies),
    handleRequest(new Request(heatmapUrl), harness.env, harness.dependencies),
  ]);
  expect(first.status).toBe(200);
  expect(second.status).toBe(200);
  expect(harness.fetchCalls.toSorted()).toStrictEqual(["bloodline", "similar"]);
});

it("stores heatmap stats with a 36 hour catalog cache TTL", async () => {
  const harness = createHeatmapHarness();
  await handleRequest(new Request(heatmapUrl), harness.env, harness.dependencies);
  const descriptor = heatmapStatsDescriptor({
    date: "20260822",
    includeDistance: true,
    includeSurface: true,
    includeTurn: true,
    includeVenue: true,
    keibajoCode: "07",
    raceBango: "08",
    source: "jra",
    years: 10,
  });
  const cached = harness.cacheEntries.get(cacheRequestFor(descriptor).url);
  expect(cached?.headers.get("Cache-Control")).toBe("public, max-age=129600");
});

it("queries R2 SQL for heatmap bloodline and similar aggregates without details", async () => {
  const harness = createHeatmapHarness();
  const response = await handleRequest(new Request(heatmapUrl), harness.env, harness.dependencies);
  expect(response.status).toBe(200);
  expect(response.headers.get("X-Catalog-Cache")).toBe("r2-sql");
  await expect(response.json()).resolves.toStrictEqual({
    bloodlineRows: [
      {
        category: "sire",
        details: [],
        name: "Deep Impact",
        places: 4,
        shows: 6,
        starts: 20,
        umaban: 1,
        wins: 2,
      },
    ],
    similarRows: [
      {
        details: [],
        kind: "jockey",
        name: "Take",
        places: 3,
        shows: 5,
        starts: 10,
        umaban: 1,
        wins: 1,
      },
    ],
  });
  expect(harness.fetchCalls.toSorted()).toStrictEqual(["bloodline", "similar"]);
});

it("reads heatmap stats from Cache API then KV before R2 SQL", async () => {
  const harness = createHeatmapHarness();
  const descriptor = heatmapStatsDescriptor({
    date: "20260822",
    includeDistance: true,
    includeSurface: true,
    includeTurn: true,
    includeVenue: true,
    keibajoCode: "07",
    raceBango: "08",
    source: "jra",
    years: 10,
  });
  const cachedBody = JSON.stringify({
    bloodlineRows: [],
    similarRows: [],
  });
  await harness.dependencies.cache.put(
    cacheRequestFor(descriptor),
    new Response(cachedBody, {
      headers: { "Content-Type": "application/json; charset=utf-8" },
    }),
  );
  const cacheHit = await handleRequest(new Request(heatmapUrl), harness.env, harness.dependencies);
  expect(cacheHit.headers.get("X-Catalog-Cache")).toBe("cache-api");
  expect(harness.fetchCalls).toStrictEqual([]);

  const kvHarness = createHeatmapHarness();
  await kvHarness.env.CATALOG_KV.put(kvKeyFor(descriptor), cachedBody, { expirationTtl: 120 });
  const kvHit = await handleRequest(new Request(heatmapUrl), kvHarness.env, kvHarness.dependencies);
  expect(kvHit.headers.get("X-Catalog-Cache")).toBe("kv");
  expect(kvHarness.fetchCalls).toStrictEqual([]);
});

it("keeps includeOwner=0 on the default similar SQL and cache key", async () => {
  const harness = createHeatmapHarness();
  const response = await handleRequest(
    new Request(`${heatmapUrl}&includeOwner=0`),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  expect(harness.fetchCalls.toSorted()).toStrictEqual(["bloodline", "similar"]);
  expect(harness.queryBodies.join("\n")).not.toMatch("'owner' AS kind");
  expect(harness.queryBodies.join("\n")).not.toMatch("banushimei");
  const descriptor = heatmapStatsDescriptor({
    date: "20260822",
    includeDistance: true,
    includeOwner: false,
    includeSurface: true,
    includeTurn: true,
    includeVenue: true,
    keibajoCode: "07",
    raceBango: "08",
    source: "jra",
    years: 10,
  });
  expect(cacheRequestFor(descriptor).url).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/win-rate-heatmap-stats?date=20260822&keibajoCode=07&raceBango=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=1&nameTrim=ideographic&emptyTurnBypass=1",
  );
});

it("keeps includeJockeyFrame=0 on the default similar SQL and cache key", async () => {
  const harness = createHeatmapHarness();
  const response = await handleRequest(
    new Request(`${heatmapUrl}&includeJockeyFrame=0`),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  expect(harness.fetchCalls.toSorted()).toStrictEqual(["bloodline", "similar"]);
  expect(harness.queryBodies.join("\n")).not.toMatch("'jockeyFrame' AS kind");
  expect(harness.queryBodies.join("\n")).not.toMatch("wakuban AS frame");
  const descriptor = heatmapStatsDescriptor({
    date: "20260822",
    includeDistance: true,
    includeJockeyFrame: false,
    includeOwner: false,
    includeSurface: true,
    includeTurn: true,
    includeVenue: true,
    keibajoCode: "07",
    raceBango: "08",
    source: "jra",
    years: 10,
  });
  expect(cacheRequestFor(descriptor).url).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/win-rate-heatmap-stats?date=20260822&keibajoCode=07&raceBango=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=1&nameTrim=ideographic&emptyTurnBypass=1",
  );
});

it("splits the heatmap cache key and similar SQL when includeJockeyFrame=1", async () => {
  const harness = createHeatmapHarness();
  const response = await handleRequest(
    new Request(`${heatmapUrl}&includeJockeyFrame=1`),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  expect(harness.fetchCalls.toSorted()).toStrictEqual(["bloodline", "similar"]);
  expect(harness.queryBodies.join("\n")).toMatch("'jockeyFrame' AS kind");
  expect(harness.queryBodies.join("\n")).toMatch("wakuban AS frame");
  expect(harness.queryBodies.join("\n")).toMatch("tn.kind = 'jockeyFrame'");
  const descriptor = heatmapStatsDescriptor({
    date: "20260822",
    includeDistance: true,
    includeJockeyFrame: true,
    includeSurface: true,
    includeTurn: true,
    includeVenue: true,
    keibajoCode: "07",
    raceBango: "08",
    source: "jra",
    years: 10,
  });
  expect(kvKeyFor(descriptor)).toBe(
    "catalog:v2:v2/win-rate-heatmap-stats?date=20260822&keibajoCode=07&raceBango=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=1&nameTrim=ideographic&emptyTurnBypass=1&includeJockeyFrame=1",
  );
  expect(harness.kvEntries.has(kvKeyFor(descriptor))).toBe(true);
});

it("splits the heatmap cache key and similar SQL when includeOwner=1", async () => {
  const harness = createHeatmapHarness();
  const response = await handleRequest(
    new Request(`${heatmapUrl}&includeOwner=1`),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  expect(harness.fetchCalls.toSorted()).toStrictEqual(["bloodline", "similar"]);
  expect(harness.queryBodies.join("\n")).toMatch("'owner' AS kind");
  expect(harness.queryBodies.join("\n")).toMatch("banushimei");
  const descriptor = heatmapStatsDescriptor({
    date: "20260822",
    includeDistance: true,
    includeOwner: true,
    includeSurface: true,
    includeTurn: true,
    includeVenue: true,
    keibajoCode: "07",
    raceBango: "08",
    source: "jra",
    years: 10,
  });
  expect(kvKeyFor(descriptor)).toBe(
    "catalog:v2:v2/win-rate-heatmap-stats?date=20260822&keibajoCode=07&raceBango=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=1&nameTrim=ideographic&emptyTurnBypass=1&includeOwner=1",
  );
  expect(harness.kvEntries.has(kvKeyFor(descriptor))).toBe(true);
});

it("rejects heatmap stats requests with missing or invalid filters", async () => {
  const harness = createHeatmapHarness();
  const missingSource = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=08&day=22&keibajoCode=07&raceNumber=08",
    ),
    harness.env,
    harness.dependencies,
  );
  const badSource = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=08&day=22&keibajoCode=07&raceNumber=08&source=all",
    ),
    harness.env,
    harness.dependencies,
  );
  const badFlag = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=08&day=22&keibajoCode=07&raceNumber=08&source=jra&includeVenue=2",
    ),
    harness.env,
    harness.dependencies,
  );
  const badYears = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=08&day=22&keibajoCode=07&raceNumber=08&source=jra&years=0",
    ),
    harness.env,
    harness.dependencies,
  );
  const badDate = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=02&day=31&keibajoCode=07&raceNumber=08&source=jra",
    ),
    harness.env,
    harness.dependencies,
  );
  const badOwner = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=08&day=22&keibajoCode=07&raceNumber=08&source=jra&includeOwner=2",
    ),
    harness.env,
    harness.dependencies,
  );
  const badJockeyFrame = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=08&day=22&keibajoCode=07&raceNumber=08&source=jra&includeJockeyFrame=2",
    ),
    harness.env,
    harness.dependencies,
  );
  expect(missingSource.status).toBe(400);
  expect(badSource.status).toBe(400);
  expect(badFlag.status).toBe(400);
  expect(badYears.status).toBe(400);
  expect(badDate.status).toBe(400);
  expect(badOwner.status).toBe(400);
  expect(badJockeyFrame.status).toBe(400);
});
