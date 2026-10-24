import arcProfiles from "./fixtures/arc-id-bloodline/profiles.json";
import arcFixture from "./fixtures/arc-id-bloodline/qualified.json";
import { expect, it, vi } from "vitest";

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

// A cached day that knows this race has no named pedigree keeps the per-race
// count path these tests exercise; the day-level skip has its own tests.
const DAY_KNOWN_KEY = "heatmap-day-known-bloodline:v2:jra:20260822";

const createHeatmapHarness = () => {
  const cacheEntries = new Map<string, Response>();
  const kvEntries = new Map<string, string>([[DAY_KNOWN_KEY, '{"07:08":[]}']]);
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
      if (new TextEncoder().encode(key).byteLength > 512)
        throw new Error("KV key exceeds 512 bytes");
      return kvEntries.get(key) ?? null;
    },
    async put(key, value) {
      if (new TextEncoder().encode(key).byteLength > 512)
        throw new Error("KV key exceeds 512 bytes");
      kvEntries.set(key, value);
    },
  };
  const knownCountQueries: string[] = [];
  const fetchImpl: Fetcher = async (_input, init) => {
    const rawBody = init?.body;
    const text = typeof rawBody === "string" ? rawBody : "";
    const parsed: unknown = JSON.parse(text);
    const query = isRecord(parsed) && typeof parsed.query === "string" ? parsed.query : "";
    if (query.includes("SELECT m.race_source"))
      return Response.json({ success: true, result: { rows: [] } });
    // The day snapshot query; an empty day keeps the per-race count path.
    if (query.includes("se.keibajo_code,\n  se.race_bango,"))
      return Response.json({ success: true, result: { rows: [] } });
    if (query.includes("AS known")) {
      knownCountQueries.push(query);
      return Response.json({ result: { rows: [{ known: 1 }] }, success: true });
    }
    queryBodies.push(query);
    const literalBloodline: boolean = query.includes("END AS b0");
    fetchCalls.push(
      query.includes("'sire' AS category") || literalBloodline ? "bloodline" : "similar",
    );
    if (literalBloodline) {
      return Response.json({
        result: { rows: [{ b0: "Sire's Son", places: 2, shows: 2, starts: 3, wins: 1 }] },
        success: true,
      });
    }
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
  return { cacheEntries, dependencies, env, fetchCalls, knownCountQueries, kvEntries, queryBodies };
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

it("bounds heatmap R2 SQL requests with a 60 second abort signal", async () => {
  const harness = createHeatmapHarness();
  const timeoutSpy = vi.spyOn(AbortSignal, "timeout");
  const signals: Array<AbortSignal | null | undefined> = [];
  const dependencies: WorkerDependencies = {
    ...harness.dependencies,
    fetchImpl: async (input, init) => {
      signals.push(init?.signal);
      return harness.dependencies.fetchImpl(input, init);
    },
  };
  const response = await handleRequest(new Request(heatmapUrl), harness.env, dependencies);
  expect(response.status).toBe(200);
  // executeR2Sql bounds each attempt at 20s; the heatmap wrapper adds its 60s bound.
  expect(timeoutSpy.mock.calls.filter((call) => call[0] === 60_000).length).toBe(3);
  expect(timeoutSpy.mock.calls.filter((call) => call[0] === 20_000).length).toBe(3);
  expect(signals.map((signal) => signal instanceof AbortSignal)).toStrictEqual([true, true, true]);
  timeoutSpy.mockRestore();
});

it("re-executes a heatmap query after an aborted R2 SQL request", async () => {
  const harness = createHeatmapHarness();
  const attempts: string[] = [];
  const failingDependencies: WorkerDependencies = {
    ...harness.dependencies,
    fetchImpl: async () => {
      attempts.push("aborted");
      throw new DOMException("The operation was aborted due to timeout", "TimeoutError");
    },
  };
  const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const failed = await handleRequest(new Request(heatmapUrl), harness.env, failingDependencies);
  expect(failed.ok).toBe(false);
  const failureLogs = consoleError.mock.calls
    .filter((call) => call[0] === "[pc-keiba-r2-catalog] heatmap R2 SQL query failed")
    .map((call) => {
      const parsed: unknown = JSON.parse(String(call[1]));
      return isRecord(parsed) ? [parsed.query, parsed.detail, parsed.raceBango] : [];
    });
  expect(failureLogs.toSorted()).toStrictEqual([
    ["bloodline", "The operation was aborted due to timeout", "08"],
    ["similar", "The operation was aborted due to timeout", "08"],
  ]);
  consoleError.mockRestore();
  const retried = await handleRequest(new Request(heatmapUrl), harness.env, harness.dependencies);
  expect(retried.status).toBe(200);
  expect(attempts).toStrictEqual(["aborted", "aborted", "aborted"]);
  expect(harness.fetchCalls.toSorted()).toStrictEqual(["bloodline", "similar"]);
});

it("logs non-Error heatmap query failures as strings", async () => {
  const harness = createHeatmapHarness();
  const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const dependencies: WorkerDependencies = {
    ...harness.dependencies,
    fetchImpl: () => Promise.reject(new Error("raw").message),
  };
  const failed = await handleRequest(new Request(heatmapUrl), harness.env, dependencies);
  expect(failed.ok).toBe(false);
  const details = consoleError.mock.calls
    .filter((call) => call[0] === "[pc-keiba-r2-catalog] heatmap R2 SQL query failed")
    .map((call) => {
      const parsed: unknown = JSON.parse(String(call[1]));
      return isRecord(parsed) ? parsed.detail : null;
    });
  expect(details).toStrictEqual(["raw", "raw"]);
  consoleError.mockRestore();
});

it("returns similar rows and flags bloodline when only the bloodline query fails", async () => {
  const harness = createHeatmapHarness();
  const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const dependencies: WorkerDependencies = {
    ...harness.dependencies,
    fetchImpl: async (input, init) => {
      const rawBody = init?.body;
      const text = typeof rawBody === "string" ? rawBody : "";
      if (text.includes("'sire' AS category")) {
        throw new DOMException("The operation was aborted due to timeout", "TimeoutError");
      }
      return harness.dependencies.fetchImpl(input, init);
    },
  };
  const response = await handleRequest(new Request(heatmapUrl), harness.env, dependencies);
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(isRecord(body) ? [body.bloodlineUnavailable, body.bloodlineRows] : []).toStrictEqual([
    true,
    [],
  ]);
  expect(isRecord(body) && Array.isArray(body.similarRows) ? body.similarRows.length : 0).toBe(1);
  // The flagged heatmap is kept only past the next sweep, alongside the
  // similar query's answer.
  expect(harness.cacheEntries.size).toBe(1);
  const cached = [...harness.cacheEntries.values()][0];
  expect(cached?.headers.get("Cache-Control")).toBe("public, max-age=1200");
  expect(
    [...harness.kvEntries.keys()].filter((key) => !key.startsWith("r2sql-result:")).length,
  ).toBe(2);
  expect(
    [...harness.kvEntries.keys()].filter((key) => key.startsWith("r2sql-result:")).length,
  ).toBe(1);
  consoleError.mockRestore();
});

it("returns empty bloodline rows without waiting when no runner has a known pedigree", async () => {
  const harness = createHeatmapHarness();
  const bloodlineSignals: Array<AbortSignal | null | undefined> = [];
  const dependencies: WorkerDependencies = {
    ...harness.dependencies,
    fetchImpl: async (input, init) => {
      const rawBody = init?.body;
      const text = typeof rawBody === "string" ? rawBody : "";
      if (text.includes("AS known")) {
        return Response.json({ result: { rows: [{ known: 0 }] }, success: true });
      }
      if (text.includes("matched_history") && text.includes("'sire' AS category")) {
        bloodlineSignals.push(init?.signal);
        return new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => {
            reject(new DOMException("The operation was aborted", "AbortError"));
          });
        });
      }
      return harness.dependencies.fetchImpl(input, init);
    },
  };
  const response = await handleRequest(new Request(heatmapUrl), harness.env, dependencies);
  expect(response.status).toBe(200);
  const body: unknown = await response.json();
  expect(isRecord(body) ? [body.bloodlineUnavailable, body.bloodlineRows] : []).toStrictEqual([
    undefined,
    [],
  ]);
  expect(bloodlineSignals.map((signal) => signal?.aborted)).toStrictEqual([true]);
  expect(
    [...harness.kvEntries.keys()].filter((key) => !key.startsWith("r2sql-result:")).length,
  ).toBe(2);
});

it("waits for the bloodline query when the known pedigree count fails", async () => {
  const harness = createHeatmapHarness();
  const dependencies: WorkerDependencies = {
    ...harness.dependencies,
    fetchImpl: async (input, init) => {
      const rawBody = init?.body;
      const text = typeof rawBody === "string" ? rawBody : "";
      if (text.includes("AS known")) {
        return Response.json(
          { errors: [{ code: 1, message: "boom" }], success: false },
          {
            status: 500,
          },
        );
      }
      return harness.dependencies.fetchImpl(input, init);
    },
  };
  const response = await handleRequest(new Request(heatmapUrl), harness.env, dependencies);
  const body: unknown = await response.json();
  expect(isRecord(body) && Array.isArray(body.bloodlineRows) ? body.bloodlineRows.length : 0).toBe(
    1,
  );
});

it("waits for the bloodline query when the known pedigree count is malformed", async () => {
  const harness = createHeatmapHarness();
  const dependencies: WorkerDependencies = {
    ...harness.dependencies,
    fetchImpl: async (input, init) => {
      const rawBody = init?.body;
      const text = typeof rawBody === "string" ? rawBody : "";
      if (text.includes("AS known")) {
        return Response.json({ result: { rows: [] }, success: true });
      }
      return harness.dependencies.fetchImpl(input, init);
    },
  };
  const response = await handleRequest(new Request(heatmapUrl), harness.env, dependencies);
  const body: unknown = await response.json();
  expect(isRecord(body) && Array.isArray(body.bloodlineRows) ? body.bloodlineRows.length : 0).toBe(
    1,
  );
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
    bloodlineRows: [
      {
        category: "sire",
        name: "Test sire",
        starts: 10,
        umaban: 1,
        wins: 1,
        places: 2,
        shows: 3,
        details: [],
      },
    ],
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
    "https://pc-keiba-r2-catalog-cache.internal/v3/win-rate-heatmap-stats?date=20260822&keibajoCode=07&raceBango=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=1&nameTrim=ideographic&emptyTurnBypass=1&ungradedOp=1&emptyGradeMatch=2",
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
    "https://pc-keiba-r2-catalog-cache.internal/v3/win-rate-heatmap-stats?date=20260822&keibajoCode=07&raceBango=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=1&nameTrim=ideographic&emptyTurnBypass=1&ungradedOp=1&emptyGradeMatch=2",
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
    "catalog:v2:v3/win-rate-heatmap-stats?date=20260822&keibajoCode=07&raceBango=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=1&nameTrim=ideographic&emptyTurnBypass=1&includeJockeyFrame=1&ungradedOp=1&emptyGradeMatch=2",
  );
  expect(harness.kvEntries.has(kvKeyFor(descriptor))).toBe(true);
});

it("splits the heatmap cache key and similar SQL when includeClass=1", async () => {
  const harness = createHeatmapHarness();
  const response = await handleRequest(
    new Request(`${heatmapUrl}&includeClass=1&includeAge=1`),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  expect(harness.queryBodies.join("\n")).toMatch(
    "btrim(coalesce(ra.kyoso_joken_code, '')) = btrim(coalesce(cr.kyoso_joken_code, ''))",
  );
  expect(harness.queryBodies.join("\n")).toMatch(
    "btrim(coalesce(ra.kyoso_shubetsu_code, '')) = btrim(coalesce(cr.kyoso_shubetsu_code, ''))",
  );
  const descriptor = heatmapStatsDescriptor({
    date: "20260822",
    includeAge: true,
    includeClass: true,
    includeDistance: true,
    includeSurface: true,
    includeTurn: true,
    includeVenue: true,
    keibajoCode: "07",
    raceBango: "08",
    source: "jra",
    years: 10,
  });
  expect(kvKeyFor(descriptor)).toBe(
    "catalog:v2:v3/win-rate-heatmap-stats?date=20260822&keibajoCode=07&raceBango=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=1&nameTrim=ideographic&emptyTurnBypass=1&ungradedOp=1&emptyGradeMatch=2&includeAge=1&includeClass=1",
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
    "catalog:v2:v3/win-rate-heatmap-stats?date=20260822&keibajoCode=07&raceBango=08&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=1&nameTrim=ideographic&emptyTurnBypass=1&includeOwner=1&ungradedOp=1&emptyGradeMatch=2",
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
  const badClass = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=08&day=22&keibajoCode=07&raceNumber=08&source=jra&includeClass=2",
    ),
    harness.env,
    harness.dependencies,
  );
  expect(badClass.status).toBe(400);
});

it("invalidates an overseas aggregate when only historical mapping content changes and fails closed", async () => {
  const harness = createHeatmapHarness();
  const published = { gate: 16, failed: false };
  const dependencies: WorkerDependencies = {
    ...harness.dependencies,
    fetchImpl: async (input, init) => {
      if (String(init?.body).includes("SELECT race_source, source, kaisai_nen")) {
        if (published.failed) throw new Error("revision offline");
        return Response.json({
          success: true,
          result: {
            rows: [
              {
                race_source: "jra",
                source: "netkeiba",
                kaisai_nen: "2026",
                kaisai_tsukihi: "0816",
                keibajo_code: "A8",
                race_bango: "04",
                umaban: "01",
                gate_number: published.gate,
              },
            ],
          },
        });
      }
      return harness.dependencies.fetchImpl(input, init);
    },
  };
  const request = new Request(heatmapUrl.replace("keibajoCode=07", "keibajoCode=A8"));
  expect((await handleRequest(request, harness.env, dependencies)).status).toBe(200);
  expect((await handleRequest(request, harness.env, dependencies)).status).toBe(200);
  expect(harness.fetchCalls).toHaveLength(2);
  published.gate = 1;
  expect((await handleRequest(request, harness.env, dependencies)).status).toBe(200);
  expect(harness.fetchCalls).toHaveLength(4);
  published.failed = true;
  expect((await handleRequest(request, harness.env, dependencies)).status).toBe(502);
  expect(harness.fetchCalls).toHaveLength(4);
});

it.each(["https://public.example", "https://pc-keiba-r2-catalog.internal"])(
  "rejects removed revision HTTP route without SQL for %s",
  async (origin) => {
    const harness = createHeatmapHarness();
    const response = await handleRequest(
      new Request(
        `${origin}/v1/gate-mapping-revision?year=2026&month=10&day=04&keibajoCode=A8&raceNumber=05`,
        { headers: { "x-internal": "true" } },
      ),
      harness.env,
      harness.dependencies,
    );
    expect(response.status).toBe(404);
    expect(harness.queryBodies).toStrictEqual([]);
  },
);

it("wires explicit foreign owner stats and separate venue rows", async () => {
  const harness = createHeatmapHarness();
  const fetchImpl: Fetcher = async (_input, init) => {
    const body = String(init?.body);
    if (body.includes("SELECT m.race_source"))
      return Response.json({
        success: true,
        result: {
          rows: [
            {
              race_source: "jra",
              source: "netkeiba",
              kaisai_nen: "2026",
              kaisai_tsukihi: "1004",
              keibajo_code: "A8",
              race_bango: "05",
              umaban: "01",
              source_horse_id: "000a029d42",
              source_jockey_id: "05504",
              source_trainer_id: "05701",
              source_owner_id: "a002e7",
              sire_source_id: "000a0115e2",
              sire_name: "Sea The Stars",
              sire_sire_source_id: "",
              sire_sire_name: "Cape Cross",
              dam_source_id: "",
              dam_name: "",
              dam_sire_source_id: "",
              dam_sire_name: "Selkirk",
            },
          ],
        },
      });
    const rows = body.includes("oversea_person_race_history")
      ? [
          {
            umaban: "01",
            source_person_id: "a002e7",
            kind: "owner",
            starts: 3,
            wins: 0,
            places: 1,
            shows: 2,
            conflicts: 0,
          },
          {
            umaban: "01",
            source_person_id: "a002e7",
            kind: "ownerVenue",
            starts: 2,
            wins: 0,
            places: 1,
            shows: 1,
            conflicts: 0,
          },
        ]
      : [];
    return Response.json({ success: true, result: { rows } });
  };
  const url =
    "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=10&day=04&keibajoCode=A8&raceNumber=05&source=jra&includeOwner=1&includeForeignSourceStats=1&includeVenue=0&includeDistance=0&includeSurface=0&includeTurn=0";
  const response = await handleRequest(new Request(url), harness.env, {
    ...harness.dependencies,
    fetchImpl,
  });
  expect(response.status).toBe(200);
  expect(await response.json()).toMatchObject({
    similarRows: [
      {
        kind: "owner",
        sourcePersonId: "a002e7",
        statsSource: "netkeiba",
        statsScope: "stored-results-10y",
        wins: 0,
      },
    ],
    foreignOwnerVenueRows: [{ kind: "ownerVenue", statsScope: "stored-results-venue-30y" }],
    mappingRevision: expect.stringMatching(/^gate-v1-[a-f0-9]{64}$/u),
  });
  expect(
    (
      await handleRequest(
        new Request(url.replace("includeForeignSourceStats=1", "includeForeignSourceStats=bad")),
        harness.env,
        harness.dependencies,
      )
    ).status,
  ).toBe(400);
});

it("keeps actual Arc keys within 512 bytes across miss and hit without leaking ledger", async () => {
  const harness = createHeatmapHarness();
  const queries: string[] = [];
  const fetchImpl: Fetcher = async (_url, init) => {
    const body = String(init?.body);
    queries.push(body);
    if (body.includes("LIMIT 5001") && body.includes("m.kaisai_nen = '2026'"))
      return Response.json({
        success: true,
        result: {
          rows: arcFixture.rows.map((target) => ({
            source: "netkeiba",
            race_source: "jra",
            kaisai_nen: "2026",
            kaisai_tsukihi: "1004",
            keibajo_code: "A8",
            race_bango: "05",
            umaban: target.umaban,
            source_horse_id: target.people.horse,
            source_jockey_id: target.people.jockey,
            source_trainer_id: target.people.trainer,
            source_owner_id: target.people.owner,
            gate_number: 1,
          })),
        },
      });
    if (body.includes("FROM pc_keiba.oversea_horse_pedigree WHERE"))
      return Response.json({ success: true, result: { rows: arcProfiles } });
    const rows = body.includes("'sire' AS category") ? [bloodlineSqlRow] : [];
    return Response.json({ success: true, result: { rows } });
  };
  const url =
    heatmapUrl
      .replace(
        "month=08&day=22&keibajoCode=07&raceNumber=08",
        "month=10&day=04&keibajoCode=A8&raceNumber=05",
      )
      .replace("includeDistance=1", "includeDistance=0")
      .replace("includeTurn=1", "includeTurn=0") +
    "&includeOwner=1&includeJockeyFrame=1&includeRaceTitle=1&includeForeignSourceStats=1";
  const dependencies = { ...harness.dependencies, fetchImpl };
  const first = await handleRequest(new Request(url), harness.env, dependencies);
  const body = await first.text();
  expect(first.status).toBe(200);
  const payload: import("./types").WinRateHeatmapStatsPayload = JSON.parse(body);
  expect(payload.arcQualifiedStats?.rows).toHaveLength(160);
  expect(payload.arcQualifiedStats?.rows[0]).toMatchObject({
    kind: "jockey",
    source: "netkeiba",
    people: { jockey: "05504" },
    since: "2016-10-04",
    before: "2026-10-04",
    scope: "arc-series",
    status: "unobserved",
  });
  expect(payload.arcQualifiedStats).toMatchObject({
    coverage: { complete: false, rawRows: 0, invalidHorseRows: 0 },
  });
  expect(body).not.toMatch(/dependencyTuple|"evidence":/u);
  expect(queries.filter((q) => q.includes("LIMIT 5001"))).toHaveLength(4);
  queries.length = 0;
  const second = await handleRequest(new Request(url), harness.env, dependencies);
  expect(await second.text()).toBe(body);
  expect(queries.filter((q) => q.includes("LIMIT 5001"))).toHaveLength(4);
  expect(queries.some((q) => q.includes("target_names"))).toBe(false);
  expect(JSON.parse(body).knownBloodlineCount).toBe(112);
});

it("qualified Arc failure does not serve native or old cached stats", async () => {
  const harness = createHeatmapHarness();
  const fetchImpl: Fetcher = async () => {
    throw new Error("qualified unavailable");
  };
  const url = heatmapUrl.replace(
    "month=08&day=22&keibajoCode=07&raceNumber=08",
    "month=10&day=04&keibajoCode=A8&raceNumber=05",
  );
  const response = await handleRequest(new Request(url), harness.env, {
    ...harness.dependencies,
    fetchImpl,
  });
  expect(response.status).toBe(502);
  expect(harness.cacheEntries.size).toBe(0);
});

it("holds the account-wide R2 SQL gate for heatmap queries but not for light reads", async () => {
  const harness = createHeatmapHarness();
  const acquire = vi.fn<() => Promise<string | null>>(async () => "slot");
  const release = vi.fn<(token: string) => Promise<void>>(async () => undefined);
  const env: Env = { ...harness.env, R2_SQL_GATE: { getByName: () => ({ acquire, release }) } };
  const response = await handleRequest(new Request(heatmapUrl), env, harness.dependencies);
  expect(response.status).toBe(200);
  expect(acquire).toHaveBeenCalledTimes(harness.fetchCalls.length + 1);
  expect(release).toHaveBeenCalledTimes(acquire.mock.calls.length);
  acquire.mockClear();
  await handleRequest(new Request("https://catalog.test/health"), env, harness.dependencies);
  expect(acquire).not.toHaveBeenCalled();
});

const dayRunner = (umaban: number, sire: string): Record<string, unknown> => ({
  damDamSire: "不明",
  damSire: "不明",
  damSireSire: "不明",
  ketto_toroku_bango: `20200000${String(umaban).padStart(2, "0")}`,
  sire,
  sireDamSire: "不明",
  sireSire: "不明",
  sireSireSire: "不明",
  umaban,
});

it("queries the cached day's named runners as IN-lists and skips the per-race pedigree count", async () => {
  const harness = createHeatmapHarness();
  harness.kvEntries.set(DAY_KNOWN_KEY, JSON.stringify({ "07:08": [dayRunner(7, "Sire's Son")] }));
  const response = await handleRequest(new Request(heatmapUrl), harness.env, harness.dependencies);
  expect(response.status).toBe(200);
  expect(harness.knownCountQueries).toStrictEqual([]);
  expect(harness.fetchCalls.toSorted()).toStrictEqual(["bloodline", "similar"]);
  const bloodline = harness.queryBodies.find((query) => query.includes("END AS b0"));
  expect(bloodline).toMatch("IN ('Sire''s Son')");
  expect(bloodline).toMatch("se.ketto_toroku_bango NOT IN ('2020000007')");
  expect(bloodline).not.toMatch("current_entries");
  const body = (await response.json()) as { bloodlineRows: Record<string, unknown>[] };
  expect(body.bloodlineRows.map((row) => [row.umaban, row.category, row.starts])).toStrictEqual([
    [7, "sire", 3],
  ]);
  expect(bloodline).not.toMatch(
    "WHERE se.kaisai_nen = '2026'\n    AND se.kaisai_tsukihi = '0822'\n    AND se.keibajo_code = '07'",
  );
});

it("computes the day's runners once on a miss and reuses them for later races", async () => {
  const harness = createHeatmapHarness();
  harness.kvEntries.delete(DAY_KNOWN_KEY);
  const dayQueries: string[] = [];
  const dependencies: WorkerDependencies = {
    ...harness.dependencies,
    fetchImpl: async (input, init) => {
      const text = typeof init?.body === "string" ? init.body : "";
      if (text.includes("se.keibajo_code,\\n  se.race_bango,")) {
        dayQueries.push(text);
        return Response.json({
          result: {
            rows: [
              { ...dayRunner(7, "Sire"), keibajo_code: "07", race_bango: "08" },
              { ...dayRunner(8, "Other"), keibajo_code: "07", race_bango: "08" },
              { ...dayRunner(1, "不明"), keibajo_code: "07", race_bango: "01" },
            ],
          },
          success: true,
        });
      }
      return harness.dependencies.fetchImpl(input, init);
    },
  };
  const first = await handleRequest(new Request(heatmapUrl), harness.env, dependencies);
  expect(first.status).toBe(200);
  expect(dayQueries.length).toBe(1);
  expect(harness.knownCountQueries).toStrictEqual([]);
  expect(JSON.parse(harness.kvEntries.get(DAY_KNOWN_KEY) ?? "{}")).toStrictEqual({
    "07:01": [dayRunner(1, "不明")],
    "07:08": [dayRunner(7, "Sire"), dayRunner(8, "Other")],
  });
  const other = await handleRequest(
    new Request(heatmapUrl.replace("raceNumber=08", "raceNumber=01")),
    harness.env,
    dependencies,
  );
  expect(other.status).toBe(200);
  expect(dayQueries.length).toBe(1);
  expect(harness.knownCountQueries.length).toBe(1);
});

it.each(["not json", "[1]", '{"07:01":3}', '{"07:08":3}', '{"07:08":[{"umaban":1}]}'])(
  "falls back to the per-race count when the cached day cannot answer for this race: %s",
  async (body) => {
    const harness = createHeatmapHarness();
    harness.kvEntries.set(DAY_KNOWN_KEY, body);
    const dependencies: WorkerDependencies = {
      ...harness.dependencies,
      fetchImpl: async (input, init) => {
        const text = typeof init?.body === "string" ? init.body : "";
        if (text.includes("se.keibajo_code,\\n  se.race_bango,")) {
          return Response.json({ result: { rows: [] }, success: true });
        }
        return harness.dependencies.fetchImpl(input, init);
      },
    };
    const response = await handleRequest(new Request(heatmapUrl), harness.env, dependencies);
    expect(response.status).toBe(200);
    expect(harness.knownCountQueries.length).toBe(1);
  },
);

it("falls back to the per-race count when the day computation fails", async () => {
  const harness = createHeatmapHarness();
  harness.kvEntries.delete(DAY_KNOWN_KEY);
  const dependencies: WorkerDependencies = {
    ...harness.dependencies,
    fetchImpl: async (input, init) => {
      const text = typeof init?.body === "string" ? init.body : "";
      if (text.includes("se.keibajo_code,\\n  se.race_bango,")) {
        throw new Error("day query stalled");
      }
      return harness.dependencies.fetchImpl(input, init);
    },
  };
  const response = await handleRequest(new Request(heatmapUrl), harness.env, dependencies);
  expect(response.status).toBe(200);
  expect(harness.knownCountQueries.length).toBe(1);
  expect(harness.kvEntries.has(DAY_KNOWN_KEY)).toBe(false);
});
