import { afterEach, expect, it, vi } from "vitest";

import { cacheRequestFor, kvKeyFor } from "./cache";
import worker from "./index";
import type { CacheStore, Env, Fetcher, KvStore, WorkerDependencies } from "./types";
import { handleRequest } from "./worker";

const featureRow = (): Record<string, unknown> => ({
  kaisai_nen: "2026",
  kaisai_tsukihi: "0715",
  keibajo_code: "5",
  ketto_toroku_bango: "2023100001",
  race_bango: "1",
  race_date: "20260715",
  source: "jra",
  umaban: "7",
});

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const createHarness = (rows: unknown[] = []) => {
  const cacheEntries = new Map<string, Response>();
  const kvEntries = new Map<string, string>();
  const cacheCalls: { deletes: string[]; matches: string[]; puts: string[] } = {
    deletes: [],
    matches: [],
    puts: [],
  };
  const kvCalls: { deletes: string[]; gets: string[]; puts: string[] } = {
    deletes: [],
    gets: [],
    puts: [],
  };
  const fetchCalls: Array<{ input: string; init?: RequestInit }> = [];
  const fetchState = {
    response: Response.json({ result: { rows }, success: true }),
  };
  const cache: CacheStore = {
    async delete(request) {
      cacheCalls.deletes.push(request.url);
      return cacheEntries.delete(request.url);
    },
    async match(request) {
      cacheCalls.matches.push(request.url);
      return cacheEntries.get(request.url)?.clone();
    },
    async put(request, response) {
      cacheCalls.puts.push(request.url);
      cacheEntries.set(request.url, response.clone());
    },
  };
  const kv: KvStore = {
    async delete(key) {
      kvCalls.deletes.push(key);
      kvEntries.delete(key);
    },
    async get(key) {
      kvCalls.gets.push(key);
      return kvEntries.get(key) ?? null;
    },
    async put(key, value) {
      kvCalls.puts.push(key);
      kvEntries.set(key, value);
    },
  };
  const fetchImpl: Fetcher = async (input, init) => {
    fetchCalls.push({ input: String(input), init });
    return fetchState.response.clone();
  };
  const env: Env = {
    ADMIN_TOKEN: "admin-secret",
    CACHE_TTL_SECONDS: "15",
    CATALOG_KV: kv,
    KV_TTL_SECONDS: "120",
    R2_SQL_ACCOUNT_ID: "account",
    R2_SQL_BUCKET_NAME: "pc-keiba-r2-catalog",
    R2_SQL_NAMESPACE: "pc_keiba",
    R2_SQL_TOKEN: "r2-secret",
  };
  const dependencies: WorkerDependencies = { cache, fetchImpl };
  return {
    cacheCalls,
    cacheEntries,
    dependencies,
    env,
    fetchCalls,
    fetchState,
    kvCalls,
    kvEntries,
  };
};

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

it("serves health and rejects unknown routes", async () => {
  const harness = createHarness();
  const health = await handleRequest(
    new Request("https://catalog.test/health"),
    harness.env,
    harness.dependencies,
  );
  const missing = await handleRequest(
    new Request("https://catalog.test/v1/sql", { method: "POST" }),
    harness.env,
    harness.dependencies,
  );
  expect(health.status).toBe(200);
  await expect(health.json()).resolves.toStrictEqual({ name: "pc-keiba-r2-catalog", ok: true });
  expect(missing.status).toBe(404);
  await expect(missing.json()).resolves.toStrictEqual({ error: "not_found" });
});

it("exports a Cloudflare Worker entrypoint", async () => {
  const harness = createHarness();
  vi.stubGlobal("caches", { default: harness.dependencies.cache });
  vi.stubGlobal("fetch", harness.dependencies.fetchImpl);
  const response = await worker.fetch(new Request("https://catalog.test/health"), harness.env);
  expect(response.status).toBe(200);
});

it("queries R2 SQL for race keys and writes both cache tiers", async () => {
  const harness = createHarness([
    { keibajo_code: "5", race_bango: "1", race_date: "20260715", source: "jra" },
    { keibajo_code: "83", race_bango: "9", race_date: "2026-07-15", source: "nar" },
  ]);
  const response = await handleRequest(
    new Request("https://catalog.test/v1/race-keys?date=20260715"),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("X-Catalog-Cache")).toBe("r2-sql");
  await expect(response.json()).resolves.toStrictEqual({
    rows: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0715",
        keibajo_code: "05",
        race_bango: "01",
        source: "jra",
      },
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0715",
        keibajo_code: "83",
        race_bango: "09",
        source: "nar",
      },
    ],
  });
  expect(harness.fetchCalls).toHaveLength(1);
  expect(harness.cacheCalls.puts).toHaveLength(1);
  expect(harness.kvCalls.puts).toHaveLength(1);
});

it("queries fixed race-feature SQL and returns DailyRaceEntryRow objects", async () => {
  const harness = createHarness([featureRow()]);
  const response = await handleRequest(
    new Request(
      "https://catalog.test/v1/race-features?date=20260715&source=jra&keibajoCode=5&raceBango=1",
    ),
    harness.env,
    harness.dependencies,
  );
  const payload = await response.json();
  expect(response.status).toBe(200);
  if (!isRecord(payload) || !Array.isArray(payload.rows)) {
    throw new Error("expected rows envelope");
  }
  expect(payload.rows).toHaveLength(1);
  expect(payload.rows[0]).toMatchObject({
    kaisai_nen: "2026",
    kaisai_tsukihi: "0715",
    keibajo_code: "05",
    race_bango: "01",
    race_date: "20260715",
    source: "jra",
    umaban: 7,
  });
  const requestBody = String(harness.fetchCalls[0]?.init?.body);
  expect(requestBody).toMatch("keibajo_code = '05'");
  expect(requestBody).toMatch("race_bango = '01'");
});

it("queries running-style features with no-store and bypasses Cache API and KV", async () => {
  const harness = createHarness([
    {
      bamei: "Catalog Horse",
      career_win_rate: 0.25,
      category: "jra",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0715",
      keibajo_code: "05",
      ketto_toroku_bango: "2023100001",
      race_bango: "01",
      race_date: "20260715",
      source: "jra",
      umaban: 7,
    },
  ]);
  const response = await handleRequest(
    new Request(
      "https://catalog.test/v1/running-style-features?date=20260715&source=jra&keibajoCode=5&raceBango=1",
    ),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("Cache-Control")).toBe("no-store");
  expect(response.headers.get("X-Catalog-Cache")).toBeNull();
  expect(harness.cacheCalls.matches).toHaveLength(0);
  expect(harness.cacheCalls.puts).toHaveLength(0);
  expect(harness.kvCalls.gets).toHaveLength(0);
  expect(harness.kvCalls.puts).toHaveLength(0);
  expect(harness.fetchCalls).toHaveLength(1);
  await expect(response.json()).resolves.toMatchObject({
    featureNames: ["career_win_rate"],
    generation: "raw-iceberg-v1",
    rows: [
      {
        raceKey: "jra:20260715:05:01",
        umaban: 7,
      },
    ],
  });
  expect(String(harness.fetchCalls[0]?.init?.body)).toMatch("limit 18");
});

it("requires all running-style race filters and a separated source", async () => {
  const harness = createHarness();
  const urls = [
    "?date=20260715&source=all&keibajoCode=05&raceBango=01",
    "?date=20260715&source=jra&raceBango=01",
    "?date=20260715&source=jra&keibajoCode=05",
    "?date=20260715&source=ban-ei&keibajoCode=05&raceBango=01",
  ];
  const responses = await Promise.all(
    urls.map((query) =>
      handleRequest(
        new Request(`https://catalog.test/v1/running-style-features${query}`),
        harness.env,
        harness.dependencies,
      ),
    ),
  );
  expect(responses.map((response) => response.status)).toStrictEqual([400, 400, 400, 400]);
  expect(harness.fetchCalls).toHaveLength(0);
});

it("reads Cache API before KV and R2 SQL", async () => {
  const harness = createHarness();
  const descriptor = { date: "20260715", kind: "race-keys" } satisfies Parameters<
    typeof cacheRequestFor
  >[0];
  harness.cacheEntries.set(
    cacheRequestFor(descriptor).url,
    Response.json({ rows: [{ race_key: "cached" }] }),
  );
  harness.kvEntries.set(kvKeyFor(descriptor), '{"rows":[{"race_key":"kv"}]}');
  const response = await handleRequest(
    new Request("https://catalog.test/v1/race-keys?date=20260715"),
    harness.env,
    harness.dependencies,
  );
  expect(response.headers.get("X-Catalog-Cache")).toBe("cache-api");
  await expect(response.json()).resolves.toStrictEqual({ rows: [{ race_key: "cached" }] });
  expect(harness.kvCalls.gets).toHaveLength(0);
  expect(harness.fetchCalls).toHaveLength(0);
});

it("reads KV before R2 SQL and repopulates Cache API", async () => {
  const harness = createHarness();
  const descriptor = { date: "20260715", kind: "race-keys" } satisfies Parameters<
    typeof kvKeyFor
  >[0];
  harness.kvEntries.set(kvKeyFor(descriptor), '{"rows":[{"race_key":"kv"}]}');
  const response = await handleRequest(
    new Request("https://catalog.test/v1/race-keys?date=20260715"),
    harness.env,
    harness.dependencies,
  );
  expect(response.headers.get("X-Catalog-Cache")).toBe("kv");
  await expect(response.json()).resolves.toStrictEqual({ rows: [{ race_key: "kv" }] });
  expect(harness.cacheCalls.puts).toHaveLength(1);
  expect(harness.fetchCalls).toHaveLength(0);
});

it("ignores malformed KV and tolerates cache write failures", async () => {
  const harness = createHarness([]);
  const descriptor = { date: "20260715", kind: "race-keys" } satisfies Parameters<
    typeof kvKeyFor
  >[0];
  harness.kvEntries.set(kvKeyFor(descriptor), "bad-json");
  harness.dependencies.cache.put = async () => {
    throw new Error("cache down");
  };
  harness.env.CATALOG_KV.put = async () => {
    throw new Error("kv down");
  };
  const response = await handleRequest(
    new Request("https://catalog.test/v1/race-keys?date=20260715"),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("X-Catalog-Cache")).toBe("r2-sql");
});

it("validates public endpoint query parameters", async () => {
  const harness = createHarness();
  const badDate = await handleRequest(
    new Request("https://catalog.test/v1/race-keys?date=2026-07-15"),
    harness.env,
    harness.dependencies,
  );
  const missingSource = await handleRequest(
    new Request("https://catalog.test/v1/race-features?date=20260715"),
    harness.env,
    harness.dependencies,
  );
  const badSource = await handleRequest(
    new Request("https://catalog.test/v1/race-features?date=20260715&source=banei"),
    harness.env,
    harness.dependencies,
  );
  const badCode = await handleRequest(
    new Request("https://catalog.test/v1/race-features?date=20260715&source=all&keibajoCode=123"),
    harness.env,
    harness.dependencies,
  );
  expect(badDate.status).toBe(400);
  expect(missingSource.status).toBe(400);
  expect(badSource.status).toBe(400);
  expect(badCode.status).toBe(400);
});

it("maps R2 SQL failures to a stable upstream error", async () => {
  const harness = createHarness();
  harness.fetchState.response = new Response("denied", { status: 403 });
  const consoleMock = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const response = await handleRequest(
    new Request("https://catalog.test/v1/race-keys?date=20260715"),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(502);
  await expect(response.json()).resolves.toStrictEqual({ error: "r2_sql_unavailable" });
  expect(consoleMock).toHaveBeenCalledOnce();
});

it("requires the internal token before purging", async () => {
  const harness = createHarness();
  const response = await handleRequest(
    new Request("https://catalog.test/admin/purge?date=20260715", { method: "POST" }),
    harness.env,
    harness.dependencies,
  );
  const missingSecret = await handleRequest(
    new Request("https://catalog.test/admin/purge?date=20260715", {
      headers: { Authorization: "Bearer admin-secret" },
      method: "POST",
    }),
    { ...harness.env, ADMIN_TOKEN: undefined },
    harness.dependencies,
  );
  expect(response.status).toBe(401);
  expect(missingSecret.status).toBe(401);
  expect(harness.cacheCalls.deletes).toHaveLength(0);
});

it("purges all default date keys with an authorized request", async () => {
  const harness = createHarness();
  const response = await handleRequest(
    new Request("https://catalog.test/admin/purge?date=20260715", {
      headers: { Authorization: "Bearer admin-secret" },
      method: "POST",
    }),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  await expect(response.json()).resolves.toStrictEqual({ ok: true, purged: 5 });
  expect(harness.cacheCalls.deletes).toHaveLength(5);
  expect(harness.kvCalls.deletes).toHaveLength(5);
});

it("supports authenticated DELETE purge without touching R2 SQL or raw data", async () => {
  const harness = createHarness();
  const response = await handleRequest(
    new Request("https://catalog.test/admin/purge?date=20260715&source=jra", {
      headers: { Authorization: "Bearer admin-secret" },
      method: "DELETE",
    }),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  await expect(response.json()).resolves.toStrictEqual({ ok: true, purged: 1 });
  expect(harness.cacheCalls.deletes).toHaveLength(1);
  expect(harness.kvCalls.deletes).toHaveLength(1);
  expect(harness.fetchCalls).toHaveLength(0);
  expect(harness.cacheCalls.deletes[0]).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/race-features?date=20260715&source=jra",
  );
});

it("purges one source and race-specific cache key", async () => {
  const harness = createHarness();
  const response = await handleRequest(
    new Request(
      "https://catalog.test/admin/purge?date=20260715&source=ban-ei&keibajoCode=83&raceBango=9",
      {
        headers: { Authorization: "Bearer admin-secret" },
        method: "POST",
      },
    ),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  await expect(response.json()).resolves.toStrictEqual({ ok: true, purged: 1 });
  expect(harness.cacheCalls.deletes[0]).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/race-features?date=20260715&source=ban-ei&keibajoCode=83&raceBango=09",
  );
});

it("validates authorized purge parameters", async () => {
  const harness = createHarness();
  const response = await handleRequest(
    new Request("https://catalog.test/admin/purge?date=bad&source=nope", {
      headers: { Authorization: "Bearer admin-secret" },
      method: "POST",
    }),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(400);
});
