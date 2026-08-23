import { afterEach, expect, it, vi } from "vitest";

import { cacheRequestFor, heatmapStatsDescriptor, kvKeyFor } from "./cache";
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
    FINISH_POSITION_ATTESTATION_TOKEN: "attestation-secret",
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

it("requires the attestation Bearer token before querying fresh race entries", async () => {
  const harness = createHarness([{ ketto_toroku_bango: "2023100001", umaban: 1 }]);
  const missing = await handleRequest(
    new Request(
      "https://catalog.test/v1/internal/fresh-race-entries?date=20260823&source=jra&keibajoCode=07&raceBango=09",
    ),
    harness.env,
    harness.dependencies,
  );
  const wrong = await handleRequest(
    new Request(
      "https://catalog.test/v1/internal/fresh-race-entries?date=20260823&source=jra&keibajoCode=07&raceBango=09",
      { headers: { Authorization: "Bearer wrong" } },
    ),
    harness.env,
    harness.dependencies,
  );
  expect(missing.status).toBe(401);
  await expect(missing.json()).resolves.toStrictEqual({ error: "unauthorized" });
  expect(wrong.status).toBe(401);
  await expect(wrong.json()).resolves.toStrictEqual({ error: "unauthorized" });
  expect(harness.fetchCalls).toHaveLength(0);
  expect(harness.cacheCalls.matches).toHaveLength(0);
  expect(harness.kvCalls.gets).toHaveLength(0);
});

it("queries fresh race entries directly with an echoed exact scope and no cache access", async () => {
  const harness = createHarness([
    { ketto_toroku_bango: "2023100002", umaban: "2" },
    { ketto_toroku_bango: "2023100001", umaban: 1 },
  ]);
  const response = await handleRequest(
    new Request(
      "https://catalog.test/v1/internal/fresh-race-entries?date=20260823&source=jra&keibajoCode=7&raceBango=9",
      { headers: { Authorization: "Bearer attestation-secret" } },
    ),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("Cache-Control")).toBe("no-store");
  expect(response.headers.get("X-Catalog-Cache")).toBeNull();
  await expect(response.json()).resolves.toStrictEqual({
    date: "20260823",
    entries: [
      { kettoTorokuBango: "2023100001", umaban: 1 },
      { kettoTorokuBango: "2023100002", umaban: 2 },
    ],
    keibajoCode: "07",
    raceBango: "09",
    source: "jra",
  });
  expect(harness.fetchCalls).toHaveLength(1);
  expect(String(harness.fetchCalls[0]?.init?.body)).toMatch("FROM pc_keiba.jvd_se");
  expect(String(harness.fetchCalls[0]?.init?.body)).toMatch("keibajo_code = '07'");
  expect(String(harness.fetchCalls[0]?.init?.body)).toMatch("race_bango = '09'");
  expect(harness.cacheCalls.matches).toHaveLength(0);
  expect(harness.cacheCalls.puts).toHaveLength(0);
  expect(harness.kvCalls.gets).toHaveLength(0);
  expect(harness.kvCalls.puts).toHaveLength(0);
});

it("does not coalesce concurrent fresh entrant requests", async () => {
  const harness = createHarness([{ ketto_toroku_bango: "2023100001", umaban: 1 }]);
  const request = (): Request =>
    new Request(
      "https://catalog.test/v1/internal/fresh-race-entries?date=20260823&source=jra&keibajoCode=07&raceBango=09",
      { headers: { Authorization: "Bearer attestation-secret" } },
    );
  const [first, second] = await Promise.all([
    handleRequest(request(), harness.env, harness.dependencies),
    handleRequest(request(), harness.env, harness.dependencies),
  ]);
  expect(first.status).toBe(200);
  expect(second.status).toBe(200);
  expect(harness.fetchCalls).toHaveLength(2);
  expect(harness.cacheCalls.matches).toHaveLength(0);
  expect(harness.kvCalls.gets).toHaveLength(0);
});

it("requires every fresh entrant scope and rejects an invalid source", async () => {
  const harness = createHarness([{ ketto_toroku_bango: "2023100001", umaban: 1 }]);
  const missingRace = await handleRequest(
    new Request(
      "https://catalog.test/v1/internal/fresh-race-entries?date=20260823&source=jra&keibajoCode=07",
      { headers: { Authorization: "Bearer attestation-secret" } },
    ),
    harness.env,
    harness.dependencies,
  );
  const invalidSource = await handleRequest(
    new Request(
      "https://catalog.test/v1/internal/fresh-race-entries?date=20260823&source=all&keibajoCode=07&raceBango=09",
      { headers: { Authorization: "Bearer attestation-secret" } },
    ),
    harness.env,
    harness.dependencies,
  );
  expect(missingRace.status).toBe(400);
  await expect(missingRace.json()).resolves.toStrictEqual({ error: "raceBango is required" });
  expect(invalidSource.status).toBe(400);
  await expect(invalidSource.json()).resolves.toStrictEqual({
    error: "source must be jra, nar, or ban-ei",
  });
  expect(harness.fetchCalls).toHaveLength(0);
});

it("fails closed when fresh entrant rows are empty or duplicated", async () => {
  const emptyHarness = createHarness();
  const duplicateHarness = createHarness([
    { ketto_toroku_bango: "2023100001", umaban: 1 },
    { ketto_toroku_bango: "2023100001", umaban: 1 },
  ]);
  const empty = await handleRequest(
    new Request(
      "https://catalog.test/v1/internal/fresh-race-entries?date=20260823&source=jra&keibajoCode=07&raceBango=09",
      { headers: { Authorization: "Bearer attestation-secret" } },
    ),
    emptyHarness.env,
    emptyHarness.dependencies,
  );
  const duplicate = await handleRequest(
    new Request(
      "https://catalog.test/v1/internal/fresh-race-entries?date=20260823&source=jra&keibajoCode=07&raceBango=09",
      { headers: { Authorization: "Bearer attestation-secret" } },
    ),
    duplicateHarness.env,
    duplicateHarness.dependencies,
  );
  expect(empty.status).toBe(502);
  await expect(empty.json()).resolves.toStrictEqual({
    code: null,
    detail: "fresh race entries are empty",
    error: "fresh_race_entries_unavailable",
  });
  expect(duplicate.status).toBe(502);
  await expect(duplicate.json()).resolves.toStrictEqual({
    code: null,
    detail: "fresh race entries contain duplicates",
    error: "fresh_race_entries_unavailable",
  });
});

it("queries and caches race trainings with the Training-compatible envelope", async () => {
  const harness = createHarness([
    {
      bamei: "Catalog Horse",
      chokyo_jikoku: "0615",
      chokyo_nengappi: "20260714",
      premium_workout_index: 1,
      training_data_source: "netkeiba",
      training_type: "ウッド",
      umaban: "07",
    },
  ]);
  const response = await handleRequest(
    new Request("https://catalog.test/v1/race-trainings?date=20260715&keibajoCode=5&raceBango=1"),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("X-Catalog-Cache")).toBe("r2-sql");
  await expect(response.json()).resolves.toMatchObject({
    rows: [
      {
        bamei: "Catalog Horse",
        chokyoJikoku: "0615",
        chokyoNengappi: "20260714",
        premiumWorkoutIndex: 1,
        trainingDataSource: "netkeiba",
        trainingType: "ウッド",
        umaban: "07",
      },
    ],
  });
  expect(String(harness.fetchCalls[0]?.init?.body)).toMatch("INNER JOIN pc_keiba.jvd_hc w");
  expect(String(harness.fetchCalls[0]?.init?.body)).toMatch(
    "FROM pc_keiba.netkeiba_training_workouts n",
  );
  expect(harness.cacheCalls.puts).toHaveLength(1);
  expect(harness.kvCalls.puts).toHaveLength(1);
});

it("requires every race-training filter", async () => {
  const harness = createHarness();
  const missingDate = await handleRequest(
    new Request("https://catalog.test/v1/race-trainings?keibajoCode=05&raceBango=01"),
    harness.env,
    harness.dependencies,
  );
  const missingVenue = await handleRequest(
    new Request("https://catalog.test/v1/race-trainings?date=20260715&raceBango=01"),
    harness.env,
    harness.dependencies,
  );
  const missingRace = await handleRequest(
    new Request("https://catalog.test/v1/race-trainings?date=20260715&keibajoCode=05"),
    harness.env,
    harness.dependencies,
  );
  expect(missingDate.status).toBe(400);
  await expect(missingDate.json()).resolves.toStrictEqual({ error: "date must match YYYYMMDD" });
  expect(missingVenue.status).toBe(400);
  await expect(missingVenue.json()).resolves.toStrictEqual({ error: "keibajoCode is required" });
  expect(missingRace.status).toBe(400);
  await expect(missingRace.json()).resolves.toStrictEqual({ error: "raceBango is required" });
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

it("validates and forwards a running-style target umaban", async () => {
  const harness = createHarness([{ ...featureRow(), umaban: 7 }]);
  const response = await handleRequest(
    new Request(
      "https://catalog.test/v1/running-style-features?date=20260715&source=jra&keibajoCode=5&raceBango=1&umaban=7",
    ),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  expect(String(harness.fetchCalls[0]?.init?.body)).toMatch(
    "try_cast(nullif(umaban, '') AS INT) = 7",
  );
  expect(String(harness.fetchCalls[0]?.init?.body)).toMatch("limit 1");

  const invalid = await handleRequest(
    new Request(
      "https://catalog.test/v1/running-style-features?date=20260715&source=jra&keibajoCode=5&raceBango=1&umaban=19",
    ),
    harness.env,
    harness.dependencies,
  );
  expect(invalid.status).toBe(400);
  await expect(invalid.json()).resolves.toStrictEqual({
    error: "umaban must be an integer from 1 to 18",
  });
});

it("retries running-style features without ORDER BY and sorts by umaban when R2 SQL rejects the query as too deep", async () => {
  const fetchCalls: Array<{ input: string; init?: RequestInit }> = [];
  const fetchImpl: Fetcher = async (input, init) => {
    fetchCalls.push({ input: String(input), init });
    if (fetchCalls.length === 1) {
      return Response.json(
        { errors: [{ code: 40018, message: "query expression too deep" }], success: false },
        { status: 400 },
      );
    }
    return Response.json({
      result: {
        rows: [
          { ...featureRow(), race_bango: "1", umaban: "3" },
          { ...featureRow(), race_bango: "1", umaban: "1" },
          { ...featureRow(), race_bango: "1", umaban: "2" },
        ],
      },
      success: true,
    });
  };
  const env: Env = {
    ADMIN_TOKEN: "admin-secret",
    CACHE_TTL_SECONDS: "15",
    CATALOG_KV: {
      async delete() {},
      async get() {
        return null;
      },
      async put() {},
    },
    KV_TTL_SECONDS: "120",
    R2_SQL_ACCOUNT_ID: "account",
    R2_SQL_BUCKET_NAME: "pc-keiba-r2-catalog",
    R2_SQL_NAMESPACE: "pc_keiba",
    R2_SQL_TOKEN: "r2-secret",
  };
  const cache: CacheStore = {
    async delete() {
      return false;
    },
    async match() {
      return undefined;
    },
    async put() {},
  };
  const response = await handleRequest(
    new Request(
      "https://catalog.test/v1/running-style-features?date=20260715&source=jra&keibajoCode=5&raceBango=1",
    ),
    env,
    { cache, fetchImpl },
  );
  expect(response.status).toBe(200);
  expect(fetchCalls).toHaveLength(2);
  expect(String(fetchCalls[0]?.init?.body)).toMatch("order by umaban limit 18");
  expect(String(fetchCalls[1]?.init?.body)).not.toMatch("order by umaban");
  expect(String(fetchCalls[1]?.init?.body)).toMatch("limit 18");
  await expect(response.json()).resolves.toMatchObject({
    rows: [{ umaban: 1 }, { umaban: 2 }, { umaban: 3 }],
  });
});

it("sorts a venue-level fallback by race_bango then umaban, tolerating absent sort keys", async () => {
  const fetchCalls: Array<{ input: string; init?: RequestInit }> = [];
  const fetchImpl: Fetcher = async (input, init) => {
    fetchCalls.push({ input: String(input), init });
    if (fetchCalls.length === 1) {
      return Response.json(
        { errors: [{ code: 40018, message: "query expression too deep" }], success: false },
        { status: 400 },
      );
    }
    return Response.json({
      result: {
        rows: [
          { ...featureRow(), race_bango: "02", umaban: "1" },
          { ...featureRow(), race_bango: "01", umaban: "2" },
          { ...featureRow(), race_bango: "01", umaban: null },
        ],
      },
      success: true,
    });
  };
  const env: Env = {
    ADMIN_TOKEN: "admin-secret",
    CACHE_TTL_SECONDS: "15",
    CATALOG_KV: {
      async delete() {},
      async get() {
        return null;
      },
      async put() {},
    },
    KV_TTL_SECONDS: "120",
    R2_SQL_ACCOUNT_ID: "account",
    R2_SQL_BUCKET_NAME: "pc-keiba-r2-catalog",
    R2_SQL_NAMESPACE: "pc_keiba",
    R2_SQL_TOKEN: "r2-secret",
  };
  const cache: CacheStore = {
    async delete() {
      return false;
    },
    async match() {
      return undefined;
    },
    async put() {},
  };
  const response = await handleRequest(
    new Request(
      "https://catalog.test/v1/running-style-features?date=20260715&source=jra&keibajoCode=05",
    ),
    env,
    { cache, fetchImpl },
  );
  expect(response.status).toBe(200);
  expect(fetchCalls).toHaveLength(2);
  expect(String(fetchCalls[1]?.init?.body)).toMatch("limit 216");
  await expect(response.json()).resolves.toMatchObject({
    rows: [
      { raceBango: "01", umaban: 0 },
      { raceBango: "01", umaban: 2 },
      { raceBango: "02", umaban: 1 },
    ],
  });
});

it("groups a venue-level fallback by race when race_bango arrives as a JSON number", async () => {
  const fetchCalls: Array<{ input: string; init?: RequestInit }> = [];
  const fetchImpl: Fetcher = async (input, init) => {
    fetchCalls.push({ input: String(input), init });
    if (fetchCalls.length === 1) {
      return Response.json(
        { errors: [{ code: 40018, message: "query expression too deep" }], success: false },
        { status: 400 },
      );
    }
    return Response.json({
      result: {
        rows: [
          { ...featureRow(), race_bango: 2, umaban: "5" },
          { ...featureRow(), race_bango: 1, umaban: "9" },
          { ...featureRow(), race_bango: 2, umaban: "4" },
        ],
      },
      success: true,
    });
  };
  const env: Env = {
    ADMIN_TOKEN: "admin-secret",
    CACHE_TTL_SECONDS: "15",
    CATALOG_KV: {
      async delete() {},
      async get() {
        return null;
      },
      async put() {},
    },
    KV_TTL_SECONDS: "120",
    R2_SQL_ACCOUNT_ID: "account",
    R2_SQL_BUCKET_NAME: "pc-keiba-r2-catalog",
    R2_SQL_NAMESPACE: "pc_keiba",
    R2_SQL_TOKEN: "r2-secret",
  };
  const cache: CacheStore = {
    async delete() {
      return false;
    },
    async match() {
      return undefined;
    },
    async put() {},
  };
  const response = await handleRequest(
    new Request(
      "https://catalog.test/v1/running-style-features?date=20260715&source=jra&keibajoCode=05",
    ),
    env,
    { cache, fetchImpl },
  );
  expect(response.status).toBe(200);
  await expect(response.json()).resolves.toMatchObject({
    rows: [
      { raceBango: "01", umaban: 9 },
      { raceBango: "02", umaban: 4 },
      { raceBango: "02", umaban: 5 },
    ],
  });
});

it("sorts a venue-level fallback row with an unusable race_bango, then rejects it in normalise", async () => {
  const harness = createHarness();
  let calls = 0;
  const consoleMock = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const response = await handleRequest(
    new Request(
      "https://catalog.test/v1/running-style-features?date=20260715&source=jra&keibajoCode=05",
    ),
    harness.env,
    {
      cache: harness.dependencies.cache,
      fetchImpl: () => {
        calls += 1;
        if (calls === 1) {
          return Promise.resolve(
            Response.json(
              { errors: [{ code: 40018, message: "query expression too deep" }], success: false },
              { status: 400 },
            ),
          );
        }
        return Promise.resolve(
          Response.json({
            result: {
              rows: [
                { ...featureRow(), race_bango: null, umaban: "1" },
                { ...featureRow(), race_bango: "02", umaban: "2" },
              ],
            },
            success: true,
          }),
        );
      },
    },
  );
  expect(response.status).toBe(502);
  await expect(response.json()).resolves.toMatchObject({
    detail: "R2 SQL row is missing race_bango",
  });
  expect(consoleMock).toHaveBeenCalledOnce();
});

it("requires all running-style race filters and a separated source", async () => {
  const harness = createHarness();
  const urls = [
    "?date=20260715&source=all&keibajoCode=05&raceBango=01",
    "?date=20260715&source=jra&raceBango=01",
    "?date=20260715&source=jra&keibajoCode=05&raceBango=1x",
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

it("builds every race at the venue in one R2 SQL call when raceBango is omitted", async () => {
  const harness = createHarness([
    { ...featureRow(), race_bango: "02", umaban: "1" },
    { ...featureRow(), race_bango: "01", umaban: "2" },
  ]);
  const response = await handleRequest(
    new Request(
      "https://catalog.test/v1/running-style-features?date=20260715&source=jra&keibajoCode=05",
    ),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  expect(harness.fetchCalls).toHaveLength(1);
  expect(String(harness.fetchCalls[0]?.init?.body)).toMatch(
    "order by race_bango, umaban limit 216",
  );
  expect(String(harness.fetchCalls[0]?.init?.body)).not.toMatch("AND race_bango = ");
});

it("keeps the decade-wide history CTEs identical between a race build and a venue build", async () => {
  const harness = createHarness();
  await handleRequest(
    new Request(
      "https://catalog.test/v1/running-style-features?date=20260715&source=jra&keibajoCode=05&raceBango=01",
    ),
    harness.env,
    harness.dependencies,
  );
  const venueHarness = createHarness();
  await handleRequest(
    new Request(
      "https://catalog.test/v1/running-style-features?date=20260715&source=jra&keibajoCode=05",
    ),
    venueHarness.env,
    venueHarness.dependencies,
  );
  const raceSql = String(harness.fetchCalls[0]?.init?.body);
  const venueSql = String(venueHarness.fetchCalls[0]?.init?.body);
  const historyOf = (sql: string): string => sql.slice(0, sql.indexOf("target_se AS"));
  expect(historyOf(venueSql)).toBe(historyOf(raceSql));
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
  await expect(response.json()).resolves.toStrictEqual({
    code: null,
    detail: "R2 SQL HTTP 403",
    error: "r2_sql_unavailable",
  });
  expect(consoleMock).toHaveBeenCalledOnce();
});

it("surfaces the Cloudflare R2 SQL error code and message on a 502", async () => {
  const harness = createHarness();
  harness.fetchState.response = Response.json(
    { errors: [{ code: 40018, message: "query expression too deep" }], success: false },
    { status: 400 },
  );
  const consoleMock = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const response = await handleRequest(
    new Request("https://catalog.test/v1/race-keys?date=20260715"),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(502);
  await expect(response.json()).resolves.toStrictEqual({
    code: 40018,
    detail: "R2 SQL HTTP 400: 40018 query expression too deep",
    error: "r2_sql_unavailable",
  });
  expect(consoleMock).toHaveBeenCalledOnce();
});

it("surfaces a non-Error throw as a string detail without a code", async () => {
  const harness = createHarness();
  harness.dependencies.fetchImpl = () => Promise.reject("socket reset");
  const consoleMock = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const response = await handleRequest(
    new Request("https://catalog.test/v1/race-keys?date=20260715"),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(502);
  await expect(response.json()).resolves.toStrictEqual({
    code: null,
    detail: "socket reset",
    error: "r2_sql_unavailable",
  });
  expect(consoleMock).toHaveBeenCalledOnce();
});

it("logs the failing path and query string alongside the error detail", async () => {
  const harness = createHarness();
  harness.fetchState.response = new Response("denied", { status: 403 });
  const consoleMock = vi.spyOn(console, "error").mockImplementation(() => undefined);
  await handleRequest(
    new Request("https://catalog.test/v1/race-keys?date=20260715"),
    harness.env,
    harness.dependencies,
  );
  expect(consoleMock).toHaveBeenCalledWith(
    "[pc-keiba-r2-catalog] request failed",
    '{"code":null,"detail":"R2 SQL HTTP 403","path":"/v1/race-keys","search":"?date=20260715"}',
  );
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

it("purges one source and its exact race-training cache key", async () => {
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
  await expect(response.json()).resolves.toStrictEqual({ ok: true, purged: 2 });
  expect(harness.cacheCalls.deletes[0]).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/race-features?date=20260715&source=ban-ei&keibajoCode=83&raceBango=09",
  );
  expect(harness.cacheCalls.deletes[1]).toBe(
    "https://pc-keiba-r2-catalog-cache.internal/v2/race-trainings?date=20260715&keibajoCode=83&raceBango=09",
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

it("rejects unknown win-rate heatmap stats routes", async () => {
  const harness = createHarness();
  const missing = await handleRequest(
    new Request("https://catalog.test/v1/win-rate-heatmap-stat?year=2026&month=7&day=15"),
    harness.env,
    harness.dependencies,
  );
  const posted = await handleRequest(
    new Request("https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=7&day=15", {
      method: "POST",
    }),
    harness.env,
    harness.dependencies,
  );
  expect(missing.status).toBe(404);
  await expect(missing.json()).resolves.toStrictEqual({ error: "not_found" });
  expect(posted.status).toBe(404);
  await expect(posted.json()).resolves.toStrictEqual({ error: "not_found" });
});

it("requires heatmap identity query params and rejects invalid flags", async () => {
  const harness = createHarness();
  const missingYear = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?month=7&day=15&keibajoCode=5&raceNumber=1&source=jra",
    ),
    harness.env,
    harness.dependencies,
  );
  const missingMonth = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&day=15&keibajoCode=5&raceNumber=1&source=jra",
    ),
    harness.env,
    harness.dependencies,
  );
  const missingDay = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=7&keibajoCode=5&raceNumber=1&source=jra",
    ),
    harness.env,
    harness.dependencies,
  );
  const missingSource = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=7&day=15&keibajoCode=5&raceNumber=1",
    ),
    harness.env,
    harness.dependencies,
  );
  const missingVenue = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=7&day=15&raceNumber=1&source=jra",
    ),
    harness.env,
    harness.dependencies,
  );
  const missingRace = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=7&day=15&keibajoCode=5&source=jra",
    ),
    harness.env,
    harness.dependencies,
  );
  const badSource = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=7&day=15&keibajoCode=5&raceNumber=1&source=all",
    ),
    harness.env,
    harness.dependencies,
  );
  const badFlag = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=7&day=15&keibajoCode=5&raceNumber=1&source=jra&includeVenue=2",
    ),
    harness.env,
    harness.dependencies,
  );
  const badDistance = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=7&day=15&keibajoCode=5&raceNumber=1&source=jra&includeDistance=yes",
    ),
    harness.env,
    harness.dependencies,
  );
  const badSurface = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=7&day=15&keibajoCode=5&raceNumber=1&source=jra&includeSurface=2",
    ),
    harness.env,
    harness.dependencies,
  );
  const badTurn = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=7&day=15&keibajoCode=5&raceNumber=1&source=jra&includeTurn=2",
    ),
    harness.env,
    harness.dependencies,
  );
  const badYears = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=7&day=15&keibajoCode=5&raceNumber=1&source=jra&years=0",
    ),
    harness.env,
    harness.dependencies,
  );
  const tooManyYears = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=7&day=15&keibajoCode=5&raceNumber=1&source=jra&years=51",
    ),
    harness.env,
    harness.dependencies,
  );
  const badDay = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=2&day=31&keibajoCode=5&raceNumber=1&source=jra",
    ),
    harness.env,
    harness.dependencies,
  );
  expect(missingYear.status).toBe(400);
  await expect(missingYear.json()).resolves.toStrictEqual({ error: "year must match YYYY" });
  expect(missingMonth.status).toBe(400);
  await expect(missingMonth.json()).resolves.toStrictEqual({
    error: "month must contain one or two digits",
  });
  expect(missingDay.status).toBe(400);
  await expect(missingDay.json()).resolves.toStrictEqual({
    error: "day must contain one or two digits",
  });
  expect(missingSource.status).toBe(400);
  await expect(missingSource.json()).resolves.toStrictEqual({ error: "source is required" });
  expect(missingVenue.status).toBe(400);
  await expect(missingVenue.json()).resolves.toStrictEqual({ error: "keibajoCode is required" });
  expect(missingRace.status).toBe(400);
  await expect(missingRace.json()).resolves.toStrictEqual({ error: "raceNumber is required" });
  expect(badSource.status).toBe(400);
  await expect(badSource.json()).resolves.toStrictEqual({ error: "source must be jra or nar" });
  expect(badFlag.status).toBe(400);
  await expect(badFlag.json()).resolves.toStrictEqual({ error: "includeVenue must be 0 or 1" });
  expect(badDistance.status).toBe(400);
  await expect(badDistance.json()).resolves.toStrictEqual({
    error: "includeDistance must be 0 or 1",
  });
  expect(badSurface.status).toBe(400);
  await expect(badSurface.json()).resolves.toStrictEqual({
    error: "includeSurface must be 0 or 1",
  });
  expect(badTurn.status).toBe(400);
  await expect(badTurn.json()).resolves.toStrictEqual({ error: "includeTurn must be 0 or 1" });
  expect(badYears.status).toBe(400);
  await expect(badYears.json()).resolves.toStrictEqual({
    error: "years must be an integer from 1 to 50",
  });
  expect(tooManyYears.status).toBe(400);
  await expect(tooManyYears.json()).resolves.toStrictEqual({
    error: "years must be an integer from 1 to 50",
  });
  expect(badDay.status).toBe(400);
  await expect(badDay.json()).resolves.toStrictEqual({
    error: "year, month, and day must be a valid calendar date",
  });
  expect(harness.fetchCalls).toHaveLength(0);
});

it("queries R2 SQL for heatmap stats and maps empty details arrays", async () => {
  const harness = createHarness();
  harness.dependencies.fetchImpl = async (input, init) => {
    harness.fetchCalls.push({ input: String(input), init });
    const query = String(init?.body);
    if (query.includes("ketto_joho_01b")) {
      return Response.json({
        result: {
          rows: [
            {
              category: "sire",
              name: "Deep Impact",
              places: 3,
              shows: 4,
              starts: 10,
              umaban: 7,
              wins: 2,
            },
          ],
        },
        success: true,
      });
    }
    return Response.json({
      result: {
        rows: [
          {
            kind: "jockey",
            name: "Take",
            places: 2,
            shows: 3,
            starts: 8,
            umaban: 7,
            wins: 1,
          },
          {
            kind: "trainer",
            name: "Fujisawa",
            places: 1,
            shows: 1,
            starts: 5,
            umaban: 7,
            wins: 0,
          },
        ],
      },
      success: true,
    });
  };
  const response = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=7&day=15&keibajoCode=5&raceNumber=1&source=jra",
    ),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("X-Catalog-Cache")).toBe("r2-sql");
  await expect(response.json()).resolves.toStrictEqual({
    bloodlineRows: [
      {
        category: "sire",
        details: [],
        name: "Deep Impact",
        places: 3,
        shows: 4,
        starts: 10,
        umaban: 7,
        wins: 2,
      },
    ],
    similarRows: [
      {
        details: [],
        kind: "jockey",
        name: "Take",
        places: 2,
        shows: 3,
        starts: 8,
        umaban: 7,
        wins: 1,
      },
      {
        details: [],
        kind: "trainer",
        name: "Fujisawa",
        places: 1,
        shows: 1,
        starts: 5,
        umaban: 7,
        wins: 0,
      },
    ],
  });
  expect(harness.fetchCalls).toHaveLength(2);
  expect(String(harness.fetchCalls[0]?.init?.body)).toMatch("ketto_joho_01b");
  expect(String(harness.fetchCalls[1]?.init?.body)).toMatch("'jockey' AS kind");
  expect(harness.cacheCalls.puts).toHaveLength(1);
  expect(harness.kvCalls.puts).toHaveLength(1);
});

it("reads cached heatmap stats from Cache API and KV", async () => {
  const cacheHarness = createHarness();
  const descriptor = heatmapStatsDescriptor({
    date: "20260715",
    includeDistance: true,
    includeSurface: true,
    includeTurn: true,
    includeVenue: true,
    keibajoCode: "05",
    raceBango: "01",
    source: "jra",
    years: 10,
  });
  cacheHarness.cacheEntries.set(
    cacheRequestFor(descriptor).url,
    Response.json({ bloodlineRows: [{ name: "cached" }], similarRows: [] }),
  );
  const cached = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=07&day=15&keibajoCode=05&raceNumber=01&source=jra&years=10&includeVenue=1&includeDistance=1&includeSurface=1&includeTurn=1",
    ),
    cacheHarness.env,
    cacheHarness.dependencies,
  );
  expect(cached.headers.get("X-Catalog-Cache")).toBe("cache-api");
  await expect(cached.json()).resolves.toStrictEqual({
    bloodlineRows: [{ name: "cached" }],
    similarRows: [],
  });
  expect(cacheHarness.fetchCalls).toHaveLength(0);

  const kvHarness = createHarness();
  kvHarness.kvEntries.set(
    kvKeyFor(descriptor),
    '{"bloodlineRows":[{"name":"kv"}],"similarRows":[]}',
  );
  const kvResponse = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=7&day=15&keibajoCode=5&raceNumber=1&source=jra",
    ),
    kvHarness.env,
    kvHarness.dependencies,
  );
  expect(kvResponse.headers.get("X-Catalog-Cache")).toBe("kv");
  await expect(kvResponse.json()).resolves.toStrictEqual({
    bloodlineRows: [{ name: "kv" }],
    similarRows: [],
  });
  expect(kvHarness.fetchCalls).toHaveLength(0);
  expect(kvHarness.cacheCalls.puts).toHaveLength(1);
});

it("queries NAR heatmap stats with optional filters off and ignores rows-shaped KV", async () => {
  const harness = createHarness([]);
  const descriptor = heatmapStatsDescriptor({
    date: "20260715",
    includeDistance: false,
    includeSurface: false,
    includeTurn: false,
    includeVenue: false,
    keibajoCode: "03",
    raceBango: "08",
    source: "nar",
    years: 5,
  });
  harness.kvEntries.set(kvKeyFor(descriptor), '{"rows":[]}');
  const response = await handleRequest(
    new Request(
      "https://catalog.test/v1/win-rate-heatmap-stats?year=2026&month=7&day=15&keibajoCode=3&raceNumber=8&source=nar&years=5&includeVenue=0&includeDistance=0&includeSurface=0&includeTurn=0",
    ),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  await expect(response.json()).resolves.toStrictEqual({ bloodlineRows: [], similarRows: [] });
  expect(harness.fetchCalls).toHaveLength(2);
  expect(String(harness.fetchCalls[0]?.init?.body)).toMatch("FROM pc_keiba.nvd_se se");
  expect(String(harness.fetchCalls[0]?.init?.body)).toMatch("FROM pc_keiba.jvd_se se");
  expect(String(harness.fetchCalls[0]?.init?.body)).toMatch("UNION ALL");
  expect(String(harness.fetchCalls[0]?.init?.body)).toMatch(
    "concat(ra.kaisai_nen, ra.kaisai_tsukihi) >= '20210715'",
  );
  expect(String(harness.fetchCalls[0]?.init?.body)).not.toMatch("AND ra.keibajo_code = '03'");
});
