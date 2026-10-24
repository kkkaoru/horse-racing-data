// Runs with bun through Vitest; R2 SQL is mocked.
import { afterEach, expect, it, vi } from "vitest";

import { cacheRequestFor, horseRaceResultsDescriptor, kvKeyFor } from "./cache";
import type { CacheStore, Env, Fetcher, KvStore, WorkerDependencies } from "./types";
import { handleRequest } from "./worker";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const currentSqlRow = {
  current_jockey: "Take",
  current_umaban: "01",
  ketto_toroku_bango: "2023100001",
};
const historySqlRow = {
  bamei: "Deep",
  kaisai_nen: "2025",
  kaisai_tsukihi: "0715",
  keibajo_code: "05",
  ketto_toroku_bango: "2023100001",
  race_bango: "11",
};

afterEach(() => {
  vi.useRealTimers();
});

const createHarness = (current: unknown[], history: unknown[] = []) => {
  const cacheEntries = new Map<string, Response>();
  const kvEntries = new Map<string, string>();
  const kvTtls: string[] = [];
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
    async put(key, value, options) {
      kvEntries.set(key, value);
      kvTtls.push(`${key.slice(0, 15)}:${String(options?.expirationTtl)}`);
    },
  };
  const fetchImpl: Fetcher = async (_input, init) => {
    const rawBody = init?.body;
    const text = typeof rawBody === "string" ? rawBody : "";
    const parsed: unknown = JSON.parse(text);
    const query = isRecord(parsed) && typeof parsed.query === "string" ? parsed.query : "";
    queryBodies.push(query);
    return Response.json({
      result: { rows: query.includes("AS current_umaban") ? current : history },
      success: true,
    });
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
  return { cacheEntries, dependencies, env, kvEntries, kvTtls, queryBodies };
};

it("queries and caches domestic horse race results with training TTL", async () => {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(Date.parse("2026-10-24T03:00:00Z"));
  const harness = createHarness([currentSqlRow], [historySqlRow, historySqlRow]);
  const response = await handleRequest(
    new Request(
      "https://catalog.test/v1/horse-race-results?date=20260715&keibajoCode=5&raceBango=1&source=jra&sourceScope=all",
    ),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("X-Catalog-Cache")).toBe("r2-sql");
  await expect(response.json()).resolves.toStrictEqual({
    rows: [
      {
        babajotaiCodeDirt: null,
        babajotaiCodeShiba: null,
        bamei: "Deep",
        banushimei: null,
        barei: null,
        bataiju: null,
        blinkerShiyoKubun: null,
        chokyoshimeiRyakusho: null,
        corner1: null,
        corner2: null,
        corner3: null,
        corner4: null,
        currentBarei: null,
        currentJockey: "Take",
        currentSeibetsuCode: null,
        currentUmaban: "01",
        futanJuryo: null,
        gradeCode: null,
        hassoJikoku: null,
        juryoShubetsuCode: null,
        kaisaiNen: "2025",
        kaisaiTsukihi: "0715",
        kakuteiChakujun: null,
        keibajoCode: "05",
        kettoTorokuBango: "2023100001",
        kishumeiRyakusho: null,
        kohan3f: null,
        kyori: null,
        kyosoJokenCode: null,
        kyosoJokenMeisho: null,
        kyosoKigoCode: null,
        kyosomeiFukudai: null,
        kyosomeiHondai: null,
        kyosomeiKakkonai: null,
        kyosoShubetsuCode: null,
        raceBango: "11",
        seibetsuCode: null,
        shussoTosu: null,
        sohaTime: null,
        tanshoNinkijun: null,
        tanshoOdds: null,
        tenkoCode: null,
        timeSa: null,
        trackCode: null,
        umaban: null,
        wakuban: null,
        zogenFugo: null,
        zogenSa: null,
      },
    ],
  });
  expect(harness.queryBodies).toHaveLength(2);
  expect(harness.queryBodies[0]).toMatch("FROM pc_keiba.jvd_se\nWHERE kaisai_nen = '2026'");
  expect(harness.queryBodies[1]).toMatch("FROM pc_keiba.jvd_se se");
  expect(harness.queryBodies[1]).toMatch("FROM pc_keiba.nvd_se se");
  expect(harness.queryBodies[1]).toMatch("se.ketto_toroku_bango IN ('2023100001')");
  expect(harness.queryBodies[1]).toMatch("concat(kaisai_nen, kaisai_tsukihi) < '20260715'");
  expect(harness.queryBodies[1]).not.toMatch("oversea_");
  // A settled race day: its whole history is reused for six hours.
  expect(harness.kvTtls).toStrictEqual(["r2sql-result:v1:21600", "catalog:v2:v2/h:120"]);
  const descriptor = horseRaceResultsDescriptor({
    date: "20260715",
    keibajoCode: "05",
    raceBango: "01",
    source: "jra",
    sourceScope: "all",
  });
  const cached = harness.cacheEntries.get(cacheRequestFor(descriptor).url);
  expect(cached?.headers.get("Cache-Control")).toBe("public, max-age=15");
});

it("prefers raceBango when both raceBango and raceNumber are present", async () => {
  const harness = createHarness([]);
  const response = await handleRequest(
    new Request(
      "https://catalog.test/v1/horse-race-results?date=20260715&keibajoCode=05&raceBango=03&raceNumber=08&source=jra",
    ),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  expect(harness.queryBodies.join("\n")).toMatch("race_bango = '03'");
});

it("accepts raceNumber as an alias for raceBango", async () => {
  const harness = createHarness([currentSqlRow]);
  const response = await handleRequest(
    new Request(
      "https://catalog.test/v1/horse-race-results?date=20260715&keibajoCode=05&raceNumber=8&source=nar&sourceScope=jra",
    ),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  expect(harness.queryBodies[0]).toMatch("race_bango = '08'");
  expect(harness.queryBodies[0]).toMatch("FROM pc_keiba.nvd_se");
  expect(harness.queryBodies[1]).toMatch("FROM pc_keiba.jvd_se se");
  expect(harness.queryBodies[1]).not.toMatch("nvd_ra");
});

it("reads the race day's unsettled history fresh and caches only the settled part", async () => {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(Date.parse("2026-10-24T03:00:00Z"));
  const harness = createHarness(
    [currentSqlRow, { ...currentSqlRow, current_umaban: "02", ketto_toroku_bango: "2022100002" }],
    [historySqlRow],
  );
  const response = await handleRequest(
    new Request(
      "https://catalog.test/v1/horse-race-results?date=20261024&keibajoCode=05&raceBango=11&source=jra&sourceScope=jra",
    ),
    harness.env,
    harness.dependencies,
  );
  expect(response.status).toBe(200);
  expect(harness.queryBodies).toHaveLength(3);
  const settled = harness.queryBodies.slice(1).find((body) => !body.includes(">= '20261023'"));
  const recent = harness.queryBodies.find((body) => body.includes(">= '20261023'"));
  expect(settled).toMatch("IN ('2022100002', '2023100001')");
  expect(settled).toMatch("se.kaisai_nen >= '2024' AND se.kaisai_nen <= '2026'");
  expect(settled).toMatch("WHERE concat(kaisai_nen, kaisai_tsukihi) < '20261023'");
  expect(recent).toMatch("< '20261024'\n  AND concat(kaisai_nen, kaisai_tsukihi) >= '20261023'");
  expect(harness.kvTtls).toStrictEqual(["r2sql-result:v1:21600", "catalog:v2:v2/h:120"]);
});

it("skips the history scans when the race has no registered runners", async () => {
  const harness = createHarness([], [historySqlRow]);
  const response = await handleRequest(
    new Request(
      "https://catalog.test/v1/horse-race-results?date=20261024&keibajoCode=05&raceBango=11&source=jra",
    ),
    harness.env,
    harness.dependencies,
  );
  await expect(response.json()).resolves.toStrictEqual({ rows: [] });
  expect(harness.queryBodies).toHaveLength(1);
});

it("reads horse race results from Cache API then KV before R2 SQL", async () => {
  const harness = createHarness([currentSqlRow], [historySqlRow]);
  const descriptor = horseRaceResultsDescriptor({
    date: "20260715",
    keibajoCode: "05",
    raceBango: "01",
    source: "jra",
    sourceScope: "all",
  });
  await harness.dependencies.cache.put(
    cacheRequestFor(descriptor),
    new Response('{"rows":[]}', {
      headers: { "Content-Type": "application/json; charset=utf-8" },
    }),
  );
  const cacheHit = await handleRequest(
    new Request(
      "https://catalog.test/v1/horse-race-results?date=20260715&keibajoCode=05&raceBango=01&source=jra",
    ),
    harness.env,
    harness.dependencies,
  );
  expect(cacheHit.headers.get("X-Catalog-Cache")).toBe("cache-api");
  expect(harness.queryBodies).toStrictEqual([]);

  const kvHarness = createHarness([currentSqlRow], [historySqlRow]);
  await kvHarness.env.CATALOG_KV.put(kvKeyFor(descriptor), '{"rows":[]}', { expirationTtl: 120 });
  const kvHit = await handleRequest(
    new Request(
      "https://catalog.test/v1/horse-race-results?date=20260715&keibajoCode=05&raceBango=01&source=jra",
    ),
    kvHarness.env,
    kvHarness.dependencies,
  );
  expect(kvHit.headers.get("X-Catalog-Cache")).toBe("kv");
  expect(kvHarness.queryBodies).toStrictEqual([]);
});

it("rejects horse-race-results requests with missing or invalid filters", async () => {
  const harness = createHarness([]);
  const missingDate = await handleRequest(
    new Request(
      "https://catalog.test/v1/horse-race-results?keibajoCode=05&raceBango=01&source=jra",
    ),
    harness.env,
    harness.dependencies,
  );
  const missingRace = await handleRequest(
    new Request(
      "https://catalog.test/v1/horse-race-results?date=20260715&keibajoCode=05&source=jra",
    ),
    harness.env,
    harness.dependencies,
  );
  const badSource = await handleRequest(
    new Request(
      "https://catalog.test/v1/horse-race-results?date=20260715&keibajoCode=05&raceBango=01&source=all",
    ),
    harness.env,
    harness.dependencies,
  );
  const badScope = await handleRequest(
    new Request(
      "https://catalog.test/v1/horse-race-results?date=20260715&keibajoCode=05&raceBango=01&source=jra&sourceScope=ban-ei",
    ),
    harness.env,
    harness.dependencies,
  );
  const badRaceBango = await handleRequest(
    new Request(
      "https://catalog.test/v1/horse-race-results?date=20260715&keibajoCode=05&raceBango=1x&source=jra",
    ),
    harness.env,
    harness.dependencies,
  );
  expect(missingDate.status).toBe(400);
  expect(missingRace.status).toBe(400);
  expect(badSource.status).toBe(400);
  expect(badScope.status).toBe(400);
  expect(badRaceBango.status).toBe(400);
});
