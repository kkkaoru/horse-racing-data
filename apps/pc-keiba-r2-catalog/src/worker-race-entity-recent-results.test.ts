// Run with bun (bunx vitest).
import { expect, it, vi } from "vitest";

vi.mock("hyparquet", () => ({
  parquetReadObjects: vi.fn(async ({ file }: { file: ArrayBuffer }) => {
    const value: unknown = JSON.parse(new TextDecoder().decode(file));
    return Array.isArray(value) ? value : [];
  }),
}));

import type { CacheStore, Env, Fetcher, KvStore, ObjectStore, WorkerDependencies } from "./types";
import { handleRequest } from "./worker";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const targetRow = {
  entity_bucket: "a",
  entity_id: "21379",
  entity_name: "Jockey",
  horse_id: "2022103916",
  horse_name: "Horse",
  race_name: "Target",
  race_start_time: "1240",
  runner_found: true,
};

const warmTargetRow = {
  horse_bucket: "a",
  horse_id: "2022103916",
  horse_name: "Horse",
  horse_number: "07",
  jockey_bucket: "b",
  jockey_id: "21379",
  jockey_name: "Jockey",
  owner_bucket: "c",
  owner_id: "768006",
  owner_name: "Owner",
  race_name: "Target",
  race_start_time: "1240",
  trainer_bucket: "d",
  trainer_id: "20692",
  trainer_name: "Trainer",
};

const historyRow = {
  finish_position: 1,
  horse_id: "2022103916",
  horse_name: "Horse",
  jockey_id: "21379",
  jockey_name: "Jockey",
  kaisai_nen: "2026",
  kaisai_tsukihi: "0827",
  keibajo_code: "50",
  owner_id: "768006",
  race_bango: "04",
  race_id: "nar:20260827:50:04",
  race_start_sort_key: "202608271210",
  race_start_time: "1210",
  result_id: "nar:20260827:50:04:07:2022103916",
  source: "nar",
  trainer_id: "20692",
};

const harness = (queryRows: (unknown[] | Error)[]) => {
  const cacheEntries = new Map<string, Response>();
  const kvEntries = new Map<string, string>();
  const queries: string[] = [];
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
    const body = typeof init?.body === "string" ? init.body : "{}";
    const parsed: unknown = JSON.parse(body);
    queries.push(isRecord(parsed) && typeof parsed.query === "string" ? parsed.query : "");
    const result = queryRows[queries.length - 1] ?? [];
    if (result instanceof Error) throw result;
    return Response.json({ result: { rows: result }, success: true });
  };
  const env: Env = {
    CATALOG_KV: kv,
    R2_SQL_ACCOUNT_ID: "account",
    R2_SQL_BUCKET_NAME: "bucket",
    R2_SQL_NAMESPACE: "catalog",
    R2_SQL_TOKEN: "token",
    RACE_ENTITY_CURSOR_SECRET: "cursor-signing-secret-at-least-32-characters",
    RACE_ENTITY_WARM_TOKEN: "warm-token",
  };
  const dependencies: WorkerDependencies = { cache, fetchImpl };
  return { cacheEntries, dependencies, env, kvEntries, queries };
};

const url =
  "https://catalog.test/v1/race-entity-recent-results?date=20260827&keibajoCode=50&raceBango=05&source=nar&horseNumber=7&entityType=jockey&limit=1";

const encoder = new TextEncoder();

const objectStore = (entries: Record<string, unknown>): ObjectStore => {
  const objects = new Map(
    Object.entries(entries).map(([key, value]) => [key, encoder.encode(JSON.stringify(value))]),
  );
  return {
    async get(key) {
      const bytes = objects.get(key);
      return bytes === undefined
        ? null
        : { body: new Blob([bytes]).stream(), size: bytes.byteLength };
    },
  };
};

const catalogTable = (key: string, rows: unknown[], partition: string) => ({
  dataPrefix: "",
  partitions: { [partition]: [[key, encoder.encode(JSON.stringify(rows)).byteLength]] },
  snapshotId: `${key}-snapshot`,
});

it("serves a cold page entirely through native R2 objects", async () => {
  const test = harness([]);
  const prefix = "entity-serving-v1";
  const raw = {
    bamei: "Horse",
    entity_bucket: "a",
    entity_id: "21379",
    entity_name: "Jockey",
    entity_type: "jockey",
    hasso_jikoku: "1210",
    kaisai_nen: "2026",
    kaisai_tsukihi: "0820",
    keibajo_code: "50",
    ketto_toroku_bango: "2022103916",
    kishu_code: "21379",
    kishumei_ryakusho: "Jockey",
    race_bango: "04",
    result_id: "nar:20260820:50:04:07:2022103916",
    source: "nar",
    umaban: "07",
  };
  const raceRows = [
    {
      hasso_jikoku: "1240",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0827",
      keibajo_code: "50",
      kyosomei_hondai: "Target",
      race_bango: "05",
    },
  ];
  const runnerRows = [
    {
      bamei: "Horse",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0827",
      keibajo_code: "50",
      ketto_toroku_bango: "2022103916",
      kishu_code: "21379",
      kishumei_ryakusho: "Jockey",
      race_bango: "05",
      umaban: "07",
    },
  ];
  const historyRows = [raw];
  const raceKey = `${prefix}/race.parquet`;
  const runnerKey = `${prefix}/runner.parquet`;
  const historyKey = `${prefix}/history.parquet`;
  test.env.CATALOG_OBJECTS = objectStore({
    "entity-catalog-serving-v1/manifest.json": {
      history: catalogTable(historyKey, historyRows, "jockey/nar/a/2026"),
      raw: {
        nvd_ra: catalogTable(raceKey, raceRows, "2026"),
        nvd_se: catalogTable(runnerKey, runnerRows, "2026"),
      },
      version: 1,
    },
    [historyKey]: historyRows,
    [raceKey]: raceRows,
    [runnerKey]: runnerRows,
  });
  const response = await handleRequest(new Request(url), test.env, test.dependencies);
  expect(response.status).toBe(200);
  expect(response.headers.get("X-Catalog-Cache")).toBe("r2-catalog-parquet");
  expect(test.queries).toEqual([]);
  const payload = (await response.json()) as { results: unknown[] };
  expect(payload.results).toHaveLength(1);
});

it("warms every canonical first page for a race into Cache API and KV", async () => {
  const test = harness([
    [warmTargetRow],
    [{ ...historyRow, matched_entity_id: "2022103916" }],
    [{ ...historyRow, matched_entity_id: "21379" }],
    [{ ...historyRow, matched_entity_id: "20692" }],
    [{ ...historyRow, matched_entity_id: "768006" }],
  ]);
  const warmUrl =
    "https://catalog.test/v1/internal/race-entity-recent-results/warm?date=20260827&keibajoCode=50&raceBango=05&source=nar";
  const denied = await handleRequest(
    new Request(warmUrl, { method: "POST" }),
    test.env,
    test.dependencies,
  );
  expect(denied.status).toBe(404);
  const response = await handleRequest(
    new Request(warmUrl, {
      headers: { Authorization: "Bearer warm-token", "X-PC-Keiba-Cache-Warm": "queue" },
      method: "POST",
    }),
    test.env,
    test.dependencies,
  );
  expect(response.status).toBe(200);
  await expect(response.json()).resolves.toStrictEqual({ pages: 4, runners: 1 });
  expect(test.queries).toHaveLength(5);
  expect(test.queries[0]).toMatch(/ORDER BY try_cast/u);
  expect(test.queries.slice(1)).toHaveLength(4);
  expect(test.cacheEntries.size).toBe(4);
  expect(test.kvEntries.size).toBe(4);

  const warmedHorse = await handleRequest(
    new Request(url.replace("entityType=jockey&limit=1", "entityType=horse")),
    test.env,
    test.dependencies,
  );
  expect(warmedHorse.headers.get("X-Catalog-Cache")).toBe("cache-api");
  expect(test.queries).toHaveLength(5);
});

it("skips unavailable warm identities and handles races without runners", async () => {
  const partial = harness([
    [{ ...warmTargetRow, owner_id: "000000" }],
    [{ ...historyRow, matched_entity_id: "2022103916" }],
    [{ ...historyRow, matched_entity_id: "21379" }],
    [{ ...historyRow, matched_entity_id: "20692" }],
  ]);
  const warmUrl =
    "https://catalog.test/v1/internal/race-entity-recent-results/warm?date=20260827&keibajoCode=50&raceBango=05&source=nar";
  const partialResponse = await handleRequest(
    new Request(warmUrl, {
      headers: { Authorization: "Bearer warm-token", "X-PC-Keiba-Cache-Warm": "queue" },
      method: "POST",
    }),
    partial.env,
    partial.dependencies,
  );
  await expect(partialResponse.json()).resolves.toStrictEqual({ pages: 3, runners: 1 });

  const empty = harness([[]]);
  const emptyResponse = await handleRequest(
    new Request(warmUrl, {
      headers: { Authorization: "Bearer warm-token", "X-PC-Keiba-Cache-Warm": "queue" },
      method: "POST",
    }),
    empty.env,
    empty.dependencies,
  );
  await expect(emptyResponse.json()).resolves.toStrictEqual({ pages: 0, runners: 0 });

  const missingHorseNumber = harness([[{ ...warmTargetRow, horse_number: null }]]);
  const missingHorseNumberResponse = await handleRequest(
    new Request(warmUrl, {
      headers: { Authorization: "Bearer warm-token", "X-PC-Keiba-Cache-Warm": "queue" },
      method: "POST",
    }),
    missingHorseNumber.env,
    missingHorseNumber.dependencies,
  );
  await expect(missingHorseNumberResponse.json()).resolves.toStrictEqual({ pages: 0, runners: 1 });
});

it("serves canonical jockey history from the indexed R2 SQL table and populates both caches", async () => {
  const test = harness([
    [targetRow],
    [historyRow, { ...historyRow, result_id: "nar:20260820:50:01:01:1" }],
  ]);
  const response = await handleRequest(new Request(url), test.env, test.dependencies);
  expect(response.status).toBe(200);
  expect(response.headers.get("X-Catalog-Cache")).toBe("r2-sql");
  const value: unknown = await response.json();
  expect(value).toMatchObject({
    entity: { entityId: "21379", entityType: "jockey", horseNumber: "07" },
    pagination: { effectiveLimit: 1, hasMore: true, requestedLimit: 1, returned: 1 },
  });
  expect(test.queries).toHaveLength(2);
  expect(test.queries[0]).toMatch(/kishu_code/u);
  expect(test.queries[1]).toMatch(/race_entity_history_v1/u);
  expect(test.queries[1]).toMatch(/LIMIT 2/u);
  expect(test.cacheEntries.size).toBe(1);
  expect(test.kvEntries.size).toBe(1);

  const cached = await handleRequest(new Request(url), test.env, test.dependencies);
  expect(cached.headers.get("X-Catalog-Cache")).toBe("cache-api");
  expect(test.queries).toHaveLength(2);
});

it("falls back from Cache API to KV without querying R2 SQL", async () => {
  const test = harness([[targetRow], [historyRow]]);
  const first = await handleRequest(new Request(url), test.env, test.dependencies);
  expect(first.status).toBe(200);
  test.cacheEntries.clear();
  const second = await handleRequest(new Request(url), test.env, test.dependencies);
  expect(second.headers.get("X-Catalog-Cache")).toBe("kv");
  expect(test.queries).toHaveLength(2);
});

it("uses entity defaults and reports invalid entity and limit", async () => {
  const defaults = harness([[targetRow], [historyRow]]);
  const defaultResponse = await handleRequest(
    new Request(url.replace("&limit=1", "")),
    defaults.env,
    defaults.dependencies,
  );
  expect(defaultResponse.status).toBe(200);
  await expect(defaultResponse.json()).resolves.toMatchObject({
    pagination: { effectiveLimit: 10, requestedLimit: 10 },
  });

  const invalidEntity = await handleRequest(
    new Request(url.replace("entityType=jockey", "entityType=breeder")),
    defaults.env,
    defaults.dependencies,
  );
  expect(invalidEntity.status).toBe(400);
  await expect(invalidEntity.json()).resolves.toStrictEqual({
    error: {
      code: "INVALID_ENTITY_TYPE",
      message: "entityType must be horse, jockey, trainer, or owner.",
    },
  });

  const invalidLimit = await handleRequest(
    new Request(url.replace("limit=1", "limit=31")),
    defaults.env,
    defaults.dependencies,
  );
  expect(invalidLimit.status).toBe(400);
  await expect(invalidLimit.json()).resolves.toStrictEqual({
    error: {
      code: "INVALID_LIMIT",
      message: "limit must be an integer from 1 to 30 for jockey.",
    },
  });
});

it("distinguishes race, runner, horse, entity ID, entity, and history errors", async () => {
  const missingRace = harness([[]]);
  expect(
    (await handleRequest(new Request(url), missingRace.env, missingRace.dependencies)).status,
  ).toBe(404);

  const missingRunner = harness([[{ ...targetRow, runner_found: false }]]);
  await expect(
    (await handleRequest(new Request(url), missingRunner.env, missingRunner.dependencies)).json(),
  ).resolves.toMatchObject({ error: { code: "RUNNER_NOT_FOUND" } });

  const missingHorse = harness([[{ ...targetRow, horse_id: "0000000000" }]]);
  await expect(
    (await handleRequest(new Request(url), missingHorse.env, missingHorse.dependencies)).json(),
  ).resolves.toMatchObject({ error: { code: "HORSE_NOT_FOUND" } });

  const missingId = harness([[{ ...targetRow, entity_id: "00000" }]]);
  await expect(
    (await handleRequest(new Request(url), missingId.env, missingId.dependencies)).json(),
  ).resolves.toMatchObject({ error: { code: "ENTITY_ID_NOT_AVAILABLE" } });

  const malformedTarget = harness([[{ ...targetRow, entity_bucket: "z" }]]);
  await expect(
    (
      await handleRequest(new Request(url), malformedTarget.env, malformedTarget.dependencies)
    ).json(),
  ).resolves.toMatchObject({ error: { code: "MALFORMED_TARGET_DATA" } });

  const missingEntity = harness([[{ ...targetRow, entity_name: null }]]);
  await expect(
    (await handleRequest(new Request(url), missingEntity.env, missingEntity.dependencies)).json(),
  ).resolves.toMatchObject({ error: { code: "JOCKEY_NOT_FOUND" } });

  const missingHistory = harness([[targetRow], []]);
  await expect(
    (await handleRequest(new Request(url), missingHistory.env, missingHistory.dependencies)).json(),
  ).resolves.toMatchObject({
    pagination: { hasMore: false, nextCursor: null, returned: 0 },
    results: [],
  });
});

it("rejects a cache miss when the cursor signing secret is unavailable", async () => {
  const test = harness([[targetRow]]);
  delete test.env.RACE_ENTITY_CURSOR_SECRET;
  const response = await handleRequest(new Request(url), test.env, test.dependencies);
  expect(response.status).toBe(502);
  await expect(response.json()).resolves.toStrictEqual({
    error: {
      code: "UPSTREAM_ERROR",
      message: "The race entity cursor signing secret is unavailable.",
    },
  });
});

it("reports malformed rows, invalid route inputs, timeout, and upstream errors", async () => {
  const malformed = harness([[targetRow], [{ ...historyRow, kaisai_nen: null }]]);
  await expect(
    (await handleRequest(new Request(url), malformed.env, malformed.dependencies)).json(),
  ).resolves.toMatchObject({ error: { code: "MALFORMED_HISTORY_DATA" } });

  const invalidHorse = harness([]);
  await expect(
    (
      await handleRequest(
        new Request(url.replace("horseNumber=7", "horseNumber=x")),
        invalidHorse.env,
        invalidHorse.dependencies,
      )
    ).json(),
  ).resolves.toMatchObject({ error: { code: "RUNNER_NOT_FOUND" } });

  const invalidSource = harness([]);
  await expect(
    (
      await handleRequest(
        new Request(url.replace("source=nar", "source=all")),
        invalidSource.env,
        invalidSource.dependencies,
      )
    ).json(),
  ).resolves.toMatchObject({ error: { code: "RACE_NOT_FOUND" } });

  const timeout = harness([new Error("query timeout")]);
  const timeoutResponse = await handleRequest(new Request(url), timeout.env, timeout.dependencies);
  expect(timeoutResponse.status).toBe(504);
  await expect(timeoutResponse.json()).resolves.toMatchObject({ error: { code: "TIMEOUT" } });

  const upstream = harness([new Error("network")]);
  const upstreamResponse = await handleRequest(
    new Request(url),
    upstream.env,
    upstream.dependencies,
  );
  expect(upstreamResponse.status).toBe(502);
  await expect(upstreamResponse.json()).resolves.toMatchObject({
    error: { code: "UPSTREAM_ERROR" },
  });
});

it("maps an individually oversized result to malformed history data", async () => {
  const test = harness([[targetRow], [{ ...historyRow, race_name: "x".repeat(70_000) }]]);
  const response = await handleRequest(new Request(url), test.env, test.dependencies);
  expect(response.status).toBe(502);
  await expect(response.json()).resolves.toMatchObject({
    error: { code: "MALFORMED_HISTORY_DATA" },
  });
});

it("rejects a cursor reused for another entity", async () => {
  const first = harness([
    [targetRow],
    [historyRow, { ...historyRow, result_id: "nar:20260820:50:01:01:1" }],
  ]);
  const response = await handleRequest(new Request(url), first.env, first.dependencies);
  const value: unknown = await response.json();
  const cursor =
    isRecord(value) && isRecord(value.pagination) && typeof value.pagination.nextCursor === "string"
      ? value.pagination.nextCursor
      : "";
  const second = harness([[{ ...targetRow, entity_id: "99999" }]]);
  const reused = await handleRequest(
    new Request(`${url}&cursor=${encodeURIComponent(cursor)}`),
    second.env,
    second.dependencies,
  );
  expect(reused.status).toBe(400);
  await expect(reused.json()).resolves.toMatchObject({ error: { code: "INVALID_CURSOR" } });
});
