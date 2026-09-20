// Runs with bun via Vitest; no network or storage calls escape these mocks.
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { mockDeep } from "vitest-mock-extended";
import gateway from "./vector-gateway";
import { D1AuditInputError } from "./d1-audit";

const mocks = vi.hoisted(() => ({
  query: vi.fn(),
  upsert: vi.fn(),
  catalog: vi.fn(),
  deadLetters: vi.fn<typeof import("./ingestion-dead-letter").handleIngestionDeadLetters>(),
  monitor: vi.fn<typeof import("./ingestion-monitor").monitorIngestionArchives>(),
  status: vi.fn<typeof import("./ingestion-monitor").readIngestionMonitorStatus>(),
  audit: vi.fn<typeof import("./d1-audit").queryD1Audit>(),
  sql: vi.fn<typeof import("./r2-sql").executeR2Sql>(),
}));
vi.mock("./d1-audit", async () => ({
  ...(await vi.importActual<typeof import("./d1-audit")>("./d1-audit")),
  queryD1Audit: mocks.audit,
}));
vi.mock("./r2-sql", () => ({ executeR2Sql: mocks.sql }));
vi.mock("./vector-search", () => ({
  queryHistoricalVectors: mocks.query,
  upsertHistoricalVectors: mocks.upsert,
}));
vi.mock("./worker", () => ({ default: { fetch: mocks.catalog } }));
vi.mock("./ingestion-dead-letter", () => ({ handleIngestionDeadLetters: mocks.deadLetters }));
vi.mock("./ingestion-monitor", () => ({
  INGESTION_MONITOR_CRON: "2-57/5 * * * *",
  monitorIngestionArchives: mocks.monitor,
  readIngestionMonitorStatus: mocks.status,
}));

const env: Parameters<typeof gateway.fetch>[1] = {
  ADMIN_TOKEN: "status-secret",
  INGESTION_ARCHIVE_JOURNAL: mockDeep<CatalogBindings["INGESTION_ARCHIVE_JOURNAL"]>(),
  VECTOR_ADMIN_TOKEN: "test-secret",
  D1_AUDIT_TOKEN: "audit-secret",
  CATALOG_KV: { get: vi.fn(), put: vi.fn(), delete: vi.fn() },
  CORNER_VECTORS: {
    query: vi.fn(),
    upsert: vi.fn(),
    insert: vi.fn(),
    describe: vi.fn(),
    deleteByIds: vi.fn(),
    getByIds: vi.fn(),
  },
  R2_SQL_ACCOUNT_ID: "account",
  R2_SQL_BUCKET_NAME: "catalog",
  R2_SQL_NAMESPACE: "pc_keiba",
  R2_SQL_TOKEN: "secret",
};
test.each([undefined, "wrong", "audit-secret", "test-secret"])(
  "status authorization precedes all binding I/O: %s",
  async (token) => {
    const response = await gateway.fetch(
      new Request("https://example.test/v1/internal/ingestion/status", {
        headers: token === undefined ? {} : { Authorization: `Bearer ${token}` },
      }),
      env,
    );
    expect(response.status).toBe(401);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(mocks.status).not.toHaveBeenCalled();
    expect(mocks.catalog).not.toHaveBeenCalled();
  },
);
test.each([undefined, "wrong", "status-secret", "test-secret"])(
  "calendar authentication precedes provider I/O: %s",
  async (token) => {
    const response = await gateway.fetch(
      new Request("https://example.test/v1/internal/race-calendar?year=2026", {
        headers: token === undefined ? {} : { Authorization: `Bearer ${token}` },
      }),
      env,
    );
    expect(response.status).toBe(401);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(mocks.sql).not.toHaveBeenCalled();
    expect(mocks.catalog).not.toHaveBeenCalled();
  },
);
test("reads the bounded calendar using the existing audit credential", async () => {
  mocks.sql.mockResolvedValueOnce([
    { kaisai_nen: "2026", kaisai_tsukihi: "0917", jra_count: 0, nar_count: 24 },
  ]);
  const response = await gateway.fetch(
    new Request("https://example.test/v1/internal/race-calendar?year=2026", {
      headers: { Authorization: "Bearer audit-secret" },
    }),
    env,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({
    days: [{ year: "2026", month: "09", day: "17", jraCount: 0, narCount: 24 }],
  });
  expect(mocks.sql).toHaveBeenCalledTimes(1);
  expect(mocks.catalog).not.toHaveBeenCalled();
});
test("does not accept calendar mutations from authenticated auditors", async () => {
  const response = await gateway.fetch(
    new Request("https://example.test/v1/internal/race-calendar?year=2026", {
      method: "POST",
      headers: { Authorization: "Bearer audit-secret" },
      body: "{}",
    }),
    env,
  );
  expect(response.status).toBe(405);
  expect(mocks.sql).not.toHaveBeenCalled();
});
test("calendar auditors cannot choose SQL or a namespace", async () => {
  const response = await gateway.fetch(
    new Request("https://example.test/v1/internal/race-calendar?year=2026&namespace=other", {
      headers: { Authorization: "Bearer audit-secret" },
    }),
    env,
  );
  expect(response.status).toBe(400);
  expect(mocks.sql).not.toHaveBeenCalled();
});
test.each([undefined, "wrong", "status-secret", "test-secret"])(
  "year summaries require the audit credential before provider I/O: %s",
  async (token) => {
    const response = await gateway.fetch(
      new Request("https://example.test/v1/internal/race-years", {
        headers: token === undefined ? {} : { Authorization: `Bearer ${token}` },
      }),
      env,
    );
    expect(response.status).toBe(401);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(mocks.sql).not.toHaveBeenCalled();
    expect(mocks.catalog).not.toHaveBeenCalled();
  },
);
test("reads year summaries using the existing audit credential", async () => {
  mocks.sql.mockResolvedValueOnce([{ year: "2026", race_count: 500, day_count: 260 }]);
  const response = await gateway.fetch(
    new Request("https://example.test/v1/internal/race-years", {
      headers: { Authorization: "Bearer audit-secret" },
    }),
    env,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({
    years: [{ year: "2026", raceCount: 500, dayCount: 260 }],
  });
  expect(mocks.sql).toHaveBeenCalledTimes(1);
  expect(mocks.catalog).not.toHaveBeenCalled();
});
test("rejects year-summary mutation by an authenticated auditor", async () => {
  const response = await gateway.fetch(
    new Request("https://example.test/v1/internal/race-years", {
      method: "POST",
      headers: { Authorization: "Bearer audit-secret" },
      body: "{}",
    }),
    env,
  );
  expect(response.status).toBe(405);
  expect(mocks.sql).not.toHaveBeenCalled();
});
test("year-summary auditors cannot choose SQL or namespace", async () => {
  const response = await gateway.fetch(
    new Request("https://example.test/v1/internal/race-years?namespace=other&sql=SELECT%201", {
      headers: { Authorization: "Bearer audit-secret" },
    }),
    env,
  );
  expect(response.status).toBe(400);
  expect(mocks.sql).not.toHaveBeenCalled();
});
test.each([undefined, "wrong", "status-secret", "test-secret"])(
  "day-list audit authentication precedes provider I/O: %s",
  async (token) => {
    const response: Response = await gateway.fetch(
      new Request("https://example.test/v1/internal/race-day-list?date=20240229", {
        headers: token === undefined ? {} : { Authorization: `Bearer ${token}` },
      }),
      env,
    );
    expect(response.status).toBe(401);
    expect(mocks.sql).not.toHaveBeenCalled();
    expect(mocks.catalog).not.toHaveBeenCalled();
  },
);
test("auditors can read genuine empty day lists", async () => {
  mocks.sql.mockResolvedValueOnce([]);
  const response: Response = await gateway.fetch(
    new Request("https://example.test/v1/internal/race-day-list?date=20240229", {
      headers: { Authorization: "Bearer audit-secret" },
    }),
    env,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ races: [] });
  expect(mocks.sql).toHaveBeenCalledTimes(1);
  expect(mocks.catalog).not.toHaveBeenCalled();
});
test("day-list auditors cannot mutate data", async () => {
  const response: Response = await gateway.fetch(
    new Request("https://example.test/v1/internal/race-day-list?date=20240229", {
      method: "POST",
      headers: { Authorization: "Bearer audit-secret" },
      body: "{}",
    }),
    env,
  );
  expect(response.status).toBe(405);
  expect(mocks.sql).not.toHaveBeenCalled();
});
test("day-list auditors cannot select namespace or SQL", async () => {
  const response: Response = await gateway.fetch(
    new Request(
      "https://example.test/v1/internal/race-day-list?date=20240229&namespace=other&sql=SELECT%201",
      {
        headers: { Authorization: "Bearer audit-secret" },
      },
    ),
    env,
  );
  expect(response.status).toBe(400);
  expect(mocks.sql).not.toHaveBeenCalled();
});
test.each([undefined, "wrong", "status-secret", "test-secret"])(
  "jockey-list audit rejects unrelated credentials: %s",
  async (token) => {
    const response: Response = await gateway.fetch(
      new Request("https://example.test/v1/internal/race-day-list-with-jockeys?date=20240229", {
        headers: token === undefined ? {} : { Authorization: `Bearer ${token}` },
      }),
      env,
    );
    expect(response.status).toBe(401);
    expect(mocks.sql).not.toHaveBeenCalled();
    expect(mocks.catalog).not.toHaveBeenCalled();
  },
);
test("jockey-list auditors get one read-only query", async () => {
  mocks.sql.mockResolvedValueOnce([]);
  const response: Response = await gateway.fetch(
    new Request("https://example.test/v1/internal/race-day-list-with-jockeys?date=20240229", {
      headers: { Authorization: "Bearer audit-secret" },
    }),
    env,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ races: [] });
  expect(mocks.sql).toHaveBeenCalledTimes(1);
  expect(mocks.catalog).not.toHaveBeenCalled();
});
test("jockey-list auditors cannot mutate", async () => {
  const response: Response = await gateway.fetch(
    new Request("https://example.test/v1/internal/race-day-list-with-jockeys?date=20240229", {
      method: "POST",
      headers: { Authorization: "Bearer audit-secret" },
    }),
    env,
  );
  expect(response.status).toBe(405);
  expect(mocks.sql).not.toHaveBeenCalled();
});
test("jockey-list auditors cannot choose SQL or namespace", async () => {
  const response: Response = await gateway.fetch(
    new Request(
      "https://example.test/v1/internal/race-day-list-with-jockeys?date=20240229&namespace=other&sql=SELECT%201",
      {
        headers: { Authorization: "Bearer audit-secret" },
      },
    ),
    env,
  );
  expect(response.status).toBe(400);
  expect(mocks.sql).not.toHaveBeenCalled();
});
test("the default entrypoint does not expose the named-binding jockey path", async () => {
  const actual = await vi.importActual<typeof import("./worker")>("./worker");
  const network = vi.fn<typeof fetch>().mockRejectedValue(new Error("Unexpected network I/O"));
  vi.stubGlobal("fetch", network);
  mocks.catalog.mockImplementationOnce(actual.default.fetch);
  const response: Response = await gateway.fetch(
    new Request("https://example.test/v1/race-day-list-with-jockeys?date=20240229"),
    env,
  );
  expect(response.status).toBe(404);
  expect(mocks.catalog).toHaveBeenCalledTimes(1);
  expect(mocks.sql).not.toHaveBeenCalled();
  expect(network).not.toHaveBeenCalled();
  expect(await response.json()).toStrictEqual({ error: "not_found" });
});
test("status is disabled when the administrator token is absent", async () => {
  const response = await gateway.fetch(
    new Request("https://example.test/v1/internal/ingestion/status"),
    { ...env, ADMIN_TOKEN: undefined },
  );
  expect(response.status).toBe(401);
  expect(mocks.status).not.toHaveBeenCalled();
});
test("status rejects mutations before reading the journal", async () => {
  const response = await gateway.fetch(
    new Request("https://example.test/v1/internal/ingestion/status", {
      method: "POST",
      headers: { Authorization: "Bearer status-secret" },
    }),
    env,
  );
  expect(response.status).toBe(405);
  expect(mocks.status).not.toHaveBeenCalled();
});
test("status returns uncached diagnostic state without claiming coverage", async () => {
  mocks.status.mockResolvedValue({
    sampledAtMs: 2,
    lastObservedAtMs: 1,
    assessment: "unknown",
    oldestPendingArchiveAt: null,
    oldestPendingNotificationAtMs: 1,
    coverageVerified: false,
    notificationDeliveryVerified: false,
  });
  const response = await gateway.fetch(
    new Request("https://example.test/v1/internal/ingestion/status", {
      headers: { Authorization: "Bearer status-secret" },
    }),
    env,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({
    sampledAtMs: 2,
    lastObservedAtMs: 1,
    assessment: "unknown",
    oldestPendingArchiveAt: null,
    oldestPendingNotificationAtMs: 1,
    coverageVerified: false,
    notificationDeliveryVerified: false,
  });
  expect(mocks.catalog).not.toHaveBeenCalled();
});
test("status storage failures are sanitized and uncached", async () => {
  mocks.status.mockRejectedValue(new Error("private storage details"));
  const response = await gateway.fetch(
    new Request("https://example.test/v1/internal/ingestion/status", {
      headers: { Authorization: "Bearer status-secret" },
    }),
    env,
  );
  expect(response.status).toBe(503);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ error: "Ingestion monitor status unavailable" });
});

const validQuery = {
  namespace: "corner-v1",
  source: "jra",
  raceDate: "20260916",
  earliestDate: "20230916",
  topK: 80,
  values: [0, 0, 0, 0, 0, 0, 0, 0],
};

beforeEach(() => {
  vi.clearAllMocks();
  vi.stubGlobal("caches", { default: { match: vi.fn(), put: vi.fn(), delete: vi.fn() } });
  mocks.audit.mockReset().mockResolvedValue(Response.json({ audit: true }));
  mocks.sql.mockResolvedValue([]);
  mocks.query.mockResolvedValue([]);
  mocks.upsert.mockResolvedValue({ mutationId: "accepted-1" });
  mocks.catalog.mockResolvedValue(Response.json({ catalog: true }));
});

afterEach(() => {
  vi.unstubAllGlobals();
});

const auditRequest = (
  body: unknown = {
    operation: "manifest",
    snapshotId: "s",
    databaseName: "db",
    tableName: "events",
  },
): Request =>
  new Request("https://example.test/v1/internal/d1/audit", {
    method: "POST",
    headers: { Authorization: "Bearer audit-secret" },
    body: JSON.stringify(body),
  });

test("the dedicated monitor schedule does not invoke business Queue or HTTP handlers", async () => {
  vi.spyOn(console, "info").mockImplementation(() => undefined);
  const monitorEnv = mockDeep<CatalogBindings>();
  await gateway.scheduled(mockDeep<ScheduledController>({ cron: "2-57/5 * * * *" }), monitorEnv);
  expect(mocks.monitor).toHaveBeenCalledWith(monitorEnv);
  expect(mocks.deadLetters).not.toHaveBeenCalled();
  expect(mocks.catalog).not.toHaveBeenCalled();
});
test("unexpected schedules fail closed before monitoring I/O", async () => {
  await expect(
    gateway.scheduled(
      mockDeep<ScheduledController>({ cron: "* * * * *" }),
      mockDeep<CatalogBindings>(),
    ),
  ).rejects.toThrow("Unexpected ingestion monitor schedule");
  expect(mocks.monitor).not.toHaveBeenCalled();
});

test("Queue events use the isolated dead-letter archival handler", async () => {
  const batch = mockDeep<MessageBatch<unknown>>();
  const queueEnv = mockDeep<CatalogBindings>();
  await gateway.queue(batch, queueEnv);
  expect(mocks.deadLetters).toHaveBeenCalledWith(batch, queueEnv);
  expect(mocks.catalog).not.toHaveBeenCalled();
});

test.each(["accept", "archive/pending"])(
  "HTTP requests do not dispatch named ingestion RPC methods: %s",
  async (path) => {
    mocks.catalog.mockResolvedValue(new Response(null, { status: 404 }));
    const response = await gateway.fetch(
      new Request(`https://example.test/v1/internal/ingestion/${path}`, {
        method: "POST",
        body: "{}",
      }),
      env,
    );
    expect(response.status).toBe(404);
    expect(mocks.catalog).toHaveBeenCalledTimes(1);
    expect(mocks.upsert).not.toHaveBeenCalled();
    expect(mocks.audit).not.toHaveBeenCalled();
  },
);

test("audit requests require their own credential before I/O", async () => {
  const request = new Request("https://example.test/v1/internal/d1/audit", {
    headers: { Authorization: "Bearer test-secret" },
  });
  expect((await gateway.fetch(request, env)).status).toBe(401);
  expect((await gateway.fetch(auditRequest(), { ...env, D1_AUDIT_TOKEN: undefined })).status).toBe(
    401,
  );
  expect(mocks.audit).not.toHaveBeenCalled();
});

test("audit rejects wrong methods and malformed input", async () => {
  expect(
    (
      await gateway.fetch(
        new Request("https://example.test/v1/internal/d1/audit", {
          headers: { Authorization: "Bearer audit-secret" },
        }),
        env,
      )
    ).status,
  ).toBe(405);
  expect((await gateway.fetch(auditRequest([]), env)).status).toBe(400);
  expect((await gateway.fetch(auditRequest({ operation: "write" }), env)).status).toBe(400);
});

test("audit routes use the configured Catalog and cache scope", async () => {
  mocks.audit.mockImplementation(async (_input, dependencies) =>
    Response.json({ rows: await dependencies.query("SELECT 1"), scope: dependencies.cacheScope }),
  );
  expect(await (await gateway.fetch(auditRequest(), env)).json()).toStrictEqual({
    rows: [],
    scope: "account/catalog",
  });
  expect(mocks.sql).toHaveBeenCalledOnce();
});

test.each([new D1AuditInputError("bad column"), new Error("private provider details")])(
  "audit separates invalid requests from provider failures",
  async (error) => {
    mocks.audit.mockRejectedValue(error);
    const result = await gateway.fetch(auditRequest(), env);
    expect(result.status).toBe(error instanceof D1AuditInputError ? 400 : 503);
    expect(await result.text()).not.toContain("private provider details");
  },
);

test.each([undefined, "wrong", "status-secret", "test-secret"])(
  "race detail audit authenticates before provider I/O: %s",
  async (token) => {
    const response = await gateway.fetch(
      new Request("https://example.test/v1/internal/race-detail", {
        headers: token === undefined ? {} : { Authorization: `Bearer ${token}` },
      }),
      env,
    );
    expect(response.status).toBe(401);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(mocks.sql).not.toHaveBeenCalled();
    expect(mocks.catalog).not.toHaveBeenCalled();
  },
);
test("race detail audit rejects writes and missing configured credentials", async () => {
  const request = new Request("https://example.test/v1/internal/race-detail", {
    method: "POST",
    headers: { Authorization: "Bearer audit-secret" },
  });
  expect((await gateway.fetch(request, env)).status).toBe(405);
  expect((await gateway.fetch(request, { ...env, D1_AUDIT_TOKEN: undefined })).status).toBe(401);
  expect(mocks.sql).not.toHaveBeenCalled();
});
test.each([
  "",
  "source=jra",
  "source=jra&date=20260913",
  "source=jra&date=20260913&keibajoCode=06",
  "source=invalid&date=20260913&keibajoCode=06&raceBango=11",
  "source=jra&date=20260229&keibajoCode=06&raceBango=11",
  "source=jra&source=nar&date=20260913&keibajoCode=06&raceBango=11",
  "source=jra&date=20260913&keibajoCode=06&raceBango=11&sql=SELECT+1",
])("race detail audit rejects malformed parameters: %s", async (params) => {
  const response = await gateway.fetch(
    new Request(`https://example.test/v1/internal/race-detail?${params}`, {
      headers: { Authorization: "Bearer audit-secret" },
    }),
    env,
  );
  expect(response.status).toBe(400);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(mocks.sql).not.toHaveBeenCalled();
});
test("race detail audit returns authoritative absence uncached", async () => {
  const response = await gateway.fetch(
    new Request(
      "https://example.test/v1/internal/race-detail?source=nar&date=20260913&keibajoCode=44&raceBango=11",
      {
        headers: { Authorization: "Bearer audit-secret" },
      },
    ),
    env,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ row: null });
  expect(mocks.sql.mock.calls[0]?.[1]).toMatch(/FROM pc_keiba\.nvd_ra/u);
  expect(mocks.catalog).not.toHaveBeenCalled();
});
test("race detail audit returns the complete validated projection", async () => {
  mocks.sql.mockResolvedValueOnce([
    {
      kaisai_nen: "2026",
      kaisai_tsukihi: "0913",
      keibajo_code: "06",
      race_bango: "11",
      kaisai_kai: null,
      kaisai_nichime: null,
      kyosomei_hondai: " Race ",
      kyosomei_fukudai: "",
      kyosomei_kakkonai: null,
      grade_code: null,
      kyoso_shubetsu_code: null,
      kyoso_kigo_code: null,
      juryo_shubetsu_code: null,
      kyoso_joken_code: null,
      kyoso_joken_meisho: null,
      kyori: "1800",
      track_code: null,
      hasso_jikoku: null,
      toroku_tosu: null,
      shusso_tosu: null,
      tenko_code: null,
      babajotai_code_shiba: null,
      babajotai_code_dirt: null,
    },
  ]);
  const response = await gateway.fetch(
    new Request(
      "https://example.test/v1/internal/race-detail?source=jra&date=20260913&keibajoCode=06&raceBango=11",
      {
        headers: { Authorization: "Bearer audit-secret" },
      },
    ),
    env,
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toMatchObject({
    row: { source: "jra", kyosomeiHondai: " Race ", kyori: "1800" },
  });
  expect(mocks.sql).toHaveBeenCalledTimes(1);
});
test("race detail audit sanitizes failures without retrying or reporting absence", async () => {
  const errorLog = vi.spyOn(console, "error").mockImplementation(() => undefined);
  mocks.sql.mockRejectedValueOnce(new Error("private credentials"));
  const response = await gateway.fetch(
    new Request(
      "https://example.test/v1/internal/race-detail?source=jra&date=20260913&keibajoCode=06&raceBango=11",
      {
        headers: { Authorization: "Bearer audit-secret" },
      },
    ),
    env,
  );
  expect(response.status).toBe(503);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ error: "Catalog race detail unavailable" });
  expect(errorLog).toHaveBeenCalledWith('{"event":"race_detail_read_failed"}');
  expect(mocks.sql).toHaveBeenCalledTimes(1);
  expect(mocks.catalog).not.toHaveBeenCalled();
  errorLog.mockRestore();
});

test("the named race-detail service does not create a public HTTP route", async () => {
  mocks.catalog.mockResolvedValueOnce(new Response(null, { status: 404 }));
  const response: Response = await gateway.fetch(
    new Request(
      "https://example.test/v1/race-detail?source=jra&date=20260816&keibajoCode=A8&raceBango=04",
    ),
    env,
  );
  expect(response.status).toBe(404);
  expect(mocks.sql).not.toHaveBeenCalled();
  expect(mocks.catalog).toHaveBeenCalledTimes(1);
});

test("delegates non-vector requests to the unchanged Catalog worker", async () => {
  const response: Response = await gateway.fetch(new Request("https://example.test/health"), env);
  expect(await response.json()).toStrictEqual({ catalog: true });
  expect(mocks.catalog).toHaveBeenCalledTimes(1);
});

test.each([undefined, "Bearer wrong-token", "Bearer same-length", ""])(
  "rejects unauthenticated query before I/O: %s",
  async (authorization) => {
    const headers: Headers = new Headers();
    if (authorization !== undefined) headers.set("Authorization", authorization);
    const response: Response = await gateway.fetch(
      new Request("https://example.test/v1/internal/vectors/query", { headers }),
      env,
    );
    expect(response.status).toBe(401);
    expect(mocks.query).not.toHaveBeenCalled();
  },
);

test("fails closed when the admin secret is missing", async () => {
  const response: Response = await gateway.fetch(
    new Request("https://example.test/v1/internal/vectors/query"),
    { ...env, VECTOR_ADMIN_TOKEN: undefined },
  );
  expect(response.status).toBe(401);
});

test("requires POST even for authenticated requests", async () => {
  const response: Response = await gateway.fetch(
    new Request("https://example.test/v1/internal/vectors/query", {
      headers: { Authorization: "Bearer test-secret" },
    }),
    env,
  );
  expect(response.status).toBe(405);
});

test("routes filtered historical search to Vectorize, never Neon", async () => {
  const response: Response = await gateway.fetch(
    new Request("https://example.test/v1/internal/vectors/query", {
      method: "POST",
      headers: { Authorization: "Bearer test-secret" },
      body: JSON.stringify({
        ...validQuery,
        venue: "06",
        trackPrefix: "1",
        hasFinish: true,
        distanceMin: 1200,
        distanceMax: 2200,
      }),
    }),
    env,
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("Cache-Control")).toBe("no-store");
  expect(await response.json()).toStrictEqual({ neighbors: [] });
  expect(mocks.query.mock.calls[0]?.[1]).toStrictEqual({
    dimensions: 8,
    earliestDate: "20230916",
    filters: {
      venue: "06",
      trackPrefix: "1",
      hasFinish: true,
      distance: { $gte: 1200, $lte: 2200 },
    },
    namespace: "corner-v1",
    raceDate: "20260916",
    source: "jra",
    topK: 80,
    values: [0, 0, 0, 0, 0, 0, 0, 0],
  });
});

test("supports NAR venue exclusion without a track constraint", async () => {
  const response: Response = await gateway.fetch(
    new Request("https://example.test/v1/internal/vectors/query", {
      method: "POST",
      headers: { Authorization: "Bearer test-secret" },
      body: JSON.stringify({ ...validQuery, source: "nar", excludeVenue: "83" }),
    }),
    env,
  );
  expect(response.status).toBe(200);
  expect(mocks.query.mock.calls[0]?.[1].filters).toStrictEqual({ venue: { $ne: "83" } });
});

test("returns 202 for accepted upserts rather than claiming searchable completion", async () => {
  const response: Response = await gateway.fetch(
    new Request("https://example.test/v1/internal/vectors/upsert", {
      method: "POST",
      headers: { Authorization: "Bearer test-secret" },
      body: JSON.stringify({
        namespace: "corner-v1",
        vectors: [
          {
            id: "horse-1",
            namespace: "corner-v1",
            values: [0, 0, 0, 0, 0, 0, 0, 0],
            metadata: { source: "jra", raceDate: "20260915", distance: 1800, hasFinish: true },
          },
        ],
      }),
    }),
    env,
  );
  expect(response.status).toBe(202);
  expect(await response.json()).toStrictEqual({ mutationId: "accepted-1" });
  expect(mocks.upsert).toHaveBeenCalledTimes(1);
});

test.each([
  { source: "other" },
  { namespace: 1 },
  { topK: "80" },
  { values: "invalid" },
  { values: ["1"] },
  { venue: "06", excludeVenue: "83" },
  { hasFinish: 1 },
  { distanceMin: 1000 },
  { distanceMax: 1000 },
  { distanceMin: 2000, distanceMax: 1000 },
])("rejects malformed query fields: %j", async (overrides) => {
  const response: Response = await gateway.fetch(
    new Request("https://example.test/v1/internal/vectors/query", {
      method: "POST",
      headers: { Authorization: "Bearer test-secret" },
      body: JSON.stringify({ ...validQuery, ...overrides }),
    }),
    env,
  );
  expect(response.status).toBe(400);
  expect(mocks.query).not.toHaveBeenCalled();
});

test.each(["null", "[]", "{", "x".repeat(1024 * 1024 + 1)])(
  "rejects malformed or oversized bodies",
  async (body) => {
    const response: Response = await gateway.fetch(
      new Request("https://example.test/v1/internal/vectors/query", {
        method: "POST",
        headers: { Authorization: "Bearer test-secret" },
        body,
      }),
      env,
    );
    expect(response.status).toBe(400);
  },
);

test("rejects an absent body", async () => {
  const response: Response = await gateway.fetch(
    new Request("https://example.test/v1/internal/vectors/query", {
      method: "POST",
      headers: { Authorization: "Bearer test-secret" },
    }),
    env,
  );
  expect(response.status).toBe(400);
});

test.each([
  { vectors: "bad" },
  { vectors: [null] },
  { vectors: [{ id: "1", namespace: "corner-v1", values: [0], metadata: null }] },
  { vectors: [{ id: "1", namespace: "corner-v1", values: [0], metadata: { nested: {} } }] },
])("rejects malformed upsert fields: %j", async (body) => {
  const response: Response = await gateway.fetch(
    new Request("https://example.test/v1/internal/vectors/upsert", {
      method: "POST",
      headers: { Authorization: "Bearer test-secret" },
      body: JSON.stringify({ namespace: "corner-v1", ...body }),
    }),
    env,
  );
  expect(response.status).toBe(400);
  expect(mocks.upsert).not.toHaveBeenCalled();
});

test("does not leak provider errors or retry through Neon", async () => {
  mocks.query.mockRejectedValueOnce(new Error("private provider payload"));
  const response: Response = await gateway.fetch(
    new Request("https://example.test/v1/internal/vectors/query", {
      method: "POST",
      headers: { Authorization: "Bearer test-secret" },
      body: JSON.stringify(validQuery),
    }),
    env,
  );
  expect(response.status).toBe(503);
  expect(await response.json()).toStrictEqual({ error: "Vector operation unavailable or invalid" });
});
