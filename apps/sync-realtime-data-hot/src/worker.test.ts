// Run with bun.
import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("./fetch-odds", () => ({
  fetchAndStoreOdds: vi.fn(async () => null),
}));

vi.mock("./odds-cache", () => ({
  OddsCacheHot: class {},
  writeCachedOdds: vi.fn(async () => undefined),
}));

import { fetchAndStoreOdds } from "./fetch-odds";
import worker, {
  buildOddsPayloadFromD1,
  handleFetchRequest,
  handleGetOdds,
  handleImportOddsChunk,
  handleQueue,
  handleScheduled,
  handleUpsertOddsFetchState,
  isAuthorizedInternalRequest,
  parseRaceKeyFromPath,
  processArchiveJob,
  processFetchOddsJob,
  runScheduledArchive,
  runScheduledPlan,
} from "./worker";
import type { Env, Job } from "./types";

interface CacheMockHandle {
  match: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
  delete: ReturnType<typeof vi.fn>;
}

let cacheMock: CacheMockHandle;

beforeEach(() => {
  cacheMock = {
    delete: vi.fn(async () => true),
    match: vi.fn(async () => undefined),
    put: vi.fn(async () => undefined),
  };
  vi.stubGlobal("caches", { default: cacheMock });
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

const buildKv = (): KVNamespace =>
  ({
    delete: vi.fn(async () => undefined),
    get: vi.fn(async () => null),
    put: vi.fn(async () => undefined),
  }) as unknown as KVNamespace;

const buildR2 = (): R2Bucket =>
  ({
    delete: vi.fn(async () => undefined),
    get: vi.fn(async () => null),
    head: vi.fn(async () => null),
    list: vi.fn(async () => ({ objects: [] })),
    put: vi.fn(async () => ({})),
  }) as unknown as R2Bucket;

const buildQueue = (): Queue<Job> =>
  ({
    send: vi.fn(async () => undefined),
    sendBatch: vi.fn(async () => undefined),
  }) as unknown as Queue<Job>;

interface BuildDbOptions {
  latest?: { results: unknown[] };
  tansho?: { results: unknown[] };
  byType?: { results: unknown[] };
  upsertRun?: ReturnType<typeof vi.fn>;
  logRun?: ReturnType<typeof vi.fn>;
  archiveCandidates?: { results: unknown[] };
}

const buildDb = (options: BuildDbOptions = {}): D1Database => {
  const prepareMock = vi.fn((sql: string) => {
    const lowered = sql.toLowerCase();
    if (lowered.includes("from odds_snapshots") && lowered.includes("max(fetched_at)")) {
      const all = vi.fn(async () => options.latest ?? { results: [] });
      return { bind: vi.fn(() => ({ all })) };
    }
    if (lowered.includes("odds_type = 'tansho'")) {
      const all = vi.fn(async () => options.tansho ?? { results: [] });
      return { bind: vi.fn(() => ({ all })) };
    }
    if (lowered.includes("from odds_snapshots") && lowered.includes("group by")) {
      const all = vi.fn(async () => options.archiveCandidates ?? { results: [] });
      return { bind: vi.fn(() => ({ all })) };
    }
    if (lowered.includes("from odds_snapshots")) {
      const all = vi.fn(async () => options.byType ?? { results: [] });
      return { bind: vi.fn(() => ({ all })) };
    }
    if (lowered.includes("from odds_fetch_state")) {
      const all = vi.fn(async () => ({ results: [] }));
      return { bind: vi.fn(() => ({ all })) };
    }
    if (lowered.includes("insert into fetch_logs")) {
      const run = options.logRun ?? vi.fn(async () => ({ meta: { changes: 1 } }));
      return { bind: vi.fn(() => ({ run })) };
    }
    if (lowered.includes("insert into odds_fetch_state")) {
      const run = options.upsertRun ?? vi.fn(async () => ({ meta: { changes: 1 } }));
      return { bind: vi.fn(() => ({ run })) };
    }
    const run = vi.fn(async () => ({ meta: { changes: 1 } }));
    return { bind: vi.fn(() => ({ run })) };
  });
  return { prepare: prepareMock } as unknown as D1Database;
};

const buildEnv = (overrides: Partial<Env> = {}): Env =>
  ({
    ODDS_ARCHIVE: buildR2(),
    ODDS_HOT_KV: buildKv(),
    PC_KEIBA_VIEWER_INTERNAL_TOKEN: "secret",
    REALTIME_HOT_DB: buildDb(),
    REALTIME_HOT_JOBS: buildQueue(),
    ...overrides,
  }) as unknown as Env;

it("parseRaceKeyFromPath returns the decoded race key", () => {
  expect(parseRaceKeyFromPath("/api/odds/nar%3A20260528%3A42%3A01")).toBe("nar:20260528:42:01");
});

it("parseRaceKeyFromPath returns null for unmatched path", () => {
  expect(parseRaceKeyFromPath("/health")).toBeNull();
});

it("isAuthorizedInternalRequest returns false when env token is unset", () => {
  const env = buildEnv({ PC_KEIBA_VIEWER_INTERNAL_TOKEN: undefined });
  expect(
    isAuthorizedInternalRequest(
      new Request("https://x/", { headers: { "x-pc-keiba-internal-token": "secret" } }),
      env,
    ),
  ).toBe(false);
});

it("isAuthorizedInternalRequest returns true when header matches token", () => {
  const env = buildEnv();
  expect(
    isAuthorizedInternalRequest(
      new Request("https://x/", { headers: { "x-pc-keiba-internal-token": "secret" } }),
      env,
    ),
  ).toBe(true);
});

it("isAuthorizedInternalRequest returns false when header missing", () => {
  expect(isAuthorizedInternalRequest(new Request("https://x/"), buildEnv())).toBe(false);
});

it("buildOddsPayloadFromD1 returns empty payload when D1 has no rows", async () => {
  const payload = await buildOddsPayloadFromD1(buildEnv(), "nar:20260528:42:01");
  expect(payload).toStrictEqual({
    fetchedAt: null,
    history: [],
    historyByType: {},
    latest: {},
  });
});

it("buildOddsPayloadFromD1 returns payload with fetched data", async () => {
  const env = buildEnv({
    REALTIME_HOT_DB: buildDb({
      latest: {
        results: [
          {
            average_odds: null,
            combination: "01",
            fetched_at: "2026-05-28T10:00:00+09:00",
            max_odds: null,
            min_odds: null,
            odds: 2.5,
            odds_type: "tansho",
            rank: 1,
          },
        ],
      },
      tansho: {
        results: [
          {
            combination: "01",
            fetched_at: "2026-05-28T10:00:00+09:00",
            odds: 2.5,
            rank: 1,
          },
        ],
      },
    }),
  });
  const payload = await buildOddsPayloadFromD1(env, "nar:20260528:42:01");
  expect(payload.fetchedAt).toBe("2026-05-28T10:00:00+09:00");
});

it("handleGetOdds returns force-fresh payload directly from D1", async () => {
  const env = buildEnv();
  const response = await handleGetOdds(
    env,
    new Request("https://x/api/odds/nar:20260528:42:01?fresh=1"),
    "nar:20260528:42:01",
  );
  expect(response.status).toBe(200);
  expect(cacheMock.put).toHaveBeenCalled();
});

it("handleGetOdds returns the edge-cached response when present", async () => {
  const cached = new Response(JSON.stringify({ cached: true }), { status: 200 });
  cacheMock.match.mockResolvedValueOnce(cached);
  const response = await handleGetOdds(
    buildEnv(),
    new Request("https://x/api/odds/nar:20260528:42:01"),
    "nar:20260528:42:01",
  );
  expect(response).toBe(cached);
});

it("handleGetOdds returns the KV mirror payload when fresh", async () => {
  const env = buildEnv();
  const kvGet = env.ODDS_HOT_KV.get as unknown as ReturnType<typeof vi.fn>;
  kvGet.mockResolvedValueOnce(
    JSON.stringify({
      fetchedAt: new Date().toISOString(),
      latest: { tansho: [{ combination: "01" }] },
    }),
  );
  const response = await handleGetOdds(
    env,
    new Request("https://x/api/odds/nar:20260528:42:01"),
    "nar:20260528:42:01",
  );
  expect(response.status).toBe(200);
  expect(cacheMock.put).toHaveBeenCalled();
});

it("handleGetOdds returns the D1 result cache payload when present", async () => {
  cacheMock.match.mockResolvedValueOnce(undefined);
  cacheMock.match.mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        fetchedAt: "2026-05-28T10:00:00+09:00",
        history: [],
        historyByType: {},
        latest: {},
      }),
    ),
  );
  const response = await handleGetOdds(
    buildEnv(),
    new Request("https://x/api/odds/nar:20260528:42:01"),
    "nar:20260528:42:01",
  );
  expect(response.status).toBe(200);
});

it("handleGetOdds falls back to D1 query when no cache layer hits", async () => {
  const response = await handleGetOdds(
    buildEnv(),
    new Request("https://x/api/odds/nar:20260528:42:01"),
    "nar:20260528:42:01",
  );
  expect(response.status).toBe(200);
  expect(cacheMock.put).toHaveBeenCalled();
});

it("handleUpsertOddsFetchState returns 401 when unauthorized", async () => {
  const response = await handleUpsertOddsFetchState(
    buildEnv(),
    new Request("https://x/api/internal/odds-fetch-state", {
      body: "{}",
      method: "POST",
    }),
  );
  expect(response.status).toBe(401);
});

it("handleUpsertOddsFetchState writes state and invalidates KV list", async () => {
  const env = buildEnv();
  const response = await handleUpsertOddsFetchState(
    env,
    new Request("https://x/api/internal/odds-fetch-state", {
      body: JSON.stringify({
        debaUrl: "https://example.com",
        kaisaiNen: "2026",
        kaisaiTsukihi: "0528",
        keibajoCode: "42",
        oddsLinksJson: "{}",
        raceBango: "01",
        raceKey: "nar:20260528:42:01",
        raceStartAtJst: "2026-05-28T10:00:00+09:00",
        source: "nar",
      }),
      headers: { "x-pc-keiba-internal-token": "secret" },
      method: "POST",
    }),
  );
  expect(response.status).toBe(200);
  expect(vi.mocked(env.ODDS_HOT_KV.delete)).toHaveBeenCalledWith("odds:race-list:v1:nar:20260528");
});

it("handleImportOddsChunk returns 401 when unauthorized", async () => {
  const response = await handleImportOddsChunk(
    buildEnv(),
    new Request("https://x/api/internal/import-odds-chunk", { method: "POST" }),
  );
  expect(response.status).toBe(401);
});

it("handleImportOddsChunk returns 202 placeholder when authorized", async () => {
  const response = await handleImportOddsChunk(
    buildEnv(),
    new Request("https://x/api/internal/import-odds-chunk", {
      headers: { "x-pc-keiba-internal-token": "secret" },
      method: "POST",
    }),
  );
  expect(response.status).toBe(202);
});

it("handleFetchRequest returns health payload at root", async () => {
  const response = await handleFetchRequest(buildEnv(), new Request("https://x/"));
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({ name: "sync-realtime-data-hot", ok: true });
});

it("handleFetchRequest routes upsert endpoint", async () => {
  const response = await handleFetchRequest(
    buildEnv(),
    new Request("https://x/api/internal/odds-fetch-state", { method: "POST" }),
  );
  expect(response.status).toBe(401);
});

it("handleFetchRequest routes import endpoint", async () => {
  const response = await handleFetchRequest(
    buildEnv(),
    new Request("https://x/api/internal/import-odds-chunk", { method: "POST" }),
  );
  expect(response.status).toBe(401);
});

it("handleFetchRequest routes GET /api/odds/:raceKey", async () => {
  const response = await handleFetchRequest(
    buildEnv(),
    new Request("https://x/api/odds/nar:20260528:42:01?fresh=1"),
  );
  expect(response.status).toBe(200);
});

it("handleFetchRequest returns 404 for unknown path", async () => {
  const response = await handleFetchRequest(buildEnv(), new Request("https://x/unknown"));
  expect(response.status).toBe(404);
});

it("runScheduledPlan returns early outside polling window", async () => {
  const env = buildEnv();
  await runScheduledPlan(env, new Date("2026-05-28T13:00:00Z"));
  expect(vi.mocked(env.REALTIME_HOT_JOBS.send)).not.toHaveBeenCalled();
});

it("runScheduledPlan calls planOddsFetches when inside polling window", async () => {
  const env = buildEnv();
  await runScheduledPlan(env, new Date("2026-05-28T01:00:00Z"));
  expect(vi.mocked(env.REALTIME_HOT_DB.prepare)).toHaveBeenCalled();
});

it("runScheduledArchive lists candidates and pushes to R2", async () => {
  const env = buildEnv({
    REALTIME_HOT_DB: buildDb({
      archiveCandidates: {
        results: [
          {
            fetched_at: "2026-05-20T10:00:00+09:00",
            odds_type: "tansho",
            race_key: "nar:20260520:42:01",
            snapshot_json: "[]",
          },
        ],
      },
    }),
  });
  await runScheduledArchive(env, new Date("2026-05-28T13:00:00Z"));
  expect(vi.mocked(env.ODDS_ARCHIVE.put)).toHaveBeenCalledTimes(1);
});

it("handleScheduled dispatches plan cron to runScheduledPlan", async () => {
  const env = buildEnv();
  await handleScheduled(
    {
      cron: "* * * * *",
      scheduledTime: new Date("2026-05-28T01:00:00Z").getTime(),
      type: "scheduled",
    } as unknown as ScheduledEvent,
    env,
  );
  expect(vi.mocked(env.REALTIME_HOT_DB.prepare)).toHaveBeenCalled();
});

it("handleScheduled dispatches archive cron to runScheduledArchive", async () => {
  const env = buildEnv();
  await handleScheduled(
    {
      cron: "0 4 * * *",
      scheduledTime: new Date("2026-05-28T04:00:00Z").getTime(),
      type: "scheduled",
    } as unknown as ScheduledEvent,
    env,
  );
  expect(vi.mocked(env.REALTIME_HOT_DB.prepare)).toHaveBeenCalled();
});

it("handleScheduled does nothing for unknown cron", async () => {
  const env = buildEnv();
  await handleScheduled(
    {
      cron: "*/5 * * * *",
      scheduledTime: new Date().getTime(),
      type: "scheduled",
    } as unknown as ScheduledEvent,
    env,
  );
  expect(vi.mocked(env.REALTIME_HOT_DB.prepare)).not.toHaveBeenCalled();
});

it("processFetchOddsJob returns early when fetchAndStoreOdds yields null", async () => {
  vi.mocked(fetchAndStoreOdds).mockResolvedValueOnce(null);
  const env = buildEnv();
  await processFetchOddsJob(env, "nar:20260528:42:01");
  expect(cacheMock.delete).not.toHaveBeenCalled();
  expect(vi.mocked(env.ODDS_HOT_KV.put)).not.toHaveBeenCalled();
});

it("processFetchOddsJob fans out cache writes on success (NAR)", async () => {
  vi.mocked(fetchAndStoreOdds).mockResolvedValueOnce({
    fetchedAt: "2026-05-28T10:00:00+09:00",
    inserted: 11,
    latest: { tansho: [{ combination: "01", odds: 2.5 }] },
  });
  const env = buildEnv();
  await processFetchOddsJob(env, "nar:20260528:42:01");
  expect(cacheMock.delete).toHaveBeenCalled();
  expect(vi.mocked(env.ODDS_HOT_KV.put)).toHaveBeenCalled();
});

it("processFetchOddsJob detects JRA source prefix on success", async () => {
  vi.mocked(fetchAndStoreOdds).mockResolvedValueOnce({
    fetchedAt: "2026-05-28T10:00:00+09:00",
    inserted: 11,
    latest: { tansho: [{ combination: "01", odds: 2.5 }] },
  });
  const env = buildEnv();
  await processFetchOddsJob(env, "jra:20260528:08:01");
  expect(cacheMock.delete).toHaveBeenCalled();
});

it("processArchiveJob delegates to runScheduledArchive", async () => {
  const env = buildEnv();
  await processArchiveJob(env, new Date("2026-05-28T13:00:00Z"));
  expect(vi.mocked(env.REALTIME_HOT_DB.prepare)).toHaveBeenCalled();
});

it("handleQueue acks fetch-odds messages", async () => {
  const env = buildEnv();
  const ack = vi.fn();
  const retry = vi.fn();
  const batch = {
    messages: [{ ack, body: { raceKey: "nar:20260528:42:01", type: "fetch-odds" }, retry }],
  } as unknown as MessageBatch<Job>;
  await handleQueue(batch, env);
  expect(ack).toHaveBeenCalledTimes(1);
});

it("handleQueue acks archive-odds-to-r2 messages", async () => {
  const env = buildEnv();
  const ack = vi.fn();
  const retry = vi.fn();
  const batch = {
    messages: [{ ack, body: { date: "20260528", type: "archive-odds-to-r2" }, retry }],
  } as unknown as MessageBatch<Job>;
  await handleQueue(batch, env);
  expect(ack).toHaveBeenCalledTimes(1);
});

it("handleQueue acks unknown jobs without retry", async () => {
  const env = buildEnv();
  const ack = vi.fn();
  const retry = vi.fn();
  const batch = {
    messages: [{ ack, body: { date: "20260528", type: "plan-odds-fetches" }, retry }],
  } as unknown as MessageBatch<Job>;
  await handleQueue(batch, env);
  expect(ack).toHaveBeenCalledTimes(1);
  expect(retry).not.toHaveBeenCalled();
});

it("handleQueue retries on error", async () => {
  vi.mocked(fetchAndStoreOdds).mockRejectedValueOnce(new Error("scrape failed"));
  const env = buildEnv();
  const ack = vi.fn();
  const retry = vi.fn();
  const batch = {
    messages: [{ ack, body: { raceKey: "nar:20260528:42:01", type: "fetch-odds" }, retry }],
  } as unknown as MessageBatch<Job>;
  await handleQueue(batch, env);
  expect(retry).toHaveBeenCalledTimes(1);
});

it("default worker fetch dispatches to handleFetchRequest", async () => {
  const response = await worker.fetch(new Request("https://x/"), buildEnv());
  expect(response.status).toBe(200);
});

it("default worker scheduled dispatches to handleScheduled", async () => {
  const env = buildEnv();
  await worker.scheduled(
    {
      cron: "* * * * *",
      scheduledTime: new Date("2026-05-28T01:00:00Z").getTime(),
      type: "scheduled",
    } as unknown as ScheduledEvent,
    env,
  );
  expect(vi.mocked(env.REALTIME_HOT_DB.prepare)).toHaveBeenCalled();
});

it("default worker queue dispatches to handleQueue", async () => {
  const env = buildEnv();
  const ack = vi.fn();
  const retry = vi.fn();
  const batch = {
    messages: [{ ack, body: { raceKey: "nar:20260528:42:01", type: "fetch-odds" }, retry }],
  } as unknown as MessageBatch<Job>;
  await worker.queue(batch, env);
  expect(ack).toHaveBeenCalled();
});
