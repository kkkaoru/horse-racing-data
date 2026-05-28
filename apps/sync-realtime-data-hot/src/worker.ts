import { fetchAndStoreOdds } from "./fetch-odds";
import { formatError } from "./format-error";
import {
  isForceFreshRequest,
  purgeD1ResultCacheForRace,
  purgeEdgeCache,
  readD1ResultCache,
  readFromEdgeCache,
  writeD1ResultCache,
  writeToEdgeCache,
} from "./gates/edge-cache";
import { readLatestOddsFromKv, writeLatestOddsToKv } from "./gates/latest-odds-kv-mirror";
import { computeArchiveCutoffIso, putArchiveRowToR2 } from "./gates/r2-archive";
import { invalidateRaceListInKv, patchLastFetchInKv } from "./gates/race-list-kv-cache";
import { shouldRunOddsCron } from "./gates/polling-window-gate";
import { jsonResponse } from "./http";
import { writeCachedOdds } from "./odds-cache";
import { planOddsFetches } from "./plan";
import {
  bulkInsertOddsSnapshotRows,
  getLatestOddsFromD1,
  listArchiveCandidatesBeforeCutoff,
  listOddsHistoryByType,
  listTanshoHistory,
  logFetch,
  toHorseTrends,
  toOddsTrendsByType,
  upsertOddsFetchState,
  type ImportOddsSnapshotRow,
} from "./storage";
import { getTodayJst } from "./time";
import type { Env, Job, OddsData, OddsType, OddsFetchStateUpsertInput } from "./types";

const PLAN_ODDS_FETCHES_CRON = "* * * * *";
const ARCHIVE_ODDS_CRON = "0 4 * * *";
const ARCHIVE_QUERY_LIMIT = 200;
const D1_RESULT_CACHE_QUERIES = ["latest", "tanshoHistory", "oddsHistoryByType"];

interface OddsPayload {
  fetchedAt: string | null;
  history: ReturnType<typeof toHorseTrends>;
  historyByType: ReturnType<typeof toOddsTrendsByType>;
  latest: Partial<Record<OddsType, OddsData[]>>;
}

export const parseRaceKeyFromPath = (pathname: string): string | null => {
  const match = pathname.match(/^\/api\/odds\/(.+)$/);
  return match ? decodeURIComponent(match[1]!) : null;
};

export const buildOddsPayloadFromD1 = async (env: Env, raceKey: string): Promise<OddsPayload> => {
  const [latest, tansho, byType] = await Promise.all([
    getLatestOddsFromD1(env.REALTIME_HOT_DB, raceKey),
    listTanshoHistory(env.REALTIME_HOT_DB, raceKey),
    listOddsHistoryByType(env.REALTIME_HOT_DB, raceKey),
  ]);
  return {
    fetchedAt: latest?.fetchedAt ?? null,
    history: toHorseTrends(tansho),
    historyByType: toOddsTrendsByType(byType),
    latest: latest?.latest ?? {},
  };
};

export const handleGetOdds = async (
  env: Env,
  request: Request,
  raceKey: string,
): Promise<Response> => {
  if (isForceFreshRequest(request)) {
    const payload = await buildOddsPayloadFromD1(env, raceKey);
    await writeD1ResultCache(raceKey, "payload", payload, env);
    return jsonResponse(payload);
  }
  const cached = await readFromEdgeCache(raceKey);
  if (cached) {
    return cached;
  }
  const mirrored = await readLatestOddsFromKv(env, raceKey, {
    allowStale: false,
    now: new Date(),
  });
  if (mirrored) {
    const payload: OddsPayload = {
      fetchedAt: mirrored.fetchedAt,
      history: [],
      historyByType: {},
      latest: mirrored.latest,
    };
    await writeToEdgeCache(raceKey, payload, env);
    return jsonResponse(payload);
  }
  const d1Cached = await readD1ResultCache<OddsPayload>(raceKey, "payload");
  if (d1Cached) {
    await writeToEdgeCache(raceKey, d1Cached, env);
    return jsonResponse(d1Cached);
  }
  const payload = await buildOddsPayloadFromD1(env, raceKey);
  await writeD1ResultCache(raceKey, "payload", payload, env);
  await writeToEdgeCache(raceKey, payload, env);
  return jsonResponse(payload);
};

export const isAuthorizedInternalRequest = (request: Request, env: Env): boolean => {
  const token = env.PC_KEIBA_VIEWER_INTERNAL_TOKEN;
  if (!token) {
    return false;
  }
  return request.headers.get("x-pc-keiba-internal-token") === token;
};

export const handleUpsertOddsFetchState = async (env: Env, request: Request): Promise<Response> => {
  if (!isAuthorizedInternalRequest(request, env)) {
    return jsonResponse({ error: "unauthorized" }, { status: 401 });
  }
  const body = (await request.json()) as OddsFetchStateUpsertInput;
  await upsertOddsFetchState(env.REALTIME_HOT_DB, body);
  await invalidateRaceListInKv(env, body.source, `${body.kaisaiNen}${body.kaisaiTsukihi}`);
  return jsonResponse({ ok: true });
};

interface ImportOddsChunkRequest {
  rows: ImportOddsSnapshotRow[];
}

export const handleImportOddsChunk = async (env: Env, request: Request): Promise<Response> => {
  if (!isAuthorizedInternalRequest(request, env)) {
    return jsonResponse({ error: "unauthorized" }, { status: 401 });
  }
  const body = (await request.json()) as ImportOddsChunkRequest;
  const inserted = await bulkInsertOddsSnapshotRows(env.REALTIME_HOT_DB, body.rows ?? []);
  return jsonResponse({ inserted });
};

interface MigrationStateRequest {
  key: string;
  value: string;
}

export const handleMigrationState = async (env: Env, request: Request): Promise<Response> => {
  if (!isAuthorizedInternalRequest(request, env)) {
    return jsonResponse({ error: "unauthorized" }, { status: 401 });
  }
  const body = (await request.json()) as MigrationStateRequest;
  await env.ODDS_HOT_KV.put(`odds:migration:${body.key}`, body.value);
  return jsonResponse({ ok: true });
};

export const handleGetMigrationState = async (env: Env, request: Request): Promise<Response> => {
  if (!isAuthorizedInternalRequest(request, env)) {
    return jsonResponse({ error: "unauthorized" }, { status: 401 });
  }
  const url = new URL(request.url);
  const key = url.searchParams.get("key");
  if (!key) {
    return jsonResponse({ error: "key is required" }, { status: 400 });
  }
  const value = await env.ODDS_HOT_KV.get(`odds:migration:${key}`);
  return jsonResponse({ key, value });
};

export const handleFetchRequest = async (env: Env, request: Request): Promise<Response> => {
  const url = new URL(request.url);
  if (url.pathname === "/") {
    return jsonResponse({ name: "sync-realtime-data-hot", ok: true });
  }
  if (request.method === "POST" && url.pathname === "/api/internal/odds-fetch-state") {
    return handleUpsertOddsFetchState(env, request);
  }
  if (request.method === "POST" && url.pathname === "/api/internal/import-odds-chunk") {
    return handleImportOddsChunk(env, request);
  }
  if (request.method === "POST" && url.pathname === "/api/internal/migration-state") {
    return handleMigrationState(env, request);
  }
  if (request.method === "GET" && url.pathname === "/api/internal/migration-state") {
    return handleGetMigrationState(env, request);
  }
  const raceKey = parseRaceKeyFromPath(url.pathname);
  if (request.method === "GET" && raceKey) {
    return handleGetOdds(env, request, raceKey);
  }
  return jsonResponse({ error: "not found" }, { status: 404 });
};

export const runScheduledPlan = async (env: Env, now: Date): Promise<void> => {
  if (!shouldRunOddsCron(now)) {
    return;
  }
  await planOddsFetches(env, now, getTodayJst(now));
};

export const runScheduledArchive = async (env: Env, now: Date): Promise<void> => {
  const cutoff = computeArchiveCutoffIso(env, now);
  const candidates = await listArchiveCandidatesBeforeCutoff(env.REALTIME_HOT_DB, {
    cutoffIso: cutoff,
    limit: ARCHIVE_QUERY_LIMIT,
  });
  await Promise.all(
    candidates.map((row) =>
      putArchiveRowToR2(env, {
        fetchedAt: row.fetched_at,
        oddsType: row.odds_type,
        raceKey: row.race_key,
        snapshotJson: row.snapshot_json,
      }),
    ),
  );
};

export const handleScheduled = async (event: ScheduledEvent, env: Env): Promise<void> => {
  const now = new Date(event.scheduledTime);
  if (event.cron === PLAN_ODDS_FETCHES_CRON) {
    await runScheduledPlan(env, now);
    return;
  }
  if (event.cron === ARCHIVE_ODDS_CRON) {
    await runScheduledArchive(env, now);
  }
};

export const processFetchOddsJob = async (env: Env, raceKey: string): Promise<void> => {
  const result = await fetchAndStoreOdds(env, raceKey, new Date());
  if (!result) {
    return;
  }
  const source = raceKey.startsWith("jra:") ? "jra" : "nar";
  const yyyymmdd = raceKey.slice(4, 12);
  await purgeEdgeCache(raceKey);
  await purgeD1ResultCacheForRace(raceKey, D1_RESULT_CACHE_QUERIES);
  await writeLatestOddsToKv(env, raceKey, {
    fetchedAt: result.fetchedAt,
    latest: result.latest,
  });
  await patchLastFetchInKv(env, source, yyyymmdd, raceKey, result.fetchedAt);
  await writeCachedOdds(env, raceKey, {
    fetchedAt: result.fetchedAt,
    history: [],
    historyByType: {},
    latest: result.latest,
  });
};

export const processArchiveJob = async (env: Env, now: Date): Promise<void> => {
  await runScheduledArchive(env, now);
};

export const handleQueue = async (batch: MessageBatch<Job>, env: Env): Promise<void> => {
  for (const message of batch.messages) {
    try {
      const job = message.body;
      if (job.type === "fetch-odds") {
        await processFetchOddsJob(env, job.raceKey);
        message.ack();
        continue;
      }
      if (job.type === "archive-odds-to-r2") {
        await processArchiveJob(env, new Date());
        message.ack();
        continue;
      }
      message.ack();
    } catch (error) {
      try {
        await logFetch(env.REALTIME_HOT_DB, message.body.type, "error", null, formatError(error));
      } catch {
        // Swallow logging failures so we still acknowledge with retry.
      }
      message.retry();
    }
  }
};

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    return handleFetchRequest(env, request);
  },
  async scheduled(event: ScheduledEvent, env: Env): Promise<void> {
    await handleScheduled(event, env);
  },
  async queue(batch: MessageBatch<Job>, env: Env): Promise<void> {
    await handleQueue(batch, env);
  },
};
