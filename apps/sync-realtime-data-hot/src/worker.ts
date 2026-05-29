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
import {
  computeArchiveCutoffIso,
  putArchiveRowToR2,
  putFinalBackupRowToR2,
  type FinalBackupGroupRow,
} from "./gates/r2-archive";
import { invalidateRaceListInKv, patchLastFetchInKv } from "./gates/race-list-kv-cache";
import { jsonResponse } from "./http";
import { extractYyyymmddFromRaceKey } from "./race-key";
import { readCachedOdds, writeCachedOdds } from "./odds-cache";
import { planOddsFetches } from "./plan";
import { populateMultiDayOddsFetchState, populateTodayOddsFetchState } from "./scheduled-race-list";
import {
  bulkInsertOddsSnapshotRows,
  countOddsFetchStateForDate,
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
import { addDaysToYyyymmdd, getTodayJst } from "./time";
import type { Env, Job, OddsData, OddsType, OddsFetchStateUpsertInput } from "./types";

const PLAN_ODDS_FETCHES_CRON = "* * * * *";
const ARCHIVE_ODDS_CRON = "0 4 * * *";
const POPULATE_MULTI_DAY_CRON = "55 20 * * *";
const ARCHIVE_QUERY_LIMIT = 200;
const D1_RESULT_CACHE_QUERIES = ["latest", "tanshoHistory", "oddsHistoryByType"];
const PLAN_DAYS_AHEAD = 2;

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

const readDoCacheSafe = async (env: Env, raceKey: string): Promise<OddsPayload | null> => {
  try {
    const cached = await readCachedOdds(env, raceKey);
    if (!cached) {
      return null;
    }
    return {
      fetchedAt: cached.fetchedAt,
      history: cached.history,
      historyByType: cached.historyByType,
      latest: cached.latest,
    };
  } catch {
    return null;
  }
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
  const doCached = await readDoCacheSafe(env, raceKey);
  if (doCached && doCached.history.length > 0) {
    await writeToEdgeCache(raceKey, doCached, env);
    return jsonResponse(doCached);
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

interface R2ArchiveOddsRow {
  id: number;
  race_key: string;
  fetched_at: string;
  odds_type: string;
  combination: string;
  odds: number | null;
  min_odds: number | null;
  max_odds: number | null;
  average_odds: number | null;
  rank: number | null;
}

interface R2ArchiveRowsRequest {
  rows: R2ArchiveOddsRow[];
}

interface R2ArchiveRowsResponse {
  groups: number;
  rows: number;
}

const buildFinalBackupGroupKey = (row: R2ArchiveOddsRow): string =>
  `${row.race_key}|${row.odds_type}|${row.fetched_at.slice(0, 10)}`;

export const groupRowsForFinalBackup = (
  rows: R2ArchiveOddsRow[],
): Map<string, FinalBackupGroupRow> => {
  const groups = new Map<string, { meta: FinalBackupGroupRow; rows: R2ArchiveOddsRow[] }>();
  rows.forEach((row) => {
    const key = buildFinalBackupGroupKey(row);
    const existing = groups.get(key);
    if (existing) {
      existing.rows.push(row);
      return;
    }
    groups.set(key, {
      meta: {
        fetchedAt: row.fetched_at,
        oddsType: row.odds_type,
        payloadJson: "",
        raceKey: row.race_key,
      },
      rows: [row],
    });
  });
  const finalized = new Map<string, FinalBackupGroupRow>();
  groups.forEach((value, key) => {
    finalized.set(key, { ...value.meta, payloadJson: JSON.stringify(value.rows) });
  });
  return finalized;
};

export const handleR2ArchiveRows = async (env: Env, request: Request): Promise<Response> => {
  if (!isAuthorizedInternalRequest(request, env)) {
    return jsonResponse({ error: "unauthorized" }, { status: 401 });
  }
  const body = (await request.json()) as R2ArchiveRowsRequest;
  const rows = body.rows ?? [];
  const groups = groupRowsForFinalBackup(rows);
  await Promise.all(Array.from(groups.values()).map((group) => putFinalBackupRowToR2(env, group)));
  const response: R2ArchiveRowsResponse = { groups: groups.size, rows: rows.length };
  return jsonResponse(response);
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

export const handleRunPopulateToday = async (env: Env, request: Request): Promise<Response> => {
  if (!isAuthorizedInternalRequest(request, env)) {
    return jsonResponse({ error: "unauthorized" }, { status: 401 });
  }
  const result = await populateTodayOddsFetchState(env, new Date());
  return jsonResponse(result);
};

export const handleRunPopulateMultiDay = async (env: Env, request: Request): Promise<Response> => {
  if (!isAuthorizedInternalRequest(request, env)) {
    return jsonResponse({ error: "unauthorized" }, { status: 401 });
  }
  const result = await populateMultiDayOddsFetchState(env, new Date());
  return jsonResponse(result);
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
  if (request.method === "POST" && url.pathname === "/api/internal/r2-archive-rows") {
    return handleR2ArchiveRows(env, request);
  }
  if (request.method === "POST" && url.pathname === "/api/internal/migration-state") {
    return handleMigrationState(env, request);
  }
  if (request.method === "GET" && url.pathname === "/api/internal/migration-state") {
    return handleGetMigrationState(env, request);
  }
  if (request.method === "POST" && url.pathname === "/api/internal/run-populate-today") {
    return handleRunPopulateToday(env, request);
  }
  if (request.method === "POST" && url.pathname === "/api/internal/run-populate-multi-day") {
    return handleRunPopulateMultiDay(env, request);
  }
  const raceKey = parseRaceKeyFromPath(url.pathname);
  if (request.method === "GET" && raceKey) {
    return handleGetOdds(env, request, raceKey);
  }
  return jsonResponse({ error: "not found" }, { status: 404 });
};

export const collectPlanDates = (now: Date): string[] => {
  const today = getTodayJst(now);
  const futureDates = Array.from({ length: PLAN_DAYS_AHEAD }, (_, offset) =>
    addDaysToYyyymmdd(today, offset + 1),
  );
  return [today, ...futureDates];
};

export const runScheduledPlan = async (env: Env, now: Date): Promise<void> => {
  const todayYyyymmdd = getTodayJst(now);
  // Fallback self-discovery: when odds_fetch_state has no row for today, the
  // legacy worker either has not forwarded yet or is down. Run a one-shot
  // populate so the planner has something to enqueue this tick instead of
  // skipping for hours. Multi-day populate (cron 55 20 * * *) seeds tomorrow's
  // and the day-after's races; the polling planner then iterates today + next
  // 2 days every minute so races scheduled hours in advance still get the
  // hourly fetch cadence even at 23:41 JST the night before.
  const stateCount = await countOddsFetchStateForDate(
    env.REALTIME_HOT_DB,
    todayYyyymmdd.slice(0, 4),
    todayYyyymmdd.slice(4, 8),
  );
  if (stateCount === 0) {
    await populateTodayOddsFetchState(env, now);
  }
  await Promise.all(collectPlanDates(now).map((yyyymmdd) => planOddsFetches(env, now, yyyymmdd)));
};

export const runScheduledPopulateMultiDay = async (env: Env, now: Date): Promise<void> => {
  await populateMultiDayOddsFetchState(env, now);
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
    return;
  }
  if (event.cron === POPULATE_MULTI_DAY_CRON) {
    await runScheduledPopulateMultiDay(env, now);
  }
};

export const processFetchOddsJob = async (env: Env, raceKey: string): Promise<void> => {
  const result = await fetchAndStoreOdds(env, raceKey, new Date());
  if (!result) {
    return;
  }
  const yyyymmdd = extractYyyymmddFromRaceKey(raceKey);
  if (!yyyymmdd) {
    await logFetch(
      env.REALTIME_HOT_DB,
      "fetch-odds",
      "error",
      raceKey,
      "invalid raceKey format",
    ).catch(() => undefined);
    return;
  }
  const source = raceKey.startsWith("jra:") ? "jra" : "nar";
  await purgeEdgeCache(raceKey);
  await purgeD1ResultCacheForRace(raceKey, D1_RESULT_CACHE_QUERIES);
  await writeLatestOddsToKv(env, raceKey, {
    fetchedAt: result.fetchedAt,
    latest: result.latest,
  });
  await patchLastFetchInKv(env, source, yyyymmdd, raceKey, result.fetchedAt);
  await writeCachedOdds(env, raceKey, {
    fetchedAt: result.fetchedAt,
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
