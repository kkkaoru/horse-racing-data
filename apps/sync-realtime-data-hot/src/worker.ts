import { getExpectedRaceCountForDate } from "./expected-race-count";
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
import { shouldRunOddsCron } from "./gates/polling-window-gate";
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
import {
  getCachedOddsFetchStateCount,
  invalidateOddsFetchStateCount,
} from "./odds-fetch-state-count-cache";
import { planOddsFetches } from "./plan";
import { populateMultiDayOddsFetchState, populateTodayOddsFetchState } from "./scheduled-race-list";
import {
  bulkInsertOddsSnapshotRows,
  getLatestOddsFromD1,
  listArchiveCandidatesBeforeCutoff,
  listClosingBackfillCandidates,
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
// re-populate today's odds_fetch_state after morning NAR Neon sync completes
const MORNING_POPULATE_MULTI_DAY_CRON = "0 23 * * *";
const POPULATE_MULTI_DAY_CRONS: ReadonlySet<string> = new Set([
  POPULATE_MULTI_DAY_CRON,
  MORNING_POPULATE_MULTI_DAY_CRON,
]);
// JST 22:30 = UTC 13:30. Synchronous closing-odds re-fetch for races whose
// last poll happened before raceStart - 5min (final betting window missed).
const CLOSING_BACKFILL_CRON = "30 13 * * *";
const ARCHIVE_QUERY_LIMIT = 200;
const D1_RESULT_CACHE_QUERIES = ["latest", "tanshoHistory", "oddsHistoryByType"];
const PLAN_DAYS_AHEAD = 2;
// Silent-death detector for the per-minute scheduled cron. `dispatchScheduledByCron`
// writes this key at the very top of every tick so a missing value == cron is dead.
// TTL 600s is wide enough that a few skipped ticks do not trigger false alerts but
// narrow enough that 6h of silence has no value present at all.
const CRON_HEARTBEAT_KV_KEY = "cron:heartbeat:scheduled";
const CRON_HEARTBEAT_TTL_SECONDS = 600;
// `handleCronHealth` returns 503 once the latest heartbeat exceeds this age. 300s
// (5 scheduled ticks for the `* * * * *` cron) is conservative — by then any
// reasonable polling-window cadence should have refreshed the key.
const CRON_HEALTH_MAX_AGE_SECONDS = 300;
const MILLISECONDS_PER_SECOND = 1000;
const STALE_HEALTH_STATUS = 503;
const MISSING_HEALTH_STATUS = 503;
const FRESH_HEALTH_STATUS = 200;
// Minimum distinct tansho snapshots required for the DO cache to be trusted
// as a trend source. The viewer's オッズ推移 line chart becomes visually
// meaningful around 5-10 timepoints. Below this floor the DO is treated as
// shallow (e.g. fresh races, or races whose DO state was built under an
// older per-type history cap) and we fall through to the D1 path which
// returns full uncapped history.
const MIN_DO_TRUSTED_SNAPSHOTS = 10;
// Once a planner tick observes stateCount == expectedCount (and both are >0),
// today's `odds_fetch_state` for this JST day is fully seeded and the self-
// discovery populate gate cannot fire. Writing this KV flag lets later
// per-minute ticks skip BOTH the D1 count(*) and the expected-count read for
// the next 10 minutes — the dominant per-tick work in the steady-state
// polling window. TTL 600s keeps the safety margin small enough that a
// late-day populate (e.g. operator manually re-syncs NAR) is re-detected
// within 10 min by the next post-expiry tick.
const PLAN_STABLE_FLAG_KV_PREFIX = "expected-race-count:stable:";
const PLAN_STABLE_FLAG_TTL_SECONDS = 600;
const PLAN_STABLE_FLAG_VALUE = "1";

interface OddsPayload {
  fetchedAt: string | null;
  history: ReturnType<typeof toHorseTrends>;
  historyByType: ReturnType<typeof toOddsTrendsByType>;
  latest: Partial<Record<OddsType, OddsData[]>>;
}

interface CronHealthBody {
  ok: boolean;
  lastTickAt: string | null;
  ageSeconds: number | null;
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

// Counts distinct `fetchedAt` values across all tansho trend points in the
// cached payload. `history` lists one entry per horse, so its raw length is
// not a reliable proxy for how many snapshots the viewer can draw.
const countTanshoSnapshots = (cached: OddsPayload): number => {
  const trends = cached.history;
  if (trends.length === 0) {
    return 0;
  }
  const fetchedAts = new Set<string>();
  trends.forEach((trend) => {
    trend.points.forEach((point) => {
      fetchedAts.add(point.fetchedAt);
    });
  });
  return fetchedAts.size;
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
  if (doCached && countTanshoSnapshots(doCached) >= MIN_DO_TRUSTED_SNAPSHOTS) {
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

const buildStaleHealthBody = (lastTickAt: string, ageSeconds: number): CronHealthBody => ({
  ageSeconds,
  lastTickAt,
  ok: false,
});

const buildMissingHealthBody = (): CronHealthBody => ({
  ageSeconds: null,
  lastTickAt: null,
  ok: false,
});

const buildFreshHealthBody = (lastTickAt: string, ageSeconds: number): CronHealthBody => ({
  ageSeconds,
  lastTickAt,
  ok: true,
});

export const handleCronHealth = async (env: Env, request: Request): Promise<Response> => {
  if (!isAuthorizedInternalRequest(request, env)) {
    return jsonResponse({ error: "unauthorized" }, { status: 401 });
  }
  const lastTickAt = await env.ODDS_HOT_KV.get(CRON_HEARTBEAT_KV_KEY);
  if (!lastTickAt) {
    return jsonResponse(buildMissingHealthBody(), { status: MISSING_HEALTH_STATUS });
  }
  const ageMs = Date.now() - new Date(lastTickAt).getTime();
  const ageSeconds = Math.floor(ageMs / MILLISECONDS_PER_SECOND);
  if (ageSeconds > CRON_HEALTH_MAX_AGE_SECONDS) {
    return jsonResponse(buildStaleHealthBody(lastTickAt, ageSeconds), {
      status: STALE_HEALTH_STATUS,
    });
  }
  return jsonResponse(buildFreshHealthBody(lastTickAt, ageSeconds), {
    status: FRESH_HEALTH_STATUS,
  });
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
  if (request.method === "GET" && url.pathname === "/api/internal/cron-health") {
    return handleCronHealth(env, request);
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

const logScheduledError = async (env: Env, jobType: string, error: unknown): Promise<void> => {
  try {
    await logFetch(env.REALTIME_HOT_DB, jobType, "error", null, formatError(error));
  } catch (logError) {
    // Last-resort: cron must not throw even when D1 itself is down.
    console.error("scheduled cron failed and logFetch also threw", logError);
  }
};

const safeCountOddsFetchStateForDate = async (
  env: Env,
  todayYyyymmdd: string,
): Promise<number | null> => {
  try {
    return await getCachedOddsFetchStateCount(env, todayYyyymmdd);
  } catch (error) {
    await logScheduledError(env, "scheduled-plan-count-state-error", error);
    return null;
  }
};

const safeGetExpectedRaceCount = async (
  env: Env,
  todayYyyymmdd: string,
  now: Date,
): Promise<number | null> => {
  try {
    return await getExpectedRaceCountForDate(env, todayYyyymmdd, { now });
  } catch (error) {
    await logScheduledError(env, "scheduled-plan-expected-count-error", error);
    return null;
  }
};

const safePopulateTodayOddsFetchState = async (
  env: Env,
  now: Date,
  todayYyyymmdd: string,
): Promise<void> => {
  try {
    await populateTodayOddsFetchState(env, now);
    // Drop the stale count cache so the next planner tick re-reads D1 and
    // observes the fresh row total instead of looping on the pre-populate value.
    await invalidateOddsFetchStateCount(env, todayYyyymmdd);
  } catch (error) {
    await logScheduledError(env, "scheduled-plan-populate-error", error);
  }
};

const buildPlanStableFlagKey = (todayYyyymmdd: string): string =>
  `${PLAN_STABLE_FLAG_KV_PREFIX}${todayYyyymmdd}`;

const isPlanStable = async (env: Env, todayYyyymmdd: string): Promise<boolean> => {
  try {
    const cached = await env.ODDS_HOT_KV.get(buildPlanStableFlagKey(todayYyyymmdd));
    return cached === PLAN_STABLE_FLAG_VALUE;
  } catch {
    // KV read failure must never break the planner — fall through to the
    // full D1 + Hyperdrive count path so the populate self-heal still runs.
    return false;
  }
};

const writePlanStableFlag = async (env: Env, todayYyyymmdd: string): Promise<void> => {
  try {
    await env.ODDS_HOT_KV.put(buildPlanStableFlagKey(todayYyyymmdd), PLAN_STABLE_FLAG_VALUE, {
      expirationTtl: PLAN_STABLE_FLAG_TTL_SECONDS,
    });
  } catch (error) {
    await logScheduledError(env, "scheduled-plan-stable-flag-write-error", error);
  }
};

const runPopulateGate = async (env: Env, now: Date, todayYyyymmdd: string): Promise<void> => {
  // Self-discovery: compare today's actual `odds_fetch_state` row count
  // against the expected total from Hyperdrive (`jvd_ra` + `nvd_ra`).
  // When the count is short — e.g. the 05:55 JST populate ran before NAR
  // venues (Ban'ei, Mizusawa) published their `keiba.go` RaceList HTML, so
  // only a partial day was seeded — re-run populate so the planner does not
  // skip the missing venue for the rest of the day. The legacy `=== 0`
  // gate hid this regression because a single JRA venue was enough to lock
  // populate out for the entire day.
  const stateCount = await safeCountOddsFetchStateForDate(env, todayYyyymmdd);
  const expectedCount = await safeGetExpectedRaceCount(env, todayYyyymmdd, now);
  if (stateCount === null || expectedCount === null) {
    return;
  }
  if (stateCount < expectedCount) {
    await safePopulateTodayOddsFetchState(env, now, todayYyyymmdd);
    return;
  }
  // stateCount == expectedCount path. Only write the stable flag when both
  // numbers are positive — a zero-race day (no JRA, no NAR scheduled) must
  // not lock in `stable` because there is nothing to short-circuit and a
  // later populate could still legitimately add rows.
  if (stateCount > 0) {
    await writePlanStableFlag(env, todayYyyymmdd);
  }
};

const runPlanForDate = async (env: Env, now: Date, yyyymmdd: string): Promise<void> => {
  try {
    await planOddsFetches(env, now, yyyymmdd);
  } catch (error) {
    await logScheduledError(env, "scheduled-plan-odds-error", error);
  }
};

export const runScheduledPlan = async (env: Env, now: Date): Promise<void> => {
  const todayYyyymmdd = getTodayJst(now);
  // Steady-state short-circuit: once a recent tick has confirmed
  // stateCount == expectedCount with both > 0, today's `odds_fetch_state` is
  // fully seeded and the populate self-heal gate cannot fire. The stable
  // flag's TTL caps how long we trust that observation; on expiry the next
  // tick re-runs the full D1 count + Hyperdrive expected-count path.
  if (!(await isPlanStable(env, todayYyyymmdd))) {
    await runPopulateGate(env, now, todayYyyymmdd);
  }
  // `Promise.allSettled` ensures one rejected date never blocks the others.
  // Individual rejections are logged inside `runPlanForDate`.
  await Promise.allSettled(
    collectPlanDates(now).map((yyyymmdd) => runPlanForDate(env, now, yyyymmdd)),
  );
};

export const runScheduledPopulateMultiDay = async (env: Env, now: Date): Promise<void> => {
  await populateMultiDayOddsFetchState(env, now);
};

const formatBackfillFailure = (outcome: PromiseRejectedResult): string => String(outcome.reason);

const isRejected = (outcome: PromiseSettledResult<unknown>): outcome is PromiseRejectedResult =>
  outcome.status === "rejected";

const BACKFILL_FAILURE_SAMPLE_LIMIT = 3;

export const runScheduledClosingBackfill = async (env: Env, now: Date): Promise<void> => {
  const today = getTodayJst(now);
  const candidates = await listClosingBackfillCandidates(
    env.REALTIME_HOT_DB,
    today.slice(0, 4),
    today.slice(4, 8),
  );
  const outcomes = await Promise.allSettled(
    candidates.map((raceKey) => fetchAndStoreOdds(env, raceKey, now)),
  );
  const failures = outcomes.filter(isRejected).map(formatBackfillFailure);
  await logFetch(
    env.REALTIME_HOT_DB,
    "scheduled-closing-backfill",
    failures.length === 0 ? "ok" : "warn",
    null,
    JSON.stringify({
      candidates: candidates.length,
      failures: failures.length,
      sampleFailures: failures.slice(0, BACKFILL_FAILURE_SAMPLE_LIMIT),
    }),
  );
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

interface DispatchScheduledArgs {
  env: Env;
  cron: string;
  now: Date;
  ctx: ExecutionContext;
}

// Heartbeat write. Must be the first thing in `dispatchScheduledByCron` so a
// cron-branch failure still leaves a recent timestamp behind. If the KV write
// itself rejects (namespace mis-bound, KV outage), we record one fetch_logs
// row tagged `cron-heartbeat-kv-error` and continue — the heartbeat path must
// never become the thing that kills the cron.
const writeCronHeartbeat = async (env: Env, now: Date): Promise<void> => {
  try {
    await env.ODDS_HOT_KV.put(CRON_HEARTBEAT_KV_KEY, now.toISOString(), {
      expirationTtl: CRON_HEARTBEAT_TTL_SECONDS,
    });
  } catch (error) {
    await logFetch(
      env.REALTIME_HOT_DB,
      "cron-heartbeat-kv-error",
      "error",
      null,
      formatError(error),
    ).catch(() => undefined);
  }
};

const dispatchScheduledByCron = async (args: DispatchScheduledArgs): Promise<void> => {
  // Record the heartbeat first so a cron-branch failure further down still
  // leaves a recent ISO timestamp in KV. `/api/internal/cron-health` reads
  // this key; missing == 503.
  await writeCronHeartbeat(args.env, args.now);
  // `ctx` is threaded through so any future `ctx.waitUntil(...)` hook in the
  // per-cron branches can extend the cron's lifetime beyond the immediate
  // body. Today the existing branches are fully awaited so no waitUntil call
  // is required, but Module Workers require the 3-arg signature on the
  // exported `scheduled` handler regardless.
  void args.ctx;
  if (args.cron === PLAN_ODDS_FETCHES_CRON) {
    // Race-window gate: planner cron only runs when at least one race is
    // within [now - 30min, now + 3h]. The other crons (`0 4`, `55 20`) bypass
    // the gate because populate / archive must run outside the race window.
    if (!(await shouldRunOddsCron(args.env, args.now))) {
      return;
    }
    await runScheduledPlan(args.env, args.now);
    return;
  }
  if (args.cron === ARCHIVE_ODDS_CRON) {
    await runScheduledArchive(args.env, args.now);
    return;
  }
  if (POPULATE_MULTI_DAY_CRONS.has(args.cron)) {
    await runScheduledPopulateMultiDay(args.env, args.now);
    return;
  }
  if (args.cron === CLOSING_BACKFILL_CRON) {
    await runScheduledClosingBackfill(args.env, args.now);
    return;
  }
};

export const handleScheduled = async (
  controller: ScheduledController,
  env: Env,
  ctx: ExecutionContext,
): Promise<void> => {
  const now = new Date(controller.scheduledTime);
  try {
    await dispatchScheduledByCron({ cron: controller.cron, ctx, env, now });
  } catch (error) {
    await logScheduledError(env, "scheduled-cron-error", error);
  }
};

const formatOuterThrowMessage = (error: unknown): string =>
  error instanceof Error ? `${error.message}\n${error.stack ?? ""}` : String(error);

export const reportScheduledOuterThrow = async (env: Env, error: unknown): Promise<void> => {
  const message = formatOuterThrowMessage(error);
  console.error("scheduled-outer-throw", message);
  try {
    await logFetch(env.REALTIME_HOT_DB, "scheduled-outer-throw", "error", null, message);
  } catch (logError) {
    console.error("scheduled-outer-throw logFetch fallback", logError);
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
  async scheduled(controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    try {
      await handleScheduled(controller, env, ctx);
    } catch (error) {
      await reportScheduledOuterThrow(env, error);
    }
  },
  async queue(batch: MessageBatch<Job>, env: Env): Promise<void> {
    await handleQueue(batch, env);
  },
};
