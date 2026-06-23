// Run with bun. Aggregates multi-dimensional system state into a single
// /api/internal/health JSON response so external uptime monitors can detect
// silent-failure modes (R2 archive cron stuck, scheduled-plan-odds-error
// spikes, closing-backfill warnings) in real time instead of via manual
// fetch_logs scans.
//
// All KV/D1 reads are wrapped in try/catch so a transient binding outage
// surfaces as a structured ok=false check rather than a 500. Expensive D1
// queries (today races, polling progress, recent errors) are cached in
// ODDS_HOT_KV with short TTLs (60-300s) to avoid hammering D1 from external
// monitor pings.

import { getJstDateParts, getTodayJst } from "../time";
import type { Env } from "../types";

const HEARTBEAT_KV_KEY = "cron:heartbeat:scheduled";
const HEARTBEAT_THRESHOLD_SECONDS = 300;
export const ARCHIVE_LAST_SUCCESS_KV_KEY = "cron:archive:last-success";
const ARCHIVE_THRESHOLD_SECONDS = 86_400;
const ARCHIVE_FAILURE_SNAPSHOT_KV_KEY = "monitor:archive:last-failure-snapshot";
const ARCHIVE_FAILURE_SNAPSHOT_TTL_SECONDS = 60;
export const CLOSING_BACKFILL_LAST_RUN_KV_KEY = "cron:closing-backfill:last-run";
const CLOSING_BACKFILL_THRESHOLD_SECONDS = 90_000;
const TODAY_RACES_SNAPSHOT_KV_PREFIX = "monitor:today-races:snapshot:";
const TODAY_RACES_SNAPSHOT_TTL_SECONDS = 300;
const TODAY_POLLING_SNAPSHOT_KV_PREFIX = "monitor:today-polling:snapshot:";
const TODAY_POLLING_SNAPSHOT_TTL_SECONDS = 60;
const RECENT_ERRORS_SNAPSHOT_KV_KEY = "monitor:recent-errors:snapshot";
const RECENT_ERRORS_SNAPSHOT_TTL_SECONDS = 60;
const RECENT_ERRORS_THRESHOLD_COUNT = 5;
const RECENT_ERRORS_SAMPLE_LIMIT = 3;
const RECENT_ERRORS_WINDOW_SECONDS = 3_600;
const RECENT_ERRORS_QUERY_LIMIT = 100;
const EXPECTED_RACE_COUNT_KV_PREFIX = "expected-race-count:";
const RECENT_FETCH_WINDOW_MINUTES = 10;
const STARTED_RACE_THRESHOLD_MINUTES = 5;
const POLLING_WINDOW_START_HOUR = 10;
const POLLING_WINDOW_END_HOUR = 22;
const MILLISECONDS_PER_SECOND = 1_000;
const SECONDS_PER_MINUTE = 60;
const MINUTES_PER_HOUR = 60;

export interface CronHeartbeatCheck {
  ok: boolean;
  lastTickAt: string | null;
  ageSeconds: number | null;
  thresholdSeconds: number;
}

export interface ArchiveCronCheck {
  ok: boolean;
  lastSuccessAt: string | null;
  lastFailureAt: string | null;
  ageSinceSuccessSeconds: number | null;
  thresholdSeconds: number;
}

export interface ClosingBackfillCronCheck {
  ok: boolean;
  lastRunAt: string | null;
  lastRunStatus: string | null;
  lastCandidates: number | null;
  lastFailures: number | null;
  ageSeconds: number | null;
  thresholdSeconds: number;
}

export interface TodayRacesPopulatedCheck {
  ok: boolean;
  yyyymmdd: string;
  expected: number | null;
  actual: number | null;
}

export interface TodayPollingProgressCheck {
  ok: boolean;
  yyyymmdd: string;
  totalRaces: number;
  racesWithRecentFetch: number;
  racesStartedNotPolled: number;
  minutesSincePollWindowOpen: number;
}

export interface RecentErrorsCheck {
  ok: boolean;
  errorsLastHour: number;
  samplesLastHour: string[];
  thresholdCount: number;
}

export interface HealthChecks {
  cron_heartbeat: CronHeartbeatCheck;
  archive_cron: ArchiveCronCheck;
  closing_backfill_cron: ClosingBackfillCronCheck;
  today_races_populated: TodayRacesPopulatedCheck;
  today_polling_progress: TodayPollingProgressCheck;
  recent_errors: RecentErrorsCheck;
}

export interface HealthReport {
  ok: boolean;
  checks: HealthChecks;
}

interface ClosingBackfillKvValue {
  at: string;
  status: string;
  candidates: number;
  failures: number;
}

interface TodayRacesSnapshot {
  expected: number | null;
  actual: number | null;
}

interface PollingProgressSummary {
  total: number;
  recent: number;
  startedNotPolled: number;
}

interface RecentErrorsSnapshot {
  errorsLastHour: number;
  samplesLastHour: string[];
}

interface FetchStateCountRow {
  count: number;
}

interface RaceStartLastFetchRow {
  race_start_at_jst: string;
  last_odds_fetch_at: string | null;
}

interface FetchLogRow {
  message: string | null;
  created_at: string;
}

interface ArchiveFailureRow {
  created_at: string;
}

const computeAgeSeconds = (lastIso: string, now: Date): number =>
  Math.floor((now.getTime() - new Date(lastIso).getTime()) / MILLISECONDS_PER_SECOND);

const safeKvGet = async (env: Env, key: string): Promise<string | null> => {
  try {
    return await env.ODDS_HOT_KV.get(key);
  } catch {
    return null;
  }
};

const safeKvPut = async (env: Env, key: string, value: string, ttl: number): Promise<void> => {
  try {
    await env.ODDS_HOT_KV.put(key, value, { expirationTtl: ttl });
  } catch {
    // Snapshot write failures must never fail the health endpoint — the
    // primary signal is the underlying check result. The next monitor ping
    // will simply re-query D1 instead of reusing the cache.
  }
};

const parseInteger = (value: string | null): number | null => {
  if (value === null) {
    return null;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isNaN(parsed) ? null : parsed;
};

export const buildCronHeartbeatCheck = async (env: Env, now: Date): Promise<CronHeartbeatCheck> => {
  const lastTickAt = await safeKvGet(env, HEARTBEAT_KV_KEY);
  if (lastTickAt === null) {
    return {
      ageSeconds: null,
      lastTickAt: null,
      ok: false,
      thresholdSeconds: HEARTBEAT_THRESHOLD_SECONDS,
    };
  }
  const ageSeconds = computeAgeSeconds(lastTickAt, now);
  return {
    ageSeconds,
    lastTickAt,
    ok: ageSeconds <= HEARTBEAT_THRESHOLD_SECONDS,
    thresholdSeconds: HEARTBEAT_THRESHOLD_SECONDS,
  };
};

const queryLatestArchiveFailureTimestamp = async (env: Env): Promise<string | null> => {
  try {
    const row = await env.REALTIME_HOT_DB.prepare(
      `select created_at from fetch_logs where job_type = 'scheduled-archive-to-r2' and status = 'warn' order by created_at desc limit 1`,
    ).first<ArchiveFailureRow>();
    return row?.created_at ?? null;
  } catch {
    return null;
  }
};

const resolveArchiveFailureTimestamp = async (env: Env): Promise<string | null> => {
  const cached = await safeKvGet(env, ARCHIVE_FAILURE_SNAPSHOT_KV_KEY);
  if (cached !== null) {
    return cached;
  }
  const fresh = await queryLatestArchiveFailureTimestamp(env);
  if (fresh !== null) {
    await safeKvPut(
      env,
      ARCHIVE_FAILURE_SNAPSHOT_KV_KEY,
      fresh,
      ARCHIVE_FAILURE_SNAPSHOT_TTL_SECONDS,
    );
  }
  return fresh;
};

export const buildArchiveCronCheck = async (env: Env, now: Date): Promise<ArchiveCronCheck> => {
  const lastSuccessAt = await safeKvGet(env, ARCHIVE_LAST_SUCCESS_KV_KEY);
  const lastFailureAt = await resolveArchiveFailureTimestamp(env);
  if (lastSuccessAt === null) {
    return {
      ageSinceSuccessSeconds: null,
      lastFailureAt,
      lastSuccessAt: null,
      ok: false,
      thresholdSeconds: ARCHIVE_THRESHOLD_SECONDS,
    };
  }
  const ageSinceSuccessSeconds = computeAgeSeconds(lastSuccessAt, now);
  return {
    ageSinceSuccessSeconds,
    lastFailureAt,
    lastSuccessAt,
    ok: ageSinceSuccessSeconds <= ARCHIVE_THRESHOLD_SECONDS,
    thresholdSeconds: ARCHIVE_THRESHOLD_SECONDS,
  };
};

const isClosingBackfillKvValue = (
  parsed: Partial<ClosingBackfillKvValue>,
): parsed is ClosingBackfillKvValue =>
  typeof parsed.at === "string" &&
  typeof parsed.status === "string" &&
  typeof parsed.candidates === "number" &&
  typeof parsed.failures === "number";

const parseClosingBackfillKvValue = (raw: string): ClosingBackfillKvValue | null => {
  try {
    const parsed = JSON.parse(raw) as Partial<ClosingBackfillKvValue>;
    return isClosingBackfillKvValue(parsed) ? parsed : null;
  } catch {
    return null;
  }
};

const evaluateClosingBackfillOk = (failures: number, ageSeconds: number): boolean => {
  if (ageSeconds > CLOSING_BACKFILL_THRESHOLD_SECONDS) {
    return false;
  }
  return failures === 0;
};

const buildClosingBackfillMissing = (): ClosingBackfillCronCheck => ({
  ageSeconds: null,
  lastCandidates: null,
  lastFailures: null,
  lastRunAt: null,
  lastRunStatus: null,
  ok: false,
  thresholdSeconds: CLOSING_BACKFILL_THRESHOLD_SECONDS,
});

export const buildClosingBackfillCheck = async (
  env: Env,
  now: Date,
): Promise<ClosingBackfillCronCheck> => {
  const raw = await safeKvGet(env, CLOSING_BACKFILL_LAST_RUN_KV_KEY);
  if (raw === null) {
    return buildClosingBackfillMissing();
  }
  const parsed = parseClosingBackfillKvValue(raw);
  if (parsed === null) {
    return buildClosingBackfillMissing();
  }
  const ageSeconds = computeAgeSeconds(parsed.at, now);
  return {
    ageSeconds,
    lastCandidates: parsed.candidates,
    lastFailures: parsed.failures,
    lastRunAt: parsed.at,
    lastRunStatus: parsed.status,
    ok: evaluateClosingBackfillOk(parsed.failures, ageSeconds),
    thresholdSeconds: CLOSING_BACKFILL_THRESHOLD_SECONDS,
  };
};

const isTodayRacesSnapshot = (parsed: Partial<TodayRacesSnapshot>): parsed is TodayRacesSnapshot =>
  (parsed.expected === null || typeof parsed.expected === "number") &&
  (parsed.actual === null || typeof parsed.actual === "number");

const parseTodayRacesSnapshot = (raw: string): TodayRacesSnapshot | null => {
  try {
    const parsed = JSON.parse(raw) as Partial<TodayRacesSnapshot>;
    return isTodayRacesSnapshot(parsed) ? parsed : null;
  } catch {
    return null;
  }
};

const queryTodayRaceCount = async (env: Env, yyyymmdd: string): Promise<number | null> => {
  try {
    const row = await env.REALTIME_HOT_DB.prepare(
      `select count(*) as count from odds_fetch_state where kaisai_nen = ? and kaisai_tsukihi = ?`,
    )
      .bind(yyyymmdd.slice(0, 4), yyyymmdd.slice(4, 8))
      .first<FetchStateCountRow>();
    return row?.count ?? 0;
  } catch {
    return null;
  }
};

const readExpectedRaceCount = async (env: Env, yyyymmdd: string): Promise<number | null> =>
  parseInteger(await safeKvGet(env, `${EXPECTED_RACE_COUNT_KV_PREFIX}${yyyymmdd}`));

const evaluateTodayRacesOk = (snapshot: TodayRacesSnapshot): boolean => {
  if (snapshot.expected === null || snapshot.actual === null) {
    return false;
  }
  if (snapshot.expected === 0) {
    return true;
  }
  return snapshot.actual >= snapshot.expected;
};

export const buildTodayRacesPopulatedCheck = async (
  env: Env,
  now: Date,
): Promise<TodayRacesPopulatedCheck> => {
  const yyyymmdd = getTodayJst(now);
  const cached = await safeKvGet(env, `${TODAY_RACES_SNAPSHOT_KV_PREFIX}${yyyymmdd}`);
  const cachedSnapshot = cached === null ? null : parseTodayRacesSnapshot(cached);
  if (cachedSnapshot !== null) {
    return {
      actual: cachedSnapshot.actual,
      expected: cachedSnapshot.expected,
      ok: evaluateTodayRacesOk(cachedSnapshot),
      yyyymmdd,
    };
  }
  const expected = await readExpectedRaceCount(env, yyyymmdd);
  const actual = await queryTodayRaceCount(env, yyyymmdd);
  const snapshot: TodayRacesSnapshot = { actual, expected };
  await safeKvPut(
    env,
    `${TODAY_RACES_SNAPSHOT_KV_PREFIX}${yyyymmdd}`,
    JSON.stringify(snapshot),
    TODAY_RACES_SNAPSHOT_TTL_SECONDS,
  );
  return { actual, expected, ok: evaluateTodayRacesOk(snapshot), yyyymmdd };
};

// `Number()` (not `Number.parseInt`) follows the existing `polling-window-gate`
// pattern: `Number(undefined) === NaN` which compares false against every
// numeric threshold, so a hypothetical missing JST part keeps the window
// closed instead of throwing a type error.
const parseJstHourMinute = (now: Date): { hour: number; minute: number } => {
  const parts = getJstDateParts(now);
  return { hour: Number(parts.hour), minute: Number(parts.minute) };
};

const isInsidePollingWindow = (now: Date): boolean => {
  const { hour } = parseJstHourMinute(now);
  return hour >= POLLING_WINDOW_START_HOUR && hour < POLLING_WINDOW_END_HOUR;
};

const computeMinutesSinceWindowOpen = (now: Date): number => {
  const { hour, minute } = parseJstHourMinute(now);
  const totalMinutes = hour * MINUTES_PER_HOUR + minute;
  const windowOpenMinutes = POLLING_WINDOW_START_HOUR * MINUTES_PER_HOUR;
  return Math.max(0, totalMinutes - windowOpenMinutes);
};

const queryPollingProgressRows = async (
  env: Env,
  yyyymmdd: string,
): Promise<RaceStartLastFetchRow[] | null> => {
  try {
    const result = await env.REALTIME_HOT_DB.prepare(
      `select race_start_at_jst, last_odds_fetch_at from odds_fetch_state where kaisai_nen = ? and kaisai_tsukihi = ?`,
    )
      .bind(yyyymmdd.slice(0, 4), yyyymmdd.slice(4, 8))
      .all<RaceStartLastFetchRow>();
    return result.results;
  } catch {
    return null;
  }
};

const isRowRecentlyFetched = (row: RaceStartLastFetchRow, recentThresholdMs: number): boolean => {
  if (row.last_odds_fetch_at === null) {
    return false;
  }
  return new Date(row.last_odds_fetch_at).getTime() > recentThresholdMs;
};

const isRowStartedNotPolled = (
  row: RaceStartLastFetchRow,
  now: Date,
  startedThresholdOffsetMs: number,
): boolean => {
  const raceStartMs = new Date(row.race_start_at_jst).getTime();
  if (raceStartMs >= now.getTime()) {
    return false;
  }
  if (row.last_odds_fetch_at === null) {
    return true;
  }
  return new Date(row.last_odds_fetch_at).getTime() < raceStartMs - startedThresholdOffsetMs;
};

export const summarizePollingProgressRows = (
  rows: RaceStartLastFetchRow[],
  now: Date,
): PollingProgressSummary => {
  const recentThresholdMs =
    now.getTime() - RECENT_FETCH_WINDOW_MINUTES * SECONDS_PER_MINUTE * MILLISECONDS_PER_SECOND;
  const startedThresholdOffsetMs =
    STARTED_RACE_THRESHOLD_MINUTES * SECONDS_PER_MINUTE * MILLISECONDS_PER_SECOND;
  const recent = rows.filter((row) => isRowRecentlyFetched(row, recentThresholdMs)).length;
  const startedNotPolled = rows.filter((row) =>
    isRowStartedNotPolled(row, now, startedThresholdOffsetMs),
  ).length;
  return { recent, startedNotPolled, total: rows.length };
};

const isPollingProgressSummary = (
  parsed: Partial<PollingProgressSummary>,
): parsed is PollingProgressSummary =>
  typeof parsed.total === "number" &&
  typeof parsed.recent === "number" &&
  typeof parsed.startedNotPolled === "number";

const parsePollingSnapshot = (raw: string): PollingProgressSummary | null => {
  try {
    const parsed = JSON.parse(raw) as Partial<PollingProgressSummary>;
    return isPollingProgressSummary(parsed) ? parsed : null;
  } catch {
    return null;
  }
};

const evaluatePollingOk = (startedNotPolled: number, insideWindow: boolean): boolean => {
  if (!insideWindow) {
    return true;
  }
  return startedNotPolled === 0;
};

const buildPollingResult = (
  summary: PollingProgressSummary,
  yyyymmdd: string,
  insideWindow: boolean,
  minutesSincePollWindowOpen: number,
): TodayPollingProgressCheck => ({
  minutesSincePollWindowOpen,
  ok: evaluatePollingOk(summary.startedNotPolled, insideWindow),
  racesStartedNotPolled: summary.startedNotPolled,
  racesWithRecentFetch: summary.recent,
  totalRaces: summary.total,
  yyyymmdd,
});

export const buildTodayPollingProgressCheck = async (
  env: Env,
  now: Date,
): Promise<TodayPollingProgressCheck> => {
  const yyyymmdd = getTodayJst(now);
  const cached = await safeKvGet(env, `${TODAY_POLLING_SNAPSHOT_KV_PREFIX}${yyyymmdd}`);
  const insideWindow = isInsidePollingWindow(now);
  const minutesSincePollWindowOpen = computeMinutesSinceWindowOpen(now);
  const cachedSnapshot = cached === null ? null : parsePollingSnapshot(cached);
  if (cachedSnapshot !== null) {
    return buildPollingResult(cachedSnapshot, yyyymmdd, insideWindow, minutesSincePollWindowOpen);
  }
  const rows = await queryPollingProgressRows(env, yyyymmdd);
  const summary =
    rows === null
      ? { recent: 0, startedNotPolled: 0, total: 0 }
      : summarizePollingProgressRows(rows, now);
  await safeKvPut(
    env,
    `${TODAY_POLLING_SNAPSHOT_KV_PREFIX}${yyyymmdd}`,
    JSON.stringify(summary),
    TODAY_POLLING_SNAPSHOT_TTL_SECONDS,
  );
  return buildPollingResult(summary, yyyymmdd, insideWindow, minutesSincePollWindowOpen);
};

const extractErrorSamples = (rows: FetchLogRow[]): string[] =>
  rows
    .map((row) => row.message ?? "")
    .filter((message) => message.length > 0)
    .slice(0, RECENT_ERRORS_SAMPLE_LIMIT);

const queryRecentErrors = async (env: Env, now: Date): Promise<RecentErrorsSnapshot | null> => {
  const cutoffMs = now.getTime() - RECENT_ERRORS_WINDOW_SECONDS * MILLISECONDS_PER_SECOND;
  const cutoffIso = new Date(cutoffMs).toISOString();
  try {
    const result = await env.REALTIME_HOT_DB.prepare(
      `select message, created_at from fetch_logs where status = 'error' and created_at >= ? order by created_at desc limit ?`,
    )
      .bind(cutoffIso, RECENT_ERRORS_QUERY_LIMIT)
      .all<FetchLogRow>();
    return {
      errorsLastHour: result.results.length,
      samplesLastHour: extractErrorSamples(result.results),
    };
  } catch {
    return null;
  }
};

const isRecentErrorsSnapshot = (
  parsed: Partial<RecentErrorsSnapshot>,
): parsed is RecentErrorsSnapshot =>
  typeof parsed.errorsLastHour === "number" && Array.isArray(parsed.samplesLastHour);

const sanitizeSnapshotSamples = (samples: unknown[]): string[] =>
  samples.filter((entry): entry is string => typeof entry === "string");

const parseRecentErrorsSnapshot = (raw: string): RecentErrorsSnapshot | null => {
  try {
    const parsed = JSON.parse(raw) as Partial<RecentErrorsSnapshot>;
    if (!isRecentErrorsSnapshot(parsed)) {
      return null;
    }
    return {
      errorsLastHour: parsed.errorsLastHour,
      samplesLastHour: sanitizeSnapshotSamples(parsed.samplesLastHour),
    };
  } catch {
    return null;
  }
};

const buildRecentErrorsResult = (snapshot: RecentErrorsSnapshot): RecentErrorsCheck => ({
  errorsLastHour: snapshot.errorsLastHour,
  ok: snapshot.errorsLastHour <= RECENT_ERRORS_THRESHOLD_COUNT,
  samplesLastHour: snapshot.samplesLastHour,
  thresholdCount: RECENT_ERRORS_THRESHOLD_COUNT,
});

export const buildRecentErrorsCheck = async (env: Env, now: Date): Promise<RecentErrorsCheck> => {
  const cached = await safeKvGet(env, RECENT_ERRORS_SNAPSHOT_KV_KEY);
  const cachedSnapshot = cached === null ? null : parseRecentErrorsSnapshot(cached);
  if (cachedSnapshot !== null) {
    return buildRecentErrorsResult(cachedSnapshot);
  }
  const fresh = await queryRecentErrors(env, now);
  const snapshot = fresh ?? { errorsLastHour: 0, samplesLastHour: [] };
  await safeKvPut(
    env,
    RECENT_ERRORS_SNAPSHOT_KV_KEY,
    JSON.stringify(snapshot),
    RECENT_ERRORS_SNAPSHOT_TTL_SECONDS,
  );
  return buildRecentErrorsResult(snapshot);
};

const allChecksOk = (checks: HealthChecks): boolean =>
  checks.cron_heartbeat.ok &&
  checks.archive_cron.ok &&
  checks.closing_backfill_cron.ok &&
  checks.today_races_populated.ok &&
  checks.today_polling_progress.ok &&
  checks.recent_errors.ok;

export const buildHealthReport = async (env: Env, now: Date): Promise<HealthReport> => {
  // Promise.all so total endpoint latency is bounded by the slowest check
  // (target <= 500ms) rather than the sum. Each check is independent and
  // never throws — they capture failures internally as ok=false.
  const [
    cron_heartbeat,
    archive_cron,
    closing_backfill_cron,
    today_races_populated,
    today_polling_progress,
    recent_errors,
  ] = await Promise.all([
    buildCronHeartbeatCheck(env, now),
    buildArchiveCronCheck(env, now),
    buildClosingBackfillCheck(env, now),
    buildTodayRacesPopulatedCheck(env, now),
    buildTodayPollingProgressCheck(env, now),
    buildRecentErrorsCheck(env, now),
  ]);
  const checks: HealthChecks = {
    archive_cron,
    closing_backfill_cron,
    cron_heartbeat,
    recent_errors,
    today_polling_progress,
    today_races_populated,
  };
  return { checks, ok: allChecksOk(checks) };
};
