// Run with bun. Day-wide netkeiba workout ingestion.
// R2 Catalog is the authoritative first write; Neon is updated only by the
// daily-keiba-sync pipeline after the Catalog commit succeeds.

import {
  buildPremiumUrl,
  fetchPremiumHtml,
  getPremiumRaceConfig,
  mergeNetkeibaTrainingWorkouts,
  parseNetkeibaTrainingWorkouts,
  type PremiumTrainingWorkout,
} from "./premium-race";
import { triggerRaceCacheBust } from "./viewer-race-cache-bust";
import type { Env } from "./types";

const CATALOG_ORIGIN = "https://pc-keiba-r2-catalog.internal";
const DAILY_SYNC_ORIGIN = "https://daily-keiba-sync.internal";
const VIEWER_ORIGIN = "https://pc-keiba-viewer.internal";
const DEFAULT_WORK_PATH = "/race/oikiri.html?race_id={sourceRaceId}";
const PROCESSING_STALE_SECONDS = 30 * 60;
const FINALIZE_DELAY_SECONDS = 60;
const CONCURRENCY = 6;
const IN_FLIGHT_STATUSES = new Set<string>(["catalog_pending", "processing"]);
// Every discover job starts its own 5-minute retry chain for a failed day, so
// chains pile up (5-6 in parallel on 2026-10-23, each re-fetching every race
// through Browser Run). A failure younger than this gap means another chain
// just tried; let this one end. Below the 5-minute retry delay so the chain
// that made the last attempt always survives.
const FAILED_RETRY_GAP_SECONDS = 4 * 60;
const BLOCKER_ERROR_PATTERN =
  /^netkeiba (?:returned no workouts for|workout did not match) (\S+)$/u;

interface SyncStateRow {
  attempted_at: string;
  error_message: string | null;
  status: string;
}

interface PremiumLinkRow {
  race_key: string;
  source_race_id: string;
}

interface CatalogEntry {
  keibajoCode: string;
  kettoTorokuBango: string;
  raceBango: string;
  source: "jra";
  umaban: number;
}

interface WorkoutRecord extends Record<string, null | number | string> {
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  ketto_toroku_bango: string;
  race_bango: string;
  workout_key: string;
}

interface DailyRunStatus {
  catalog_ready: boolean;
  error_stage: string | null;
  status: string;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const requireString = (value: unknown, field: string): string => {
  if (typeof value !== "string" || value.length === 0)
    throw new Error(`Invalid netkeiba day sync ${field}`);
  return value;
};

const parseCatalogEntries = (value: unknown): CatalogEntry[] => {
  if (!isRecord(value) || !Array.isArray(value.entries))
    throw new Error("Invalid R2 Catalog entry response");
  return value.entries.map((entry) => {
    if (!isRecord(entry) || entry.source !== "jra" || !Number.isInteger(entry.umaban))
      throw new Error("Invalid R2 Catalog entry row");
    return {
      keibajoCode: requireString(entry.keibajoCode, "keibajoCode"),
      kettoTorokuBango: requireString(entry.kettoTorokuBango, "kettoTorokuBango"),
      raceBango: requireString(entry.raceBango, "raceBango"),
      source: "jra",
      umaban: Number(entry.umaban),
    };
  });
};

const raceParts = (raceKey: string): { date: string; keibajoCode: string; raceBango: string } => {
  const match = /^jra:(\d{4}):(\d{4}):(\d{2}):(\d{2})$/u.exec(raceKey);
  if (match === null) throw new Error("Invalid netkeiba race key");
  return {
    date: `${match[1]!}${match[2]!}`,
    keibajoCode: match[3]!,
    raceBango: match[4]!,
  };
};

const sha256Hex = async (value: string): Promise<string> => {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
};

const workoutKey = (workout: PremiumTrainingWorkout): Promise<string> =>
  sha256Hex(
    JSON.stringify({
      course: workout.course,
      courseDirection: workout.courseDirection,
      trainingDate: workout.trainingDate,
      trainingTime: workout.trainingTime,
      trainingType: workout.trainingType,
      workoutIndex: workout.workoutIndex,
    }),
  );

const intermediateUrl = (url: string): string => {
  const parsed = new URL(url);
  parsed.searchParams.set("type", "1");
  return parsed.toString();
};

const fetchRaceWorkouts = async (
  env: Env,
  date: string,
  sourceRaceId: string,
): Promise<PremiumTrainingWorkout[]> => {
  const config = getPremiumRaceConfig(env);
  if (!config.origin) throw new Error("netkeiba premium fetch is not configured");
  const workUrl = buildPremiumUrl(
    config,
    config.workPathTemplate ?? DEFAULT_WORK_PATH,
    { sourceRaceId },
    { source: "jra" },
  );
  if (workUrl === null) throw new Error("netkeiba workout URL is unavailable");
  const [finalHtml, middleHtml] = await Promise.all([
    fetchPremiumHtml(config, workUrl),
    fetchPremiumHtml(config, intermediateUrl(workUrl)),
  ]);
  return mergeNetkeibaTrainingWorkouts([
    parseNetkeibaTrainingWorkouts(finalHtml, date),
    parseNetkeibaTrainingWorkouts(middleHtml, date),
  ]);
};

const mapWithConcurrency = async <T, U>(
  values: readonly T[],
  mapper: (value: T) => Promise<U>,
): Promise<U[]> => {
  const result: U[] = [];
  for (let offset = 0; offset < values.length; offset += CONCURRENCY) {
    result.push(...(await Promise.all(values.slice(offset, offset + CONCURRENCY).map(mapper))));
  }
  return result;
};

const listPremiumLinks = async (db: D1Database, date: string): Promise<PremiumLinkRow[]> => {
  const result = await db
    .prepare(
      `select race_key, source_race_id
         from premium_race_links
        where race_key like ?
        order by race_key`,
    )
    .bind(`jra:${date.slice(0, 4)}:${date.slice(4)}:%`)
    .all<PremiumLinkRow>();
  return result.results;
};

const hasJraRaces = async (db: D1Database, date: string): Promise<boolean> =>
  (await db
    .prepare(
      `select 1 as found
         from realtime_race_sources
        where source = 'jra' and kaisai_nen = ? and kaisai_tsukihi = ?
        limit 1`,
    )
    .bind(date.slice(0, 4), date.slice(4, 8))
    .first()) !== null;

const loadCatalogEntries = async (env: Env, date: string): Promise<CatalogEntry[]> => {
  if (!env.R2_CATALOG_INGESTION_TOKEN) throw new Error("R2 Catalog ingestion token is missing");
  const url = new URL("/v1/internal/fresh-race-entries-bulk", CATALOG_ORIGIN);
  url.searchParams.set("date", date);
  url.searchParams.set("source", "jra");
  const response = await env.PC_KEIBA_R2_CATALOG.fetch(
    new Request(url, {
      headers: { Authorization: `Bearer ${env.R2_CATALOG_INGESTION_TOKEN}` },
    }),
  );
  if (!response.ok) throw new Error(`R2 Catalog entries failed with HTTP ${response.status}`);
  return parseCatalogEntries(await response.json());
};

const toWorkoutRecord = async (
  date: string,
  link: PremiumLinkRow,
  workout: PremiumTrainingWorkout,
  entry: CatalogEntry,
  now: Date,
): Promise<WorkoutRecord> => {
  const parts = raceParts(link.race_key);
  const timestamp = now.toISOString();
  const jstDate = new Intl.DateTimeFormat("sv-SE", {
    day: "2-digit",
    month: "2-digit",
    timeZone: "Asia/Tokyo",
    year: "numeric",
  })
    .format(now)
    .replaceAll("-", "");
  return {
    babamawari: workout.courseDirection,
    bamei: workout.horseName,
    chokyo_jikoku: workout.trainingTime,
    chokyo_nengappi: workout.trainingDate,
    comment_text: workout.commentText,
    course: workout.course,
    created_at: timestamp,
    data_kubun: "1",
    data_sakusei_nengappi: jstDate,
    evaluation_grade: workout.evaluationGrade,
    evaluation_text: workout.evaluationText,
    fetched_at: timestamp,
    kaisai_nen: date.slice(0, 4),
    kaisai_tsukihi: date.slice(4),
    keibajo_code: parts.keibajoCode,
    ketto_toroku_bango: entry.kettoTorokuBango,
    lap_time_10f: workout.lapTime10f,
    lap_time_1f: workout.lapTime1f,
    lap_time_2f: workout.lapTime2f,
    lap_time_3f: workout.lapTime3f,
    lap_time_4f: workout.lapTime4f,
    lap_time_5f: workout.lapTime5f,
    lap_time_6f: workout.lapTime6f,
    lap_time_7f: workout.lapTime7f,
    lap_time_8f: workout.lapTime8f,
    lap_time_9f: workout.lapTime9f,
    race_bango: parts.raceBango,
    record_id: "NK",
    rider_name: workout.riderName,
    source_race_id: link.source_race_id,
    source_url: `https://race.netkeiba.com/race/oikiri.html?race_id=${link.source_race_id}`,
    time_gokei_10f: workout.timeGokei10f,
    time_gokei_2f: workout.timeGokei2f,
    time_gokei_3f: workout.timeGokei3f,
    time_gokei_4f: workout.timeGokei4f,
    time_gokei_5f: workout.timeGokei5f,
    time_gokei_6f: workout.timeGokei6f,
    time_gokei_7f: workout.timeGokei7f,
    time_gokei_8f: workout.timeGokei8f,
    time_gokei_9f: workout.timeGokei9f,
    tracen_kubun: workout.tracenKubun,
    training_type: workout.trainingType,
    umaban: String(entry.umaban).padStart(2, "0"),
    updated_at: timestamp,
    workout_index: workout.workoutIndex,
    workout_key: await workoutKey(workout),
  };
};

const buildDayRecords = async (
  env: Env,
  date: string,
  links: readonly PremiumLinkRow[],
  entries: readonly CatalogEntry[],
  now: Date,
): Promise<WorkoutRecord[]> => {
  const entryMap = new Map(
    entries.map((entry) => [
      `${entry.keibajoCode.padStart(2, "0")}:${entry.raceBango.padStart(2, "0")}:${String(entry.umaban)}`,
      entry,
    ]),
  );
  const groups = await mapWithConcurrency(links, async (link) => {
    const parts = raceParts(link.race_key);
    const workouts = await fetchRaceWorkouts(env, date, link.source_race_id);
    if (workouts.length === 0)
      throw new Error(`netkeiba returned no workouts for ${link.race_key}`);
    const rows = await Promise.all(
      workouts.map(async (workout): Promise<WorkoutRecord> => {
        const entry = entryMap.get(
          `${parts.keibajoCode}:${parts.raceBango}:${String(Number(workout.horseNumber))}`,
        );
        if (entry === undefined) throw new Error(`netkeiba workout did not match ${link.race_key}`);
        return toWorkoutRecord(date, link, workout, entry, now);
      }),
    );
    return rows;
  });
  return groups.flat();
};

const stageWithDailySync = async (
  env: Env,
  date: string,
  records: readonly WorkoutRecord[],
): Promise<string> => {
  if (!env.DAILY_KEIBA_SYNC || !env.REALTIME_ADMIN_TOKEN)
    throw new Error("daily-keiba-sync staging is not configured");
  const response = await env.DAILY_KEIBA_SYNC.fetch(
    new Request(new URL("/internal/stage-netkeiba-training", DAILY_SYNC_ORIGIN), {
      body: JSON.stringify({ records, runDate: date, tableName: "netkeiba_training_workouts" }),
      headers: {
        Authorization: `Bearer ${env.REALTIME_ADMIN_TOKEN}`,
        "Content-Type": "application/json",
      },
      method: "POST",
    }),
  );
  if (!response.ok) throw new Error(`daily-keiba-sync staging failed with HTTP ${response.status}`);
  const value: unknown = await response.json();
  if (!isRecord(value)) throw new Error("Invalid daily-keiba-sync staging response");
  return requireString(value.runId, "runId");
};

const markState = async (
  db: D1Database,
  date: string,
  status: "catalog_pending" | "failed" | "processing" | "succeeded",
  options: { catalogRunId?: string; error?: string; workoutCount?: number } = {},
): Promise<void> => {
  const now = new Date().toISOString();
  await db
    .prepare(
      `insert into netkeiba_training_day_sync_state (
         race_date, status, catalog_run_id, workout_count, error_message, attempted_at, completed_at
       ) values (?, ?, ?, ?, ?, ?, ?)
       on conflict(race_date) do update set
         status = excluded.status,
         catalog_run_id = coalesce(excluded.catalog_run_id, netkeiba_training_day_sync_state.catalog_run_id),
         workout_count = case when excluded.workout_count > 0 then excluded.workout_count else netkeiba_training_day_sync_state.workout_count end,
         error_message = excluded.error_message,
         attempted_at = excluded.attempted_at,
         completed_at = excluded.completed_at`,
    )
    .bind(
      date,
      status,
      options.catalogRunId ?? null,
      options.workoutCount ?? 0,
      options.error ?? null,
      now,
      status === "succeeded" ? now : null,
    )
    .run();
};

const loadSyncState = (db: D1Database, date: string): Promise<SyncStateRow | null> =>
  db
    .prepare(
      "select status, attempted_at, error_message from netkeiba_training_day_sync_state where race_date = ?",
    )
    .bind(date)
    .first<SyncStateRow>();

const isOlderThan = (attemptedAt: string, seconds: number): boolean =>
  Date.parse(attemptedAt) < Date.now() - seconds * 1000;

const isStale = (attemptedAt: string): boolean =>
  isOlderThan(attemptedAt, PROCESSING_STALE_SECONDS);

const shouldStart = (row: SyncStateRow | null): boolean => {
  if (row === null) return true;
  if (row.status === "failed") return isOlderThan(row.attempted_at, FAILED_RETRY_GAP_SECONDS);
  if (row.status === "succeeded") return false;
  return isStale(row.attempted_at);
};

// The race that blocked the previous attempt (no workouts yet, or a workout
// whose horse is not in the Catalog entries yet) usually still blocks; probe it
// alone (2 page fetches) before re-fetching every race of the day.
const probePreviousBlocker = async (
  env: Env,
  date: string,
  input: {
    entries: readonly CatalogEntry[];
    links: readonly PremiumLinkRow[];
    previousError: string | null;
  },
): Promise<void> => {
  const blockedRaceKey = BLOCKER_ERROR_PATTERN.exec(input.previousError ?? "")?.[1];
  const blocker = input.links.find((link) => link.race_key === blockedRaceKey);
  if (blocker === undefined) return;
  await buildDayRecords(env, date, [blocker], input.entries, new Date());
};

// True while another run owns the day and may still finish. A redelivered job
// hitting this window must be re-checked after the stale window, because the
// owning run can die without writing a terminal state.
export const isNetkeibaTrainingDayInFlight = async (
  db: D1Database,
  date: string,
): Promise<boolean> => {
  const row = await loadSyncState(db, date);
  if (row === null) return false;
  return IN_FLIGHT_STATUSES.has(row.status) && !isStale(row.attempted_at);
};

export const NETKEIBA_TRAINING_RECHECK_DELAY_SECONDS = PROCESSING_STALE_SECONDS;

export const syncNetkeibaTrainingDay = async (env: Env, date: string): Promise<number> => {
  const state = await loadSyncState(env.REALTIME_DB, date);
  if (!shouldStart(state)) return 0;
  // A day without JRA races (e.g. a NAR-only weekday) has nothing to sync.
  // Skip without touching the Catalog or recording state: on 2026-09-23 the
  // day was marked failed and retried every ~10 minutes, each retry logging
  // "fresh race entries are empty". A JRA day with missing links or entries
  // still fails and retries, so a broken premium discovery stays visible.
  if (!(await hasJraRaces(env.REALTIME_DB, date))) return 0;
  await markState(env.REALTIME_DB, date, "processing");
  try {
    const [links, entries] = await Promise.all([
      listPremiumLinks(env.REALTIME_DB, date),
      loadCatalogEntries(env, date),
    ]);
    if (links.length === 0 || entries.length === 0)
      throw new Error("netkeiba day sync source rows are missing");
    await probePreviousBlocker(env, date, {
      entries,
      links,
      previousError: state?.error_message ?? null,
    });
    const records = await buildDayRecords(env, date, links, entries, new Date());
    const catalogRunId = await stageWithDailySync(env, date, records);
    await markState(env.REALTIME_DB, date, "catalog_pending", {
      catalogRunId,
      workoutCount: records.length,
    });
    await env.REALTIME_JOBS.send(
      { catalogRunId, date, type: "finalize-netkeiba-training-day" },
      { delaySeconds: FINALIZE_DELAY_SECONDS },
    );
    return records.length;
  } catch (error) {
    await markState(env.REALTIME_DB, date, "failed", {
      error: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
};

const dailyRunStatus = async (env: Env, runId: string): Promise<DailyRunStatus> => {
  if (!env.DAILY_KEIBA_SYNC || !env.REALTIME_ADMIN_TOKEN)
    throw new Error("daily-keiba-sync status is not configured");
  const url = new URL("/internal/run-status", DAILY_SYNC_ORIGIN);
  url.searchParams.set("runId", runId);
  const response = await env.DAILY_KEIBA_SYNC.fetch(
    new Request(url, { headers: { Authorization: `Bearer ${env.REALTIME_ADMIN_TOKEN}` } }),
  );
  if (!response.ok) throw new Error(`daily-keiba-sync status failed with HTTP ${response.status}`);
  const value: unknown = await response.json();
  if (!isRecord(value)) throw new Error("Invalid daily-keiba-sync status response");
  if (value.catalog_ready !== undefined && typeof value.catalog_ready !== "boolean")
    throw new Error("Invalid daily-keiba-sync Catalog readiness");
  return {
    // An older deployment only proves readiness after its complete pipeline succeeds.
    // Explicit false from the new projection must never fall back to aggregate success.
    catalog_ready: value.catalog_ready ?? value.status === "succeeded",
    error_stage: value.error_stage === null ? null : requireString(value.error_stage, "errorStage"),
    status: requireString(value.status, "status"),
  };
};

const purgeCatalogTrainingCache = async (
  env: Env,
  date: string,
  link: PremiumLinkRow,
): Promise<void> => {
  if (!env.R2_CATALOG_INGESTION_TOKEN) throw new Error("R2 Catalog ingestion token is missing");
  const parts = raceParts(link.race_key);
  const url = new URL("/admin/purge", CATALOG_ORIGIN);
  url.searchParams.set("date", date);
  url.searchParams.set("keibajoCode", parts.keibajoCode);
  url.searchParams.set("raceBango", parts.raceBango);
  url.searchParams.set("kind", "race-trainings");
  const response = await env.PC_KEIBA_R2_CATALOG.fetch(
    new Request(url, {
      headers: { Authorization: `Bearer ${env.R2_CATALOG_INGESTION_TOKEN}` },
      method: "POST",
    }),
  );
  if (!response.ok) throw new Error(`R2 Catalog purge failed with HTTP ${response.status}`);
};

const WORKOUT_TIME_FIELDS = [
  "timeGokei10f",
  "timeGokei9f",
  "timeGokei8f",
  "timeGokei7f",
  "timeGokei6f",
  "timeGokei5f",
  "timeGokei4f",
  "timeGokei3f",
  "timeGokei2f",
  "lapTime10f",
  "lapTime9f",
  "lapTime8f",
  "lapTime7f",
  "lapTime6f",
  "lapTime5f",
  "lapTime4f",
  "lapTime3f",
  "lapTime2f",
  "lapTime1f",
] as const;

const hasWorkoutData = (value: unknown): boolean => {
  if (!isRecord(value) || !Array.isArray(value.trainings) || value.trainings.length === 0)
    return false;
  const coverage = new Map<string, boolean>();
  const timedHorses = new Set<string>();
  for (const training of value.trainings) {
    if (!isRecord(training) || typeof training.umaban !== "string" || training.umaban.length === 0)
      return false;
    const hasDatedWorkout =
      typeof training.chokyoNengappi === "string" && /^\d{8}$/u.test(training.chokyoNengappi);
    const hasRecordedTime = WORKOUT_TIME_FIELDS.some((field) => {
      const fieldValue = training[field];
      return typeof fieldValue === "string" && fieldValue !== "" && fieldValue !== "0000";
    });
    const timed = hasDatedWorkout && hasRecordedTime;
    if (timed) timedHorses.add(training.umaban);
    const placeholder =
      training.trainingType === "-" &&
      training.trainingDataSource === "jra" &&
      training.chokyoNengappi === "" &&
      training.chokyoJikoku === "" &&
      !hasRecordedTime;
    coverage.set(training.umaban, (coverage.get(training.umaban) ?? false) || timed || placeholder);
  }
  return timedHorses.size > 0 && Array.from(coverage.values()).every(Boolean);
};

const hasDataTop = (value: unknown): boolean =>
  isRecord(value) && Array.isArray(value.dataTopHorses) && value.dataTopHorses.length > 0;

const warmViewerRace = async (env: Env, link: PremiumLinkRow): Promise<void> => {
  if (!env.PC_KEIBA_VIEWER) throw new Error("Viewer service binding is missing");
  const parts = raceParts(link.race_key);
  const date = parts.date;
  const basePath = `/api/races/${date.slice(0, 4)}/${date.slice(4, 6)}/${date.slice(6, 8)}/${parts.keibajoCode}/${parts.raceBango}/sections`;
  // A plain GET (no __cacheWarm) returns the JSON body that is validated below.
  // The viewer answers __cacheWarm with an empty 204, which broke JSON parsing.
  const responses = await Promise.all(
    ["training", "premium-data-top"].map((section) =>
      env.PC_KEIBA_VIEWER!.fetch(new Request(new URL(`${basePath}/${section}`, VIEWER_ORIGIN))),
    ),
  );
  const payloads = await Promise.all(
    responses.map(async (response) => {
      if (!response.ok) throw new Error(`Viewer cache warm failed with HTTP ${response.status}`);
      return response.json();
    }),
  );
  if (!hasWorkoutData(payloads[0]) || !hasDataTop(payloads[1]))
    throw new Error(`Viewer cache warm returned incomplete data for ${link.race_key}`);
};

const refreshRaceTraining = async (env: Env, date: string, link: PremiumLinkRow): Promise<void> => {
  await purgeCatalogTrainingCache(env, date, link);
  const parts = raceParts(link.race_key);
  const bust = await triggerRaceCacheBust(
    env,
    {
      keibajoCode: parts.keibajoCode,
      mmdd: date.slice(4),
      raceBango: parts.raceBango,
      scope: "training",
      source: "jra",
      year: date.slice(0, 4),
    },
    { waitForCompletion: true },
  );
  if (bust.status !== "ok") throw new Error(`Viewer cache bust failed for ${link.race_key}`);
  await warmViewerRace(env, link);
};

// One race's failure must not leave the other races on the day with stale
// training caches, so every race is attempted and the first failure is rethrown.
const purgeAndWarm = async (env: Env, date: string): Promise<void> => {
  const links = await listPremiumLinks(env.REALTIME_DB, date);
  const outcomes = await mapWithConcurrency(links, (link) =>
    refreshRaceTraining(env, date, link).then(
      () => null,
      (error: unknown) => error,
    ),
  );
  const firstFailure = outcomes.find((outcome) => outcome !== null);
  if (firstFailure !== undefined) throw firstFailure;
};

export const finalizeNetkeibaTrainingDay = async (
  env: Env,
  date: string,
  catalogRunId: string,
): Promise<string> => {
  const run = await dailyRunStatus(env, catalogRunId);
  if (run.catalog_ready) {
    try {
      await purgeAndWarm(env, date);
      await markState(env.REALTIME_DB, date, "succeeded");
      return "succeeded";
    } catch (error) {
      await markState(env.REALTIME_DB, date, "failed", {
        error: error instanceof Error ? error.message : String(error),
      });
      throw error;
    }
  }
  if (run.status.includes("failed") || run.error_stage !== null) {
    await markState(env.REALTIME_DB, date, "failed", {
      error: run.error_stage ?? "daily-keiba-sync failed",
    });
    throw new Error("daily-keiba-sync netkeiba training run failed");
  }
  await env.REALTIME_JOBS.send(
    { catalogRunId, date, type: "finalize-netkeiba-training-day" },
    { delaySeconds: FINALIZE_DELAY_SECONDS },
  );
  return run.status === "succeeded" ? "catalog_pending" : run.status;
};

export const netkeibaTrainingDaySyncInternals = {
  hasDataTop,
  hasWorkoutData,
  parseCatalogEntries,
  raceParts,
};
