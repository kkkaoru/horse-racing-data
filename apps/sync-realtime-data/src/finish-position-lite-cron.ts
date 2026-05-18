// Run with bun. Cron entrypoint that scans Postgres for upcoming/recent
// races without a `finish_position_inference_state` row, inserts pending
// rows in D1, and enqueues per-race jobs onto the finish-position queue.
// The queue consumer then runs runFinishPositionLiteInference per race.

import type { Pool } from "pg";

import { getFinishPositionPool } from "./finish-position-lite-pool";
import type { Env, FinishPositionLiteJob } from "./types";

const PENDING_STATUS = "pending";
const CRON_LOOKBACK_DAYS = 3;
const CRON_LOOKAHEAD_DAYS = 7;
const MAX_RACES_PER_TICK = 80;
const SUPPORTED_SOURCES: ReadonlyArray<"jra" | "nar"> = ["jra", "nar"];

interface RaceRow {
  source: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
}

const RACE_SCAN_QUERY = `
  select distinct
    source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
  from race_entry_corner_features
  where source = $1
    and race_date >= $2
    and race_date <= $3
  order by kaisai_nen desc, kaisai_tsukihi desc, keibajo_code, race_bango
  limit $4
`;

const padDate = (value: number): string => String(value).padStart(2, "0");

const formatRaceDate = (now: Date, offsetDays: number): string => {
  const offsetMs = offsetDays * 24 * 60 * 60 * 1000;
  const target = new Date(now.getTime() + offsetMs);
  const jstOffsetMinutes = 9 * 60;
  const jst = new Date(target.getTime() + jstOffsetMinutes * 60 * 1000);
  return `${jst.getUTCFullYear()}${padDate(jst.getUTCMonth() + 1)}${padDate(jst.getUTCDate())}`;
};

const buildRaceKey = (row: RaceRow): string =>
  `${row.source}:${row.kaisai_nen}${row.kaisai_tsukihi}:${row.keibajo_code}:${row.race_bango}`;

const fetchCandidateRaces = async (
  pool: Pool,
  source: "jra" | "nar",
  fromDate: string,
  toDate: string,
): Promise<RaceRow[]> => {
  const result = await pool.query<RaceRow>(RACE_SCAN_QUERY, [
    source,
    fromDate,
    toDate,
    MAX_RACES_PER_TICK,
  ]);
  return result.rows;
};

const filterRacesNeedingInference = async (
  db: D1Database,
  candidates: ReadonlyArray<RaceRow>,
): Promise<RaceRow[]> => {
  if (candidates.length === 0) return [];
  const keys = candidates.map(buildRaceKey);
  const placeholders = keys.map(() => "?").join(",");
  const existing = await db
    .prepare(`select race_key from finish_position_inference_state where race_key in (${placeholders})`)
    .bind(...keys)
    .all<{ race_key: string }>();
  const seen = new Set(existing.results.map((row) => row.race_key));
  return candidates.filter((row) => !seen.has(buildRaceKey(row)));
};

const insertPendingState = async (
  db: D1Database,
  rows: ReadonlyArray<RaceRow>,
  modelVersion: string,
  nowIso: string,
): Promise<void> => {
  const statements = rows.map((row) =>
    db
      .prepare(
        `insert or ignore into finish_position_inference_state (
          race_key, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          status, model_version, attempted_at
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .bind(
        buildRaceKey(row),
        row.source,
        row.kaisai_nen,
        row.kaisai_tsukihi,
        row.keibajo_code,
        row.race_bango,
        PENDING_STATUS,
        modelVersion,
        nowIso,
      ),
  );
  if (statements.length === 0) return;
  await db.batch([...statements]);
};

const enqueueJobs = async (
  queue: Queue<FinishPositionLiteJob>,
  rows: ReadonlyArray<RaceRow>,
  modelVersion: string,
  nowIso: string,
): Promise<void> => {
  await Promise.all(
    rows.map((row) =>
      queue.send({
        kaisaiNen: row.kaisai_nen,
        kaisaiTsukihi: row.kaisai_tsukihi,
        keibajoCode: row.keibajo_code,
        modelVersion,
        predictedAt: nowIso,
        raceBango: row.race_bango,
        source: row.source,
        type: "finish-position-lite-infer",
      }),
    ),
  );
};

const resolveModelVersion = (source: "jra" | "nar"): string => `${source}-lite-lgbm-v1.0`;

const isInferenceEnabled = (env: Env): boolean => env.FINISH_POSITION_LITE_ENABLED === "1";

interface CronSummary {
  source: "jra" | "nar";
  scanned: number;
  enqueued: number;
}

const runForSource = async (
  env: Env,
  source: "jra" | "nar",
  fromDate: string,
  toDate: string,
  nowIso: string,
): Promise<CronSummary> => {
  const pool = getFinishPositionPool(env);
  const candidates = await fetchCandidateRaces(pool, source, fromDate, toDate);
  const needed = await filterRacesNeedingInference(env.REALTIME_DB, candidates);
  if (needed.length === 0) return { enqueued: 0, scanned: candidates.length, source };
  const modelVersion = resolveModelVersion(source);
  await insertPendingState(env.REALTIME_DB, needed, modelVersion, nowIso);
  await enqueueJobs(env.FINISH_POSITION_LITE_JOBS, needed, modelVersion, nowIso);
  return { enqueued: needed.length, scanned: candidates.length, source };
};

export const runFinishPositionLiteCronTick = async (
  env: Env,
  now: Date,
): Promise<ReadonlyArray<CronSummary>> => {
  if (!isInferenceEnabled(env)) return [];
  const fromDate = formatRaceDate(now, -CRON_LOOKBACK_DAYS);
  const toDate = formatRaceDate(now, CRON_LOOKAHEAD_DAYS);
  const nowIso = now.toISOString();
  return Promise.all(SUPPORTED_SOURCES.map((source) => runForSource(env, source, fromDate, toDate, nowIso)));
};
