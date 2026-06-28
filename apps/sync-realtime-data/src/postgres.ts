// run with: bun
import "pg-cloudflare";
import { Pool } from "pg";

import {
  getCachedDailyPgRows,
  setCachedDailyPgRows,
  type DailyPgCacheSource,
} from "./daily-pg-cache";
import type { Env } from "./types";

interface PgRaceRow {
  hasso_jikoku: string | null;
  kaisai_kai?: string | null;
  kaisai_nichime?: string | null;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  kyosomei_hondai: string | null;
  race_bango: string;
}

// Idle pool clients keep the Neon compute clock alive. Releasing them after
// a short idle window lets Neon autosuspend faster between cron ticks.
const POOL_IDLE_TIMEOUT_MS = 10 * 1000;

let pool: Pool | null = null;

const getConnectionString = (env: Env): string => {
  if (env.DATABASE_TARGET === "cloudflare" && env.HYPERDRIVE?.connectionString) {
    return env.HYPERDRIVE.connectionString;
  }
  if (env.HYPERDRIVE?.connectionString) {
    return env.HYPERDRIVE.connectionString;
  }
  if (env.DATABASE_URL_NEON) {
    return env.DATABASE_URL_NEON;
  }
  throw new Error("HYPERDRIVE or DATABASE_URL_NEON is required.");
};

const getPool = (env: Env): Pool => {
  if (!pool) {
    pool = new Pool({
      connectionString: getConnectionString(env),
      max: 2,
      idleTimeoutMillis: POOL_IDLE_TIMEOUT_MS,
    });
  }
  return pool;
};

interface FetchByDateArgs {
  source: DailyPgCacheSource;
  sql: string;
  targetDate: string;
}

const runDailyPgFetch = async (env: Env, args: FetchByDateArgs): Promise<PgRaceRow[]> => {
  const cached = getCachedDailyPgRows<PgRaceRow>({
    source: args.source,
    targetDate: args.targetDate,
  });
  if (cached) return [...cached];
  const result = await getPool(env).query<PgRaceRow>(args.sql, [
    args.targetDate.slice(0, 4),
    args.targetDate.slice(4, 8),
  ]);
  setCachedDailyPgRows<PgRaceRow>(
    { source: args.source, targetDate: args.targetDate },
    result.rows,
  );
  return result.rows;
};

const NAR_RACES_SQL = `
  select
    kaisai_nen,
    kaisai_tsukihi,
    keibajo_code,
    race_bango,
    hasso_jikoku,
    kyosomei_hondai
  from nvd_ra
  where kaisai_nen = $1
    and kaisai_tsukihi = $2
    and hasso_jikoku is not null
  order by hasso_jikoku asc, keibajo_code asc, race_bango asc
`;

const JRA_RACES_SQL = `
  select
    kaisai_nen,
    kaisai_tsukihi,
    keibajo_code,
    race_bango,
    hasso_jikoku,
    kyosomei_hondai,
    kaisai_kai,
    kaisai_nichime
  from jvd_ra
  where kaisai_nen = $1
    and kaisai_tsukihi = $2
    and hasso_jikoku is not null
  order by hasso_jikoku asc, keibajo_code asc, race_bango asc
`;

export const fetchNarRacesByDate = (env: Env, targetDate: string): Promise<PgRaceRow[]> =>
  runDailyPgFetch(env, { source: "nar", sql: NAR_RACES_SQL, targetDate });

export const fetchJraRacesByDate = (env: Env, targetDate: string): Promise<PgRaceRow[]> =>
  runDailyPgFetch(env, { source: "jra", sql: JRA_RACES_SQL, targetDate });
