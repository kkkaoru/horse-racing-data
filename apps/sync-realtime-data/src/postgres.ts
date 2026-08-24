// run with: bun
import "pg-cloudflare";
import { Client } from "pg";

import {
  getCachedDailyPgRows,
  setCachedDailyPgRows,
  type DailyPgCacheSource,
} from "./daily-pg-cache";
import type { HyperdriveBinding } from "./types";

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

interface PostgresEnv {
  DATABASE_TARGET?: string;
  DATABASE_URL_NEON?: string;
  HYPERDRIVE?: HyperdriveBinding;
}

const getConnectionString = (env: PostgresEnv): string => {
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

interface FetchByDateArgs {
  source: DailyPgCacheSource;
  sql: string;
  targetDate: string;
}

const runDailyPgFetch = async (env: PostgresEnv, args: FetchByDateArgs): Promise<PgRaceRow[]> => {
  const cached = getCachedDailyPgRows<PgRaceRow>({
    source: args.source,
    targetDate: args.targetDate,
  });
  if (cached) return [...cached];
  const client = new Client({
    connectionString: getConnectionString(env),
  });
  try {
    await client.connect();
    const result = await client.query<PgRaceRow>(args.sql, [
      args.targetDate.slice(0, 4),
      args.targetDate.slice(4, 8),
    ]);
    setCachedDailyPgRows<PgRaceRow>(
      { source: args.source, targetDate: args.targetDate },
      result.rows,
    );
    return result.rows;
  } catch (error) {
    console.error(
      JSON.stringify({
        error: error instanceof Error ? error.message : String(error),
        message: "Postgres daily race fetch failed",
        stage: `postgres.fetch-${args.source}-races`,
        targetDate: args.targetDate,
      }),
    );
    throw error;
  } finally {
    await client.end();
  }
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

export const fetchNarRacesByDate = (env: PostgresEnv, targetDate: string): Promise<PgRaceRow[]> =>
  runDailyPgFetch(env, { source: "nar", sql: NAR_RACES_SQL, targetDate });

export const fetchJraRacesByDate = (env: PostgresEnv, targetDate: string): Promise<PgRaceRow[]> =>
  runDailyPgFetch(env, { source: "jra", sql: JRA_RACES_SQL, targetDate });
