import "pg-cloudflare";
import { Pool } from "pg";

import type { Env } from "./types";

interface PgRaceRow {
  hasso_jikoku: string | null;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  kyosomei_hondai: string | null;
  race_bango: string;
}

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
    });
  }
  return pool;
};

export const fetchNarRacesByDate = async (env: Env, targetDate: string): Promise<PgRaceRow[]> => {
  const result = await getPool(env).query<PgRaceRow>(
    `
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
    `,
    [targetDate.slice(0, 4), targetDate.slice(4, 8)],
  );
  return result.rows;
};
