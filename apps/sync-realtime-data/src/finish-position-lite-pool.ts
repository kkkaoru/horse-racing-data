// Run with bun. Lazily reused Postgres pool for the finish-position lite
// inference path. Separate from the worker's existing pool so the
// connection limit can be tuned independently.

import "pg-cloudflare";
import { Pool } from "pg";

import type { Env } from "./types";

// Raised from 12 → 24 (2026-06-04 incident) to absorb concurrent
// running-style inference + retry storms. Hyperdrive fan-in caps upstream
// PG connection usage, so 24 here is safe against Neon's plan max.
const DEFAULT_POOL_SIZE = 24;
let pool: Pool | null = null;

const getConnectionString = (env: Env): string => {
  if (env.HYPERDRIVE?.connectionString) return env.HYPERDRIVE.connectionString;
  if (env.DATABASE_URL_NEON) return env.DATABASE_URL_NEON;
  throw new Error("HYPERDRIVE or DATABASE_URL_NEON is required for finish-position pool");
};

export const getFinishPositionPool = (env: Env): Pool => {
  if (pool !== null) return pool;
  pool = new Pool({
    connectionString: getConnectionString(env),
    max: DEFAULT_POOL_SIZE,
  });
  return pool;
};
