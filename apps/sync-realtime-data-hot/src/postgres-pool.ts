// Run with bun. Lazy Postgres pool over Hyperdrive binding, used by the hot
// worker's self-discovery path that lists today's races directly from
// Hyperdrive instead of relying on the legacy worker's forward push.

import "pg-cloudflare";
import { Pool } from "pg";

import type { Env } from "./types";

const DEFAULT_POOL_SIZE = 4;
let pool: Pool | null = null;

export const getHotPool = (env: Env): Pool => {
  if (pool !== null) return pool;
  if (!env.HYPERDRIVE) {
    throw new Error("HYPERDRIVE binding is required for hot self-discovery");
  }
  pool = new Pool({
    connectionString: env.HYPERDRIVE.connectionString,
    max: DEFAULT_POOL_SIZE,
  });
  return pool;
};
