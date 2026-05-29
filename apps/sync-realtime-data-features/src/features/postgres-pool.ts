// Run with bun. Lazy Postgres pool over Hyperdrive binding.

import "pg-cloudflare";
import { Pool } from "pg";

import type { Env } from "../types";

const DEFAULT_POOL_SIZE = 12;
let pool: Pool | null = null;

export const getFeaturesPool = (env: Env): Pool => {
  if (pool !== null) return pool;
  if (!env.HYPERDRIVE) {
    throw new Error("HYPERDRIVE binding is required for features build");
  }
  pool = new Pool({
    connectionString: env.HYPERDRIVE.connectionString,
    max: DEFAULT_POOL_SIZE,
  });
  return pool;
};
