// Run with bun. Lazy Postgres pool over Hyperdrive binding.
//
// Self-healing: Hyperdrive may recycle TCP sockets (idle close, pooler restart,
// network drop). When any pg.Pool client errors, we drop the cached singleton
// so the next getFeaturesPool() call constructs a fresh Pool with live sockets.

import "pg-cloudflare";
import { Pool } from "pg";

import type { Env } from "../types";

const DEFAULT_POOL_SIZE = 12;
const IDLE_TIMEOUT_MS = 10_000;
const CONNECTION_TIMEOUT_MS = 5_000;
const QUERY_TIMEOUT_MS = 60_000;

let pool: Pool | null = null;

export const getFeaturesPool = (env: Env): Pool => {
  if (pool !== null) return pool;
  if (!env.HYPERDRIVE) {
    throw new Error("HYPERDRIVE binding is required for features build");
  }
  const newPool = new Pool({
    connectionString: env.HYPERDRIVE.connectionString,
    max: DEFAULT_POOL_SIZE,
    idleTimeoutMillis: IDLE_TIMEOUT_MS,
    connectionTimeoutMillis: CONNECTION_TIMEOUT_MS,
    query_timeout: QUERY_TIMEOUT_MS,
  });
  newPool.on("error", (error) => {
    console.error("[features-pool] pool error, resetting singleton", error);
    pool = null;
  });
  pool = newPool;
  return pool;
};
