// Run with bun. Lazy Postgres pool over Hyperdrive binding.
//
// Self-healing: Hyperdrive may recycle TCP sockets (idle close, pooler restart,
// network drop). When any pg.Pool client errors, we drop the cached singleton
// so the next getFeaturesPool() call constructs a fresh Pool with live sockets.

import "pg-cloudflare";
import { Pool } from "pg";

import type { Env } from "../types";

// Pool size was raised 12 -> 24 because burst recompute traffic (multiple JRA
// races forwarded in parallel by forward-race-for-features) plus 60s query
// timeout drained all 12 clients and surfaced as
// "timeout exceeded when trying to connect" (pg-pool wait-queue timeout).
// CONNECTION_TIMEOUT_MS was bumped 5_000 -> 15_000 so a Hyperdrive cold-start
// or origin reconnect does not flap the singleton during a single tick.
// QUERY_TIMEOUT_MS was raised 60_000 -> 120_000 on 2026-07-04: queue
// consumers rebuilding past (finished) races run result-history joins that
// exceeded 60s under concurrent morning Neon load, surfacing as
// "Query read timeout" (wrangler tail showed the fetchAllRaceFeatures ->
// buildRaceFeatures exception at wallTime 60246ms). Normal (non-past) builds
// finish in 5-15s, so this only extends the ceiling for the slow path. The
// queue consumer's wall-time budget is I/O-bound waiting on Postgres, so
// doubling the timeout is safe.
const DEFAULT_POOL_SIZE = 24;
const IDLE_TIMEOUT_MS = 10_000;
const CONNECTION_TIMEOUT_MS = 15_000;
const QUERY_TIMEOUT_MS = 120_000;

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
