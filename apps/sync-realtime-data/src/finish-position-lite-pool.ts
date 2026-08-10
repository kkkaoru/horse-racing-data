// Run with bun. Lazily reused Postgres pool for the finish-position lite
// inference path. Separate from the worker's existing pool so the
// connection limit can be tuned independently.

import "pg-cloudflare";
import { Pool, type ClientBase } from "pg";

import { formatErrorLogLine } from "./format-error";
import type { Env } from "./types";

// Raised from 12 → 24 (2026-06-04 incident) to absorb concurrent
// running-style inference + retry storms. Hyperdrive fan-in caps upstream
// PG connection usage, so 24 here is safe against Neon's plan max.
const DEFAULT_POOL_SIZE = 24;
const SET_DEFAULT_TRANSACTION_READ_ONLY_SQL = "SET default_transaction_read_only TO off";
let pool: Pool | null = null;
let writePool: Pool | null = null;

const getConnectionString = (env: Env): string => {
  if (env.HYPERDRIVE?.connectionString) return env.HYPERDRIVE.connectionString;
  if (env.DATABASE_URL_NEON) return env.DATABASE_URL_NEON;
  throw new Error("HYPERDRIVE or DATABASE_URL_NEON is required for finish-position pool");
};

const WRITE_CONNECTION_REQUIRED_ERROR =
  "DATABASE_URL_NEON or NEON_DATABASE_URL is required for finish-position write pool";

const getWriteConnectionString = (env: Env): string => {
  if (env.DATABASE_URL_NEON) return env.DATABASE_URL_NEON;
  if (env.NEON_DATABASE_URL) return env.NEON_DATABASE_URL;
  throw new Error(WRITE_CONNECTION_REQUIRED_ERROR);
};

// Belt-and-suspenders only: clear session default_transaction_read_only on new
// TCP connects. Do not throw from onConnect — pg-pool destroys the client.
const onWritePoolConnect = async (client: ClientBase): Promise<void> => {
  try {
    await client.query(SET_DEFAULT_TRANSACTION_READ_ONLY_SQL);
  } catch (error: unknown) {
    console.error(
      formatErrorLogLine(
        "Finish-position write pool SET failed",
        { setting: "default_transaction_read_only" },
        error,
      ),
    );
  }
};

export const getFinishPositionPool = (env: Env): Pool => {
  if (pool !== null) return pool;
  pool = new Pool({
    connectionString: getConnectionString(env),
    max: DEFAULT_POOL_SIZE,
  });
  return pool;
};

export const getFinishPositionWritePool = (env: Env): Pool => {
  if (writePool !== null) return writePool;
  writePool = new Pool({
    connectionString: getWriteConnectionString(env),
    max: DEFAULT_POOL_SIZE,
    onConnect: onWritePoolConnect,
  });
  return writePool;
};
