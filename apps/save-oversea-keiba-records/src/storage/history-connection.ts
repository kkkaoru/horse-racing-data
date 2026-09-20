// This file runs with Bun. Connections are explicit and never logged.
import { createHash } from "node:crypto";
import { Pool, type PoolClient } from "pg";
import { createHistoryDatabase, type HistoryDatabase } from "./history-database";
import type { PostgresPool, PostgresPoolClient } from "./pg-client";

export interface HistoryConnection {
  readonly targetFingerprint: string;
  readonly database: HistoryDatabase;
  readonly close: () => Promise<void>;
}
export type HistoryDatabaseTarget = "local" | "production";
const ENVIRONMENT_KEYS: Readonly<Record<HistoryDatabaseTarget, string>> = {
  local: "OVERSEA_HISTORY_LOCAL_DATABASE_URL",
  production: "OVERSEA_HISTORY_PRODUCTION_DATABASE_URL",
};
const TLS_MODES: ReadonlySet<string> = new Set(["require", "verify-ca", "verify-full"]);
const CONNECTION_TIMEOUT_MS: number = 10000;
const STATEMENT_TIMEOUT_MS: number = 60000;
const POOL_SIZE: number = 1;

const connectionString = (
  env: Readonly<Record<string, string | undefined>>,
  target: HistoryDatabaseTarget,
): string => {
  const value: string | undefined = env[ENVIRONMENT_KEYS[target]];
  if (!value) throw new Error("An explicit private history database URL is required.");
  const url: URL = new URL(value);
  if (
    (url.protocol !== "postgres:" && url.protocol !== "postgresql:") ||
    !url.hostname ||
    !url.username ||
    url.pathname.length < 2 ||
    url.hash !== ""
  ) {
    throw new Error("History database URL is invalid.");
  }
  const sslMode: string | null = url.searchParams.get("sslmode");
  if (target === "production" && (sslMode === null || !TLS_MODES.has(sslMode))) {
    throw new Error("Production history database requires an explicit TLS mode.");
  }
  return value;
};

const queryOn =
  (target: Pick<PoolClient, "query">): PostgresPoolClient["query"] =>
  async (text, values) => {
    const result = await target.query<Record<string, unknown>>(
      text,
      values === undefined ? undefined : [...values],
    );
    return { rowCount: result.rowCount, rows: result.rows };
  };

export const createHistoryConnection = (
  env: Readonly<Record<string, string | undefined>>,
  target: HistoryDatabaseTarget,
): HistoryConnection => {
  const url: string = connectionString(env, target);
  const native: Pool = new Pool({
    connectionString: url,
    max: POOL_SIZE,
    connectionTimeoutMillis: CONNECTION_TIMEOUT_MS,
    statement_timeout: STATEMENT_TIMEOUT_MS,
  });
  const pool: PostgresPool = {
    query: queryOn(native),
    end: (): Promise<void> => native.end(),
    connect: async (): Promise<PostgresPoolClient> => {
      const client: PoolClient = await native.connect();
      return { query: queryOn(client), release: (): void => client.release() };
    },
  };
  return {
    targetFingerprint: createHash("sha256").update(JSON.stringify({ target, url })).digest("hex"),
    database: createHistoryDatabase(pool),
    close: pool.end,
  };
};
