import "server-only";
import { getCloudflareContext } from "@opennextjs/cloudflare";
import { drizzle } from "drizzle-orm/node-postgres";
import type { NodePgDatabase } from "drizzle-orm/node-postgres";
import { Pool } from "pg";
import { cache } from "react";

import * as schema from "./schema";

const DATABASE_TARGETS = ["local", "neon", "cloudflare"] as const;
type DatabaseTarget = (typeof DATABASE_TARGETS)[number];
type Db = NodePgDatabase<typeof schema>;

const DEFAULT_STATEMENT_TIMEOUT_MS = 15_000;
const DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS = 15_000;

const isDatabaseTarget = (value: string | undefined): value is DatabaseTarget =>
  value === "local" || value === "neon" || value === "cloudflare";

export const getDatabaseTarget = (): DatabaseTarget =>
  isDatabaseTarget(process.env.PC_KEIBA_DATABASE_TARGET)
    ? process.env.PC_KEIBA_DATABASE_TARGET
    : "local";

const getCloudflareConnectionString = (): string => {
  const { env } = getCloudflareContext();
  const { HYPERDRIVE: hyperdrive } = env;

  if (!hyperdrive?.connectionString) {
    throw new Error("HYPERDRIVE binding is required for PC_KEIBA_DATABASE_TARGET=cloudflare.");
  }

  return hyperdrive.connectionString;
};

const getConnectionString = (databaseTarget: DatabaseTarget): string => {
  if (databaseTarget === "cloudflare") {
    return getCloudflareConnectionString();
  }

  const connectionString =
    databaseTarget === "neon"
      ? process.env.DATABASE_URL_NEON
      : (process.env.DATABASE_URL_LOCAL ?? process.env.DATABASE_URL);

  if (!connectionString) {
    const envName = databaseTarget === "neon" ? "DATABASE_URL_NEON" : "DATABASE_URL_LOCAL";
    throw new Error(`${envName} is required for PC_KEIBA_DATABASE_TARGET=${databaseTarget}.`);
  }

  return connectionString;
};

const globalForDb = globalThis as typeof globalThis & {
  pcKeibaViewerPools?: Partial<Record<DatabaseTarget, Pool>>;
  pcKeibaViewerDbs?: Partial<Record<DatabaseTarget, Db>>;
};

const getPoolOptions = (databaseTarget: DatabaseTarget): ConstructorParameters<typeof Pool>[0] => {
  const statementTimeoutMs = Number(process.env.PC_KEIBA_DB_STATEMENT_TIMEOUT_MS);
  const idleInTransactionTimeoutMs = Number(process.env.PC_KEIBA_DB_IDLE_IN_TRANSACTION_TIMEOUT_MS);
  return {
    connectionString: getConnectionString(databaseTarget),
    idle_in_transaction_session_timeout: Number.isFinite(idleInTransactionTimeoutMs)
      ? idleInTransactionTimeoutMs
      : DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS,
    max: databaseTarget === "cloudflare" ? 1 : 8,
    statement_timeout: Number.isFinite(statementTimeoutMs)
      ? statementTimeoutMs
      : DEFAULT_STATEMENT_TIMEOUT_MS,
  };
};

const createDb = (databaseTarget: DatabaseTarget): Db => {
  const pool = new Pool(getPoolOptions(databaseTarget));

  return drizzle(pool, { schema });
};

const getCloudflareDb = cache((): Db => createDb("cloudflare"));

export const getDb = (): Db => {
  const databaseTarget = getDatabaseTarget();

  if (databaseTarget === "cloudflare") {
    return getCloudflareDb();
  }

  const existingDb = globalForDb.pcKeibaViewerDbs?.[databaseTarget];

  if (existingDb) {
    return existingDb;
  }

  const existingPool = globalForDb.pcKeibaViewerPools?.[databaseTarget];
  const pool = existingPool ?? new Pool(getPoolOptions(databaseTarget));
  const createdDb = drizzle(pool, { schema });

  globalForDb.pcKeibaViewerPools = {
    ...globalForDb.pcKeibaViewerPools,
    [databaseTarget]: pool,
  };
  globalForDb.pcKeibaViewerDbs = {
    ...globalForDb.pcKeibaViewerDbs,
    [databaseTarget]: createdDb,
  };

  return createdDb;
};
