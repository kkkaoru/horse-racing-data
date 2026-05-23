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
const DEFAULT_LOCAL_DATABASE_URL =
  "postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing";

const isDatabaseTarget = (value: string | undefined): value is DatabaseTarget =>
  value === "local" || value === "neon" || value === "cloudflare";

const isNodeDevelopmentRuntime = (): boolean =>
  process.env.NODE_ENV === "development" && typeof process.versions?.node === "string";

const shouldUseLocalDbInNodeDev = (databaseTarget: DatabaseTarget): boolean =>
  databaseTarget === "cloudflare" &&
  isNodeDevelopmentRuntime() &&
  process.env.PC_KEIBA_ALLOW_CLOUDFLARE_DB_IN_NEXT_DEV !== "1";

export const getDatabaseTarget = (): DatabaseTarget =>
  isDatabaseTarget(process.env.PC_KEIBA_DATABASE_TARGET)
    ? shouldUseLocalDbInNodeDev(process.env.PC_KEIBA_DATABASE_TARGET)
      ? "local"
      : process.env.PC_KEIBA_DATABASE_TARGET
    : "local";

const normalizeLocalConnectionString = (connectionString: string): string => {
  try {
    const url = new URL(connectionString);
    if (url.hostname === "localhost") {
      url.hostname = "127.0.0.1";
    }
    return url.toString();
  } catch {
    return connectionString;
  }
};

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

  if (databaseTarget === "local") {
    return normalizeLocalConnectionString(
      process.env.DATABASE_URL_LOCAL ?? DEFAULT_LOCAL_DATABASE_URL,
    );
  }

  const connectionString = process.env.DATABASE_URL_NEON;

  if (!connectionString) {
    throw new Error("DATABASE_URL_NEON is required for PC_KEIBA_DATABASE_TARGET=neon.");
  }

  return connectionString;
};

const globalForDb = globalThis as typeof globalThis & {
  pcKeibaViewerPools?: Partial<Record<DatabaseTarget, Pool>>;
  pcKeibaViewerDbs?: Partial<Record<DatabaseTarget, Db>>;
  pcKeibaViewerConnectionStrings?: Partial<Record<DatabaseTarget, string>>;
};

type PoolOptions = NonNullable<ConstructorParameters<typeof Pool>[0]>;

const getPoolOptions = (databaseTarget: DatabaseTarget): PoolOptions => {
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

export const getDb = (): Db => {
  const databaseTarget = getDatabaseTarget();

  if (databaseTarget === "cloudflare") {
    return getCloudflareDb();
  }

  const poolOptions = getPoolOptions(databaseTarget);
  const existingDb = globalForDb.pcKeibaViewerDbs?.[databaseTarget];
  const existingConnectionString = globalForDb.pcKeibaViewerConnectionStrings?.[databaseTarget];

  if (existingDb && existingConnectionString === poolOptions.connectionString) {
    return existingDb;
  }

  const existingPool = globalForDb.pcKeibaViewerPools?.[databaseTarget];
  if (existingPool && existingConnectionString !== poolOptions.connectionString) {
    void existingPool.end().catch(() => {
      // Best effort during dev HMR when the selected DB connection changed.
    });
  }

  const pool =
    existingPool && existingConnectionString === poolOptions.connectionString
      ? existingPool
      : new Pool(poolOptions);
  const createdDb = drizzle(pool, { schema });

  globalForDb.pcKeibaViewerPools = {
    ...globalForDb.pcKeibaViewerPools,
    [databaseTarget]: pool,
  };
  globalForDb.pcKeibaViewerDbs = {
    ...globalForDb.pcKeibaViewerDbs,
    [databaseTarget]: createdDb,
  };
  globalForDb.pcKeibaViewerConnectionStrings = {
    ...globalForDb.pcKeibaViewerConnectionStrings,
    [databaseTarget]: poolOptions.connectionString,
  };

  return createdDb;
};

const createDb = (databaseTarget: DatabaseTarget): Db => {
  const pool = new Pool(getPoolOptions(databaseTarget));

  return drizzle(pool, { schema });
};

const getCloudflareDb = cache((): Db => createDb("cloudflare"));

export const getPgPool = (): Pool => {
  const databaseTarget = getDatabaseTarget();
  if (databaseTarget !== "cloudflare") {
    getDb();
    const cachedPool = globalForDb.pcKeibaViewerPools?.[databaseTarget];
    if (cachedPool) {
      return cachedPool;
    }
  }
  return (getCloudflareDb() as unknown as { $client: Pool }).$client;
};
