import "server-only";
import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";

import * as schema from "./schema";

const DATABASE_TARGETS = ["local", "neon"] as const;

type DatabaseTarget = (typeof DATABASE_TARGETS)[number];

const isDatabaseTarget = (value: string | undefined): value is DatabaseTarget =>
  value === "local" || value === "neon";

const databaseTarget = isDatabaseTarget(process.env.PC_KEIBA_DATABASE_TARGET)
  ? process.env.PC_KEIBA_DATABASE_TARGET
  : "local";

const connectionString =
  databaseTarget === "neon"
    ? process.env.DATABASE_URL_NEON
    : (process.env.DATABASE_URL_LOCAL ?? process.env.DATABASE_URL);

if (!connectionString) {
  const envName = databaseTarget === "neon" ? "DATABASE_URL_NEON" : "DATABASE_URL_LOCAL";
  throw new Error(`${envName} is required for PC_KEIBA_DATABASE_TARGET=${databaseTarget}.`);
}

const globalForDb = globalThis as typeof globalThis & {
  pcKeibaViewerPools?: Partial<Record<DatabaseTarget, Pool>>;
};

const existingPool = globalForDb.pcKeibaViewerPools?.[databaseTarget];
const pool = existingPool ?? new Pool({ connectionString, max: 8 });

if (process.env.NODE_ENV !== "production") {
  globalForDb.pcKeibaViewerPools = {
    ...globalForDb.pcKeibaViewerPools,
    [databaseTarget]: pool,
  };
}

export const db = drizzle(pool, { schema });
