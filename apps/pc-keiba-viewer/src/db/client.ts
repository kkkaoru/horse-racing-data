import "server-only";
import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";

import * as schema from "./schema";

const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
  throw new Error("DATABASE_URL is required.");
}

const globalForDb = globalThis as typeof globalThis & {
  pcKeibaViewerPool?: Pool;
};

const pool =
  globalForDb.pcKeibaViewerPool ??
  new Pool({
    connectionString,
    max: 8,
  });

if (process.env.NODE_ENV !== "production") {
  globalForDb.pcKeibaViewerPool = pool;
}

export const db = drizzle(pool, { schema });
