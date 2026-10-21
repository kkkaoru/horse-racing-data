#!/usr/bin/env bun
// Run with bun. Pull the race-dated training tables from Neon into local PostgreSQL.
//
//   bun run training:pull-neon                       # all PULL_TABLES
//   bun run training:pull-neon -- --dry-run          # read and report, write nothing
//   bun run training:pull-neon -- --tables jvd_se,nvd_se --lookback-days 30
//
// Reads NEON_PRIMARY_URL from the repo-root .env and DATABASE_URL from this app's .env
// (both loaded by the package script). Logic and tests live in src/neon-pull/core.ts.
import pg from "pg";
import {
  DEFAULT_INSERT_BATCH_ROWS,
  DEFAULT_LOOKBACK_DAYS,
  PULL_TABLES,
  runPull,
  type PullDatabase,
  type PullTarget,
  type Row,
} from "../src/neon-pull/core";

interface CliOptions {
  tables: readonly string[];
  lookbackDays: number;
  dryRun: boolean;
}

const argValue = (argv: readonly string[], name: string): string | undefined => {
  const index = argv.indexOf(name);
  return index >= 0 ? argv[index + 1] : undefined;
};

const parseArgs = (argv: readonly string[]): CliOptions => ({
  tables: argValue(argv, "--tables")?.split(",").filter(Boolean) ?? PULL_TABLES,
  lookbackDays: Number(argValue(argv, "--lookback-days") ?? DEFAULT_LOOKBACK_DAYS),
  dryRun: argv.includes("--dry-run"),
});

const requireEnv = (name: string): string => {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
};

const asDatabase = (client: pg.PoolClient): PullDatabase => ({
  query: async (sql, params) =>
    (await client.query<Row>(sql, params ? [...params] : undefined)).rows,
});

const asTarget = (client: pg.PoolClient, dryRun: boolean): PullTarget => ({
  query: async (sql, params) =>
    dryRun && sql.startsWith("insert ")
      ? []
      : (await client.query<Row>(sql, params ? [...params] : undefined)).rows,
  transaction: async (work) => {
    await client.query("begin");
    try {
      const result = await work();
      await client.query(dryRun ? "rollback" : "commit");
      return result;
    } catch (error) {
      await client.query("rollback");
      throw error;
    }
  },
});

const main = async (): Promise<void> => {
  const options = parseArgs(process.argv.slice(2));
  const neon = new pg.Pool({ connectionString: requireEnv("NEON_PRIMARY_URL"), max: 1 });
  const local = new pg.Pool({ connectionString: requireEnv("DATABASE_URL"), max: 1 });
  const [neonClient, localClient] = await Promise.all([neon.connect(), local.connect()]);
  try {
    const results = await runPull(asDatabase(neonClient), asTarget(localClient, options.dryRun), {
      tables: options.tables,
      lookbackDays: options.lookbackDays,
      batchRows: DEFAULT_INSERT_BATCH_ROWS,
    });
    console.log(JSON.stringify({ dryRun: options.dryRun, results }, null, 2));
  } finally {
    neonClient.release();
    localClient.release();
    await Promise.all([neon.end(), local.end()]);
  }
};

await main();
