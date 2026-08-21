#!/usr/bin/env bun
// Run with Bun: bun run --cwd apps/local-postgresql scrape:netkeiba-training.

import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { runNetkeibaTrainingImport } from "../src/netkeiba-training/core";
import {
  readEnvValue,
  resolveRealtimeAdminToken,
  resolveRealtimeDiscoveryDates,
} from "../src/replica-push/realtime-discovery";

export interface PsqlCommandOptions {
  command: readonly string[];
  sql: string;
}

export type PsqlCommandRunner = (options: PsqlCommandOptions) => Promise<string>;

interface LocalPsqlConfig {
  containerName: string;
  database: string;
  user: string;
}

const scriptDir = dirname(fileURLToPath(import.meta.url));
const appDir = resolve(scriptDir, "..");
const localEnvPath = resolve(appDir, ".env");
const migrationPath = resolve(appDir, "sql/20260822000000_create_netkeiba_training_workouts.sql");
const realtimeDevVarsPath = resolve(appDir, "../sync-realtime-data/.dev.vars");
const DEFAULT_API_BASE_URL = "https://sync-realtime-data.kkk4oru.com";
const DEFAULT_CONTAINER_NAME = "horse-racing-local-postgresql";
const DEFAULT_POSTGRES_DATABASE = "horse_racing";
const DEFAULT_POSTGRES_USER = "horse_racing";

export const buildPsqlCommand = (config: LocalPsqlConfig): readonly string[] => [
  "container",
  "exec",
  "-i",
  config.containerName,
  "psql",
  "-X",
  "-v",
  "ON_ERROR_STOP=1",
  "-U",
  config.user,
  "-d",
  config.database,
  "-Atq",
];

const runPsqlCommand: PsqlCommandRunner = async (options) => {
  const subprocess = Bun.spawn([...options.command], {
    stderr: "pipe",
    stdin: "pipe",
    stdout: "pipe",
  });
  subprocess.stdin.write(options.sql);
  subprocess.stdin.end();
  const [exitCode, stdout, stderr] = await Promise.all([
    subprocess.exited,
    new Response(subprocess.stdout).text(),
    new Response(subprocess.stderr).text(),
  ]);
  if (exitCode === 0) return stdout;
  throw new Error(`Local PostgreSQL command failed (${exitCode}): ${stderr.trim()}`);
};

export const executeLocalSql = async (
  sql: string,
  config: LocalPsqlConfig,
  runner: PsqlCommandRunner,
): Promise<string> => runner({ command: buildPsqlCommand(config), sql });

const loadOptionalFile = async (path: string): Promise<string> =>
  Bun.file(path)
    .text()
    .catch(() => "");

if (import.meta.main) {
  try {
    const [localEnv, migrationSql] = await Promise.all([
      loadOptionalFile(localEnvPath),
      Bun.file(migrationPath).text(),
    ]);
    const explicitToken = process.env.REALTIME_ADMIN_TOKEN;
    const devVars =
      explicitToken === undefined || explicitToken === ""
        ? await loadOptionalFile(realtimeDevVarsPath)
        : "";
    const token = resolveRealtimeAdminToken(explicitToken, devVars);
    const config: LocalPsqlConfig = {
      containerName: process.env.LOCAL_POSTGRES_CONTAINER_NAME || DEFAULT_CONTAINER_NAME,
      database:
        process.env.POSTGRES_DB ||
        readEnvValue(localEnv, "POSTGRES_DB") ||
        DEFAULT_POSTGRES_DATABASE,
      user:
        process.env.POSTGRES_USER ||
        readEnvValue(localEnv, "POSTGRES_USER") ||
        DEFAULT_POSTGRES_USER,
    };
    const count = await runNetkeibaTrainingImport({
      apiBaseUrl: process.env.SYNC_REALTIME_DATA_BASE_URL || DEFAULT_API_BASE_URL,
      dates: resolveRealtimeDiscoveryDates(process.env.SYNC_REALTIME_DATA_DATE, new Date()),
      executeSql: (sql) => executeLocalSql(sql, config, runPsqlCommand),
      fetcher: fetch,
      log: console.log,
      migrationSql,
      now: () => new Date(),
      token,
    });
    console.log(`Netkeiba training import completed successfully: ${count} workouts stored.`);
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
