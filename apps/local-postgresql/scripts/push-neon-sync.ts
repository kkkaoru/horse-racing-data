#!/usr/bin/env bun
import { existsSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import {
  buildConfig,
  buildDependencySql,
  buildMetadataSql,
  buildNeonApplySql,
  parseDependencyEdges,
  parseTableMetadata,
  quoteIdentifier,
  runPushSync,
  type ProgressEvent,
  type TableMetadata,
  type DependencyEdge,
} from "../src/replica-push/core";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const appDir = resolve(scriptDir, "..");
const envPath = resolve(appDir, ".env");
const replicaEnvPath = resolve(appDir, ".env.replica");
const analyticsIndexesPath = resolve(appDir, "sql", "analytics-indexes.sql");

type CommandResult = {
  stdout: string;
  stderr: string;
};

function parseEnvFile(path: string): Record<string, string> {
  const values: Record<string, string> = {};
  const content = readFileSync(path, "utf8");

  for (const rawLine of content.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (line === "" || line.startsWith("#")) {
      continue;
    }

    const match = /^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/.exec(line);
    if (!match) {
      continue;
    }

    const key = match[1]!;
    let value = match[2]!.trim();
    if (
      (value.startsWith("'") && value.endsWith("'")) ||
      (value.startsWith('"') && value.endsWith('"'))
    ) {
      value = value.slice(1, -1);
    }
    values[key] = value;
  }

  return values;
}

function loadEnvironment(): Record<string, string | undefined> {
  if (!existsSync(envPath)) {
    throw new Error(`Missing ${envPath}`);
  }
  if (!existsSync(replicaEnvPath)) {
    throw new Error(`Missing ${replicaEnvPath}`);
  }

  return {
    ...parseEnvFile(envPath),
    ...parseEnvFile(replicaEnvPath),
    ...process.env,
  };
}

function runCommand(command: string, args: string[], input?: string): Promise<CommandResult> {
  return new Promise((resolveCommand, reject) => {
    const child = spawn(command, args, {
      stdio: ["pipe", "pipe", "pipe"],
    });

    const stdoutChunks: Buffer[] = [];
    const stderrChunks: Buffer[] = [];

    child.stdout.on("data", (chunk: Buffer) => stdoutChunks.push(chunk));
    child.stderr.on("data", (chunk: Buffer) => stderrChunks.push(chunk));
    child.on("error", reject);
    child.on("close", (code) => {
      const stdout = Buffer.concat(stdoutChunks).toString("utf8");
      const stderr = Buffer.concat(stderrChunks).toString("utf8");
      if (code === 0) {
        resolveCommand({ stdout, stderr });
      } else {
        reject(new Error(`${command} ${args.join(" ")} exited with code ${code}\n${stderr}`));
      }
    });

    if (input !== undefined) {
      child.stdin.end(input);
    } else {
      child.stdin.end();
    }
  });
}

function dockerComposeArgs(env: Record<string, string | undefined>, sql: string): string[] {
  return [
    "compose",
    "--env-file",
    envPath,
    "--project-directory",
    appDir,
    "exec",
    "-T",
    "postgres",
    "psql",
    "-U",
    env.POSTGRES_USER ?? "",
    "-d",
    env.POSTGRES_DB ?? "",
    "-At",
    "-F",
    "\t",
    "-c",
    sql,
  ];
}

function neonPsqlArgs(env: Record<string, string | undefined>, extraArgs: string[] = []): string[] {
  const neonUrl = env.NEON_DIRECT_DATABASE_URL;
  if (!neonUrl) {
    throw new Error("NEON_DIRECT_DATABASE_URL is required");
  }

  return ["run", "--rm", "-i", "postgres:18-alpine", "psql", neonUrl, ...extraArgs];
}

async function loadTableMetadata(
  env: Record<string, string | undefined>,
): Promise<TableMetadata[]> {
  const config = buildConfig(env);
  const sql = buildMetadataSql(config.selectedTables);
  const { stdout } = await runCommand("docker", dockerComposeArgs(env, sql));
  return parseTableMetadata(stdout);
}

function printUsage(): void {
  console.log(`Usage: bun run ./scripts/push-neon-sync.ts [--indexes-only]

Environment:
  REPLICA_SYNC_TABLES             Comma-separated table list. Empty means all PK tables.
  REPLICA_SYNC_CONCURRENCY        Number of tables to sync in parallel. Default: auto.
                                  Use "auto" to choose per dependency level from row counts.
  REPLICA_SYNC_DELETE             Delete Neon rows missing locally. Default: true.
  REPLICA_SYNC_INDEXES            Apply sql/analytics-indexes.sql to Neon after rows sync.
                                  Default: true.
  NEON_CONNECT_TIMEOUT_SECONDS    Wait timeout for Neon cold start. Default: 120.
  NEON_CONNECT_RETRY_SECONDS      Retry interval while Neon is warming. Default: 5.
`);
}

async function checkNeonReady(env: Record<string, string | undefined>): Promise<boolean> {
  try {
    await runCommand("docker", neonPsqlArgs(env, ["-v", "ON_ERROR_STOP=1", "-qAtc", "select 1"]));
    return true;
  } catch {
    return false;
  }
}

async function loadDependencyEdges(
  env: Record<string, string | undefined>,
): Promise<DependencyEdge[]> {
  const config = buildConfig(env);
  const sql = buildDependencySql(config.selectedTables);
  const { stdout } = await runCommand("docker", dockerComposeArgs(env, sql));
  return parseDependencyEdges(stdout);
}

function syncTableWithPsql(
  env: Record<string, string | undefined>,
  table: TableMetadata,
  deleteMissingRows: boolean,
): Promise<void> {
  return new Promise((resolveSync, reject) => {
    const quotedTable = quoteIdentifier(table.tableName);
    const localCopySql = `COPY (SELECT ${table.columnList} FROM public.${quotedTable}) TO STDOUT WITH (FORMAT csv, NULL '\\N');`;
    const neonSql = buildNeonApplySql(table, deleteMissingRows);

    const local = spawn("docker", dockerComposeArgs(env, localCopySql), {
      stdio: ["ignore", "pipe", "pipe"],
    });
    const neon = spawn("docker", neonPsqlArgs(env, ["-v", "ON_ERROR_STOP=1", "-q"]), {
      stdio: ["pipe", "ignore", "pipe"],
    });

    const errors: string[] = [];
    let settled = false;

    const fail = (error: Error) => {
      if (settled) {
        return;
      }
      settled = true;
      local.kill();
      neon.kill();
      reject(error);
    };

    local.on("error", fail);
    neon.on("error", fail);
    local.stderr.on("data", (chunk: Buffer) => errors.push(`local ${table.tableName}: ${chunk}`));
    neon.stderr.on("data", (chunk: Buffer) => errors.push(`neon ${table.tableName}: ${chunk}`));

    neon.stdin.write(`${neonSql.preCopySql}\n`);
    local.stdout.pipe(neon.stdin, { end: false });
    local.stdout.on("end", () => {
      neon.stdin.end(`${neonSql.postCopySql}\n`);
    });

    let localExitCode: number | null = null;
    let neonExitCode: number | null = null;

    const maybeResolve = () => {
      if (settled || localExitCode === null || neonExitCode === null) {
        return;
      }
      settled = true;
      if (localExitCode === 0 && neonExitCode === 0) {
        resolveSync();
      } else {
        reject(
          new Error(
            `Failed to sync ${table.tableName}: local=${localExitCode}, neon=${neonExitCode}\n${errors.join("")}`,
          ),
        );
      }
    };

    local.on("close", (code) => {
      localExitCode = code;
      maybeResolve();
    });
    neon.on("close", (code) => {
      neonExitCode = code;
      maybeResolve();
    });
  });
}

function formatNow(): string {
  return new Date().toLocaleString("sv-SE", {
    timeZone: "Asia/Tokyo",
    hour12: false,
    timeZoneName: "short",
  });
}

function shouldSyncIndexes(env: Record<string, string | undefined>): boolean {
  return env.REPLICA_SYNC_INDEXES !== "false";
}

async function syncAnalyticsIndexes(env: Record<string, string | undefined>): Promise<void> {
  if (!shouldSyncIndexes(env)) {
    console.log("Skipping Neon analytics index sync: REPLICA_SYNC_INDEXES=false");
    return;
  }
  if (!existsSync(analyticsIndexesPath)) {
    throw new Error(`Missing ${analyticsIndexesPath}`);
  }

  console.log(`[${formatNow()}] Applying analytics indexes to Neon`);
  const sql = readFileSync(analyticsIndexesPath, "utf8");
  await runCommand("docker", neonPsqlArgs(env, ["-v", "ON_ERROR_STOP=1", "-q"]), sql);
  console.log(`[${formatNow()}] Applied analytics indexes to Neon`);
}

function reportProgress(event: ProgressEvent): void {
  switch (event.type) {
    case "start":
      console.log(
        `Starting dependency-aware push sync to Neon: tables=${event.totalTables}, estimated_rows=${event.totalEstimatedRows}, dependency_levels=${event.dependencyLevels}, concurrency=${event.concurrency}`,
      );
      break;
    case "level-start":
      console.log(
        `[${formatNow()}] Starting dependency level ${event.dependencyLevel}: tables=${event.levelTables}, estimated_rows=${event.levelEstimatedRows}, concurrency=${event.concurrency}`,
      );
      break;
    case "neon-wait-start":
      console.log(
        `Waiting for Neon connection: timeout=${event.timeoutSeconds}s, retry=${event.retrySeconds}s`,
      );
      break;
    case "neon-wait-retry":
      console.log(
        `Neon is not ready yet: elapsed=${event.elapsedSeconds}s, retry_in=${event.retrySeconds}s`,
      );
      break;
    case "neon-ready":
      console.log(`Neon is ready: elapsed=${event.elapsedSeconds}s`);
      break;
    case "table-start":
      console.log(
        `[${formatNow()}] Syncing public.${event.tableName}: level=${event.dependencyLevel}, level_concurrency=${event.levelConcurrency}, est_rows=${event.estimatedRows}, running_tables=${event.runningTables}, completed_tables=${event.completedTables}, remaining_tables=${event.remainingTables}, remaining_est_rows=${event.remainingEstimatedRows}, elapsed=${event.elapsedSeconds}s, eta=${event.etaSeconds}s`,
      );
      break;
    case "table-done":
      console.log(
        `[${formatNow()}] Done public.${event.tableName}: level=${event.dependencyLevel}, level_concurrency=${event.levelConcurrency}, table_elapsed=${event.tableElapsedSeconds}s, progress=${event.completedTables}/${event.totalTables}, running_tables=${event.runningTables}, synced_est_rows=${event.syncedEstimatedRows}/${event.totalEstimatedRows}, remaining_tables=${event.remainingTables}, remaining_est_rows=${event.remainingEstimatedRows}, elapsed=${event.elapsedSeconds}s, eta=${event.etaSeconds}s`,
      );
      break;
    case "complete":
      console.log(
        `Push sync completed: tables=${event.totalTables}, estimated_rows=${event.totalEstimatedRows}, elapsed=${event.elapsedSeconds}s`,
      );
      break;
  }
}

async function main(): Promise<void> {
  if (process.argv.includes("--help") || process.argv.includes("-h")) {
    printUsage();
    return;
  }

  const env = loadEnvironment();
  if (process.argv.includes("--indexes-only")) {
    await syncAnalyticsIndexes(env);
    return;
  }

  const config = buildConfig(env);
  const tables = await loadTableMetadata(env);
  const dependencyEdges = await loadDependencyEdges(env);

  await runPushSync(
    tables,
    config,
    {
      nowSeconds: () => Math.floor(Date.now() / 1000),
      sleep: (milliseconds) =>
        new Promise((resolveSleep) => setTimeout(resolveSleep, milliseconds)),
      checkNeonReady: () => checkNeonReady(env),
      syncTable: (table) => syncTableWithPsql(env, table, config.deleteMissingRows),
      report: reportProgress,
    },
    dependencyEdges,
  );
  await syncAnalyticsIndexes(env);
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
