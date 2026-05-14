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
const defaultExcludedLogTables = new Set(["finish_position_tuning_random_trials"]);

type CommandResult = {
  stdout: string;
  stderr: string;
};

type CliOptions = {
  allowLogTables: Set<string>;
  indexesOnly: boolean;
};

function parseArgs(args: string[]): CliOptions {
  const options: CliOptions = {
    allowLogTables: new Set(),
    indexesOnly: false,
  };
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === "--indexes-only") {
      options.indexesOnly = true;
    } else if (arg === "--allow-log-table" || arg === "--allow-log-tables") {
      const value = args[index + 1];
      if (value === undefined) {
        throw new Error(`${arg} requires a comma-separated table list.`);
      }
      for (const tableName of value.split(",")) {
        const trimmed = tableName.trim();
        if (trimmed !== "") {
          options.allowLogTables.add(trimmed);
        }
      }
      index += 1;
    } else if (arg !== "--help" && arg !== "-h") {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }
  return options;
}

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

function redactSecrets(value: string): string {
  return value
    .replaceAll(/postgres(?:ql)?:\/\/[^\s"']+/g, "postgresql://[redacted]")
    .replaceAll(/npg_[A-Za-z0-9_-]+/g, "npg_[redacted]");
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
        reject(
          new Error(
            redactSecrets(`${command} ${args.join(" ")} exited with code ${code}\n${stderr}`),
          ),
        );
      }
    });

    if (input !== undefined) {
      child.stdin.end(input);
    } else {
      child.stdin.end();
    }
  });
}

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", "'\\''")}'`;
}

function runCopyPipeline(
  env: Record<string, string | undefined>,
  localCopySql: string,
  neonCopySql: string,
): Promise<void> {
  return new Promise((resolvePipeline, reject) => {
    const command = [
      "docker",
      "compose",
      "--env-file",
      shellQuote(envPath),
      "--project-directory",
      shellQuote(appDir),
      "exec",
      "-T",
      "postgres",
      "psql",
      "-U",
      '"$POSTGRES_USER"',
      "-d",
      '"$POSTGRES_DB"',
      "-At",
      "-F",
      "\"$(printf '\\t')\"",
      "-c",
      '"$LOCAL_COPY_SQL"',
      "|",
      "docker",
      "run",
      "--rm",
      "-i",
      "postgres:18-alpine",
      "psql",
      '"$NEON_DIRECT_DATABASE_URL"',
      "-v",
      "ON_ERROR_STOP=1",
      "-q",
      "-c",
      '"$NEON_COPY_SQL"',
    ].join(" ");

    const child = spawn("bash", ["-o", "pipefail", "-c", command], {
      env: {
        ...process.env,
        POSTGRES_USER: env.POSTGRES_USER ?? "",
        POSTGRES_DB: env.POSTGRES_DB ?? "",
        NEON_DIRECT_DATABASE_URL: env.NEON_DIRECT_DATABASE_URL ?? "",
        LOCAL_COPY_SQL: localCopySql,
        NEON_COPY_SQL: neonCopySql,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });

    const stderrChunks: Buffer[] = [];
    child.stderr.on("data", (chunk: Buffer) => stderrChunks.push(chunk));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolvePipeline();
        return;
      }
      reject(
        new Error(
          `COPY pipeline exited with code ${code}\n${Buffer.concat(stderrChunks).toString("utf8")}`,
        ),
      );
    });
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
  console.log(`Usage: bun run ./scripts/push-neon-sync.ts [--indexes-only] [--allow-log-table TABLE[,TABLE...]]

Environment:
  REPLICA_SYNC_TABLES             Comma-separated table list. Empty means all PK tables.
  REPLICA_SYNC_CONCURRENCY        Number of tables to sync in parallel. Default: auto.
                                  Use "auto" to choose per dependency level from row counts.
  REPLICA_SYNC_APPLY_MODE         "replace" or "upsert". Default: replace.
                                  replace truncates the Neon table and inserts staged rows in one
                                  transaction; upsert preserves row-level conflict behavior.
  REPLICA_SYNC_SKIP_UNCHANGED     Skip tables whose local and Neon row checksums match.
                                  Default: true.
  REPLICA_SYNC_COPY_BATCH_ROWS    Optional rows per COPY batch. Empty means one COPY per table.
  REPLICA_SYNC_DELETE             Delete Neon rows missing locally. Default: true.
  REPLICA_SYNC_INDEXES            Apply sql/analytics-indexes.sql to Neon after rows sync.
                                  Default: true.
  NEON_CONNECT_TIMEOUT_SECONDS    Wait timeout for Neon cold start. Default: 120.
  NEON_CONNECT_RETRY_SECONDS      Retry interval while Neon is warming. Default: 5.

Options:
  --allow-log-table TABLE          Allow syncing a log/experiment table that is excluded by
                                  default. Repeat or pass comma-separated names.
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

function copyBatchRows(env: Record<string, string | undefined>): number | undefined {
  if (env.REPLICA_SYNC_COPY_BATCH_ROWS === undefined || env.REPLICA_SYNC_COPY_BATCH_ROWS === "") {
    return undefined;
  }
  const parsed = Number(env.REPLICA_SYNC_COPY_BATCH_ROWS);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : undefined;
}

function shouldSkipUnchanged(env: Record<string, string | undefined>): boolean {
  return env.REPLICA_SYNC_SKIP_UNCHANGED !== "false";
}

function buildTableChecksumSql(table: TableMetadata): string {
  const quotedTable = quoteIdentifier(table.tableName);
  const rowJsonSql = "row_to_json(row_data)::text";
  const hashSql = `md5(${rowJsonSql})`;

  return `
SELECT
  count(*)::text,
  coalesce(sum((('x' || substr(${hashSql}, 1, 16))::bit(64)::bigint)::numeric), 0)::text,
  coalesce(sum((('x' || substr(${hashSql}, 17, 16))::bit(64)::bigint)::numeric), 0)::text
FROM (
  SELECT ${table.columnList}
  FROM public.${quotedTable}
) AS row_data;
`.trim();
}

async function loadTableChecksum(
  env: Record<string, string | undefined>,
  table: TableMetadata,
  target: "local" | "neon",
): Promise<string> {
  const sql = buildTableChecksumSql(table);
  const result =
    target === "local"
      ? await runCommand("docker", dockerComposeArgs(env, sql))
      : await runCommand(
          "docker",
          neonPsqlArgs(env, ["-v", "ON_ERROR_STOP=1", "-qAt", "-F", "\t", "-c", sql]),
        );

  return result.stdout.trim();
}

async function tableIsUnchanged(
  env: Record<string, string | undefined>,
  table: TableMetadata,
): Promise<boolean> {
  const [localChecksum, neonChecksum] = await Promise.all([
    loadTableChecksum(env, table, "local"),
    loadTableChecksum(env, table, "neon"),
  ]);

  return localChecksum === neonChecksum;
}

async function loadDependencyEdges(
  env: Record<string, string | undefined>,
): Promise<DependencyEdge[]> {
  const config = buildConfig(env);
  const sql = buildDependencySql(config.selectedTables);
  const { stdout } = await runCommand("docker", dockerComposeArgs(env, sql));
  return parseDependencyEdges(stdout);
}

async function syncTableWithPsql(
  env: Record<string, string | undefined>,
  table: TableMetadata,
  deleteMissingRows: boolean,
  applyMode: "replace" | "upsert",
): Promise<void> {
  const quotedTable = quoteIdentifier(table.tableName);
  const stageTableName = `replica_sync_stage_${process.pid}_${table.tableName.replaceAll(/[^A-Za-z0-9_]/g, "_")}`;
  const neonSql = buildNeonApplySql(table, deleteMissingRows, stageTableName, false, applyMode);
  const batchRows = copyBatchRows(env);

  if (shouldSkipUnchanged(env) && (await tableIsUnchanged(env, table))) {
    console.log(`[${formatNow()}] Skipping public.${table.tableName}: unchanged`);
    return;
  }

  await runCommand(
    "docker",
    neonPsqlArgs(env, ["-v", "ON_ERROR_STOP=1", "-q"]),
    neonSql.preCopySql,
  );

  try {
    if (batchRows === undefined) {
      const localCopySql = `COPY (SELECT ${table.columnList} FROM public.${quotedTable}) TO STDOUT WITH (FORMAT csv, NULL '\\N');`;
      await runCopyPipeline(env, localCopySql, neonSql.copySql).catch((error: unknown) => {
        const message = error instanceof Error ? error.message : String(error);
        throw new Error(`Failed to copy ${table.tableName}\n${message}`);
      });
    } else {
      const { stdout: countOutput } = await runCommand(
        "docker",
        dockerComposeArgs(env, `SELECT count(*) FROM public.${quotedTable};`),
      );
      const rowCount = Number(countOutput.trim());
      for (let offset = 0; offset < rowCount; offset += batchRows) {
        const localCopySql = `COPY (SELECT ${table.columnList} FROM public.${quotedTable} ORDER BY ${table.primaryKeyList} LIMIT ${batchRows} OFFSET ${offset}) TO STDOUT WITH (FORMAT csv, NULL '\\N');`;
        await runCopyPipeline(env, localCopySql, neonSql.copySql).catch((error: unknown) => {
          const message = error instanceof Error ? error.message : String(error);
          throw new Error(`Failed to copy ${table.tableName} offset=${offset}\n${message}`);
        });
      }
    }
    await runCommand(
      "docker",
      neonPsqlArgs(env, ["-v", "ON_ERROR_STOP=1", "-q"]),
      neonSql.postCopySql,
    );
  } catch (error) {
    await runCommand("docker", neonPsqlArgs(env, ["-q"]), neonSql.cleanupSql).catch(
      () => undefined,
    );
    throw error;
  }
}

function formatNow(): string {
  return new Date().toLocaleString("sv-SE", {
    timeZone: "Asia/Tokyo",
    hour12: false,
    timeZoneName: "short",
  });
}

function formatDuration(seconds: number): string {
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const remainingSeconds = seconds % 60;
  if (hours > 0) {
    return `${hours}h${minutes}m${remainingSeconds}s`;
  }
  if (minutes > 0) {
    return `${minutes}m${remainingSeconds}s`;
  }
  return `${remainingSeconds}s`;
}

function formatTableList(tableNames: string[]): string {
  return tableNames.length > 0 ? tableNames.join(", ") : "(none)";
}

function filterDefaultExcludedLogTables<T extends { tableName: string }>(
  rows: T[],
  allowLogTables: Set<string>,
): T[] {
  const excluded = rows.filter(
    (row) => defaultExcludedLogTables.has(row.tableName) && !allowLogTables.has(row.tableName),
  );
  if (excluded.length > 0) {
    console.log(
      `Skipping log tables by default: ${formatTableList(excluded.map((row) => row.tableName))}. Use --allow-log-table to sync them.`,
    );
  }
  return rows.filter(
    (row) => !defaultExcludedLogTables.has(row.tableName) || allowLogTables.has(row.tableName),
  );
}

function filterDependencyEdgesByTables(
  edges: DependencyEdge[],
  tables: TableMetadata[],
): DependencyEdge[] {
  const tableNames = new Set(tables.map((table) => table.tableName));
  return edges.filter(
    (edge) => tableNames.has(edge.childTable) && tableNames.has(edge.parentTable),
  );
}

function printProgressTargets(event: {
  completedTableNames: string[];
  runningTableNames: string[];
  remainingTableNames: string[];
  etaSeconds: number;
}): void {
  console.log(
    `  completed_targets=${event.completedTableNames.length}: ${formatTableList(event.completedTableNames)}`,
  );
  console.log(
    `  running_targets=${event.runningTableNames.length}: ${formatTableList(event.runningTableNames)}`,
  );
  console.log(
    `  remaining_targets=${event.remainingTableNames.length}: ${formatTableList(event.remainingTableNames)}`,
  );
  console.log(`  estimated_time_remaining=${formatDuration(event.etaSeconds)}`);
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
      printProgressTargets(event);
      break;
    case "table-done":
      console.log(
        `[${formatNow()}] Done public.${event.tableName}: level=${event.dependencyLevel}, level_concurrency=${event.levelConcurrency}, table_elapsed=${event.tableElapsedSeconds}s, progress=${event.completedTables}/${event.totalTables}, running_tables=${event.runningTables}, synced_est_rows=${event.syncedEstimatedRows}/${event.totalEstimatedRows}, remaining_tables=${event.remainingTables}, remaining_est_rows=${event.remainingEstimatedRows}, elapsed=${event.elapsedSeconds}s, eta=${event.etaSeconds}s`,
      );
      printProgressTargets(event);
      break;
    case "complete":
      console.log(
        `Push sync completed: tables=${event.totalTables}, estimated_rows=${event.totalEstimatedRows}, elapsed=${event.elapsedSeconds}s`,
      );
      break;
  }
}

async function main(): Promise<void> {
  const cliOptions = parseArgs(process.argv.slice(2));
  if (process.argv.includes("--help") || process.argv.includes("-h")) {
    printUsage();
    return;
  }

  const env = loadEnvironment();
  if (cliOptions.indexesOnly) {
    await syncAnalyticsIndexes(env);
    return;
  }

  const config = buildConfig(env);
  const tables = filterDefaultExcludedLogTables(
    await loadTableMetadata(env),
    cliOptions.allowLogTables,
  );
  const dependencyEdges = filterDependencyEdgesByTables(await loadDependencyEdges(env), tables);

  await runPushSync(
    tables,
    config,
    {
      nowSeconds: () => Math.floor(Date.now() / 1000),
      sleep: (milliseconds) =>
        new Promise((resolveSleep) => setTimeout(resolveSleep, milliseconds)),
      checkNeonReady: () => checkNeonReady(env),
      syncTable: (table) =>
        syncTableWithPsql(env, table, config.deleteMissingRows, config.applyMode),
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
