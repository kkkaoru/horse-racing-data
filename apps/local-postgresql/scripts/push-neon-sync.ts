#!/usr/bin/env bun
import { existsSync, mkdirSync, openSync, readFileSync, writeSync, closeSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn, type ChildProcess } from "node:child_process";
import {
  buildConfig,
  buildDependencySql,
  buildFingerprintSql,
  buildIncrementalApplySql,
  buildIncrementalCopyFromSql,
  incrementalComparatorForTimestampColumn,
  buildMetadataSql,
  buildNeonApplySql,
  buildTableProfileSql,
  buildTimestampFingerprintSql,
  parseDependencyEdges,
  parseFingerprintLine,
  parseTableMetadata,
  parseTableProfiles,
  pkExpression,
  quoteIdentifier,
  runPushSync,
  shouldRefreshInclusiveIncrementalMarker,
  timestampKeyExpression,
  type ProgressEvent,
  type PushSyncConfig,
  type TableMetadata,
  type TableProfile,
  type DependencyEdge,
} from "../src/replica-push/core";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const appDir = resolve(scriptDir, "..");
const envPath = resolve(appDir, ".env");
const replicaEnvPath = resolve(appDir, ".env.replica");
const analyticsIndexesPath = resolve(appDir, "sql", "analytics-indexes.sql");
const defaultExcludedLogTables = new Set([
  "finish_position_tuning_random_trials",
  "race_finish_position_features",
]);
const DEFAULT_OPERATION_TIMEOUT_SECONDS = 600;
const OPERATION_TIMEOUT_ENV_KEY = "REPLICA_SYNC_OPERATION_TIMEOUT_SECONDS";

type CommandResult = {
  stdout: string;
  stderr: string;
};

type CliOptions = {
  allowLogTables: Set<string>;
  indexesOnly: boolean;
  verbose: boolean;
  logFile: string | null;
  noLogFile: boolean;
};

function parseArgs(args: string[]): CliOptions {
  const options: CliOptions = {
    allowLogTables: new Set(),
    indexesOnly: false,
    verbose: false,
    logFile: null,
    noLogFile: false,
  };
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === "--indexes-only") {
      options.indexesOnly = true;
    } else if (arg === "--verbose" || arg === "-v") {
      options.verbose = true;
    } else if (arg === "--no-log-file") {
      options.noLogFile = true;
    } else if (arg === "--log-file") {
      const value = args[index + 1];
      if (value === undefined) {
        throw new Error(`${arg} requires a path argument.`);
      }
      options.logFile = value;
      index += 1;
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

let verboseLogging = false;
let logFileFd: number | null = null;

function setVerboseLogging(value: boolean): void {
  verboseLogging = value;
}

function openLogFile(path: string): void {
  const dir = dirname(path);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  logFileFd = openSync(path, "a");
}

function closeLogFile(): void {
  if (logFileFd !== null) {
    closeSync(logFileFd);
    logFileFd = null;
  }
}

function defaultLogPath(): string {
  return resolve(appDir, "tmp", "push-neon-sync.log");
}

function formatBigNumber(value: number): string {
  if (value >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(2)}M`;
  }
  if (value >= 1_000) {
    return `${(value / 1_000).toFixed(1)}k`;
  }
  return String(value);
}

function formatPercent(synced: number, total: number): string {
  if (total <= 0) {
    return "100.0%";
  }
  return `${((synced / total) * 100).toFixed(1)}%`;
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

function applyTimeoutEnv(env: Record<string, string | undefined>): void {
  const value = env[OPERATION_TIMEOUT_ENV_KEY];
  if (value !== undefined && process.env[OPERATION_TIMEOUT_ENV_KEY] === undefined) {
    process.env[OPERATION_TIMEOUT_ENV_KEY] = value;
  }
}

function resolveOperationTimeoutMs(): number {
  const raw = process.env[OPERATION_TIMEOUT_ENV_KEY];
  const fallback = DEFAULT_OPERATION_TIMEOUT_SECONDS * 1000;
  if (raw === undefined || raw === "") return fallback;
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed * 1000 : fallback;
}

function killProcessGroup(child: ChildProcess): void {
  if (child.pid === undefined || child.killed) return;
  try {
    process.kill(-child.pid, "SIGKILL");
  } catch {
    try {
      child.kill("SIGKILL");
    } catch {
      // Already exited.
    }
  }
}

function armTimeout(child: ChildProcess, label: string, reject: (error: Error) => void): () => void {
  const timeoutMs = resolveOperationTimeoutMs();
  const handle = setTimeout(() => {
    killProcessGroup(child);
    reject(new Error(`${label} timed out after ${Math.round(timeoutMs / 1000)}s and was killed`));
  }, timeoutMs);
  return () => clearTimeout(handle);
}

function runCommand(command: string, args: string[], input?: string): Promise<CommandResult> {
  return new Promise((resolveCommand, reject) => {
    const child = spawn(command, args, {
      stdio: ["pipe", "pipe", "pipe"],
      detached: true,
    });

    const stdoutChunks: Buffer[] = [];
    const stderrChunks: Buffer[] = [];
    const label = redactSecrets(`${command} ${args.join(" ")}`);
    const cancelTimeout = armTimeout(child, label, reject);

    child.stdout.on("data", (chunk: Buffer) => stdoutChunks.push(chunk));
    child.stderr.on("data", (chunk: Buffer) => stderrChunks.push(chunk));
    child.on("error", (error) => {
      cancelTimeout();
      reject(error);
    });
    child.on("close", (code) => {
      cancelTimeout();
      const stdout = Buffer.concat(stdoutChunks).toString("utf8");
      const stderr = Buffer.concat(stderrChunks).toString("utf8");
      if (code === 0) {
        resolveCommand({ stdout, stderr });
      } else {
        reject(new Error(redactSecrets(`${label} exited with code ${code}\n${stderr}`)));
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
      detached: true,
    });

    const stderrChunks: Buffer[] = [];
    const cancelTimeout = armTimeout(child, "COPY pipeline", reject);
    child.stderr.on("data", (chunk: Buffer) => stderrChunks.push(chunk));
    child.on("error", (error) => {
      cancelTimeout();
      reject(error);
    });
    child.on("close", (code) => {
      cancelTimeout();
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
  REPLICA_SYNC_OPERATION_TIMEOUT_SECONDS
                                  Per-operation timeout that kills any docker/psql child that
                                  hangs (e.g. occasional docker run --rm cleanup hangs on Colima).
                                  Default: 600.

Options:
  --verbose, -v                    Use the legacy multi-line progress format.
                                  Default: compact one-line-per-event format.
  --log-file PATH                  Append progress to PATH (default: tmp/push-neon-sync.log).
  --no-log-file                    Disable file logging (stdout only).
  --indexes-only                   Skip data sync, only apply analytics indexes.
  --allow-log-table TABLE          Allow syncing a log/experiment table that is excluded by
                                  default. Repeat or pass comma-separated names.

Strategy environment variables:
  REPLICA_SYNC_STRATEGY            "auto" (default) routes per-table to
                                  timestamp-incremental / pk-incremental / full-replace based
                                  on PG metadata. "full" forces the legacy full-replace path.
  REPLICA_SYNC_SMALL_TABLE_MAX_ROWS Tables with reltuples <= this run full-replace.
                                  Default: 10000.
  REPLICA_SYNC_UPDATE_CHURN_MIN_TUPLES Tables with n_tup_upd >= this are treated as mutable.
                                  Default: 1000.

Default exclusions (local-only training tables): finish_position_tuning_random_trials,
race_finish_position_features. Use --allow-log-table to opt them back in.

Real-time progress:
  tail -f apps/local-postgresql/tmp/push-neon-sync.log

  Or use the companion status checker for a side-by-side row-count report:
  bun run apps/local-postgresql/scripts/push-neon-status.ts
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

async function loadTableProfileMap(
  env: Record<string, string | undefined>,
  config: PushSyncConfig,
): Promise<Map<string, TableProfile>> {
  const sql = buildTableProfileSql(config.selectedTables);
  const { stdout } = await runCommand("docker", dockerComposeArgs(env, sql));
  const profiles = parseTableProfiles(stdout, config.strategyThresholds, config.strategyMode);
  return new Map(profiles.map((profile) => [profile.tableName, profile]));
}

async function loadFingerprint(
  env: Record<string, string | undefined>,
  table: TableMetadata,
  target: "local" | "neon",
  tsColumn: string | null,
): Promise<{ count: number; marker: string }> {
  const sql =
    tsColumn === null ? buildFingerprintSql(table) : buildTimestampFingerprintSql(table, tsColumn);
  const result =
    target === "local"
      ? await runCommand("docker", dockerComposeArgs(env, sql))
      : await runCommand(
          "docker",
          neonPsqlArgs(env, ["-v", "ON_ERROR_STOP=1", "-qAt", "-F", "\t", "-c", sql]),
        );
  return parseFingerprintLine(result.stdout);
}

async function syncTableWithPsql(
  env: Record<string, string | undefined>,
  table: TableMetadata,
  deleteMissingRows: boolean,
  applyMode: "replace" | "upsert",
  profile: TableProfile | undefined,
): Promise<void> {
  if (profile && profile.strategy !== "full-replace") {
    await syncTableIncrementally(env, table, profile);
    return;
  }
  await syncTableFullReplace(env, table, deleteMissingRows, applyMode);
}

async function syncTableIncrementally(
  env: Record<string, string | undefined>,
  table: TableMetadata,
  profile: TableProfile,
): Promise<void> {
  const tsColumn = profile.strategy === "timestamp-incremental" ? profile.timestampColumn : null;
  const [localFp, neonFp] = await Promise.all([
    loadFingerprint(env, table, "local", tsColumn),
    loadFingerprint(env, table, "neon", tsColumn),
  ]);
  const refreshInclusiveMarker = shouldRefreshInclusiveIncrementalMarker(tsColumn);
  if (
    localFp.count === neonFp.count &&
    localFp.marker === neonFp.marker &&
    !refreshInclusiveMarker
  ) {
    writeLine(
      `[${formatNow()}] ⊘ ${table.tableName}: unchanged via ${profile.strategy} (count=${localFp.count}, marker=${truncateMarker(localFp.marker)})`,
    );
    return;
  }
  if (localFp.count === neonFp.count && localFp.marker === neonFp.marker) {
    writeLine(
      `[${formatNow()}] ↻ ${table.tableName}: refreshing latest ${tsColumn} marker via ${profile.strategy} (count=${localFp.count}, marker=${truncateMarker(localFp.marker)})`,
    );
  }
  const keyExpression = tsColumn === null ? pkExpression(table) : timestampKeyExpression(tsColumn);
  const stageTableName = `replica_sync_stage_inc_${process.pid}_${table.tableName.replaceAll(/[^A-Za-z0-9_]/g, "_")}`;
  const incSql = buildIncrementalApplySql(table, stageTableName, false);
  const localCopySql = buildIncrementalCopyFromSql(table, {
    keyExpression,
    neonMarker: neonFp.marker,
    comparator: incrementalComparatorForTimestampColumn(tsColumn),
  });

  await runCommand("docker", neonPsqlArgs(env, ["-v", "ON_ERROR_STOP=1", "-q"]), incSql.preCopySql);
  try {
    await runCopyPipeline(env, localCopySql, incSql.copySql).catch((error: unknown) => {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`Failed to incremental-copy ${table.tableName}\n${message}`);
    });
    await runCommand(
      "docker",
      neonPsqlArgs(env, ["-v", "ON_ERROR_STOP=1", "-q"]),
      incSql.postCopySql,
    );
  } catch (error) {
    await runCommand("docker", neonPsqlArgs(env, ["-q"]), incSql.cleanupSql).catch(() => undefined);
    throw error;
  }

  const verifyFp = await loadFingerprint(env, table, "neon", tsColumn);
  if (verifyFp.count !== localFp.count) {
    writeLine(
      `[${formatNow()}] ⚠ ${table.tableName}: incremental verify mismatch (local=${localFp.count}, neon=${verifyFp.count}) — falling back to full-replace`,
    );
    await syncTableFullReplace(env, table, true, "upsert");
    return;
  }
  writeLine(
    `[${formatNow()}] ✚ ${table.tableName}: ${profile.strategy} synced (count ${neonFp.count} → ${verifyFp.count}, marker=${truncateMarker(localFp.marker)})`,
  );
}

function truncateMarker(marker: string): string {
  if (marker.length <= 40) return marker;
  return `${marker.slice(0, 37)}...`;
}

async function syncTableFullReplace(
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
    writeLine(`[${formatNow()}] ⊘ ${table.tableName}: unchanged (skip)`);
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
  if (verboseLogging) {
    reportProgressVerbose(event);
    return;
  }
  reportProgressCompact(event);
}

function reportProgressCompact(event: ProgressEvent): void {
  switch (event.type) {
    case "start":
      writeLine(
        `[${formatNow()}] start: ${event.totalTables} tables, ${formatBigNumber(event.totalEstimatedRows)} rows, levels=${event.dependencyLevels}, concurrency=${event.concurrency}`,
      );
      break;
    case "level-start":
      writeLine(
        `[${formatNow()}] level ${event.dependencyLevel}: ${event.levelTables} tables, ${formatBigNumber(event.levelEstimatedRows)} rows, concurrency=${event.concurrency}`,
      );
      break;
    case "neon-wait-start":
      writeLine(`[${formatNow()}] Neon connect: timeout=${event.timeoutSeconds}s`);
      break;
    case "neon-wait-retry":
      writeLine(
        `[${formatNow()}] Neon warming: ${event.elapsedSeconds}s elapsed, retry in ${event.retrySeconds}s`,
      );
      break;
    case "neon-ready":
      writeLine(`[${formatNow()}] Neon ready (${event.elapsedSeconds}s)`);
      break;
    case "table-start": {
      const total = event.completedTables + event.runningTables + event.remainingTables;
      const num = event.completedTables + event.runningTables;
      writeLine(
        `[${formatNow()}] ▶ (${num}/${total}) ${event.tableName}: ${formatBigNumber(event.estimatedRows)} rows`,
      );
      break;
    }
    case "table-done": {
      const pct = formatPercent(event.syncedEstimatedRows, event.totalEstimatedRows);
      writeLine(
        `[${formatNow()}] ✓ (${event.completedTables}/${event.totalTables}) ${event.tableName} in ${formatDuration(event.tableElapsedSeconds)} — total ${pct}, ${formatDuration(event.elapsedSeconds)} elapsed, ETA ${formatDuration(event.etaSeconds)}`,
      );
      break;
    }
    case "complete":
      writeLine(
        `[${formatNow()}] ✓ DONE: ${event.totalTables} tables / ${formatBigNumber(event.totalEstimatedRows)} rows in ${formatDuration(event.elapsedSeconds)}`,
      );
      break;
  }
}

function reportProgressVerbose(event: ProgressEvent): void {
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

function writeLine(message: string): void {
  const line = `${message}\n`;
  process.stdout.write(line);
  if (logFileFd !== null) {
    writeSync(logFileFd, line);
  }
}

async function main(): Promise<void> {
  const cliOptions = parseArgs(process.argv.slice(2));
  if (process.argv.includes("--help") || process.argv.includes("-h")) {
    printUsage();
    return;
  }

  setVerboseLogging(cliOptions.verbose);
  if (!cliOptions.noLogFile) {
    const logPath = cliOptions.logFile ?? defaultLogPath();
    openLogFile(logPath);
    writeLine(`[${formatNow()}] log file: ${logPath}`);
  }
  try {
    await runSync(cliOptions);
  } finally {
    closeLogFile();
  }
}

async function runSync(cliOptions: CliOptions): Promise<void> {
  const env = loadEnvironment();
  applyTimeoutEnv(env);
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
  const profileMap = await loadTableProfileMap(env, config);
  logStrategySummary(profileMap, tables);

  await runPushSync(
    tables,
    config,
    {
      nowSeconds: () => Math.floor(Date.now() / 1000),
      sleep: (milliseconds) =>
        new Promise((resolveSleep) => setTimeout(resolveSleep, milliseconds)),
      checkNeonReady: () => checkNeonReady(env),
      syncTable: (table) =>
        syncTableWithPsql(
          env,
          table,
          config.deleteMissingRows,
          config.applyMode,
          profileMap.get(table.tableName),
        ),
      report: reportProgress,
    },
    dependencyEdges,
  );
  await syncAnalyticsIndexes(env);
}

function logStrategySummary(profileMap: Map<string, TableProfile>, tables: TableMetadata[]): void {
  const counts = { timestampIncremental: 0, pkIncremental: 0, fullReplace: 0, unknown: 0 };
  for (const table of tables) {
    const profile = profileMap.get(table.tableName);
    if (!profile) {
      counts.unknown += 1;
      continue;
    }
    if (profile.strategy === "timestamp-incremental") counts.timestampIncremental += 1;
    else if (profile.strategy === "pk-incremental") counts.pkIncremental += 1;
    else counts.fullReplace += 1;
  }
  writeLine(
    `[${formatNow()}] strategy: timestamp-incremental=${counts.timestampIncremental}, pk-incremental=${counts.pkIncremental}, full-replace=${counts.fullReplace}, unknown=${counts.unknown}`,
  );
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
