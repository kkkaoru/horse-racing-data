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
  buildJsonlRecord,
  buildStageTableName,
  incrementalComparatorForTimestampColumn,
  buildMetadataSql,
  buildNeonApplySql,
  buildTableProfileSql,
  buildTimestampFingerprintSql,
  computeBackoffDelayMs,
  computeChunkEtaSeconds,
  computeChunkPlan,
  decideVerifyMismatchAction,
  formatRowsPerSecond,
  isVerifyMismatchSkipError,
  parseDependencyEdges,
  parseFingerprintLine,
  parseTableMetadata,
  parseTableProfiles,
  pkExpression,
  quoteIdentifier,
  resolveDefaultFullReplaceBatchRows,
  resolveOperationTimeoutPolicy,
  resolvePerTableIdleMs,
  resolvePerTableWallClockMs,
  resolvePositiveIntegerEnv,
  resolveRetryBackoffConfig,
  resolveSkipTables,
  resolveVerifyMismatchPolicy,
  runPushSync,
  runWithRetry,
  buildNeonPsqlArgs,
  shouldRefreshInclusiveIncrementalMarker,
  timestampKeyExpression,
  VerifyMismatchSkipError,
  type OperationTimeoutPolicy,
  type ProgressEvent,
  type PushSyncConfig,
  type RetryBackoffConfig,
  type TableMetadata,
  type TableProfile,
  type DependencyEdge,
  type VerifyMismatchPolicy,
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
const DEFAULT_MAX_ATTEMPTS = 5;
const MAX_ATTEMPTS_ENV_KEY = "REPLICA_SYNC_MAX_ATTEMPTS";
const DEFAULT_REINCREMENTAL_ROLLBACK_DAYS = 7;
const REINCREMENTAL_ROLLBACK_DAYS_ENV_KEY = "REPLICA_SYNC_REINCREMENTAL_ROLLBACK_DAYS";
const JSONL_LOG_ENV_KEY = "REPLICA_SYNC_LOG_JSONL";
const TIMEOUT_WARN_INTERVAL_MS = 30_000;

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
  maxAttempts: number | null;
};

function parseArgs(args: string[]): CliOptions {
  const options: CliOptions = {
    allowLogTables: new Set(),
    indexesOnly: false,
    verbose: false,
    logFile: null,
    noLogFile: false,
    maxAttempts: null,
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
    } else if (arg === "--max-attempts") {
      const value = args[index + 1];
      if (value === undefined) {
        throw new Error(`${arg} requires a positive integer.`);
      }
      const parsed = Number(value);
      if (!Number.isInteger(parsed) || parsed < 1) {
        throw new Error(`${arg} requires a positive integer, got: ${value}`);
      }
      options.maxAttempts = parsed;
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

function resolveMaxAttempts(
  override: number | null,
  env: Record<string, string | undefined>,
): number {
  return resolvePositiveIntegerEnv(override, env[MAX_ATTEMPTS_ENV_KEY], DEFAULT_MAX_ATTEMPTS);
}

function sleepMs(milliseconds: number): Promise<void> {
  return new Promise<void>((resolveSleep) => setTimeout(resolveSleep, milliseconds));
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

interface TimeoutContext {
  policy: OperationTimeoutPolicy;
  wallClockMs: number;
  idleMs: number;
  tableName: string | null;
}

interface ArmTimeoutOptions {
  child: ChildProcess;
  label: string;
  context: TimeoutContext;
  reject: (error: Error) => void;
  onIdle?: (chunk: Buffer) => void;
}

interface TimeoutHandles {
  cancel: () => void;
  bumpIdle: () => void;
}

function armTimeout(options: ArmTimeoutOptions): TimeoutHandles {
  const { child, label, context, reject } = options;
  const wallClockMs = context.wallClockMs;
  const idleMs = context.idleMs;
  const warningRatio = context.policy.warningRatio;
  const startedAt = Date.now();
  const state = { idleHandle: scheduleIdle(), wallHandle: scheduleWall(), wallWarned: false };

  function scheduleIdle(): NodeJS.Timeout {
    return setTimeout(() => {
      killProcessGroup(child);
      reject(
        new Error(
          `${label} idle for ${Math.round(idleMs / 1000)}s (no stdout/stderr) and was killed`,
        ),
      );
    }, idleMs);
  }

  function scheduleWall(): NodeJS.Timeout {
    return setTimeout(() => {
      killProcessGroup(child);
      reject(
        new Error(
          `${label} timed out after ${Math.round(wallClockMs / 1000)}s wall-clock and was killed`,
        ),
      );
    }, wallClockMs);
  }

  const warnHandle = setInterval(() => {
    if (state.wallWarned) return;
    const elapsedMs = Date.now() - startedAt;
    if (elapsedMs >= wallClockMs * warningRatio) {
      state.wallWarned = true;
      emitTimeoutWarning({
        label,
        tableName: context.tableName,
        elapsedSeconds: Math.round(elapsedMs / 1000),
        timeoutSeconds: Math.round(wallClockMs / 1000),
        kind: "wall-clock",
      });
    }
  }, TIMEOUT_WARN_INTERVAL_MS);

  return {
    cancel: () => {
      clearTimeout(state.idleHandle);
      clearTimeout(state.wallHandle);
      clearInterval(warnHandle);
    },
    bumpIdle: () => {
      clearTimeout(state.idleHandle);
      state.idleHandle = scheduleIdle();
    },
  };
}

let activeTimeoutPolicy: OperationTimeoutPolicy | null = null;
let activeEnv: Record<string, string | undefined> = {};

function setActiveTimeoutPolicy(policy: OperationTimeoutPolicy): void {
  activeTimeoutPolicy = policy;
}

function setActiveEnv(env: Record<string, string | undefined>): void {
  activeEnv = env;
}

function activePolicy(): OperationTimeoutPolicy {
  if (activeTimeoutPolicy !== null) return activeTimeoutPolicy;
  return resolveOperationTimeoutPolicy(activeEnv);
}

function buildTimeoutContext(tableName: string | null): TimeoutContext {
  const policy = activePolicy();
  const fallbackWallClockMs = policy.wallClockMs;
  const fallbackIdleMs = policy.idleMs;
  const wallClockMs =
    tableName === null
      ? fallbackWallClockMs
      : resolvePerTableWallClockMs({ env: activeEnv, tableName, fallbackWallClockMs });
  const idleMs =
    tableName === null
      ? fallbackIdleMs
      : resolvePerTableIdleMs({ env: activeEnv, tableName, fallbackIdleMs });
  return { policy, wallClockMs, idleMs, tableName };
}

interface TimeoutWarningInput {
  label: string;
  tableName: string | null;
  elapsedSeconds: number;
  timeoutSeconds: number;
  kind: "idle" | "wall-clock";
}

function emitTimeoutWarning(input: TimeoutWarningInput): void {
  const namedTable = input.tableName ?? "(no-table)";
  writeLine(
    `[${formatNow()}] ⚠ ${namedTable}: ${input.label} still running after ${input.elapsedSeconds}s (${Math.round((input.elapsedSeconds / input.timeoutSeconds) * 100)}% of ${input.timeoutSeconds}s ${input.kind} timeout) — will kill soon`,
  );
  writeJsonl({
    type: "timeout-warning",
    tableName: input.tableName ?? "",
    label: input.label,
    elapsedSeconds: input.elapsedSeconds,
    timeoutSeconds: input.timeoutSeconds,
    kind: input.kind,
  });
}

interface RunCommandOptions {
  input?: string;
  tableName?: string | null;
}

function runCommand(
  command: string,
  args: string[],
  options: RunCommandOptions = {},
): Promise<CommandResult> {
  return new Promise((resolveCommand, reject) => {
    const child = spawn(command, args, {
      stdio: ["pipe", "pipe", "pipe"],
      detached: true,
    });

    const stdoutChunks: Buffer[] = [];
    const stderrChunks: Buffer[] = [];
    const label = redactSecrets(`${command} ${args.join(" ")}`);
    const tableName = options.tableName ?? null;
    const handles = armTimeout({
      child,
      label,
      context: buildTimeoutContext(tableName),
      reject,
    });

    child.stdout.on("data", (chunk: Buffer) => {
      stdoutChunks.push(chunk);
      handles.bumpIdle();
    });
    child.stderr.on("data", (chunk: Buffer) => {
      stderrChunks.push(chunk);
      handles.bumpIdle();
    });
    child.on("error", (error) => {
      handles.cancel();
      reject(error);
    });
    child.on("close", (code) => {
      handles.cancel();
      const stdout = Buffer.concat(stdoutChunks).toString("utf8");
      const stderr = Buffer.concat(stderrChunks).toString("utf8");
      if (code === 0) {
        resolveCommand({ stdout, stderr });
      } else {
        reject(new Error(redactSecrets(`${label} exited with code ${code}\n${stderr}`)));
      }
    });

    if (options.input !== undefined) {
      child.stdin.end(options.input);
    } else {
      child.stdin.end();
    }
  });
}

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", "'\\''")}'`;
}

interface RunCopyPipelineOptions {
  env: Record<string, string | undefined>;
  localCopySql: string;
  neonCopySql: string;
  tableName: string | null;
}

function runCopyPipeline(options: RunCopyPipelineOptions): Promise<void> {
  return new Promise((resolvePipeline, reject) => {
    const { env, localCopySql, neonCopySql, tableName } = options;
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
    const handles = armTimeout({
      child,
      label: tableName === null ? "COPY pipeline" : `COPY pipeline (${tableName})`,
      context: buildTimeoutContext(tableName),
      reject,
    });
    child.stderr.on("data", (chunk: Buffer) => {
      stderrChunks.push(chunk);
      handles.bumpIdle();
    });
    child.on("error", (error) => {
      handles.cancel();
      reject(error);
    });
    child.on("close", (code) => {
      handles.cancel();
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
  return buildNeonPsqlArgs({
    neonUrl: env.NEON_DIRECT_DATABASE_URL,
    containerName: env.REPLICA_SYNC_NEON_PSQL_CONTAINER,
    extraArgs,
  });
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
  REPLICA_SYNC_SKIP_TABLES        Comma-separated list of tables to skip from sync entirely
                                  (no count, no stage, no insert, no verify). Independent of
                                  REPLICA_SYNC_SKIP_UNCHANGED. Example:
                                  finish_position_cron_executions,running_style_cron_executions.
  REPLICA_SYNC_COPY_BATCH_ROWS    Rows per COPY batch in full-replace path. Default: 500000.
                                  Empty value keeps the 500000 default; the legacy
                                  "one COPY per table" behavior is no longer the default.
  REPLICA_SYNC_DELETE             Delete Neon rows missing locally. Default: true.
  REPLICA_SYNC_INDEXES            Apply sql/analytics-indexes.sql to Neon after rows sync.
                                  Default: true.
  NEON_CONNECT_TIMEOUT_SECONDS    Wait timeout for Neon cold start. Default: 120.
  NEON_CONNECT_RETRY_SECONDS      Retry interval while Neon is warming. Default: 5.
  REPLICA_SYNC_OPERATION_TIMEOUT_SECONDS
                                  Wall-clock timeout that kills any docker/psql child that
                                  hangs. Default: 3600.
  REPLICA_SYNC_OPERATION_TIMEOUT_SECONDS_<table>
                                  Per-table override for the wall-clock timeout (seconds).
                                  Example: REPLICA_SYNC_OPERATION_TIMEOUT_SECONDS_jvd_se=7200.
  REPLICA_SYNC_IDLE_TIMEOUT_SECONDS
                                  Kill the child if stdout/stderr is silent for this many
                                  seconds. Catches Neon stalls without waiting wall-clock.
                                  Default: 300.
  REPLICA_SYNC_FULL_REPLACE_THRESHOLD_PERCENT
                                  Verify-mismatch retries the incremental copy with an
                                  older marker (instead of full-replace) when the diff is
                                  strictly below this percent of max(local, neon).
                                  Default: 1 (i.e. 1% drift).
  REPLICA_SYNC_REINCREMENTAL_ROLLBACK_DAYS
                                  How many days the timestamp marker is rolled back before
                                  re-running an incremental copy on verify mismatch.
                                  Default: 7.
  REPLICA_SYNC_LOG_JSONL          When set to 1 or true, append one JSON line per progress
                                  event to tmp/push-neon-sync.jsonl. Default: off.
  REPLICA_SYNC_NEON_PSQL_CONTAINER
                                  Name of the long-lived local container to exec psql in for
                                  Neon connections. The script reuses this container instead of
                                  spawning a disposable one per query, which avoids the docker
                                  run --rm cleanup hangs that accumulate zombie containers on
                                  Colima. Default: horse-racing-local-postgresql.
  REPLICA_SYNC_MAX_ATTEMPTS       Max per-table COPY attempts before giving up on a single
                                  table. Retries are scoped to the failing COPY only — the
                                  whole script is no longer respawned on transient TLS errors.
                                  Default: 5.
  REPLICA_SYNC_RETRY_BASE_DELAY_SECONDS
                                  Base delay for exponential backoff (delay grows as
                                  base * 2^(attempt-1) + jitter). Default: 5.
  REPLICA_SYNC_RETRY_MAX_DELAY_SECONDS
                                  Maximum backoff cap in seconds. Default: 60.
  REPLICA_SYNC_RETRY_DELAY_SECONDS
                                  Legacy alias: if set and the base-delay variable is unset,
                                  this is used as the backoff base. Default: 5.
  REPLICA_VERIFY_MISMATCH_THRESHOLD_ROWS
                                  When incremental verify shows the Neon row count differs
                                  from local by AT MOST this many rows AND the table is large
                                  (see large-table threshold), the table is SKIPPED instead of
                                  falling back to full-replace. Default: 10.
  REPLICA_VERIFY_MISMATCH_LARGE_TABLE_ROWS
                                  Row-count cutoff above which a small verify mismatch causes
                                  a skip. Tables smaller than this still fall back to
                                  full-replace as before. Default: 100000.
  REPLICA_VERIFY_MISMATCH_FORCE_FULL_REPLACE
                                  Set to true to restore the legacy behavior of always
                                  falling back to full-replace on any verify mismatch.
                                  Default: false.

Options:
  --verbose, -v                    Use the legacy multi-line progress format.
                                  Default: compact one-line-per-event format.
  --log-file PATH                  Append progress to PATH (default: tmp/push-neon-sync.log).
  --no-log-file                    Disable file logging (stdout only).
  --indexes-only                   Skip data sync, only apply analytics indexes.
  --allow-log-table TABLE          Allow syncing a log/experiment table that is excluded by
                                  default. Repeat or pass comma-separated names.
  --max-attempts N                 Override REPLICA_SYNC_MAX_ATTEMPTS. Pass 1 to disable retry.

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

interface SyncTableOptions {
  env: Record<string, string | undefined>;
  table: TableMetadata;
  deleteMissingRows: boolean;
  applyMode: "replace" | "upsert";
  profile: TableProfile | undefined;
  retry: {
    maxAttempts: number;
    backoff: RetryBackoffConfig;
  };
  verifyMismatchPolicy: VerifyMismatchPolicy;
}

async function syncTableWithPsql(options: SyncTableOptions): Promise<void> {
  const useIncremental = options.profile && options.profile.strategy !== "full-replace";
  if (useIncremental && options.profile) {
    await syncTableIncrementally({
      env: options.env,
      table: options.table,
      profile: options.profile,
      retry: options.retry,
      verifyMismatchPolicy: options.verifyMismatchPolicy,
    });
    return;
  }
  await syncTableFullReplace({
    env: options.env,
    table: options.table,
    deleteMissingRows: options.deleteMissingRows,
    applyMode: options.applyMode,
    retry: options.retry,
  });
}

interface SyncTableIncrementallyOptions {
  env: Record<string, string | undefined>;
  table: TableMetadata;
  profile: TableProfile;
  retry: {
    maxAttempts: number;
    backoff: RetryBackoffConfig;
  };
  verifyMismatchPolicy: VerifyMismatchPolicy;
}

async function syncTableIncrementally(options: SyncTableIncrementallyOptions): Promise<void> {
  const { env, table, profile, retry, verifyMismatchPolicy } = options;
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
  const stageTableName = buildStageTableName({
    kind: "incremental",
    pid: process.pid,
    tableName: table.tableName,
  });
  const incSql = buildIncrementalApplySql(table, stageTableName, false);
  const localCopySql = buildIncrementalCopyFromSql(table, {
    keyExpression,
    neonMarker: neonFp.marker,
    comparator: incrementalComparatorForTimestampColumn(tsColumn),
  });

  await runIncrementalCopyWithRetry({ env, table, incSql, localCopySql, retry });

  const verifyFp = await loadFingerprint(env, table, "neon", tsColumn);
  if (verifyFp.count !== localFp.count) {
    const action = decideVerifyMismatchAction({
      tableName: table.tableName,
      localCount: localFp.count,
      neonCount: verifyFp.count,
      rowCount: profile.rowCount,
      policy: verifyMismatchPolicy,
    });
    if (action.kind === "skip") {
      writeLine(`[${formatNow()}] ❌ ${action.message}`);
      throw new VerifyMismatchSkipError({
        tableName: table.tableName,
        localCount: localFp.count,
        neonCount: verifyFp.count,
        rowCount: profile.rowCount,
        message: action.message,
      });
    }
    if (action.kind === "re-incremental") {
      const reincrementalHandled = await tryReincrementalReplay({
        env,
        table,
        profile,
        tsColumn,
        neonFp,
        retry,
        message: action.message,
      });
      if (reincrementalHandled) return;
      writeLine(
        `[${formatNow()}] ⚠ ${table.tableName}: re-incremental skipped (marker rollback not supported for this strategy) — falling back to full-replace`,
      );
    } else {
      writeLine(`[${formatNow()}] ⚠ ${action.message}`);
    }
    await syncTableFullReplace({
      env,
      table,
      deleteMissingRows: true,
      applyMode: "upsert",
      retry,
    });
    return;
  }
  writeLine(
    `[${formatNow()}] ✚ ${table.tableName}: ${profile.strategy} synced (count ${neonFp.count} → ${verifyFp.count}, marker=${truncateMarker(localFp.marker)})`,
  );
}

interface RunIncrementalCopyWithRetryOptions {
  env: Record<string, string | undefined>;
  table: TableMetadata;
  incSql: ReturnType<typeof buildIncrementalApplySql>;
  localCopySql: string;
  retry: {
    maxAttempts: number;
    backoff: RetryBackoffConfig;
  };
}

async function runIncrementalCopyWithRetry(
  options: RunIncrementalCopyWithRetryOptions,
): Promise<void> {
  const { env, table, incSql, localCopySql, retry } = options;
  await runWithRetry(
    async () => {
      await runCommand("docker", neonPsqlArgs(env, ["-v", "ON_ERROR_STOP=1", "-q"]), {
        input: incSql.preCopySql,
        tableName: table.tableName,
      });
      try {
        await runCopyPipeline({
          env,
          localCopySql,
          neonCopySql: incSql.copySql,
          tableName: table.tableName,
        }).catch((error: unknown) => {
          const message = error instanceof Error ? error.message : String(error);
          throw new Error(`Failed to incremental-copy ${table.tableName}\n${message}`);
        });
        await runCommand("docker", neonPsqlArgs(env, ["-v", "ON_ERROR_STOP=1", "-q"]), {
          input: incSql.postCopySql,
          tableName: table.tableName,
        });
      } catch (error) {
        await runCleanupIgnoringFailure({
          env,
          cleanupSql: incSql.cleanupSql,
          tableName: table.tableName,
        });
        throw error;
      }
    },
    {
      maxAttempts: retry.maxAttempts,
      retryDelayMs: retry.backoff.baseMs,
      sleep: sleepMs,
      computeDelayMs: (attempt) => computeBackoffDelayMs(attempt, retry.backoff),
      onAttemptFailed: (info) => {
        writeLine(
          `[${formatNow()}] ↺ ${table.tableName}: copy attempt ${info.attempt}/${info.maxAttempts} failed: ${describeError(info.error)} — retry in ${(info.retryDelayMs / 1000).toFixed(1)}s (backoff attempt ${info.attempt})`,
        );
      },
      onRetrySucceeded: (info) => {
        writeLine(
          `[${formatNow()}] ↻ ${table.tableName}: copy retry succeeded on attempt ${info.attempt}/${info.maxAttempts}`,
        );
      },
    },
  );
}

function truncateMarker(marker: string): string {
  if (marker.length <= 40) return marker;
  return `${marker.slice(0, 37)}...`;
}

interface SyncTableFullReplaceOptions {
  env: Record<string, string | undefined>;
  table: TableMetadata;
  deleteMissingRows: boolean;
  applyMode: "replace" | "upsert";
  retry: {
    maxAttempts: number;
    backoff: RetryBackoffConfig;
  };
}

async function syncTableFullReplace(options: SyncTableFullReplaceOptions): Promise<void> {
  const { env, table, deleteMissingRows, applyMode, retry } = options;
  const quotedTable = quoteIdentifier(table.tableName);
  const stageTableName = buildStageTableName({
    kind: "full",
    pid: process.pid,
    tableName: table.tableName,
  });
  const neonSql = buildNeonApplySql(table, deleteMissingRows, stageTableName, false, applyMode);
  const batchRows = resolveDefaultFullReplaceBatchRows(env);

  if (shouldSkipUnchanged(env) && (await tableIsUnchanged(env, table))) {
    writeLine(`[${formatNow()}] ⊘ ${table.tableName}: unchanged (skip)`);
    return;
  }

  await runWithRetry(() => runFullReplaceOnce({ env, table, quotedTable, neonSql, batchRows }), {
    maxAttempts: retry.maxAttempts,
    retryDelayMs: retry.backoff.baseMs,
    sleep: sleepMs,
    computeDelayMs: (attempt) => computeBackoffDelayMs(attempt, retry.backoff),
    onAttemptFailed: (info) => {
      writeLine(
        `[${formatNow()}] ↺ ${table.tableName}: copy attempt ${info.attempt}/${info.maxAttempts} failed: ${describeError(info.error)} — retry in ${(info.retryDelayMs / 1000).toFixed(1)}s (backoff attempt ${info.attempt})`,
      );
    },
    onRetrySucceeded: (info) => {
      writeLine(
        `[${formatNow()}] ↻ ${table.tableName}: copy retry succeeded on attempt ${info.attempt}/${info.maxAttempts}`,
      );
    },
  });
}

interface RunFullReplaceOnceOptions {
  env: Record<string, string | undefined>;
  table: TableMetadata;
  quotedTable: string;
  neonSql: ReturnType<typeof buildNeonApplySql>;
  batchRows: number;
}

async function runFullReplaceOnce(options: RunFullReplaceOnceOptions): Promise<void> {
  const { env, table, quotedTable, neonSql, batchRows } = options;
  await runCommand("docker", neonPsqlArgs(env, ["-v", "ON_ERROR_STOP=1", "-q"]), {
    input: neonSql.preCopySql,
    tableName: table.tableName,
  });

  try {
    const { stdout: countOutput } = await runCommand(
      "docker",
      dockerComposeArgs(env, `SELECT count(*) FROM public.${quotedTable};`),
      { tableName: table.tableName },
    );
    const rowCount = Number(countOutput.trim());
    await runFullReplaceChunked({ env, table, quotedTable, neonSql, batchRows, rowCount });
    await runCommand("docker", neonPsqlArgs(env, ["-v", "ON_ERROR_STOP=1", "-q"]), {
      input: neonSql.postCopySql,
      tableName: table.tableName,
    });
  } catch (error) {
    await runCleanupIgnoringFailure({
      env,
      cleanupSql: neonSql.cleanupSql,
      tableName: table.tableName,
    });
    throw error;
  }
}

interface RunCleanupIgnoringFailureOptions {
  env: Record<string, string | undefined>;
  cleanupSql: string;
  tableName: string;
}

async function runCleanupIgnoringFailure(options: RunCleanupIgnoringFailureOptions): Promise<void> {
  await runCommand("docker", neonPsqlArgs(options.env, ["-q"]), {
    input: options.cleanupSql,
    tableName: options.tableName,
  }).catch((error: unknown) => {
    writeLine(
      `[${formatNow()}] ⚠ ${options.tableName}: cleanup after failure raised ${describeError(error)} — preCopySql DROP IF EXISTS will self-heal on retry`,
    );
  });
}

interface RunFullReplaceChunkedOptions extends RunFullReplaceOnceOptions {
  rowCount: number;
}

async function runFullReplaceChunked(options: RunFullReplaceChunkedOptions): Promise<void> {
  const { env, table, quotedTable, neonSql, batchRows, rowCount } = options;
  const plan = computeChunkPlan(rowCount, batchRows);
  const context = buildTimeoutContext(table.tableName);
  reportChunkPlan({
    table,
    plan,
    rowCount,
    wallClockSeconds: Math.round(context.wallClockMs / 1000),
    idleSeconds: Math.round(context.idleMs / 1000),
  });
  if (plan.chunkCount === 0) return;
  const tableStartedAt = nowMs();
  await iterateChunks({
    chunkCount: plan.chunkCount,
    chunkRows: plan.chunkRows,
    rowCount,
    run: async (chunkIndex, offset) => {
      const chunkStartedAt = nowMs();
      const localCopySql = `COPY (SELECT ${table.columnList} FROM public.${quotedTable} ORDER BY ${table.primaryKeyList} LIMIT ${plan.chunkRows} OFFSET ${offset}) TO STDOUT WITH (FORMAT csv, NULL '\\N');`;
      await runCopyPipeline({
        env,
        localCopySql,
        neonCopySql: neonSql.copySql,
        tableName: table.tableName,
      }).catch((error: unknown) => {
        const message = error instanceof Error ? error.message : String(error);
        throw new Error(`Failed to copy ${table.tableName} offset=${offset}\n${message}`);
      });
      reportChunkDone({
        table,
        chunkIndex,
        chunkCount: plan.chunkCount,
        rowsDone: Math.min((chunkIndex + 1) * plan.chunkRows, rowCount),
        rowsTotal: rowCount,
        chunkElapsedSeconds: Math.max(Math.round((nowMs() - chunkStartedAt) / 1000), 0),
        tableElapsedSeconds: Math.max(Math.round((nowMs() - tableStartedAt) / 1000), 0),
      });
    },
  });
}

interface IterateChunksOptions {
  chunkCount: number;
  chunkRows: number;
  rowCount: number;
  run: (chunkIndex: number, offset: number) => Promise<void>;
}

async function iterateChunks(options: IterateChunksOptions): Promise<void> {
  const indexes = Array.from({ length: options.chunkCount }, (_unused, index) => index);
  await indexes.reduce(
    (previous, chunkIndex) =>
      previous.then(() => options.run(chunkIndex, chunkIndex * options.chunkRows)),
    Promise.resolve(),
  );
}

function nowMs(): number {
  return Date.now();
}

interface ReportChunkPlanInput {
  table: TableMetadata;
  plan: ReturnType<typeof computeChunkPlan>;
  rowCount: number;
  wallClockSeconds: number;
  idleSeconds: number;
}

function reportChunkPlan(input: ReportChunkPlanInput): void {
  const event: ProgressEvent = {
    type: "chunk-plan",
    tableName: input.table.tableName,
    rowCount: input.rowCount,
    chunkCount: input.plan.chunkCount,
    chunkRows: input.plan.chunkRows,
    wallClockTimeoutSeconds: input.wallClockSeconds,
    idleTimeoutSeconds: input.idleSeconds,
  };
  writeLine(
    `[${formatNow()}] ▶ ${input.table.tableName}: ${formatBigNumber(input.rowCount)} rows → ${input.plan.chunkCount} chunks (${formatBigNumber(input.plan.chunkRows)}/chunk) — timeout=${input.wallClockSeconds}s wall / ${input.idleSeconds}s idle`,
  );
  writeJsonl(event);
}

interface ReportChunkDoneInput {
  table: TableMetadata;
  chunkIndex: number;
  chunkCount: number;
  rowsDone: number;
  rowsTotal: number;
  chunkElapsedSeconds: number;
  tableElapsedSeconds: number;
}

function reportChunkDone(input: ReportChunkDoneInput): void {
  const rowsPerSecond = formatRowsPerSecond(input.rowsDone, input.tableElapsedSeconds);
  const etaTableSeconds = computeChunkEtaSeconds(
    input.rowsDone,
    input.rowsTotal,
    input.tableElapsedSeconds,
  );
  const event: ProgressEvent = {
    type: "chunk-done",
    tableName: input.table.tableName,
    chunkIndex: input.chunkIndex + 1,
    chunkCount: input.chunkCount,
    rowsDone: input.rowsDone,
    rowsTotal: input.rowsTotal,
    chunkElapsedSeconds: input.chunkElapsedSeconds,
    tableElapsedSeconds: input.tableElapsedSeconds,
    rowsPerSecond,
    etaTableSeconds,
  };
  const percent = formatPercent(input.rowsDone, input.rowsTotal);
  writeLine(
    `[${formatNow()}] · ${input.table.tableName} chunk ${input.chunkIndex + 1}/${input.chunkCount} — ${formatBigNumber(input.rowsDone)}/${formatBigNumber(input.rowsTotal)} rows (${percent}) in ${formatDuration(input.chunkElapsedSeconds)} (${formatBigNumber(Math.round(rowsPerSecond))} r/s), ETA table ${formatDuration(etaTableSeconds)}`,
  );
  writeJsonl(event);
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
  await runCommand("docker", neonPsqlArgs(env, ["-v", "ON_ERROR_STOP=1"]), {
    input: sql,
    tableName: "(analytics-indexes)",
  });
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

let jsonlFd: number | null = null;
let jsonlStartedAt = 0;

function openJsonlSidecarIfEnabled(env: Record<string, string | undefined>): void {
  const raw = env[JSONL_LOG_ENV_KEY];
  if (raw !== "1" && raw !== "true") return;
  const jsonlPath = resolve(appDir, "tmp", "push-neon-sync.jsonl");
  const dir = dirname(jsonlPath);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  jsonlFd = openSync(jsonlPath, "a");
  jsonlStartedAt = Date.now();
}

function closeJsonlSidecar(): void {
  if (jsonlFd !== null) {
    closeSync(jsonlFd);
    jsonlFd = null;
  }
}

function writeJsonl(event: ProgressEvent): void {
  if (jsonlFd === null) return;
  const elapsedSeconds = Math.max(Math.round((Date.now() - jsonlStartedAt) / 1000), 0);
  const record = buildJsonlRecord({ tsIso: new Date().toISOString(), event, elapsedSeconds });
  writeSync(jsonlFd, `${JSON.stringify(record)}\n`);
}

interface TryReincrementalReplayOptions {
  env: Record<string, string | undefined>;
  table: TableMetadata;
  profile: TableProfile;
  tsColumn: string | null;
  neonFp: { count: number; marker: string };
  retry: { maxAttempts: number; backoff: RetryBackoffConfig };
  message: string;
}

async function tryReincrementalReplay(options: TryReincrementalReplayOptions): Promise<boolean> {
  const { env, table, profile, tsColumn, neonFp, retry, message } = options;
  if (profile.strategy !== "timestamp-incremental" || tsColumn === null) return false;
  const rolledBackMarker = rollbackTimestampMarker(env, neonFp.marker, tsColumn);
  if (rolledBackMarker === null) return false;
  writeLine(
    `[${formatNow()}] ↻ ${table.tableName}: re-incremental replay — ${message}; rolled marker from ${truncateMarker(neonFp.marker)} to ${truncateMarker(rolledBackMarker)}`,
  );
  const keyExpression = timestampKeyExpression(tsColumn);
  const stageTableName = buildStageTableName({
    kind: "reincremental",
    pid: process.pid,
    tableName: table.tableName,
  });
  const incSql = buildIncrementalApplySql(table, stageTableName, false);
  const localCopySql = buildIncrementalCopyFromSql(table, {
    keyExpression,
    neonMarker: rolledBackMarker,
    comparator: incrementalComparatorForTimestampColumn(tsColumn),
  });
  await runIncrementalCopyWithRetry({ env, table, incSql, localCopySql, retry });
  const verifyFp = await loadFingerprint(env, table, "neon", tsColumn);
  if (verifyFp.count !== profile.rowCount && verifyFp.count !== neonFp.count) {
    writeLine(
      `[${formatNow()}] ↻ ${table.tableName}: re-incremental verified neon=${verifyFp.count}`,
    );
  }
  return true;
}

function resolveReincrementalRollbackDays(env: Record<string, string | undefined>): number {
  const raw = env[REINCREMENTAL_ROLLBACK_DAYS_ENV_KEY];
  if (raw === undefined || raw === "") return DEFAULT_REINCREMENTAL_ROLLBACK_DAYS;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) return DEFAULT_REINCREMENTAL_ROLLBACK_DAYS;
  return parsed;
}

function rollbackTimestampMarker(
  env: Record<string, string | undefined>,
  marker: string,
  tsColumn: string,
): string | null {
  if (marker === "") return null;
  const days = resolveReincrementalRollbackDays(env);
  if (tsColumn === "data_sakusei_nengappi") return rollbackDateOnlyMarker(marker, days);
  return rollbackIsoTimestampMarker(marker, days);
}

function rollbackDateOnlyMarker(marker: string, days: number): string | null {
  if (!/^\d{8}$/.test(marker)) return null;
  const year = Number(marker.slice(0, 4));
  const month = Number(marker.slice(4, 6));
  const day = Number(marker.slice(6, 8));
  const date = new Date(Date.UTC(year, month - 1, day));
  date.setUTCDate(date.getUTCDate() - days);
  const yyyy = date.getUTCFullYear().toString().padStart(4, "0");
  const mm = (date.getUTCMonth() + 1).toString().padStart(2, "0");
  const dd = date.getUTCDate().toString().padStart(2, "0");
  return `${yyyy}${mm}${dd}`;
}

function rollbackIsoTimestampMarker(marker: string, days: number): string | null {
  const parsed = new Date(marker);
  if (Number.isNaN(parsed.getTime())) return null;
  parsed.setUTCDate(parsed.getUTCDate() - days);
  return parsed.toISOString();
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
    closeJsonlSidecar();
  }
}

function describeError(error: unknown): string {
  return redactSecrets(error instanceof Error ? error.message : String(error));
}

async function runSync(cliOptions: CliOptions): Promise<void> {
  const env = loadEnvironment();
  setActiveEnv(env);
  setActiveTimeoutPolicy(resolveOperationTimeoutPolicy(env));
  openJsonlSidecarIfEnabled(env);
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

  const maxAttempts = resolveMaxAttempts(cliOptions.maxAttempts, env);
  const backoff = resolveRetryBackoffConfig(env);
  const verifyMismatchPolicy = resolveVerifyMismatchPolicy(env);
  const skipTables = resolveSkipTables(env);
  const skippedTables: VerifyMismatchSkipError[] = [];

  await runPushSync(
    tables,
    config,
    {
      nowSeconds: () => Math.floor(Date.now() / 1000),
      sleep: sleepMs,
      checkNeonReady: () => checkNeonReady(env),
      syncTable: (table) =>
        syncTableWithSkipTracking({
          env,
          table,
          deleteMissingRows: config.deleteMissingRows,
          applyMode: config.applyMode,
          profile: profileMap.get(table.tableName),
          retry: { maxAttempts, backoff },
          verifyMismatchPolicy,
          skipTables,
          skippedTables,
        }),
      report: reportProgress,
    },
    dependencyEdges,
  );
  await syncAnalyticsIndexes(env);
  reportSkippedTables(skippedTables);
}

interface SyncTableWithSkipTrackingOptions extends SyncTableOptions {
  skipTables: ReadonlySet<string>;
  skippedTables: VerifyMismatchSkipError[];
}

async function syncTableWithSkipTracking(options: SyncTableWithSkipTrackingOptions): Promise<void> {
  if (options.skipTables.has(options.table.tableName)) {
    writeLine(
      `[${formatNow()}] ⊘ ${options.table.tableName}: skipped via REPLICA_SYNC_SKIP_TABLES`,
    );
    return;
  }
  try {
    await syncTableWithPsql({
      env: options.env,
      table: options.table,
      deleteMissingRows: options.deleteMissingRows,
      applyMode: options.applyMode,
      profile: options.profile,
      retry: options.retry,
      verifyMismatchPolicy: options.verifyMismatchPolicy,
    });
  } catch (error) {
    if (isVerifyMismatchSkipError(error)) {
      options.skippedTables.push(error);
      return;
    }
    throw error;
  }
}

function reportSkippedTables(skipped: VerifyMismatchSkipError[]): void {
  if (skipped.length === 0) return;
  writeLine(
    `[${formatNow()}] ⚠ Skipped ${skipped.length} table(s) due to verify mismatch; manual reconcile required:`,
  );
  for (const skip of skipped) {
    writeLine(
      `[${formatNow()}]   - ${skip.tableName} (local=${skip.localCount}, neon=${skip.neonCount}, rowCount=${skip.rowCount})`,
    );
  }
  process.exitCode = 1;
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
