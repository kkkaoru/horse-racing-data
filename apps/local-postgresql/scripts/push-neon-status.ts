#!/usr/bin/env bun
/**
 * Progress / status checker for push-neon-sync.
 *
 * - Side-by-side row count diff between docker (local) and Neon for key tables.
 * - Tails the last N lines of the sync log if present.
 * - Optional --watch refreshes every N seconds.
 *
 * Usage:
 *   bun run scripts/push-neon-status.ts                # one-shot report
 *   bun run scripts/push-neon-status.ts --watch 10     # refresh every 10s
 *   bun run scripts/push-neon-status.ts --tail 30      # show last 30 log lines
 *   bun run scripts/push-neon-status.ts --tables apd_se_jv,nvd_se
 */
import { existsSync, readFileSync, statSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

const KEY_TABLES = [
  "finish_position_active_models",
  "model_prediction_evaluations",
  "race_finish_position_model_predictions",
  "race_entry_corner_features",
  "race_finish_position_features",
  "race_running_style_model_predictions",
  "race_corner_position_model_predictions",
  "jvd_se",
  "nvd_se",
  "apd_se_jv",
  "apd_se_nv",
  "jvd_hc",
  "jvd_um",
];
const DEFAULT_TAIL_LINES = 20;
const DEFAULT_WATCH_INTERVAL_SECONDS = 10;
const MIN_WATCH_INTERVAL_SECONDS = 2;
const MAX_WATCH_INTERVAL_SECONDS = 600;
const PSQL_CONNECT_TIMEOUT_SECONDS = 15;

interface CliOptions {
  tables: readonly string[];
  watchSeconds: number | null;
  tailLines: number;
  logFile: string | null;
}

interface CommandResult {
  stdout: string;
  stderr: string;
  exitCode: number;
}

interface RowCountSummary {
  tableName: string;
  dockerRows: number | null;
  neonRows: number | null;
  diff: number | null;
}

interface ActiveModelRow {
  category: string;
  modelVersion: string;
  activatedAt: string;
}

interface ActiveModelComparison {
  category: string;
  docker: string;
  neon: string;
  same: boolean;
}

const scriptDir = dirname(fileURLToPath(import.meta.url));
const appDir = resolve(scriptDir, "..");
const defaultLogFile = resolve(appDir, "tmp", "push-neon-sync.log");
const envPath = resolve(appDir, ".env");
const replicaEnvPath = resolve(appDir, ".env.replica");

function parseArgs(args: string[]): CliOptions {
  const options: CliOptions = {
    tables: KEY_TABLES,
    watchSeconds: null,
    tailLines: DEFAULT_TAIL_LINES,
    logFile: null,
  };
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === "--watch") {
      const value = args[index + 1];
      options.watchSeconds = parsePositiveInt(value, DEFAULT_WATCH_INTERVAL_SECONDS);
      if (value !== undefined && /^[0-9]+$/.test(value)) {
        index += 1;
      }
    } else if (arg === "--tail") {
      const value = args[index + 1];
      options.tailLines = parsePositiveInt(value, DEFAULT_TAIL_LINES);
      if (value !== undefined && /^[0-9]+$/.test(value)) {
        index += 1;
      }
    } else if (arg === "--tables") {
      const value = args[index + 1];
      if (value === undefined) {
        throw new Error("--tables requires a comma-separated value.");
      }
      options.tables = value.split(",").map((name) => name.trim()).filter((name) => name !== "");
      index += 1;
    } else if (arg === "--log-file") {
      const value = args[index + 1];
      if (value === undefined) {
        throw new Error("--log-file requires a path.");
      }
      options.logFile = value;
      index += 1;
    } else if (arg === "--help" || arg === "-h") {
      printUsage();
      process.exit(0);
    } else {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }
  return options;
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  if (value === undefined) {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function clampWatchInterval(seconds: number): number {
  if (seconds < MIN_WATCH_INTERVAL_SECONDS) {
    return MIN_WATCH_INTERVAL_SECONDS;
  }
  if (seconds > MAX_WATCH_INTERVAL_SECONDS) {
    return MAX_WATCH_INTERVAL_SECONDS;
  }
  return seconds;
}

function printUsage(): void {
  console.log(
    `Usage: bun run ./scripts/push-neon-status.ts [--watch N] [--tail N] [--tables CSV] [--log-file PATH]

Defaults:
  --tail 20                Show last 20 lines from tmp/push-neon-sync.log
  --tables                 ${KEY_TABLES.join(",")}

Examples:
  bun run scripts/push-neon-status.ts
  bun run scripts/push-neon-status.ts --watch 10
  bun run scripts/push-neon-status.ts --tables jvd_se,nvd_se --tail 50
`,
  );
}

function parseEnvFile(path: string): Record<string, string> {
  const values: Record<string, string> = {};
  if (!existsSync(path)) {
    return values;
  }
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
  return { ...process.env, ...parseEnvFile(envPath), ...parseEnvFile(replicaEnvPath) };
}

async function runCommand(
  command: string,
  args: string[],
  options: { timeoutSeconds?: number } = {},
): Promise<CommandResult> {
  return new Promise((resolveCommand, reject) => {
    const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"] });
    const stdoutChunks: Buffer[] = [];
    const stderrChunks: Buffer[] = [];
    let settled = false;
    const timer = options.timeoutSeconds
      ? setTimeout(() => {
          if (!settled) {
            settled = true;
            child.kill("SIGKILL");
            reject(new Error(`timeout after ${options.timeoutSeconds}s`));
          }
        }, options.timeoutSeconds * 1000)
      : null;
    child.stdout.on("data", (chunk: Buffer) => stdoutChunks.push(chunk));
    child.stderr.on("data", (chunk: Buffer) => stderrChunks.push(chunk));
    child.on("error", (error) => {
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      reject(error);
    });
    child.on("close", (code) => {
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      resolveCommand({
        stdout: Buffer.concat(stdoutChunks).toString("utf8"),
        stderr: Buffer.concat(stderrChunks).toString("utf8"),
        exitCode: code ?? 0,
      });
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

function neonPsqlArgs(env: Record<string, string | undefined>, sql: string): string[] {
  const neonUrl = env.NEON_DIRECT_DATABASE_URL;
  if (!neonUrl) {
    throw new Error("NEON_DIRECT_DATABASE_URL is required (set in .env.replica)");
  }
  return [
    "run",
    "--rm",
    "-i",
    "postgres:18-alpine",
    "psql",
    neonUrl,
    "-v",
    "ON_ERROR_STOP=1",
    "-At",
    "-F",
    "\t",
    "-c",
    sql,
  ];
}

async function queryDockerSingleColumn(
  env: Record<string, string | undefined>,
  sql: string,
): Promise<string[]> {
  const { stdout, exitCode, stderr } = await runCommand("docker", dockerComposeArgs(env, sql), {
    timeoutSeconds: PSQL_CONNECT_TIMEOUT_SECONDS,
  });
  if (exitCode !== 0) {
    throw new Error(`docker psql failed: ${stderr.trim()}`);
  }
  return stdout
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line !== "");
}

async function queryNeonSingleColumn(
  env: Record<string, string | undefined>,
  sql: string,
): Promise<string[]> {
  const { stdout, exitCode, stderr } = await runCommand("docker", neonPsqlArgs(env, sql), {
    timeoutSeconds: PSQL_CONNECT_TIMEOUT_SECONDS,
  });
  if (exitCode !== 0) {
    throw new Error(`neon psql failed: ${stderr.trim()}`);
  }
  return stdout
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line !== "");
}

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

function buildExistsAndCountSql(tables: readonly string[]): string {
  const exists = tables
    .map(
      (table) =>
        `select '${table}' as t, (to_regclass('public.${quoteIdentifier(table)}') is not null) as exists`,
    )
    .join(" union all ");
  return exists;
}

function buildCountSql(table: string): string {
  return `select count(*) from public.${quoteIdentifier(table)}`;
}

async function loadRowCounts(
  env: Record<string, string | undefined>,
  tables: readonly string[],
): Promise<RowCountSummary[]> {
  const [dockerExisting, neonExisting] = await Promise.all([
    loadExistingTables(env, tables, "docker"),
    loadExistingTables(env, tables, "neon"),
  ]);
  const dockerCounts = await loadCounts(env, [...dockerExisting], "docker");
  const neonCounts = await loadCounts(env, [...neonExisting], "neon");
  return tables.map((table) => {
    const dockerCount = dockerExisting.has(table) ? dockerCounts.get(table) ?? null : null;
    const neonCount = neonExisting.has(table) ? neonCounts.get(table) ?? null : null;
    const diff =
      dockerCount === null || neonCount === null ? null : neonCount - dockerCount;
    return {
      tableName: table,
      dockerRows: dockerCount,
      neonRows: neonCount,
      diff,
    };
  });
}

async function loadExistingTables(
  env: Record<string, string | undefined>,
  tables: readonly string[],
  target: "docker" | "neon",
): Promise<Set<string>> {
  if (tables.length === 0) return new Set();
  const sql = buildExistsAndCountSql(tables);
  const lines =
    target === "docker"
      ? await queryDockerSingleColumn(env, sql)
      : await queryNeonSingleColumn(env, sql);
  const result = new Set<string>();
  for (const line of lines) {
    const [name, existsValue] = line.split("\t");
    if (name === undefined || existsValue === undefined) continue;
    if (existsValue.trim() === "t") {
      result.add(name);
    }
  }
  return result;
}

async function loadCounts(
  env: Record<string, string | undefined>,
  tables: string[],
  target: "docker" | "neon",
): Promise<Map<string, number>> {
  const result = new Map<string, number>();
  if (tables.length === 0) {
    return result;
  }
  const unionSql = tables
    .map(
      (table) =>
        `select '${table}' as t, (select count(*) from public.${quoteIdentifier(table)})::text as c`,
    )
    .join(" union all ");
  const lines =
    target === "docker"
      ? await queryDockerSingleColumn(env, unionSql)
      : await queryNeonSingleColumn(env, unionSql);
  for (const line of lines) {
    const [name, value] = line.split("\t");
    if (name === undefined || value === undefined) continue;
    const count = Number(value);
    if (Number.isFinite(count)) {
      result.set(name, count);
    }
  }
  return result;
}

function parseTableCountLines(lines: string[]): Map<string, number> {
  const result = new Map<string, number>();
  for (const line of lines) {
    const [name, count] = line.split("\t");
    if (name === undefined || count === undefined) continue;
    result.set(name, Number(count));
  }
  return result;
}

async function loadActiveModels(
  env: Record<string, string | undefined>,
): Promise<ActiveModelComparison[]> {
  const sql =
    "select category, model_version, activated_at::text from public.finish_position_active_models order by category";
  const [dockerLines, neonLines] = await Promise.all([
    queryDockerSingleColumn(env, sql),
    queryNeonSingleColumn(env, sql),
  ]);
  const dockerModels = parseActiveModelLines(dockerLines);
  const neonModels = parseActiveModelLines(neonLines);
  const categories = Array.from(
    new Set([
      ...dockerModels.map((row) => row.category),
      ...neonModels.map((row) => row.category),
    ]),
  ).sort();
  return categories.map((category) => {
    const dockerRow = dockerModels.find((row) => row.category === category);
    const neonRow = neonModels.find((row) => row.category === category);
    const dockerVersion = dockerRow?.modelVersion ?? "—";
    const neonVersion = neonRow?.modelVersion ?? "—";
    return {
      category,
      docker: dockerVersion,
      neon: neonVersion,
      same: dockerVersion === neonVersion,
    };
  });
}

function parseActiveModelLines(lines: string[]): ActiveModelRow[] {
  return lines.flatMap((line) => {
    const parts = line.split("\t");
    if (parts.length < 3) return [];
    return [{ category: parts[0]!, modelVersion: parts[1]!, activatedAt: parts[2]! }];
  });
}

function formatNumber(value: number): string {
  return value.toLocaleString("en-US");
}

function padRight(value: string, width: number): string {
  return value.length >= width ? value : value + " ".repeat(width - value.length);
}

function padLeft(value: string, width: number): string {
  return value.length >= width ? value : " ".repeat(width - value.length) + value;
}

function formatNullableNumber(value: number | null): string {
  return value === null ? "—" : formatNumber(value);
}

function formatDiff(diff: number | null): string {
  if (diff === null) return "—";
  if (diff === 0) return "0 ✓";
  if (diff > 0) return `+${formatNumber(diff)}`;
  return formatNumber(diff);
}

function renderRowCountTable(rows: RowCountSummary[]): string {
  const tableHeader = "table";
  const dockerHeader = "docker";
  const neonHeader = "neon";
  const diffHeader = "diff";
  const tableWidth = Math.max(tableHeader.length, ...rows.map((row) => row.tableName.length));
  const dockerWidth = Math.max(
    dockerHeader.length,
    ...rows.map((row) => formatNullableNumber(row.dockerRows).length),
  );
  const neonWidth = Math.max(
    neonHeader.length,
    ...rows.map((row) => formatNullableNumber(row.neonRows).length),
  );
  const diffWidth = Math.max(diffHeader.length, ...rows.map((row) => formatDiff(row.diff).length));
  const lines: string[] = [];
  lines.push(
    `${padRight(tableHeader, tableWidth)}  ${padLeft(dockerHeader, dockerWidth)}  ${padLeft(neonHeader, neonWidth)}  ${padLeft(diffHeader, diffWidth)}`,
  );
  lines.push(
    `${"-".repeat(tableWidth)}  ${"-".repeat(dockerWidth)}  ${"-".repeat(neonWidth)}  ${"-".repeat(diffWidth)}`,
  );
  for (const row of rows) {
    lines.push(
      `${padRight(row.tableName, tableWidth)}  ${padLeft(formatNullableNumber(row.dockerRows), dockerWidth)}  ${padLeft(formatNullableNumber(row.neonRows), neonWidth)}  ${padLeft(formatDiff(row.diff), diffWidth)}`,
    );
  }
  return lines.join("\n");
}

function renderActiveModels(rows: ActiveModelComparison[]): string {
  const headerLines = ["category  docker_active                    neon_active                      status"];
  for (const row of rows) {
    const status = row.same ? "✓ same" : "⚠ drift";
    headerLines.push(
      `${padRight(row.category, 8)}  ${padRight(row.docker, 32)}  ${padRight(row.neon, 32)}  ${status}`,
    );
  }
  return headerLines.join("\n");
}

function tailFile(path: string, n: number): string[] {
  if (!existsSync(path)) {
    return [];
  }
  const content = readFileSync(path, "utf8");
  const lines = content.split(/\r?\n/);
  return lines.filter((line) => line.length > 0).slice(-n);
}

function formatLogFileSummary(path: string, lines: string[]): string {
  if (!existsSync(path)) {
    return `(no log file at ${path} — run push-neon-sync first)`;
  }
  const stat = statSync(path);
  const sizeKb = (stat.size / 1024).toFixed(1);
  const mtime = stat.mtime.toLocaleString();
  const header = `log file: ${path} (${sizeKb} kB, modified ${mtime})`;
  if (lines.length === 0) {
    return `${header}\n(log file is empty)`;
  }
  return [header, "", ...lines].join("\n");
}

async function printReport(options: CliOptions): Promise<void> {
  const env = loadEnvironment();
  const startedAt = Date.now();
  const [rowCounts, activeModels] = await Promise.all([
    loadRowCounts(env, options.tables),
    loadActiveModels(env),
  ]);
  const elapsedMs = Date.now() - startedAt;
  console.log(`=== push-neon status @ ${new Date().toLocaleString()} (queried in ${elapsedMs} ms) ===`);
  console.log("");
  console.log("--- active models (jra / nar / ban-ei) ---");
  console.log(renderActiveModels(activeModels));
  console.log("");
  console.log("--- row counts (docker vs neon) ---");
  console.log(renderRowCountTable(rowCounts));
  console.log("");
  const logFile = options.logFile ?? defaultLogFile;
  console.log("--- sync log tail ---");
  const logLines = tailFile(logFile, options.tailLines);
  console.log(formatLogFileSummary(logFile, logLines));
}

async function watchReport(options: CliOptions, intervalSeconds: number): Promise<void> {
  for (;;) {
    process.stdout.write("\x1bc"); // clear screen
    await printReport(options);
    console.log("");
    console.log(`(refreshing every ${intervalSeconds}s — press Ctrl-C to stop)`);
    await new Promise((resolveSleep) => setTimeout(resolveSleep, intervalSeconds * 1000));
  }
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2));
  if (options.watchSeconds === null) {
    await printReport(options);
    return;
  }
  await watchReport(options, clampWatchInterval(options.watchSeconds));
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
