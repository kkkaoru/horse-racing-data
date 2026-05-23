// Run with:
//   bun run running-style:push-remote -- --date 20260524

import { spawn } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { resolveRunningStyleDateYmd } from "../src/running-style-date-progress";

const WRANGLER_COMMAND = "bunx";
const DATABASE_NAME = "sync-realtime-data";

export interface PushRunningStyleDateCliArgs {
  dateYmd: string;
}

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined || value.length === 0) {
    throw new Error(`${name} requires a value.`);
  }
  return value;
};

export const parsePushRunningStyleDateCliArgs = (
  argv: readonly string[],
  now = new Date(),
): PushRunningStyleDateCliArgs => {
  let dateRaw: string | undefined;
  let year: number | undefined;
  for (let index = 0; index < argv.length; index += 1) {
    const name = argv[index];
    const value = argv[index + 1];
    if (name === "--date") {
      dateRaw = requireValue(name, value);
      index += 1;
      continue;
    }
    if (name === "--year") {
      year = Number.parseInt(requireValue(name, value), 10);
      index += 1;
      continue;
    }
    if (name === undefined) {
      break;
    }
    throw new Error(`Unknown argument: ${name}`);
  }
  if (dateRaw === undefined) {
    throw new Error("Usage: bun run running-style:push-remote -- --date MM-DD [--year YYYY]");
  }
  return { dateYmd: resolveRunningStyleDateYmd(dateRaw, year, now) };
};

interface D1ExportRow {
  bamei: string | null;
  category: string;
  horse_number: number;
  kaisai_nen: string;
  ketto_toroku_bango: string;
  model_version: string;
  p_nige: number;
  p_oikomi: number;
  p_sashi: number;
  p_senkou: number;
  predicted_at: string;
  predicted_label: string;
  race_key: string;
}

const escapeSqlString = (value: string): string => value.replaceAll("'", "''");

const formatSqlValue = (value: string | null): string =>
  value === null ? "null" : `'${escapeSqlString(value)}'`;

export const buildInsertSql = (row: D1ExportRow): string =>
  `insert or replace into race_running_styles (
  race_key, horse_number, ketto_toroku_bango, bamei, category, kaisai_nen,
  model_version, p_nige, p_senkou, p_sashi, p_oikomi, predicted_label, predicted_at
) values (
  '${escapeSqlString(row.race_key)}',
  ${row.horse_number},
  '${escapeSqlString(row.ketto_toroku_bango)}',
  ${formatSqlValue(row.bamei)},
  '${escapeSqlString(row.category)}',
  '${escapeSqlString(row.kaisai_nen)}',
  '${escapeSqlString(row.model_version)}',
  ${row.p_nige},
  ${row.p_senkou},
  ${row.p_sashi},
  ${row.p_oikomi},
  '${escapeSqlString(row.predicted_label)}',
  '${escapeSqlString(row.predicted_at)}'
);`;

const spawnWrangler = async (args: readonly string[]): Promise<void> => {
  const stderrChunks: Buffer[] = [];
  const child = spawn(WRANGLER_COMMAND, [...args], { stdio: ["ignore", "inherit", "pipe"] });
  child.stderr.on("data", (chunk: Buffer) => stderrChunks.push(chunk));
  const exitCode = await new Promise<number>((resolvePromise, rejectPromise) => {
    child.once("error", rejectPromise);
    child.once("close", (code) => resolvePromise(code ?? 0));
  });
  if (exitCode !== 0) {
    throw new Error(
      `wrangler failed (exit ${exitCode}): ${Buffer.concat(stderrChunks).toString("utf8")}`,
    );
  }
};

const readLocalRows = async (dateYmd: string): Promise<D1ExportRow[]> => {
  const command = `select race_key, horse_number, ketto_toroku_bango, bamei, category, kaisai_nen, model_version, p_nige, p_senkou, p_sashi, p_oikomi, predicted_label, predicted_at from race_running_styles where race_key like '%${dateYmd}%' order by race_key, horse_number`;
  const stderrChunks: Buffer[] = [];
  const stdoutChunks: Buffer[] = [];
  const child = spawn(
    WRANGLER_COMMAND,
    ["wrangler", "d1", "execute", DATABASE_NAME, "--local", "--command", command, "--json"],
    { stdio: ["ignore", "pipe", "pipe"] },
  );
  child.stdout.on("data", (chunk: Buffer) => stdoutChunks.push(chunk));
  child.stderr.on("data", (chunk: Buffer) => stderrChunks.push(chunk));
  const exitCode = await new Promise<number>((resolvePromise, rejectPromise) => {
    child.once("error", rejectPromise);
    child.once("close", (code) => resolvePromise(code ?? 0));
  });
  if (exitCode !== 0) {
    throw new Error(
      `local d1 read failed (exit ${exitCode}): ${Buffer.concat(stderrChunks).toString("utf8")}`,
    );
  }
  const payload = JSON.parse(Buffer.concat(stdoutChunks).toString("utf8")) as Array<{
    results: D1ExportRow[];
  }>;
  return payload[0]?.results ?? [];
};

const run = async (): Promise<void> => {
  const args = parsePushRunningStyleDateCliArgs(process.argv.slice(2));
  const rows = await readLocalRows(args.dateYmd);
  if (rows.length === 0) {
    throw new Error(`No local race_running_styles rows found for ${args.dateYmd}.`);
  }
  const tempDir = await mkdtemp(join(tmpdir(), "running-style-push-"));
  const sqlPath = join(tempDir, `running-style-${args.dateYmd}.sql`);
  try {
    await writeFile(sqlPath, rows.map(buildInsertSql).join("\n"), "utf8");
    console.log(`[running-style:push-remote] exporting rows=${rows.length} date=${args.dateYmd}`);
    await spawnWrangler([
      "wrangler",
      "d1",
      "execute",
      DATABASE_NAME,
      "--remote",
      `--file=${sqlPath}`,
    ]);
    console.log(`[running-style:push-remote] applied remote rows=${rows.length}`);
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
};

if (import.meta.main) {
  run().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}
