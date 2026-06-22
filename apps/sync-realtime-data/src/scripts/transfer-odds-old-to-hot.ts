// One-shot CLI to transfer NAR odds_snapshots from the legacy
// `sync-realtime-data` D1 into the new `sync-realtime-data-hot` D1 for a
// given JST date.
//
// Use case (2026-06-22): the high-frequency hot-worker per-minute odds
// polling cron entered a silent-death state and stopped firing for 6+
// hours, so the final 10-15 min betting window was never captured into the
// hot D1. The companion `backfill-nar-realtime-date.ts` re-fetches the
// closing odds from keiba.go and writes them to the legacy D1; this script
// then copies those rows into the hot D1 (which is what the viewer reads
// from) and purges the per-race `odds:latest:<race_key>` KV mirror so the
// next viewer fetch re-reads fresh from D1 instead of returning the stale
// mirror.
//
// Run with bun:
//   bun apps/sync-realtime-data/src/scripts/transfer-odds-old-to-hot.ts 20260622
//
// The script:
//   1. Reads all NAR `odds_snapshots` rows for the date from the legacy D1
//      using `wrangler d1 execute sync-realtime-data --remote --json`. The
//      legacy schema has an FK to `realtime_race_sources`; the hot schema
//      does NOT have that table, so no FK-parent inserts are needed.
//   2. Builds multi-row `INSERT OR IGNORE INTO odds_snapshots ...` batches
//      and applies them against the hot D1 with `wrangler d1 execute
//      sync-realtime-data-hot --remote --file ...`.
//   3. Purges the per-race `odds:latest:<race_key>` KV keys in ONE
//      `wrangler kv bulk delete` call.
//
// `INSERT OR IGNORE` is the safe-rerun shape — the hot `odds_snapshots`
// table has its own AUTOINCREMENT `id` primary key plus indexes on
// `(race_key, odds_type, fetched_at)`, so rerunning with the same set of
// (race_key, odds_type, combination, fetched_at) source rows is a no-op
// once the rows already exist.

import type { OddsType } from "../types";

const LEGACY_DATABASE_NAME = "sync-realtime-data";
const HOT_DATABASE_NAME = "sync-realtime-data-hot";
const HOT_ODDS_LATEST_KV_NAMESPACE_ID = "844a7bf58c514402b7d5ae3149734052";
const ODDS_LATEST_KV_KEY_PREFIX = "odds:latest";
const DATE_ARG_PATTERN = /^\d{8}$/u;
const SQL_TMP_PATH_PREFIX = "/tmp/transfer-odds-old-to-hot-";
const KV_KEYS_TMP_PATH_PREFIX = "/tmp/transfer-odds-old-to-hot-kv-keys-";
const PATH_SANITIZE_PATTERN = /[^a-zA-Z0-9_-]/gu;
const DEFAULT_INSERT_CHUNK_SIZE = 50;
const PATH_PARENT_NAVIGATION = "../../";
const USAGE_MESSAGE = "usage: bun src/scripts/transfer-odds-old-to-hot.ts YYYYMMDD";

export interface OldOddsRow {
  race_key: string;
  fetched_at: string;
  odds_type: OddsType;
  combination: string;
  odds: number | null;
  min_odds: number | null;
  max_odds: number | null;
  average_odds: number | null;
  rank: number | null;
}

interface BuildInsertBatchesInput {
  rows: readonly OldOddsRow[];
  chunkSize: number;
}

interface WranglerRunner {
  (args: readonly string[]): Promise<string>;
}

interface FileWriter {
  (path: string, contents: string): Promise<unknown>;
}

interface ListOldRowsDeps {
  runWranglerImpl: WranglerRunner;
}

interface ApplyInsertBatchesInput {
  batches: readonly string[];
  runWranglerImpl: WranglerRunner;
  targetDate: string;
  writeFileImpl: FileWriter;
}

interface PurgeKvKeysInput {
  keysJson: string;
  runWranglerImpl: WranglerRunner;
  targetDate: string;
  writeFileImpl: FileWriter;
}

interface RunTransferInput {
  listOldRows: (targetDate: string) => Promise<OldOddsRow[]>;
  log: (message: string) => void;
  runWranglerImpl: WranglerRunner;
  targetDate: string;
  writeFileImpl: FileWriter;
}

interface RunTransferSummary {
  oddsRowsInserted: number;
  kvKeysPurged: number;
  raceKeys: number;
}

interface WranglerD1ResponseEntry {
  success: boolean;
  results?: OldOddsRow[];
}

export const sqlString = (value: string): string => `'${value.replace(/'/gu, "''")}'`;

export const sqlNullableNumber = (value: number | null | undefined): string =>
  value === null || value === undefined ? "null" : String(value);

export const parseDateArg = (argv: readonly string[]): string => {
  const candidate = argv[2];
  if (!candidate || !DATE_ARG_PATTERN.test(candidate)) {
    throw new Error(USAGE_MESSAGE);
  }
  return candidate;
};

const buildSingleRowTuple = (row: OldOddsRow): string =>
  [
    `(${sqlString(row.race_key)}`,
    sqlString(row.fetched_at),
    sqlString(row.odds_type),
    sqlString(row.combination),
    sqlNullableNumber(row.odds),
    sqlNullableNumber(row.min_odds),
    sqlNullableNumber(row.max_odds),
    sqlNullableNumber(row.average_odds),
    `${sqlNullableNumber(row.rank)})`,
  ].join(", ");

const buildBatchSql = (chunk: readonly OldOddsRow[]): string =>
  [
    "insert or ignore into odds_snapshots",
    "  (race_key, fetched_at, odds_type, combination, odds, min_odds, max_odds, average_odds, rank)",
    `values ${chunk.map(buildSingleRowTuple).join(",\n       ")};`,
  ].join("\n");

const chunkRows = (rows: readonly OldOddsRow[], chunkSize: number): OldOddsRow[][] =>
  Array.from({ length: Math.ceil(rows.length / chunkSize) }, (_unused, index) =>
    rows.slice(index * chunkSize, index * chunkSize + chunkSize),
  );

export const buildInsertBatches = ({ rows, chunkSize }: BuildInsertBatchesInput): string[] => {
  if (rows.length === 0 || chunkSize <= 0) {
    return [];
  }
  return chunkRows(rows, chunkSize).map(buildBatchSql);
};

const uniqueRaceKeys = (rows: readonly OldOddsRow[]): string[] => {
  const seen = new Set<string>();
  rows.forEach((row) => seen.add(row.race_key));
  return Array.from(seen);
};

export const buildKvKeysJson = (rows: readonly OldOddsRow[]): string =>
  JSON.stringify(uniqueRaceKeys(rows).map((raceKey) => `${ODDS_LATEST_KV_KEY_PREFIX}:${raceKey}`));

export const listOldOddsRows = async (
  targetDate: string,
  { runWranglerImpl }: ListOldRowsDeps,
): Promise<OldOddsRow[]> => {
  const output = await runWranglerImpl([
    "d1",
    "execute",
    LEGACY_DATABASE_NAME,
    "--remote",
    "--json",
    "--command",
    [
      "select race_key, fetched_at, odds_type, combination, odds, min_odds, max_odds, average_odds, rank",
      "from odds_snapshots",
      `where race_key like ${sqlString(`nar:${targetDate}:%`)}`,
      "order by race_key, fetched_at, odds_type, combination",
    ].join("\n"),
  ]);
  const parsed = JSON.parse(output) as WranglerD1ResponseEntry[];
  if (!parsed[0]?.success) {
    throw new Error(`failed to list old odds_snapshots for ${targetDate}`);
  }
  return parsed[0].results ?? [];
};

const sanitizeForPath = (value: string): string => value.replace(PATH_SANITIZE_PATTERN, "_");

export const buildSqlFilePath = (targetDate: string, batchIndex: number): string =>
  `${SQL_TMP_PATH_PREFIX}${sanitizeForPath(targetDate)}-${batchIndex}.sql`;

export const buildKvKeysFilePath = (targetDate: string): string =>
  `${KV_KEYS_TMP_PATH_PREFIX}${sanitizeForPath(targetDate)}.json`;

export const applyInsertBatches = async ({
  batches,
  runWranglerImpl,
  targetDate,
  writeFileImpl,
}: ApplyInsertBatchesInput): Promise<void> => {
  await Promise.all(
    batches.map((sql, batchIndex) =>
      writeFileImpl(buildSqlFilePath(targetDate, batchIndex), `${sql}\n`),
    ),
  );
  for (const [batchIndex] of batches.entries()) {
    await runWranglerImpl([
      "d1",
      "execute",
      HOT_DATABASE_NAME,
      "--remote",
      "--json",
      "--file",
      buildSqlFilePath(targetDate, batchIndex),
    ]);
  }
};

export const purgeKvKeys = async ({
  keysJson,
  runWranglerImpl,
  targetDate,
  writeFileImpl,
}: PurgeKvKeysInput): Promise<void> => {
  const file = buildKvKeysFilePath(targetDate);
  await writeFileImpl(file, keysJson);
  await runWranglerImpl([
    "kv",
    "bulk",
    "delete",
    "--namespace-id",
    HOT_ODDS_LATEST_KV_NAMESPACE_ID,
    "--remote",
    file,
  ]);
};

export const runTransfer = async ({
  listOldRows,
  log,
  runWranglerImpl,
  targetDate,
  writeFileImpl,
}: RunTransferInput): Promise<RunTransferSummary> => {
  const rows = await listOldRows(targetDate);
  log(`fetched ${rows.length} legacy odds_snapshots rows for ${targetDate}`);
  const batches = buildInsertBatches({ chunkSize: DEFAULT_INSERT_CHUNK_SIZE, rows });
  if (batches.length > 0) {
    await applyInsertBatches({ batches, runWranglerImpl, targetDate, writeFileImpl });
  }
  log(`applied ${batches.length} insert batches into hot D1`);
  const raceKeys = uniqueRaceKeys(rows);
  const keysJson = buildKvKeysJson(rows);
  if (raceKeys.length > 0) {
    await purgeKvKeys({ keysJson, runWranglerImpl, targetDate, writeFileImpl });
  }
  log(`purged ${raceKeys.length} odds:latest KV keys`);
  const summary: RunTransferSummary = {
    kvKeysPurged: raceKeys.length,
    oddsRowsInserted: rows.length,
    raceKeys: raceKeys.length,
  };
  log(JSON.stringify(summary));
  return summary;
};

interface BunGlobal {
  argv: readonly string[];
  spawn: (
    command: readonly string[],
    options: { cwd: string; stderr: "pipe"; stdout: "pipe" },
  ) => {
    exited: Promise<number>;
    stderr: ReadableStream;
    stdout: ReadableStream;
  };
  write: (path: string, contents: string) => Promise<number>;
}

declare const Bun: BunGlobal;

/* v8 ignore start - bun-only CLI entry point, structurally untestable */
if (import.meta.main) {
  const runWranglerImpl: WranglerRunner = async (args) => {
    const proc = Bun.spawn(["bun", "wrangler", ...args], {
      cwd: new URL(PATH_PARENT_NAVIGATION, import.meta.url).pathname,
      stderr: "pipe",
      stdout: "pipe",
    });
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
      proc.exited,
    ]);
    if (exitCode !== 0) {
      throw new Error(`wrangler ${args.join(" ")} failed: ${stderr || stdout}`);
    }
    return stdout;
  };
  const targetDate = parseDateArg(Bun.argv);
  await runTransfer({
    listOldRows: (date) => listOldOddsRows(date, { runWranglerImpl }),
    log: console.log,
    runWranglerImpl,
    targetDate,
    writeFileImpl: Bun.write,
  });
}
/* v8 ignore stop */
