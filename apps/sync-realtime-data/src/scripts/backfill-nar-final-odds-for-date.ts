// One-shot CLI to backfill **final odds** for NAR races on a given date into the
// new `sync-realtime-data-hot` D1 `odds_snapshots` table.
//
// Use case (2026-06-03): the high-frequency hot-worker odds polling stalled
// at 17:21 JST on 2026-06-02 and never produced any odds rows for 0603. From
// the dev box (which can reach keiba.go directly, unlike the Worker behind
// CloudFront blocks) we re-fetch each already-started NAR race's final odds
// from keiba.go and insert them into the new D1.
//
// Run with bun:
//   bun run apps/sync-realtime-data/src/scripts/backfill-nar-final-odds-for-date.ts 20260603
//
// The script:
//   1. Reads race sources for the date from the legacy `sync-realtime-data`
//      D1 (only place `realtime_race_sources` lives — the hot D1 has only
//      odds tables) using `wrangler d1 execute ... --remote --json`.
//   2. Filters to races whose start time has passed (= final odds available).
//   3. For each race, fetches the entry HTML, extracts the odds-page links,
//      and fetches `tansho` / `fukusho` / `umaren` / etc. via the same parser
//      pipeline the production worker uses.
//   4. Computes `fetched_at = race_start + 2 min` (matches the production
//      `FINAL_ODDS_FETCH_DELAY_MINUTES` slot definition in `time.ts`).
//   5. Emits per-race idempotent SQL (`INSERT ... SELECT ... WHERE NOT EXISTS`)
//      against the new `sync-realtime-data-hot` D1.
//   6. Invokes `wrangler d1 execute sync-realtime-data-hot --remote --file ...`
//      per race via `Bun.spawn`.
//
// The `odds_snapshots` table has no natural unique constraint (id is
// AUTOINCREMENT), so we guard each INSERT with `NOT EXISTS (SELECT 1 ...)`
// matching on `(race_key, odds_type, combination, fetched_at)` so reruns
// are safe.

import { extractOddsLinks, fetchOdds, fetchRacePage } from "../keiba-go";
import type { OddsData, OddsType } from "../types";

const LEGACY_DATABASE_NAME = "sync-realtime-data";
const HOT_DATABASE_NAME = "sync-realtime-data-hot";
const FINAL_ODDS_DELAY_MINUTES = 2;
const MS_PER_MINUTE = 60_000;
const DATE_ARG_PATTERN = /^\d{8}$/u;
const SQL_TMP_PATH_PREFIX = "/tmp/backfill-nar-final-odds-";
const SQL_PATH_SANITIZE_PATTERN = /[^a-zA-Z0-9_-]/gu;

export interface NarRaceSource {
  deba_url: string;
  race_key: string;
  race_start_at_jst: string;
}

interface OddsRowWithType extends OddsData {
  type: OddsType;
}

interface BuildOddsInsertSqlInput {
  fetchedAt: string;
  odds: Partial<Record<OddsType, OddsData[]>>;
  raceKey: string;
}

interface FilterAlreadyStartedInput {
  now: Date;
  races: readonly NarRaceSource[];
}

interface WranglerRunner {
  (args: string[]): Promise<string>;
}

interface FileWriter {
  (path: string, contents: string): Promise<unknown>;
}

interface BackfillRaceInput {
  fetchOddsImpl: typeof fetchOdds;
  fetchPageImpl: typeof fetchRacePage;
  runWranglerImpl: WranglerRunner;
  race: NarRaceSource;
  writeFileImpl: FileWriter;
}

export interface BackfillRaceSummary {
  fetchedAt: string;
  oddsRows: number;
  raceKey: string;
  typesFetched: number;
}

interface RunBackfillInput {
  backfillRaceImpl: (race: NarRaceSource) => Promise<BackfillRaceSummary>;
  listSources: (targetDate: string) => Promise<NarRaceSource[]>;
  log: (message: string) => void;
  now: Date;
  targetDate: string;
}

interface RunBackfillSummary {
  oddsRows: number;
  races: number;
  skipped: number;
}

interface ListSourcesDeps {
  runWranglerImpl: WranglerRunner;
}

interface WranglerD1Response {
  success: boolean;
  results?: NarRaceSource[];
}

export const sqlString = (value: string): string => `'${value.replace(/'/gu, "''")}'`;

export const sqlNullableNumber = (value: number | null | undefined): string =>
  value === null || value === undefined ? "null" : String(value);

export const parseTargetDate = (argv: readonly string[]): string => {
  const candidate = argv[2];
  if (!candidate || !DATE_ARG_PATTERN.test(candidate)) {
    throw new Error("usage: bun src/scripts/backfill-nar-final-odds-for-date.ts YYYYMMDD");
  }
  return candidate;
};

export const buildFinalFetchedAt = (raceStartAtJst: string): string => {
  const parsed = new Date(raceStartAtJst);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`invalid race_start_at_jst: ${raceStartAtJst}`);
  }
  const shifted = new Date(parsed.getTime() + FINAL_ODDS_DELAY_MINUTES * MS_PER_MINUTE);
  const jstShift = 9 * 60 * MS_PER_MINUTE;
  return `${new Date(shifted.getTime() + jstShift).toISOString().slice(0, 19)}+09:00`;
};

export const filterAlreadyStarted = ({ now, races }: FilterAlreadyStartedInput): NarRaceSource[] =>
  races.filter((race) => {
    const startMs = new Date(race.race_start_at_jst).getTime();
    return Number.isFinite(startMs) && startMs <= now.getTime();
  });

const ODDS_TYPES_FOR_FLATTEN: readonly OddsType[] = [
  "tansho",
  "fukusho",
  "wakuren",
  "umaren",
  "umatan",
  "wide",
  "3renpuku",
  "3rentan",
];

const flattenOddsByType = (odds: Partial<Record<OddsType, OddsData[]>>): OddsRowWithType[] =>
  ODDS_TYPES_FOR_FLATTEN.flatMap((type) => (odds[type] ?? []).map((row) => ({ ...row, type })));

const buildSingleInsertSql = (row: OddsRowWithType, raceKey: string, fetchedAt: string): string =>
  [
    "insert into odds_snapshots",
    "  (race_key, fetched_at, odds_type, combination, odds, min_odds, max_odds, average_odds, rank)",
    `select ${sqlString(raceKey)}, ${sqlString(fetchedAt)}, ${sqlString(row.type)},`,
    `       ${sqlString(row.combination)}, ${sqlNullableNumber(row.odds)},`,
    `       ${sqlNullableNumber(row.minOdds)}, ${sqlNullableNumber(row.maxOdds)},`,
    `       ${sqlNullableNumber(row.averageOdds)}, ${sqlNullableNumber(row.rank)}`,
    "where not exists (",
    "  select 1 from odds_snapshots",
    `  where race_key = ${sqlString(raceKey)}`,
    `    and odds_type = ${sqlString(row.type)}`,
    `    and combination = ${sqlString(row.combination)}`,
    `    and fetched_at = ${sqlString(fetchedAt)}`,
    ");",
  ].join("\n");

export const buildOddsInsertSql = ({
  fetchedAt,
  odds,
  raceKey,
}: BuildOddsInsertSqlInput): string[] =>
  flattenOddsByType(odds).map((row) => buildSingleInsertSql(row, raceKey, fetchedAt));

export const listNarRaceSources = async (
  targetDate: string,
  { runWranglerImpl }: ListSourcesDeps,
): Promise<NarRaceSource[]> => {
  const output = await runWranglerImpl([
    "d1",
    "execute",
    LEGACY_DATABASE_NAME,
    "--remote",
    "--json",
    "--command",
    [
      "select race_key, deba_url, race_start_at_jst",
      "from realtime_race_sources",
      "where source = 'nar'",
      `  and kaisai_nen = ${sqlString(targetDate.slice(0, 4))}`,
      `  and kaisai_tsukihi = ${sqlString(targetDate.slice(4, 8))}`,
      "order by race_start_at_jst, keibajo_code, race_bango",
    ].join("\n"),
  ]);
  const parsed = JSON.parse(output) as WranglerD1Response[];
  if (!parsed[0]?.success) {
    throw new Error(`failed to list NAR race sources for ${targetDate}`);
  }
  return parsed[0].results ?? [];
};

const sanitizeRaceKeyForPath = (raceKey: string): string =>
  raceKey.replace(SQL_PATH_SANITIZE_PATTERN, "_");

export const buildSqlFilePath = (raceKey: string): string =>
  `${SQL_TMP_PATH_PREFIX}${sanitizeRaceKeyForPath(raceKey)}.sql`;

export const backfillRaceFinalOdds = async ({
  fetchOddsImpl,
  fetchPageImpl,
  race,
  runWranglerImpl,
  writeFileImpl,
}: BackfillRaceInput): Promise<BackfillRaceSummary> => {
  const fetchedAt = buildFinalFetchedAt(race.race_start_at_jst);
  const entryHtml = await fetchPageImpl(race.deba_url);
  const oddsLinks = extractOddsLinks(entryHtml, race.deba_url);
  const odds = await fetchOddsImpl(race.deba_url, oddsLinks);
  const insertSql = buildOddsInsertSql({ fetchedAt, odds, raceKey: race.race_key });
  if (insertSql.length === 0) {
    return { fetchedAt, oddsRows: 0, raceKey: race.race_key, typesFetched: 0 };
  }
  const file = buildSqlFilePath(race.race_key);
  await writeFileImpl(file, `${insertSql.join("\n")}\n`);
  await runWranglerImpl(["d1", "execute", HOT_DATABASE_NAME, "--remote", "--json", "--file", file]);
  return {
    fetchedAt,
    oddsRows: insertSql.length,
    raceKey: race.race_key,
    typesFetched: Object.keys(odds).length,
  };
};

export const runBackfill = async ({
  backfillRaceImpl,
  listSources,
  log,
  now,
  targetDate,
}: RunBackfillInput): Promise<RunBackfillSummary> => {
  const sources = await listSources(targetDate);
  const alreadyStarted = filterAlreadyStarted({ now, races: sources });
  const skipped = sources.length - alreadyStarted.length;
  log(
    `backfilling NAR final odds for ${targetDate}: ${alreadyStarted.length} ready / ${sources.length} total (skipped ${skipped} upcoming)`,
  );
  const summaries: BackfillRaceSummary[] = [];
  for (const race of alreadyStarted) {
    const summary = await backfillRaceImpl(race);
    summaries.push(summary);
    log(JSON.stringify(summary));
  }
  const total: RunBackfillSummary = {
    oddsRows: summaries.reduce((acc, item) => acc + item.oddsRows, 0),
    races: summaries.length,
    skipped,
  };
  log(JSON.stringify(total));
  return total;
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
      cwd: new URL("../../", import.meta.url).pathname,
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
  const targetDate = parseTargetDate(Bun.argv);
  await runBackfill({
    backfillRaceImpl: (race) =>
      backfillRaceFinalOdds({
        fetchOddsImpl: fetchOdds,
        fetchPageImpl: fetchRacePage,
        race,
        runWranglerImpl,
        writeFileImpl: Bun.write,
      }),
    listSources: (date) => listNarRaceSources(date, { runWranglerImpl }),
    log: console.log,
    now: new Date(),
    targetDate,
  });
}
/* v8 ignore stop */
