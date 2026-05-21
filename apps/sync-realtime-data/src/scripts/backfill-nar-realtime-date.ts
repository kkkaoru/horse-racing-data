import {
  buildRaceResultUrl,
  extractOddsLinks,
  fetchOdds,
  fetchRacePage,
  parseHorseWeights,
  parseRaceEntries,
  parseRaceEntryHorseNumbers,
  parseRaceResultExcludedHorseNumbers,
  parseRaceResultHorseWeights,
  parseRaceResults,
} from "../keiba-go";
import { toJstIsoString } from "../time";
import type { OddsData, OddsType, RaceEntry, RaceResult } from "../types";

declare const Bun: {
  argv: string[];
  spawn: (
    command: string[],
    options: { cwd: string; stderr: "pipe"; stdout: "pipe" },
  ) => {
    exited: Promise<number>;
    stderr: ReadableStream;
    stdout: ReadableStream;
  };
  write: (path: string, contents: string) => Promise<number>;
};

interface RaceSourceRow {
  deba_url: string;
  race_key: string;
}

const DATABASE_NAME = "sync-realtime-data";

const sqlString = (value: string): string => `'${value.replace(/'/gu, "''")}'`;

const sqlNullableString = (value: string | null | undefined): string =>
  value === null || value === undefined ? "null" : sqlString(value);

const sqlNullableNumber = (value: number | null | undefined): string =>
  value === null || value === undefined ? "null" : String(value);

const runWrangler = async (args: string[]): Promise<string> => {
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

const listRaceSources = async (targetDate: string): Promise<RaceSourceRow[]> => {
  const output = await runWrangler([
    "d1",
    "execute",
    DATABASE_NAME,
    "--remote",
    "--json",
    "--command",
    [
      "select race_key, deba_url",
      "from realtime_race_sources",
      "where source = 'nar'",
      `  and kaisai_nen = ${sqlString(targetDate.slice(0, 4))}`,
      `  and kaisai_tsukihi = ${sqlString(targetDate.slice(4, 8))}`,
      "order by keibajo_code, race_bango",
    ].join("\n"),
  ]);
  const parsed = JSON.parse(output) as Array<{ results?: RaceSourceRow[]; success: boolean }>;
  if (!parsed[0]?.success) {
    throw new Error(`failed to list race sources for ${targetDate}`);
  }
  return parsed[0].results ?? [];
};

const appendEntrySql = (
  sql: string[],
  raceKey: string,
  fetchedAt: string,
  entries: Omit<RaceEntry, "fetchedAt">[],
): void => {
  if (entries.length === 0) {
    return;
  }
  sql.push(`delete from race_entry_snapshots where race_key = ${sqlString(raceKey)};`);
  for (const entry of entries) {
    sql.push(
      [
        "insert into race_entry_snapshots",
        "  (race_key, fetched_at, horse_number, horse_name, jockey_name, status)",
        "values",
        `  (${sqlString(raceKey)}, ${sqlString(fetchedAt)}, ${sqlString(entry.horseNumber)},`,
        `   ${sqlNullableString(entry.horseName)}, ${sqlNullableString(entry.jockeyName)},`,
        `   ${sqlNullableString(entry.status)});`,
      ].join("\n"),
    );
  }
};

const appendOddsSql = (
  sql: string[],
  raceKey: string,
  fetchedAt: string,
  odds: Partial<Record<OddsType, OddsData[]>>,
): number => {
  const rows = Object.entries(odds).flatMap(([type, values]) =>
    (values ?? []).map((row) => ({ ...row, type })),
  );
  if (rows.length === 0) {
    return 0;
  }
  sql.push(`delete from odds_snapshots where race_key = ${sqlString(raceKey)};`);
  for (const row of rows) {
    sql.push(
      [
        "insert into odds_snapshots",
        "  (race_key, fetched_at, odds_type, combination, odds, min_odds, max_odds, average_odds, rank)",
        "values",
        `  (${sqlString(raceKey)}, ${sqlString(fetchedAt)}, ${sqlString(row.type)},`,
        `   ${sqlString(row.combination)}, ${sqlNullableNumber(row.odds)},`,
        `   ${sqlNullableNumber(row.minOdds)}, ${sqlNullableNumber(row.maxOdds)},`,
        `   ${sqlNullableNumber(row.averageOdds)}, ${sqlNullableNumber(row.rank)});`,
      ].join("\n"),
    );
  }
  return rows.length;
};

const appendResultSql = (
  sql: string[],
  raceKey: string,
  fetchedAt: string,
  results: Omit<RaceResult, "fetchedAt">[],
): void => {
  if (results.length === 0) {
    return;
  }
  sql.push(`delete from race_result_snapshots where race_key = ${sqlString(raceKey)};`);
  for (const result of results) {
    sql.push(
      [
        "insert into race_result_snapshots",
        "  (race_key, fetched_at, horse_number, horse_name, finish_position, time)",
        "values",
        `  (${sqlString(raceKey)}, ${sqlString(fetchedAt)}, ${sqlString(result.horseNumber)},`,
        `   ${sqlNullableString(result.horseName)}, ${sqlString(result.finishPosition)},`,
        `   ${sqlNullableString(result.time)});`,
      ].join("\n"),
    );
  }
};

const appendWeightSql = (
  sql: string[],
  raceKey: string,
  fetchedAt: string,
  weights: ReturnType<typeof parseHorseWeights>,
): void => {
  if (weights.length === 0) {
    return;
  }
  sql.push(`delete from horse_weight_snapshots where race_key = ${sqlString(raceKey)};`);
  for (const weight of weights) {
    sql.push(
      [
        "insert into horse_weight_snapshots",
        "  (race_key, fetched_at, horse_number, horse_name, weight, change_sign, change_amount)",
        "values",
        `  (${sqlString(raceKey)}, ${sqlString(fetchedAt)}, ${sqlString(weight.horseNumber)},`,
        `   ${sqlNullableString(weight.horseName)}, ${sqlNullableNumber(weight.weight)},`,
        `   ${sqlNullableString(weight.changeSign)}, ${sqlNullableNumber(weight.changeAmount)});`,
      ].join("\n"),
    );
  }
};

const applyRaceSql = async (raceKey: string, sql: string[]): Promise<void> => {
  const file = `/tmp/sync-realtime-${raceKey.replace(/[^a-zA-Z0-9_-]/gu, "_")}.sql`;
  await Bun.write(file, `${sql.join("\n")}\n`);
  await runWrangler(["d1", "execute", DATABASE_NAME, "--remote", "--json", "--file", file]);
};

const backfillRace = async (race: RaceSourceRow) => {
  const fetchedAt = toJstIsoString();
  const [entryHtml, resultHtml] = await Promise.all([
    fetchRacePage(race.deba_url),
    fetchRacePage(buildRaceResultUrl(race.deba_url)),
  ]);
  const entries = parseRaceEntries(entryHtml);
  const oddsLinks = extractOddsLinks(entryHtml, race.deba_url);
  const [odds, results] = await Promise.all([
    fetchOdds(race.deba_url, oddsLinks),
    Promise.resolve(parseRaceResults(resultHtml)),
  ]);
  const entryHorseNumbers = parseRaceEntryHorseNumbers(entryHtml);
  const excludedHorseNumbers = new Set(parseRaceResultExcludedHorseNumbers(resultHtml));
  const expectedHorseCount = entryHorseNumbers.filter(
    (horseNumber) => !excludedHorseNumbers.has(horseNumber),
  ).length;
  const weights = parseHorseWeights(entryHtml);
  const finalWeights = weights.length > 0 ? weights : parseRaceResultHorseWeights(resultHtml);

  const sql: string[] = [];
  appendEntrySql(sql, race.race_key, fetchedAt, entries);
  const oddsRows = appendOddsSql(sql, race.race_key, fetchedAt, odds);
  appendResultSql(sql, race.race_key, fetchedAt, results);
  appendWeightSql(sql, race.race_key, fetchedAt, finalWeights);
  const resultComplete = expectedHorseCount > 0 && results.length >= expectedHorseCount;
  sql.push(
    [
      "update realtime_race_sources",
      `set odds_links_json = ${sqlString(JSON.stringify(oddsLinks))},`,
      `    last_odds_fetch_at = ${oddsRows > 0 ? sqlString(fetchedAt) : "last_odds_fetch_at"},`,
      "    last_odds_queued_at = null,",
      "    odds_fetch_lock_until = null,",
      `    last_result_fetch_at = ${results.length > 0 ? sqlString(fetchedAt) : "last_result_fetch_at"},`,
      "    last_result_queued_at = null,",
      "    result_fetch_lock_until = null,",
      `    result_complete_at = ${resultComplete ? sqlString(fetchedAt) : "result_complete_at"},`,
      `    result_expected_horse_count = ${expectedHorseCount},`,
      `    result_saved_horse_count = ${results.length},`,
      `    last_weight_fetch_at = ${finalWeights.length > 0 ? sqlString(fetchedAt) : "last_weight_fetch_at"},`,
      `    updated_at = ${sqlString(fetchedAt)}`,
      `where race_key = ${sqlString(race.race_key)};`,
    ].join("\n"),
  );
  sql.push(
    [
      "insert into fetch_logs (race_key, job_type, status, message, created_at)",
      "values",
      `  (${sqlString(race.race_key)}, 'backfill-nar-realtime', 'ok',`,
      `   ${sqlString(
        JSON.stringify({
          entries: entries.length,
          expectedHorseCount,
          oddsRows,
          results: results.length,
          weights: finalWeights.length,
        }),
      )}, ${sqlString(fetchedAt)});`,
    ].join("\n"),
  );
  await applyRaceSql(race.race_key, sql);
  return {
    entries: entries.length,
    expectedHorseCount,
    oddsRows,
    raceKey: race.race_key,
    results: results.length,
    weights: finalWeights.length,
  };
};

const targetDate = Bun.argv[2];
if (!targetDate || !/^\d{8}$/u.test(targetDate)) {
  throw new Error("usage: bun src/scripts/backfill-nar-realtime-date.ts YYYYMMDD");
}

const races = await listRaceSources(targetDate);
console.log(`backfilling ${races.length} NAR races for ${targetDate}`);
const summaries = [];
for (const race of races) {
  const summary = await backfillRace(race);
  summaries.push(summary);
  console.log(JSON.stringify(summary));
}
console.log(
  JSON.stringify({
    oddsRows: summaries.reduce((total, item) => total + item.oddsRows, 0),
    races: summaries.length,
    results: summaries.reduce((total, item) => total + item.results, 0),
    weights: summaries.reduce((total, item) => total + item.weights, 0),
  }),
);
