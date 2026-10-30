// This file runs with Bun. Fills past overseas post positions (gate_number) from ahonoora.com
// race-history pages. Fetch archives the raw pages; apply writes local only where gate_number
// is NULL and the page agrees with the stored horse number and finish. Production receives the
// values through source-ids-production (fill-only), never directly from here.
import assert from "node:assert/strict";
import { open, readFile, readdir } from "node:fs/promises";
import { join } from "node:path";
import { localHistoryEnvironment } from "./history-local-mirror";
import {
  decodeAhonoora,
  parseAhonooraRace,
  yearPageNames,
  type AhonooraRace,
  type AhonooraRunner,
} from "./sources/ahonoora-post-position-parser";
import { createTargetPool, type TargetPool } from "./storage/history-connection";
import type { PostgresPoolClient, PostgresQueryResult } from "./storage/pg-client";

export interface PostPositionInput {
  readonly argv: readonly string[];
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly fetchImpl?: (url: string) => Promise<Response>;
  readonly sleep?: (ms: number) => Promise<void>;
}
export interface StoredRunner {
  readonly race_bango: string;
  readonly umaban: string;
  readonly kakutei_chakujun: string;
  readonly ijo_kubun_code: string;
  readonly has_source_row: boolean;
  readonly gate_number: string | null;
  readonly seibetsu_code: string;
  readonly barei: string;
  readonly futan_juryo: string;
}
export interface ProfileFill {
  readonly raceBango: string;
  readonly umaban: string;
  readonly column: ProfileColumn;
  readonly value: string;
}
export type ProfileColumn = "seibetsu_code" | "barei" | "futan_juryo";
export interface GateUpdate {
  readonly raceBango: string;
  readonly umaban: string;
  readonly gate: number;
}
export interface PostPositionPlan {
  readonly updates: readonly GateUpdate[];
  readonly profileFills: readonly ProfileFill[];
  readonly profileConflicts: readonly string[];
  readonly unchanged: readonly string[];
  readonly withoutSourceRow: readonly string[];
  readonly notStored: readonly string[];
  readonly mismatches: readonly string[];
  readonly conflicts: readonly string[];
}
export interface PostPositionReport {
  readonly status: "complete";
  readonly operation: "fetched" | "planned" | "applied";
  readonly pages: number;
  readonly races: number;
  readonly unparsedPages: readonly string[];
  readonly unmatchedPages: readonly string[];
  readonly updated: number;
  readonly profileFilled: number;
  readonly profileConflicts: readonly string[];
  readonly unchanged: number;
  readonly withoutSourceRow: number;
  readonly notStored: number;
  readonly mismatches: readonly string[];
  readonly conflicts: readonly string[];
}
interface RaceMatch {
  readonly file: string;
  readonly race: AhonooraRace;
  readonly kaisaiNen: string;
  readonly kaisaiTsukihi: string;
  readonly runners: readonly StoredRunner[];
}

// JV 1 取消, 2 除外, 3 競走除外: never started, so no gate and never a page runner.
const NON_STARTER_CODES: ReadonlySet<string> = new Set(["1", "2", "3"]);
const ORIGIN: string = "https://ahonoora.com/";
const PAGE_INTERVAL_MS: number = 2000;
const FILE_MODE: number = 0o600;
const YEAR_PAGE_PATTERN: RegExp = /^[a-z0-9]+_\d{4}\.html$/u;
const VENUE_PATTERN: RegExp = /^[A-Z][0-9A-Z]$/u;
const MS_PER_DAY: number = 24 * 60 * 60 * 1000;
const USAGE: string =
  "Use post-positions-fetch DIRECTORY MIN_YEAR SERIES... or post-positions-plan|post-positions-apply DIRECTORY VENUE local [--confirm-write].";
// Older JV titles separate words with a full-width space or "・"; the pages never do.
const TITLE_SQL: string = "replace(btrim(replace(r.kyosomei_hondai, chr(12288), '')), '・', '')";
// The JST card date of a US evening race can be the next day, so both days are searched.
const RUNNERS_SQL: string = `SELECT r.kaisai_nen, r.kaisai_tsukihi, s.race_bango, s.umaban, s.kakutei_chakujun,
  s.seibetsu_code, s.barei, s.futan_juryo, s.ijo_kubun_code,
  (o.umaban IS NOT NULL) AS has_source_row, o.gate_number::text AS gate_number
FROM jvd_ra r
JOIN jvd_se s USING (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
LEFT JOIN oversea_runner_source_id o
  ON o.race_source = 'jra' AND o.source = 'netkeiba' AND o.kaisai_nen = s.kaisai_nen
 AND o.kaisai_tsukihi = s.kaisai_tsukihi AND o.keibajo_code = s.keibajo_code
 AND o.race_bango = s.race_bango AND o.umaban = s.umaban
WHERE r.keibajo_code = $1 AND r.kaisai_nen || r.kaisai_tsukihi = ANY($2::text[]) AND ${TITLE_SQL} = $3
ORDER BY r.kaisai_nen, r.kaisai_tsukihi, s.race_bango, s.umaban`;
const UPDATE_SQL: string = `UPDATE oversea_runner_source_id SET gate_number = $1::smallint, updated_at = now()
WHERE race_source = 'jra' AND source = 'netkeiba' AND kaisai_nen = $2 AND kaisai_tsukihi = $3
  AND keibajo_code = $4 AND race_bango = $5 AND umaban = $6 AND gate_number IS NULL`;

// Values the JV layout uses for "not provided"; only these are filled, never a stored value.
const EMPTY_PROFILE_VALUES: ReadonlySet<string> = new Set(["", "0", "00", "000"]);
const PROFILE_SQL: Readonly<Record<ProfileColumn, string>> = {
  seibetsu_code: profileUpdateSql("seibetsu_code"),
  barei: profileUpdateSql("barei"),
  futan_juryo: profileUpdateSql("futan_juryo"),
};
const WEIGHT_TENTHS: number = 10;
const WEIGHT_WIDTH: number = 3;

const twoDigits = (value: number): string => String(value).padStart(2, "0");

const nextDay = (date: string): string =>
  new Date(
    Date.parse(`${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6)}T00:00:00Z`) + MS_PER_DAY,
  )
    .toISOString()
    .slice(0, 10)
    .replaceAll("-", "");

const sameUmaban = (runner: AhonooraRunner, local: StoredRunner): boolean =>
  runner.umaban === null || twoDigits(runner.umaban) === local.umaban;

// Placed runners match on their unique finish (pages without 馬番, coupled "1a" entries);
// a numeric page horse number must still agree. Unplaced runners need that number.
const storedFor = (
  runner: AhonooraRunner,
  stored: readonly StoredRunner[],
): StoredRunner | undefined =>
  runner.finish === null
    ? stored.find(
        (local) =>
          runner.umaban !== null &&
          twoDigits(runner.umaban) === local.umaban &&
          !NON_STARTER_CODES.has(local.ijo_kubun_code.trim()),
      )
    : stored.find((local) => Number(local.kakutei_chakujun) === runner.finish);

// JV codes: seibetsu_code 1/2/3, barei two digits, futan_juryo in 0.1 kg (55.5 -> "555").
const pageProfile = (runner: AhonooraRunner): Readonly<Record<ProfileColumn, string | null>> => ({
  seibetsu_code: runner.sexCode,
  barei: runner.age === null ? null : twoDigits(runner.age),
  futan_juryo:
    runner.weightKg === null
      ? null
      : String(Math.round(runner.weightKg * WEIGHT_TENTHS)).padStart(WEIGHT_WIDTH, "0"),
});

const profileChanges = (
  runner: AhonooraRunner,
  local: StoredRunner,
): { column: ProfileColumn; value: string; empty: boolean }[] =>
  Object.entries(pageProfile(runner)).flatMap(([column, value]) =>
    isProfileColumn(column) && value !== null && local[column].trim() !== value
      ? [{ column, value, empty: EMPTY_PROFILE_VALUES.has(local[column].trim()) }]
      : [],
  );

export const planPostPositions = (
  race: AhonooraRace,
  stored: readonly StoredRunner[],
): PostPositionPlan => {
  const checked = race.runners.map((runner) => {
    const local: StoredRunner | undefined = storedFor(runner, stored);
    return {
      runner,
      local,
      key: `${race.date}:${race.title}:${runner.finish === null ? "-" : String(runner.finish)}:${String(runner.gate)}`,
      agrees: local !== undefined && sameUmaban(runner, local),
    };
  });
  const usable = checked.filter((entry) => entry.agrees && entry.local?.has_source_row === true);
  const profiles = checked.flatMap((entry) =>
    entry.agrees && entry.local !== undefined
      ? profileChanges(entry.runner, entry.local).map((change) => ({
          ...change,
          key: entry.key,
          raceBango: String(entry.local?.race_bango),
          umaban: String(entry.local?.umaban),
        }))
      : [],
  );
  return {
    profileFills: profiles
      .filter((change) => change.empty)
      .map(({ raceBango, umaban, column, value }) => ({ raceBango, umaban, column, value })),
    profileConflicts: profiles
      .filter((change) => !change.empty)
      .map((change) => `${change.key}:${change.column}`),
    updates: usable
      .filter((entry) => entry.local?.gate_number === null)
      .map((entry) => ({
        raceBango: String(entry.local?.race_bango),
        umaban: String(entry.local?.umaban),
        gate: entry.runner.gate,
      })),
    unchanged: usable
      .filter((entry) => entry.local?.gate_number === String(entry.runner.gate))
      .map((entry) => entry.key),
    withoutSourceRow: checked
      .filter((entry) => entry.agrees && entry.local?.has_source_row !== true)
      .map((entry) => entry.key),
    // Old editions often hold only the runners whose own histories were archived.
    notStored: checked.filter((entry) => entry.local === undefined).map((entry) => entry.key),
    mismatches: checked
      .filter((entry) => entry.local !== undefined && !entry.agrees)
      .map((entry) => entry.key),
    conflicts: usable
      .filter(
        (entry) =>
          entry.local?.gate_number !== null &&
          entry.local?.gate_number !== String(entry.runner.gate),
      )
      .map((entry) => entry.key),
  };
};

const toRunners = (result: PostgresQueryResult): (StoredRunner & { readonly day: string })[] =>
  result.rows.map((row) => ({
    day: `${String(row.kaisai_nen)}${String(row.kaisai_tsukihi)}`,
    race_bango: String(row.race_bango),
    umaban: String(row.umaban),
    kakutei_chakujun: String(row.kakutei_chakujun ?? ""),
    ijo_kubun_code: String(row.ijo_kubun_code ?? ""),
    has_source_row: row.has_source_row === true,
    gate_number: typeof row.gate_number === "string" ? row.gate_number : null,
    seibetsu_code: String(row.seibetsu_code ?? ""),
    barei: String(row.barei ?? ""),
    futan_juryo: String(row.futan_juryo ?? ""),
  }));

const matchRace = async (
  client: Pick<PostgresPoolClient, "query">,
  venue: string,
  file: string,
  race: AhonooraRace,
): Promise<RaceMatch | null> => {
  const rows = toRunners(
    await client.query(RUNNERS_SQL, [
      venue,
      [race.date, nextDay(race.date)],
      race.title.replaceAll("・", ""),
    ]),
  );
  const days: string[] = [...new Set(rows.map((row) => `${row.day}:${row.race_bango}`))];
  // Zero or several candidate races cannot be matched safely, so the page is reported instead.
  if (days.length !== 1) return null;
  const day: string = String(rows[0]?.day);
  return {
    file,
    race,
    kaisaiNen: day.slice(0, 4),
    kaisaiTsukihi: day.slice(4),
    runners: rows,
  };
};

interface ParsedPage {
  readonly file: string;
  readonly race: AhonooraRace | null;
}

// A page the parser rejects (e.g. a repeated post position) is reported, never guessed.
const parsePage = (file: string, bytes: Uint8Array): ParsedPage => {
  try {
    return { file, race: parseAhonooraRace(decodeAhonoora(bytes)) };
  } catch {
    return { file, race: null };
  }
};

const readPages = async (directory: string): Promise<ParsedPage[]> =>
  Promise.all(
    (await readdir(directory))
      .filter((name) => YEAR_PAGE_PATTERN.test(name))
      .toSorted()
      .map(async (file) => parsePage(file, await readFile(join(directory, file)))),
  );

// Serial on purpose: one transaction client, and paced requests to the source site.
const sequential = async <Item, Result>(
  items: readonly Item[],
  run: (item: Item) => Promise<Result>,
): Promise<Result[]> => {
  const [first, ...rest]: readonly Item[] = items;
  if (first === undefined) return [];
  const result: Result = await run(first);
  return [result, ...(await sequential(rest, run))];
};

const updateParameters = (venue: string, matches: readonly RaceMatch[]): string[][] =>
  matches.flatMap((match) =>
    planPostPositions(match.race, match.runners).updates.map((update) => [
      String(update.gate),
      match.kaisaiNen,
      match.kaisaiTsukihi,
      venue,
      update.raceBango,
      update.umaban,
    ]),
  );

const profileParameters = (
  venue: string,
  matches: readonly RaceMatch[],
): { column: ProfileColumn; values: string[] }[] =>
  matches.flatMap((match) =>
    planPostPositions(match.race, match.runners).profileFills.map((fill) => ({
      column: fill.column,
      values: [
        fill.value,
        match.kaisaiNen,
        match.kaisaiTsukihi,
        venue,
        fill.raceBango,
        fill.umaban,
      ],
    })),
  );

const writeUpdates = async (
  client: PostgresPoolClient,
  venue: string,
  matches: readonly RaceMatch[],
): Promise<void> => {
  try {
    await client.query("BEGIN");
    const counts: number[] = await sequential(updateParameters(venue, matches), async (values) =>
      Number((await client.query(UPDATE_SQL, values)).rowCount),
    );
    const profileCounts: number[] = await sequential(
      profileParameters(venue, matches),
      async ({ column, values }) =>
        Number((await client.query(PROFILE_SQL[column], values)).rowCount),
    );
    assert(
      [...counts, ...profileCounts].every((count) => count === 1),
      "A post-position update did not match exactly one row.",
    );
    await client.query("COMMIT");
  } catch (error: unknown) {
    await client.query("ROLLBACK");
    throw error;
  }
};

const sumPlans = (
  matches: readonly RaceMatch[],
): Omit<
  PostPositionReport,
  "status" | "operation" | "pages" | "races" | "unparsedPages" | "unmatchedPages"
> => {
  const plans: PostPositionPlan[] = matches.map((match) =>
    planPostPositions(match.race, match.runners),
  );
  return {
    updated: plans.flatMap((plan) => plan.updates).length,
    profileFilled: plans.flatMap((plan) => plan.profileFills).length,
    profileConflicts: plans.flatMap((plan) => plan.profileConflicts),
    unchanged: plans.flatMap((plan) => plan.unchanged).length,
    withoutSourceRow: plans.flatMap((plan) => plan.withoutSourceRow).length,
    notStored: plans.flatMap((plan) => plan.notStored).length,
    mismatches: plans.flatMap((plan) => plan.mismatches),
    conflicts: plans.flatMap((plan) => plan.conflicts),
  };
};

const storePostPositions = async (
  input: PostPositionInput,
  directory: string,
  venue: string,
): Promise<PostPositionReport> => {
  const apply: boolean = input.argv[0] === "post-positions-apply";
  const pages = await readPages(directory);
  const target: TargetPool = createTargetPool(localHistoryEnvironment(input.env), "local");
  const client: PostgresPoolClient = await target.pool.connect();
  try {
    // One pg client runs one query at a time.
    const found: (RaceMatch | null)[] = await sequential(pages, async ({ file, race }) =>
      race === null ? null : matchRace(client, venue, file, race),
    );
    const matches: RaceMatch[] = found.filter((match): match is RaceMatch => match !== null);
    const report = sumPlans(matches);
    if (apply && report.updated + report.profileFilled > 0)
      await writeUpdates(client, venue, matches);
    return {
      status: "complete",
      operation: apply ? "applied" : "planned",
      pages: pages.length,
      races: matches.length,
      unparsedPages: pages.filter((page) => page.race === null).map((page) => page.file),
      unmatchedPages: pages
        .filter((page) => page.race !== null && !matches.some((match) => match.file === page.file))
        .map((page) => page.file),
      ...report,
    };
  } finally {
    client.release();
    await target.pool.end();
  }
};

const writeNew = async (path: string, bytes: Uint8Array): Promise<boolean> => {
  try {
    const handle = await open(path, "wx", FILE_MODE);
    try {
      await handle.writeFile(bytes);
    } finally {
      await handle.close();
    }
    return true;
  } catch (error: unknown) {
    if (error instanceof Error && "code" in error && error.code === "EEXIST") return false;
    throw error;
  }
};

const fetchBytes = async (input: PostPositionInput, file: string): Promise<Uint8Array> => {
  const fetchImpl: (url: string) => Promise<Response> = input.fetchImpl ?? fetch;
  const response: Response = await fetchImpl(`${ORIGIN}${file}`);
  if (!response.ok) throw new Error(`Ahonoora page ${file} returned ${String(response.status)}.`);
  return new Uint8Array(await response.arrayBuffer());
};

const pause = async (input: PostPositionInput): Promise<void> =>
  (input.sleep ?? ((ms: number) => new Promise((resolve) => setTimeout(resolve, ms))))(
    PAGE_INTERVAL_MS,
  );

// Serial and paced; existing archived pages are kept and never fetched again.
const fetchPages = async (
  input: PostPositionInput,
  directory: string,
  minYear: number,
  series: readonly string[],
): Promise<PostPositionReport> => {
  const files: string[] = (
    await sequential(series, async (name) =>
      yearPageNames(decodeAhonoora(await fetchBytes(input, `${name}.html`)), name, minYear),
    )
  ).flat();
  const existing: ReadonlySet<string> = new Set(await readdir(directory));
  const written: boolean[] = await sequential(
    files.filter((name) => !existing.has(name)),
    async (file) => {
      await pause(input);
      return writeNew(join(directory, file), await fetchBytes(input, file));
    },
  );
  return {
    status: "complete",
    operation: "fetched",
    pages: written.filter(Boolean).length,
    races: files.length,
    unparsedPages: [],
    unmatchedPages: [],
    updated: 0,
    profileFilled: 0,
    profileConflicts: [],
    unchanged: 0,
    withoutSourceRow: 0,
    notStored: 0,
    mismatches: [],
    conflicts: [],
  };
};

export const runPostPositions = async (input: PostPositionInput): Promise<PostPositionReport> => {
  const [action, directory, second, third, fourth]: readonly (string | undefined)[] = input.argv;
  if (action === "post-positions-fetch") {
    assert(
      directory !== undefined && /^\d{4}$/u.test(String(second)) && input.argv.length > 3,
      USAGE,
    );
    return fetchPages(input, directory, Number(second), input.argv.slice(3));
  }
  const plan: boolean = action === "post-positions-plan" && input.argv.length === 4;
  const apply: boolean =
    action === "post-positions-apply" && input.argv.length === 5 && fourth === "--confirm-write";
  assert(
    (plan || apply) &&
      directory !== undefined &&
      VENUE_PATTERN.test(String(second)) &&
      third === "local",
    USAGE,
  );
  return storePostPositions(input, directory, String(second));
};

function profileUpdateSql(column: ProfileColumn): string {
  return `UPDATE jvd_se SET ${column} = $1
WHERE kaisai_nen = $2 AND kaisai_tsukihi = $3 AND keibajo_code = $4 AND race_bango = $5 AND umaban = $6
  AND (${column} IS NULL OR btrim(${column}) IN ('', '0', '00', '000'))`;
}

function isProfileColumn(value: string): value is ProfileColumn {
  return value === "seibetsu_code" || value === "barei" || value === "futan_juryo";
}
