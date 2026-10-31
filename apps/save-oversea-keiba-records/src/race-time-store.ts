// This file runs with Bun. Fills race times and closing-3F times of stored horse histories from
// archived horse pages (already fetched through the production proxy): only NULL columns are set.
import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";
import { localHistoryEnvironment } from "./history-local-mirror";
import {
  parseSecondaryHorseRaceTimes,
  type SecondaryHorseRaceTime,
  type SecondaryRaceTimeIndexes,
  type SecondaryResultMarkupProfile,
} from "./sources/secondary-result-parser";
import { createTargetPool, type TargetPool } from "./storage/history-connection";
import type { PostgresPoolClient } from "./storage/pg-client";

export interface RaceTimeStoreInput {
  readonly argv: readonly string[];
  readonly env: Readonly<Record<string, string | undefined>>;
}
export interface RaceTimeRow {
  readonly sourceHorseId: string;
  readonly sourceRaceId: string;
  readonly raceTimeText: string | null;
  readonly raceTimeSeconds: string | null;
  readonly lastThreeFurlongsSeconds: string | null;
  readonly raceTimeSource: string;
  readonly raceTimeKind: "measured" | "estimated";
}
export interface StoredRaceTime {
  readonly sourceHorseId: string;
  readonly sourceRaceId: string;
  readonly raceTimeText: string | null;
  readonly lastThreeFurlongsSeconds: string | null;
}
export interface RaceTimeStorePlan {
  readonly fills: readonly RaceTimeRow[];
  readonly conflicts: readonly string[];
  readonly unmatched: number;
}
export interface RaceTimeStoreReport {
  readonly status: "complete";
  readonly operation: "planned" | "applied";
  readonly target: "local" | "production";
  readonly archivedHorses: number;
  readonly parsedRaces: number;
  readonly withTime: number;
  readonly withLastThreeFurlongs: number;
  readonly filled: number;
  readonly unmatched: number;
  readonly conflicts: readonly string[];
}

const SOURCE: string = "netkeiba";
const PROFILE_ENV: string = "OVERSEA_RACE_TIME_PROFILE_PATH";
const HORSE_DIRECTORY_PATTERN: RegExp = /^horse-([0-9A-Za-z]+)(?:-v\d+)?$/u;
const RAW_PAGE_SUFFIX: string = ".raw";
const TIME_PATTERN: RegExp = /^(?:(\d+):)?(\d{1,2}\.\d{1,2})$/u;
const LAST_THREE_PATTERN: RegExp = /^\d{2}\.\d$/u;
const SECONDS_PER_MINUTE: number = 60;
const TIME_DECIMALS: number = 2;
const SELECT_SQL: string = `SELECT source_horse_id, source_race_id, race_time_text,
  last_three_furlongs_seconds::text AS last_three_furlongs_seconds
FROM oversea_horse_race_history
WHERE source = $1 AND source_horse_id = ANY($2::text[]) AND source_race_id IS NOT NULL`;
// Unavailable values are sent as "" (the client has no NULL parameter) and turned back into NULL.
const UPDATE_SQL: string = `UPDATE oversea_horse_race_history SET
  race_time_text = COALESCE(race_time_text, NULLIF($4, '')),
  race_time_seconds = COALESCE(race_time_seconds, NULLIF($5, '')::numeric),
  last_three_furlongs_seconds = COALESCE(last_three_furlongs_seconds, NULLIF($6, '')::numeric),
  race_time_source = CASE WHEN race_time_text IS NULL AND $4 <> '' THEN $7 ELSE race_time_source END,
  race_time_kind = CASE WHEN race_time_text IS NULL AND $4 <> '' THEN $8 ELSE race_time_kind END,
  updated_at = now()
WHERE source = $1 AND source_horse_id = $2 AND source_race_id = $3
  AND ((race_time_text IS NULL AND $4 <> '') OR (last_three_furlongs_seconds IS NULL AND $6 <> ''))`;
const USAGE: string =
  "Use race-times-plan HISTORY_ROOT TARGET or race-times-apply HISTORY_ROOT TARGET --confirm-write; TARGET is local or production.";

// "1:48.03" or "58.12" (seconds only); anything else is not a time and stays unset.
export const raceTimeSeconds = (text: string): string | null => {
  const match: RegExpExecArray | null = TIME_PATTERN.exec(text);
  return match === null
    ? null
    : (Number(match[1] ?? "0") * SECONDS_PER_MINUTE + Number(match[2])).toFixed(TIME_DECIMALS);
};

export const raceTimeRow = (sourceHorseId: string, time: SecondaryHorseRaceTime): RaceTimeRow => {
  const seconds: string | null = raceTimeSeconds(time.raceTimeText);
  return {
    sourceHorseId,
    sourceRaceId: time.sourceRaceId,
    raceTimeText: seconds === null ? null : time.raceTimeText,
    raceTimeSeconds: seconds,
    lastThreeFurlongsSeconds: LAST_THREE_PATTERN.test(time.lastThreeFurlongsText)
      ? time.lastThreeFurlongsText
      : null,
    raceTimeSource: SOURCE,
    raceTimeKind: "measured",
  };
};

const rowKey = (row: { sourceHorseId: string; sourceRaceId: string }): string =>
  `${row.sourceHorseId}:${row.sourceRaceId}`;

const differs = (stored: string | null, parsed: string | null): boolean =>
  stored !== null && parsed !== null && Number(stored) !== Number(parsed);

export const planRaceTimeStore = (
  parsed: readonly RaceTimeRow[],
  existing: readonly StoredRaceTime[],
): RaceTimeStorePlan => {
  const storedByKey: ReadonlyMap<string, StoredRaceTime> = new Map(
    existing.map((row) => [rowKey(row), row]),
  );
  const matched: RaceTimeRow[] = parsed.filter((row) => storedByKey.has(rowKey(row)));
  const conflicts: string[] = matched.flatMap((row) => {
    const stored: StoredRaceTime | undefined = storedByKey.get(rowKey(row));
    return [
      stored?.raceTimeText != null &&
      row.raceTimeText !== null &&
      stored.raceTimeText !== row.raceTimeText
        ? `${rowKey(row)}:race_time_text`
        : null,
      differs(stored?.lastThreeFurlongsSeconds ?? null, row.lastThreeFurlongsSeconds)
        ? `${rowKey(row)}:last_three_furlongs_seconds`
        : null,
    ].filter((conflict): conflict is string => conflict !== null);
  });
  const fills: RaceTimeRow[] = matched.filter((row) => {
    const stored: StoredRaceTime | undefined = storedByKey.get(rowKey(row));
    return (
      (stored?.raceTimeText == null && row.raceTimeText !== null) ||
      (stored?.lastThreeFurlongsSeconds == null && row.lastThreeFurlongsSeconds !== null)
    );
  });
  return { fills, conflicts, unmatched: parsed.length - matched.length };
};

const isIndexes = (value: unknown): value is SecondaryRaceTimeIndexes =>
  typeof value === "object" &&
  value !== null &&
  "time" in value &&
  Number.isSafeInteger(value.time) &&
  "lastThreeFurlongs" in value &&
  Number.isSafeInteger(value.lastThreeFurlongs);

const loadIndexes = async (env: RaceTimeStoreInput["env"]): Promise<SecondaryRaceTimeIndexes> => {
  const path: string | undefined = env[PROFILE_ENV];
  assert(path !== undefined && path !== "", `${PROFILE_ENV} is required.`);
  const indexes: unknown = JSON.parse(await readFile(path, "utf8"));
  assert(
    isIndexes(indexes),
    "The race time profile needs time and lastThreeFurlongs cell indexes.",
  );
  return indexes;
};

const isMarkupPlan = (
  value: unknown,
): value is { profile: { markup: SecondaryResultMarkupProfile } } =>
  typeof value === "object" &&
  value !== null &&
  "profile" in value &&
  typeof value.profile === "object" &&
  value.profile !== null &&
  "markup" in value.profile;

// Each archived horse snapshot carries the markup it was collected with; unreadable pages are skipped.
const parseHorseDirectory = async (
  root: string,
  directory: string,
  indexes: SecondaryRaceTimeIndexes,
): Promise<RaceTimeRow[]> => {
  const sourceHorseId: string = String(HORSE_DIRECTORY_PATTERN.exec(directory)?.[1]);
  const plan: unknown = JSON.parse(await readFile(join(root, directory, "plan.json"), "utf8"));
  assert(isMarkupPlan(plan), `History plan of ${directory} has no markup profile.`);
  const pages: string[] = (await readdir(join(root, directory, "pages"))).filter((name) =>
    name.endsWith(RAW_PAGE_SUFFIX),
  );
  const parsed: RaceTimeRow[][] = await Promise.all(
    pages.toSorted().map(async (page) => {
      const html: string = await readFile(join(root, directory, "pages", page), "utf8");
      try {
        return parseSecondaryHorseRaceTimes(html, plan.profile.markup, indexes).map((time) =>
          raceTimeRow(sourceHorseId, time),
        );
      } catch {
        return [];
      }
    }),
  );
  return parsed.flat();
};

const storedText = (value: unknown): string | null => (typeof value === "string" ? value : null);

const selectExisting = async (
  client: Pick<PostgresPoolClient, "query">,
  rows: readonly RaceTimeRow[],
): Promise<StoredRaceTime[]> =>
  (
    await client.query(SELECT_SQL, [SOURCE, [...new Set(rows.map((row) => row.sourceHorseId))]])
  ).rows.map((row) => ({
    sourceHorseId: String(row.source_horse_id),
    sourceRaceId: String(row.source_race_id),
    raceTimeText: storedText(row.race_time_text),
    lastThreeFurlongsSeconds: storedText(row.last_three_furlongs_seconds),
  }));

const writeFills = async (
  target: TargetPool,
  rows: readonly RaceTimeRow[],
): Promise<RaceTimeStorePlan> => {
  const client: PostgresPoolClient = await target.pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("LOCK TABLE oversea_horse_race_history IN SHARE ROW EXCLUSIVE MODE");
    const plan: RaceTimeStorePlan = planRaceTimeStore(rows, await selectExisting(client, rows));
    for (const row of plan.fills) {
      await client.query(UPDATE_SQL, [
        SOURCE,
        row.sourceHorseId,
        row.sourceRaceId,
        row.raceTimeText ?? "",
        row.raceTimeSeconds ?? "",
        row.lastThreeFurlongsSeconds ?? "",
        row.raceTimeSource,
        row.raceTimeKind,
      ]);
    }
    const readback: RaceTimeStorePlan = planRaceTimeStore(rows, await selectExisting(client, rows));
    assert.equal(readback.fills.length, 0, "Race time readback is missing filled values.");
    assert.deepEqual(
      readback.conflicts,
      plan.conflicts,
      "Race time readback changed stored values.",
    );
    await client.query("COMMIT");
    return plan;
  } catch (error: unknown) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
};

// Several snapshots (e.g. -v2) of one horse list the same race; the first readable one wins.
const uniqueRows = (rows: readonly RaceTimeRow[]): RaceTimeRow[] => [
  ...new Map(rows.toReversed().map((row) => [rowKey(row), row])).values(),
];

export interface RaceTimeApplyInput {
  readonly env: RaceTimeStoreInput["env"];
  readonly target: "local" | "production";
  readonly rows: readonly RaceTimeRow[];
  readonly apply: boolean;
}

// Plans (or applies, fill-only in one locked transaction) parsed rows against one target.
export const applyRaceTimeRows = async ({
  env,
  target,
  rows,
  apply,
}: RaceTimeApplyInput): Promise<RaceTimeStorePlan> => {
  if (rows.length === 0) return planRaceTimeStore(rows, []);
  const pool: TargetPool = createTargetPool(
    target === "local" ? localHistoryEnvironment(env) : env,
    target,
  );
  try {
    return apply
      ? await writeFills(pool, rows)
      : planRaceTimeStore(rows, await selectExisting(pool.pool, rows));
  } finally {
    await pool.pool.end();
  }
};

export const runRaceTimeStore = async ({
  argv,
  env,
}: RaceTimeStoreInput): Promise<RaceTimeStoreReport> => {
  const [action, root, target, confirmation]: readonly (string | undefined)[] = argv;
  const apply: boolean = action === "race-times-apply";
  assert(
    root !== undefined &&
      (target === "local" || target === "production") &&
      ((action === "race-times-plan" && argv.length === 3) ||
        (apply && argv.length === 4 && confirmation === "--confirm-write")),
    USAGE,
  );
  const indexes: SecondaryRaceTimeIndexes = await loadIndexes(env);
  const directories: string[] = (await readdir(root)).filter((name) =>
    HORSE_DIRECTORY_PATTERN.test(name),
  );
  const rows: RaceTimeRow[] = uniqueRows(
    (
      await Promise.all(
        directories.map((directory) => parseHorseDirectory(root, directory, indexes)),
      )
    ).flat(),
  );
  const plan: RaceTimeStorePlan = await applyRaceTimeRows({ env, target, rows, apply });
  return {
    status: "complete",
    operation: apply ? "applied" : "planned",
    target,
    archivedHorses: new Set(directories.map((name) => HORSE_DIRECTORY_PATTERN.exec(name)?.[1]))
      .size,
    parsedRaces: rows.length,
    withTime: rows.filter((row) => row.raceTimeText !== null).length,
    withLastThreeFurlongs: rows.filter((row) => row.lastThreeFurlongsSeconds !== null).length,
    filled: plan.fills.length,
    unmatched: plan.unmatched,
    conflicts: plan.conflicts,
  };
};
