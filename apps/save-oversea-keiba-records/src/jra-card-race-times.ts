// This file runs with Bun. Race times (and closing 3F where published) of the current runners'
// recent races from a saved official JRA overseas card, filled only where empty on the target.
// The card's markup lives in an operator-owned private profile, never in this repository.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { localHistoryEnvironment } from "./history-local-mirror";
import {
  applyRaceTimeRows,
  raceTimeSeconds,
  type RaceTimeRow,
  type RaceTimeStorePlan,
} from "./race-time-store";
import { createTargetPool, type TargetPool } from "./storage/history-connection";

export interface JraCardRaceTimeProfile {
  readonly runnerStart: string;
  readonly pastRunStart: string;
  readonly horseNumberPattern: string;
  readonly datePattern: string;
  readonly finishPattern: string;
  readonly timePattern: string;
  readonly lastThreeFurlongsPattern: string;
}
export interface JraCardPastRun {
  readonly horseNumber: string;
  readonly raceDate: string;
  readonly finishPosition: number | null;
  readonly raceTimeText: string | null;
  readonly lastThreeFurlongsSeconds: string | null;
}
export interface StoredHistoryRace {
  readonly sourceHorseId: string;
  readonly sourceRaceId: string;
  readonly raceDate: string;
}
export interface JraCardRaceTimesInput {
  readonly argv: readonly string[];
  readonly env: Readonly<Record<string, string | undefined>>;
}
export interface JraCardRaceTimesReport {
  readonly status: "complete";
  readonly operation: "planned" | "applied";
  readonly target: "local" | "production";
  readonly runners: number;
  readonly pastRuns: number;
  readonly matched: number;
  readonly unmatched: number;
  readonly filled: number;
  readonly conflicts: readonly string[];
}

const SOURCE: string = "jra";
const PROFILE_ENV: string = "OVERSEA_JRA_CARD_RACE_TIME_PROFILE_PATH";
const PROFILE_KEYS: readonly (keyof JraCardRaceTimeProfile)[] = [
  "runnerStart",
  "pastRunStart",
  "horseNumberPattern",
  "datePattern",
  "finishPattern",
  "timePattern",
  "lastThreeFurlongsPattern",
];
const TAG_PATTERN: RegExp = /<[^>]*>/gu;
const SPACE_PATTERN: RegExp = /\s+/gu;
const LAST_THREE_PATTERN: RegExp = /^\d{2}\.\d$/u;
const DATE_PAD: number = 2;
const HORSE_NUMBER_PAD: number = 2;
const KEY_PATTERN: RegExp = /^\d{4}$/u;
const VENUE_PATTERN: RegExp = /^[0-9A-Z]{2}$/u;
const RACE_PATTERN: RegExp = /^\d{2}$/u;
const SOURCE_IDS_SQL: string = `SELECT umaban, source_horse_id FROM oversea_runner_source_id
WHERE source = 'netkeiba' AND race_source = 'jra' AND kaisai_nen = $1 AND kaisai_tsukihi = $2
  AND keibajo_code = $3 AND race_bango = $4 AND source_horse_id IS NOT NULL`;
const HISTORY_SQL: string = `SELECT source_horse_id, source_race_id, to_char(race_date, 'YYYY-MM-DD') AS race_date
FROM oversea_horse_race_history
WHERE source = 'netkeiba' AND source_horse_id = ANY($1::text[]) AND source_race_id IS NOT NULL`;
const USAGE: string =
  "Use jra-card-race-times-plan CARD YYYY MMDD VENUE RACE TARGET or jra-card-race-times-apply CARD YYYY MMDD VENUE RACE TARGET --confirm-write; TARGET is local or production.";

const text = (html: string): string => html.replace(TAG_PATTERN, " ").replace(SPACE_PATTERN, " ");

const firstGroup = (pattern: RegExp, value: string): string | null =>
  pattern.exec(value)?.[1]?.trim() ?? null;

const isProfile = (value: unknown): value is JraCardRaceTimeProfile =>
  typeof value === "object" &&
  value !== null &&
  PROFILE_KEYS.every((key) => key in value && typeof Reflect.get(value, key) === "string");

const pastRun = (
  horseNumber: string,
  cell: string,
  patterns: Readonly<Record<keyof JraCardRaceTimeProfile, RegExp>>,
): JraCardPastRun | null => {
  const plain: string = text(cell);
  const date: RegExpExecArray | null = patterns.datePattern.exec(plain);
  if (date === null) return null;
  const finish: string | null = firstGroup(patterns.finishPattern, plain);
  const time: string | null = firstGroup(patterns.timePattern, plain);
  const closing: string | null = firstGroup(patterns.lastThreeFurlongsPattern, plain);
  return {
    horseNumber,
    raceDate: `${String(date[1])}-${String(date[2]).padStart(DATE_PAD, "0")}-${String(date[3]).padStart(DATE_PAD, "0")}`,
    finishPosition: finish !== null && /^\d+$/u.test(finish) ? Number(finish) : null,
    raceTimeText: time !== null && raceTimeSeconds(time) !== null ? time : null,
    lastThreeFurlongsSeconds: closing !== null && LAST_THREE_PATTERN.test(closing) ? closing : null,
  };
};

// Each runner block starts at the profile's runner token; its recent races follow as cells.
export const parseJraCardPastRuns = (
  html: string,
  profile: JraCardRaceTimeProfile,
): JraCardPastRun[] => {
  const patterns: Readonly<Record<keyof JraCardRaceTimeProfile, RegExp>> = {
    runnerStart: new RegExp(profile.runnerStart, "u"),
    pastRunStart: new RegExp(profile.pastRunStart, "u"),
    horseNumberPattern: new RegExp(profile.horseNumberPattern, "u"),
    datePattern: new RegExp(profile.datePattern, "u"),
    finishPattern: new RegExp(profile.finishPattern, "u"),
    timePattern: new RegExp(profile.timePattern, "u"),
    lastThreeFurlongsPattern: new RegExp(profile.lastThreeFurlongsPattern, "u"),
  };
  return html
    .split(profile.runnerStart)
    .slice(1)
    .flatMap((block) => {
      const [head = "", ...cells] = block.split(profile.pastRunStart);
      const number: string | null = firstGroup(patterns.horseNumberPattern, text(head));
      if (number === null || !/^\d+$/u.test(number)) return [];
      const horseNumber: string = number.padStart(HORSE_NUMBER_PAD, "0");
      return cells
        .map((cell) => pastRun(horseNumber, cell, patterns))
        .filter((run): run is JraCardPastRun => run !== null);
    });
};

// The card lists each horse's own time: a winner's is measured, a beaten runner's is the
// winner's time plus the published margin, so it is kept as estimated.
export const matchJraCardPastRuns = (
  runs: readonly JraCardPastRun[],
  horseIds: ReadonlyMap<string, string>,
  stored: readonly StoredHistoryRace[],
): RaceTimeRow[] =>
  runs.flatMap((run) => {
    const sourceHorseId: string | undefined = horseIds.get(run.horseNumber);
    const races: StoredHistoryRace[] = stored.filter(
      (race) => race.sourceHorseId === sourceHorseId && race.raceDate === run.raceDate,
    );
    const [race] = races;
    if (races.length !== 1 || race === undefined) return [];
    if (run.raceTimeText === null && run.lastThreeFurlongsSeconds === null) return [];
    return [
      {
        sourceHorseId: race.sourceHorseId,
        sourceRaceId: race.sourceRaceId,
        raceTimeText: run.raceTimeText,
        raceTimeSeconds: run.raceTimeText === null ? null : raceTimeSeconds(run.raceTimeText),
        lastThreeFurlongsSeconds: run.lastThreeFurlongsSeconds,
        raceTimeSource: SOURCE,
        raceTimeKind: run.finishPosition === 1 ? "measured" : "estimated",
      },
    ];
  });

const loadProfile = async (env: JraCardRaceTimesInput["env"]): Promise<JraCardRaceTimeProfile> => {
  const path: string | undefined = env[PROFILE_ENV];
  assert(path !== undefined && path !== "", `${PROFILE_ENV} is required.`);
  const profile: unknown = JSON.parse(await readFile(path, "utf8"));
  assert(isProfile(profile), "The JRA card race time profile is incomplete.");
  return profile;
};

const runnerHorseIds = async (
  env: JraCardRaceTimesInput["env"],
  key: readonly string[],
): Promise<Map<string, string>> => {
  const pool: TargetPool = createTargetPool(localHistoryEnvironment(env), "local");
  try {
    const result = await pool.pool.query(SOURCE_IDS_SQL, [...key]);
    return new Map(result.rows.map((row) => [String(row.umaban), String(row.source_horse_id)]));
  } finally {
    await pool.pool.end();
  }
};

const storedRaces = async (
  env: JraCardRaceTimesInput["env"],
  target: "local" | "production",
  horseIds: readonly string[],
): Promise<StoredHistoryRace[]> => {
  const pool: TargetPool = createTargetPool(
    target === "local" ? localHistoryEnvironment(env) : env,
    target,
  );
  try {
    const result = await pool.pool.query(HISTORY_SQL, [[...horseIds]]);
    return result.rows.map((row) => ({
      sourceHorseId: String(row.source_horse_id),
      sourceRaceId: String(row.source_race_id),
      raceDate: String(row.race_date),
    }));
  } finally {
    await pool.pool.end();
  }
};

export const runJraCardRaceTimes = async ({
  argv,
  env,
}: JraCardRaceTimesInput): Promise<JraCardRaceTimesReport> => {
  const [action, card, year, monthDay, venue, race, target, confirmation] = argv;
  const apply: boolean =
    action === "jra-card-race-times-apply" &&
    argv.length === 8 &&
    confirmation === "--confirm-write";
  assert(
    card !== undefined &&
      KEY_PATTERN.test(String(year)) &&
      KEY_PATTERN.test(String(monthDay)) &&
      VENUE_PATTERN.test(String(venue)) &&
      RACE_PATTERN.test(String(race)) &&
      (target === "local" || target === "production") &&
      ((action === "jra-card-race-times-plan" && argv.length === 7) || apply),
    USAGE,
  );
  const profile: JraCardRaceTimeProfile = await loadProfile(env);
  const runs: JraCardPastRun[] = parseJraCardPastRuns(await readFile(card, "utf8"), profile);
  const horseIds: Map<string, string> = await runnerHorseIds(env, [
    String(year),
    String(monthDay),
    String(venue),
    String(race),
  ]);
  const stored: StoredHistoryRace[] = await storedRaces(env, target, [...horseIds.values()]);
  const rows: RaceTimeRow[] = matchJraCardPastRuns(runs, horseIds, stored);
  const plan: RaceTimeStorePlan = await applyRaceTimeRows({ env, target, rows, apply });
  return {
    status: "complete",
    operation: apply ? "applied" : "planned",
    target,
    runners: new Set(runs.map((run) => run.horseNumber)).size,
    pastRuns: runs.length,
    matched: rows.length,
    unmatched: runs.length - rows.length,
    filled: plan.fills.length,
    conflicts: plan.conflicts,
  };
};
