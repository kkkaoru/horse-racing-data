// This file runs with Bun. Adds missing runners of a past overseas edition from a cached secondary
// result page. Existing jvd_ra / jvd_se rows are never updated: official JV rows stay authoritative.
import { mapJvdRows } from "./domain/jvd-mapper";
import {
  parseSecondaryRaceResult,
  type SecondaryRaceResult,
  type SecondaryRaceResultProfile,
  type SecondaryRaceResultRunner,
} from "./sources/secondary-race-result-parser";
import { buildJvdRaUpsert, buildJvdSeUpsert } from "./storage/upsert-sql";
import type { ExecutableSqlStatement, PostgresQueryOutcome } from "./storage/pg-client";
import type { JvdRaRow, JvdSeRow, ParsedRace, ParsedRunner, RaceStorageIdentity } from "./types";

export interface PastEditionExecutor {
  readonly execute: (statement: ExecutableSqlStatement) => Promise<PostgresQueryOutcome>;
}
export interface PastEditionPorts {
  readonly readFile: (path: string) => Promise<string>;
  readonly executor: PastEditionExecutor;
  readonly withTransaction: (
    callback: (executor: PastEditionExecutor) => Promise<void>,
  ) => Promise<void>;
  readonly log: (message: string) => void;
}
export interface RunPastEditionInput {
  readonly argv: readonly string[];
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly ports: PastEditionPorts;
}
export interface PastEditionArgs {
  readonly file: string;
  readonly sourceRaceId: string;
  readonly raceName: string;
  readonly venueCode: string;
  readonly raceNumber: string;
  readonly confirmedFieldSize: number | null;
  readonly apply: boolean;
}
export interface PastEditionPlan {
  readonly raceKey: string;
  readonly insertRace: JvdRaRow | null;
  readonly insertRunners: readonly JvdSeRow[];
  readonly keptRunners: number;
  readonly sourceIds: readonly ExecutableSqlStatement[];
  readonly parsedRunners: number;
  readonly starters: number;
  readonly sourcePartial: boolean;
}

export const OVERSEA_RACE_RESULT_PROFILE_PATH: string = "OVERSEA_RACE_RESULT_PROFILE_PATH";
const USAGE: string =
  "Use past-edition FILE --source-race-id ID --race-name NAME --venue-code CODE --race-number NN [--confirmed-field-size N] [--apply].";
const VALUE_OPTIONS: ReadonlySet<string> = new Set([
  "--source-race-id",
  "--race-name",
  "--venue-code",
  "--race-number",
  "--confirmed-field-size",
]);
const FIELD_SIZE_PATTERN: RegExp = /^\d{1,2}$/u;
const VENUE_CODE_PATTERN: RegExp = /^[A-Z][0-9A-Z]$/u;
const RACE_NUMBER_PATTERN: RegExp = /^\d{2}$/u;
const SOURCE_RACE_ID_PATTERN: RegExp = /^[0-9A-Za-z]+$/u;
const TIME_PATTERN: RegExp = /^(\d+):(\d{2})\.(\d)/u;
// JV abnormality codes for the published non-finish labels.
const IRREGULAR_CODES: ReadonlyMap<string, string> = new Map([
  ["取", "1"],
  ["除", "2"],
  ["中", "4"],
  ["失", "5"],
]);
const NON_STARTERS: ReadonlySet<string> = new Set(["取", "除"]);
const TWO_DIGITS: number = 2;
const SELECT_RACE_SQL: string =
  "SELECT count(*)::text AS count FROM jvd_ra WHERE kaisai_nen = $1 AND kaisai_tsukihi = $2 AND keibajo_code = $3 AND race_bango = $4";
const SELECT_RUNNERS_SQL: string =
  "SELECT umaban FROM jvd_se WHERE kaisai_nen = $1 AND kaisai_tsukihi = $2 AND keibajo_code = $3 AND race_bango = $4";
const INSERT_SOURCE_ID_SQL: string = `INSERT INTO oversea_runner_source_id
  (race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban, source, source_horse_id, source_jockey_id, source_trainer_id, source_url)
VALUES ('jra', $1, $2, $3, $4, $5, 'netkeiba', $6, NULLIF($7, ''), NULLIF($8, ''), $9)
ON CONFLICT DO NOTHING`;

export const parsePastEditionArgs = (argv: readonly string[]): PastEditionArgs => {
  const [file, ...rest]: readonly string[] = argv;
  const options: Map<string, string> = new Map();
  const flags: string[] = [];
  rest.forEach((value: string, index: number) => {
    if (VALUE_OPTIONS.has(rest[index - 1] ?? "")) return;
    if (VALUE_OPTIONS.has(value)) options.set(value, rest[index + 1] ?? "");
    else flags.push(value);
  });
  const args: PastEditionArgs = {
    file: file ?? "",
    sourceRaceId: options.get("--source-race-id") ?? "",
    raceName: options.get("--race-name") ?? "",
    venueCode: options.get("--venue-code") ?? "",
    raceNumber: options.get("--race-number") ?? "",
    confirmedFieldSize: options.has("--confirmed-field-size")
      ? Number(options.get("--confirmed-field-size"))
      : null,
    apply: flags.includes("--apply"),
  };
  const valid: boolean =
    args.file !== "" &&
    args.raceName !== "" &&
    SOURCE_RACE_ID_PATTERN.test(args.sourceRaceId) &&
    VENUE_CODE_PATTERN.test(args.venueCode) &&
    RACE_NUMBER_PATTERN.test(args.raceNumber) &&
    (!options.has("--confirmed-field-size") ||
      FIELD_SIZE_PATTERN.test(options.get("--confirmed-field-size") ?? "")) &&
    flags.every((flag: string) => flag === "--apply");
  if (!valid) throw new Error(USAGE);
  return args;
};

const pad = (value: number): string => String(value).padStart(TWO_DIGITS, "0");

// Published "m:ss.ff" becomes JV "mss" + tenths; missing times stay the JV zero placeholder.
const encodeTime = (text: string): string => {
  const match: RegExpExecArray | null = TIME_PATTERN.exec(text);
  return match === null ? "0000" : `${String(match[1])}${String(match[2])}${String(match[3])}`;
};

const toParsedRunner = (runner: SecondaryRaceResultRunner): ParsedRunner => ({
  horseNumber: runner.horseNumber,
  gate: runner.gate,
  horseName: runner.horseName,
  sex: runner.sex,
  age: runner.age,
  coatColour: "",
  weightCarriedKg: runner.weightCarriedKg,
  jockeyAbbrev: runner.jockeyName,
  trainerAbbrev: runner.trainerName,
  trainerCountry: "",
  owner: "",
  winOdds: runner.winOdds,
  popularity: runner.popularity,
  formRecord: "",
  sire: "",
  dam: "",
  damsire: "",
});

const toParsedRace = (result: SecondaryRaceResult, raceName: string): ParsedRace => ({
  raceName,
  grade: "G1",
  date: result.date,
  venue: "",
  country: "",
  distanceMetres: result.distanceMetres,
  surface: result.surface,
  direction: result.direction,
  startTime: "",
  runners: result.runners.map(toParsedRunner),
});

const withResult = (row: JvdSeRow, runner: SecondaryRaceResultRunner): JvdSeRow => {
  const finish: string = runner.finishPosition === null ? "00" : pad(runner.finishPosition);
  return {
    ...row,
    kakutei_chakujun: finish,
    nyusen_juni: finish,
    ijo_kubun_code: IRREGULAR_CODES.get(runner.finishPositionText) ?? "0",
    soha_time: encodeTime(runner.timeText),
  };
};

const sourceIdStatement = (
  row: JvdSeRow,
  runner: SecondaryRaceResultRunner,
  sourceRaceId: string,
): ExecutableSqlStatement => ({
  text: INSERT_SOURCE_ID_SQL,
  values: [
    row.kaisai_nen,
    row.kaisai_tsukihi,
    row.keibajo_code,
    row.race_bango,
    row.umaban,
    String(runner.sourceHorseId),
    runner.sourceJockeyId ?? "",
    runner.sourceTrainerId ?? "",
    `https://db.netkeiba.com/race/${sourceRaceId}/`,
  ],
});

interface MappedRunner {
  readonly row: JvdSeRow;
  readonly runner: SecondaryRaceResultRunner;
}
interface ExistingRace {
  readonly raceExists: boolean;
  readonly umaban: ReadonlySet<string>;
}

export const buildPastEditionPlan = (
  result: SecondaryRaceResult,
  args: PastEditionArgs,
  existing: ExistingRace,
): PastEditionPlan => {
  const race: ParsedRace = toParsedRace(result, args.raceName);
  const storageIdentity: RaceStorageIdentity = {
    venueCode: args.venueCode,
    raceNumber: args.raceNumber,
  };
  const runners: readonly MappedRunner[] = result.runners.flatMap(
    (runner: SecondaryRaceResultRunner): MappedRunner[] =>
      mapJvdRows({
        race: { ...race, runners: [toParsedRunner(runner)] },
        storageIdentity,
        resolvedCodes: new Map(),
      }).runners.map((row: JvdSeRow) => ({ row: withResult(row, runner), runner })),
  );
  const starters: number = result.runners.filter(
    (runner: SecondaryRaceResultRunner) => !NON_STARTERS.has(runner.finishPositionText),
  ).length;
  const raceRow: JvdRaRow = mapJvdRows({ race, storageIdentity, resolvedCodes: new Map() }).race;
  // Some source pages list only part of the field; a horse-number gap without an explicit
  // field-size confirmation must not become a new race with a partial runner list.
  const highestNumber: number = Math.max(...result.runners.map((runner) => runner.horseNumber));
  const sourcePartial: boolean =
    highestNumber > result.runners.length && args.confirmedFieldSize !== starters;
  const createsPartialRace: boolean = sourcePartial && !existing.raceExists;
  return {
    raceKey: `${raceRow.kaisai_nen}/${raceRow.kaisai_tsukihi}/${args.venueCode}/${args.raceNumber}`,
    insertRace:
      existing.raceExists || createsPartialRace ? null : { ...raceRow, shusso_tosu: pad(starters) },
    insertRunners: runners
      .filter(() => !createsPartialRace)
      .filter(({ row }: MappedRunner) => !existing.umaban.has(row.umaban))
      .map(({ row }: MappedRunner) => row),
    keptRunners: runners.filter(({ row }: MappedRunner) => existing.umaban.has(row.umaban)).length,
    sourceIds: runners.flatMap(({ row, runner }: MappedRunner) =>
      runner.sourceHorseId === null ? [] : [sourceIdStatement(row, runner, args.sourceRaceId)],
    ),
    parsedRunners: result.runners.length,
    starters,
    sourcePartial,
  };
};

const isProfile = (value: unknown): value is SecondaryRaceResultProfile =>
  typeof value === "object" &&
  value !== null &&
  "tableMarker" in value &&
  typeof value.tableMarker === "string" &&
  "fields" in value &&
  typeof value.fields === "object" &&
  "trainerPrefixes" in value &&
  Array.isArray(value.trainerPrefixes);

const loadProfile = async (
  env: RunPastEditionInput["env"],
  ports: PastEditionPorts,
): Promise<SecondaryRaceResultProfile> => {
  const path: string | undefined = env[OVERSEA_RACE_RESULT_PROFILE_PATH];
  if (path === undefined || path === "")
    throw new Error(`Set ${OVERSEA_RACE_RESULT_PROFILE_PATH} to the private race-result profile.`);
  const profile: unknown = JSON.parse(await ports.readFile(path));
  if (!isProfile(profile)) throw new Error("Private race-result profile is invalid.");
  return profile;
};

const report = (plan: PastEditionPlan, apply: boolean): string =>
  JSON.stringify({
    raceKey: plan.raceKey,
    mode: apply ? "applied" : "dry-run",
    insertRace: plan.insertRace !== null,
    insertRunners: plan.insertRunners.map((row: JvdSeRow) => row.umaban),
    keptExistingRunners: plan.keptRunners,
    sourceIdInserts: plan.sourceIds.length,
    parsedRunners: plan.parsedRunners,
    starters: plan.starters,
    sourcePartial: plan.sourcePartial,
  });

export const runPastEdition = async ({
  argv,
  env,
  ports,
}: RunPastEditionInput): Promise<string> => {
  const args: PastEditionArgs = parsePastEditionArgs(argv);
  const [html, profile]: [string, SecondaryRaceResultProfile] = await Promise.all([
    ports.readFile(args.file),
    loadProfile(env, ports),
  ]);
  const result: SecondaryRaceResult = parseSecondaryRaceResult(html, profile);
  const keys: readonly string[] = [
    result.date.slice(0, 4),
    result.date.slice(5, 7) + result.date.slice(8, 10),
    args.venueCode,
    args.raceNumber,
  ];
  const [race, runners]: [PostgresQueryOutcome, PostgresQueryOutcome] = await Promise.all([
    ports.executor.execute({ text: SELECT_RACE_SQL, values: keys }),
    ports.executor.execute({ text: SELECT_RUNNERS_SQL, values: keys }),
  ]);
  const plan: PastEditionPlan = buildPastEditionPlan(result, args, {
    raceExists: race.rows[0]?.count !== "0",
    umaban: new Set(runners.rows.map((row) => String(row.umaban))),
  });
  if (args.apply) {
    await ports.withTransaction(async (executor: PastEditionExecutor) => {
      const statements: readonly ExecutableSqlStatement[] = [
        ...(plan.insertRace === null ? [] : [buildJvdRaUpsert(plan.insertRace)]),
        ...plan.insertRunners.map(buildJvdSeUpsert),
        ...plan.sourceIds,
      ];
      for (const statement of statements) await executor.execute(statement);
    });
  }
  const message: string = report(plan, args.apply);
  ports.log(message);
  return message;
};
